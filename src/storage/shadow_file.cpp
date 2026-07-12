#include "storage/shadow_file.h"

#include <algorithm>
#include <array>
#include <limits>

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "common/random_engine.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/database_header.h"
#include "storage/file_db_id_utils.h"
#include "storage/file_handle.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

static constexpr uint64_t CHECKPOINT_PAGE_WATERMARK_MAGIC = 0x4B555A554350574D;
static constexpr uint64_t SHADOW_REPLAY_BITMAP_BYTES = 32ull * 1024 * 1024;
static constexpr uint64_t BITS_PER_BITMAP_WORD = 64;
static constexpr uint64_t SHADOW_REPLAY_BITMAP_WORDS =
    SHADOW_REPLAY_BITMAP_BYTES / sizeof(uint64_t);
static constexpr uint64_t PAGES_PER_SHADOW_REPLAY_BITMAP =
    SHADOW_REPLAY_BITMAP_WORDS * BITS_PER_BITMAP_WORD;
static constexpr uint64_t MAX_SHADOW_REPLAY_BITMAPS =
    (static_cast<uint64_t>(INVALID_PAGE_IDX) + PAGES_PER_SHADOW_REPLAY_BITMAP - 1) /
    PAGES_PER_SHADOW_REPLAY_BITMAP;

void ShadowPageRecord::serialize(Serializer& serializer) const {
    serializer.write<file_idx_t>(originalFileIdx);
    serializer.write<page_idx_t>(originalPageIdx);
    serializer.write<page_idx_t>(shadowPageIdx);
}

ShadowPageRecord ShadowPageRecord::deserialize(
    Deserializer& deserializer, bool includeShadowPageIdx) {
    file_idx_t originalFileIdx = INVALID_FILE_IDX;
    page_idx_t originalPageIdx = INVALID_PAGE_IDX;
    page_idx_t shadowPageIdx = INVALID_PAGE_IDX;
    deserializer.deserializeValue<file_idx_t>(originalFileIdx);
    deserializer.deserializeValue<page_idx_t>(originalPageIdx);
    if (includeShadowPageIdx) {
        deserializer.deserializeValue<page_idx_t>(shadowPageIdx);
    }
    return ShadowPageRecord{originalFileIdx, originalPageIdx, shadowPageIdx};
}

ShadowFile::ShadowFile(BufferManager& bm, VirtualFileSystem* vfs, const std::string& databasePath)
    : bm{bm}, shadowFilePath{StorageUtils::getShadowFilePath(databasePath)}, vfs{vfs},
      shadowingFH{nullptr} {
    KU_ASSERT(vfs);
}

void ShadowFile::clearShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        shadowPagesMap.at(originalFile).erase(originalPage);
        if (shadowPagesMap.at(originalFile).empty()) {
            shadowPagesMap.erase(originalFile);
        }
    }
}

page_idx_t ShadowFile::getOrCreateShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        return shadowPagesMap[originalFile][originalPage];
    }
    const auto shadowPageIdx = getOrCreateShadowingFH()->addNewPage();
    shadowPagesMap[originalFile][originalPage] = shadowPageIdx;
    shadowPageRecords.push_back({originalFile, originalPage, shadowPageIdx});
    return shadowPageIdx;
}

page_idx_t ShadowFile::getShadowPage(file_idx_t originalFile, page_idx_t originalPage) const {
    KU_ASSERT(hasShadowPage(originalFile, originalPage));
    return shadowPagesMap.at(originalFile).at(originalPage);
}

void ShadowFile::applyShadowPages(ClientContext& context) const {
    const auto pageBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    auto dataFileInfo = StorageManager::Get(context)->getDataFH()->getFileInfo();
    KU_ASSERT(shadowingFH);
    uint64_t pagesApplied = 0;
    for (const auto& record : shadowPageRecords) {
        shadowingFH->readPageFromDisk(pageBuffer.get(), record.shadowPageIdx);
        dataFileInfo->writeFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            record.originalPageIdx * KUZU_PAGE_SIZE);
        // Acquire page state lock before updating the in-memory frame. This ensures concurrent
        // optimistic readers will detect the version change and retry, seeing the new page data.
        MemoryManager::Get(context)->getBufferManager()->updateFrameIfPageIsInFrame(
            record.originalFileIdx, pageBuffer.get(), record.originalPageIdx);
        if (pageAppliedHookForTesting) {
            pageAppliedHookForTesting(++pagesApplied);
        }
    }
    dataFileInfo->syncFile();
}

static ku_uuid_t getOldDatabaseID(FileInfo& dataFileInfo) {
    auto oldHeader = DatabaseHeader::readDatabaseHeader(dataFileInfo);
    if (!oldHeader.has_value()) {
        throw InternalException("Found a shadow file for database {} but no valid database header. "
                                "The database is corrupted, please recreate it.");
    }
    return oldHeader->databaseID;
}

void ShadowFile::replayShadowPageRecords(ClientContext& context, ku_uuid_t expectedDatabaseID,
    std::optional<ku_uuid_t> expectedCheckpointID,
    std::optional<page_idx_t> expectedCheckpointStartDataFileNumPages,
    std::optional<page_idx_t> expectedCheckpointEndDataFileNumPages) {
    if (context.getDBConfig()->readOnly) {
        throw RuntimeException("Couldn't replay shadow pages under read-only mode. Please re-open "
                               "the database with read-write mode to replay shadow pages.");
    }
    auto vfs = VirtualFileSystem::GetUnsafe(context);
    auto shadowFilePath = StorageUtils::getShadowFilePath(context.getDatabasePath());
    auto shadowFileInfo = vfs->openFile(shadowFilePath, FileOpenFlags(FileFlags::READ_ONLY));

    if (!vfs->fileOrPathExists(context.getDatabasePath(), &context)) {
        throw RuntimeException(stringFormat(
            "Found shadow file {} but no corresponding database file. This file "
            "may have been left behind from a previous database with the same name. Deleting it "
            "permanently destroys the remaining checkpoint recovery evidence; preserve a copy "
            "for recovery analysis before taking any destructive action.",
            shadowFilePath));
    }
    auto storageManager = StorageManager::Get(context);
    try {
        storageManager->initDataFileHandle(vfs, &context);
    } catch (IOException& e) {
        throw RuntimeException(stringFormat(
            "Found shadow file {} but no corresponding database file. This file "
            "may have been left behind from a previous database with the same name. Deleting it "
            "permanently destroys the remaining checkpoint recovery evidence; preserve a copy "
            "for recovery analysis before taking any destructive action.",
            shadowFilePath));
    }
    auto dataFileHandle = storageManager->getDataFH();
    auto dataFileInfo = dataFileHandle->getFileInfo();

    ShadowFileHeader header;
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    shadowFileInfo->readFromFile(headerBuffer.get(), KUZU_PAGE_SIZE, 0);
    memcpy(&header, headerBuffer.get(), sizeof(ShadowFileHeader));

    // When replaying the shadow file we haven't read the database ID from the database
    // header yet
    // So we need to do it separately here to verify the shadow file matches the database
    auto oldDatabaseID = getOldDatabaseID(*dataFileInfo);
    FileDBIDUtils::verifyDatabaseID(*shadowFileInfo, oldDatabaseID, expectedDatabaseID);
    FileDBIDUtils::verifyDatabaseID(*shadowFileInfo, oldDatabaseID, header.databaseID);
    if (expectedCheckpointID.has_value()) {
        if (!expectedCheckpointStartDataFileNumPages.has_value() ||
            !expectedCheckpointEndDataFileNumPages.has_value()) {
            throw RuntimeException("Checkpoint WAL has incomplete shadow replay authorization.");
        }
        if (header.checkpointPageWatermarkMagic != CHECKPOINT_PAGE_WATERMARK_MAGIC) {
            throw RuntimeException(
                "Identified checkpoint WAL requires an identified shadow file.");
        }
        if ((header.checkpointStartDataFileNumPages ^
                header.checkpointStartDataFileNumPagesCheck) !=
                std::numeric_limits<page_idx_t>::max() ||
            header.checkpointStartDataFileNumPages == 0) {
            throw RuntimeException("Checkpoint shadow file has an invalid page watermark.");
        }
        if (expectedCheckpointID->value != header.checkpointID.value) {
            throw RuntimeException(
                "Checkpoint WAL and shadow file identify different checkpoints.");
        }
        if (*expectedCheckpointStartDataFileNumPages !=
            header.checkpointStartDataFileNumPages) {
            throw RuntimeException(
                "Checkpoint WAL and shadow file identify different page watermarks.");
        }
        const auto checkpointStartDataFileSize =
            static_cast<uint64_t>(header.checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE;
        if (dataFileInfo->getFileSize() < checkpointStartDataFileSize) {
            throw RuntimeException(
                "Checkpoint page watermark exceeds the database file size.");
        }
        const auto checkpointEndDataFileSize =
            static_cast<uint64_t>(*expectedCheckpointEndDataFileNumPages) * KUZU_PAGE_SIZE;
        if (dataFileInfo->getFileSize() > checkpointEndDataFileSize) {
            throw RuntimeException(
                "Checkpoint end watermark is below the database file size.");
        }
    } else {
        if (expectedCheckpointStartDataFileNumPages.has_value() ||
            expectedCheckpointEndDataFileNumPages.has_value()) {
            throw RuntimeException("Legacy checkpoint WAL has unexpected page bounds.");
        }
        if (header.checkpointPageWatermarkMagic != 0) {
            throw RuntimeException("Legacy checkpoint WAL requires a legacy shadow file.");
        }
    }

    const auto shadowFileSize = shadowFileInfo->getFileSize();
    const auto numShadowPages = static_cast<uint64_t>(header.numShadowPages);
    if (header.numShadowPages == INVALID_PAGE_IDX || shadowFileSize < KUZU_PAGE_SIZE) {
        throw RuntimeException(
            "Checkpoint shadow page count is inconsistent with the shadow file size.");
    }
    const auto completePagesInFile = shadowFileSize / KUZU_PAGE_SIZE;
    if (numShadowPages > completePagesInFile - 1) {
        throw RuntimeException(
            "Checkpoint shadow page count is inconsistent with the shadow file size.");
    }
    const auto shadowPageRecordsOffset = (numShadowPages + 1) * KUZU_PAGE_SIZE;
    const auto identifiedCheckpoint = expectedCheckpointID.has_value();
    const uint64_t serializedShadowPageRecordSize = sizeof(file_idx_t) + sizeof(page_idx_t) +
                                                    (identifiedCheckpoint ? sizeof(page_idx_t) : 0);
    const auto shadowPageRecordBytes = shadowFileSize - shadowPageRecordsOffset;
    if (shadowPageRecordBytes < sizeof(uint64_t) ||
        numShadowPages >
            (shadowPageRecordBytes - sizeof(uint64_t)) / serializedShadowPageRecordSize) {
        throw RuntimeException(
            "Checkpoint shadow page count is inconsistent with the shadow file size.");
    }

    const auto makeRecordDeserializer = [&]() {
        auto reader = std::make_unique<BufferedFileReader>(*shadowFileInfo);
        reader->resetReadOffset(shadowPageRecordsOffset);
        return Deserializer{std::move(reader)};
    };
    const auto readAndValidateRecordCount = [&](Deserializer& deSer) {
        uint64_t numShadowPageRecords = 0;
        deSer.deserializeValue(numShadowPageRecords);
        if (numShadowPageRecords != numShadowPages) {
            throw RuntimeException("Checkpoint shadow page and record counts do not match.");
        }
    };
    const auto expectedDataFileIdx = dataFileHandle->getFileIndex();
    const auto maxDataFilePages = context.getDBConfig()->maxDBSize / KUZU_PAGE_SIZE;
    const auto checkpointEndDataFileNumPages =
        expectedCheckpointEndDataFileNumPages.value_or(maxDataFilePages);
    const auto validateRecord = [&](const ShadowPageRecord& record) {
        if (record.originalFileIdx != expectedDataFileIdx) {
            throw RuntimeException("Checkpoint shadow record targets an invalid file.");
        }
        if (record.originalPageIdx == INVALID_PAGE_IDX ||
            static_cast<uint64_t>(record.originalPageIdx) >= checkpointEndDataFileNumPages) {
            throw RuntimeException("Checkpoint shadow record targets an invalid data-file page.");
        }
    };

    std::array<bool, MAX_SHADOW_REPLAY_BITMAPS> occupiedWindows{};
    std::array<page_idx_t, MAX_SHADOW_REPLAY_BITMAPS> windowMinTargets;
    std::array<page_idx_t, MAX_SHADOW_REPLAY_BITMAPS> windowMaxTargets{};
    windowMinTargets.fill(INVALID_PAGE_IDX);
    {
        auto deSer = makeRecordDeserializer();
        readAndValidateRecordCount(deSer);
        page_idx_t previousTarget = INVALID_PAGE_IDX;
        page_idx_t maxTarget = 0;
        for (uint64_t i = 0; i < numShadowPages; i++) {
            const auto record = ShadowPageRecord::deserialize(deSer, identifiedCheckpoint);
            validateRecord(record);
            if (identifiedCheckpoint && i > 0 && record.originalPageIdx <= previousTarget) {
                throw RuntimeException(
                    "Identified checkpoint shadow targets are not strictly ordered.");
            }
            previousTarget = record.originalPageIdx;
            uint64_t duplicateValue;
            if (identifiedCheckpoint) {
                if (record.shadowPageIdx == 0 || record.shadowPageIdx > numShadowPages) {
                    throw RuntimeException(
                        "Identified checkpoint shadow record targets an invalid shadow page.");
                }
                duplicateValue = record.shadowPageIdx - 1;
            } else {
                duplicateValue = record.originalPageIdx;
            }
            maxTarget = std::max(maxTarget, record.originalPageIdx);
            const auto window = duplicateValue / PAGES_PER_SHADOW_REPLAY_BITMAP;
            occupiedWindows[window] = true;
            windowMinTargets[window] =
                std::min<uint64_t>(windowMinTargets[window], duplicateValue);
            windowMaxTargets[window] =
                std::max<uint64_t>(windowMaxTargets[window], duplicateValue);
        }
        if (identifiedCheckpoint) {
            if (dataFileInfo->getFileSize() % KUZU_PAGE_SIZE != 0) {
                throw RuntimeException(
                    "Checkpoint database file has a partial trailing page.");
            }
            auto replayedDataFileNumPages = dataFileInfo->getFileSize() / KUZU_PAGE_SIZE;
            if (numShadowPages > 0) {
                replayedDataFileNumPages =
                    std::max(replayedDataFileNumPages, static_cast<uint64_t>(maxTarget) + 1);
            }
            if (replayedDataFileNumPages != *expectedCheckpointEndDataFileNumPages) {
                throw RuntimeException(
                    "Checkpoint shadow records do not restore the authenticated end watermark.");
            }
        }
    }

    const auto duplicateValueLimit = identifiedCheckpoint ? numShadowPages :
                                                            std::min(maxDataFilePages,
                                                                static_cast<uint64_t>(
                                                                    INVALID_PAGE_IDX));
    std::vector<uint64_t> seenTargets;
    for (uint64_t window = 0; window < MAX_SHADOW_REPLAY_BITMAPS; window++) {
        if (!occupiedWindows[window]) {
            continue;
        }
        const auto windowBoundaryStart = window * PAGES_PER_SHADOW_REPLAY_BITMAP;
        const auto windowBoundaryEnd = std::min(
            windowBoundaryStart + PAGES_PER_SHADOW_REPLAY_BITMAP, duplicateValueLimit);
        const auto targetStart = static_cast<uint64_t>(windowMinTargets[window]);
        const auto targetEnd = static_cast<uint64_t>(windowMaxTargets[window]) + 1;
        const auto bitmapWordCount =
            (targetEnd - targetStart + BITS_PER_BITMAP_WORD - 1) / BITS_PER_BITMAP_WORD;
        seenTargets.assign(bitmapWordCount, uint64_t{0});
        auto deSer = makeRecordDeserializer();
        readAndValidateRecordCount(deSer);
        for (uint64_t i = 0; i < numShadowPages; i++) {
            const auto record = ShadowPageRecord::deserialize(deSer, identifiedCheckpoint);
            validateRecord(record);
            const auto duplicateValue = identifiedCheckpoint ?
                                            static_cast<uint64_t>(record.shadowPageIdx - 1) :
                                            static_cast<uint64_t>(record.originalPageIdx);
            if (duplicateValue < windowBoundaryStart || duplicateValue >= windowBoundaryEnd) {
                continue;
            }
            if (duplicateValue < targetStart || duplicateValue >= targetEnd) {
                throw RuntimeException("Checkpoint shadow target partitioning failed.");
            }
            const auto pageOffset = duplicateValue - targetStart;
            const auto wordIdx = pageOffset / BITS_PER_BITMAP_WORD;
            const auto bit = uint64_t{1} << (pageOffset % BITS_PER_BITMAP_WORD);
            if (seenTargets[wordIdx] & bit) {
                throw RuntimeException(identifiedCheckpoint ?
                                           "Identified checkpoint shadow records contain duplicate "
                                           "shadow-page sources." :
                                           "Checkpoint shadow file contains duplicate data-file "
                                           "page targets.");
            }
            seenTargets[wordIdx] |= bit;
        }
    }

    const auto pageBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    auto deSer = makeRecordDeserializer();
    readAndValidateRecordCount(deSer);
    for (uint64_t i = 0; i < numShadowPages; i++) {
        const auto record = ShadowPageRecord::deserialize(deSer, identifiedCheckpoint);
        validateRecord(record);
        const auto shadowPageIdx = identifiedCheckpoint ? record.shadowPageIdx : i + 1;
        shadowFileInfo->readFromFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            shadowPageIdx * KUZU_PAGE_SIZE);
        dataFileInfo->writeFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            static_cast<uint64_t>(record.originalPageIdx) * KUZU_PAGE_SIZE);
    }
    dataFileInfo->syncFile();
}

void ShadowFile::rollbackCheckpoint(ClientContext& context,
    std::optional<ku_uuid_t> expectedDatabaseID,
    std::optional<ku_uuid_t> expectedCheckpointID,
    std::optional<page_idx_t> expectedCheckpointStartDataFileNumPages) {
    auto vfs = VirtualFileSystem::GetUnsafe(context);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(context.getDatabasePath());
    if (!vfs->fileOrPathExists(shadowFilePath, &context)) {
        if (!expectedCheckpointID.has_value()) {
            return;
        }
        if (!expectedDatabaseID.has_value() ||
            !expectedCheckpointStartDataFileNumPages.has_value()) {
            throw RuntimeException("Checkpoint WAL has incomplete rollback authorization.");
        }
        if (!vfs->fileOrPathExists(context.getDatabasePath(), &context)) {
            throw RuntimeException(
                "Found a WAL checkpoint begin without its shadow file or corresponding database "
                "file. Deleting the remaining WAL permanently destroys checkpoint recovery "
                "evidence; preserve a copy for recovery analysis before taking any destructive "
                "action.");
        }
        auto dataFileInfo = vfs->openFile(
            context.getDatabasePath(), FileOpenFlags(FileFlags::READ_ONLY), &context);
        const auto databaseHeader = DatabaseHeader::readDatabaseHeader(*dataFileInfo);
        if (!databaseHeader.has_value()) {
            throw RuntimeException(
                "WAL checkpoint begin has no shadow file and no valid database header.");
        }
        if (databaseHeader->databaseID.value != expectedDatabaseID->value) {
            throw RuntimeException(
                "WAL checkpoint begin identifies a different database.");
        }
        const auto checkpointStartFileSize = static_cast<uint64_t>(
            *expectedCheckpointStartDataFileNumPages) * KUZU_PAGE_SIZE;
        if (*expectedCheckpointStartDataFileNumPages == 0 ||
            dataFileInfo->getFileSize() != checkpointStartFileSize) {
            throw RuntimeException(
                "WAL checkpoint begin has no shadow file and rollback completion is ambiguous.");
        }
        return;
    }
    auto shadowFileInfo =
        vfs->openFile(shadowFilePath, FileOpenFlags(FileFlags::READ_ONLY), &context);
    if (shadowFileInfo->getFileSize() < KUZU_PAGE_SIZE) {
        if (expectedCheckpointID.has_value()) {
            throw RuntimeException("WAL checkpoint begin record has a truncated shadow file.");
        }
        return;
    }
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    shadowFileInfo->readFromFile(headerBuffer.get(), KUZU_PAGE_SIZE, 0);
    ShadowFileHeader header;
    memcpy(&header, headerBuffer.get(), sizeof(ShadowFileHeader));
    if (header.checkpointPageWatermarkMagic == 0) {
        if (expectedCheckpointID.has_value()) {
            throw RuntimeException(
                "WAL checkpoint begin record has no matching identified shadow file.");
        }
        return;
    }
    if (header.checkpointPageWatermarkMagic != CHECKPOINT_PAGE_WATERMARK_MAGIC) {
        throw RuntimeException("Checkpoint shadow file has an invalid identity marker.");
    }
    if ((header.checkpointStartDataFileNumPages ^
            header.checkpointStartDataFileNumPagesCheck) !=
        std::numeric_limits<page_idx_t>::max()) {
        throw RuntimeException("Checkpoint shadow file has an invalid page watermark.");
    }
    if (!vfs->fileOrPathExists(context.getDatabasePath(), &context)) {
        throw RuntimeException(
            "Found an identified checkpoint shadow file but no corresponding database file. "
            "Deleting it permanently destroys the remaining checkpoint recovery evidence; "
            "preserve a copy for recovery analysis before taking any destructive action.");
    }

    std::unique_ptr<FileInfo> dataFileInfo;
    if (context.getDBConfig()->readOnly) {
        dataFileInfo = vfs->openFile(
            context.getDatabasePath(), FileOpenFlags(FileFlags::READ_ONLY), &context);
    } else {
        dataFileInfo = vfs->openFile(context.getDatabasePath(),
            FileOpenFlags{FileFlags::WRITE | FileFlags::READ_ONLY, FileLockType::WRITE_LOCK},
            &context);
    }
    const auto databaseHeader = DatabaseHeader::readDatabaseHeader(*dataFileInfo);
    if (!databaseHeader.has_value()) {
        throw RuntimeException(
            "Found an identified checkpoint shadow file but no valid database header.");
    }
    if (databaseHeader->databaseID.value != header.databaseID.value ||
        (expectedDatabaseID.has_value() &&
            expectedDatabaseID->value != header.databaseID.value)) {
        throw RuntimeException(
            "Checkpoint shadow file identifies a different database.");
    }
    if (!expectedCheckpointID.has_value()) {
        if (expectedCheckpointStartDataFileNumPages.has_value()) {
            throw RuntimeException("Checkpoint WAL has incomplete rollback authorization.");
        }
        const auto checkpointStartFileSize = static_cast<uint64_t>(
            header.checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE;
        if (header.numShadowPages == 0 &&
            header.checkpointStartDataFileNumPages != 0 &&
            shadowFileInfo->getFileSize() == KUZU_PAGE_SIZE &&
            dataFileInfo->getFileSize() == checkpointStartFileSize) {
            return;
        }
        throw RuntimeException(
            "Found an unfinished checkpoint shadow file with no matching WAL checkpoint record.");
    }
    if (expectedCheckpointID->value != header.checkpointID.value) {
        throw RuntimeException(
            "Checkpoint WAL and shadow file identify different checkpoints.");
    }
    if (!expectedCheckpointStartDataFileNumPages.has_value() ||
        *expectedCheckpointStartDataFileNumPages != header.checkpointStartDataFileNumPages) {
        throw RuntimeException(
            "Checkpoint WAL and shadow file identify different page watermarks.");
    }
    if (header.checkpointStartDataFileNumPages == 0) {
        throw RuntimeException("Checkpoint shadow file has an invalid page watermark.");
    }
    const auto checkpointStartFileSize = static_cast<uint64_t>(
        header.checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE;
    if (dataFileInfo->getFileSize() < checkpointStartFileSize) {
        throw RuntimeException(
            "Checkpoint page watermark exceeds the database file size.");
    }
    if (dataFileInfo->getFileSize() == checkpointStartFileSize) {
        return;
    }
    if (context.getDBConfig()->readOnly) {
        return;
    }
    dataFileInfo->truncate(checkpointStartFileSize);
    dataFileInfo->syncFile();
}

ku_uuid_t ShadowFile::beginCheckpoint(ClientContext& context, page_idx_t dataFileNumPages) {
    if (checkpointStartDataFileNumPages == INVALID_PAGE_IDX) {
        checkpointStartDataFileNumPages = dataFileNumPages;
    }
    if (!checkpointID.has_value()) {
        checkpointID = UUID::generateRandomUUID(RandomEngine::Get(context));
    }
    getOrCreateShadowingFH();
    writeHeader(context);
    shadowingFH->getFileInfo()->syncFile();
#if !defined(__WASM__)
    vfs->syncFileCreation(*shadowingFH->getFileInfo());
#endif
    return *checkpointID;
}

void ShadowFile::writeHeader(ClientContext& context) const {
    ShadowFileHeader header;
    header.numShadowPages = shadowPageRecords.size();
    header.databaseID = StorageManager::Get(context)->getOrInitDatabaseID(context);
    header.checkpointPageWatermarkMagic = CHECKPOINT_PAGE_WATERMARK_MAGIC;
    KU_ASSERT(checkpointID.has_value());
    header.checkpointID = *checkpointID;
    header.checkpointStartDataFileNumPages = checkpointStartDataFileNumPages;
    header.checkpointStartDataFileNumPagesCheck = ~checkpointStartDataFileNumPages;
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    memcpy(headerBuffer.get(), &header, sizeof(ShadowFileHeader));
    KU_ASSERT(shadowingFH && !shadowingFH->isInMemoryMode());
    shadowingFH->writePageToFile(headerBuffer.get(), 0);
}

void ShadowFile::flushAll(main::ClientContext& context) {
    shadowingFH->flushAllDirtyPagesInFrames();
    std::sort(shadowPageRecords.begin(), shadowPageRecords.end(),
        [](const ShadowPageRecord& left, const ShadowPageRecord& right) {
            return std::tie(left.originalFileIdx, left.originalPageIdx) <
                   std::tie(right.originalFileIdx, right.originalPageIdx);
        });
    writeHeader(context);
    // Append shadow page records to the end of the file.
    const auto writer = std::make_shared<BufferedFileWriter>(*shadowingFH->getFileInfo());
    writer->setFileOffset(shadowingFH->getNumPages() * KUZU_PAGE_SIZE);
    Serializer ser(writer);
    KU_ASSERT(shadowPageRecords.size() + 1 == shadowingFH->getNumPages());
    ser.serializeVector(shadowPageRecords);
    writer->flush();
    // Sync the file to disk.
    writer->sync();
#if !defined(__WASM__)
    vfs->syncFileCreation(*shadowingFH->getFileInfo());
#endif
}

void ShadowFile::clear(BufferManager& bm) {
    KU_ASSERT(shadowingFH);
    bm.removeFilePagesFromFrames(*shadowingFH);
    shadowPagesMap.clear();
    shadowPageRecords.clear();
    checkpointStartDataFileNumPages = INVALID_PAGE_IDX;
    checkpointID.reset();
}

void ShadowFile::reset() {
    shadowingFH->resetFileInfo();
    shadowingFH = nullptr;
#if defined(__WASM__)
    vfs->removeFileIfExists(shadowFilePath);
#else
    vfs->removeFileIfExistsDurably(shadowFilePath);
#endif
}

FileHandle* ShadowFile::getOrCreateShadowingFH() {
    if (!shadowingFH) {
        shadowingFH = bm.getFileHandle(shadowFilePath,
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs, nullptr);
        if (shadowingFH->getNumPages() == 0) {
            // Reserve the first page for the header.
            shadowingFH->addNewPage();
        }
    }
    return shadowingFH;
}

} // namespace storage
} // namespace kuzu
