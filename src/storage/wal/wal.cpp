#include "storage/wal/wal.h"

#include "common/exception/runtime.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/in_mem_file_writer.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/db_config.h"
#include "storage/file_db_id_utils.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/checksum_writer.h"
#include "storage/wal/local_wal.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& dbPath, bool readOnly, bool enableChecksums, VirtualFileSystem* vfs)
    : walPath{StorageUtils::getWALFilePath(dbPath)},
      checkpointWalPath{StorageUtils::getCheckpointWALFilePath(dbPath)},
      inMemory{main::DBConfig::isDBPathInMemory(dbPath)}, readOnly{readOnly}, vfs{vfs},
      enableChecksums(enableChecksums), syncFileCreationOnNextFlush{false} {}

WAL::~WAL() {}

void WAL::logCommittedWAL(LocalWAL& localWAL, main::ClientContext* context) {
    KU_ASSERT(!readOnly);
    if (inMemory || localWAL.getSize() == 0) {
        return; // No need to log empty WAL.
    }
    std::unique_lock lck{mtx};
    initWriter(context);
    localWAL.inMemWriter->flush(*serializer->getWriter());
    flushAndSyncNoLock(true);
}

void WAL::logAndFlushCheckpointStart(
    main::ClientContext* context, ku_uuid_t checkpointID,
    page_idx_t checkpointStartDataFileNumPages, bool frozenWAL) {
    CheckpointBeginRecord walRecord{checkpointID, checkpointStartDataFileNumPages};
    if (frozenWAL) {
        auto frozenFileInfo = vfs->openFile(checkpointWalPath,
            FileOpenFlags(FileFlags::READ_ONLY | FileFlags::WRITE), context);
        std::shared_ptr<Writer> writer = std::make_shared<BufferedFileWriter>(*frozenFileInfo);
        auto& bufferedWriter = writer->cast<BufferedFileWriter>();
        if (enableChecksums) {
            writer = std::make_shared<ChecksumWriter>(
                std::move(writer), *MemoryManager::Get(*context));
        }
        Serializer frozenSerializer{std::move(writer)};
        bufferedWriter.setFileOffset(frozenFileInfo->getFileSize());
        frozenSerializer.getWriter()->onObjectBegin();
        walRecord.serialize(frozenSerializer);
        frozenSerializer.getWriter()->onObjectEnd();
        frozenSerializer.getWriter()->flush();
        frozenSerializer.getWriter()->sync();
        return;
    }
    std::unique_lock lck{mtx};
    initWriter(context);
    addNewWALRecordNoLock(walRecord);
    flushAndSyncNoLock();
}

void WAL::logAndFlushCheckpoint(main::ClientContext* context, ku_uuid_t checkpointID) {
    std::unique_lock lck{mtx};
    initWriter(context);
    CheckpointRecordV2 walRecord{
        checkpointID, StorageManager::Get(*context)->getDataFH()->getNumPages()};
    addNewWALRecordNoLock(walRecord);
    flushAndSyncNoLock();
}

bool WAL::rotateForCheckpoint(main::ClientContext* /*context*/) {
    std::unique_lock lck{mtx};
    if (inMemory) {
        return false;
    }
    if (vfs->fileOrPathExists(checkpointWalPath)) {
        throw RuntimeException(
            "Cannot start checkpoint while a checkpoint WAL is pending recovery. Restart the "
            "database before retrying checkpoint.");
    }
    if (!serializer && !vfs->fileOrPathExists(walPath)) {
        return false;
    }
    if (serializer) {
        flushAndSyncNoLock();
        fileInfo.reset();
        serializer.reset();
    }
#if defined(__WASM__)
    vfs->renameFile(walPath, checkpointWalPath);
#else
    vfs->renameFileDurably(walPath, checkpointWalPath);
#endif
    if (checkpointRotationHookForTesting) {
        checkpointRotationHookForTesting();
    }
    return true;
}

void WAL::logAndFlushCheckpointToFrozen(
    main::ClientContext* context, ku_uuid_t checkpointID) {
    auto frozenFileInfo = vfs->openFile(checkpointWalPath,
        FileOpenFlags(FileFlags::READ_ONLY | FileFlags::WRITE), context);

    std::shared_ptr<Writer> writer = std::make_shared<BufferedFileWriter>(*frozenFileInfo);
    auto& bufferedWriter = writer->cast<BufferedFileWriter>();
    if (enableChecksums) {
        writer = std::make_shared<ChecksumWriter>(std::move(writer), *MemoryManager::Get(*context));
    }
    auto frozenSerializer = std::make_unique<Serializer>(std::move(writer));
    bufferedWriter.setFileOffset(frozenFileInfo->getFileSize());

    CheckpointRecordV2 walRecord{
        checkpointID, StorageManager::Get(*context)->getDataFH()->getNumPages()};
    frozenSerializer->getWriter()->onObjectBegin();
    walRecord.serialize(*frozenSerializer);
    frozenSerializer->getWriter()->onObjectEnd();
    frozenSerializer->getWriter()->flush();
    frozenSerializer->getWriter()->sync();
}

void WAL::clearFrozenWAL() {
#if defined(__WASM__)
    vfs->removeFileIfExists(checkpointWalPath);
#else
    if (!inMemory) {
        vfs->removeFileIfExistsDurably(checkpointWalPath);
    }
#endif
}

bool WAL::hasFrozenWAL() const {
    return !inMemory && vfs->fileOrPathExists(checkpointWalPath);
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::clear() {
    std::unique_lock lck{mtx};
    serializer->getWriter()->clear();
}

void WAL::reset() {
    std::unique_lock lck{mtx};
    fileInfo.reset();
    serializer.reset();
#if defined(__WASM__)
    vfs->removeFileIfExists(walPath);
#else
    if (!inMemory) {
        vfs->removeFileIfExistsDurably(walPath);
    }
#endif
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::flushAndSyncNoLock(bool isCommit) {
    serializer->getWriter()->flush();
    if (isCommit && commitSyncHookForTesting) {
        commitSyncHookForTesting();
    }
    serializer->getWriter()->sync();
    if (syncFileCreationOnNextFlush) {
#if !defined(__WASM__)
        vfs->syncFileCreation(*fileInfo);
#endif
        syncFileCreationOnNextFlush = false;
    }
}

void WAL::setCommitSyncHookForTesting(std::function<void()> hook) {
    std::unique_lock lck{mtx};
    commitSyncHookForTesting = std::move(hook);
}

void WAL::setCheckpointRotationHookForTesting(std::function<void()> hook) {
    std::unique_lock lck{mtx};
    checkpointRotationHookForTesting = std::move(hook);
}

uint64_t WAL::getFileSize() {
    std::unique_lock lck{mtx};
    if (!serializer) {
        return 0;
    }
    return serializer->getWriter()->getSize();
}

void WAL::writeHeader(main::ClientContext& context) {
    serializer->getWriter()->onObjectBegin();
    FileDBIDUtils::writeDatabaseID(*serializer,
        StorageManager::Get(context)->getOrInitDatabaseID(context));
    serializer->write(enableChecksums);
    serializer->getWriter()->onObjectEnd();
}

void WAL::initWriter(main::ClientContext* context) {
    if (serializer) {
        return;
    }
    fileInfo = vfs->openFile(walPath,
        FileOpenFlags(FileFlags::CREATE_IF_NOT_EXISTS | FileFlags::READ_ONLY | FileFlags::WRITE),
        context);

    std::shared_ptr<Writer> writer = std::make_shared<BufferedFileWriter>(*fileInfo);
    auto& bufferedWriter = writer->cast<BufferedFileWriter>();
    if (enableChecksums) {
        writer = std::make_shared<ChecksumWriter>(std::move(writer), *MemoryManager::Get(*context));
    }
    serializer = std::make_unique<Serializer>(std::move(writer));

    // Write the databaseID at the start of the WAL if needed
    // This is used to ensure that when replaying the WAL matches the database
    if (fileInfo->getFileSize() == 0) {
        writeHeader(*context);
        syncFileCreationOnNextFlush = true;
    }

    // WAL should always be APPEND only. We don't want to overwrite the file as it may still
    // contain records not replayed. This can happen if checkpoint is not triggered before the
    // Database is closed last time.
    bufferedWriter.setFileOffset(fileInfo->getFileSize());
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::addNewWALRecordNoLock(const WALRecord& walRecord) {
    KU_ASSERT(walRecord.type != WALRecordType::INVALID_RECORD);
    KU_ASSERT(!inMemory);
    KU_ASSERT(serializer != nullptr);
    serializer->getWriter()->onObjectBegin();
    walRecord.serialize(*serializer);
    serializer->getWriter()->onObjectEnd();
}

WAL* WAL::Get(const main::ClientContext& context) {
    KU_ASSERT(context.getDatabase() && context.getDatabase()->getStorageManager());
    return &context.getDatabase()->getStorageManager()->getWAL();
}

} // namespace storage
} // namespace kuzu
