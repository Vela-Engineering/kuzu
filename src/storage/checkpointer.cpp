#include "storage/checkpointer.h"

#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "common/file_system/file_system.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/in_mem_file_writer.h"
#include "extension/extension_manager.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/database_header.h"
#include "storage/shadow_utils.h"
#include "storage/page_manager.h"
#include "storage/storage_manager.h"
#include "storage/wal/local_wal.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace storage {

Checkpointer::Checkpointer(main::ClientContext& clientContext)
    : clientContext{clientContext},
      isInMemory{main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())} {}

Checkpointer::~Checkpointer() = default;

PageRange Checkpointer::serializeCatalog(const catalog::Catalog& catalog,
    StorageManager& storageManager) {
    auto catalogWriter =
        std::make_shared<common::InMemFileWriter>(*MemoryManager::Get(clientContext));
    common::Serializer catalogSerializer(catalogWriter);
    catalog.serialize(catalogSerializer);
    auto pageAllocator = storageManager.getDataFH()->getPageManager();
    return catalogWriter->flush(*pageAllocator, storageManager.getShadowFile());
}

PageRange Checkpointer::serializeCatalogSnapshot(const catalog::Catalog& catalog,
    StorageManager& storageManager) {
    auto catalogWriter =
        std::make_shared<common::InMemFileWriter>(*MemoryManager::Get(clientContext));
    common::Serializer catalogSerializer(catalogWriter);
    catalog.serializeSnapshot(catalogSerializer, snapshotTS);
    auto pageAllocator = storageManager.getDataFH()->getPageManager();
    return catalogWriter->flush(*pageAllocator, storageManager.getShadowFile());
}

PageRange Checkpointer::serializeMetadata(const catalog::Catalog& catalog,
    StorageManager& storageManager, const std::vector<PageRange>& livePageRanges) {
    auto metadataWriter =
        std::make_shared<common::InMemFileWriter>(*MemoryManager::Get(clientContext));
    common::Serializer metadataSerializer(metadataWriter);
    storageManager.serialize(catalog, metadataSerializer);

    // We need to preallocate the pages for the page manager before we actually serialize it,
    // this is because the page manager needs to track the pages used for itself.
    // The number of pages needed for the page manager should only decrease after making an
    // additional allocation, so we just calculate the number of pages needed to serialize the
    // current state of the page manager.
    // Thus, it is possible that we allocate an extra page that we won't end up writing to when we
    // flush the metadata writer. This may cause a discrepancy between the number of tracked pages
    // and the number of physical pages in the file but shouldn't cause any actual incorrect
    // behavior in the database.
    auto& pageManager = *storageManager.getDataFH()->getPageManager();
    const auto pagesForPageManager = pageManager.estimatePagesNeededForSerialize();
    auto pageAllocator = storageManager.getDataFH()->getPageManager();
    const auto allocatedPages = pageAllocator->allocatePageRange(
        metadataWriter->getNumPagesToFlush() + pagesForPageManager);
    for (const auto& livePageRange : livePageRanges) {
        pageManager.removeFreePageRange(livePageRange);
    }
    pageManager.removeFreePageRange(allocatedPages);
    pageManager.serialize(metadataSerializer);

    metadataWriter->flush(allocatedPages, pageAllocator->getDataFH(),
        storageManager.getShadowFile());
    return allocatedPages;
}

PageRange Checkpointer::serializeMetadataSnapshot(const catalog::Catalog& catalog,
    StorageManager& storageManager, const std::vector<PageRange>& livePageRanges) {
    auto metadataWriter =
        std::make_shared<common::InMemFileWriter>(*MemoryManager::Get(clientContext));
    common::Serializer metadataSerializer(metadataWriter);
    const transaction::Transaction snapshotTxn(transaction::TransactionType::CHECKPOINT,
        transaction::Transaction::DUMMY_TRANSACTION_ID, snapshotTS);
    storageManager.serialize(catalog, snapshotTxn, metadataSerializer);

    auto& pageManager = *storageManager.getDataFH()->getPageManager();
    const auto pagesForPageManager = pageManager.estimatePagesNeededForSerialize();
    auto pageAllocator = storageManager.getDataFH()->getPageManager();
    const auto allocatedPages = pageAllocator->allocatePageRange(
        metadataWriter->getNumPagesToFlush() + pagesForPageManager);
    for (const auto& livePageRange : livePageRanges) {
        pageManager.removeFreePageRange(livePageRange);
    }
    pageManager.removeFreePageRange(allocatedPages);
    pageManager.serialize(metadataSerializer);

    metadataWriter->flush(allocatedPages, pageAllocator->getDataFH(),
        storageManager.getShadowFile());
    return allocatedPages;
}

void Checkpointer::writeCheckpoint() {
    if (isInMemory) {
        return;
    }

    auto storageManager = StorageManager::Get(clientContext);
    walRotated = storageManager->getWAL().rotateForCheckpoint(&clientContext);

    auto databaseHeader = *storageManager->getOrInitDatabaseHeader(clientContext);
    auto& shadowFile = storageManager->getShadowFile();
    checkpointID = shadowFile.beginCheckpoint(
        clientContext, storageManager->getDataFH()->getNumPages());
    checkpointBeginWriteStarted = true;
    storageManager->getWAL().logAndFlushCheckpointStart(&clientContext, checkpointID,
        shadowFile.getCheckpointStartDataFileNumPages(), walRotated);
    bool hasStorageChanges = checkpointStorage();
    serializeCatalogAndMetadata(databaseHeader, hasStorageChanges);
    writeDatabaseHeader(databaseHeader);
    logCheckpointAndApplyShadowPages(walRotated);

    // Snapshot versions before postCheckpointCleanup resets change tracking.
    catalogVersionAtCheckpoint = catalog::Catalog::Get(clientContext)->getVersion();
    pageManagerVersionAtCheckpoint = storageManager->getDataFH()->getPageManager()->getVersion();
}

void Checkpointer::writeRecoveryCheckpoint() {
    if (isInMemory) {
        return;
    }

    auto storageManager = StorageManager::Get(clientContext);
    if (storageManager->isReadOnly()) {
        return;
    }

    auto databaseHeader = *storageManager->getOrInitDatabaseHeader(clientContext);
    auto& shadowFile = storageManager->getShadowFile();
    checkpointID = shadowFile.beginCheckpoint(
        clientContext, storageManager->getDataFH()->getNumPages());
    checkpointBeginWriteStarted = true;
    storageManager->getWAL().logAndFlushCheckpointStart(&clientContext, checkpointID,
        shadowFile.getCheckpointStartDataFileNumPages(), false);
    const auto hasStorageChanges = checkpointStorage();
    serializeCatalogAndMetadata(databaseHeader, hasStorageChanges);
    writeDatabaseHeader(databaseHeader);

    shadowFile.flushAll(clientContext);
    auto& wal = storageManager->getWAL();
    markCheckpointMarkerWriteStarted();
    wal.logAndFlushCheckpoint(&clientContext, checkpointID);
    markCheckpointMarkerWritten();
    markShadowApplicationStarted();
    shadowFile.applyShadowPages(clientContext);

    catalogVersionAtCheckpoint = catalog::Catalog::Get(clientContext)->getVersion();
    pageManagerVersionAtCheckpoint = storageManager->getDataFH()->getPageManager()->getVersion();
    storageManager->finalizeCheckpoint();
    auto bufferManager = MemoryManager::Get(clientContext)->getBufferManager();
    bufferManager->removeEvictedCandidates();
    catalog::Catalog::Get(clientContext)->resetVersion(catalogVersionAtCheckpoint);
    storageManager->getDataFH()->getPageManager()->resetVersion(pageManagerVersionAtCheckpoint);

    shadowFile.clear(*bufferManager);
    shadowFile.reset();
    wal.clearFrozenWAL();
    wal.reset();
}

void Checkpointer::beginCheckpoint(common::transaction_t snapshotTimestamp) {
    if (isInMemory) {
        return;
    }

    snapshotTS = snapshotTimestamp;
    shadowApplicationStarted = false;
    checkpointBeginWriteStarted = false;
    checkpointMarkerWriteStarted = false;
    checkpointMarkerWritten = false;

    auto storageManager = StorageManager::Get(clientContext);
    walRotationStarted = true;
    walRotated = storageManager->getWAL().rotateForCheckpoint(&clientContext);

    checkpointHeader = *storageManager->getOrInitDatabaseHeader(clientContext);
    auto& shadowFile = storageManager->getShadowFile();
    checkpointID = shadowFile.beginCheckpoint(
        clientContext, storageManager->getDataFH()->getNumPages());
    checkpointBeginWriteStarted = true;
    storageManager->getWAL().logAndFlushCheckpointStart(&clientContext, checkpointID,
        shadowFile.getCheckpointStartDataFileNumPages(), walRotated);

    // Capture versions before any transaction can start after WAL rotation.
    catalogVersionAtCheckpoint = catalog::Catalog::Get(clientContext)->getVersion();
    pageManagerVersionAtCheckpoint = storageManager->getDataFH()->getPageManager()->getVersion();
    tableEpochWatermarks = storageManager->captureChangeEpochs();
}

void Checkpointer::checkpointStoragePhase() {
    if (isInMemory) {
        return;
    }
    hasStorageChanges = checkpointStorage();
}

void Checkpointer::finishCheckpoint() {
    if (isInMemory) {
        return;
    }
    serializeCatalogAndMetadata(checkpointHeader, hasStorageChanges);
    writeDatabaseHeader(checkpointHeader);
    logCheckpointAndApplyShadowPages(walRotated);
}

void Checkpointer::postCheckpointCleanup() {
    if (isInMemory) {
        return;
    }

    auto storageManager = StorageManager::Get(clientContext);
    storageManager->finalizeCheckpoint();
    auto bufferManager = MemoryManager::Get(clientContext)->getBufferManager();
    bufferManager->removeEvictedCandidates();

    catalog::Catalog::Get(clientContext)->resetVersion(catalogVersionAtCheckpoint);
    auto* dataFH = storageManager->getDataFH();
    dataFH->getPageManager()->resetVersion(pageManagerVersionAtCheckpoint);
    auto& shadowFile = storageManager->getShadowFile();
    shadowFile.clear(*bufferManager);
    shadowFile.reset();
    if (walRotated) {
        storageManager->getWAL().clearFrozenWAL();
    } else {
        storageManager->getWAL().reset();
    }
}

void Checkpointer::retryShadowApplication() {
    StorageManager::Get(clientContext)->getShadowFile().applyShadowPages(clientContext);
}

bool Checkpointer::checkpointStorage() {
    const auto storageManager = StorageManager::Get(clientContext);
    auto pageAllocator = storageManager->getDataFH()->getPageManager();
    if (snapshotTS > 0) {
        const transaction::Transaction snapshotTxn(transaction::TransactionType::CHECKPOINT,
            transaction::Transaction::DUMMY_TRANSACTION_ID, snapshotTS);
        return storageManager->checkpoint(&clientContext, snapshotTxn, *pageAllocator,
            tableEpochWatermarks);
    }
    return storageManager->checkpoint(&clientContext, *pageAllocator);
}

static PageRange getDatabaseHeaderPageRange() {
    return {common::StorageConstants::DB_HEADER_PAGE_IDX, 1};
}

static bool isValidPageRange(const PageRange& pageRange) {
    return pageRange.startPageIdx != common::INVALID_PAGE_IDX && pageRange.numPages > 0;
}

static bool removeFreePageRangeIfValid(PageManager& pageManager, PageRange pageRange) {
    return isValidPageRange(pageRange) && pageManager.removeFreePageRange(pageRange);
}

static void freePageRangeIfValid(PageManager& pageManager, PageRange pageRange,
    const std::vector<PageRange>& rangesToExclude) {
    if (isValidPageRange(pageRange)) {
        pageManager.freePageRange(pageRange, rangesToExclude);
    }
}

void Checkpointer::serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
    bool storageChanges) {
    const auto storageManager = StorageManager::Get(clientContext);
    const auto catalog = catalog::Catalog::Get(clientContext);
    auto* dataFH = storageManager->getDataFH();
    auto& pageManager = *dataFH->getPageManager();
    const bool useSnapshot = snapshotTS > 0;
    const auto oldCatalogPageRange = databaseHeader.catalogPageRange;
    const auto oldMetadataPageRange = databaseHeader.metadataPageRange;
    const auto databaseHeaderPageRange = getDatabaseHeaderPageRange();
    const bool catalogChanged = catalog->changedSinceLastCheckpoint();
    const bool pageManagerChanged = pageManager.changedSinceLastCheckpoint();
    bool repairedHeaderFreePages = pageManager.removeFreePageRange(databaseHeaderPageRange);
    repairedHeaderFreePages =
        removeFreePageRangeIfValid(pageManager, oldCatalogPageRange) || repairedHeaderFreePages;
    repairedHeaderFreePages =
        removeFreePageRangeIfValid(pageManager, oldMetadataPageRange) || repairedHeaderFreePages;

    if (oldCatalogPageRange.startPageIdx == common::INVALID_PAGE_IDX || catalogChanged) {
        databaseHeader.catalogPageRange =
            useSnapshot ? serializeCatalogSnapshot(*catalog, *storageManager) :
                          serializeCatalog(*catalog, *storageManager);
        freePageRangeIfValid(pageManager, oldCatalogPageRange,
            {databaseHeaderPageRange, databaseHeader.catalogPageRange, oldMetadataPageRange});
    }
    if (oldMetadataPageRange.startPageIdx == common::INVALID_PAGE_IDX || storageChanges ||
        catalogChanged || pageManagerChanged || repairedHeaderFreePages) {
        std::vector<PageRange> livePageRanges = {
            databaseHeaderPageRange, databaseHeader.catalogPageRange};
        freePageRangeIfValid(pageManager, oldMetadataPageRange, livePageRanges);
        databaseHeader.metadataPageRange =
            useSnapshot ? serializeMetadataSnapshot(*catalog, *storageManager, livePageRanges) :
                          serializeMetadata(*catalog, *storageManager, livePageRanges);
    }
}

void Checkpointer::writeDatabaseHeader(const DatabaseHeader& header) {
    auto headerWriter =
        std::make_shared<common::InMemFileWriter>(*MemoryManager::Get(clientContext));
    common::Serializer headerSerializer(headerWriter);
    header.serialize(headerSerializer);
    auto headerPage = headerWriter->getPage(0);

    const auto storageManager = StorageManager::Get(clientContext);
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto shadowHeader = ShadowUtils::createShadowVersionIfNecessaryAndPinPage(
        common::StorageConstants::DB_HEADER_PAGE_IDX, true /* skipReadingOriginalPage */, *dataFH,
        shadowFile);
    memcpy(shadowHeader.frame, headerPage.data(), common::KUZU_PAGE_SIZE);
    shadowFile.getShadowingFH().unpinPage(shadowHeader.shadowPage);

    // Update the in-memory database header with the new version
    StorageManager::Get(clientContext)->setDatabaseHeader(std::make_unique<DatabaseHeader>(header));
}

void Checkpointer::logCheckpointAndApplyShadowPages(bool walRotated) {
    const auto storageManager = StorageManager::Get(clientContext);
    auto& shadowFile = storageManager->getShadowFile();
    shadowFile.flushAll(clientContext);
    auto wal = WAL::Get(clientContext);
    markCheckpointMarkerWriteStarted();
    if (walRotated) {
        wal->logAndFlushCheckpointToFrozen(&clientContext, checkpointID);
    } else {
        wal->logAndFlushCheckpoint(&clientContext, checkpointID);
    }
    markCheckpointMarkerWritten();
    markShadowApplicationStarted();
    shadowFile.applyShadowPages(clientContext);
}

void Checkpointer::rollback() {
    if (isInMemory) {
        return;
    }
    const auto storageManager = StorageManager::Get(clientContext);
    auto catalog = catalog::Catalog::Get(clientContext);
    // Any pages freed during the checkpoint are no longer freed
    storageManager->rollbackCheckpoint(*catalog);
}

bool Checkpointer::canAutoCheckpoint(const main::ClientContext& clientContext,
    const transaction::Transaction& transaction) {
    if (clientContext.isInMemory()) {
        return false;
    }
    if (!clientContext.getDBConfig()->autoCheckpoint) {
        return false;
    }
    if (transaction.isRecovery()) {
        // Recovery transactions are not allowed to trigger auto checkpoint.
        return false;
    }
    auto wal = WAL::Get(clientContext);
    if (wal->hasFrozenWAL()) {
        return false;
    }
    const auto expectedSize = transaction.getLocalWAL().getSize() + wal->getFileSize();
    return expectedSize > clientContext.getDBConfig()->checkpointThreshold;
}

void Checkpointer::readCheckpoint() {
    auto storageManager = StorageManager::Get(clientContext);
    if (!storageManager->getDataFH()) {
        storageManager->initDataFileHandle(common::VirtualFileSystem::GetUnsafe(clientContext),
            &clientContext);
    }
    if (!isInMemory && storageManager->getDataFH()->getNumPages() > 0) {
        readCheckpoint(&clientContext, catalog::Catalog::Get(clientContext), storageManager);
    }
    extension::ExtensionManager::Get(clientContext)->autoLoadLinkedExtensions(&clientContext);
}

void Checkpointer::readCheckpoint(main::ClientContext* context, catalog::Catalog* catalog,
    StorageManager* storageManager) {
    auto fileInfo = storageManager->getDataFH()->getFileInfo();
    auto reader = std::make_unique<common::BufferedFileReader>(*fileInfo);
    common::Deserializer deSer(std::move(reader));
    auto currentHeader = std::make_unique<DatabaseHeader>(DatabaseHeader::deserialize(deSer));
    // If the catalog page range is invalid, it means there is no catalog to read; thus, the
    // database is empty.
    if (currentHeader->catalogPageRange.startPageIdx != common::INVALID_PAGE_IDX) {
        deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
            currentHeader->catalogPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
        catalog->deserialize(deSer);
        deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
            currentHeader->metadataPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
        storageManager->deserialize(context, catalog, deSer);
        storageManager->getDataFH()->getPageManager()->deserialize(deSer,
            {getDatabaseHeaderPageRange(), currentHeader->catalogPageRange,
                currentHeader->metadataPageRange});
    }
    storageManager->setDatabaseHeader(std::move(currentHeader));
}

} // namespace storage
} // namespace kuzu
