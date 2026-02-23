#include "storage/checkpointer.h"

#include "catalog/catalog.h"
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
#include "storage/storage_manager.h"
#include "storage/wal/local_wal.h"
#include "transaction/transaction.h"

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
    StorageManager& storageManager) {
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
    pageManager.serialize(metadataSerializer);

    metadataWriter->flush(allocatedPages, pageAllocator->getDataFH(),
        storageManager.getShadowFile());
    return allocatedPages;
}

PageRange Checkpointer::serializeMetadataSnapshot(const catalog::Catalog& catalog,
    StorageManager& storageManager) {
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

    auto databaseHeader =
        *storageManager->getOrInitDatabaseHeader(clientContext);
    bool hasStorageChanges = checkpointStorage();
    serializeCatalogAndMetadata(databaseHeader, hasStorageChanges);
    writeDatabaseHeader(databaseHeader);
    logCheckpointAndApplyShadowPages(walRotated);

    // Snapshot versions while the write gate is still held, before postCheckpointCleanup
    // runs with the gate released. This prevents capturing version bumps from new writers.
    catalogVersionAtCheckpoint = catalog::Catalog::Get(clientContext)->getVersion();
    pageManagerVersionAtCheckpoint = storageManager->getDataFH()->getPageManager()->getVersion();
}

void Checkpointer::beginCheckpoint(common::transaction_t snapshotTimestamp) {
    if (isInMemory) {
        return;
    }

    snapshotTS = snapshotTimestamp;

    auto storageManager = StorageManager::Get(clientContext);
    walRotated = storageManager->getWAL().rotateForCheckpoint(&clientContext);

    checkpointHeader = *storageManager->getOrInitDatabaseHeader(clientContext);

    // Capture versions while the write gate is still held, before checkpointStoragePhase
    // runs with the gate released. This prevents losing version bumps from new writers
    // that start after the gate is released.
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
    if (walRotated) {
        storageManager->getWAL().clearFrozenWAL();
    } else {
        storageManager->getWAL().reset();
    }
    storageManager->getShadowFile().reset();
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

void Checkpointer::serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
    bool storageChanges) {
    const auto storageManager = StorageManager::Get(clientContext);
    const auto catalog = catalog::Catalog::Get(clientContext);
    auto* dataFH = storageManager->getDataFH();
    const bool useSnapshot = snapshotTS > 0;

    if (databaseHeader.catalogPageRange.startPageIdx == common::INVALID_PAGE_IDX ||
        catalog->changedSinceLastCheckpoint()) {
        databaseHeader.updateCatalogPageRange(*dataFH->getPageManager(),
            useSnapshot ? serializeCatalogSnapshot(*catalog, *storageManager)
                        : serializeCatalog(*catalog, *storageManager));
    }
    if (databaseHeader.metadataPageRange.startPageIdx == common::INVALID_PAGE_IDX ||
        storageChanges || catalog->changedSinceLastCheckpoint() ||
        dataFH->getPageManager()->changedSinceLastCheckpoint()) {
        databaseHeader.freeMetadataPageRange(*dataFH->getPageManager());
        databaseHeader.metadataPageRange =
            useSnapshot ? serializeMetadataSnapshot(*catalog, *storageManager)
                        : serializeMetadata(*catalog, *storageManager);
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
    if (walRotated) {
        wal->logAndFlushCheckpointToFrozen(&clientContext);
    } else {
        wal->logAndFlushCheckpoint(&clientContext);
    }
    shadowFile.applyShadowPages(clientContext);
    auto bufferManager = MemoryManager::Get(clientContext)->getBufferManager();
    if (!walRotated) {
        wal->clear();
    }
    shadowFile.clear(*bufferManager);
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
    const auto expectedSize = transaction.getLocalWAL().getSize() + wal->getFileSize();
    return expectedSize > clientContext.getDBConfig()->checkpointThreshold;
}

void Checkpointer::readCheckpoint() {
    auto storageManager = StorageManager::Get(clientContext);
    storageManager->initDataFileHandle(common::VirtualFileSystem::GetUnsafe(clientContext),
        &clientContext);
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
        storageManager->getDataFH()->getPageManager()->deserialize(deSer);
    }
    storageManager->setDatabaseHeader(std::move(currentHeader));
}

} // namespace storage
} // namespace kuzu
