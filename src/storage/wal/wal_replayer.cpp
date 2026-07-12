#include "storage/wal/wal_replayer.h"

#include <limits>

#include "binder/binder.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_entry/type_catalog_entry.h"
#include "common/file_system/file_info.h"
#include "common/file_system/file_system.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "extension/extension_manager.h"
#include "main/client_context.h"
#include "processor/expression_mapper.h"
#include "storage/checkpointer.h"
#include "storage/database_header.h"
#include "storage/file_db_id_utils.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "storage/wal/checksum_reader.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction_context.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

static constexpr std::string_view checksumMismatchMessage =
    "Checksum verification failed, the WAL file is corrupted.";

WALReplayer::WALReplayer(main::ClientContext& clientContext) : clientContext{clientContext} {
    walPath = StorageUtils::getWALFilePath(clientContext.getDatabasePath());
    checkpointWalPath = StorageUtils::getCheckpointWALFilePath(clientContext.getDatabasePath());
    shadowFilePath = StorageUtils::getShadowFilePath(clientContext.getDatabasePath());
}

static WALHeader readWALHeader(Deserializer& deserializer) {
    WALHeader header{};
    deserializer.deserializeValue(header.databaseID);

    // It is possible to read a value other than 0/1 when deserializing the flag
    // This causes some weird behaviours with some toolchains so we manually do the conversion here
    uint8_t enableChecksumsBytes = 0;
    deserializer.deserializeValue(enableChecksumsBytes);
    header.enableChecksums = enableChecksumsBytes != 0;

    return header;
}

static Deserializer initDeserializer(FileInfo& fileInfo, main::ClientContext& clientContext,
    bool enableChecksums) {
    if (enableChecksums) {
        return Deserializer{std::make_unique<ChecksumReader>(fileInfo,
            *MemoryManager::Get(clientContext), checksumMismatchMessage)};
    } else {
        return Deserializer{std::make_unique<BufferedFileReader>(fileInfo)};
    }
}

static void checkWALHeader(const WALHeader& header, bool enableChecksums) {
    if (enableChecksums != header.enableChecksums) {
        throw RuntimeException(stringFormat(
            "The database you are trying to open was serialized with enableChecksums={} but you "
            "are trying to open it with enableChecksums={}. Please open your database using the "
            "correct enableChecksums config. If you wish to change this for your database, please "
            "use the export/import functionality.",
            TypeUtils::toString(header.enableChecksums), TypeUtils::toString(enableChecksums)));
    }
}

static uint64_t getReadOffset(Deserializer& deSer, bool enableChecksums) {
    if (enableChecksums) {
        return deSer.getReader()->cast<ChecksumReader>()->getReadOffset();
    } else {
        return deSer.getReader()->cast<BufferedFileReader>()->getReadOffset();
    }
}

static void verifyWALDatabaseID(main::ClientContext& context, FileInfo& walFileInfo,
    ku_uuid_t walDatabaseID) {
    auto vfs = VirtualFileSystem::GetUnsafe(context);
    if (!vfs->fileOrPathExists(context.getDatabasePath(), &context)) {
        throw RuntimeException(
            "Found a checkpoint WAL but no corresponding database file.");
    }
    auto dataFileInfo =
        vfs->openFile(context.getDatabasePath(), FileOpenFlags(FileFlags::READ_ONLY), &context);
    const auto databaseHeader = DatabaseHeader::readDatabaseHeader(*dataFileInfo);
    if (!databaseHeader.has_value()) {
        throw RuntimeException("Found a checkpoint WAL but no valid database header.");
    }
    FileDBIDUtils::verifyDatabaseID(
        walFileInfo, databaseHeader->databaseID, walDatabaseID);
}

void WALReplayer::replay(bool throwOnWalReplayFailure, bool enableChecksums) const {
    auto vfs = VirtualFileSystem::GetUnsafe(clientContext);
    Checkpointer checkpointer(clientContext);
    const bool hasFrozenWAL = vfs->fileOrPathExists(checkpointWalPath, &clientContext);
    const bool hasActiveWAL = vfs->fileOrPathExists(walPath, &clientContext);

    if (!hasFrozenWAL && !hasActiveWAL) {
        ShadowFile::rollbackCheckpoint(
            clientContext, std::nullopt, std::nullopt, std::nullopt);
        removeFileIfExists(shadowFilePath);
        checkpointer.readCheckpoint();
        return;
    }

    std::unique_ptr<FileInfo> frozenFileInfo;
    std::unique_ptr<FileInfo> activeFileInfo;
    WALReplayInfo frozenReplayInfo;
    WALReplayInfo activeReplayInfo;
    if (hasFrozenWAL) {
        frozenFileInfo = openWALFile(checkpointWalPath);
        if (frozenFileInfo->getFileSize() > 0) {
            syncWALFile(*frozenFileInfo);
            frozenReplayInfo =
                dryReplay(*frozenFileInfo, throwOnWalReplayFailure, enableChecksums);
        }
    }
    if (hasActiveWAL) {
        activeFileInfo = openWALFile(walPath);
        if (activeFileInfo->getFileSize() > 0) {
            syncWALFile(*activeFileInfo);
            activeReplayInfo = dryReplay(*activeFileInfo, throwOnWalReplayFailure, enableChecksums);
        }
    }

    const auto replayShadowPagesIfPresent = [&](const WALReplayInfo& replayInfo) {
        if (replayInfo.checkpointEndDataFileNumPages.has_value()) {
            auto dataFileInfo = vfs->openFile(clientContext.getDatabasePath(),
                FileOpenFlags(FileFlags::READ_ONLY), &clientContext);
            const auto checkpointEndDataFileSize = static_cast<uint64_t>(
                *replayInfo.checkpointEndDataFileNumPages) * KUZU_PAGE_SIZE;
            if (dataFileInfo->getFileSize() > checkpointEndDataFileSize) {
                throw RuntimeException(
                    "Checkpoint end watermark is below the database file size.");
            }
            if (!vfs->fileOrPathExists(shadowFilePath, &clientContext) &&
                dataFileInfo->getFileSize() != checkpointEndDataFileSize) {
                throw RuntimeException(
                    "Checkpoint marker has no shadow file and installation is ambiguous.");
            }
        }
        if (!vfs->fileOrPathExists(shadowFilePath, &clientContext)) {
            if (replayInfo.checkpointID.has_value() &&
                replayInfo.checkpointStartDataFileNumPages.has_value() &&
                replayInfo.checkpointEndDataFileNumPages.has_value()) {
                // A valid V2 begin proves durable shadow creation, so absence after its marker
                // means shadow application and shadow-first cleanup completed.
                return;
            }
            throw RuntimeException(
                "Legacy checkpoint marker has no shadow file; checkpoint state is ambiguous.");
        }
        auto shadowFileInfo = vfs->openFile(shadowFilePath,
            FileOpenFlags(FileFlags::READ_ONLY), &clientContext);
        if (shadowFileInfo->getFileSize() == 0) {
            throw RuntimeException("Checkpoint shadow file is empty.");
        }
        shadowFileInfo.reset();
        ShadowFile::replayShadowPageRecords(
            clientContext, replayInfo.databaseID, replayInfo.checkpointID,
            replayInfo.checkpointStartDataFileNumPages,
            replayInfo.checkpointEndDataFileNumPages);
    };
    const auto rollbackShadowCheckpoint = [&]() {
        if (frozenReplayInfo.checkpointID.has_value() &&
            activeReplayInfo.checkpointID.has_value()) {
            throw RuntimeException("Both WAL files contain unfinished checkpoints.");
        }
        const WALReplayInfo* replayInfo = nullptr;
        if (frozenReplayInfo.checkpointID.has_value()) {
            replayInfo = &frozenReplayInfo;
        } else if (activeReplayInfo.checkpointID.has_value()) {
            replayInfo = &activeReplayInfo;
        } else if (frozenReplayInfo.hasHeader) {
            replayInfo = &frozenReplayInfo;
        } else if (activeReplayInfo.hasHeader) {
            replayInfo = &activeReplayInfo;
        }
        if (!replayInfo) {
            ShadowFile::rollbackCheckpoint(
                clientContext, std::nullopt, std::nullopt, std::nullopt);
            return;
        }
        ShadowFile::rollbackCheckpoint(clientContext, replayInfo->databaseID,
            replayInfo->checkpointID, replayInfo->checkpointStartDataFileNumPages);
    };
    const auto validateWALBeforeShadowCleanup =
        [&](FileInfo* fileInfo, const WALReplayInfo& replayInfo) {
        if (!fileInfo || fileInfo->getFileSize() == 0) {
            return;
        }
        if (!replayInfo.hasHeader) {
            throw RuntimeException("Nonempty WAL has no valid header.");
        }
        verifyWALDatabaseID(clientContext, *fileInfo, replayInfo.databaseID);
    };

    try {
        validateWALBeforeShadowCleanup(frozenFileInfo.get(), frozenReplayInfo);
        validateWALBeforeShadowCleanup(activeFileInfo.get(), activeReplayInfo);

        if (activeReplayInfo.isLastRecordCheckpoint) {
            if (frozenReplayInfo.isLastRecordCheckpoint) {
                throw RuntimeException("Both the active and checkpoint WAL files end with a "
                                       "checkpoint record.");
            }
            if (frozenReplayInfo.checkpointID.has_value()) {
                throw RuntimeException(
                    "Active checkpoint marker follows an incomplete frozen checkpoint.");
            }
            verifyWALDatabaseID(clientContext, *activeFileInfo, activeReplayInfo.databaseID);
            frozenFileInfo.reset();
            replayShadowPagesIfPresent(activeReplayInfo);
            activeFileInfo.reset();
            removeFileIfExists(checkpointWalPath);
            removeWALAndShadowFiles();
            checkpointer.readCheckpoint();
            return;
        }

        bool replayedFrozenWAL = false;
        if (frozenFileInfo) {
            if (frozenFileInfo->getFileSize() == 0 || frozenReplayInfo.offsetDeserialized == 0) {
                frozenFileInfo.reset();
                rollbackShadowCheckpoint();
                removeFileIfExists(checkpointWalPath);
                removeFileIfExists(shadowFilePath);
                checkpointer.readCheckpoint();
            } else if (frozenReplayInfo.isLastRecordCheckpoint) {
                verifyWALDatabaseID(clientContext, *frozenFileInfo, frozenReplayInfo.databaseID);
                replayShadowPagesIfPresent(frozenReplayInfo);
                frozenFileInfo.reset();
                removeFileIfExists(shadowFilePath);
                removeFileIfExists(checkpointWalPath);
                checkpointer.readCheckpoint();
            } else {
                const auto rollbackCheckpoint = frozenReplayInfo.checkpointID.has_value();
                rollbackShadowCheckpoint();
                removeFileIfExists(shadowFilePath);
                checkpointer.readCheckpoint();
                if (rollbackCheckpoint) {
                    truncateWALFile(*frozenFileInfo, frozenReplayInfo.offsetDeserialized);
                }
                replayWALFile(*frozenFileInfo, frozenReplayInfo, enableChecksums);
                if (!rollbackCheckpoint) {
                    truncateWALFile(*frozenFileInfo, frozenReplayInfo.offsetDeserialized);
                }
                replayedFrozenWAL = true;
            }
        } else {
            rollbackShadowCheckpoint();
            removeFileIfExists(shadowFilePath);
            checkpointer.readCheckpoint();
        }

        if (activeFileInfo) {
            if (activeFileInfo->getFileSize() == 0) {
                activeFileInfo.reset();
                removeFileIfExists(walPath);
            } else {
                const auto rollbackCheckpoint = activeReplayInfo.checkpointID.has_value();
                if (rollbackCheckpoint) {
                    truncateWALFile(*activeFileInfo, activeReplayInfo.offsetDeserialized);
                    if (activeReplayInfo.offsetDeserialized == 0) {
                        activeFileInfo.reset();
                        removeFileIfExists(walPath);
                    } else {
                        replayWALFile(*activeFileInfo, activeReplayInfo, enableChecksums);
                    }
                } else {
                    replayWALFile(*activeFileInfo, activeReplayInfo, enableChecksums);
                    truncateWALFile(*activeFileInfo, activeReplayInfo.offsetDeserialized);
                }
            }
        }

        if (replayedFrozenWAL && !StorageManager::Get(clientContext)->isReadOnly()) {
            frozenFileInfo.reset();
            activeFileInfo.reset();
            checkpointer.writeRecoveryCheckpoint();
        }
    } catch (const std::exception&) {
        auto transactionContext = TransactionContext::Get(clientContext);
        if (transactionContext->hasActiveTransaction()) {
            transactionContext->rollback();
        }
        throw;
    }
}

void WALReplayer::replayWALFile(FileInfo& fileInfo, const WALReplayInfo& replayInfo,
    bool enableChecksums) const {
    Deserializer deserializer = initDeserializer(fileInfo, clientContext, enableChecksums);
    if (replayInfo.offsetDeserialized > 0) {
        deserializer.getReader()->onObjectBegin();
        const auto walHeader = readWALHeader(deserializer);
        FileDBIDUtils::verifyDatabaseID(fileInfo,
            StorageManager::Get(clientContext)->getOrInitDatabaseID(clientContext),
            walHeader.databaseID);
        deserializer.getReader()->onObjectEnd();
    }
    while (getReadOffset(deserializer, enableChecksums) < replayInfo.offsetDeserialized) {
        KU_ASSERT(!deserializer.finished());
        auto walRecord = WALRecord::deserialize(deserializer, clientContext);
        replayWALRecord(*walRecord);
    }
}

WALReplayer::WALReplayInfo WALReplayer::dryReplay(FileInfo& fileInfo, bool throwOnWalReplayFailure,
    bool enableChecksums) const {
    uint64_t offsetDeserialized = 0;
    bool isLastRecordCheckpoint = false;
    ku_uuid_t databaseID{0};
    bool hasHeader = false;
    std::optional<ku_uuid_t> checkpointID;
    std::optional<page_idx_t> checkpointStartDataFileNumPages;
    std::optional<page_idx_t> checkpointEndDataFileNumPages;
    bool checkpointProtocolFailure = false;
    WALRecordType encounteredType = WALRecordType::INVALID_RECORD;
    try {
        Deserializer deserializer = initDeserializer(fileInfo, clientContext, enableChecksums);

        // Skip the databaseID here, we'll verify it when we actually replay
        deserializer.getReader()->onObjectBegin();
        const auto walHeader = readWALHeader(deserializer);
        databaseID = walHeader.databaseID;
        checkWALHeader(walHeader, enableChecksums);
        deserializer.getReader()->onObjectEnd();
        hasHeader = true;

        bool finishedDeserializing = deserializer.finished();
        while (!finishedDeserializing) {
            encounteredType = WALRecordType::INVALID_RECORD;
            auto walRecord = WALRecord::deserialize(
                deserializer, clientContext, &encounteredType);
            finishedDeserializing = deserializer.finished();
            if (checkpointID.has_value() &&
                walRecord->type != WALRecordType::CHECKPOINT_RECORD_V2) {
                checkpointProtocolFailure = true;
                throw RuntimeException("WAL contains a record after checkpoint begin.");
            }
            switch (walRecord->type) {
            case WALRecordType::CHECKPOINT_BEGIN_RECORD: {
                const auto& checkpointBeginRecord = walRecord->cast<CheckpointBeginRecord>();
                if ((checkpointBeginRecord.checkpointStartDataFileNumPages ^
                        checkpointBeginRecord.checkpointStartDataFileNumPagesCheck) !=
                        std::numeric_limits<page_idx_t>::max() ||
                    checkpointBeginRecord.checkpointStartDataFileNumPages == 0) {
                    checkpointProtocolFailure = true;
                    throw RuntimeException("WAL checkpoint begin has an invalid page watermark.");
                }
                checkpointID = checkpointBeginRecord.checkpointID;
                checkpointStartDataFileNumPages =
                    checkpointBeginRecord.checkpointStartDataFileNumPages;
            } break;
            case WALRecordType::CHECKPOINT_RECORD:
            case WALRecordType::CHECKPOINT_RECORD_V2: {
                if (!finishedDeserializing) {
                    checkpointProtocolFailure = true;
                    throw RuntimeException("WAL checkpoint marker is not the final record.");
                }
                if (walRecord->type == WALRecordType::CHECKPOINT_RECORD_V2) {
                    const auto& checkpointRecord = walRecord->cast<CheckpointRecordV2>();
                    const auto markerCheckpointID = checkpointRecord.checkpointID;
                    if (!checkpointID.has_value()) {
                        checkpointProtocolFailure = true;
                        throw RuntimeException(
                            "WAL checkpoint marker has no matching checkpoint begin record.");
                    }
                    if (checkpointID->value != markerCheckpointID.value) {
                        checkpointProtocolFailure = true;
                        throw RuntimeException("WAL checkpoint identities do not match.");
                    }
                    if ((checkpointRecord.checkpointEndDataFileNumPages ^
                            checkpointRecord.checkpointEndDataFileNumPagesCheck) !=
                            std::numeric_limits<page_idx_t>::max() ||
                        checkpointRecord.checkpointEndDataFileNumPages <
                            *checkpointStartDataFileNumPages ||
                        static_cast<uint64_t>(checkpointRecord.checkpointEndDataFileNumPages) >
                            clientContext.getDBConfig()->maxDBSize / KUZU_PAGE_SIZE) {
                        checkpointProtocolFailure = true;
                        throw RuntimeException(
                            "WAL checkpoint marker has invalid data-file page bounds.");
                    }
                    checkpointID = markerCheckpointID;
                    checkpointEndDataFileNumPages =
                        checkpointRecord.checkpointEndDataFileNumPages;
                } else if (checkpointID.has_value()) {
                    checkpointProtocolFailure = true;
                    throw RuntimeException(
                        "Legacy checkpoint marker follows an identified checkpoint.");
                }
                isLastRecordCheckpoint = true;
                finishedDeserializing = true;
                offsetDeserialized = getReadOffset(deserializer, enableChecksums);
            } break;
            case WALRecordType::COMMIT_RECORD: {
                // Update the offset to the end of the last commit record.
                offsetDeserialized = getReadOffset(deserializer, enableChecksums);
            } break;
            default: {
                // DO NOTHING.
            }
            }
        }
    } catch (...) {
        // If we hit an exception while deserializing, we assume that the WAL file is (partially)
        // corrupted. This should only happen for records of the last transaction recorded.
        if (encounteredType == WALRecordType::CHECKPOINT_BEGIN_RECORD ||
            encounteredType == WALRecordType::CHECKPOINT_RECORD_V2 ||
            encounteredType == WALRecordType::CHECKPOINT_RECORD) {
            checkpointProtocolFailure = true;
        }
        if (throwOnWalReplayFailure || checkpointProtocolFailure || checkpointID.has_value()) {
            throw;
        }
    }
    return {offsetDeserialized, isLastRecordCheckpoint, databaseID, hasHeader, checkpointID,
        checkpointStartDataFileNumPages, checkpointEndDataFileNumPages};
}

void WALReplayer::replayWALRecord(WALRecord& walRecord) const {
    switch (walRecord.type) {
    case WALRecordType::BEGIN_TRANSACTION_RECORD: {
        TransactionContext::Get(clientContext)->beginRecoveryTransaction();
    } break;
    case WALRecordType::COMMIT_RECORD: {
        TransactionContext::Get(clientContext)->commit();
    } break;
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD: {
        replayCreateCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD: {
        replayDropCatalogEntryRecord(walRecord);
    } break;
    case WALRecordType::ALTER_TABLE_ENTRY_RECORD: {
        replayAlterTableEntryRecord(walRecord);
    } break;
    case WALRecordType::TABLE_INSERTION_RECORD: {
        replayTableInsertionRecord(walRecord);
    } break;
    case WALRecordType::NODE_DELETION_RECORD: {
        replayNodeDeletionRecord(walRecord);
    } break;
    case WALRecordType::NODE_UPDATE_RECORD: {
        replayNodeUpdateRecord(walRecord);
    } break;
    case WALRecordType::REL_DELETION_RECORD: {
        replayRelDeletionRecord(walRecord);
    } break;
    case WALRecordType::REL_DETACH_DELETE_RECORD: {
        replayRelDetachDeletionRecord(walRecord);
    } break;
    case WALRecordType::REL_UPDATE_RECORD: {
        replayRelUpdateRecord(walRecord);
    } break;
    case WALRecordType::COPY_TABLE_RECORD: {
        replayCopyTableRecord(walRecord);
    } break;
    case WALRecordType::UPDATE_SEQUENCE_RECORD: {
        replayUpdateSequenceRecord(walRecord);
    } break;
    case WALRecordType::LOAD_EXTENSION_RECORD: {
        replayLoadExtensionRecord(walRecord);
    } break;
    case WALRecordType::CHECKPOINT_RECORD:
    case WALRecordType::CHECKPOINT_RECORD_V2: {
        // This record should not be replayed. It is only used to indicate that the previous records
        // had been replayed and shadow files are created.
        KU_UNREACHABLE;
    }
    case WALRecordType::CHECKPOINT_BEGIN_RECORD: {
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void WALReplayer::replayCreateCatalogEntryRecord(WALRecord& walRecord) const {
    auto catalog = Catalog::Get(clientContext);
    auto transaction = transaction::Transaction::Get(clientContext);
    auto storageManager = StorageManager::Get(clientContext);
    auto& record = walRecord.cast<CreateCatalogEntryRecord>();
    switch (record.ownedCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        auto& entry = record.ownedCatalogEntry->constCast<TableCatalogEntry>();
        auto newEntry = catalog->createTableEntry(transaction,
            entry.getBoundCreateTableInfo(transaction, record.isInternal));
        storageManager->createTable(newEntry->ptrCast<TableCatalogEntry>());
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        auto& macroEntry = record.ownedCatalogEntry->constCast<ScalarMacroCatalogEntry>();
        catalog->addScalarMacroFunction(transaction, macroEntry.getName(),
            macroEntry.getMacroFunction()->copy());
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        auto& sequenceEntry = record.ownedCatalogEntry->constCast<SequenceCatalogEntry>();
        catalog->createSequence(transaction,
            sequenceEntry.getBoundCreateSequenceInfo(record.isInternal));
    } break;
    case CatalogEntryType::TYPE_ENTRY: {
        auto& typeEntry = record.ownedCatalogEntry->constCast<TypeCatalogEntry>();
        catalog->createType(transaction, typeEntry.getName(), typeEntry.getLogicalType().copy());
    } break;
    case CatalogEntryType::INDEX_ENTRY: {
        catalog->createIndex(transaction, std::move(record.ownedCatalogEntry));
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayDropCatalogEntryRecord(const WALRecord& walRecord) const {
    auto& dropEntryRecord = walRecord.constCast<DropCatalogEntryRecord>();
    auto catalog = Catalog::Get(clientContext);
    auto transaction = transaction::Transaction::Get(clientContext);
    const auto entryID = dropEntryRecord.entryID;
    switch (dropEntryRecord.entryType) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        KU_ASSERT(Catalog::Get(clientContext));
        catalog->dropTableEntry(transaction, entryID);
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        catalog->dropSequence(transaction, entryID);
    } break;
    case CatalogEntryType::INDEX_ENTRY: {
        catalog->dropIndex(transaction, entryID);
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY: {
        catalog->dropMacroEntry(transaction, entryID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void WALReplayer::replayAlterTableEntryRecord(const WALRecord& walRecord) const {
    auto binder = Binder(&clientContext);
    auto& alterEntryRecord = walRecord.constCast<AlterTableEntryRecord>();
    auto catalog = Catalog::Get(clientContext);
    auto transaction = transaction::Transaction::Get(clientContext);
    auto storageManager = StorageManager::Get(clientContext);
    auto ownedAlterInfo = alterEntryRecord.ownedAlterInfo.get();
    catalog->alterTableEntry(transaction, *ownedAlterInfo);
    auto& pageAllocator = *PageManager::Get(clientContext);
    switch (ownedAlterInfo->alterType) {
    case AlterType::ADD_PROPERTY: {
        const auto exprBinder = binder.getExpressionBinder();
        const auto addInfo = ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
        // We don't implicit cast here since it must already be done the first time
        const auto boundDefault =
            exprBinder->bindExpression(*addInfo->propertyDefinition.defaultExpr);
        auto exprMapper = ExpressionMapper();
        const auto defaultValueEvaluator = exprMapper.getEvaluator(boundDefault);
        defaultValueEvaluator->init(ResultSet(0) /* dummy ResultSet */, &clientContext);
        const auto entry = catalog->getTableCatalogEntry(transaction, ownedAlterInfo->tableName);
        const auto& addedProp = entry->getProperty(addInfo->propertyDefinition.getName());
        TableAddColumnState state{addedProp, *defaultValueEvaluator};
        KU_ASSERT(StorageManager::Get(clientContext));
        switch (entry->getTableType()) {
        case TableType::REL: {
            for (auto& relEntryInfo : entry->cast<RelGroupCatalogEntry>().getRelEntryInfos()) {
                storageManager->getTable(relEntryInfo.oid)
                    ->addColumn(transaction, state, pageAllocator);
            }
        } break;
        case TableType::NODE: {
            storageManager->getTable(entry->getTableID())
                ->addColumn(transaction, state, pageAllocator);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    } break;
    case AlterType::ADD_FROM_TO_CONNECTION: {
        auto extraInfo = ownedAlterInfo->extraInfo->constPtrCast<BoundExtraAlterFromToConnection>();
        auto relGroupEntry = catalog->getTableCatalogEntry(transaction, ownedAlterInfo->tableName)
                                 ->ptrCast<RelGroupCatalogEntry>();
        auto relEntryInfo =
            relGroupEntry->getRelEntryInfo(extraInfo->fromTableID, extraInfo->toTableID);
        storageManager->addRelTable(relGroupEntry, *relEntryInfo);
    } break;
    default:
        break;
    }
}

void WALReplayer::replayTableInsertionRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    switch (insertionRecord.tableType) {
    case TableType::NODE: {
        replayNodeTableInsertRecord(walRecord);
    } break;
    case TableType::REL: {
        replayRelTableInsertRecord(walRecord);
    } break;
    default: {
        throw RuntimeException("Invalid table type for insertion replay in WAL record.");
    }
    }
}

void WALReplayer::replayNodeTableInsertRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    const auto tableID = insertionRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<NodeTable>();
    KU_ASSERT(!insertionRecord.ownedVectors.empty());
    const auto anchorState = insertionRecord.ownedVectors[0]->state;
    const auto numNodes = anchorState->getSelVector().getSelSize();
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(anchorState);
    }
    std::vector<ValueVector*> propertyVectors(insertionRecord.ownedVectors.size());
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        propertyVectors[i] = insertionRecord.ownedVectors[i].get();
    }
    KU_ASSERT(table.getPKColumnID() < insertionRecord.ownedVectors.size());
    auto& pkVector = *insertionRecord.ownedVectors[table.getPKColumnID()];
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    const auto insertState =
        std::make_unique<NodeTableInsertState>(*nodeIDVector, pkVector, propertyVectors);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    table.initInsertState(&clientContext, *insertState);
    anchorState->getSelVectorUnsafe().setToFiltered(1);
    for (auto i = 0u; i < numNodes; i++) {
        anchorState->getSelVectorUnsafe()[0] = i;
        table.insert(transaction::Transaction::Get(clientContext), *insertState);
    }
}

void WALReplayer::replayRelTableInsertRecord(const WALRecord& walRecord) const {
    const auto& insertionRecord = walRecord.constCast<TableInsertionRecord>();
    const auto tableID = insertionRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<RelTable>();
    KU_ASSERT(!insertionRecord.ownedVectors.empty());
    const auto anchorState = insertionRecord.ownedVectors[0]->state;
    const auto numRels = anchorState->getSelVector().getSelSize();
    anchorState->getSelVectorUnsafe().setToFiltered(1);
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        insertionRecord.ownedVectors[i]->setState(anchorState);
    }
    std::vector<ValueVector*> propertyVectors;
    for (auto i = 0u; i < insertionRecord.ownedVectors.size(); i++) {
        if (i < LOCAL_REL_ID_COLUMN_ID) {
            // Skip the first two vectors which are the src nodeID and the dst nodeID.
            continue;
        }
        propertyVectors.push_back(insertionRecord.ownedVectors[i].get());
    }
    const auto insertState = std::make_unique<RelTableInsertState>(
        *insertionRecord.ownedVectors[LOCAL_BOUND_NODE_ID_COLUMN_ID],
        *insertionRecord.ownedVectors[LOCAL_NBR_NODE_ID_COLUMN_ID], propertyVectors);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    for (auto i = 0u; i < numRels; i++) {
        anchorState->getSelVectorUnsafe()[0] = i;
        table.initInsertState(&clientContext, *insertState);
        table.insert(transaction::Transaction::Get(clientContext), *insertState);
    }
}

void WALReplayer::replayNodeDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<NodeDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<NodeTable>();
    const auto anchorState = deletionRecord.ownedPKVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{deletionRecord.nodeOffset, deletionRecord.tableID});
    const auto deleteState =
        std::make_unique<NodeTableDeleteState>(*nodeIDVector, *deletionRecord.ownedPKVector);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    table.delete_(transaction::Transaction::Get(clientContext), *deleteState);
}

void WALReplayer::replayNodeUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<NodeUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<NodeTable>();
    const auto anchorState = updateRecord.ownedPropertyVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    nodeIDVector->setState(anchorState);
    nodeIDVector->setValue<internalID_t>(0,
        internalID_t{updateRecord.nodeOffset, updateRecord.tableID});
    const auto updateState = std::make_unique<NodeTableUpdateState>(updateRecord.columnID,
        *nodeIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    table.update(transaction::Transaction::Get(clientContext), *updateState);
}

void WALReplayer::replayRelDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<RelDeletionRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<RelTable>();
    const auto anchorState = deletionRecord.ownedRelIDVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto deleteState =
        std::make_unique<RelTableDeleteState>(*deletionRecord.ownedSrcNodeIDVector,
            *deletionRecord.ownedDstNodeIDVector, *deletionRecord.ownedRelIDVector);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    table.delete_(transaction::Transaction::Get(clientContext), *deleteState);
}

void WALReplayer::replayRelDetachDeletionRecord(const WALRecord& walRecord) const {
    const auto& deletionRecord = walRecord.constCast<RelDetachDeleteRecord>();
    const auto tableID = deletionRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<RelTable>();
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    const auto anchorState = deletionRecord.ownedSrcNodeIDVector->state;
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto dstNodeIDVector =
        std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    const auto relIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    dstNodeIDVector->setState(anchorState);
    relIDVector->setState(anchorState);
    const auto deleteState = std::make_unique<RelTableDeleteState>(
        *deletionRecord.ownedSrcNodeIDVector, *dstNodeIDVector, *relIDVector);
    deleteState->detachDeleteDirection = deletionRecord.direction;
    table.detachDelete(transaction::Transaction::Get(clientContext), deleteState.get());
}

void WALReplayer::replayRelUpdateRecord(const WALRecord& walRecord) const {
    const auto& updateRecord = walRecord.constCast<RelUpdateRecord>();
    const auto tableID = updateRecord.tableID;
    auto& table = StorageManager::Get(clientContext)->getTable(tableID)->cast<RelTable>();
    const auto anchorState = updateRecord.ownedRelIDVector->state;
    KU_ASSERT(anchorState == updateRecord.ownedSrcNodeIDVector->state &&
              anchorState == updateRecord.ownedSrcNodeIDVector->state &&
              anchorState == updateRecord.ownedPropertyVector->state);
    KU_ASSERT(anchorState->getSelVector().getSelSize() == 1);
    const auto updateState = std::make_unique<RelTableUpdateState>(updateRecord.columnID,
        *updateRecord.ownedSrcNodeIDVector, *updateRecord.ownedDstNodeIDVector,
        *updateRecord.ownedRelIDVector, *updateRecord.ownedPropertyVector);
    KU_ASSERT(transaction::Transaction::Get(clientContext) &&
              transaction::Transaction::Get(clientContext)->isRecovery());
    table.update(transaction::Transaction::Get(clientContext), *updateState);
}

void WALReplayer::replayCopyTableRecord(const WALRecord&) const {
    // DO NOTHING.
}

void WALReplayer::replayUpdateSequenceRecord(const WALRecord& walRecord) const {
    auto& sequenceEntryRecord = walRecord.constCast<UpdateSequenceRecord>();
    const auto sequenceID = sequenceEntryRecord.sequenceID;
    const auto entry =
        Catalog::Get(clientContext)
            ->getSequenceEntry(transaction::Transaction::Get(clientContext), sequenceID);
    entry->nextKVal(transaction::Transaction::Get(clientContext), sequenceEntryRecord.kCount);
}

void WALReplayer::replayLoadExtensionRecord(const WALRecord& walRecord) const {
    const auto& loadExtensionRecord = walRecord.constCast<LoadExtensionRecord>();
    extension::ExtensionManager::Get(clientContext)
        ->loadExtension(loadExtensionRecord.path, &clientContext);
}

void WALReplayer::removeWALAndShadowFiles() const {
    removeFileIfExists(shadowFilePath);
    removeFileIfExists(walPath);
}

void WALReplayer::removeFileIfExists(const std::string& path) const {
    if (StorageManager::Get(clientContext)->isReadOnly()) {
        return;
    }
#if !defined(__WASM__)
    auto vfs = VirtualFileSystem::GetUnsafe(clientContext);
    vfs->removeFileIfExistsDurably(path);
#else
    auto vfs = VirtualFileSystem::GetUnsafe(clientContext);
    if (vfs->fileOrPathExists(path, &clientContext)) {
        vfs->removeFileIfExists(path);
    }
#endif
}

std::unique_ptr<FileInfo> WALReplayer::openWALFile(const std::string& path) const {
    auto flag = FileFlags::READ_ONLY;
    if (!StorageManager::Get(clientContext)->isReadOnly()) {
        flag |= FileFlags::WRITE; // The write flag here is to ensure the file is opened with O_RDWR
                                  // so that we can sync it.
    }
    return VirtualFileSystem::GetUnsafe(clientContext)->openFile(path, FileOpenFlags(flag));
}

void WALReplayer::syncWALFile(const FileInfo& fileInfo) const {
    if (StorageManager::Get(clientContext)->isReadOnly()) {
        return;
    }
    fileInfo.syncFile();
}

void WALReplayer::truncateWALFile(FileInfo& fileInfo, uint64_t size) const {
    if (StorageManager::Get(clientContext)->isReadOnly()) {
        return;
    }
    if (fileInfo.getFileSize() > size) {
        fileInfo.truncate(size);
        fileInfo.syncFile();
    }
}

} // namespace storage
} // namespace kuzu
