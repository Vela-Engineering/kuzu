#pragma once

#include <unordered_map>
#include <vector>

#include "common/types/types.h"
#include "common/uniq_lock.h"
#include "storage/database_header.h"
#include "storage/page_range.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace catalog {
class Catalog;
}
namespace common {
class VirtualFileSystem;
} // namespace common
namespace testing {
struct FSMLeakChecker;
}
namespace main {
class AttachedKuzuDatabase;
} // namespace main

namespace storage {
class StorageManager;
class WALReplayer;

class Checkpointer {
    friend class main::AttachedKuzuDatabase;
    friend class WALReplayer;
    friend struct testing::FSMLeakChecker;

public:
    explicit Checkpointer(main::ClientContext& clientContext);
    virtual ~Checkpointer();

    void writeCheckpoint();
    void beginCheckpoint(common::transaction_t snapshotTS);
    // Storage materialization phase for the snapshot captured by beginCheckpoint().
    void checkpointStoragePhase();
    void finishCheckpoint();
    // Cleanup after the core checkpoint.
    virtual void postCheckpointCleanup();
    void retryShadowApplication();
    void rollback();
    bool wasWalRotated() const { return walRotated; }
    bool wasShadowApplicationStarted() const { return shadowApplicationStarted; }
    bool wasCheckpointBeginWriteStarted() const { return checkpointBeginWriteStarted; }
    bool wasCheckpointMarkerWriteStarted() const { return checkpointMarkerWriteStarted; }
    bool wasCheckpointMarkerWritten() const { return checkpointMarkerWritten; }

    void readCheckpoint();

    static bool canAutoCheckpoint(const main::ClientContext& clientContext,
        const transaction::Transaction& transaction);

protected:
    virtual bool checkpointStorage();
    virtual void serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
        bool hasStorageChanges);
    virtual void writeDatabaseHeader(const DatabaseHeader& header);
    virtual void logCheckpointAndApplyShadowPages(bool walRotated);
    void markShadowApplicationStarted() { shadowApplicationStarted = true; }
    void markCheckpointMarkerWriteStarted() { checkpointMarkerWriteStarted = true; }
    void markCheckpointMarkerWritten() { checkpointMarkerWritten = true; }

private:
    static void readCheckpoint(main::ClientContext* context, catalog::Catalog* catalog,
        StorageManager* storageManager);
    void writeRecoveryCheckpoint();

    PageRange serializeCatalog(const catalog::Catalog& catalog, StorageManager& storageManager);
    PageRange serializeCatalogSnapshot(const catalog::Catalog& catalog,
        StorageManager& storageManager);
    PageRange serializeMetadata(const catalog::Catalog& catalog, StorageManager& storageManager,
        const std::vector<PageRange>& livePageRanges);
    PageRange serializeMetadataSnapshot(const catalog::Catalog& catalog,
        StorageManager& storageManager, const std::vector<PageRange>& livePageRanges);

protected:
    main::ClientContext& clientContext;
    bool isInMemory;
    bool walRotated = false;
    bool shadowApplicationStarted = false;
    bool checkpointBeginWriteStarted = false;
    bool checkpointMarkerWriteStarted = false;
    bool checkpointMarkerWritten = false;
    common::ku_uuid_t checkpointID{0};
    // Snapshot timestamp captured at drain time for MVCC catalog serialization.
    common::transaction_t snapshotTS = 0;
    // Database header captured during beginCheckpoint for use in finishCheckpoint.
    DatabaseHeader checkpointHeader{};
    // Whether storage had changes during checkpointStorage.
    bool hasStorageChanges = false;
    // Versions restored by postCheckpointCleanup() after checkpoint-owned version bumps.
    uint64_t catalogVersionAtCheckpoint = 0;
    uint64_t pageManagerVersionAtCheckpoint = 0;
    // Per-table changeEpoch watermarks captured under the transaction gate.
    // Used as the stable epoch for snapshot checkpointing.
    std::unordered_map<common::table_id_t, uint64_t> tableEpochWatermarks;
};

} // namespace storage
} // namespace kuzu
