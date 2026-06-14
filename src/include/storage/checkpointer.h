#pragma once

#include <unordered_map>

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

class Checkpointer {
    friend class main::AttachedKuzuDatabase;
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
    void postCheckpointCleanup();
    void rollback();
    bool wasWalRotated() const { return walRotated; }

    void readCheckpoint();

    static bool canAutoCheckpoint(const main::ClientContext& clientContext,
        const transaction::Transaction& transaction);

protected:
    virtual bool checkpointStorage();
    virtual void serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
        bool hasStorageChanges);
    virtual void writeDatabaseHeader(const DatabaseHeader& header);
    virtual void logCheckpointAndApplyShadowPages(bool walRotated);

private:
    static void readCheckpoint(main::ClientContext* context, catalog::Catalog* catalog,
        StorageManager* storageManager);

    PageRange serializeCatalog(const catalog::Catalog& catalog, StorageManager& storageManager);
    PageRange serializeCatalogSnapshot(const catalog::Catalog& catalog,
        StorageManager& storageManager);
    PageRange serializeMetadata(const catalog::Catalog& catalog, StorageManager& storageManager);
    PageRange serializeMetadataSnapshot(const catalog::Catalog& catalog,
        StorageManager& storageManager);

protected:
    main::ClientContext& clientContext;
    bool isInMemory;
    bool walRotated = false;
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
