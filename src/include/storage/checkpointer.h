#pragma once

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
    // Cleanup after the core checkpoint that does not require the write gate.
    // Safe to call while new writers are active.
    void postCheckpointCleanup();
    void rollback();

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
    PageRange serializeMetadata(const catalog::Catalog& catalog, StorageManager& storageManager);

protected:
    main::ClientContext& clientContext;
    bool isInMemory;
    bool walRotated = false;
    // Versions captured at the end of writeCheckpoint() while the write gate is still held.
    // Used by postCheckpointCleanup() to safely reset version tracking without losing
    // concurrent version bumps from writers that started after the gate was released.
    uint64_t catalogVersionAtCheckpoint = 0;
    uint64_t pageManagerVersionAtCheckpoint = 0;
};

} // namespace storage
} // namespace kuzu
