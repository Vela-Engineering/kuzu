#pragma once

#include <functional>

#include "storage/wal/wal_record.h"

namespace kuzu {
namespace testing {
class BaseGraphTest;
} // namespace testing
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
} // namespace common

namespace storage {
class LocalWAL;
class WAL {
    friend class testing::BaseGraphTest;

public:
    WAL(const std::string& dbPath, bool readOnly, bool enableChecksums,
        common::VirtualFileSystem* vfs);
    ~WAL();

    void logCommittedWAL(LocalWAL& localWAL, main::ClientContext* context);
    void logAndFlushCheckpoint(main::ClientContext* context);

    bool rotateForCheckpoint(main::ClientContext* context);
    void logAndFlushCheckpointToFrozen(main::ClientContext* context);
    void clearFrozenWAL();
    bool hasFrozenWAL() const;

    // Clear any buffer in the WAL writer. Also truncate the WAL file to 0 bytes.
    void clear();
    // Reset the WAL writer to nullptr, and remove the WAL file if it exists.
    void reset();

    uint64_t getFileSize();

    static WAL* Get(const main::ClientContext& context);

private:
    void setCommitSyncHookForTesting(std::function<void()> hook);
    void initWriter(main::ClientContext* context);
    void addNewWALRecordNoLock(const WALRecord& walRecord);
    void flushAndSyncNoLock(bool isCommit = false);
    void writeHeader(main::ClientContext& context);

private:
    std::mutex mtx;
    std::string walPath;
    std::string checkpointWalPath;
    bool inMemory;
    [[maybe_unused]] bool readOnly;
    common::VirtualFileSystem* vfs;
    std::unique_ptr<common::FileInfo> fileInfo;

    // Since most writes to the shared WAL will be flushing local WAL (which has its own checksums),
    // these writes can go through the normal writer. We do still need a checksum writer though for
    // writing COMMIT/CHECKPOINT records
    std::unique_ptr<common::Serializer> serializer;
    bool enableChecksums;
    bool syncFileCreationOnNextFlush;
    std::function<void()> commitSyncHookForTesting;
};

} // namespace storage
} // namespace kuzu
