#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/constants.h"
#include "common/uniq_lock.h"
#include "storage/checkpointer.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace main {
class ClientContext;
class Database;
} // namespace main

namespace testing {
class DBTest;
class FlakyBufferManager;
class FlakyCheckpointer;
} // namespace testing

namespace transaction {

class TransactionManager {
    friend class testing::DBTest;
    friend class testing::FlakyBufferManager;
    friend class testing::FlakyCheckpointer;

    using init_checkpointer_func_t =
        std::function<std::unique_ptr<storage::Checkpointer>(main::ClientContext&)>;
    static std::unique_ptr<storage::Checkpointer> initCheckpointer(
        main::ClientContext& clientContext);

public:
    // Timestamp starts from 1. 0 is reserved for the dummy system transaction.
    explicit TransactionManager(storage::WAL& wal)
        : wal{wal}, lastTransactionID{Transaction::START_TRANSACTION_ID}, lastTimestamp{1} {
        initCheckpointerFunc = initCheckpointer;
    }
    ~TransactionManager();

    Transaction* beginTransaction(main::ClientContext& clientContext, TransactionType type);

    void commit(main::ClientContext& clientContext, Transaction* transaction);
    void rollback(main::ClientContext& clientContext, Transaction* transaction);

    void checkpoint(main::ClientContext& clientContext);
    // Shuts down the background auto-checkpoint worker before database teardown.
    void shutdownAutoCheckpointWorker();
    // Returns the last background auto-checkpoint error, or an empty string if the last run
    // succeeded.
    std::string getLastAutoCheckpointErrorMessage();

    static TransactionManager* Get(const main::ClientContext& context);

private:
    void checkpointNoLock(main::ClientContext& clientContext);
    void scheduleAutoCheckpoint(main::ClientContext& clientContext);
    void runAutoCheckpointWorker(main::Database* database);
    bool shouldRunAutoCheckpoint(main::ClientContext& clientContext) const;
    void clearAutoCheckpointErrorMessage();
    void setAutoCheckpointErrorMessage(std::string errorMessage);

    // This function locks the mutex to stop new write transactions and waits until all active
    // write transactions leave the system. Read transactions are allowed to continue, as
    // checkpoint does not need to wait for snapshot-isolated readers.
    common::UniqLock stopNewWriteTransactionsAndWaitUntilAllWriteTransactionsLeave();

    bool hasActiveWriteTransactionNoLock() const {
        return activeWriteTransactionCount.load(std::memory_order_acquire) > 0;
    }

    void decrementActiveWriteTransactionCount();

    // Note: Used by DBTest::createDB only.
    void setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(uint64_t waitTimeInMicros) {
        checkpointWaitTimeoutInMicros = waitTimeInMicros;
    }

    void clearTransactionNoLock(common::transaction_t transactionID);
    void setInitCheckpointerFuncForTesting(init_checkpointer_func_t initFunc);

private:
    storage::WAL& wal;
    std::vector<std::unique_ptr<Transaction>> activeTransactions;
    common::transaction_t lastTransactionID;
    common::transaction_t lastTimestamp;
    // This mutex serializes begin/commit/rollback calls to protect activeTransactions.
    std::mutex mtxForSerializingPublicFunctionCalls;
    std::mutex mtxForStartingNewTransactions;
    // Prevents concurrent checkpoints. Separate from mtxForSerializingPublicFunctionCalls so
    // that active writers can commit/rollback while the checkpoint is draining them.
    std::mutex mtxForCheckpoint;
    // protects condition-variable waits for active write transactions to reach zero.
    std::mutex mtxForActiveWriteTransactions;
    std::condition_variable cvActiveWriteTransactionsChanged;
    // active write/recovery transaction count used by checkpoint drain waits without holding
    // mtxForSerializingPublicFunctionCalls, which would deadlock.
    std::atomic<uint32_t> activeWriteTransactionCount{0};
    uint64_t checkpointWaitTimeoutInMicros = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS;

    std::mutex mtxForAutoCheckpoint;
    std::condition_variable cvAutoCheckpoint;
    std::thread autoCheckpointWorker;
    bool autoCheckpointRequested = false;
    bool stopAutoCheckpointWorker = false;
    std::string lastAutoCheckpointErrorMessage;

    std::mutex mtxForInitCheckpointerFunc;
    init_checkpointer_func_t initCheckpointerFunc;
};
} // namespace transaction
} // namespace kuzu
