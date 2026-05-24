#include "transaction/transaction_manager.h"

#include "common/exception/checkpoint.h"
#include "common/exception/transaction_manager.h"
#include "main/attached_database.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/db_config.h"
#include "storage/checkpointer.h"
#include "storage/wal/local_wal.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace transaction {

TransactionManager::~TransactionManager() {
    shutdownAutoCheckpointWorker();
}

Transaction* TransactionManager::beginTransaction(main::ClientContext& clientContext,
    TransactionType type) {
    // only acquire the write gate for write/recovery transactions. Read-only transactions
    // can start freely during checkpoint since they use snapshot isolation.
    std::unique_lock newTransactionLck{mtxForStartingNewTransactions, std::defer_lock};
    if (type != TransactionType::READ_ONLY) {
        newTransactionLck.lock();
    }
    std::unique_lock publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    switch (type) {
    case TransactionType::READ_ONLY: {
        auto transaction =
            std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
        activeTransactions.push_back(std::move(transaction));
        return activeTransactions.back().get();
    }
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        if (!clientContext.getDBConfig()->experimentalConcurrentWrites &&
            hasActiveWriteTransactionNoLock()) {
            throw TransactionManagerException(
                "Cannot start a new write transaction in the system. "
                "Only one write transaction at a time is allowed in the system.");
        }
        auto transaction =
            std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
        if (transaction->shouldLogToWAL()) {
            transaction->getLocalWAL().logBeginTransaction();
        }
        activeTransactions.push_back(std::move(transaction));
        activeWriteTransactionCount.fetch_add(1, std::memory_order_release);
        return activeTransactions.back().get();
    }
        // LCOV_EXCL_START
    default: {
        throw TransactionManagerException("Invalid transaction type to begin transaction.");
    }
        // LCOV_EXCL_STOP
    }
}

void TransactionManager::commit(main::ClientContext& clientContext, Transaction* transaction) {
    bool shouldForceCheckpoint = false;
    bool shouldAutoCheckpoint = false;
    {
        std::unique_lock lck{mtxForSerializingPublicFunctionCalls};
        clientContext.cleanUp();
        switch (transaction->getType()) {
        case TransactionType::READ_ONLY: {
            clearTransactionNoLock(transaction->getID());
        } break;
        case TransactionType::RECOVERY:
        case TransactionType::WRITE: {
            lastTimestamp++;
            transaction->commitTS = lastTimestamp;
            transaction->commit(&wal);
            shouldForceCheckpoint = transaction->shouldForceCheckpoint();
            if (!shouldForceCheckpoint) {
                shouldAutoCheckpoint = Checkpointer::canAutoCheckpoint(clientContext, *transaction);
            }
            clearTransactionNoLock(transaction->getID());
            decrementActiveWriteTransactionCount();
        } break;
            // LCOV_EXCL_START
        default: {
            throw TransactionManagerException("Invalid transaction type to commit.");
        }
            // LCOV_EXCL_STOP
        }
    }
    // Checkpoint outside the public function lock so active writers can finish
    // (commit/rollback) during the drain phase instead of deadlocking.
    if (shouldForceCheckpoint) {
        checkpoint(clientContext);
    } else if (shouldAutoCheckpoint) {
        scheduleAutoCheckpoint(clientContext);
    }
}

// Note: We take in additional `transaction` here is due to that `transactionContext` might be
// destructed when a transaction throws an exception, while we need to roll back the active
// transaction still.
void TransactionManager::rollback(main::ClientContext& clientContext, Transaction* transaction) {
    std::unique_lock lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUp();
    switch (transaction->getType()) {
    case TransactionType::READ_ONLY: {
        clearTransactionNoLock(transaction->getID());
    } break;
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        transaction->rollback(&wal);
        clearTransactionNoLock(transaction->getID());
        decrementActiveWriteTransactionCount();
    } break;
    default: {
        throw TransactionManagerException("Invalid transaction type to rollback.");
    }
    }
}

void TransactionManager::checkpoint(main::ClientContext& clientContext) {
    if (clientContext.isInMemory()) {
        return;
    }
    // Use the dedicated checkpoint mutex so active writers can still commit/rollback
    // during the drain phase (they only need mtxForSerializingPublicFunctionCalls).
    std::unique_lock checkpointLck{mtxForCheckpoint};
    checkpointNoLock(clientContext);
}

TransactionManager* TransactionManager::Get(const main::ClientContext& context) {
    if (context.getAttachedDatabase() != nullptr) {
        return context.getAttachedDatabase()->getTransactionManager();
    }
    return context.getDatabase()->getTransactionManager();
}

UniqLock TransactionManager::stopNewWriteTransactionsAndWaitUntilAllWriteTransactionsLeave() {
    UniqLock startTransactionLock{mtxForStartingNewTransactions};
    std::unique_lock activeWriteTransactionsLck{mtxForActiveWriteTransactions};
    const auto timeout = std::chrono::microseconds(checkpointWaitTimeoutInMicros);
    if (!cvActiveWriteTransactionsChanged.wait_for(activeWriteTransactionsLck, timeout,
            [&]() { return !hasActiveWriteTransactionNoLock(); })) {
        throw TransactionManagerException(
            "Timeout waiting for active write transactions to leave the system before "
            "checkpointing. If you have an open write transaction, please close it and "
            "try again.");
    }
    return startTransactionLock;
}

void TransactionManager::decrementActiveWriteTransactionCount() {
    if (activeWriteTransactionCount.fetch_sub(1, std::memory_order_release) == 1) {
        std::lock_guard activeWriteTransactionsLck{mtxForActiveWriteTransactions};
        cvActiveWriteTransactionsChanged.notify_all();
    }
}

void TransactionManager::clearTransactionNoLock(transaction_t transactionID) {
    KU_ASSERT(std::ranges::any_of(activeTransactions.begin(), activeTransactions.end(),
        [transactionID](const auto& activeTransaction) {
            return activeTransaction->getID() == transactionID;
        }));
    std::erase_if(activeTransactions, [transactionID](const auto& activeTransaction) {
        return activeTransaction->getID() == transactionID;
    });
}

std::unique_ptr<Checkpointer> TransactionManager::initCheckpointer(
    main::ClientContext& clientContext) {
    return std::make_unique<Checkpointer>(clientContext);
}

void TransactionManager::setInitCheckpointerFuncForTesting(init_checkpointer_func_t initFunc) {
    std::lock_guard lck{mtxForInitCheckpointerFunc};
    initCheckpointerFunc = std::move(initFunc);
}

void TransactionManager::scheduleAutoCheckpoint(main::ClientContext& clientContext) {
    if (clientContext.getAttachedDatabase() != nullptr) {
        checkpoint(clientContext);
        return;
    }
    std::unique_lock lck{mtxForAutoCheckpoint};
    if (stopAutoCheckpointWorker) {
        return;
    }
    autoCheckpointRequested = true;
    if (!autoCheckpointWorker.joinable()) {
        auto database = clientContext.getDatabase();
        autoCheckpointWorker = std::thread(
            [this, database]() { runAutoCheckpointWorker(database); });
    }
    lck.unlock();
    cvAutoCheckpoint.notify_one();
}

void TransactionManager::runAutoCheckpointWorker(main::Database* database) {
    while (true) {
        {
            std::unique_lock lck{mtxForAutoCheckpoint};
            cvAutoCheckpoint.wait(lck,
                [&]() { return autoCheckpointRequested || stopAutoCheckpointWorker; });
            if (stopAutoCheckpointWorker) {
                return;
            }
            autoCheckpointRequested = false;
        }
        try {
            main::ClientContext checkpointContext(database);
            if (shouldRunAutoCheckpoint(checkpointContext)) {
                std::unique_lock checkpointLck{mtxForCheckpoint};
                if (shouldRunAutoCheckpoint(checkpointContext)) {
                    checkpointNoLock(checkpointContext);
                }
            }
            clearAutoCheckpointErrorMessage();
        } catch (std::exception& e) {
            setAutoCheckpointErrorMessage(e.what());
        } catch (...) {
            setAutoCheckpointErrorMessage("Unknown auto-checkpoint failure.");
        }
    }
}

bool TransactionManager::shouldRunAutoCheckpoint(main::ClientContext& clientContext) const {
    if (clientContext.isInMemory()) {
        return false;
    }
    if (!clientContext.getDBConfig()->autoCheckpoint) {
        return false;
    }
    return WAL::Get(clientContext)->getFileSize() >
           clientContext.getDBConfig()->checkpointThreshold;
}

void TransactionManager::clearAutoCheckpointErrorMessage() {
    std::lock_guard lck{mtxForAutoCheckpoint};
    lastAutoCheckpointErrorMessage.clear();
}

void TransactionManager::setAutoCheckpointErrorMessage(std::string errorMessage) {
    std::lock_guard lck{mtxForAutoCheckpoint};
    lastAutoCheckpointErrorMessage = std::move(errorMessage);
}

std::string TransactionManager::getLastAutoCheckpointErrorMessage() {
    std::lock_guard lck{mtxForAutoCheckpoint};
    return lastAutoCheckpointErrorMessage;
}

void TransactionManager::shutdownAutoCheckpointWorker() {
    std::thread worker;
    {
        std::lock_guard lck{mtxForAutoCheckpoint};
        stopAutoCheckpointWorker = true;
        autoCheckpointRequested = false;
        worker = std::move(autoCheckpointWorker);
    }
    cvAutoCheckpoint.notify_one();
    if (worker.joinable()) {
        worker.join();
    }
}

void TransactionManager::checkpointNoLock(main::ClientContext& clientContext) {
    // We only need to wait for active write transactions to leave the system before
    // checkpointing. Read transactions can continue safely because:
    // 1. Readers use snapshot isolation (MVCC) and only see data committed before their startTS.
    // 2. Shadow pages are applied with per-page locking, so concurrent optimistic readers will
    //    detect the version change and retry their read with the updated page data.
    // 3. The checkpoint only materializes already-committed data, which readers either already
    //    see (if committed before their startTS) or correctly skip (if committed after).
    UniqLock writeGate;
    try {
        writeGate = stopNewWriteTransactionsAndWaitUntilAllWriteTransactionsLeave();
    } catch (std::exception& e) {
        throw CheckpointException{e};
    }
    init_checkpointer_func_t initFunc;
    {
        std::lock_guard initFuncLck{mtxForInitCheckpointerFunc};
        initFunc = initCheckpointerFunc;
    }
    auto checkpointer = initFunc(clientContext);
    try {
        checkpointer->beginCheckpoint(lastTimestamp);
    } catch (std::exception& e) {
        checkpointer->rollback();
        throw CheckpointException{e};
    }
    // Release the write gate early when WAL was rotated (common case). New writers
    // create a fresh active WAL, isolated from the frozen checkpoint WAL. When WAL
    // was not rotated (no WAL existed), keep the gate to prevent WAL races.
    if (checkpointer->wasWalRotated()) {
        writeGate = {};
    }
    // Storage materialization runs after the gate is released. Writers can start new
    // transactions while tables are being checkpointed. Per-node-group locks provide
    // fine-grained mutual exclusion, and the snapshot transaction ensures a consistent
    // MVCC view of version chains.
    try {
        checkpointer->checkpointStoragePhase();
    } catch (std::exception& e) {
        checkpointer->rollback();
        throw CheckpointException{e};
    }
    try {
        checkpointer->finishCheckpoint();
    } catch (std::exception& e) {
        checkpointer->rollback();
        throw CheckpointException{e};
    }
    writeGate = {};
    checkpointer->postCheckpointCleanup();
}

} // namespace transaction
} // namespace kuzu
