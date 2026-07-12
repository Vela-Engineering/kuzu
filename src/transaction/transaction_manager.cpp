#include "transaction/transaction_manager.h"

#include "common/exception/checkpoint.h"
#include "common/exception/commit.h"
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
    std::unique_lock newTransactionLck{mtxForStartingNewTransactions};
    std::unique_lock publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    if (checkpointRecoveryRequired) {
        throw TransactionManagerException(
            "The database must be restarted to recover from an interrupted checkpoint.");
    }
    if (commitRecoveryRequired) {
        throw TransactionManagerException(
            "The database must be restarted to recover from a failed commit.");
    }
    switch (type) {
    case TransactionType::READ_ONLY: {
        auto transaction =
            std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
        activeTransactions.push_back(std::move(transaction));
        activeTransactionCount.fetch_add(1, std::memory_order_release);
        return activeTransactions.back().get();
    }
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        if (!clientContext.getDBConfig()->allowConcurrentWrites() &&
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
        activeTransactionCount.fetch_add(1, std::memory_order_release);
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
            if (commitRecoveryRequired) {
                throw TransactionManagerException(
                    "The database must be restarted to recover from a failed commit.");
            }
            lastTimestamp++;
            transaction->commitTS = lastTimestamp;
            try {
                transaction->commit(&wal, commitPublicationHookForTesting);
            } catch (WALCommitException&) {
                commitRecoveryRequired = true;
                clearTransactionNoLock(transaction->getID());
                decrementActiveWriteTransactionCount();
                throw;
            } catch (FatalCommitException&) {
                commitRecoveryRequired = true;
                clearTransactionNoLock(transaction->getID());
                decrementActiveWriteTransactionCount();
                throw;
            }
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

UniqLock TransactionManager::stopNewTransactionsAndWaitUntilAllTransactionsLeave() {
    UniqLock startTransactionLock{mtxForStartingNewTransactions};
    std::unique_lock activeTransactionsLck{mtxForActiveTransactions};
    const auto timeout = std::chrono::microseconds(checkpointWaitTimeoutInMicros);
    if (!cvActiveTransactionsChanged.wait_for(activeTransactionsLck, timeout,
            [&]() { return activeTransactionCount.load(std::memory_order_acquire) == 0; })) {
        throw TransactionManagerException(
            "Timeout waiting for active transactions to leave the system before "
            "checkpointing. If you have an open transaction, please close it and "
            "try again.");
    }
    return startTransactionLock;
}

void TransactionManager::decrementActiveWriteTransactionCount() {
    activeWriteTransactionCount.fetch_sub(1, std::memory_order_release);
}

void TransactionManager::decrementActiveTransactionCount() {
    if (activeTransactionCount.fetch_sub(1, std::memory_order_release) == 1) {
        std::lock_guard activeTransactionsLck{mtxForActiveTransactions};
        cvActiveTransactionsChanged.notify_all();
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
    decrementActiveTransactionCount();
}

std::unique_ptr<Checkpointer> TransactionManager::initCheckpointer(
    main::ClientContext& clientContext) {
    return std::make_unique<Checkpointer>(clientContext);
}

void TransactionManager::setInitCheckpointerFuncForTesting(init_checkpointer_func_t initFunc) {
    std::lock_guard lck{mtxForInitCheckpointerFunc};
    initCheckpointerFunc = std::move(initFunc);
}

void TransactionManager::setCommitPublicationHookForTesting(std::function<void()> hook) {
    std::lock_guard lck{mtxForSerializingPublicFunctionCalls};
    commitPublicationHookForTesting = std::move(hook);
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
        autoCheckpointWorker =
            std::thread([this, database]() { runAutoCheckpointWorker(database); });
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
    auto wal = WAL::Get(clientContext);
    return !wal->hasFrozenWAL() &&
           wal->getFileSize() > clientContext.getDBConfig()->checkpointThreshold;
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
    UniqLock transactionGate;
    try {
        transactionGate = stopNewTransactionsAndWaitUntilAllTransactionsLeave();
    } catch (std::exception& e) {
        throw CheckpointException{e};
    }
    if (checkpointRecoveryRequired) {
        throw CheckpointException{TransactionManagerException(
            "The database must be restarted to recover from an interrupted checkpoint.")};
    }
    if (commitRecoveryRequired) {
        throw CheckpointException{TransactionManagerException(
            "The database must be restarted to recover from a failed commit.")};
    }
    init_checkpointer_func_t initFunc;
    {
        std::lock_guard initFuncLck{mtxForInitCheckpointerFunc};
        initFunc = initCheckpointerFunc;
    }
    auto checkpointer = initFunc(clientContext);
    const auto rollbackCheckpoint = [&]() {
        try {
            checkpointer->rollback();
        } catch (std::exception& e) {
            checkpointRecoveryRequired = true;
            throw CheckpointException{e};
        }
    };
    try {
        checkpointer->beginCheckpoint(lastTimestamp);
    } catch (std::exception& e) {
        if (checkpointer->wasWalRotationStarted() ||
            checkpointer->wasCheckpointBeginWriteStarted()) {
            checkpointRecoveryRequired = true;
        }
        rollbackCheckpoint();
        throw CheckpointException{e};
    }
    try {
        checkpointer->checkpointStoragePhase();
    } catch (std::exception& e) {
        checkpointRecoveryRequired = true;
        rollbackCheckpoint();
        throw CheckpointException{e};
    }
    try {
        checkpointer->finishCheckpoint();
    } catch (std::exception& e) {
        if (checkpointer->wasCheckpointMarkerWritten()) {
            try {
                checkpointer->retryShadowApplication();
                checkpointer->postCheckpointCleanup();
            } catch (std::exception& recoveryException) {
                checkpointRecoveryRequired = true;
                const auto message = std::string{e.what()} +
                                     " In-process checkpoint recovery failed: " +
                                     recoveryException.what();
                throw CheckpointException{Exception{message}};
            }
            throw CheckpointException{e};
        }
        checkpointRecoveryRequired = true;
        rollbackCheckpoint();
        throw CheckpointException{e};
    }
    try {
        checkpointer->postCheckpointCleanup();
    } catch (std::exception& e) {
        checkpointRecoveryRequired = true;
        throw CheckpointException{e};
    }
}

} // namespace transaction
} // namespace kuzu
