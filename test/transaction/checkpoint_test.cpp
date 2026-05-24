#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <limits>

#include "api_test/private_api_test.h"
#include "common/exception/runtime.h"
#include "main/connection.h"
#include "storage/checkpointer.h"
#include "storage/storage_manager.h"
#include "storage/wal/wal.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace testing {

class FlakyCheckpointer {
public:
    explicit FlakyCheckpointer(TransactionManager::init_checkpointer_func_t initFunc)
        : initFunc(std::move(initFunc)) {}

    void setCheckpointer(main::ClientContext& context) const {
        TransactionManager::Get(context)->setInitCheckpointerFuncForTesting(initFunc);
    }

private:
    TransactionManager::init_checkpointer_func_t initFunc;
};

class BlockingCheckpointState {
public:
    uint64_t markEntered() {
        uint64_t checkpointIdx;
        {
            std::lock_guard lck{mtx};
            checkpointIdx = ++enteredCount;
        }
        cv.notify_all();
        return checkpointIdx;
    }

    void release() {
        {
            std::lock_guard lck{mtx};
            releasedCount = std::numeric_limits<uint64_t>::max();
        }
        cv.notify_all();
    }

    void releaseNext() {
        {
            std::lock_guard lck{mtx};
            releasedCount++;
        }
        cv.notify_all();
    }

    void waitUntilReleased(uint64_t checkpointIdx) {
        std::unique_lock lck{mtx};
        cv.wait(lck, [&]() { return releasedCount >= checkpointIdx; });
    }

    void markFinished() {
        {
            std::lock_guard lck{mtx};
            finishedCount++;
        }
        cv.notify_all();
    }

    bool waitUntilEntered(std::chrono::seconds timeout) {
        return waitUntilEnteredCount(1, timeout);
    }

    bool waitUntilEnteredCount(uint64_t count, std::chrono::seconds timeout) {
        std::unique_lock lck{mtx};
        return cv.wait_for(lck, timeout, [&]() { return enteredCount >= count; });
    }

    bool waitUntilFinished(std::chrono::seconds timeout) {
        return waitUntilFinishedCount(1, timeout);
    }

    bool waitUntilFinishedCount(uint64_t count, std::chrono::seconds timeout) {
        std::unique_lock lck{mtx};
        return cv.wait_for(lck, timeout, [&]() { return finishedCount >= count; });
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    uint64_t enteredCount = 0;
    uint64_t releasedCount = 0;
    uint64_t finishedCount = 0;
};

class BlockingCheckpointReleaseGuard {
public:
    explicit BlockingCheckpointReleaseGuard(std::shared_ptr<BlockingCheckpointState> state)
        : state{std::move(state)} {}

    ~BlockingCheckpointReleaseGuard() {
        if (state) {
            state->release();
        }
    }

private:
    std::shared_ptr<BlockingCheckpointState> state;
};

class BlockingCheckpointer final : public Checkpointer {
public:
    BlockingCheckpointer(main::ClientContext& clientContext,
        std::shared_ptr<BlockingCheckpointState> state)
        : Checkpointer(clientContext), state{std::move(state)} {}

    bool checkpointStorage() override {
        const auto checkpointIdx = state->markEntered();
        state->waitUntilReleased(checkpointIdx);
        const auto result = Checkpointer::checkpointStorage();
        state->markFinished();
        return result;
    }

private:
    std::shared_ptr<BlockingCheckpointState> state;
};

class FlakyCheckpointerTest : public PrivateApiTest {
public:
    std::string getInputDir() override { return "empty"; }

    void runFlakyCheckpoint(const FlakyCheckpointer& flakyCheckpointer) {
        conn->query("CALL force_checkpoint_on_close=false;");
        conn->query("CALL auto_checkpoint=false");
        conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
        for (auto i = 0; i < 5000; i++) {
            conn->query(stringFormat("CREATE (a:test {id: {}, name: 'name_{}'});", i, i));
        }
        auto context = getClientContext(*conn);
        flakyCheckpointer.setCheckpointer(*context);
        auto res = conn->query("CHECKPOINT;");
        ASSERT_FALSE(res->isSuccess());
    }

    void runTest(const FlakyCheckpointer& flakyCheckpointer) {
        runFlakyCheckpoint(flakyCheckpointer);
        createDBAndConn();
        auto res = conn->query("MATCH (a:test) RETURN COUNT(a);");
        ASSERT_TRUE(res->isSuccess());
        ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    }
};

class FlakyCheckpointerFailsOnCheckpointStorage final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnCheckpointStorage(main::ClientContext& clientContext)
        : Checkpointer(clientContext) {}

    bool checkpointStorage() override { throw RuntimeException("checkpoint failed."); }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointStorageFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnCheckpointStorage>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

TEST_F(FlakyCheckpointerTest, AutoCheckpointRunsInBackground) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=true;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL checkpoint_threshold=1;")->isSuccess());

    auto state = std::make_shared<BlockingCheckpointState>();
    auto initBlockingCheckpointer = [state](main::ClientContext& context) {
        return std::make_unique<BlockingCheckpointer>(context, state);
    };
    FlakyCheckpointer blockingCheckpointer(initBlockingCheckpointer);
    blockingCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto queryFuture = std::async(std::launch::async,
        [&]() { return conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);"); });
    BlockingCheckpointReleaseGuard releaseGuard{state};

    const auto queryStatus = queryFuture.wait_for(std::chrono::seconds(5));
    if (queryStatus != std::future_status::ready) {
        state->release();
        FAIL() << "auto-checkpoint blocked the committing query";
    }
    auto result = queryFuture.get();
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();

    if (!state->waitUntilEntered(std::chrono::seconds(5))) {
        state->release();
        FAIL() << "auto-checkpoint was not scheduled";
    }
    state->release();
    ASSERT_TRUE(state->waitUntilFinished(std::chrono::seconds(5)));
}

TEST_F(FlakyCheckpointerTest, AutoCheckpointWaitsForActiveCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL checkpoint_threshold=1;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE seed(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=true;")->isSuccess());

    auto state = std::make_shared<BlockingCheckpointState>();
    auto initBlockingCheckpointer = [state](main::ClientContext& context) {
        return std::make_unique<BlockingCheckpointer>(context, state);
    };
    FlakyCheckpointer blockingCheckpointer(initBlockingCheckpointer);
    blockingCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto manualCheckpointFuture =
        std::async(std::launch::async, [&]() { return conn->query("CHECKPOINT;"); });
    BlockingCheckpointReleaseGuard releaseGuard{state};
    ASSERT_TRUE(state->waitUntilEnteredCount(1, std::chrono::seconds(5)));

    auto writerConn = std::make_unique<main::Connection>(database.get());
    auto writeResult = writerConn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);");
    ASSERT_TRUE(writeResult->isSuccess()) << writeResult->getErrorMessage();

    state->releaseNext();
    ASSERT_TRUE(state->waitUntilEnteredCount(2, std::chrono::seconds(5)));
    state->releaseNext();
    ASSERT_TRUE(state->waitUntilFinishedCount(2, std::chrono::seconds(5)));

    ASSERT_EQ(manualCheckpointFuture.wait_for(std::chrono::seconds(5)),
        std::future_status::ready);
    auto manualCheckpointResult = manualCheckpointFuture.get();
    ASSERT_TRUE(manualCheckpointResult->isSuccess())
        << manualCheckpointResult->getErrorMessage();
}

class FlakyCheckpointerFailsOnSerialization final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnSerialization(main::ClientContext& context)
        : Checkpointer(context) {}

    void serializeCatalogAndMetadata(DatabaseHeader&, bool) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointSerializeFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnWritingHeader final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnWritingHeader(main::ClientContext& context)
        : Checkpointer(context) {}

    void writeDatabaseHeader(const DatabaseHeader&) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointWriteHeaderFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnWritingHeader>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnFlushingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnFlushingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool /*walRotated*/) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointFlushingShadowFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnFlushingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnLoggingCheckpoint final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnLoggingCheckpoint(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool /*walRotated*/) override {
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointLoggingCheckpointFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnLoggingCheckpoint>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnApplyingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnApplyingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        auto wal = WAL::Get(clientContext);
        if (walRotated) {
            wal->logAndFlushCheckpointToFrozen(&clientContext);
        } else {
            wal->logAndFlushCheckpoint(&clientContext);
        }
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointApplyingShadowFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnApplyingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnClearingFiles final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnClearingFiles(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
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
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointClearingFilesFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

// Simulates a situation where a database attempts to replay a shadow file from an older database
// with the same path
TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchExistingDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // Temporarily rename the shadow file and frozen wal file.
    // With WAL rotation, the active .wal is renamed to .wal.checkpoint during checkpoint,
    // so the frozen WAL is what survives after a failed checkpoint.
    auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    auto frozenWalFilePath = StorageUtils::getCheckpointWALFilePath(databasePath);
    auto tmpShadowFilePath = shadowFilePath + "1";
    auto tmpFrozenWalFilePath = frozenWalFilePath + "1";
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
    ASSERT_TRUE(std::filesystem::exists(frozenWalFilePath));
    std::filesystem::rename(shadowFilePath, tmpShadowFilePath);
    std::filesystem::rename(frozenWalFilePath, tmpFrozenWalFilePath);

    // Recreate a new DB with the same path as before
    createDBAndConn();
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");

    // Close the DB
    conn.reset();
    database.reset();

    // Rename the files to the original names
    std::filesystem::rename(tmpShadowFilePath, shadowFilePath);
    std::filesystem::rename(tmpFrozenWalFilePath, frozenWalFilePath);

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchNewDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchCorruptedDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // Create a new DB file and write garbage bytes to it
    std::ofstream ofs(databasePath);
    ofs << "1a1a1a1a1a1a1a1a1a1a";
    ofs.close();

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), InternalException);
}

} // namespace testing
} // namespace kuzu
