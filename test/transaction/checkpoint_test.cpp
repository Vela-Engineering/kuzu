#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <limits>

#include "api_test/private_api_test.h"
#include "common/exception/runtime.h"
#include "main/connection.h"
#include "storage/buffer_manager/memory_manager.h"
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

class BlockingCheckpointerFailsOnCheckpointStorage final : public Checkpointer {
public:
    BlockingCheckpointerFailsOnCheckpointStorage(main::ClientContext& clientContext,
        std::shared_ptr<BlockingCheckpointState> state)
        : Checkpointer(clientContext), state{std::move(state)} {}

    bool checkpointStorage() override {
        const auto checkpointIdx = state->markEntered();
        state->waitUntilReleased(checkpointIdx);
        state->markFinished();
        throw RuntimeException("checkpoint failed.");
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
    auto writerFuture = std::async(std::launch::async,
        [&]() { return writerConn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);"); });
    ASSERT_EQ(writerFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    state->releaseNext();
    ASSERT_EQ(manualCheckpointFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto manualCheckpointResult = manualCheckpointFuture.get();
    ASSERT_TRUE(manualCheckpointResult->isSuccess()) << manualCheckpointResult->getErrorMessage();
    ASSERT_EQ(writerFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto writeResult = writerFuture.get();
    ASSERT_TRUE(writeResult->isSuccess()) << writeResult->getErrorMessage();

    ASSERT_TRUE(state->waitUntilEnteredCount(2, std::chrono::seconds(5)));
    state->releaseNext();
    ASSERT_TRUE(state->waitUntilFinishedCount(2, std::chrono::seconds(5)));
}

TEST_F(FlakyCheckpointerTest, CheckpointWaitsForActiveReadTransaction) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 1});")->isSuccess());

    auto state = std::make_shared<BlockingCheckpointState>();
    auto initBlockingCheckpointer = [state](main::ClientContext& context) {
        return std::make_unique<BlockingCheckpointer>(context, state);
    };
    FlakyCheckpointer blockingCheckpointer(initBlockingCheckpointer);
    blockingCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto readConn = std::make_unique<main::Connection>(database.get());
    auto checkpointConn = std::make_unique<main::Connection>(database.get());
    ASSERT_TRUE(readConn->query("BEGIN TRANSACTION READ ONLY;")->isSuccess());
    auto readResult = readConn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(readResult->isSuccess()) << readResult->getErrorMessage();

    auto checkpointFuture =
        std::async(std::launch::async, [&]() { return checkpointConn->query("CHECKPOINT;"); });
    BlockingCheckpointReleaseGuard releaseGuard{state};
    ASSERT_EQ(checkpointFuture.wait_for(std::chrono::milliseconds(100)),
        std::future_status::timeout);
    ASSERT_FALSE(state->waitUntilEnteredCount(1, std::chrono::seconds(0)));

    ASSERT_TRUE(readConn->query("COMMIT;")->isSuccess());
    ASSERT_TRUE(state->waitUntilEntered(std::chrono::seconds(5)));
    state->releaseNext();
    ASSERT_EQ(checkpointFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto checkpointResult = checkpointFuture.get();
    ASSERT_TRUE(checkpointResult->isSuccess()) << checkpointResult->getErrorMessage();
}

TEST_F(FlakyCheckpointerTest, RecoverConcurrentWriterAfterFailedCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
    ASSERT_TRUE(conn->query(
        "CREATE REL TABLE related(FROM test TO test, since INT64, MANY_MANY);")
                    ->isSuccess());
    for (auto i = 0; i < 5000; i++) {
        auto result = conn->query(stringFormat("CREATE (a:test {id: {}, name: 'name_{}'});", i, i));
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    }

    auto state = std::make_shared<BlockingCheckpointState>();
    auto initBlockingCheckpointer = [state](main::ClientContext& context) {
        return std::make_unique<BlockingCheckpointerFailsOnCheckpointStorage>(context, state);
    };
    FlakyCheckpointer blockingCheckpointer(initBlockingCheckpointer);
    blockingCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto checkpointFuture =
        std::async(std::launch::async, [&]() { return conn->query("CHECKPOINT;"); });
    BlockingCheckpointReleaseGuard releaseGuard{state};
    ASSERT_TRUE(state->waitUntilEntered(std::chrono::seconds(5)));

    auto writerConn = std::make_unique<main::Connection>(database.get());
    auto writerFuture = std::async(std::launch::async, [&]() {
        return writerConn->query(
            "MATCH (a:test) WHERE a.id = 0 "
            "CREATE (b:test {id: 5000, name: 'concurrent'}), "
            "(a)-[:related {since: 2026}]->(b);");
    });
    ASSERT_EQ(writerFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    state->release();
    ASSERT_TRUE(state->waitUntilFinished(std::chrono::seconds(5)));
    ASSERT_EQ(checkpointFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto checkpointResult = checkpointFuture.get();
    ASSERT_FALSE(checkpointResult->isSuccess());
    ASSERT_EQ(writerFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto writeResult = writerFuture.get();
    ASSERT_TRUE(writeResult->isSuccess()) << writeResult->getErrorMessage();

    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(activeWalPath));
    const auto frozenWalSize = std::filesystem::file_size(frozenWalPath);
    auto retryCheckpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(retryCheckpointResult->isSuccess());
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_EQ(std::filesystem::file_size(frozenWalPath), frozenWalSize);

    writeResult.reset();
    checkpointResult.reset();
    retryCheckpointResult.reset();
    writerConn.reset();
    conn.reset();
    database.reset();

    createDBAndConn();
    auto verifyRecoveredData = [&]() {
        auto countResult = conn->query("MATCH (a:test) RETURN COUNT(a);");
        ASSERT_TRUE(countResult->isSuccess()) << countResult->getErrorMessage();
        ASSERT_EQ(countResult->getNext()->getValue(0)->getValue<int64_t>(), 5001);
        auto relResult = conn->query(
            "MATCH (src:test)-[r:related]->(dst:test) "
            "RETURN src.id, dst.id, dst.name, r.since;");
        ASSERT_TRUE(relResult->isSuccess()) << relResult->getErrorMessage();
        ASSERT_EQ(relResult->getNumTuples(), 1);
        const auto tuple = relResult->getNext();
        ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 0);
        ASSERT_EQ(tuple->getValue(1)->getValue<int64_t>(), 5000);
        ASSERT_EQ(tuple->getValue(2)->getValue<std::string>(), "concurrent");
        ASSERT_EQ(tuple->getValue(3)->getValue<int64_t>(), 2026);
    };
    verifyRecoveredData();
    ASSERT_FALSE(std::filesystem::exists(frozenWalPath));
    ASSERT_FALSE(std::filesystem::exists(activeWalPath));

    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    conn.reset();
    database.reset();
    createDBAndConn();
    verifyRecoveredData();
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

TEST_F(FlakyCheckpointerTest, RecoverWriterAfterFailedNonRotatedCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 0});")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnApplyingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    ASSERT_FALSE(std::filesystem::exists(activeWalPath));

    auto writeResult = conn->query("CREATE (:test {id: 1});");
    ASSERT_TRUE(writeResult->isSuccess()) << writeResult->getErrorMessage();
    ASSERT_TRUE(std::filesystem::exists(activeWalPath));

    writeResult.reset();
    checkpointResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();

    auto result = conn->query("MATCH (n:test) RETURN n.id ORDER BY n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNumTuples(), 2);
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 0);
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
}

class FlakyCheckpointerFailsAfterClearingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsAfterClearingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        if (!walRotated) {
            throw RuntimeException("expected a rotated WAL.");
        }
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        WAL::Get(clientContext)->logAndFlushCheckpointToFrozen(&clientContext);
        markShadowApplicationStarted();
        shadowFile.applyShadowPages(clientContext);
        shadowFile.clear(*MemoryManager::Get(clientContext)->getBufferManager());
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointMarkerWithClearedShadow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsAfterClearingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsAfterRemovingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsAfterRemovingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        if (!walRotated) {
            throw RuntimeException("expected a rotated WAL.");
        }
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        WAL::Get(clientContext)->logAndFlushCheckpointToFrozen(&clientContext);
        markShadowApplicationStarted();
        shadowFile.applyShadowPages(clientContext);
        shadowFile.clear(*MemoryManager::Get(clientContext)->getBufferManager());
        shadowFile.reset();
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointMarkerWithMissingShadow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsAfterRemovingShadow>(context);
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
        markShadowApplicationStarted();
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

TEST_F(FlakyCheckpointerTest, FailedAppliedCheckpointRequiresRestart) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 0});")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    auto queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_FALSE(queryResult->isSuccess());
    ASSERT_NE(queryResult->getErrorMessage().find("must be restarted"), std::string::npos);

    checkpointResult.reset();
    queryResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();

    auto result = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
}

TEST_F(FlakyCheckpointerTest, CheckpointPreservesUntouchedCSRRegions) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE person(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE REL TABLE knows(FROM person TO person, MANY_MANY);")->isSuccess());
    ASSERT_TRUE(conn->query("UNWIND RANGE(0, 1024) AS id CREATE (:person {id: id});")->isSuccess());
    ASSERT_TRUE(conn->query(
        "MATCH (a:person), (b:person) WHERE a.id IN [0, 1024] AND b.id = 1 "
        "CREATE (a)-[:knows]->(b);")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    ASSERT_TRUE(conn->query(
        "MATCH (a:person)-[r:knows]->(:person) WHERE a.id = 0 DELETE r;")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());

    auto verifyUntouchedRegion = [&]() {
        auto deletedResult = conn->query(
            "MATCH (a:person)-[r:knows]->(:person) WHERE a.id = 0 RETURN COUNT(r);");
        ASSERT_TRUE(deletedResult->isSuccess()) << deletedResult->getErrorMessage();
        ASSERT_EQ(deletedResult->getNext()->getValue(0)->getValue<int64_t>(), 0);
        auto result = conn->query(
            "MATCH (a:person)-[r:knows]->(b:person) WHERE a.id = 1024 "
            "RETURN a.id, b.id;");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_EQ(result->getNumTuples(), 1);
        const auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 1024);
        ASSERT_EQ(tuple->getValue(1)->getValue<int64_t>(), 1);
    };
    verifyUntouchedRegion();

    conn.reset();
    database.reset();
    createDBAndConn();
    verifyUntouchedRegion();
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
