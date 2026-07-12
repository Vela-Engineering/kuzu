#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <limits>

#include "api_test/private_api_test.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/serializer.h"
#include "main/connection.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/checkpointer.h"
#include "storage/file_db_id_utils.h"
#include "storage/storage_manager.h"
#include "storage/wal/wal.h"
#include "test_runner/fsm_leak_checker.h"
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

class BlockingPostCheckpointCleanup final : public Checkpointer {
public:
    BlockingPostCheckpointCleanup(main::ClientContext& clientContext,
        std::shared_ptr<BlockingCheckpointState> state)
        : Checkpointer(clientContext), state{std::move(state)} {}

    void postCheckpointCleanup() override {
        const auto checkpointIdx = state->markEntered();
        state->waitUntilReleased(checkpointIdx);
        Checkpointer::postCheckpointCleanup();
    }

private:
    std::shared_ptr<BlockingCheckpointState> state;
};

class FlakyCheckpointerTest : public PrivateApiTest {
public:
    std::string getInputDir() override { return "empty"; }

    ShadowFileHeader readShadowHeader() const {
        const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
        std::ifstream shadowFile(shadowFilePath, std::ios::binary);
        EXPECT_TRUE(shadowFile.is_open());
        ShadowFileHeader header{};
        shadowFile.read(reinterpret_cast<char*>(&header), sizeof(header));
        EXPECT_TRUE(shadowFile.good());
        return header;
    }

    void writeShadowHeader(const ShadowFileHeader& header) const {
        const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
        std::fstream shadowFile(shadowFilePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(shadowFile.is_open());
        shadowFile.seekp(0);
        shadowFile.write(reinterpret_cast<const char*>(&header), sizeof(header));
        shadowFile.flush();
        ASSERT_TRUE(shadowFile.good());
    }

    ShadowPageRecord readShadowRecord(uint64_t recordIdx) const {
        const auto header = readShadowHeader();
        EXPECT_LT(recordIdx, header.numShadowPages);
        const auto recordOffset =
            (static_cast<uint64_t>(header.numShadowPages) + 1) * KUZU_PAGE_SIZE +
            sizeof(uint64_t) +
            recordIdx * (sizeof(file_idx_t) + 2 * sizeof(page_idx_t));
        const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
        std::ifstream shadowFile(shadowFilePath, std::ios::binary);
        EXPECT_TRUE(shadowFile.is_open());
        shadowFile.seekg(recordOffset);
        ShadowPageRecord record;
        shadowFile.read(reinterpret_cast<char*>(&record.originalFileIdx), sizeof(file_idx_t));
        shadowFile.read(reinterpret_cast<char*>(&record.originalPageIdx), sizeof(page_idx_t));
        shadowFile.read(reinterpret_cast<char*>(&record.shadowPageIdx), sizeof(page_idx_t));
        EXPECT_TRUE(shadowFile.good());
        return record;
    }

    void writeShadowRecord(uint64_t recordIdx, const ShadowPageRecord& record) const {
        const auto header = readShadowHeader();
        ASSERT_LT(recordIdx, header.numShadowPages);
        const auto recordOffset =
            (static_cast<uint64_t>(header.numShadowPages) + 1) * KUZU_PAGE_SIZE +
            sizeof(uint64_t) +
            recordIdx * (sizeof(file_idx_t) + 2 * sizeof(page_idx_t));
        const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
        std::fstream shadowFile(shadowFilePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(shadowFile.is_open());
        shadowFile.seekp(recordOffset);
        shadowFile.write(
            reinterpret_cast<const char*>(&record.originalFileIdx), sizeof(file_idx_t));
        shadowFile.write(
            reinterpret_cast<const char*>(&record.originalPageIdx), sizeof(page_idx_t));
        shadowFile.write(
            reinterpret_cast<const char*>(&record.shadowPageIdx), sizeof(page_idx_t));
        shadowFile.flush();
        ASSERT_TRUE(shadowFile.good());
    }

    std::string readDataFile() const {
        std::ifstream dataFile(databasePath, std::ios::binary);
        EXPECT_TRUE(dataFile.is_open());
        return {std::istreambuf_iterator<char>{dataFile}, std::istreambuf_iterator<char>{}};
    }

    void mutateShadowCheckpointID() const {
        auto header = readShadowHeader();
        header.checkpointID.value.low ^= 1;
        writeShadowHeader(header);
    }

    void convertShadowToLegacy() const {
        auto header = readShadowHeader();
        std::vector<ShadowPageRecord> records;
        records.reserve(header.numShadowPages);
        for (uint64_t i = 0; i < header.numShadowPages; i++) {
            records.push_back(readShadowRecord(i));
        }
        std::sort(records.begin(), records.end(),
            [](const ShadowPageRecord& left, const ShadowPageRecord& right) {
                return left.shadowPageIdx < right.shadowPageIdx;
            });
        header.checkpointPageWatermarkMagic = 0;
        header.checkpointID = ku_uuid_t{0};
        header.checkpointStartDataFileNumPages = INVALID_PAGE_IDX;
        header.checkpointStartDataFileNumPagesCheck = INVALID_PAGE_IDX;

        const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
        std::fstream shadowFile(
            shadowFilePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(shadowFile.is_open());
        shadowFile.write(reinterpret_cast<const char*>(&header), sizeof(header));
        const auto recordsOffset =
            (static_cast<uint64_t>(header.numShadowPages) + 1) * KUZU_PAGE_SIZE;
        shadowFile.seekp(recordsOffset);
        const auto numRecords = static_cast<uint64_t>(records.size());
        shadowFile.write(reinterpret_cast<const char*>(&numRecords), sizeof(numRecords));
        for (const auto& record : records) {
            shadowFile.write(reinterpret_cast<const char*>(&record.originalFileIdx),
                sizeof(record.originalFileIdx));
            shadowFile.write(reinterpret_cast<const char*>(&record.originalPageIdx),
                sizeof(record.originalPageIdx));
        }
        shadowFile.flush();
        ASSERT_TRUE(shadowFile.good());
        shadowFile.close();
        std::filesystem::resize_file(shadowFilePath,
            recordsOffset + sizeof(numRecords) +
                records.size() * (sizeof(file_idx_t) + sizeof(page_idx_t)));
    }

    template<typename... RECORDS>
    void writeCheckpointWAL(const RECORDS&... records) const {
        auto& context = *getClientContext(*conn);
        const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
        auto* vfs = VirtualFileSystem::GetUnsafe(context);
        auto frozenWalFile = vfs->openFile(frozenWalPath,
            FileOpenFlags(FileFlags::READ_ONLY | FileFlags::WRITE), &context);
        frozenWalFile->truncate(0);
        auto writer = std::make_shared<BufferedFileWriter>(*frozenWalFile);
        Serializer serializer{writer};
        serializer.getWriter()->onObjectBegin();
        FileDBIDUtils::writeDatabaseID(serializer,
            StorageManager::Get(context)->getOrInitDatabaseID(context));
        serializer.write(false);
        serializer.getWriter()->onObjectEnd();
        const auto writeRecord = [&](const WALRecord& record) {
            serializer.getWriter()->onObjectBegin();
            record.serialize(serializer);
            serializer.getWriter()->onObjectEnd();
        };
        (writeRecord(records), ...);
        serializer.getWriter()->flush();
        serializer.getWriter()->sync();
    }

    void writeLegacyCheckpointMarker() const { writeCheckpointWAL(CheckpointRecord{}); }

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
    FSMLeakChecker::checkForLeakedPages(conn.get());
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
        markCheckpointMarkerWriteStarted();
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointLoggingCheckpointFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto& shadowFile = StorageManager::Get(*getClientContext(*conn))->getShadowFile();
    uint64_t pagesApplied = 0;
    shadowFile.setPageAppliedHookForTesting([&](uint64_t) { pagesApplied++; });
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnLoggingCheckpoint>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_EQ(pagesApplied, 0);
    shadowFile.setPageAppliedHookForTesting({});
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
    auto queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_FALSE(queryResult->isSuccess());
    ASSERT_NE(queryResult->getErrorMessage().find("must be restarted"), std::string::npos);

    queryResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(queryResult->isSuccess()) << queryResult->getErrorMessage();
    ASSERT_EQ(queryResult->getNext()->getValue(0)->getValue<int64_t>(), 5000);
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
        markCheckpointMarkerWriteStarted();
        if (walRotated) {
            wal->logAndFlushCheckpointToFrozen(&clientContext, checkpointID);
        } else {
            wal->logAndFlushCheckpoint(&clientContext, checkpointID);
        }
        markCheckpointMarkerWritten();
        throw RuntimeException("checkpoint failed.");
    }
};

class FlakyCheckpointerLeavesCheckpointArtifacts final : public Checkpointer {
public:
    explicit FlakyCheckpointerLeavesCheckpointArtifacts(main::ClientContext& context)
        : Checkpointer(context) {}

    void postCheckpointCleanup() override { throw RuntimeException("checkpoint failed."); }
};

class FlakyCheckpointerWritesMismatchedCheckpointMarker final : public Checkpointer {
public:
    explicit FlakyCheckpointerWritesMismatchedCheckpointMarker(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        const auto storageManager = StorageManager::Get(clientContext);
        storageManager->getShadowFile().flushAll(clientContext);
        auto mismatchedCheckpointID = checkpointID;
        mismatchedCheckpointID.value.low ^= 1;
        markCheckpointMarkerWriteStarted();
        if (walRotated) {
            WAL::Get(clientContext)->logAndFlushCheckpointToFrozen(
                &clientContext, mismatchedCheckpointID);
        } else {
            WAL::Get(clientContext)->logAndFlushCheckpoint(
                &clientContext, mismatchedCheckpointID);
        }
        throw RuntimeException("checkpoint failed.");
    }
};

class RecoveryCheckpointerLeavesActiveMarker final : public Checkpointer {
public:
    explicit RecoveryCheckpointerLeavesActiveMarker(main::ClientContext& context)
        : Checkpointer(context) {}

    void writeActiveCheckpointMarker() {
        auto storageManager = StorageManager::Get(clientContext);
        auto databaseHeader = *storageManager->getOrInitDatabaseHeader(clientContext);
        checkpointID = storageManager->getShadowFile().beginCheckpoint(
            clientContext, storageManager->getDataFH()->getNumPages());
        storageManager->getWAL().logAndFlushCheckpointStart(&clientContext, checkpointID,
            storageManager->getShadowFile().getCheckpointStartDataFileNumPages(), false);
        const auto hasStorageChanges = checkpointStorage();
        serializeCatalogAndMetadata(databaseHeader, hasStorageChanges);
        writeDatabaseHeader(databaseHeader);
        storageManager->getShadowFile().flushAll(clientContext);
        markCheckpointMarkerWriteStarted();
        storageManager->getWAL().logAndFlushCheckpoint(&clientContext, checkpointID);
        markCheckpointMarkerWritten();
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
    runFlakyCheckpoint(flakyCheckpointer);

    auto result = conn->query("MATCH (a:test) RETURN COUNT(a);");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));

    conn.reset();
    database.reset();
    createDBAndConn();
    result = conn->query("MATCH (a:test) RETURN COUNT(a);");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 5000);
}

TEST_F(FlakyCheckpointerTest, PartialShadowApplicationRecoversInProcess) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("UNWIND RANGE(0, 2048) AS id CREATE (:test {id: id});")->isSuccess());
    auto& context = *getClientContext(*conn);
    auto& shadowFile = StorageManager::Get(context)->getShadowFile();
    auto* dataFH = StorageManager::Get(context)->getDataFH();
    std::vector<ShadowPageRecord> appliedRecords;
    std::vector<std::string> expectedPages;
    std::vector<bool> updatedResidentTargets;
    std::vector<uint64_t> applicationPageCounts;
    bool sawFrozenCheckpointWAL = false;
    const auto readPage = [](FileHandle& fileHandle, page_idx_t pageIdx) {
        std::string page(KUZU_PAGE_SIZE, '\0');
        fileHandle.readPageFromDisk(reinterpret_cast<uint8_t*>(page.data()), pageIdx);
        return page;
    };
    uint64_t hookCalls = 0;
    shadowFile.setPageAppliedHookForTesting([&](uint64_t pagesApplied) {
        hookCalls++;
        sawFrozenCheckpointWAL = sawFrozenCheckpointWAL ||
                                 std::filesystem::exists(
                                     StorageUtils::getCheckpointWALFilePath(databasePath));
        applicationPageCounts.push_back(pagesApplied);
        if (appliedRecords.empty()) {
            const auto header = readShadowHeader();
            ASSERT_GT(header.numShadowPages, 2);
            for (uint64_t i = 0; i < header.numShadowPages; i++) {
                const auto record = readShadowRecord(i);
                ASSERT_EQ(record.originalFileIdx, dataFH->getFileIndex());
                appliedRecords.push_back(record);
                expectedPages.push_back(
                    readPage(shadowFile.getShadowingFH(), record.shadowPageIdx));
                updatedResidentTargets.push_back(false);
            }
        }
        const auto recordIdx = pagesApplied - 1;
        const auto& record = appliedRecords[recordIdx];
        ASSERT_EQ(readPage(*dataFH, record.originalPageIdx), expectedPages[recordIdx]);
        if (dataFH->getPageState(record.originalPageIdx)->getState() != PageState::EVICTED) {
            updatedResidentTargets[recordIdx] = true;
            ASSERT_EQ(std::string(reinterpret_cast<char*>(dataFH->getFrame(record.originalPageIdx)),
                          KUZU_PAGE_SIZE),
                expectedPages[recordIdx]);
        } else {
            dataFH->pinPage(record.originalPageIdx, PageReadPolicy::READ_PAGE);
            dataFH->unpinPage(record.originalPageIdx);
        }
        if (pagesApplied < appliedRecords.size()) {
            const auto& nextRecord = appliedRecords[pagesApplied];
            if (dataFH->getPageState(nextRecord.originalPageIdx)->getState() == PageState::EVICTED) {
                dataFH->pinPage(nextRecord.originalPageIdx, PageReadPolicy::DONT_READ_PAGE);
                dataFH->unpinPage(nextRecord.originalPageIdx);
            }
        }
        if (hookCalls == 2) {
            throw RuntimeException("injected partial shadow application failure");
        }
    });

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    ASSERT_NE(checkpointResult->getErrorMessage().find("injected partial shadow application failure"),
        std::string::npos);
    shadowFile.setPageAppliedHookForTesting({});
    ASSERT_GT(hookCalls, 2) << checkpointResult->getErrorMessage();
    ASSERT_TRUE(sawFrozenCheckpointWAL);
    ASSERT_EQ(std::count(applicationPageCounts.begin(), applicationPageCounts.end(), 1), 2);
    ASSERT_EQ(applicationPageCounts.back(), appliedRecords.size());
    ASSERT_TRUE(std::ranges::all_of(updatedResidentTargets, [](bool updated) { return updated; }));
    for (uint64_t i = 0; i < appliedRecords.size(); i++) {
        const auto& record = appliedRecords[i];
        ASSERT_EQ(readPage(*dataFH, record.originalPageIdx), expectedPages[i]);
        ASSERT_NE(dataFH->getPageState(record.originalPageIdx)->getState(), PageState::EVICTED);
        ASSERT_EQ(std::string(reinterpret_cast<char*>(dataFH->getFrame(record.originalPageIdx)),
                      KUZU_PAGE_SIZE),
            expectedPages[i]);
    }
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));

    auto countResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(countResult->isSuccess()) << countResult->getErrorMessage();
    ASSERT_EQ(countResult->getNext()->getValue(0)->getValue<int64_t>(), 2049);
    ASSERT_TRUE(conn->query("CREATE (:test {id: 3000});")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());

    checkpointResult.reset();
    countResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    countResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(countResult->isSuccess()) << countResult->getErrorMessage();
    ASSERT_EQ(countResult->getNext()->getValue(0)->getValue<int64_t>(), 2050);
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getWALFilePath(databasePath)));

    auto& activeShadowFile = StorageManager::Get(*getClientContext(*conn))->getShadowFile();
    uint64_t activeHookCalls = 0;
    activeShadowFile.setPageAppliedHookForTesting([&](uint64_t) {
        ASSERT_FALSE(
            std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
        ASSERT_TRUE(std::filesystem::exists(StorageUtils::getWALFilePath(databasePath)));
        if (++activeHookCalls == 1) {
            throw RuntimeException("injected active WAL shadow application failure");
        }
    });
    checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    ASSERT_NE(checkpointResult->getErrorMessage().find("injected active WAL shadow application failure"),
        std::string::npos);
    activeShadowFile.setPageAppliedHookForTesting({});
    ASSERT_GT(activeHookCalls, 1);
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));

    checkpointResult.reset();
    countResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    countResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(countResult->isSuccess()) << countResult->getErrorMessage();
    ASSERT_EQ(countResult->getNext()->getValue(0)->getValue<int64_t>(), 2050);
}

TEST_F(FlakyCheckpointerTest, FailedShadowApplicationRetryRequiresRestart) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    for (auto i = 0; i <= 512; i++) {
        ASSERT_TRUE(conn->query(stringFormat("CREATE (:test {id: {}});", i))->isSuccess());
    }
    auto& context = *getClientContext(*conn);
    auto& shadowFile = StorageManager::Get(context)->getShadowFile();
    uint64_t hookCalls = 0;
    shadowFile.setPageAppliedHookForTesting([&](uint64_t) {
        hookCalls++;
        if (hookCalls == 2) {
            throw RuntimeException("injected initial shadow application failure");
        }
        if (hookCalls == 3) {
            throw RuntimeException("injected shadow application retry failure");
        }
    });

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    ASSERT_NE(checkpointResult->getErrorMessage().find("injected initial shadow application failure"),
        std::string::npos);
    const auto retryFailurePos =
        checkpointResult->getErrorMessage().find("injected shadow application retry failure");
    ASSERT_NE(retryFailurePos, std::string::npos);
    ASSERT_LT(checkpointResult->getErrorMessage().find("injected initial shadow application failure"),
        retryFailurePos);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
    auto queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_FALSE(queryResult->isSuccess());
    ASSERT_NE(queryResult->getErrorMessage().find("must be restarted"), std::string::npos);

    checkpointResult.reset();
    queryResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(queryResult->isSuccess()) << queryResult->getErrorMessage();
    ASSERT_EQ(queryResult->getNext()->getValue(0)->getValue<int64_t>(), 513);
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, TransactionGateCoversShadowRetryAndCleanup) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("UNWIND RANGE(0, 2048) AS id CREATE (:test {id: id});")->isSuccess());

    auto state = std::make_shared<BlockingCheckpointState>();
    auto initFlakyCheckpointer = [state](main::ClientContext& context) {
        return std::make_unique<BlockingPostCheckpointCleanup>(context, state);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));
    auto& shadowFile = StorageManager::Get(*getClientContext(*conn))->getShadowFile();
    uint64_t hookCalls = 0;
    shadowFile.setPageAppliedHookForTesting([&](uint64_t pagesApplied) {
        hookCalls++;
        if (hookCalls == 1 || (hookCalls > 2 && pagesApplied == 1)) {
            const auto checkpointIdx = state->markEntered();
            state->waitUntilReleased(checkpointIdx);
        }
        if (hookCalls == 2) {
            throw RuntimeException("injected gated shadow application failure");
        }
    });

    auto checkpointConn = std::make_unique<main::Connection>(database.get());
    auto checkpointFuture = std::async(
        std::launch::async, [&]() { return checkpointConn->query("CHECKPOINT;"); });
    BlockingCheckpointReleaseGuard releaseGuard{state};
    ASSERT_TRUE(state->waitUntilEnteredCount(1, std::chrono::seconds(5)));

    auto writerConn = std::make_unique<main::Connection>(database.get());
    auto writerFuture = std::async(std::launch::async,
        [&]() { return writerConn->query("CREATE (:test {id: 3000});"); });
    ASSERT_EQ(writerFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    state->releaseNext();
    ASSERT_TRUE(state->waitUntilEnteredCount(2, std::chrono::seconds(5)));
    ASSERT_EQ(writerFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    state->releaseNext();
    ASSERT_TRUE(state->waitUntilEnteredCount(3, std::chrono::seconds(5)));
    ASSERT_EQ(writerFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    state->releaseNext();

    ASSERT_EQ(checkpointFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto checkpointResult = checkpointFuture.get();
    ASSERT_FALSE(checkpointResult->isSuccess());
    ASSERT_NE(checkpointResult->getErrorMessage().find(
                  "injected gated shadow application failure"),
        std::string::npos);
    shadowFile.setPageAppliedHookForTesting({});
    ASSERT_EQ(writerFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto writerResult = writerFuture.get();
    ASSERT_TRUE(writerResult->isSuccess()) << writerResult->getErrorMessage();
    ASSERT_FALSE(state->waitUntilEnteredCount(4, std::chrono::seconds(0)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, LegacyCheckpointMarkerReplaysLegacyShadow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    writeLegacyCheckpointMarker();
    convertShadowToLegacy();

    conn.reset();
    database.reset();
    createDBAndConn();

    auto result = conn->query("MATCH (a:test) RETURN COUNT(a);");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 5000);
}

TEST_F(FlakyCheckpointerTest, LegacyCheckpointMarkerRejectsIdentifiedShadow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    writeLegacyCheckpointMarker();

    conn.reset();
    database.reset();

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, LegacyCheckpointMarkerAfterIdentifiedBeginFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    writeCheckpointWAL(CheckpointBeginRecord{
                           shadowHeader.checkpointID,
                           shadowHeader.checkpointStartDataFileNumPages,
                       },
        CheckpointRecord{});

    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, LegacyCheckpointMarkerWithoutShadowFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    writeLegacyCheckpointMarker();

    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    std::filesystem::remove(shadowFilePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
}

TEST_F(FlakyCheckpointerTest, CheckpointBeginAndMarkerIDMismatchFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerWritesMismatchedCheckpointMarker>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, AmbiguousCheckpointMarkerFailsWithoutTruncating) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();

    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    std::ofstream frozenWal(frozenWalPath, std::ios::binary | std::ios::app);
    ASSERT_TRUE(frozenWal.is_open());
    const auto recordType = static_cast<uint8_t>(WALRecordType::CHECKPOINT_RECORD_V2);
    frozenWal.write(reinterpret_cast<const char*>(&recordType), sizeof(recordType));
    frozenWal.close();
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_ANY_THROW(createDBAndConn());
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, CheckpointMarkerWithoutBeginFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    const auto checkpointEndDataFileNumPages =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    writeCheckpointWAL(
        CheckpointRecordV2{shadowHeader.checkpointID, checkpointEndDataFileNumPages});

    conn.reset();
    database.reset();
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, NonterminalCheckpointMarkerFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    const auto checkpointEndDataFileNumPages =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    writeCheckpointWAL(CheckpointBeginRecord{
                           shadowHeader.checkpointID,
                           shadowHeader.checkpointStartDataFileNumPages,
                       },
        CheckpointRecordV2{shadowHeader.checkpointID, checkpointEndDataFileNumPages},
        CommitRecord{});

    conn.reset();
    database.reset();
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, DuplicateCheckpointBeginFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    std::ifstream shadowFile(shadowFilePath, std::ios::binary);
    ASSERT_TRUE(shadowFile.is_open());
    ShadowFileHeader shadowHeader;
    shadowFile.read(reinterpret_cast<char*>(&shadowHeader), sizeof(shadowHeader));
    ASSERT_TRUE(shadowFile.good());
    shadowFile.close();
    auto& context = *getClientContext(*conn);
    WAL::Get(context)->logAndFlushCheckpointStart(&context, shadowHeader.checkpointID,
        shadowHeader.checkpointStartDataFileNumPages, true);

    conn.reset();
    database.reset();
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, TruncatedCheckpointBeginFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    writeCheckpointWAL(CheckpointBeginRecord{
        shadowHeader.checkpointID, shadowHeader.checkpointStartDataFileNumPages});

    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto completeWalPath = frozenWalPath + ".complete";
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    const auto completeWalSize = std::filesystem::file_size(frozenWalPath);
    ASSERT_GT(completeWalSize, 17);
    std::filesystem::copy_file(
        frozenWalPath, completeWalPath, std::filesystem::copy_options::overwrite_existing);
    conn.reset();
    database.reset();
    systemConfig->throwOnWalReplayFailure = false;

    for (const auto truncatedBytes : {1u, 5u, 9u, 17u}) {
        std::filesystem::copy_file(
            completeWalPath, frozenWalPath, std::filesystem::copy_options::overwrite_existing);
        std::filesystem::resize_file(frozenWalPath, completeWalSize - truncatedBytes);
        EXPECT_THROW(createDBAndConn(), RuntimeException);
        conn.reset();
        database.reset();
        EXPECT_TRUE(std::filesystem::exists(frozenWalPath));
        EXPECT_TRUE(std::filesystem::exists(shadowFilePath));
    }
    std::filesystem::remove(completeWalPath);
}

TEST_F(FlakyCheckpointerTest, TruncatedCheckpointMarkerWithoutBeginFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    const auto checkpointEndDataFileNumPages =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    writeCheckpointWAL(
        CheckpointRecordV2{shadowHeader.checkpointID, checkpointEndDataFileNumPages});

    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto completeWalPath = frozenWalPath + ".complete";
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    const auto completeWalSize = std::filesystem::file_size(frozenWalPath);
    ASSERT_GT(completeWalSize, 32);
    std::filesystem::copy_file(
        frozenWalPath, completeWalPath, std::filesystem::copy_options::overwrite_existing);
    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    systemConfig->throwOnWalReplayFailure = false;

    for (const auto truncatedBytes : {1u, 5u, 9u, 17u, 32u}) {
        std::filesystem::copy_file(
            completeWalPath, frozenWalPath, std::filesystem::copy_options::overwrite_existing);
        std::filesystem::resize_file(frozenWalPath, completeWalSize - truncatedBytes);
        EXPECT_THROW(createDBAndConn(), RuntimeException);
        conn.reset();
        database.reset();
        EXPECT_EQ(readDataFile(), dataFileBeforeRecovery);
        EXPECT_TRUE(std::filesystem::exists(frozenWalPath));
        EXPECT_TRUE(std::filesystem::exists(shadowFilePath));
    }
    std::filesystem::remove(completeWalPath);
}

TEST_F(FlakyCheckpointerTest, InvalidCheckpointMarkerEndGuardFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    const auto checkpointEndDataFileNumPages =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    auto marker = CheckpointRecordV2{shadowHeader.checkpointID, checkpointEndDataFileNumPages};
    marker.checkpointEndDataFileNumPagesCheck ^= 1;
    writeCheckpointWAL(CheckpointBeginRecord{
                           shadowHeader.checkpointID,
                           shadowHeader.checkpointStartDataFileNumPages,
                       },
        marker);

    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, CheckpointMarkerEndBeforeStartFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn.reset();
    database.reset();
    systemConfig->enableChecksums = false;
    createDBAndConn();

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);
    const auto shadowHeader = readShadowHeader();
    ASSERT_GT(shadowHeader.checkpointStartDataFileNumPages, 0);
    writeCheckpointWAL(CheckpointBeginRecord{
                           shadowHeader.checkpointID,
                           shadowHeader.checkpointStartDataFileNumPages,
                       },
        CheckpointRecordV2{
            shadowHeader.checkpointID, shadowHeader.checkpointStartDataFileNumPages - 1});

    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    systemConfig->throwOnWalReplayFailure = false;

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, EmptyCheckpointShadowFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();

    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    std::filesystem::resize_file(shadowFilePath, 0);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, InvalidShadowPageCountFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    auto shadowHeader = readShadowHeader();
    shadowHeader.numShadowPages = INVALID_PAGE_IDX;
    writeShadowHeader(shadowHeader);
    conn.reset();
    database.reset();

    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, MarkedCheckpointWatermarkMismatchFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    auto shadowHeader = readShadowHeader();
    shadowHeader.checkpointStartDataFileNumPages++;
    shadowHeader.checkpointStartDataFileNumPagesCheck =
        ~shadowHeader.checkpointStartDataFileNumPages;
    writeShadowHeader(shadowHeader);
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, MarkedCheckpointInvalidShadowIdentityFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    auto shadowHeader = readShadowHeader();
    shadowHeader.checkpointPageWatermarkMagic++;
    writeShadowHeader(shadowHeader);
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, InvalidLaterShadowFileTargetDoesNotPartiallyReplay) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_GT(readShadowHeader().numShadowPages, 1);
    auto secondRecord = readShadowRecord(1);
    secondRecord.originalFileIdx = INVALID_FILE_IDX;
    writeShadowRecord(1, secondRecord);
    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, InvalidLaterShadowPageTargetDoesNotPartiallyReplay) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_GT(readShadowHeader().numShadowPages, 1);
    auto secondRecord = readShadowRecord(1);
    secondRecord.originalPageIdx =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    writeShadowRecord(1, secondRecord);
    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, DuplicateLaterShadowTargetDoesNotPartiallyReplay) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_GT(readShadowHeader().numShadowPages, 1);
    const auto firstRecord = readShadowRecord(0);
    writeShadowRecord(1, firstRecord);
    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
    ASSERT_FALSE(
        std::filesystem::exists(StorageUtils::getTmpFilePath(databasePath)));
}

TEST_F(FlakyCheckpointerTest, DuplicateShadowPageSourceDoesNotPartiallyReplay) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_GT(readShadowHeader().numShadowPages, 1);
    const auto firstRecord = readShadowRecord(0);
    auto secondRecord = readShadowRecord(1);
    secondRecord.shadowPageIdx = firstRecord.shadowPageIdx;
    writeShadowRecord(1, secondRecord);
    conn.reset();
    database.reset();
    const auto dataFileBeforeRecovery = readDataFile();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(readDataFile(), dataFileBeforeRecovery);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, MarkedCheckpointEndBelowDatabaseFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    const auto checkpointEndDataFileNumPages =
        StorageManager::Get(*getClientContext(*conn))->getDataFH()->getNumPages();
    conn.reset();
    database.reset();
    const auto expandedDataFileSize =
        static_cast<uint64_t>(checkpointEndDataFileNumPages + 1) * KUZU_PAGE_SIZE;
    std::filesystem::resize_file(databasePath, expandedDataFileSize);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), expandedDataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, MarkedCheckpointWatermarkBeyondDatabaseFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query(
        "CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);")
                    ->isSuccess());
    ASSERT_TRUE(conn->query(
        "UNWIND RANGE(0, 5000) AS id CREATE (:test {id: id, name: 'name'});")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 5001, name: 'name'});")->isSuccess());

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));
    ASSERT_FALSE(conn->query("CHECKPOINT;")->isSuccess());

    const auto shadowHeader = readShadowHeader();
    ASSERT_GT(shadowHeader.checkpointStartDataFileNumPages, 1);
    conn.reset();
    database.reset();
    const auto truncatedDataFileSize =
        static_cast<uint64_t>(shadowHeader.checkpointStartDataFileNumPages - 1) * KUZU_PAGE_SIZE;
    std::filesystem::resize_file(databasePath, truncatedDataFileSize);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), truncatedDataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, IdentifiedShadowWithoutWALFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerLeavesCheckpointArtifacts>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    std::filesystem::remove(activeWalPath);
    std::filesystem::remove(frozenWalPath);
    const auto dataFileSize = std::filesystem::file_size(databasePath);

    systemConfig->readOnly = true;
    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));

    systemConfig->readOnly = false;
    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, HeaderOnlyIdentifiedShadowWithoutWALIsRemoved) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    auto& context = *getClientContext(*conn);
    auto storageManager = StorageManager::Get(context);
    const auto checkpointStartDataFileNumPages = storageManager->getDataFH()->getNumPages();
    storageManager->getShadowFile().beginCheckpoint(
        context, checkpointStartDataFileNumPages);

    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    std::filesystem::remove(activeWalPath);
    std::filesystem::remove(frozenWalPath);
    ASSERT_EQ(std::filesystem::file_size(shadowFilePath), KUZU_PAGE_SIZE);
    ASSERT_EQ(std::filesystem::file_size(databasePath),
        static_cast<uint64_t>(checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE);

    createDBAndConn();
    ASSERT_FALSE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, IdentifiedZeroPageShadowPayloadWithoutWALFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    auto& context = *getClientContext(*conn);
    auto storageManager = StorageManager::Get(context);
    storageManager->getShadowFile().beginCheckpoint(
        context, storageManager->getDataFH()->getNumPages());

    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    std::filesystem::remove(activeWalPath);
    std::filesystem::remove(frozenWalPath);
    std::filesystem::resize_file(shadowFilePath, KUZU_PAGE_SIZE + 1);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(shadowFilePath), KUZU_PAGE_SIZE + 1);
}

TEST_F(FlakyCheckpointerTest, IdentifiedZeroPageShadowBeyondWatermarkWithoutWALFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    auto& context = *getClientContext(*conn);
    auto storageManager = StorageManager::Get(context);
    const auto checkpointStartDataFileNumPages = storageManager->getDataFH()->getNumPages();
    storageManager->getShadowFile().beginCheckpoint(
        context, checkpointStartDataFileNumPages);

    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    std::filesystem::remove(activeWalPath);
    std::filesystem::remove(frozenWalPath);
    const auto expandedDataFileSize =
        static_cast<uint64_t>(checkpointStartDataFileNumPages + 1) * KUZU_PAGE_SIZE;
    std::filesystem::resize_file(databasePath, expandedDataFileSize);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), expandedDataFileSize);
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, RecoverFromInterruptedFrozenCheckpointRollbackCleanup) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    const auto shadowHeader = readShadowHeader();
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    std::filesystem::resize_file(databasePath,
        static_cast<uint64_t>(shadowHeader.checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE);
    std::filesystem::remove(shadowFilePath);

    createDBAndConn();
    auto result = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    ASSERT_FALSE(std::filesystem::exists(frozenWalPath));
}

TEST_F(FlakyCheckpointerTest, RecoverFromInterruptedActiveCheckpointRollbackCleanup) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 1});")->isSuccess());

    auto& context = *getClientContext(*conn);
    auto storageManager = StorageManager::Get(context);
    const auto checkpointStartDataFileNumPages = storageManager->getDataFH()->getNumPages();
    const auto checkpointID = storageManager->getShadowFile().beginCheckpoint(
        context, checkpointStartDataFileNumPages);
    storageManager->getWAL().logAndFlushCheckpointStart(
        &context, checkpointID, checkpointStartDataFileNumPages, false);

    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    conn.reset();
    database.reset();
    ASSERT_EQ(std::filesystem::file_size(databasePath),
        static_cast<uint64_t>(checkpointStartDataFileNumPages) * KUZU_PAGE_SIZE);
    std::filesystem::remove(shadowFilePath);

    createDBAndConn();
    auto result = conn->query("MATCH (n:test) RETURN n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNumTuples(), 1);
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
    ASSERT_TRUE(std::filesystem::exists(activeWalPath));
}

TEST_F(FlakyCheckpointerTest, CheckpointBeginRequiresIdentifiedShadow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    const auto shadowHeader = readShadowHeader();
    conn.reset();
    database.reset();
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    const auto movedShadowFilePath = shadowFilePath + ".moved";

    std::filesystem::rename(shadowFilePath, movedShadowFilePath);
    EXPECT_THROW(createDBAndConn(), RuntimeException);
    conn.reset();
    database.reset();
    EXPECT_TRUE(std::filesystem::exists(frozenWalPath));
    std::filesystem::rename(movedShadowFilePath, shadowFilePath);

    auto legacyShadowHeader = shadowHeader;
    legacyShadowHeader.checkpointPageWatermarkMagic = 0;
    writeShadowHeader(legacyShadowHeader);
    EXPECT_THROW(createDBAndConn(), RuntimeException);
    conn.reset();
    database.reset();
    EXPECT_TRUE(std::filesystem::exists(frozenWalPath));
    EXPECT_TRUE(std::filesystem::exists(shadowFilePath));

    writeShadowHeader(shadowHeader);
    std::filesystem::resize_file(shadowFilePath, 1);
    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, ActiveRecoveryMarkerSupersedesFrozenCommits) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 1});")->isSuccess());

    auto& context = *getClientContext(*conn);
    ASSERT_TRUE(WAL::Get(context)->rotateForCheckpoint(&context));
    RecoveryCheckpointerLeavesActiveMarker checkpointer{context};
    checkpointer.writeActiveCheckpointMarker();

    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(activeWalPath));
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));

    conn.reset();
    database.reset();
    createDBAndConn();

    auto result = conn->query("MATCH (n:test) RETURN n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNumTuples(), 1);
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
    ASSERT_FALSE(std::filesystem::exists(activeWalPath));
    ASSERT_FALSE(std::filesystem::exists(frozenWalPath));
    ASSERT_FALSE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, FailedNonRotatedCheckpointBeforeMarkerRequiresRestart) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:test {id: 0});")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    const auto activeWalPath = StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(activeWalPath));

    auto writeResult = conn->query("CREATE (:test {id: 1});");
    ASSERT_FALSE(writeResult->isSuccess());
    ASSERT_NE(writeResult->getErrorMessage().find("must be restarted"), std::string::npos);

    writeResult.reset();
    checkpointResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();

    auto result = conn->query("MATCH (n:test) RETURN n.id ORDER BY n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_EQ(result->getNumTuples(), 1);
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 0);
}

class FlakyCheckpointerFailsAfterClearingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsAfterClearingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void postCheckpointCleanup() override {
        if (!wasWalRotated()) {
            throw RuntimeException("expected a rotated WAL.");
        }
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
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

    void postCheckpointCleanup() override {
        if (!wasWalRotated()) {
            throw RuntimeException("expected a rotated WAL.");
        }
        const auto storageManager = StorageManager::Get(clientContext);
        auto& shadowFile = storageManager->getShadowFile();
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
    runFlakyCheckpoint(flakyCheckpointer);

    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    auto queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_FALSE(queryResult->isSuccess());
    ASSERT_NE(queryResult->getErrorMessage().find("must be restarted"), std::string::npos);

    queryResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(queryResult->isSuccess()) << queryResult->getErrorMessage();
    ASSERT_EQ(queryResult->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

class FlakyCheckpointerFailsOnClearingFiles final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnClearingFiles(main::ClientContext& context)
        : Checkpointer(context) {}

    void postCheckpointCleanup() override { throw RuntimeException("checkpoint failed."); }
};

TEST_F(FlakyCheckpointerTest, FailedCleanupAfterShadowRetryRequiresRestart) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY);")->isSuccess());
    for (auto i = 0; i <= 512; i++) {
        ASSERT_TRUE(conn->query(stringFormat("CREATE (:test {id: {}});", i))->isSuccess());
    }

    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*getClientContext(*conn));
    auto& shadowFile = StorageManager::Get(*getClientContext(*conn))->getShadowFile();
    uint64_t hookCalls = 0;
    shadowFile.setPageAppliedHookForTesting([&](uint64_t) {
        if (++hookCalls == 2) {
            throw RuntimeException("injected shadow application failure before cleanup");
        }
    });

    auto checkpointResult = conn->query("CHECKPOINT;");
    ASSERT_FALSE(checkpointResult->isSuccess());
    const auto initialFailurePos = checkpointResult->getErrorMessage().find(
        "injected shadow application failure before cleanup");
    const auto cleanupFailurePos = checkpointResult->getErrorMessage().find(
        "In-process checkpoint recovery failed: Runtime exception: checkpoint failed.");
    ASSERT_NE(initialFailurePos, std::string::npos);
    ASSERT_NE(cleanupFailurePos, std::string::npos);
    ASSERT_LT(initialFailurePos, cleanupFailurePos);
    shadowFile.setPageAppliedHookForTesting({});
    ASSERT_GT(hookCalls, 2);
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
    auto queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_FALSE(queryResult->isSuccess());
    ASSERT_NE(queryResult->getErrorMessage().find("must be restarted"), std::string::npos);

    checkpointResult.reset();
    queryResult.reset();
    conn.reset();
    database.reset();
    createDBAndConn();
    queryResult = conn->query("MATCH (n:test) RETURN COUNT(n);");
    ASSERT_TRUE(queryResult->isSuccess()) << queryResult->getErrorMessage();
    ASSERT_EQ(queryResult->getNext()->getValue(0)->getValue<int64_t>(), 513);
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
}

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
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getWALFilePath(databasePath)));
    ASSERT_TRUE(std::filesystem::exists(StorageUtils::getShadowFilePath(databasePath)));
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
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
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
    ASSERT_TRUE(std::filesystem::exists(frozenWalFilePath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, ShadowFileCheckpointIDMismatch) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();

    mutateShadowCheckpointID();

    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(FlakyCheckpointerTest, MarkerFreeCheckpointIDMismatchFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    mutateShadowCheckpointID();
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(FlakyCheckpointerTest, MarkerFreeCheckpointWatermarkMismatchFailsClosed) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    conn.reset();
    database.reset();
    auto shadowHeader = readShadowHeader();
    shadowHeader.checkpointStartDataFileNumPages++;
    shadowHeader.checkpointStartDataFileNumPagesCheck =
        ~shadowHeader.checkpointStartDataFileNumPages;
    writeShadowHeader(shadowHeader);
    const auto dataFileSize = std::filesystem::file_size(databasePath);
    const auto frozenWalPath = StorageUtils::getCheckpointWALFilePath(databasePath);
    const auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);

    EXPECT_THROW(createDBAndConn(), RuntimeException);
    ASSERT_EQ(std::filesystem::file_size(databasePath), dataFileSize);
    ASSERT_TRUE(std::filesystem::exists(frozenWalPath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
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
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

} // namespace testing
} // namespace kuzu
