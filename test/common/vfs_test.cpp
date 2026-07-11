#include <filesystem>
#include <fstream>
#include <string>

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

#if !defined(_WIN32) && !defined(__WASM__)
TEST(VFSTests, SyncParentDirectory) {
    const auto testDir = std::filesystem::temp_directory_path() / "kuzu_sync_parent_directory";
    const auto filePath = testDir / "database.wal";
    std::filesystem::remove_all(testDir);
    std::filesystem::create_directories(testDir);
    VirtualFileSystem vfs(filePath.string());

    ASSERT_NO_THROW(vfs.syncParentDirectory(filePath.string()));

    std::filesystem::remove_all(testDir);
    ASSERT_THROW(vfs.syncParentDirectory(filePath.string()), IOException);
}
#endif

#if !defined(__WASM__)
TEST(VFSTests, DurableRenameAndRemove) {
    const auto testDir = std::filesystem::temp_directory_path() / "kuzu_durable_file_operations";
    const auto databasePath = testDir / "database";
    const auto walPath = testDir / "database.wal";
    const auto checkpointWalPath = testDir / "database.wal.checkpoint";
    std::filesystem::remove_all(testDir);
    std::filesystem::create_directories(testDir);
    std::ofstream{walPath}.put('x');
    VirtualFileSystem vfs(databasePath.string());

    ASSERT_NO_THROW(vfs.renameFileDurably(walPath.string(), checkpointWalPath.string()));
    ASSERT_FALSE(std::filesystem::exists(walPath));
    ASSERT_TRUE(std::filesystem::exists(checkpointWalPath));

    ASSERT_NO_THROW(vfs.removeFileIfExistsDurably(checkpointWalPath.string()));
    ASSERT_FALSE(std::filesystem::exists(checkpointWalPath));
#if defined(_WIN32)
    const auto tombstonePath = checkpointWalPath.string() + ".tombstone";
    std::ofstream{tombstonePath}.put('x');
    ASSERT_NO_THROW(vfs.removeFileIfExistsDurably(checkpointWalPath.string()));
    ASSERT_FALSE(std::filesystem::exists(tombstonePath));
#endif

    std::filesystem::remove_all(testDir);
}
#endif

TEST(VFSTests, VirtualFileSystemDeleteFiles) {
    std::string homeDir = "/tmp/dbHome";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("/tmp/test1");

    // Attempt to delete files not within the list of db files (should error)
    try {
        vfs.removeFileIfExists("/tmp/test1");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/test1 is not within the allowed "
                               "list of files to be removed.");
    }
    try {
        vfs.removeFileIfExists("/tmp/dbHome");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/dbHome is not within the allowed "
                               "list of files to be removed.");
    }

    ASSERT_NO_THROW(vfs.removeFileIfExists("/tmp/dbHome.lock"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("/tmp/dbHome.shadow"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("/tmp/dbHome.wal"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("/tmp/dbHome.tmp"));

    ASSERT_TRUE(std::filesystem::exists("/tmp/test1"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("/tmp/test1");
}

#ifndef __WASM__ // home directory is not available in WASM
TEST(VFSTests, VirtualFileSystemDeleteFilesWithHome) {
    std::string homeDir = "~/tmp/dbHome";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("~/tmp/test1");

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("~/tmp/test1");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path ~/tmp/test1 is not within the allowed "
                               "list of files to be removed.");
    }
    try {
        vfs.removeFileIfExists("~/tmp/dbHome");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path ~/tmp/dbHome is not within the allowed "
                               "list of files to be removed.");
    }

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("~");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(),
            "IO exception: Error: Path ~ is not within the allowed list of files to be removed.");
    }

    ASSERT_NO_THROW(vfs.removeFileIfExists("~/tmp/dbHome.lock"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("~/tmp/dbHome.wal"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("~/tmp/dbHome.shadow"));
    ASSERT_NO_THROW(vfs.removeFileIfExists("~/tmp/dbHome.tmp"));

    ASSERT_TRUE(std::filesystem::exists("~/tmp/test1"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("~/tmp/test1");
}
#endif

TEST(VFSTests, VirtualFileSystemDeleteFilesEdgeCases) {
    std::string homeDir = "/tmp/dbHome";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("/tmp/dbHome/");
    std::filesystem::create_directories("/tmp/dbHome/../test2");
    std::filesystem::create_directories("/tmp");
    std::filesystem::create_directories("/tmp/dbHome/test2");

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("/tmp/dbHome/../test2");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/dbHome/../test2 is not within the "
                               "allowed list of files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/tmp");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp is not within the allowed list of "
                               "files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/tmp/");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/ is not within the allowed list of "
                               "files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/tmp//////////////////");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp////////////////// is not within the "
                               "allowed list of files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/tmp/./.././");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/./.././ is not within the allowed "
                               "list of files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(),
            "IO exception: Error: Path / is not within the allowed list of files to be removed.");
    }

    try {
        vfs.removeFileIfExists("/tmp/dbHome/test2");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/dbHome/test2 is not within the "
                               "allowed list of files to be removed.");
    }

    ASSERT_TRUE(std::filesystem::exists("/tmp/test2"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome/test2"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("/tmp/test2");
    std::filesystem::remove_all("/tmp/dbHome/test2");
}

#if defined(_WIN32)
TEST(VFSTests, VirtualFileSystemDeleteFilesWindowsPaths) {
    // Test Home Directory
    std::string homeDir = "C:\\Desktop\\dir";
    kuzu::common::VirtualFileSystem vfs(homeDir);

    // Setup directories for testing
    std::filesystem::create_directories("C:\\test1");

    // Mixed separators: HomeDir uses '\' while path uses '/'
    std::string mixedSeparatorPath = "C:\\Desktop/dir/test1";

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("C:\\test1");
        FAIL() << "Expected exception for path outside home directory.";
    } catch (const kuzu::common::IOException& e) {
        EXPECT_STREQ(e.what(), "IO exception: Error: Path C:\\test1 is not within the allowed list "
                               "of files to be removed.");
    }

    // Attempt to delete file inside the home directory with mixed separators (should succeed)
    try {
        vfs.removeFileIfExists(mixedSeparatorPath);
    } catch (const kuzu::common::IOException& e) {
        EXPECT_STREQ(e.what(), "IO exception: Error: Path C:\\Desktop/dir/test1 is not within the "
                               "allowed list of files to be removed.");
    }

    ASSERT_FALSE(std::filesystem::exists("C:\\Desktop\\dir\\test1")); // Should be deleted

    // Cleanup
    std::filesystem::remove_all("C:\\Desktop\\dir");
}
#endif

TEST(VFSTests, VirtualFileSystemDeleteFilesWildcardNoRemoval) {
    // Test Home Directory
    std::string homeDir = "/tmp/dbHome_wildcard/";
    kuzu::common::VirtualFileSystem vfs(homeDir);

    // Setup files and directories
    std::filesystem::create_directories("/tmp/dbHome_wildcard/test1_wildcard");
    std::filesystem::create_directories("/tmp/dbHome_wildcard/test2_wildcard");
    std::filesystem::create_directories("/tmp/dbHome_wildcard/nested_wildcard");
    std::ofstream("/tmp/dbHome_wildcard/nested_wildcard/file1.txt").close();
    std::ofstream("/tmp/dbHome_wildcard/nested_wildcard/file2.test").close();

    // Attempt to remove files with wildcard pattern
    try {
        vfs.removeFileIfExists("/tmp/dbHome_wildcard/test*");
    } catch (const kuzu::common::IOException& e) {
        // Verify the exception is thrown for unsupported wildcard
        EXPECT_STREQ(e.what(), "Error: Wildcard patterns are not supported in paths.");
    }

    // Verify files and directories still exist
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/test1_wildcard"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/test2_wildcard"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/nested_wildcard/file1.txt"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/nested_wildcard/file2.test"));

    // Cleanup
    std::filesystem::remove_all("/tmp/dbHome_wildcard");
}
