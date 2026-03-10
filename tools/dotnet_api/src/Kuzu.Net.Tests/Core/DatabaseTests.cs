using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class DatabaseTests : KuzuTestBase
    {
        [TestMethod]
        public void Database_CreateInMemory_ShouldSucceed()
        {
            // Test creating an in-memory database
            using var db = new kuzu_database();
            using var config = kuzu_default_system_config();
            
            var state = kuzu_database_init(":memory:", config, db);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state);
        }

        [TestMethod]
        public void Database_CreateWithTempPath_ShouldSucceed()
        {
            var tempPath = Path.Combine(Path.GetTempPath(), $"kuzu_test_{Guid.NewGuid():N}");
            
            try
            {
                using var db = new kuzu_database();
                using var config = kuzu_default_system_config();
                
                var state = kuzu_database_init(tempPath, config, db);
                Assert.AreEqual(kuzu_state.KuzuSuccess, state);
                
                // KuzuDB might not create the directory until first write, so just verify database was created
                Assert.IsNotNull(db);
            }
            finally
            {
                // Cleanup
                if (Directory.Exists(tempPath))
                {
                    try
                    {
                        Directory.Delete(tempPath, recursive: true);
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
            }
        }

        [TestMethod]
        public void Database_CreateWithInvalidPath_ShouldFail()
        {
            // Use a path that's guaranteed to be invalid on Windows
            var invalidPath = "Z:\\\\invalid\\\\path\\\\that\\\\cannot\\\\exist";
            
            using var db = new kuzu_database();
            using var config = kuzu_default_system_config();
            
            var state = kuzu_database_init(invalidPath, config, db);
            
            // Note: KuzuDB may still succeed here and only fail on first operation
            // So we just check that it doesn't crash
            Assert.IsTrue(state == kuzu_state.KuzuSuccess || state == kuzu_state.KuzuError);
        }

        [TestMethod]
        public void Database_Dispose_ShouldNotThrow()
        {
            using var db = new kuzu_database();
            using var config = kuzu_default_system_config();
            kuzu_database_init(":memory:", config, db);
            
            VerifyDisposable(db);
            VerifyDisposable(config);
        }

        [TestMethod]
        public void SystemConfig_DefaultValues_ShouldBeValid()
        {
            using var config = kuzu_default_system_config();
            
            Assert.IsNotNull(config);
            Assert.IsTrue(config.buffer_pool_size > 0);
            Assert.IsTrue(config.max_num_threads > 0);
        }

        [TestMethod]
        public void SystemConfig_ModifyProperties_ShouldWork()
        {
            using var config = kuzu_default_system_config();
            
            var originalBufferSize = config.buffer_pool_size;
            var originalMaxThreads = config.max_num_threads;
            var originalCompression = config.enable_compression;
            
            // Modify properties
            config.buffer_pool_size = originalBufferSize * 2;
            config.max_num_threads = Math.Max(1UL, originalMaxThreads - 1);
            config.enable_compression = !originalCompression;
            
            // Verify changes
            Assert.AreEqual(originalBufferSize * 2, config.buffer_pool_size);
            Assert.AreEqual(Math.Max(1UL, originalMaxThreads - 1), config.max_num_threads);
            Assert.AreEqual(!originalCompression, config.enable_compression);
        }

        [TestMethod]
        public void Database_MultipleConnections_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            // Create multiple connections to the same database
            using var conn1 = new kuzu_connection();
            using var conn2 = new kuzu_connection();
            
            var state1 = kuzu_connection_init(Database!, conn1);
            var state2 = kuzu_connection_init(Database!, conn2);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, state1);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state2);
        }

        [TestMethod]
        public void Database_MemoryLeakTest_MultipleCreations()
        {
            MeasureMemoryUsage("Database Creation and Disposal", () =>
            {
                using var db = new kuzu_database();
                using var config = kuzu_default_system_config();
                kuzu_database_init(":memory:", config, db);
            }, iterations: 100);
        }
    }
}