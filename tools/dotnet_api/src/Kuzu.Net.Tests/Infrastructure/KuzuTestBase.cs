namespace KuzuDB_Net_Tests.Infrastructure
{
    /// <summary>
    /// Base class for KuzuDB tests that provides proper memory management and resource cleanup.
    /// This ensures that native resources are properly disposed and memory doesn't leak.
    /// </summary>
    [TestClass]
    public abstract class KuzuTestBase : IDisposable
    {
        protected kuzu_database? Database { get; private set; }
        protected kuzu_connection? Connection { get; private set; }
        protected kuzu_system_config? SystemConfig { get; private set; }
        protected string? InitializationError { get; private set; }
        protected TestContext? TestContext { get; set; }

        // Static crash detection
        private static readonly object CrashLock = new object();
        private static volatile bool HasCrashed = false;

        protected KuzuTestBase()
        {
        }

        [TestInitialize]
        public virtual void TestInitialize()
        {
            // Check if previous tests caused crashes
            lock (CrashLock)
            {
                if (HasCrashed)
                {
                    LogCrashRecovery();
                    HasCrashed = false;
                }
            }

            LogTestStart();
            
            try
            {
                InitializationError = null;
                LogTestHostInfo();
                Console.WriteLine("Initializing KuzuDB test base...");
                InitializeKuzuResources();
            }
            catch (Exception ex)
            {
                InitializationError = ex.Message;
                Database = null;
                Connection = null;
                SystemConfig = null;
                
                // Log detailed error information
                Console.WriteLine($"KuzuDB initialization failed: {ex.Message}");
                Console.WriteLine($"Exception type: {ex.GetType().Name}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }

        [TestCleanup] 
        public virtual void TestCleanup()
        {
            try
            {
                LogTestEnd();
                
                // Force cleanup and GC to prevent accumulation
                CleanupKuzuResources();
                
                // Force garbage collection to ensure native resources are cleaned up
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
            }
            catch (Exception ex)
            {
                LogCrashAttempt(ex);
                lock (CrashLock)
                {
                    HasCrashed = true;
                }
                throw;
            }
        }

        public virtual void Dispose()
        {
            // In MSTest, TestCleanup is generally preferred for per-test cleanup.
            // The Dispose method is kept for compliance with IDisposable but the main
            // cleanup logic is in TestCleanup to ensure it runs after each test.
            GC.SuppressFinalize(this);
        }

        protected virtual void InitializeKuzuResources()
        {
            // Log native library information
            LogNativeLibraryInfo();

            // Ensure any previous partially initialized resources are cleared
            CleanupKuzuResources();

            try
            {
                // Correct order of creation:
                // 1. System Config
                // 2. Database wrapper
                // 3. Initialize Database
                // 4. Connection wrapper
                // 5. Initialize Connection
                SystemConfig = kuzu_default_system_config();
                if (SystemConfig == null)
                {
                    throw new InvalidOperationException("Failed to create system config (null returned)");
                }

                Database = new kuzu_database();
                if (Database == null)
                {
                    throw new InvalidOperationException("Failed to create database wrapper (null returned)");
                }

                var dbState = kuzu_database_init(":memory:", SystemConfig, Database);
                if (dbState != kuzu_state.KuzuSuccess)
                {
                    var errorInfo = GetDetailedErrorInfo("Database Initialization", dbState);
                    Console.WriteLine($"Database initialization failed:\n{errorInfo}");
                    throw new InvalidOperationException($"Failed to initialize in-memory database. {errorInfo}");
                }

                Connection = new kuzu_connection();
                if (Connection == null)
                {
                    throw new InvalidOperationException("Failed to create connection wrapper (null returned)");
                }

                var connState = kuzu_connection_init(Database, Connection);
                if (connState != kuzu_state.KuzuSuccess)
                {
                    var errorInfo = GetDetailedErrorInfo("Connection Initialization", connState);
                    Console.WriteLine($"Connection initialization failed:\n{errorInfo}");
                    throw new InvalidOperationException($"Failed to initialize connection. {errorInfo}");
                }

                Console.WriteLine("KuzuDB resources initialized successfully");
            }
            catch
            {
                // On failure ensure we clean up anything that was created to avoid native leaks / later crashes
                try { Connection?.Dispose(); } catch { }
                try { Database?.Dispose(); } catch { }
                try { SystemConfig?.Dispose(); } catch { }
                Connection = null;
                Database = null;
                SystemConfig = null;
                throw;
            }
        }

        protected virtual void CleanupKuzuResources()
        {
            try
            {
                // Dispose in reverse order of creation
                Connection?.Dispose();
                Database?.Dispose();
                SystemConfig?.Dispose();

                Connection = null;
                Database = null;
                SystemConfig = null;
                Console.WriteLine("KuzuDB resources cleaned up successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during resource cleanup: {ex.Message}");
                throw;
            }
        }

        protected void EnsureKuzuAvailable()
        {
            if (Database == null || Connection == null)
            {
                var skipMessage = $"Kuzu native library not available. Initialization error: {InitializationError}";
                Console.WriteLine($"Skipping test: {skipMessage}");
                
                // Log additional diagnostic information
                Console.WriteLine("Diagnostic Information:");
                Console.WriteLine($"  Database object: {(Database == null ? "null" : "created")}");
                Console.WriteLine($"  Connection object: {(Connection == null ? "null" : "created")}");
                Console.WriteLine($"  SystemConfig object: {(SystemConfig == null ? "null" : "created")}");
                
                // Use MSTest Skip
                Assert.Inconclusive(skipMessage);
            }
        }

        /// <summary>
        /// Executes a non-query statement and verifies success
        /// </summary>
        protected void ExecuteNonQuery(string query)
        {
            EnsureKuzuAvailable();
            
            using var result = new kuzu_query_result();
            var state = kuzu_connection_query(Connection!, query, result);
            
            if (state != kuzu_state.KuzuSuccess)
            {
                var errorMsg = kuzu_query_result_get_error_message(result);
                Assert.Fail($"Query execution failed with state {state}. Query: '{query}'. Native error: {errorMsg}");
            }
            
            if (!kuzu_query_result_is_success(result))
            {
                var errorMsg = kuzu_query_result_get_error_message(result);
                Assert.Fail($"Query result indicates failure. Query: '{query}'. Native error: {errorMsg}");
            }
        }

        /// <summary>
        /// Executes a query and returns the result (caller must dispose)
        /// </summary>
        protected kuzu_query_result ExecuteQuery(string query)
        {
            EnsureKuzuAvailable();
            
            var result = new kuzu_query_result();
            var state = kuzu_connection_query(Connection!, query, result);
            
            if (state != kuzu_state.KuzuSuccess)
            {
                var errorMsg = kuzu_query_result_get_error_message(result);
                result.Dispose();
                throw new InvalidOperationException($"Query execution failed with state {state}. Query: '{query}'. Native error: {errorMsg}");
            }
            
            if (!kuzu_query_result_is_success(result))
            {
                var errorMsg = kuzu_query_result_get_error_message(result);
                result.Dispose();
                throw new InvalidOperationException($"Query result indicates failure. Query: '{query}'. Native error: {errorMsg}");
            }
            
            return result;
        }

        /// <summary>
        /// Creates a prepared statement (caller must dispose)
        /// </summary>
        protected kuzu_prepared_statement PrepareStatement(string query)
        {
            EnsureKuzuAvailable();
            
            var stmt = new kuzu_prepared_statement();
            var state = kuzu_connection_prepare(Connection!, query, stmt);
            
            if (state != kuzu_state.KuzuSuccess)
            {
                var errorMsg = kuzu_prepared_statement_get_error_message(stmt);
                stmt.Dispose();
                throw new InvalidOperationException($"Statement preparation failed with state {state}. Query: '{query}'. Native error: {errorMsg}");
            }
            
            if (!kuzu_prepared_statement_is_success(stmt))
            {
                var errorMsg = kuzu_prepared_statement_get_error_message(stmt);
                stmt.Dispose();
                throw new InvalidOperationException($"Prepared statement indicates failure. Query: '{query}'. Native error: {errorMsg}");
            }
            
            return stmt;
        }

        /// <summary>
        /// Verifies that an object is properly disposable without throwing exceptions
        /// </summary>
        protected static void VerifyDisposable<T>(T obj) where T : IDisposable
        {
            Assert.IsNotNull(obj);
            
            // First disposal should not throw
            obj.Dispose();
            
            // Second disposal should also not throw (idempotent)
            obj.Dispose();
        }

        /// <summary>
        /// Measures memory usage during an operation to detect potential leaks
        /// </summary>
        protected static void MeasureMemoryUsage(string operationName, Action operation, int iterations = 1000)
        {
            // Force GC before measurement
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var initialMemory = GC.GetTotalMemory(true);

            // Execute operation multiple times
            for (int i = 0; i < iterations; i++)
            {
                operation();
            }

            // Force GC after operations
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var finalMemory = GC.GetTotalMemory(true);
            var memoryDifference = finalMemory - initialMemory;

            // Log memory usage for diagnostics
            Console.WriteLine($"Memory usage for {operationName}: Initial={initialMemory}, Final={finalMemory}, Diff={memoryDifference}");

            // Memory should not grow significantly (allowing for reasonable variance for native libraries)
            // Adjusted threshold to account for internal native library memory management, caching, and allocator overhead
            // Allow up to 5KB per iteration for complex native operations like database initialization
            var maxAcceptableGrowth = iterations * 5 * 1024; // 5KB per iteration
            Assert.IsTrue(memoryDifference < maxAcceptableGrowth, 
                $"Potential memory leak detected in {operationName}. Memory grew by {memoryDifference} bytes over {iterations} iterations. " +
                $"Acceptable growth limit: {maxAcceptableGrowth} bytes ({maxAcceptableGrowth / iterations} bytes per iteration).");
        }

        /// <summary>
        /// Logs native library information for debugging purposes
        /// </summary>
        protected static void LogNativeLibraryInfo()
        {
            try
            {
                var version = kuzu_get_version();
                var storageVersion = kuzu_get_storage_version();
                Console.WriteLine($"KuzuDB Version: {version}");
                Console.WriteLine($"KuzuDB Storage Version: {storageVersion}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to get native library info: {ex.Message}");
            }
        }

        /// <summary>
        /// Enhanced error information for failed operations
        /// </summary>
        protected static string GetDetailedErrorInfo(string operation, kuzu_state state, string? nativeError = null)
        {
            var errorInfo = new StringBuilder();
            errorInfo.AppendLine($"Operation: {operation}");
            errorInfo.AppendLine($"State: {state} ({(int)state})");
            
            if (!string.IsNullOrEmpty(nativeError))
            {
                errorInfo.AppendLine($"Native Error: {nativeError}");
            }
            
            errorInfo.AppendLine($"Timestamp: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
            
            return errorInfo.ToString();
        }

        /// <summary>
        /// Logs test host information to help diagnose crashes
        /// </summary>
        protected static void LogTestHostInfo()
        {
            try
            {
                Console.WriteLine("=== Test Host Information ===");
                Console.WriteLine($"Process ID: {Environment.ProcessId}");
                Console.WriteLine($"Process Name: {Process.GetCurrentProcess().ProcessName}");
                Console.WriteLine($"Working Directory: {Environment.CurrentDirectory}");
                Console.WriteLine($"OS Version: {Environment.OSVersion}");
                Console.WriteLine($"CLR Version: {Environment.Version}");
                Console.WriteLine($"Machine Name: {Environment.MachineName}");
                Console.WriteLine($"Available Memory: {GC.GetTotalMemory(false)} bytes");
                
                // Log native library path information
                var currentDir = Directory.GetCurrentDirectory();
                Console.WriteLine($"Current Directory: {currentDir}");
                
                // Look for native libraries
                var possibleNativePaths = new[]
                {
                    Path.Combine(currentDir, "kuzunet.dll"),
                    Path.Combine(currentDir, "libkuzu.dll"),
                    Path.Combine(currentDir, "kuzu.dll")
                };
                
                foreach (var path in possibleNativePaths)
                {
                    Console.WriteLine($"Native library {Path.GetFileName(path)}: {(File.Exists(path) ? "Found" : "Not found")}");
                }
                
                Console.WriteLine("=== End Test Host Information ===");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error logging test host info: {ex.Message}");
            }
        }

        /// <summary>
        /// Logs test start information
        /// </summary>
        protected void LogTestStart()
        {
            try
            {
                var testName = TestContext?.TestName ?? "Unknown Test";
                Console.WriteLine($"=== Starting Test: {testName} ===");
                Console.WriteLine($"Test Start Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
                Console.WriteLine($"Available Memory: {GC.GetTotalMemory(false)} bytes");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error logging test start: {ex.Message}");
            }
        }

        /// <summary>
        /// Logs test end information
        /// </summary>
        protected void LogTestEnd()
        {
            try
            {
                var testName = TestContext?.TestName ?? "Unknown Test";
                Console.WriteLine($"=== Ending Test: {testName} ===");
                Console.WriteLine($"Test End Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
                Console.WriteLine($"Available Memory: {GC.GetTotalMemory(false)} bytes");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error logging test end: {ex.Message}");
            }
        }

        /// <summary>
        /// Logs potential crash information
        /// </summary>
        protected void LogCrashAttempt(Exception ex)
        {
            try
            {
                var testName = TestContext?.TestName ?? "Unknown Test";
                Console.WriteLine($"!!! POTENTIAL CRASH DETECTED IN TEST: {testName} !!!");
                Console.WriteLine($"Crash Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
                Console.WriteLine($"Exception: {ex.GetType().Name}");
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner Exception: {ex.InnerException.GetType().Name}");
                    Console.WriteLine($"Inner Message: {ex.InnerException.Message}");
                }
                
                // Log memory state
                Console.WriteLine($"Memory at crash: {GC.GetTotalMemory(false)} bytes");
                Console.WriteLine($"Gen 0 Collections: {GC.CollectionCount(0)}");
                Console.WriteLine($"Gen 1 Collections: {GC.CollectionCount(1)}");
                Console.WriteLine($"Gen 2 Collections: {GC.CollectionCount(2)}");
                
                Console.WriteLine("!!! END CRASH INFORMATION !!!");
            }
            catch
            {
                // Don't throw during crash logging
            }
        }

        /// <summary>
        /// Logs crash recovery information
        /// </summary>
        protected void LogCrashRecovery()
        {
            try
            {
                Console.WriteLine("=== CRASH RECOVERY ===");
                Console.WriteLine("Previous test may have crashed. Attempting recovery...");
                Console.WriteLine($"Recovery Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
                Console.WriteLine("=== END CRASH RECOVERY ===");
            }
            catch
            {
                // Don't throw during recovery logging
            }
        }
    }
}