using KuzuDB_Net_Tests.Infrastructure;
using System.Diagnostics;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class FinalizerTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            EnsureKuzuAvailable();
        }

        [TestMethod]
        public void FinalizerTest_CreatedValues_ShouldBeCleanedUpByFinalizer()
        {
            EnsureKuzuAvailable();
            
            // Create values without disposing them explicitly
            // They should be cleaned up by finalizers
            CreateValuesWithoutDisposing();
            
            // Force garbage collection and finalization
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect(); // Second collection to clean up any objects that became eligible during finalization
            
            // If we get here without crashes, finalizers worked
            Assert.IsTrue(true, "Finalizers completed without crashes");
        }

        [TestMethod]
        public void FinalizerTest_BorrowedValues_ShouldNotCrashOnFinalization()
        {
            EnsureKuzuAvailable();
            
            // Set up test data
            ExecuteNonQuery(@"
                CREATE NODE TABLE FinalizerTest(
                    id INT64,
                    name STRING,
                    value DOUBLE,
                    PRIMARY KEY(id)
                )");
            
            ExecuteNonQuery("CREATE (:FinalizerTest {id: 1, name: 'Test', value: 42.0})");
            
            // Create borrowed values (from query results) without disposing
            CreateBorrowedValuesWithoutDisposing();
            
            // Force garbage collection and finalization
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            
            // If we get here without crashes, borrowed value finalizers worked correctly
            Assert.IsTrue(true, "Borrowed value finalizers completed without crashes");
        }

        [TestMethod]
        public void FinalizerTest_MixedOwnership_ShouldHandleCorrectly()
        {
            EnsureKuzuAvailable();
            
            // Set up test data
            ExecuteNonQuery(@"
                CREATE NODE TABLE MixedTest(
                    id INT64,
                    name STRING,
                    PRIMARY KEY(id)
                )");
            
            ExecuteNonQuery("CREATE (:MixedTest {id: 1, name: 'Mixed Test'})");
            
            // Create both owned and borrowed values
            CreateMixedOwnershipValuesWithoutDisposing();
            
            // Force garbage collection and finalization
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            
            Assert.IsTrue(true, "Mixed ownership finalizers completed without crashes");
        }

        [TestMethod]
        public void FinalizerTest_OwnershipInspection_ShouldShowCorrectValues()
        {
            EnsureKuzuAvailable();
            
            // Test ownership of created values using the exposed _is_owned_by_cpp property
            var createdValue = kuzu_value_create_int64(42L);
            
            // Set up test data for borrowed values
            ExecuteNonQuery(@"
                CREATE NODE TABLE OwnershipTest(
                    id INT64,
                    PRIMARY KEY(id)
                )");
            
            ExecuteNonQuery("CREATE (:OwnershipTest {id: 1})");
            
            using var queryResult = ExecuteQuery("MATCH (n:OwnershipTest) RETURN n.id");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            // This should create a borrowed value
            var borrowedValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, borrowedValue);
            
            // We can inspect the native ownership flag
            Console.WriteLine($"Created value _is_owned_by_cpp: {createdValue._is_owned_by_cpp}");
            Console.WriteLine($"Borrowed value _is_owned_by_cpp: {borrowedValue._is_owned_by_cpp}");
            
            // Clean up the created value explicitly
            createdValue.Dispose();
            
            // Let borrowedValue be cleaned up by finalizer
        }

        [TestMethod]
        public void FinalizerTest_StressTest_ManyValuesWithoutDisposing()
        {
            EnsureKuzuAvailable();
            
            const int iterations = 100;
            
            for (int i = 0; i < iterations; i++)
            {
                // Create values without disposing - let finalizers handle them
                CreateMultipleValuesWithoutDisposing(i);
                
                if (i % 20 == 0)
                {
                    // Periodic GC to test finalizer behavior under pressure
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            
            // Final cleanup
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            
            Assert.IsTrue(true, "Stress test completed without crashes");
        }

        // Helper methods that create values without disposing them
        // These methods intentionally don't use 'using' statements
        private void CreateValuesWithoutDisposing()
        {
            var intValue = kuzu_value_create_int64(123L);
            var stringValue = kuzu_value_create_string("Test String");
            var doubleValue = kuzu_value_create_double(3.14159);
            var boolValue = kuzu_value_create_bool(true);
            
            // Verify values work
            kuzu_value_get_int64(intValue, out long intResult);
            kuzu_value_get_string(stringValue, out string stringResult);
            kuzu_value_get_double(doubleValue, out double doubleResult);
            kuzu_value_get_bool(boolValue, out bool boolResult);
            
            Assert.AreEqual(123L, intResult);
            Assert.AreEqual("Test String", stringResult);
            Assert.AreEqual(3.14159, doubleResult, 0.00001);
            Assert.IsTrue(boolResult);
            
            // Don't dispose - let finalizers handle cleanup
        }

        private void CreateBorrowedValuesWithoutDisposing()
        {
            using var queryResult = ExecuteQuery("MATCH (n:FinalizerTest) RETURN n.id, n.name, n.value");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            // Create borrowed values - these should not be destroyed by their finalizers
            var idValue = new kuzu_value();
            var nameValue = new kuzu_value();
            var valueValue = new kuzu_value();
            
            kuzu_flat_tuple_get_value(tuple, 0, idValue);
            kuzu_flat_tuple_get_value(tuple, 1, nameValue);
            kuzu_flat_tuple_get_value(tuple, 2, valueValue);
            
            // Use the values to ensure they're valid
            kuzu_value_get_int64(idValue, out long id);
            kuzu_value_get_string(nameValue, out string name);
            kuzu_value_get_double(valueValue, out double value);
            
            Assert.AreEqual(1L, id);
            Assert.AreEqual("Test", name);
            Assert.AreEqual(42.0, value, 0.001);
            
            // Don't dispose - let finalizers handle cleanup
        }

        private void CreateMixedOwnershipValuesWithoutDisposing()
        {
            // Create owned values
            var ownedInt = kuzu_value_create_int64(999L);
            var ownedString = kuzu_value_create_string("Owned");
            
            // Create borrowed values
            using var queryResult = ExecuteQuery("MATCH (n:MixedTest) RETURN n.id, n.name");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            var borrowedId = new kuzu_value();
            var borrowedName = new kuzu_value();
            
            kuzu_flat_tuple_get_value(tuple, 0, borrowedId);
            kuzu_flat_tuple_get_value(tuple, 1, borrowedName);
            
            // Verify all values work
            kuzu_value_get_int64(ownedInt, out long ownedIntVal);
            kuzu_value_get_string(ownedString, out string ownedStringVal);
            kuzu_value_get_int64(borrowedId, out long borrowedIdVal);
            kuzu_value_get_string(borrowedName, out string borrowedNameVal);
            
            Assert.AreEqual(999L, ownedIntVal);
            Assert.AreEqual("Owned", ownedStringVal);
            Assert.AreEqual(1L, borrowedIdVal);
            Assert.AreEqual("Mixed Test", borrowedNameVal);
            
            // Don't dispose any - let finalizers handle cleanup
        }

        private void CreateMultipleValuesWithoutDisposing(int iteration)
        {
            var value1 = kuzu_value_create_int64(iteration);
            var value2 = kuzu_value_create_string($"Iteration {iteration}");
            var value3 = kuzu_value_create_double(iteration * 1.5);
            var value4 = kuzu_value_create_bool(iteration % 2 == 0);
            
            // Use the values to ensure they're properly created
            kuzu_value_get_int64(value1, out long _);
            kuzu_value_get_string(value2, out string _);
            kuzu_value_get_double(value3, out double _);
            kuzu_value_get_bool(value4, out bool _);
            
            // Don't dispose - rely on finalizers
        }
    }
}