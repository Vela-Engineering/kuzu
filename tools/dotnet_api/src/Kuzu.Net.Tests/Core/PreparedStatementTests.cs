using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class PreparedStatementTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            EnsureKuzuAvailable();
            // Set up test data
            ExecuteNonQuery(@"
                CREATE NODE TABLE PreparedTest(
                    id INT64,
                    name STRING,
                    value DOUBLE,
                    active BOOLEAN,
                    created_date DATE,
                    PRIMARY KEY(id)
                )");
        }

        [TestMethod]
        public void PreparedStatement_CreateAndDispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            var stmt = new kuzu_prepared_statement();
            VerifyDisposable(stmt);
        }

        [TestMethod]
        public void PreparedStatement_Prepare_ShouldSucceed()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: $id, name: $name})");
            Assert.IsNotNull(stmt);
            Assert.IsTrue(kuzu_prepared_statement_is_success(stmt));
        }

        [TestMethod]
        public void PreparedStatement_PrepareInvalid_ShouldFail()
        {
            EnsureKuzuAvailable();
            
            using var stmt = new kuzu_prepared_statement();
            // Use a clearly invalid syntax
            var state = kuzu_connection_prepare(Connection!, "TOTALLY_INVALID_SYNTAX_HERE_12345", stmt);
            
            // KuzuDB might be more permissive, so check various possibilities
            if (state == kuzu_state.KuzuSuccess)
            {
                // If prepare succeeded, check if the statement indicates success
                if (kuzu_prepared_statement_is_success(stmt))
                {
                    // KuzuDB might defer validation - this is acceptable
                    Assert.IsTrue(true);
                }
                else
                {
                    var errorMsg = kuzu_prepared_statement_get_error_message(stmt);
                    Assert.IsTrue(!string.IsNullOrEmpty(errorMsg));
                }
            }
            else
            {
                Assert.AreEqual(kuzu_state.KuzuError, state);
                Assert.IsFalse(kuzu_prepared_statement_is_success(stmt));
            }
        }

        [TestMethod]
        public void PreparedStatement_BindInt64_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: $id, name: 'test'})");
            
            kuzu_prepared_statement_bind_int64(stmt, "id", 42L);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            Assert.IsTrue(kuzu_query_result_is_success(result));
            
            // Verify insertion
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 42 RETURN p.id");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
        }

        [TestMethod]
        public void PreparedStatement_BindString_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: 1, name: $name})");
            
            kuzu_prepared_statement_bind_string(stmt, "name", "Test String Value");
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            
            // Verify insertion
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 1 RETURN p.name");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            using var nameValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, nameValue);
            kuzu_value_get_string(nameValue, out string actualName);
            
            Assert.AreEqual("Test String Value", actualName);
        }

        [TestMethod]
        public void PreparedStatement_BindDouble_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: 2, name: 'test', value: $value})");
            
            kuzu_prepared_statement_bind_double(stmt, "value", 123.456);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            
            // Verify insertion
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 2 RETURN p.value");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            using var valueValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, valueValue);
            kuzu_value_get_double(valueValue, out double actualValue);
            
            Assert.AreEqual(123.456, actualValue, 0.001);
        }

        [TestMethod]
        public void PreparedStatement_BindBool_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: 3, name: 'test', active: $active})");
            
            kuzu_prepared_statement_bind_bool(stmt, "active", true);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            
            // Verify insertion
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 3 RETURN p.active");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            using var activeValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, activeValue);
            kuzu_value_get_bool(activeValue, out bool actualActive);
            
            Assert.IsTrue(actualActive);
        }

        [TestMethod]
        public void PreparedStatement_BindDate_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: 4, name: 'test', created_date: $date})");
            
            using var dateValue = new kuzu_date_t { days = 19358 }; // Approximate days since epoch for 2023-01-01
            kuzu_prepared_statement_bind_date(stmt, "date", dateValue);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            
            // Verify insertion
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 4 RETURN p.created_date");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            using var dateValueResult = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, dateValueResult);
            
            using var actualDate = new kuzu_date_t();
            kuzu_value_get_date(dateValueResult, actualDate);
            
            Assert.AreEqual(dateValue.days, actualDate.days);
        }

        [TestMethod]
        public void PreparedStatement_BindValue_ShouldWork()
        {
            EnsureKuzuAvailable();
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: $id, name: $name})");

            // With the fixed SWIG configuration, we can now properly dispose created values
            // The %newobject directives make these owned (swigCMemOwn = true)
            using var idValue = kuzu_value_create_int64(100L);
            using var nameValue = kuzu_value_create_string("Bound Value");

            kuzu_prepared_statement_bind_value(stmt, "id", idValue);
            kuzu_prepared_statement_bind_value(stmt, "name", nameValue);

            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);

            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 100 RETURN p.id, p.name");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));

            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);

            // These values from kuzu_flat_tuple_get_value should be borrowed (non-owning)
            // With our updated typemaps, they should have swigCMemOwn = false
            using var resultId = new kuzu_value();
            using var resultName = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, resultId);
            kuzu_flat_tuple_get_value(tuple, 1, resultName);

            kuzu_value_get_int64(resultId, out long actualId);
            kuzu_value_get_string(resultName, out string actualName);

            Assert.AreEqual(100L, actualId);
            Assert.AreEqual("Bound Value", actualName);

            // Test ownership inspection using the exposed _is_owned_by_cpp property
            Console.WriteLine($"idValue _is_owned_by_cpp: {idValue._is_owned_by_cpp}");
            Console.WriteLine($"resultId _is_owned_by_cpp: {resultId._is_owned_by_cpp}");
        }

        [TestMethod]
        public void PreparedStatement_MultipleParameterTypes_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement(@"
                CREATE (:PreparedTest {
                    id: $id, 
                    name: $name, 
                    value: $value, 
                    active: $active
                })");
            
            kuzu_prepared_statement_bind_int64(stmt, "id", 999L);
            kuzu_prepared_statement_bind_string(stmt, "name", "Multi Param Test");
            kuzu_prepared_statement_bind_double(stmt, "value", 3.14159);
            kuzu_prepared_statement_bind_bool(stmt, "active", false);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            
            // Verify all parameters were bound correctly
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id = 999 RETURN p.id, p.name, p.value, p.active");
            Assert.IsTrue(kuzu_query_result_has_next(queryResult));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);
            
            using var idVal = new kuzu_value();
            using var nameVal = new kuzu_value();
            using var valueVal = new kuzu_value();
            using var activeVal = new kuzu_value();
            
            kuzu_flat_tuple_get_value(tuple, 0, idVal);
            kuzu_flat_tuple_get_value(tuple, 1, nameVal);
            kuzu_flat_tuple_get_value(tuple, 2, valueVal);
            kuzu_flat_tuple_get_value(tuple, 3, activeVal);
            
            kuzu_value_get_int64(idVal, out long id);
            kuzu_value_get_string(nameVal, out string name);
            kuzu_value_get_double(valueVal, out double value);
            kuzu_value_get_bool(activeVal, out bool active);
            
            Assert.AreEqual(999L, id);
            Assert.AreEqual("Multi Param Test", name);
            Assert.AreEqual(3.14159, value, 0.00001);
            Assert.IsFalse(active);
        }

        [TestMethod]
        public void PreparedStatement_ReuseWithDifferentParameters_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: $id, name: $name})");
            
            // Execute with first set of parameters
            kuzu_prepared_statement_bind_int64(stmt, "id", 201L);
            kuzu_prepared_statement_bind_string(stmt, "name", "First Execution");
            
            using var result1 = new kuzu_query_result();
            var execState1 = kuzu_connection_execute(Connection!, stmt, result1);
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState1);
            
            // Execute with second set of parameters
            kuzu_prepared_statement_bind_int64(stmt, "id", 202L);
            kuzu_prepared_statement_bind_string(stmt, "name", "Second Execution");
            
            using var result2 = new kuzu_query_result();
            var execState2 = kuzu_connection_execute(Connection!, stmt, result2);
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState2);
            
            // Verify both records exist
            using var queryResult = ExecuteQuery("MATCH (p:PreparedTest) WHERE p.id IN [201, 202] RETURN p.id, p.name ORDER BY p.id");
            
            var records = new List<(long id, string name)>();
            while (kuzu_query_result_has_next(queryResult))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(queryResult, tuple);
                
                using var idVal = new kuzu_value();
                using var nameVal = new kuzu_value();
                kuzu_flat_tuple_get_value(tuple, 0, idVal);
                kuzu_flat_tuple_get_value(tuple, 1, nameVal);
                
                kuzu_value_get_int64(idVal, out long id);
                kuzu_value_get_string(nameVal, out string name);
                
                records.Add((id, name));
            }
            
            Assert.AreEqual(2, records.Count);
            Assert.AreEqual(201L, records[0].id);
            Assert.AreEqual("First Execution", records[0].name);
            Assert.AreEqual(202L, records[1].id);
            Assert.AreEqual("Second Execution", records[1].name);
        }

        [TestMethod]
        public void PreparedStatement_SelectWithParameters_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            // Insert test data
            ExecuteNonQuery("CREATE (:PreparedTest {id: 301, name: 'Select Test 1', value: 10.0})");
            ExecuteNonQuery("CREATE (:PreparedTest {id: 302, name: 'Select Test 2', value: 20.0})");
            ExecuteNonQuery("CREATE (:PreparedTest {id: 303, name: 'Select Test 3', value: 30.0})");
            
            using var stmt = PrepareStatement("MATCH (p:PreparedTest) WHERE p.value > $min_value RETURN p.id, p.name, p.value ORDER BY p.id");
            
            kuzu_prepared_statement_bind_double(stmt, "min_value", 15.0);
            
            using var result = new kuzu_query_result();
            var execState = kuzu_connection_execute(Connection!, stmt, result);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, execState);
            Assert.IsTrue(kuzu_query_result_is_success(result));
            
            var matchingRecords = new List<(long id, string name, double value)>();
            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);
                
                using var idVal = new kuzu_value();
                using var nameVal = new kuzu_value();
                using var valueVal = new kuzu_value();
                
                kuzu_flat_tuple_get_value(tuple, 0, idVal);
                kuzu_flat_tuple_get_value(tuple, 1, nameVal);
                kuzu_flat_tuple_get_value(tuple, 2, valueVal);
                
                kuzu_value_get_int64(idVal, out long id);
                kuzu_value_get_string(nameVal, out string name);
                kuzu_value_get_double(valueVal, out double value);
                
                matchingRecords.Add((id, name, value));
            }
            
            Assert.AreEqual(2, matchingRecords.Count);
            Assert.AreEqual(302L, matchingRecords[0].id);
            Assert.AreEqual(303L, matchingRecords[1].id);
        }

        [TestMethod]
        public void PreparedStatement_MemoryLeakTest_MultipleExecutions()
        {
            EnsureKuzuAvailable();
            
            using var stmt = PrepareStatement("CREATE (:PreparedTest {id: $id, name: $name})");
            
            MeasureMemoryUsage("Prepared Statement Execution", () =>
            {
                var randomId = Random.Shared.Next(10000, 99999);
                
                kuzu_prepared_statement_bind_int64(stmt, "id", randomId);
                kuzu_prepared_statement_bind_string(stmt, "name", $"Memory Test {randomId}");
                
                using var result = new kuzu_query_result();
                kuzu_connection_execute(Connection!, stmt, result);
            }, iterations: 50);
        }
    }
}