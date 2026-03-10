using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class QueryResultTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            
            if (Database != null && Connection != null)
            {
                // Set up test data
                ExecuteNonQuery("CREATE NODE TABLE QueryTest(id INT64, name STRING, value DOUBLE, active BOOLEAN, PRIMARY KEY(id))");
                ExecuteNonQuery("CREATE (:QueryTest {id: 1, name: 'Item1', value: 10.5, active: true})");
                ExecuteNonQuery("CREATE (:QueryTest {id: 2, name: 'Item2', value: 20.7, active: false})");
                ExecuteNonQuery("CREATE (:QueryTest {id: 3, name: 'Item3', value: 30.9, active: true})");
            }
        }

        [TestMethod]
        public void QueryResult_IsSuccess_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id");
            Assert.IsTrue(kuzu_query_result_is_success(result));
        }

        [TestMethod]
        public void QueryResult_GetNumColumns_ShouldReturnCorrectCount()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name, q.value");
            var numColumns = kuzu_query_result_get_num_columns(result);
            Assert.AreEqual(3UL, numColumns);
        }

        [TestMethod]
        public void QueryResult_GetColumnNames_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name, q.value");
            
            var state1 = kuzu_query_result_get_column_name(result, 0, out string col1);
            var state2 = kuzu_query_result_get_column_name(result, 1, out string col2);
            var state3 = kuzu_query_result_get_column_name(result, 2, out string col3);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, state1);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state2);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state3);
            
            Assert.AreEqual("q.id", col1);
            Assert.AreEqual("q.name", col2);
            Assert.AreEqual("q.value", col3);
        }

        [TestMethod]
        public void QueryResult_GetColumnNames_RepeatedCalls_ShouldBeStable()
        {
            EnsureKuzuAvailable();

            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name, q.value");

            for (var i = 0; i < 200; i++)
            {
                var state = kuzu_query_result_get_column_name(result, 1, out string col);
                Assert.AreEqual(kuzu_state.KuzuSuccess, state);
                Assert.AreEqual("q.name", col);
            }
        }

        [TestMethod]
        public void QueryResult_GetColumnDataTypes_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name, q.value, q.active");
            
            using var type1 = new kuzu_logical_type();
            using var type2 = new kuzu_logical_type();
            using var type3 = new kuzu_logical_type();
            using var type4 = new kuzu_logical_type();
            
            var state1 = kuzu_query_result_get_column_data_type(result, 0, type1);
            var state2 = kuzu_query_result_get_column_data_type(result, 1, type2);
            var state3 = kuzu_query_result_get_column_data_type(result, 2, type3);
            var state4 = kuzu_query_result_get_column_data_type(result, 3, type4);
            
            Assert.AreEqual(kuzu_state.KuzuSuccess, state1);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state2);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state3);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state4);
            
            var typeId1 = kuzu_data_type_get_id(type1);
            var typeId2 = kuzu_data_type_get_id(type2);
            var typeId3 = kuzu_data_type_get_id(type3);
            var typeId4 = kuzu_data_type_get_id(type4);
            
            Assert.AreEqual(kuzu_data_type_id.KUZU_INT64, typeId1);
            Assert.AreEqual(kuzu_data_type_id.KUZU_STRING, typeId2);
            Assert.AreEqual(kuzu_data_type_id.KUZU_DOUBLE, typeId3);
            Assert.AreEqual(kuzu_data_type_id.KUZU_BOOL, typeId4);
        }

        [TestMethod]
        public void QueryResult_HasNext_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id ORDER BY q.id");
            Assert.IsTrue(kuzu_query_result_has_next(result));
        }

        [TestMethod]
        public void QueryResult_HasNext_EmptyResult_ShouldReturnFalse()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) WHERE q.id > 1000 RETURN q.id");
            Assert.IsFalse(kuzu_query_result_has_next(result));
        }

        [TestMethod]
        public void QueryResult_GetNext_ShouldIterateCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name ORDER BY q.id");
            
            var items = new List<(long id, string name)>();
            
            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                var getNextState = kuzu_query_result_get_next(result, tuple);
                Assert.AreEqual(kuzu_state.KuzuSuccess, getNextState);
                
                using var idValue = new kuzu_value();
                using var nameValue = new kuzu_value();
                
                kuzu_flat_tuple_get_value(tuple, 0, idValue);
                kuzu_flat_tuple_get_value(tuple, 1, nameValue);
                
                kuzu_value_get_int64(idValue, out long id);
                kuzu_value_get_string(nameValue, out string name);
                
                items.Add((id, name));
            }
            
            Assert.AreEqual(3, items.Count);
            Assert.AreEqual(1L, items[0].id);
            Assert.AreEqual("Item1", items[0].name);
            Assert.AreEqual(2L, items[1].id);
            Assert.AreEqual("Item2", items[1].name);
            Assert.AreEqual(3L, items[2].id);
            Assert.AreEqual("Item3", items[2].name);
        }

        [TestMethod]
        public void QueryResult_ResetIterator_ShouldAllowReReading()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id ORDER BY q.id");
            
            // First iteration
            var firstIterationCount = 0;
            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);
                firstIterationCount++;
            }
            
            Assert.AreEqual(3, firstIterationCount);
            Assert.IsFalse(kuzu_query_result_has_next(result));
            
            // Reset and iterate again
            kuzu_query_result_reset_iterator(result);
            
            var secondIterationCount = 0;
            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);
                secondIterationCount++;
            }
            
            Assert.AreEqual(3, secondIterationCount);
        }

        [TestMethod]
        public void QueryResult_ToString_ShouldReturnValidString()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name ORDER BY q.id");
            
            var resultString = kuzu_query_result_to_string(result);
            Assert.IsTrue(!string.IsNullOrEmpty(resultString));
            Assert.IsTrue(resultString.Contains("Item1"));
        }

        [TestMethod]
        public void QueryResult_GetNumTuples_ShouldReturnCorrectCount()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id");
            var numTuples = kuzu_query_result_get_num_tuples(result);
            Assert.AreEqual(3UL, numTuples);
        }

        [TestMethod]
        public void QueryResult_GetQuerySummary_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id");
            using var summary = new kuzu_query_summary();
            
            var state = kuzu_query_result_get_query_summary(result, summary);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state);
            
            var compilingTime = kuzu_query_summary_get_compiling_time(summary);
            var executionTime = kuzu_query_summary_get_execution_time(summary);
            
            Assert.IsTrue(compilingTime >= 0);
            Assert.IsTrue(executionTime >= 0);
        }

        [TestMethod]
        public void QueryResult_LargeResultSet_ShouldHandleCorrectly()
        {
            EnsureKuzuAvailable();
            
            // Create a larger dataset
            ExecuteNonQuery("CREATE NODE TABLE LargeTest(id INT64, PRIMARY KEY(id))");
            
            for (int i = 0; i < 1000; i++)
            {
                ExecuteNonQuery($"CREATE (:LargeTest {{id: {i}}})");
            }
            
            using var result = ExecuteQuery("MATCH (l:LargeTest) RETURN l.id ORDER BY l.id");
            
            var count = 0;
            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);
                
                using var idValue = new kuzu_value();
                kuzu_flat_tuple_get_value(tuple, 0, idValue);
                kuzu_value_get_int64(idValue, out long id);
                
                Assert.AreEqual(count, (int)id);
                count++;
            }
            
            Assert.AreEqual(1000, count);
        }

        [TestMethod]
        public void QueryResult_Dispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            
            var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id");
            VerifyDisposable(result);
        }

        [TestMethod]
        public void QueryResult_MemoryLeakTest_Iteration()
        {
            EnsureKuzuAvailable();
            
            MeasureMemoryUsage("Query Result Iteration", () =>
            {
                using var result = ExecuteQuery("MATCH (q:QueryTest) RETURN q.id, q.name");
                
                while (kuzu_query_result_has_next(result))
                {
                    using var tuple = new kuzu_flat_tuple();
                    kuzu_query_result_get_next(result, tuple);
                    
                    using var idValue = new kuzu_value();
                    using var nameValue = new kuzu_value();
                    
                    kuzu_flat_tuple_get_value(tuple, 0, idValue);
                    kuzu_flat_tuple_get_value(tuple, 1, nameValue);
                    
                    kuzu_value_get_int64(idValue, out _);
                    kuzu_value_get_string(nameValue, out _);
                }
            }, iterations: 100);
        }
    }
}