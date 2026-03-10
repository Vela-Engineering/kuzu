using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class ValueTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            EnsureKuzuAvailable();
            // Set up test data with various data types
            ExecuteNonQuery(@"
                CREATE NODE TABLE ValueTest(
                    id INT64,
                    name STRING,
                    value DOUBLE,
                    active BOOLEAN,
                    created_date DATE,
                    created_timestamp TIMESTAMP,
                    PRIMARY KEY(id)
                )");

            ExecuteNonQuery(@"
                CREATE (:ValueTest {
                    id: 1, 
                    name: 'Test Item', 
                    value: 123.45, 
                    active: true,
                    created_date: date('2023-01-01'),
                    created_timestamp: timestamp('2023-01-01 12:00:00')
                })");
        }

        [TestMethod]
        public void Value_CreateAndDispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            var value = new kuzu_value();
            VerifyDisposable(value);
        }

        [TestMethod]
        public void Value_Int64_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.id");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_INT64, kuzu_data_type_get_id(dataType));
            
            var isNull = kuzu_value_is_null(value);
            Assert.IsFalse(isNull);
            
            kuzu_value_get_int64(value, out long actualValue);
            Assert.AreEqual(1L, actualValue);
        }

        [TestMethod]
        public void Value_String_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.name");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_STRING, kuzu_data_type_get_id(dataType));
            
            var isNull = kuzu_value_is_null(value);
            Assert.IsFalse(isNull);
            
            kuzu_value_get_string(value, out string actualValue);
            Assert.AreEqual("Test Item", actualValue);
        }

        [TestMethod]
        public void Value_Double_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.value");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_DOUBLE, kuzu_data_type_get_id(dataType));
            
            kuzu_value_get_double(value, out double actualValue);
            Assert.AreEqual(123.45, actualValue, 0.01);
        }

        [TestMethod]
        public void Value_Boolean_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.active");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_BOOL, kuzu_data_type_get_id(dataType));
            
            kuzu_value_get_bool(value, out bool actualValue);
            Assert.IsTrue(actualValue);
        }

        [TestMethod]
        public void Value_Date_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.created_date");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_DATE, kuzu_data_type_get_id(dataType));
            
            using var dateValue = new kuzu_date_t();
            kuzu_value_get_date(value, dateValue);
            Assert.IsTrue(dateValue.days > 0);
        }

        [TestMethod]
        public void Value_Timestamp_ShouldWorkCorrectly()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.created_timestamp");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);
            
            using var dataType = new kuzu_logical_type();
            kuzu_value_get_data_type(value, dataType);
            Assert.AreEqual(kuzu_data_type_id.KUZU_TIMESTAMP, kuzu_data_type_get_id(dataType));
            
            using var timestampValue = new kuzu_timestamp_t();
            kuzu_value_get_timestamp(value, timestampValue);
            Assert.IsTrue(timestampValue.value > 0);
        }

        [TestMethod]
        public void Value_DateToString_ShouldReturnValidString()
        {
            EnsureKuzuAvailable();

            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.created_date");
            Assert.IsTrue(kuzu_query_result_has_next(result));

            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);

            using var value = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, value);

            using var dateValue = new kuzu_date_t();
            kuzu_value_get_date(value, dateValue);

            var state = kuzu_date_to_string(dateValue, out string dateString);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state);
            Assert.IsTrue(!string.IsNullOrEmpty(dateString));
        }

        [TestMethod]
        public void Value_NodePropertyNameAndToString_ShouldWork()
        {
            EnsureKuzuAvailable();

            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v");
            Assert.IsTrue(kuzu_query_result_has_next(result));

            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);

            using var nodeValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, nodeValue);

            var nameState = kuzu_node_val_get_property_name_at(nodeValue, 0, out string name);
            Assert.AreEqual(kuzu_state.KuzuSuccess, nameState);
            Assert.IsTrue(!string.IsNullOrEmpty(name));

            var stringState = kuzu_node_val_to_string(nodeValue, out string nodeString);
            Assert.AreEqual(kuzu_state.KuzuSuccess, stringState);
            Assert.IsTrue(!string.IsNullOrEmpty(nodeString));
        }

        [TestMethod]
        public void Value_NullValues_ShouldBeDetected()
        {
            EnsureKuzuAvailable();
            
            ExecuteNonQuery("CREATE (:ValueTest {id: 2, name: NULL, value: NULL, active: NULL})");
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) WHERE v.id = 2 RETURN v.name, v.value, v.active");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            // Check null string
            using var nameValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, nameValue);
            Assert.IsTrue(kuzu_value_is_null(nameValue));
            
            // Check null double
            using var doubleValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 1, doubleValue);
            Assert.IsTrue(kuzu_value_is_null(doubleValue));
            
            // Check null boolean
            using var boolValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 2, boolValue);
            Assert.IsTrue(kuzu_value_is_null(boolValue));
        }

        [TestMethod]
        public void Value_ToString_ShouldReturnValidString()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.id, v.name, v.value");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var idValue = new kuzu_value();
            using var nameValue = new kuzu_value();
            using var doubleValue = new kuzu_value();
            
            kuzu_flat_tuple_get_value(tuple, 0, idValue);
            kuzu_flat_tuple_get_value(tuple, 1, nameValue);
            kuzu_flat_tuple_get_value(tuple, 2, doubleValue);
            
            var idString = kuzu_value_to_string(idValue);
            var nameString = kuzu_value_to_string(nameValue);
            var doubleString = kuzu_value_to_string(doubleValue);
            
            Assert.IsTrue(!string.IsNullOrEmpty(idString));
            Assert.IsTrue(!string.IsNullOrEmpty(nameString));
            Assert.IsTrue(!string.IsNullOrEmpty(doubleString));
            
            Assert.IsTrue(idString.Contains("1"));
            Assert.IsTrue(nameString.Contains("Test Item"));
            Assert.IsTrue(doubleString.Contains("123.45"));
        }

        [TestMethod]
        public void Value_Clone_ShouldCreateIndependentCopy()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.id");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            using var originalValue = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, originalValue);
            
            using var clonedValue = kuzu_value_clone(originalValue);
            
            Assert.IsNotNull(clonedValue);
            
            kuzu_value_get_int64(originalValue, out long originalInt);
            kuzu_value_get_int64(clonedValue, out long clonedInt);
            
            Assert.AreEqual(originalInt, clonedInt);
        }

        [TestMethod]
        public void Value_MemoryLeakTest_MultipleOperations()
        {
            EnsureKuzuAvailable();
            
            MeasureMemoryUsage("Value Operations", () =>
            {
                using var result = ExecuteQuery("MATCH (v:ValueTest) RETURN v.id, v.name, v.value, v.active");
                
                if (kuzu_query_result_has_next(result))
                {
                    using var tuple = new kuzu_flat_tuple();
                    kuzu_query_result_get_next(result, tuple);
                    
                    using var idValue = new kuzu_value();
                    using var nameValue = new kuzu_value();
                    using var doubleValue = new kuzu_value();
                    using var boolValue = new kuzu_value();
                    
                    kuzu_flat_tuple_get_value(tuple, 0, idValue);
                    kuzu_flat_tuple_get_value(tuple, 1, nameValue);
                    kuzu_flat_tuple_get_value(tuple, 2, doubleValue);
                    kuzu_flat_tuple_get_value(tuple, 3, boolValue);
                    
                    kuzu_value_get_int64(idValue, out _);
                    kuzu_value_get_string(nameValue, out _);
                    kuzu_value_get_double(doubleValue, out _);
                    kuzu_value_get_bool(boolValue, out _);
                    
                    using var clonedId = kuzu_value_clone(idValue);
                    kuzu_value_to_string(clonedId);
                }
            }, iterations: 100);
        }
    }
}