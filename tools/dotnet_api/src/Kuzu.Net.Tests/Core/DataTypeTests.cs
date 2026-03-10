using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Core
{
    [TestClass]
    public class DataTypeTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            EnsureKuzuAvailable();
            // Set up test data with various data types
            ExecuteNonQuery(@"
                CREATE NODE TABLE DataTypeTest(
                    id INT64,
                    name STRING,
                    price DOUBLE,
                    active BOOLEAN,
                    created_date DATE,
                    updated_timestamp TIMESTAMP,
                    PRIMARY KEY(id)
                )");

            ExecuteNonQuery(@"
                CREATE (:DataTypeTest {
                    id: 1, 
                    name: 'Sample Item', 
                    price: 99.99, 
                    active: true,
                    created_date: date('2023-06-15'),
                    updated_timestamp: timestamp('2023-06-15 14:30:00')
                })");
        }

        [TestMethod]
        public void LogicalType_CreateAndDispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            var logicalType = new kuzu_logical_type();
            VerifyDisposable(logicalType);
        }

        [TestMethod]
        public void LogicalType_PrimitiveTypes_ShouldHaveCorrectIds()
        {
            EnsureKuzuAvailable();
            using var int64Type = new kuzu_logical_type();
            using var stringType = new kuzu_logical_type();
            using var doubleType = new kuzu_logical_type();
            using var boolType = new kuzu_logical_type();
            using var dateType = new kuzu_logical_type();
            using var timestampType = new kuzu_logical_type();
            
            kuzu_data_type_create(kuzu_data_type_id.KUZU_INT64, null, 0, int64Type);
            kuzu_data_type_create(kuzu_data_type_id.KUZU_STRING, null, 0, stringType);
            kuzu_data_type_create(kuzu_data_type_id.KUZU_DOUBLE, null, 0, doubleType);
            kuzu_data_type_create(kuzu_data_type_id.KUZU_BOOL, null, 0, boolType);
            kuzu_data_type_create(kuzu_data_type_id.KUZU_DATE, null, 0, dateType);
            kuzu_data_type_create(kuzu_data_type_id.KUZU_TIMESTAMP, null, 0, timestampType);
            
            Assert.AreEqual(kuzu_data_type_id.KUZU_INT64, kuzu_data_type_get_id(int64Type));
            Assert.AreEqual(kuzu_data_type_id.KUZU_STRING, kuzu_data_type_get_id(stringType));
            Assert.AreEqual(kuzu_data_type_id.KUZU_DOUBLE, kuzu_data_type_get_id(doubleType));
            Assert.AreEqual(kuzu_data_type_id.KUZU_BOOL, kuzu_data_type_get_id(boolType));
            Assert.AreEqual(kuzu_data_type_id.KUZU_DATE, kuzu_data_type_get_id(dateType));
            Assert.AreEqual(kuzu_data_type_id.KUZU_TIMESTAMP, kuzu_data_type_get_id(timestampType));
        }

        [TestMethod]
        public void LogicalType_Clone_ShouldCreateIndependentCopy()
        {
            EnsureKuzuAvailable();
            using var originalType = new kuzu_logical_type();
            kuzu_data_type_create(kuzu_data_type_id.KUZU_INT64, null, 0, originalType);
            
            using var clonedType = new kuzu_logical_type();
            kuzu_data_type_clone(originalType, clonedType);
            
            Assert.AreEqual(kuzu_data_type_get_id(originalType), kuzu_data_type_get_id(clonedType));
        }

        [TestMethod]
        public void Value_CreatePrimitiveTypes_ShouldWork()
        {
            EnsureKuzuAvailable();
            using var int64Value = kuzu_value_create_int64(42L);
            using var stringValue = kuzu_value_create_string("Test String");
            using var doubleValue = kuzu_value_create_double(3.14159);
            using var boolValue = kuzu_value_create_bool(true);
            using var nullValue = kuzu_value_create_null();
            
            Assert.IsNotNull(int64Value);
            Assert.IsNotNull(stringValue);
            Assert.IsNotNull(doubleValue);
            Assert.IsNotNull(boolValue);
            Assert.IsNotNull(nullValue);
            
            // Verify values
            kuzu_value_get_int64(int64Value, out long intResult);
            kuzu_value_get_string(stringValue, out string stringResult);
            kuzu_value_get_double(doubleValue, out double doubleResult);
            kuzu_value_get_bool(boolValue, out bool boolResult);
            
            Assert.AreEqual(42L, intResult);
            Assert.AreEqual("Test String", stringResult);
            Assert.AreEqual(3.14159, doubleResult, 0.00001);
            Assert.IsTrue(boolResult);
            Assert.IsTrue(kuzu_value_is_null(nullValue));
        }

        [TestMethod]
        public void Value_CreateDateAndTimestamp_ShouldWork()
        {
            EnsureKuzuAvailable();
            using var dateStruct = new kuzu_date_t { days = 19500 }; // Approximate days for 2023-05-15
            using var timestampStruct = new kuzu_timestamp_t { value = 1684080000000000L }; // Microseconds since epoch
            
            using var dateValue = kuzu_value_create_date(dateStruct);
            using var timestampValue = kuzu_value_create_timestamp(timestampStruct);
            
            Assert.IsNotNull(dateValue);
            Assert.IsNotNull(timestampValue);
            
            using var retrievedDate = new kuzu_date_t();
            using var retrievedTimestamp = new kuzu_timestamp_t();
            
            kuzu_value_get_date(dateValue, retrievedDate);
            kuzu_value_get_timestamp(timestampValue, retrievedTimestamp);
            
            Assert.AreEqual(dateStruct.days, retrievedDate.days);
            Assert.AreEqual(timestampStruct.value, retrievedTimestamp.value);
        }

        [TestMethod]
        public void Value_CreateNull_ShouldWork()
        {
            EnsureKuzuAvailable();
            using var nullValue = kuzu_value_create_null();
            
            Assert.IsNotNull(nullValue);
            Assert.IsTrue(kuzu_value_is_null(nullValue));
        }

        [TestMethod]
        public void Value_DataTypeIdentification_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery(@"
                MATCH (d:DataTypeTest) 
                RETURN d.id, d.name, d.price, d.active, d.created_date, d.updated_timestamp
            ");
            
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            // Test each column's data type
            var expectedTypes = new[]
            {
                kuzu_data_type_id.KUZU_INT64,      // id
                kuzu_data_type_id.KUZU_STRING,     // name
                kuzu_data_type_id.KUZU_DOUBLE,     // price
                kuzu_data_type_id.KUZU_BOOL,       // active
                kuzu_data_type_id.KUZU_DATE,       // created_date
                kuzu_data_type_id.KUZU_TIMESTAMP   // updated_timestamp
            };
            
            for (int i = 0; i < expectedTypes.Length; i++)
            {
                using var value = new kuzu_value();
                kuzu_flat_tuple_get_value(tuple, (ulong)i, value);
                
                using var dataType = new kuzu_logical_type();
                kuzu_value_get_data_type(value, dataType);
                
                var actualTypeId = kuzu_data_type_get_id(dataType);
                Assert.AreEqual(expectedTypes[i], actualTypeId);
            }
        }

        [TestMethod]
        public void FlatTuple_CreateAndDispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            var flatTuple = new kuzu_flat_tuple();
            VerifyDisposable(flatTuple);
        }

        [TestMethod]
        public void FlatTuple_ToString_ShouldReturnValidString()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (d:DataTypeTest) RETURN d.id, d.name");
            Assert.IsTrue(kuzu_query_result_has_next(result));
            
            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);
            
            var tupleString = kuzu_flat_tuple_to_string(tuple);
            Assert.IsTrue(!string.IsNullOrEmpty(tupleString));
            Assert.IsTrue(tupleString.Contains("1"));
            Assert.IsTrue(tupleString.Contains("Sample Item"));
        }

        [TestMethod]
        public void QuerySummary_CreateAndDispose_ShouldNotThrow()
        {
            EnsureKuzuAvailable();
            var querySummary = new kuzu_query_summary();
            VerifyDisposable(querySummary);
        }

        [TestMethod]
        public void QuerySummary_GetTimings_ShouldWork()
        {
            EnsureKuzuAvailable();
            
            using var result = ExecuteQuery("MATCH (d:DataTypeTest) RETURN COUNT(*)");
            using var summary = new kuzu_query_summary();
            
            var state = kuzu_query_result_get_query_summary(result, summary);
            Assert.AreEqual(kuzu_state.KuzuSuccess, state);
            
            var compilingTime = kuzu_query_summary_get_compiling_time(summary);
            var executionTime = kuzu_query_summary_get_execution_time(summary);
            
            Assert.IsTrue(compilingTime >= 0.0);
            Assert.IsTrue(executionTime >= 0.0);
            
            // Usually compilation + execution should be a reasonable time (< 10 seconds for this simple query)
            Assert.IsTrue(compilingTime + executionTime < 10000.0);
        }

        [TestMethod]
        public void DataType_MemoryLeakTest_MultipleOperations()
        {
            EnsureKuzuAvailable();
            MeasureMemoryUsage("Data Type Operations", () =>
            {
                using var int64Type = new kuzu_logical_type();
                using var stringType = new kuzu_logical_type();
                using var clonedInt64 = new kuzu_logical_type();
                
                kuzu_data_type_create(kuzu_data_type_id.KUZU_INT64, null, 0, int64Type);
                kuzu_data_type_create(kuzu_data_type_id.KUZU_STRING, null, 0, stringType);
                kuzu_data_type_clone(int64Type, clonedInt64);
                
                kuzu_data_type_get_id(int64Type);
                kuzu_data_type_get_id(stringType);
                kuzu_data_type_get_id(clonedInt64);
                
                using var int64Value = kuzu_value_create_int64(123L);
                using var stringValue = kuzu_value_create_string("test");
                using var clonedValue = kuzu_value_clone(int64Value);
                
                kuzu_value_get_int64(int64Value, out _);
                kuzu_value_get_string(stringValue, out _);
                kuzu_value_get_int64(clonedValue, out _);
            }, iterations: 100);
        }

        [TestMethod]
        public void Value_EdgeCases_ShouldHandleCorrectly()
        {
            EnsureKuzuAvailable();
            // Test edge cases like empty strings, zero values, negative values
            using var emptyString = kuzu_value_create_string("");
            using var zeroInt = kuzu_value_create_int64(0L);
            using var negativeInt = kuzu_value_create_int64(-42L);
            using var zeroDouble = kuzu_value_create_double(0.0);
            using var negativeDouble = kuzu_value_create_double(-3.14);
            using var falseBool = kuzu_value_create_bool(false);
            
            // Verify empty string
            kuzu_value_get_string(emptyString, out string emptyStr);
            Assert.AreEqual("", emptyStr);
            
            // Verify zero values
            kuzu_value_get_int64(zeroInt, out long zeroIntVal);
            kuzu_value_get_double(zeroDouble, out double zeroDoubleVal);
            Assert.AreEqual(0L, zeroIntVal);
            Assert.AreEqual(0.0, zeroDoubleVal);
            
            // Verify negative values
            kuzu_value_get_int64(negativeInt, out long negIntVal);
            kuzu_value_get_double(negativeDouble, out double negDoubleVal);
            Assert.AreEqual(-42L, negIntVal);
            Assert.AreEqual(-3.14, negDoubleVal, 0.01);
            
            // Verify false boolean
            kuzu_value_get_bool(falseBool, out bool falseBoolVal);
            Assert.IsFalse(falseBoolVal);
            
            // Verify all are non-null
            Assert.IsFalse(kuzu_value_is_null(emptyString));
            Assert.IsFalse(kuzu_value_is_null(zeroInt));
            Assert.IsFalse(kuzu_value_is_null(negativeInt));
            Assert.IsFalse(kuzu_value_is_null(zeroDouble));
            Assert.IsFalse(kuzu_value_is_null(negativeDouble));
            Assert.IsFalse(kuzu_value_is_null(falseBool));
        }
    }
}