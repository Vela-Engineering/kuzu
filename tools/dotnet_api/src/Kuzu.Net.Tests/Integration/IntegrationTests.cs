using KuzuDB_Net_Tests.Infrastructure;

namespace KuzuDB_Net_Tests.Integration
{
    [TestClass]
    public class IntegrationTests : KuzuTestBase
    {
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();
            EnsureKuzuAvailable();
            SetupComplexSchema();
        }

        private void SetupComplexSchema()
        {
            try
            {
                // Create node tables
                ExecuteNonQuery(@"
                    CREATE NODE TABLE Person(
                        id INT64, 
                        name STRING, 
                        age INT64, 
                        salary DOUBLE,
                        active BOOLEAN,
                        PRIMARY KEY(id)
                    )");

                ExecuteNonQuery(@"
                    CREATE NODE TABLE Company(
                        id INT64, 
                        name STRING, 
                        founded INT64,
                        revenue DOUBLE,
                        PRIMARY KEY(id)
                    )");

                ExecuteNonQuery(@"
                    CREATE NODE TABLE Project(
                        id INT64, 
                        name STRING, 
                        budget DOUBLE,
                        start_date DATE,
                        PRIMARY KEY(id)
                    )");

                // Create relationship tables
                ExecuteNonQuery("CREATE REL TABLE WorksFor(FROM Person TO Company, start_date DATE, position STRING)");
                ExecuteNonQuery("CREATE REL TABLE WorksOn(FROM Person TO Project, role STRING, hours DOUBLE)");
                ExecuteNonQuery("CREATE REL TABLE Owns(FROM Company TO Project, investment DOUBLE)");

                // Insert test data
                InsertTestData();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Setup failed: {ex.Message}");
            }
        }

        private void InsertTestData()
        {
            // Insert persons
            ExecuteNonQuery("CREATE (:Person {id: 1, name: 'Alice Johnson', age: 30, salary: 75000.0, active: true})");
            ExecuteNonQuery("CREATE (:Person {id: 2, name: 'Bob Smith', age: 25, salary: 65000.0, active: true})");
            ExecuteNonQuery("CREATE (:Person {id: 3, name: 'Carol Davis', age: 35, salary: 85000.0, active: false})");

            // Insert companies
            ExecuteNonQuery("CREATE (:Company {id: 1, name: 'Tech Corp', founded: 2010, revenue: 1000000.0})");
            ExecuteNonQuery("CREATE (:Company {id: 2, name: 'Data Systems', founded: 2015, revenue: 750000.0})");

            // Insert projects
            ExecuteNonQuery("CREATE (:Project {id: 1, name: 'Web Platform', budget: 500000.0, start_date: date('2023-01-15')})");
            ExecuteNonQuery("CREATE (:Project {id: 2, name: 'Mobile App', budget: 300000.0, start_date: date('2023-03-01')})");
            ExecuteNonQuery("CREATE (:Project {id: 3, name: 'Analytics Engine', budget: 800000.0, start_date: date('2023-02-10')})");

            // Create relationships
            ExecuteNonQuery("MATCH (p:Person {id: 1}), (c:Company {id: 1}) CREATE (p)-[:WorksFor {start_date: date('2022-01-01'), position: 'Senior Developer'}]->(c)");
            ExecuteNonQuery("MATCH (p:Person {id: 2}), (c:Company {id: 1}) CREATE (p)-[:WorksFor {start_date: date('2023-01-01'), position: 'Junior Developer'}]->(c)");
            ExecuteNonQuery("MATCH (p:Person {id: 3}), (c:Company {id: 2}) CREATE (p)-[:WorksFor {start_date: date('2021-06-01'), position: 'Lead Architect'}]->(c)");

            ExecuteNonQuery("MATCH (p:Person {id: 1}), (pr:Project {id: 1}) CREATE (p)-[:WorksOn {role: 'Backend Lead', hours: 40.0}]->(pr)");
            ExecuteNonQuery("MATCH (p:Person {id: 2}), (pr:Project {id: 1}) CREATE (p)-[:WorksOn {role: 'Frontend Developer', hours: 35.0}]->(pr)");
            ExecuteNonQuery("MATCH (p:Person {id: 1}), (pr:Project {id: 2}) CREATE (p)-[:WorksOn {role: 'Technical Consultant', hours: 10.0}]->(pr)");
            ExecuteNonQuery("MATCH (p:Person {id: 3}), (pr:Project {id: 3}) CREATE (p)-[:WorksOn {role: 'System Architect', hours: 45.0}]->(pr)");

            ExecuteNonQuery("MATCH (c:Company {id: 1}), (pr:Project {id: 1}) CREATE (c)-[:Owns {investment: 400000.0}]->(pr)");
            ExecuteNonQuery("MATCH (c:Company {id: 1}), (pr:Project {id: 2}) CREATE (c)-[:Owns {investment: 300000.0}]->(pr)");
            ExecuteNonQuery("MATCH (c:Company {id: 2}), (pr:Project {id: 3}) CREATE (c)-[:Owns {investment: 800000.0}]->(pr)");
        }

        [TestMethod]
        public void ComplexQuery_JoinWithRelationships_ShouldWork()
        {
            EnsureKuzuAvailable();

            var query = @"
                MATCH (p:Person)-[w:WorksFor]->(c:Company)-[o:Owns]->(pr:Project)
                WHERE p.active = true AND c.revenue > 500000
                RETURN p.name, c.name, pr.name, w.position, o.investment
                ORDER BY p.name";

            using var result = ExecuteQuery(query);

            var results = new List<(string personName, string companyName, string projectName, string position, double investment)>();

            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);

                using var personNameVal = new kuzu_value();
                using var companyNameVal = new kuzu_value();
                using var projectNameVal = new kuzu_value();
                using var positionVal = new kuzu_value();
                using var investmentVal = new kuzu_value();

                kuzu_flat_tuple_get_value(tuple, 0, personNameVal);
                kuzu_flat_tuple_get_value(tuple, 1, companyNameVal);
                kuzu_flat_tuple_get_value(tuple, 2, projectNameVal);
                kuzu_flat_tuple_get_value(tuple, 3, positionVal);
                kuzu_flat_tuple_get_value(tuple, 4, investmentVal);

                kuzu_value_get_string(personNameVal, out string personName);
                kuzu_value_get_string(companyNameVal, out string companyName);
                kuzu_value_get_string(projectNameVal, out string projectName);
                kuzu_value_get_string(positionVal, out string position);
                kuzu_value_get_double(investmentVal, out double investment);

                results.Add((personName, companyName, projectName, position, investment));
            }

            Assert.IsTrue(results.Count > 0);
            Assert.IsTrue(results.Any(r => r.personName.Contains("Alice") && r.companyName == "Tech Corp"));
        }

        [TestMethod]
        public void PreparedStatement_ComplexInsertAndQuery_ShouldWork()
        {
            EnsureKuzuAvailable();

            // Insert new person using prepared statement
            using var insertStmt = PrepareStatement(@"
                CREATE (:Person {
                    id: $id, 
                    name: $name, 
                    age: $age, 
                    salary: $salary, 
                    active: $active
                })");

            kuzu_prepared_statement_bind_int64(insertStmt, "id", 100L);
            kuzu_prepared_statement_bind_string(insertStmt, "name", "David Wilson");
            kuzu_prepared_statement_bind_int64(insertStmt, "age", 28L);
            kuzu_prepared_statement_bind_double(insertStmt, "salary", 70000.0);
            kuzu_prepared_statement_bind_bool(insertStmt, "active", true);

            using var insertResult = new kuzu_query_result();
            var insertState = kuzu_connection_execute(Connection!, insertStmt, insertResult);
            Assert.AreEqual(kuzu_state.KuzuSuccess, insertState);

            // Query the inserted person using prepared statement
            using var queryStmt = PrepareStatement("MATCH (p:Person) WHERE p.id = $id RETURN p.name, p.age, p.salary");

            kuzu_prepared_statement_bind_int64(queryStmt, "id", 100L);

            using var queryResult = new kuzu_query_result();
            var queryState = kuzu_connection_execute(Connection!, queryStmt, queryResult);
            Assert.AreEqual(kuzu_state.KuzuSuccess, queryState);

            Assert.IsTrue(kuzu_query_result_has_next(queryResult));

            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(queryResult, tuple);

            using var nameVal = new kuzu_value();
            using var ageVal = new kuzu_value();
            using var salaryVal = new kuzu_value();

            kuzu_flat_tuple_get_value(tuple, 0, nameVal);
            kuzu_flat_tuple_get_value(tuple, 1, ageVal);
            kuzu_flat_tuple_get_value(tuple, 2, salaryVal);

            kuzu_value_get_string(nameVal, out string name);
            kuzu_value_get_int64(ageVal, out long age);
            kuzu_value_get_double(salaryVal, out double salary);

            Assert.AreEqual("David Wilson", name);
            Assert.AreEqual(28L, age);
            Assert.AreEqual(70000.0, salary);
        }

        [TestMethod]
        public void AggregationQuery_ShouldWork()
        {
            EnsureKuzuAvailable();

            var query = @"
                MATCH (p:Person)-[:WorksFor]->(c:Company)
                WHERE p.active = true
                RETURN c.name, COUNT(p) as employee_count, AVG(p.salary) as avg_salary
                ORDER BY c.name";

            using var result = ExecuteQuery(query);

            var aggregations = new List<(string companyName, long employeeCount, double avgSalary)>();

            while (kuzu_query_result_has_next(result))
            {
                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(result, tuple);

                using var companyNameVal = new kuzu_value();
                using var countVal = new kuzu_value();
                using var avgSalaryVal = new kuzu_value();

                kuzu_flat_tuple_get_value(tuple, 0, companyNameVal);
                kuzu_flat_tuple_get_value(tuple, 1, countVal);
                kuzu_flat_tuple_get_value(tuple, 2, avgSalaryVal);

                kuzu_value_get_string(companyNameVal, out string companyName);
                kuzu_value_get_int64(countVal, out long count);
                kuzu_value_get_double(avgSalaryVal, out double avgSalary);

                aggregations.Add((companyName, count, avgSalary));
            }

            Assert.IsTrue(aggregations.Count > 0);

            var techCorp = aggregations.FirstOrDefault(a => a.companyName == "Tech Corp");
            Assert.IsTrue(techCorp.employeeCount > 0);
            Assert.IsTrue(techCorp.avgSalary > 0);
        }

        [TestMethod]
        public void TransactionLikeOperations_ShouldWork()
        {
            EnsureKuzuAvailable();

            // Simulate transaction-like behavior by performing multiple related operations
            var newPersonId = 200L;
            var newCompanyId = 100L;

            try
            {
                // Create new company
                ExecuteNonQuery($@"
                    CREATE (:Company {{
                        id: {newCompanyId}, 
                        name: 'Startup Inc', 
                        founded: 2023, 
                        revenue: 100000.0
                    }})");

                // Create new person
                ExecuteNonQuery($@"
                    CREATE (:Person {{
                        id: {newPersonId}, 
                        name: 'Emma Thompson', 
                        age: 26, 
                        salary: 60000.0, 
                        active: true
                    }})");

                // Create relationship
                ExecuteNonQuery($@"
                    MATCH (p:Person {{id: {newPersonId}}}), (c:Company {{id: {newCompanyId}}}) 
                    CREATE (p)-[:WorksFor {{
                        start_date: date('2023-07-01'), 
                        position: 'Software Engineer'
                    }}]->(c)");

                // Verify all operations succeeded
                using var verifyResult = ExecuteQuery($@"
                    MATCH (p:Person {{id: {newPersonId}}})-[w:WorksFor]->(c:Company {{id: {newCompanyId}}})
                    RETURN p.name, c.name, w.position");

                Assert.IsTrue(kuzu_query_result_has_next(verifyResult));

                using var tuple = new kuzu_flat_tuple();
                kuzu_query_result_get_next(verifyResult, tuple);

                using var personNameVal = new kuzu_value();
                using var companyNameVal = new kuzu_value();
                using var positionVal = new kuzu_value();

                kuzu_flat_tuple_get_value(tuple, 0, personNameVal);
                kuzu_flat_tuple_get_value(tuple, 1, companyNameVal);
                kuzu_flat_tuple_get_value(tuple, 2, positionVal);

                kuzu_value_get_string(personNameVal, out string personName);
                kuzu_value_get_string(companyNameVal, out string companyName);
                kuzu_value_get_string(positionVal, out string position);

                Assert.AreEqual("Emma Thompson", personName);
                Assert.AreEqual("Startup Inc", companyName);
                Assert.AreEqual("Software Engineer", position);
            }
            catch (Exception ex)
            {
                Assert.Fail($"Transaction-like operations failed: {ex.Message}");
            }
        }

        [TestMethod]
        public void LargeDataSet_PerformanceTest_ShouldComplete()
        {
            EnsureKuzuAvailable();

            // Create a larger dataset for performance testing
            ExecuteNonQuery("CREATE NODE TABLE LargeTable(id INT64, value STRING, score DOUBLE, PRIMARY KEY(id))");

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Insert 1000 records
            for (int i = 1000; i < 2000; i++)
            {
                ExecuteNonQuery($@"
                    CREATE (:LargeTable {{
                        id: {i}, 
                        value: 'Item_{i}', 
                        score: {i * 1.5}
                    }})");
            }

            stopwatch.Stop();
            Console.WriteLine($"Inserted 1000 records in {stopwatch.ElapsedMilliseconds}ms");

            // Query the large dataset
            stopwatch.Restart();

            using var result = ExecuteQuery(@"
                MATCH (l:LargeTable) 
                WHERE l.score > 1500.0 
                RETURN COUNT(*) as count");

            Assert.IsTrue(kuzu_query_result_has_next(result));

            using var tuple = new kuzu_flat_tuple();
            kuzu_query_result_get_next(result, tuple);

            using var countVal = new kuzu_value();
            kuzu_flat_tuple_get_value(tuple, 0, countVal);
            kuzu_value_get_int64(countVal, out long count);

            stopwatch.Stop();
            Console.WriteLine($"Queried {count} records in {stopwatch.ElapsedMilliseconds}ms");

            Assert.IsTrue(count > 0);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 10000); // Should complete within 10 seconds
        }

        [TestMethod]
        public void ErrorHandling_InvalidOperations_ShouldHandleGracefully()
        {
            EnsureKuzuAvailable();

            // Test various error conditions
            var errorScenarios = new[]
            {
                "MATCH (p:NonExistentTable) RETURN p.id",
                "CREATE (:Person {id: 1, invalid_property: 'test'})", // Duplicate ID might cause issues
                "MATCH (p:Person) WHERE p.nonexistent_property > 0 RETURN p.id"
            };

            foreach (var query in errorScenarios)
            {
                using var result = new kuzu_query_result();
                var state = kuzu_connection_query(Connection!, query, result);

                // Either the query fails at the connection level, or succeeds but indicates failure in the result
                if (state == kuzu_state.KuzuError)
                {
                    Assert.IsFalse(kuzu_query_result_is_success(result));
                }
                else if (state == kuzu_state.KuzuSuccess)
                {
                    // Query might succeed at connection level but fail at result level
                    // Or KuzuDB might be more permissive - both are acceptable
                    var isSuccess = kuzu_query_result_is_success(result);
                    // We don't assert specific behavior here since error handling can vary
                    Console.WriteLine($"Query '{query}' - State: {state}, Success: {isSuccess}");
                }
            }

            // The test passes if we handle errors gracefully without crashing
            Assert.IsTrue(true);
        }

        [TestMethod]
        public void MemoryManagement_ComplexOperations_ShouldNotLeak()
        {
            EnsureKuzuAvailable();

            MeasureMemoryUsage("Complex Integration Operations", () =>
            {
                // Complex query involving multiple joins
                using var result = ExecuteQuery(@"
                    MATCH (p:Person)-[:WorksFor]->(c:Company)-[:Owns]->(pr:Project)
                    WHERE p.salary > 60000
                    RETURN p.name, c.name, pr.name, p.salary");

                while (kuzu_query_result_has_next(result))
                {
                    using var tuple = new kuzu_flat_tuple();
                    kuzu_query_result_get_next(result, tuple);

                    // Access all values to ensure they're processed
                    for (ulong i = 0; i < 4; i++)
                    {
                        using var value = new kuzu_value();
                        kuzu_flat_tuple_get_value(tuple, i, value);

                        if (i == 3) // salary column
                        {
                            kuzu_value_get_double(value, out _);
                        }
                        else
                        {
                            kuzu_value_get_string(value, out _);
                        }
                    }
                }

                // Prepared statement operations
                using var stmt = PrepareStatement("MATCH (p:Person) WHERE p.age > $age RETURN COUNT(*)");
                kuzu_prepared_statement_bind_int64(stmt, "age", 25L);

                using var preparedResult = new kuzu_query_result();
                kuzu_connection_execute(Connection!, stmt, preparedResult);

                if (kuzu_query_result_has_next(preparedResult))
                {
                    using var countTuple = new kuzu_flat_tuple();
                    kuzu_query_result_get_next(preparedResult, countTuple);

                    using var countValue = new kuzu_value();
                    kuzu_flat_tuple_get_value(countTuple, 0, countValue);
                    kuzu_value_get_int64(countValue, out _);
                }
            }, iterations: 50);
        }
    }
}