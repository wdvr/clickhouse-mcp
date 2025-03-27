import os
import unittest
import json
import tempfile
from unittest import skipUnless, mock
from unittest.mock import MagicMock, patch

from clickhouse_mcp.mcp_server import (
    run_clickhouse_query,
    get_clickhouse_schema,
    explain_clickhouse_query,
    get_clickhouse_tables,
    lint_clickhouse_query,
    safe_json_dumps,
    MAX_RESPONSE_SIZE
)


class TestClickhouseQuery(unittest.TestCase):

    def setUp(self):
        # Create a patch for the clickhouse client
        self.client_patcher = patch('clickhouse_mcp.mcp_server.get_clickhouse_client')
        self.mock_get_client = self.client_patcher.start()
        self.mock_client = MagicMock()
        self.mock_get_client.return_value = self.mock_client

    def tearDown(self):
        self.client_patcher.stop()

    def test_safe_json_dumps_within_limit(self):
        """Test that json dumps works correctly when data is within size limit."""
        data = {"test": "data"}
        result = safe_json_dumps(data, max_size=1000)
        self.assertEqual(json.loads(result), data)

    def test_safe_json_dumps_exceeds_limit(self):
        """Test that json dumps truncates data when it exceeds the size limit."""
        # Create a large dataset that will exceed the limit
        large_data = {"data": "x" * MAX_RESPONSE_SIZE}
        result = safe_json_dumps(large_data)
        
        # Check if truncation warning is in the result
        self.assertIn("<RESPONSE TRUNCATED>", result)
        self.assertLess(len(result), MAX_RESPONSE_SIZE + 100)  # Allow some buffer for the warning message

    def test_run_clickhouse_query_success(self):
        """Test running a query successfully."""
        # Setup mock return value
        mock_result = MagicMock()
        mock_result.result_rows = [{"column1": "value1", "column2": "value2"}]
        mock_result.column_names = ["column1", "column2"]
        mock_result.query_id = "test_query_id"
        self.mock_client.query.return_value = mock_result

        # Call the function
        with patch('builtins.open', mock_open := mock.mock_open()) as mock_file:
            result = run_clickhouse_query("SELECT * FROM test_table")
        
        # Assertions
        self.mock_client.query.assert_called_once_with("SELECT * FROM test_table")  # No settings since measure_performance=False
        mock_file.assert_called_once()
        self.assertTrue("result_file" in result)
        self.assertTrue("/tmp/clickhouse_query_result_" in result["result_file"])
        self.assertTrue(result["result_file"].endswith(".json"))
        self.assertEqual(result["query_id"], "test_query_id")  # Check query_id is included in results

    def test_run_clickhouse_query_empty_result(self):
        """Test running a query that returns no data."""
        # Setup mock return value for empty result
        mock_result = MagicMock()
        mock_result.result_rows = []
        mock_result.column_names = ["column1", "column2"]
        mock_result.query_id = "empty_query_id"
        self.mock_client.query.return_value = mock_result

        # Call the function
        result = run_clickhouse_query("SELECT * FROM empty_table")
        
        # Assertions
        # The function now returns a dictionary with result information
        self.assertIsInstance(result, dict)
        self.assertIsNotNone(result["result_file"])
        self.assertEqual(result["result_rows"], 0)  # Should be 0 rows
        self.assertEqual(result["query_id"], "empty_query_id")
        self.assertEqual(result["columns"], ["column1", "column2"])
        
    def test_run_clickhouse_query_with_performance_metrics(self):
        """Test running a query with performance measurement enabled."""
        # Setup mock return values
        mock_query_result = MagicMock()
        mock_query_result.result_rows = [{"column1": "value1"}]
        mock_query_result.column_names = ["column1"]
        
        mock_perf_result = MagicMock()
        mock_perf_result.result_rows = [(
            "2025-03-27 10:00:00",  # event_time
            150,                    # query_duration_ms
            2048                    # memory_usage
        )]
        
        # Add query_id attribute to mock_query_result
        mock_query_result.query_id = "server_generated_query_id_12345"
        
        # Configure the mock to return different results for different queries
        def mock_query_side_effect(query):
            if "system.query_log" in query:
                return mock_perf_result
            return mock_query_result
            
        self.mock_client.query.side_effect = mock_query_side_effect
        
        # Use patch to avoid actual file operations
        with patch('builtins.open', mock.mock_open()) as mock_file, \
             patch('time.sleep') as mock_sleep:  # Also patch sleep to avoid delays
            
            result = run_clickhouse_query("SELECT * FROM test_table", measure_performance=True)
        
        # Assertions
        self.assertIn("settings enable_filesystem_cache = 0, use_query_cache = false", 
                     self.mock_client.query.call_args_list[0].args[0])
        # Should NOT contain query_id in the query string anymore
        self.assertNotIn("query_id =", self.mock_client.query.call_args_list[0].args[0])
        
        # Check that the second query (to system.query_log) uses the server-generated query_id
        self.assertIn(f"query_id = 'server_generated_query_id_12345'", 
                     str(self.mock_client.query.call_args_list[1]))
        self.assertEqual(self.mock_client.query.call_count, 2)  # Original query + query_log query
        
        # Check that performance data is included
        self.assertIn("performance", result)
        self.assertEqual(result["performance"]["duration_ms"], 150)
        self.assertEqual(result["performance"]["memory_usage"], 2048)

    def test_get_clickhouse_schema(self):
        """Test getting a table schema."""
        # Setup mock return values for both queries
        mock_describe_result = MagicMock()
        mock_describe_result.result_rows = [
            ["id", "UInt32", "", "", "", "", ""],
            ["name", "String", "", "", "", "", ""]
        ]
        
        mock_create_result = MagicMock()
        mock_create_result.result_rows = [["CREATE TABLE test_table (id UInt32, name String) ENGINE = MergeTree"]]
        
        # Set up the mock to return different results for different queries
        def mock_query_side_effect(query):
            if "DESCRIBE TABLE" in query:
                return mock_describe_result
            elif "SHOW CREATE TABLE" in query:
                return mock_create_result
            return MagicMock()
        
        self.mock_client.query.side_effect = mock_query_side_effect

        # Call the function
        result = get_clickhouse_schema("test_table")
        
        # Parse the JSON result
        parsed_result = json.loads(result)
        
        # Assertions
        self.assertEqual(self.mock_client.query.call_count, 2)
        self.assertEqual(len(parsed_result["columns"]), 2)
        self.assertEqual(parsed_result["columns"][0]["name"], "id")
        self.assertEqual(parsed_result["columns"][1]["type"], "String")
        self.assertIn("CREATE TABLE", parsed_result["create_table_statement"])

    def test_explain_clickhouse_query(self):
        """Test explaining a ClickHouse query."""
        # Setup mock return value
        mock_result = MagicMock()
        mock_result.result_rows = [{"explain": "Query plan details..."}]
        self.mock_client.query.return_value = mock_result

        # Call the function
        result = explain_clickhouse_query("SELECT * FROM test_table")
        
        # Parse the JSON result
        parsed_result = json.loads(result)
        
        # Assertions
        self.mock_client.query.assert_called_once_with("EXPLAIN SELECT * FROM test_table")
        self.assertEqual(parsed_result[0]["explain"], "Query plan details...")

    def test_get_clickhouse_tables(self):
        """Test getting the list of tables."""
        # Setup mock return value
        mock_result = MagicMock()
        mock_result.result_rows = [{"name": "table1"}, {"name": "table2"}]
        self.mock_client.query.return_value = mock_result

        # Call the function
        result = get_clickhouse_tables()
        
        # Parse the JSON result
        parsed_result = json.loads(result)
        
        # Assertions
        self.mock_client.query.assert_called_once_with("SHOW TABLES")
        self.assertEqual(len(parsed_result), 2)
        self.assertEqual(parsed_result[0]["name"], "table1")
        self.assertEqual(parsed_result[1]["name"], "table2")

    @skipUnless(os.getenv("CLICKHOUSE_HOST") and os.getenv("CLICKHOUSE_PORT"),
                reason="Skipping integration test as ClickHouse connection parameters are not set in environment variables.")
    def test_integration_run_clickhouse_query(self):
        """Integration test for running a real ClickHouse query.
        
        This test requires proper ClickHouse connection parameters to be set as environment variables:
        - CLICKHOUSE_HOST
        - CLICKHOUSE_PORT
        - CLICKHOUSE_USER
        - CLICKHOUSE_PASSWORD
        """
        # Remove the patch for this test to use the real client
        self.client_patcher.stop()
        
        try:
            # Run a simple test query that should work on any ClickHouse instance
            result = run_clickhouse_query("SELECT 1 AS test")
            
            # Check that a file was created
            self.assertIsInstance(result, dict)
            self.assertTrue("result_file" in result)
            self.assertTrue(os.path.exists(result["result_file"]))
            
            # Read the file content
            with open(result["result_file"], 'r') as f:
                content = f.read()
            
            # Parse JSON and verify the response
            data = json.loads(content)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0][0], 1)  # First row, first column should be 1
            
            # Clean up the file
            os.remove(result["result_file"])
            
        except Exception as e:
            print(f"Integration test failed: {e}")
            raise
        finally:
            # Restart the patch for other tests
            self.mock_get_client = self.client_patcher.start()
            self.mock_client = MagicMock()
            self.mock_get_client.return_value = self.mock_client


class TestClickhouseLinter(unittest.TestCase):
    def test_empty_query(self):
        """Test linting an empty query."""
        result = lint_clickhouse_query("")
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["errors_count"], 1)
        self.assertIsNone(result["formatted_query"])
        
    def test_valid_query(self):
        """Test linting a valid query."""
        query = "SELECT column1, column2 FROM table WHERE condition = 1 ORDER BY column1"
        with patch('sqlfluff.lint') as mock_lint, patch('sqlfluff.fix') as mock_fix:
            
            # Mock the lint result
            mock_lint.return_value = []  # No violations
            
            # Mock the fix result - same as input for valid query
            mock_fix.return_value = {"fix_str": query}
            
            result = lint_clickhouse_query(query)
            
            # Should be a passing result
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["errors_count"], 0)
            self.assertEqual(result["errors"], [])
            self.assertIsNone(result["formatted_query"])  # No formatted query for valid input
    
    def test_invalid_query(self):
        """Test linting an invalid query with formatting issues."""
        query = "SELECT    column1,column2     FROM table where CONDITION=1 order by column1"
        with patch('sqlfluff.lint') as mock_lint, patch('sqlfluff.fix') as mock_fix:
            
            # Create mock violations
            mock_violation1 = MagicMock()
            mock_violation1.rule_code.return_value = "L001"
            mock_violation1.description.return_value = "Unnecessary whitespace"
            mock_violation1.line_no = 1
            mock_violation1.line_pos = 5
            mock_violation1.line_str = "SELECT    column1"
            
            mock_violation2 = MagicMock()
            mock_violation2.rule_code.return_value = "L010"
            mock_violation2.description.return_value = "Keywords must be capitalized"
            mock_violation2.line_no = 1
            mock_violation2.line_pos = 30
            mock_violation2.line_str = "FROM table where"
            
            mock_lint.return_value = [mock_violation1, mock_violation2]
            
            # Mock the fix result
            formatted = "SELECT column1, column2 FROM table WHERE condition = 1 ORDER BY column1"
            mock_fix.return_value = {"fix_str": formatted}
            
            result = lint_clickhouse_query(query)
            
            # Should be a failing result with violations
            self.assertEqual(result["status"], "fail")
            self.assertEqual(result["errors_count"], 2)
            self.assertEqual(len(result["errors"]), 2)
            self.assertEqual(result["formatted_query"], formatted)
            self.assertEqual(result["errors"][0]["rule"], "L001")
            self.assertEqual(result["errors"][1]["rule"], "L010")
    
    
if __name__ == "__main__":
    unittest.main()