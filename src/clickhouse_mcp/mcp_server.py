import clickhouse_connect
import json
from typing import Optional, Any, Dict
import clickhouse_connect.common
import clickhouse_connect.driver
import clickhouse_connect.driver.client
import clickhouse_connect.driver.query
from mcp.server.fastmcp import FastMCP, Context
import os
import sys

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file


# Create an MCP server
mcp = FastMCP("PyTorch ClickHouse MCP")

clickhouse_client: Optional[clickhouse_connect.driver.client.Client] = None

# Maximum response size in bytes - set to a conservative size for context window efficiency
MAX_RESPONSE_SIZE = 10 * 1024  # 10KB


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    """Get the ClickHouse client instance.

    Returns:
        clickhouse_connect.Client: The ClickHouse client.
    """
    global clickhouse_client
    if clickhouse_client is None:
        clickhouse_client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            port=os.getenv("CLICKHOUSE_PORT"),
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database="default"
        )
    return clickhouse_client


def safe_json_dumps(data: Any, indent: int = 2, max_size: int = MAX_RESPONSE_SIZE) -> str:
    """Safely serialize data to JSON with strict size limit.

    Args:
        data: The data to serialize
        indent: Indentation level for pretty printing
        max_size: Maximum response size in bytes

    Returns:
        JSON string, truncated if needed with a warning message
    """
    # Always use indentation for readability
    json_str = json.dumps(data, indent=indent)

    # Return as-is if under the size limit
    if len(json_str) <= max_size:
        return json_str

    # Hard truncate with a clear error message
    warning_msg = (
        "\n\n<RESPONSE TRUNCATED>\n"
        "The response exceeds the maximum size limit. Please use more specific parameters or pagination.\n"
    )

    # Calculate safe truncation size
    trunc_size = max_size - len(warning_msg)
    if trunc_size < 200:  # Ensure we have some minimal content
        trunc_size = 200

    # Return truncated JSON with error message
    return json_str[:trunc_size] + warning_msg


@mcp.tool()
def readme_howto_use_clickhouse_tools() -> str:
    """Returns a guide on how to use the ClickHouse tools.
    """
    return (
        """
        Clickhouse is a column based database used in PyTorch CI.
        It is used to store and query test results and other data.

        This tool allows you to run queries, explain queries, and get the schema of the ClickHouse database.

        To run a query, use the `run_clickhouse_query` tool with a valid ClickHouse query string.
        To get the schema of a table, use the `get_clickhouse_schema` tool with the table name.
        To explain a query, use the `explain_clickhouse_query` tool with a valid ClickHouse query string.
        To get the list of tables, use the `get_clickhouse_tables` tool.
        """)


@mcp.tool()
def run_clickhouse_query(query: str) -> str:
    """Runs a ClickHouse query and returns the result as a JSON file in /tmp.
    Args:
        query (str): The ClickHouse query to execute

    Returns:
        str: The filepath of the JSON file containing the query result.
    """

    client = get_clickhouse_client()

    res: Optional[clickhouse_connect.driver.query.QueryResult] = client.query(
        query)
    if res is None:
        return "No results found for the query."
    if res.result_rows is None or len(res.result_rows) == 0:
        return "No data returned from the query."
    # Convert to JSON string with size limit
    json_result = safe_json_dumps(
        res.result_rows, indent=2, max_size=MAX_RESPONSE_SIZE
    )
    if json_result is None:
        return "Error: Failed to convert result to JSON."
    # Save to a temporary file - generate the filename to be unique
    filename = f"/tmp/clickhouse_query_result_{os.getpid()}.json"
    try:
        with open(filename, "w") as f:
            f.write(json_result)
    except IOError as e:
        return f"Error: Failed to write result to file. {e}"
    # Return the file path
    return filename


@mcp.tool()
def get_clickhouse_schema(table_name: str) -> str:
    """Get the schema of a ClickHouse table.

    Args:
        table_name (str): The name of the table to get the schema for

    Returns:
        str: The schema of the table as a JSON string
    """
    client = get_clickhouse_client()
    try:
        res = client.query(f"DESCRIBE TABLE {table_name}")
        if res is None or res.result_rows is None or len(res.result_rows) == 0:
            return "No data returned from the query."
        # Convert to JSON string with size limit
        json_result = safe_json_dumps(res.result_rows, indent=2)
        return json_result
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def explain_clickhouse_query(query: str) -> str:
    """Explain a ClickHouse query and return the result as a JSON string.

    Args:
        query (str): The ClickHouse query to explain

    Returns:
        str: The explanation of the query as a JSON string
    """
    client = get_clickhouse_client()
    try:
        res = client.query(f"EXPLAIN {query}")
        if res is None or res.result_rows is None or len(res.result_rows) == 0:
            return "No data returned from the query."
        # Convert to JSON string with size limit
        json_result = safe_json_dumps(res.result_rows, indent=2)
        return json_result
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def get_clickhouse_tables() -> str:
    """Get the list of tables in the ClickHouse database.

    Returns:
        str: The list of tables as a JSON string
    """
    client = get_clickhouse_client()
    try:
        res = client.query("SHOW TABLES")
        if res is None or res.result_rows is None or len(res.result_rows) == 0:
            return "No data returned from the query."
        # Convert to JSON string with size limit
        json_result = safe_json_dumps(res.result_rows, indent=2)
        return json_result
    except Exception as e:
        return f"Error: {e}"


# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()
