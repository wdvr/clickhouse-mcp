import clickhouse_connect.driver.ctypes
import clickhouse_connect.driver.types
import clickhouse_connect.driverc
from . import DEFAULT_BEDROCK_MODEL, DEFAULT_REGION
from .vector_search import load_faiss_index, vector_search, get_default_index_path
from langchain_community.vectorstores import FAISS
from langchain_aws import BedrockEmbeddings
import hashlib
import clickhouse_connect
import json
import datetime
from typing import Optional, Any, Dict, List
import clickhouse_connect.common
import clickhouse_connect.driver
import clickhouse_connect.driver.client
import clickhouse_connect.driver.exceptions
import clickhouse_connect.driver.query
from mcp.server.fastmcp import FastMCP, Context
import os
import time
import requests
import sqlfluff
import tempfile

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

# Import required modules for vector search


# Create an MCP server
mcp = FastMCP("PyTorch ClickHouse MCP")

clickhouse_client: Optional[clickhouse_connect.driver.client.Client] = None

# Maximum response size in bytes - set to a conservative size for context window efficiency
MAX_RESPONSE_SIZE = 10 * 1024  # 10KB


def get_clickhouse_client() -> clickhouse_connect.driver.client.Client:
    """Get the ClickHouse client instance.

    Returns:
        clickhouse_connect.Client: The ClickHouse client.

    Raises:
        ValueError: If any required environment variables are missing
        Exception: If connection to ClickHouse fails
    """
    global clickhouse_client
    if clickhouse_client is None:
        # Check for required environment variables
        host = os.getenv("CLICKHOUSE_HOST")
        port = os.getenv("CLICKHOUSE_PORT")
        username = os.getenv("CLICKHOUSE_USER")
        password = os.getenv("CLICKHOUSE_PASSWORD")

        if not all([host, port, username, password]):
            missing = []
            if not host:
                missing.append("CLICKHOUSE_HOST")
            if not port:
                missing.append("CLICKHOUSE_PORT")
            if not username:
                missing.append("CLICKHOUSE_USER")
            if not password:
                missing.append("CLICKHOUSE_PASSWORD")
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}")

        clickhouse_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database="default",
            secure=True
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

        To run a query, use the `run_clickhouse_query` tool with a valid ClickHouse query string. Note that the query can't have a ; at the end.
        To get the schema of a table, use the `get_clickhouse_schema` tool with the table name.
        To explain a query, use the `explain_clickhouse_query` tool with a valid ClickHouse query string.
        To get the list of tables, use the `get_clickhouse_tables` tool.
        Use `semantic_search_docs` to search specific clickhouse functions and syntax in case of doubt or syntax errors.
        Use `lint_clickhouse_query` to validate and format your SQL queries according to best practices.

        Use this to create and run queries if user asks things like: 'how long does the average macos build job take?'
        """)


@mcp.tool()
def run_clickhouse_query(query: str) -> Dict[str, Any]:
    """Runs a ClickHouse query and returns the result as a JSON file in /tmp as well as some statistics.
    Args:
        query (str): The ClickHouse query to execute

    Returns:
        result['result_file']: A file containing the result of the query as a JSON string
        result['time']: The time taken to execute the query
        result['rows']: The number of rows returned by the query
        result['columns']: The number of columns returned by the query
        result['first_row']: The first row of the result set
        result['error']: An error message if the query failed
        result['hash']: The hash of the query result

    """
    start_time = time.time()  # Start timing the query execution
    error = None

    if not query or not query.strip():
        return {
            "result_file": None,
            "time": 0,
            "data": None,
            "columns": [],
            'first_row': None,
            "error": "Error: Empty query provided",
            "hash": None
        }

    try:
        client = get_clickhouse_client()
    except ValueError as e:
        # Handle missing environment variables
        return {
            "result_file": None,
            "time": time.time() - start_time,
            "data": None,
            "columns": [],
            "first_row": None,
            "error": f"Configuration error: {str(e)}",
            "hash": None
        }
    except Exception as e:
        # Handle connection errors
        return {
            "result_file": None,
            "time": time.time() - start_time,
            "data": None,
            "columns": [],
            "first_row": None,
            "error": f"Connection error: Failed to connect to ClickHouse server: {str(e)}",
            "hash": None
        }

    try:
        query = query.replace("\\n", "\n").strip()
        res: Optional[clickhouse_connect.driver.query.QueryResult] = client.query(
            query)
        end_time = time.time()  # End timing the query execution

        column_names = res.column_names
        # dump to json, with converting datetime to string

        def datetime_serializer(obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        json_result = json.dumps(
            res.result_rows, indent=1, default=datetime_serializer)

        # Save to a temporary file - generate the filename to be unique
        filename = f"/tmp/clickhouse_query_result_{os.getpid()}.json"

        try:
            with open(filename, "w") as f:
                f.write(json_result)  # Write the JSON result to the file
        except IOError as e:
            return {
                "result_file": None,
                "time": end_time - start_time,
                "data": None,
                "columns": column_names,
                "first_row": None,
                "error": f"File system error: Failed to write result to file: {str(e)}",
                "hash": None
            }

        json_data = json.loads(json_result)

        # Return the file path
        return {
            "result_file": filename,
            "time": end_time - start_time,
            "data": len(json_result),
            "columns": column_names,
            "first_row": json_data[0] if json_data and len(json_data) else None,
            "error": error,
            "hash": hashlib.sha256(json_result.encode()).hexdigest()
        }

    except clickhouse_connect.driver.exceptions.ClickHouseError as e:
        return {
            "result_file": None,
            "time": time.time() - start_time,
            "data": None,
            "columns": [],
            "first_row": None,
            "error": f"Query error: {str(e)}",
            "hash": None
        }
    except Exception as e:
        return {
            "result_file": None,
            "time": time.time() - start_time,
            "data": None,
            "columns": [],
            "first_row": None,
            "error": f"Unexpected error during query execution: {str(e)}",
            "hash": None
        }


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
def get_slow_queries(last_x_hours: int) -> str:
    """Get the list of slow queries from the ClickHouse system table.

    Returns:
        str: The name of the file containing the slow queries as a JSON string
    """

    QUERY = """
    SELECT * FROM (
    SELECT
        round(avg(query_duration_ms)) AS realTimeMSAvg,
        sum(query_duration_ms) as realTimeMSTotal,
        round(quantile(0.5)(query_duration_ms)) as realTimeMSP50,
        avg(memory_usage) as memoryBytesAvg,
        sum(memory_usage) as memoryBytesTotal,
        quantile(0.5)(memory_usage) as memoryBytesP50,
        count(*) as num,
        left(query_id, -37) as name
    FROM
        clusterAllReplicas(default, system.query_log)
    WHERE
        event_time >= now() - INTERVAL {last_x_hours} HOUR
        AND event_time < now()
        AND initial_user = 'hud_user'
        AND length(query_id) > 37
        AND type = 'QueryFinish'
        AND left(query_id, -37) != 'adhoc'
    GROUP BY
        name
    ) WHERE
    realTimeMSAvg > 1000
    ORDER BY num DESC
    """

    client = get_clickhouse_client()
    try:
        res = client.query(QUERY.format(last_x_hours=last_x_hours))
        if res is None or res.result_rows is None or len(res.result_rows) == 0:
            return "No data returned from the query."
        # Find top 5 slowest
        slowest_queries = sorted(
            res.result_rows, key=lambda x: x[0], reverse=True)[:5]
        filename = f"/tmp/clickhouse_slow_queries_{time.time()}.json"
        json_result = json.dumps(res.result_rows, indent=2)
        with open(filename, "w") as f:
            f.write(json_result)
        return {
            "result_file": filename,
            "top_5_called": [{"name": r[-1], "avg_dur": r[0], "num_calls": r[-2]} for r in res.result_rows[:5]],
            "top_5_slowest": [{"name": r[-1], "avg_dur": r[0], "num_calls": r[-2]} for r in slowest_queries]
        }
    except clickhouse_connect.driver.exceptions.ClickHouseError as e:
        return f"Clickhouse query error: {e}"
    except Exception as e:
        return f"Unexpected error: {e}"


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


@mcp.tool()
def get_query_by_name(query_name: str, include_params: Optional[bool] = False) -> str:
    """ Get the ClickHouse query by its name.
    Args:
        query_name (str): The name of the query to get
        include_params (bool): Whether to include the parameters from params.json in the query
    Returns:
        str: The ClickHouse query as a JSON string
    """
    result_json = {}

    query_url = f"https://raw.githubusercontent.com/pytorch/test-infra/refs/heads/main/torchci/clickhouse_queries/{query_name}/query.sql"
    params_url = f"https://raw.githubusercontent.com/pytorch/test-infra/refs/heads/main/torchci/clickhouse_queries/{query_name}/params.json"

    try:
        query = requests.get(query_url).text
        # Fix literal '\n' character sequences (2 chars) with actual newlines
        query = query.replace("\\n", "\n").strip()
        result_json["query"] = query
    except Exception as e:
        result_json["error"] = f"Error: Failed to get query from {query_url}. {e}"
        return json.dumps(result_json)

    # get the params from the url if include_params is True
    if include_params:
        try:
            params = requests.get(params_url).text
            params_dict = json.loads(params)
            result_json["params"] = params_dict
        except Exception as e:
            result_json["error"] = f"Error: Failed to get params from {params_url}. {e}"

    return json.dumps(result_json)


# Vector store singleton
vector_store_instance: Optional[FAISS] = None


def get_vector_store() -> FAISS:
    """Get or initialize the vector store singleton.

    Returns:
        FAISS: The loaded vector store
    """
    global vector_store_instance

    if vector_store_instance is None:
        # Initialize embeddings
        embeddings = BedrockEmbeddings(
            region_name=DEFAULT_REGION,
            model_id=DEFAULT_BEDROCK_MODEL
        )

        # Load the FAISS index
        index_path = get_default_index_path()
        vector_store_instance = load_faiss_index(index_path, embeddings)

    return vector_store_instance


@mcp.tool()
def semantic_search_docs(
    query: str,
    page: int = 1,
    per_page: int = 3,
    limit: int = 300
) -> str:
    """Performs semantic search over ClickHouse documentation.

    Args:
        query: The search query text
        page: Page number (starting from 1)
        per_page: Number of results per page
        limit: Maximum character length for each result's content

    Returns:
        The formatted search results with markdown content
    """
    try:
        # Get the vector store singleton
        vector_store = get_vector_store()

        # Calculate pagination offsets
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page

        # We need to fetch enough results to cover the requested page
        num_results = end_idx

        # Perform the semantic search
        search_results = vector_search(vector_store, query, num_results)

        # Apply pagination
        paginated_results = search_results[start_idx:end_idx]

        # Prepare result text with clear delimiters
        results_text = f"Search results for: '{query}'\n\n"
        results_text += f"Page {page} of {(len(search_results) + per_page - 1) // per_page} "
        results_text += f"({len(search_results)} total results)\n\n"

        # Format each result with delimiters
        for i, result in enumerate(paginated_results):
            results_text += f"==== RESULT {i+1} ====\n"
            results_text += f"DOCUMENT: {result.metadata.get('document_title', 'Unknown')}\n"
            results_text += f"SECTION: {result.metadata.get('section_title', 'Unknown')}\n"
            results_text += f"SOURCE: {result.metadata.get('path', 'Unknown')}\n"
            results_text += "CONTENT:\n"

            # Include content with truncation if needed
            content = result.page_content
            if limit and len(content) > limit:
                content = content[:limit] + "...(truncated)"

            results_text += content + "\n"
            results_text += "==================\n\n"

        # Add pagination instructions
        if page > 1:
            results_text += "For previous results: Use page=" + \
                str(page-1) + "\n"
        if page < (len(search_results) + per_page - 1) // per_page:
            results_text += "For more results: Use page=" + str(page+1) + "\n"

        return results_text

    except FileNotFoundError:
        return "ERROR: FAISS index not found. Please make sure the index has been created."
    except Exception as e:
        return f"ERROR: Search failed: {str(e)}"


@mcp.tool()
def lint_clickhouse_query(
    query: str,
    rule_exclude: Optional[str] = None
) -> Dict[str, Any]:
    """Lint a ClickHouse SQL query using SQLFluff.

    Args:
        query (str): The ClickHouse SQL query to lint
        rule_exclude (Optional[str]): Comma-separated list of rules to exclude (e.g. "L001,L002")

    Returns:
        Dict containing:
        - 'status': Whether the query passed linting ('pass' or 'fail')
        - 'errors_count': Number of linting errors found
        - 'errors': List of linting errors with details
        - 'formatted_query': Formatted SQL query (only if different from input)
    """
    if not query or not query.strip():
        return {
            "status": "fail",
            "errors_count": 1,
            "errors": ["Empty query provided"],
            "formatted_query": None
        }
    
    try:
        # Set up config for sqlfluff
        config = {
            "dialect": "clickhouse"
        }
        if rule_exclude:
            config["exclude_rules"] = rule_exclude.split(",")
        
        # Create a temporary file for better error reporting
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp_file:
            tmp_file.write(query)
            tmp_path = tmp_file.name
        
        try:
            # Lint the query
            linting_result = sqlfluff.lint(tmp_path, config=config)
            
            # Format the query
            formatted_result = sqlfluff.fix(tmp_path, config=config)
            formatted_query = formatted_result.get("fix_str", query)
            
            # Process the linting results
            errors = []
            for violation in linting_result:
                errors.append({
                    "rule": violation.rule_code(),
                    "description": violation.description(),
                    "line": violation.line_no,
                    "position": violation.line_pos,
                    "context": violation.line_str if hasattr(violation, "line_str") else None
                })
            
            # Return structured results
            is_passing = len(errors) == 0
            
            # Only include formatted_query if it's different from input and there were errors
            formatted_query_output = None
            if not is_passing and formatted_query != query:
                formatted_query_output = formatted_query
                
            return {
                "status": "pass" if is_passing else "fail",
                "errors_count": len(errors),
                "errors": errors,
                "formatted_query": formatted_query_output
            }
        finally:
            # Clean up the temporary file
            os.unlink(tmp_path)
    
    except Exception as e:
        return {
            "status": "error",
            "errors_count": 1,
            "errors": [f"Linting error: {str(e)}"],
            "formatted_query": None
        }


# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()
