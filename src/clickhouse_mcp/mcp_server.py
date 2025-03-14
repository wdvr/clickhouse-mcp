import json
from typing import Optional, Any, Dict
from mcp.server.fastmcp import FastMCP, Context


# Create an MCP server
mcp = FastMCP("PyTorch ClickHouse MCP")

# Maximum response size in bytes - set to a conservative size for context window efficiency
MAX_RESPONSE_SIZE = 10 * 1024  # 10KB


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
        """)


# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()

