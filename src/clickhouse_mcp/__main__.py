#!/usr/bin/env python3
"""PyTorch HUD MCP server entry point."""

from clickhouse_mcp.mcp_server import mcp


def main() -> None:
    """Launch the MCP server using Streamable HTTP."""
    print("Starting PyTorch ClickHouse MCP server...")
    mcp.run(transport="sse", host="0.0.0.0")


if __name__ == "__main__":
    main()
