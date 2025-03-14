#!/usr/bin/env python3
"""
PyTorch HUD MCP server entry point
"""

from clickhouse_mcp.mcp_server import mcp

def main():
    """Main entry point for the application"""
    print("Starting PyTorch Clickhouse MCP server...")
    mcp.run()

if __name__ == "__main__":
    main()