"""ClickHouse MCP server module."""

__version__ = "0.1.0"

# Default Bedrock model ID and region for embeddings
DEFAULT_BEDROCK_MODEL = "amazon.titan-embed-text-v2:0"
DEFAULT_REGION = "us-east-1"

__all__ = ["mcp_server", "docs_search", "DEFAULT_BEDROCK_MODEL", "DEFAULT_REGION"]