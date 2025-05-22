"""ClickHouse MCP server module."""

__version__ = "0.1.0"

import importlib, http; importlib.import_module("http.client")

# Default Bedrock model ID and region for embeddings
DEFAULT_BEDROCK_MODEL = "amazon.titan-embed-text-v2:0"
DEFAULT_REGION = "us-east-1"

# Default paths
DEFAULT_INDEX_DIR = "index"
DEFAULT_FAISS_INDEX_NAME = "faiss_index"

__all__ = ["mcp_server", "docs_search", "vector_search", 
           "DEFAULT_BEDROCK_MODEL", "DEFAULT_REGION", 
           "DEFAULT_INDEX_DIR", "DEFAULT_FAISS_INDEX_NAME"]
