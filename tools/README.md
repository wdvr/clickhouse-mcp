# ClickHouse MCP Tools

This directory contains tools for processing and working with ClickHouse documentation.

## MCP Tools

### MCP Server: `src/clickhouse_mcp/mcp_server.py` 

The MCP server provides tools for interacting with ClickHouse data and documentation:

```bash
# Start the MCP server
python -m clickhouse_mcp.mcp_server
```

Available MCP tools:

1. **ClickHouse Database Tools**
   - `run_clickhouse_query`: Run ClickHouse queries and return results
   - `get_clickhouse_schema`: Get schema information for a table
   - `explain_clickhouse_query`: Get query explanation
   - `get_clickhouse_tables`: List available tables

2. **Documentation Search**
   - `semantic_search_docs`: Perform semantic search over ClickHouse documentation
     - Parameters:
       - `query`: Search query text
       - `page`: Page number (default: 1)
       - `per_page`: Results per page (default: 3)
       - `limit`: Max character length for result content (default: 300)
     - Returns plain text with markdown results and clear section delimiters

## Chunking Tools

### Original Chunking: `chunk_md.py`

The original chunking implementation splits markdown documents by H1, H2, and H3 headers.

```bash
python tools/chunk_md.py --dir path/to/docs --output output.pkl --save --preview
```

Key issues with the original implementation:
- Produces extremely small chunks (as small as 46 characters) for empty section headers
- Creates overly large chunks (up to 152,844 characters) for large intro sections
- Does not have size controls, resulting in inconsistent chunk sizes

## Search Tools

### Keyword Search: `query_docs.py`

A tool to search documentation chunks using simple keyword matching:

```bash
# Basic keyword search
python tools/query_docs.py --query "your query here" --num-results 5

# View random samples from the corpus
python tools/query_docs.py --sample 3
```

### Semantic Search: `query_docs.py --semantic`

Search documentation using semantic search with FAISS embeddings:

```bash
# Semantic search using AWS Bedrock embeddings
python tools/query_docs.py --query "your query here" --semantic --num-results 5

# Use a custom index path
python tools/query_docs.py --query "your query here" --semantic --index-path /path/to/index
```

### Vector Index Creation: `create_faiss_index.py`

Creates a FAISS vector index from the chunked documentation:

```bash
# Create a full index with default settings (saves to index/faiss_index)
python tools/create_faiss_index.py

# Create a test index with limited documents
python tools/create_faiss_index.py --test --num-results 20 --output ./index/test_index

# Create index only with documents matching a specific query
python tools/create_faiss_index.py --query "table functions" --output ./index/table_functions_index
```

Requirements for vector search:
- AWS credentials with AWS Bedrock access
- Required packages: `langchain-aws`, `faiss-cpu`, `langchain`, `langchain-community`

## Analysis Tools

### Basic Analysis: `analyze_index.py`

Simple analysis showing the largest and smallest chunks from a pickle file:

```bash
python analyze_index.py
```

### Detailed Analysis: `analyze_index_with_histogram.py`

Creates a detailed markdown report with histograms and statistics:

```bash
python analyze_index_with_histogram.py
```

The output includes:
- Basic statistics (min, max, average, median chunk sizes)
- Size distribution histograms
- Sample chunks across the size distribution
- Detailed examples of smallest and largest chunks