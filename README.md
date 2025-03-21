# ClickHouse MCP - Documentation Processing

This project provides tools for processing ClickHouse documentation, focusing on chunking markdown documents for embedding and search.

## Documentation Chunking

The project includes two chunking implementations:

1. **Original Chunking (`tools/chunk_md.py`)**
   - Splits markdown by H1, H2, and H3 headers
   - Does not control chunk sizes
   - Creates both very small (< 100 chars) and very large (> 100K chars) chunks

2. **Improved Chunking (`tools/chunk_md_improved.py`)**
   - Target chunk size: ~10,000 characters 
   - Maximum chunk size: 40,000 characters
   - Keeps small documents (≤15,000 chars) as single chunks
   - Chunks by H2 sections by default
   - Merges small sections (<1,000 chars)
   - Implements force chunking for oversized sections
   - Generates unique keys for all chunks

## Usage

### Running Chunking

```bash
python tools/chunk_md.py --save
```


### Analyzing Chunk Size Distribution

```bash
python analyze_index_with_histogram.py
```

## Testing

Run unit tests for the improved chunking implementation:

```bash
python -m unittest tests/test_chunk_md_improved.py
```

## Project Structure

- `tools/`: Chunking and processing tools
  - `chunk_md.py`: Original chunking implementation
  - `chunk_md_improved.py`: Improved chunking implementation
  - `query_docs.py`: Tool for querying the document index
- `tests/`: Unit tests
  - `test_chunk_md.py`: Tests for original chunking
  - `test_chunk_md_improved.py`: Tests for improved chunking
- `analyze_index.py`: Basic chunk size analysis
- `analyze_index_with_histogram.py`: Detailed analysis with histogram
- `run_improved_chunking.py`: Tool to compare chunking implementations

## Installation

1. Clone the repository
2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

## MCP Server

This project includes an MCP (Model Control Protocol) server with tools for ClickHouse database interaction and documentation search:

### Starting the Server

```bash
python -m src.clickhouse_mcp.mcp_server
```

### MCP Tools

1. **ClickHouse Database Tools**
   - `run_clickhouse_query`: Run ClickHouse queries
   - `get_clickhouse_schema`: Get schema for a table
   - `explain_clickhouse_query`: Explain query execution
   - `get_clickhouse_tables`: List available tables

2. **Documentation Search**
   - `semantic_search_docs`: Semantic search over ClickHouse documentation
     - Parameters:
       - `query`: Search query text
       - `page`: Page number (default: 1) 
       - `per_page`: Results per page (default: 3)
       - `limit`: Max content length (default: 300)
     - Uses a singleton pattern for efficient index loading
     - Returns clear, readable text output with document information and content

See the [Tools README](tools/README.md) for more details.