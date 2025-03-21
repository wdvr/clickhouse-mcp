# ClickHouse MCP

This project provides [MCP](https://modelcontextprotocol.io/) server for ClickHouse, including MCP tools to read the
ClickHouse database schema, explain queries, and perform semantic search over the ClickHouse documentation.

## Usage

### 1. Installing

This package can be installed directly from GitHub using pip:

```bash
pip install -e git+https://github.com/izaitsevfb/clickhouse-mcp.git#egg=clickhouse_mcp
```

(Once installed, you can run the MCP server from anywhere: `python -m clickhouse_mcp`)



### 2. Integration with Claude Code

1. Install the package
2. Put the [.mcp.json](https://github.com/izaitsevfb/clickhouse-mcp/blob/main/.mcp.json) file in
   directory where Claude Code is running. Alternatively, add `python -m clickhouse_mcp` with `claude mcp add`


### 3. Running

Add `.env` file in the directory where you're running claude code or export required environment variables
for the session. 

See [`.env.example`](.env.example) for the list of required variables related to the ClickHouse database.

AWS credentials must also be set: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` 
for semantic search to work.



# Development

## Development Installation

1. Clone the repository
2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
3. Install in development mode:
   ```bash
   pip install -e .
   ```

## Testing

Run unit tests for the improved chunking implementation:

```bash
python -m unittest tests/test_chunk_md_improved.py
```

# Tools

## Running Chunking

```bash
python tools/chunk_md.py --save
```

## Analyzing Chunk Size Distribution (after chunking is done)

```bash
python analyze_index_with_histogram.py
```

See the [Tools README](tools/README.md) for more details.

# Project Structure

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
- `index/`: Contains the FAISS index and document chunks for vector search
- `src/clickhouse_mcp/`: The main package module

