# ClickHouse MCP

This project provides [MCP](https://modelcontextprotocol.io/) server for ClickHouse, including MCP tools to read the
ClickHouse database schema, explain queries, and perform semantic search over the ClickHouse documentation.

## Usage

### Installation

0. Create a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate
```


1. Install this package directly from GitHub using pip:

```bash
pip install -e git+https://github.com/izaitsevfb/clickhouse-mcp.git#egg=clickhouse_mcp
```

(Just FYI: once installed, the MCP server could be run as: `python -m clickhouse_mcp`)


2. Add this MCP server to claude code:

```bash
claude mcp add-json clickhouse '{ "type": "streamable_http", "url": "http://localhost:8000/mcp" }'
```

Note: by default mcp config applies only to running in the current directory. If you want to use is globally, add 
`--scope user` the command above (e.g. `claude mcp add-json --scope user clickhouse  ...`).


3. Setup environment variables

Add `.env` file in the directory where you're running claude code or export required environment variables
for the session. 

See [`.env.example`](.env.example) for the list of required variables related to the ClickHouse database.

AWS credentials must also be set: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` 
for semantic search to work.

4. Start the MCP server:

```bash
python -m clickhouse_mcp
```

5. Run claude code as usual:

```bash
claude
```



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

