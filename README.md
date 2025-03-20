# ClickHouse MCP

A Python module for ClickHouse MCP server integration and documentation search.

## Setup

1. Create and activate the virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

## Usage

### 1. Query Documentation

Query ClickHouse documentation chunks using simple keyword search:

```bash
python tools/query_docs.py --query "CREATE TABLE" --num-results 3
```

Sample random chunks from the documentation:

```bash
python tools/query_docs.py --sample 5
```

### 2. Create FAISS Index

Create a FAISS vector index from document chunks using AWS Bedrock embeddings:

```bash
# Create full index
python tools/create_faiss_index.py --output ./index/faiss_index

# Test mode with limited documents
python tools/create_faiss_index.py --test --output ./index/test_faiss_index

# Test mode with query filtering
python tools/create_faiss_index.py --test --query "table creation" --output ./index/test_faiss_index

# Use a different Bedrock model or region
python tools/create_faiss_index.py --model "amazon.titan-embed-text-v1" --region "us-west-2"
```

Requirements:
- AWS credentials with Bedrock access (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
- Required packages: langchain-aws, faiss-cpu, langchain

### 3. MCP Server

Run the MCP server:

```bash
python -m src.clickhouse_mcp
```

Inspect the MCP server:
```bash
npx @modelcontextprotocol/inspector python -m clickhouse_mcp
```

## Development

### Code Quality

Run type checking with mypy:
```bash
mypy src tests
```

Run linting with ruff:
```bash
ruff check src tests
```

### Running Tests

Execute the test suite:
```bash
# Run all tests (excluding the clickhouse and venv directories)
python run_tests.py

# Or using unittest directly (only looks in tests directory)
python -m unittest discover tests
```