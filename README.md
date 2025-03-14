# ClickHouse MCP

A Python module for ClickHouse MCP server integration.

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
python -m unittest discover tests
```

## Usage

Basic usage example:
```python
```