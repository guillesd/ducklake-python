# ducklake_python

A small Python client for reading DuckLake tables using SQLAlchemy and PyArrow.  
Includes tests that exercise integration between DuckDB (with the DuckLake extension) as a control. 

> This is a pet project to showcase how easy it can be to build a DuckLake client. Please don't use in prod.

## Installing

Using pip:

```
pip install https://github.com/guillesd/ducklake-python.git
```

## Usage example
- Basic usage of the client (context manager supported):

```python
from ducklake_python import DucklakeClient

conn_str = "postgresql://user:simple@localhost:5432/ducklake_test"
data_path = "/path/to/data"

with DucklakeClient(connection_string=conn_str, data_path=data_path) as client:
    table = client.read_table("my_schema", "my_table")
    print(table.schema)
    print(table.num_rows)
```

## Devlopment setup
- Using a plain virtualenv / pip:
```
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .  # installs package and dependencies from pyproject.toml
```

- Using `uv`:
```
uv sync
```

### Running tests
- Ensure Postgres is running and accessible at the connection string used by the tests (default: `postgresql://user:simple@localhost:5432`).
- Run from project root:
```
pytest -q
```

- With uv:
```
uv run pytest
```

### Notes about tests
- The test suite (tests/test_read.py) will:
  - create a test database (session-scoped fixture) and drop it at the end of the test session
  - use DuckDB (in-memory) and attach the DuckLake Postgres-backed catalog to create examples for this client.
  - create tables, insert data, and exercise read logic (including delete-file handling)

### Implementation notes
- The client opens a SQLAlchemy Engine for catalog access and disposes it in close(); it implements __enter__/__exit__ so it can be used as a context manager.
- PyArrow is used to read parquet data files; delete-file handling is implemented by reading delete files and filtering row positions.
- The tests assume certain behaviors from the DuckLake/DuckDB-produced metadata; adjust the delete-file parsing if your delete files use different conventions.
