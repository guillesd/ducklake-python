
"""
Test suite for DuckLake Python Client

Tests the client by creating tables with DuckDB's DuckLake extension
and reading them with our custom client.
"""

import pytest
import duckdb
import pyarrow as pa
from pathlib import Path
import tempfile
import shutil
from sqlalchemy import create_engine, text
from ducklake_python import DucklakeClient


@pytest.fixture(scope="session")
def postgres_connection_str():
    """PostgreSQL connection string for the test catalog."""
    return "postgresql://user:simple@localhost:5432"

@pytest.fixture(scope="session", autouse=True)
def create_and_drop_database_catalog(request, postgres_connection_str, catalog_database_name):
    """Create and drop the test database catalog."""
    def _teardown():
        conn.execute(text(f"DROP DATABASE {catalog_database_name}"))
        conn.close()
        engine.dispose()
    engine = create_engine(postgres_connection_str, isolation_level="AUTOCOMMIT")
    conn = engine.connect()
    conn.execute(text(f"CREATE DATABASE {catalog_database_name}"))
    request.addfinalizer(_teardown)
    yield

@pytest.fixture(scope="session")
def catalog_database_name():
    return "ducklake_test"

@pytest.fixture(scope="session")
def data_directory():
    """Create a temporary directory for test data files."""
    temp_dir = tempfile.mkdtemp(prefix="ducklake_test_")
    yield temp_dir
    # Cleanup after all tests
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def duckdb_conn(postgres_connection_str, catalog_database_name, data_directory):
    """
    Create a DuckDB connection with DuckLake extension configured.
    Creates a new catalog for each test function.
    """
    conn = duckdb.connect(':memory:')
    
    # Install and load the ducklake extension
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    
    # Create a new catalog for this test
    # Use a unique catalog name to avoid conflicts
    catalog_name = "ducklake_test"

    conn.execute(f"ATTACH 'ducklake:postgres:{postgres_connection_str}/{catalog_database_name}' AS {catalog_name} (DATA_PATH '{data_directory}')")

    # Set the catalog as default
    conn.execute(f"USE {catalog_name}")
    
    yield conn
    
    # Cleanup: drop the catalog
    
    conn.close()


@pytest.fixture(scope="function")
def ducklake_client(postgres_connection_str, data_directory, catalog_database_name):
    """Create a DucklakeClient instance for testing."""
    dc = DucklakeClient(
        connection_string=f"{postgres_connection_str}/{catalog_database_name}",
        data_path=data_directory
    )
    yield dc
    dc.close()


def test_simple_table_read(duckdb_conn, ducklake_client):
    """Test creating and reading a simple table."""
    conn = duckdb_conn
    
    # Create schema and table using DuckDB
    conn.execute("CREATE SCHEMA test_schema")
    conn.execute("""
        CREATE TABLE test_schema.simple_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)
    
    # Insert test data
    conn.execute("""
        INSERT INTO test_schema.simple_table VALUES
        (1, 'Alice', 10.5),
        (2, 'Bob', 20.3),
        (3, 'Charlie', 30.7)
    """)

    
    # Read with our custom client
    dc_result = ducklake_client.read_table("test_schema", "simple_table")
    
    # Read with DuckDB DuckLake client
    duckdb_result = conn.execute("SELECT * FROM test_schema.simple_table ORDER BY id").fetch_arrow_table()
    
    # Compare results
    # Sort both tables by id to ensure consistent order
    dc_result_sorted = dc_result.sort_by([("id", "ascending")])
    
    assert dc_result_sorted.equals(duckdb_result), "Tables should be equal"


def test_table_with_nulls(duckdb_conn, ducklake_client):
    """Test reading a table with NULL values."""
    conn = duckdb_conn
    
    conn.execute("CREATE SCHEMA test_schema_nulls")
    conn.execute("""
        CREATE TABLE test_schema_nulls.nullable_table (
            id INTEGER,
            optional_value VARCHAR
        )
    """)
    
    conn.execute("""
        INSERT INTO test_schema_nulls.nullable_table VALUES
        (1, 'present'),
        (2, NULL),
        (3, 'also_present')
    """)
    
    # Read with custom client
    dc_client_result = ducklake_client.read_table("test_schema_nulls", "nullable_table")
    
    # Read with DuckDB
    duckdb_result = conn.execute("SELECT * FROM test_schema_nulls.nullable_table ORDER BY id").fetch_arrow_table()
    
    # Compare
    dc_result_sorted = dc_client_result.sort_by([("id", "ascending")])
    assert dc_result_sorted.equals(duckdb_result), "Tables with NULLs should be equal"


def test_multiple_batches(duckdb_conn, ducklake_client):
    """Test reading a table with multiple data files."""
    conn = duckdb_conn
    
    conn.execute("CREATE SCHEMA test_schema_batches")
    conn.execute("""
        CREATE TABLE test_schema_batches.multi_file_table (
            id INTEGER,
            value VARCHAR
        )
    """)
    
    # Insert data in multiple batches to create multiple files
    for i in range(3):
        conn.execute(f"""
            INSERT INTO test_schema_batches.multi_file_table 
            SELECT range AS id, 'batch_{i}_' || range AS value
            FROM range({i * 100}, {(i + 1) * 100})
        """)
    
    # Read with custom client
    dc_client_result = ducklake_client.read_table("test_schema_batches", "multi_file_table")
    
    # Read with DuckDB
    duckdb_result = conn.execute("SELECT * FROM test_schema_batches.multi_file_table ORDER BY id").fetch_arrow_table()
    
    # Compare
    dc_result_sorted = dc_client_result.sort_by([("id", "ascending")])
    assert dc_result_sorted.equals(duckdb_result), "Multi-file tables should be equal"
    assert dc_result_sorted.num_rows == 300, "Should have 300 rows"


def test_column_selection(duckdb_conn, ducklake_client):
    """Test reading specific columns from a table."""
    conn = duckdb_conn
    
    conn.execute("CREATE SCHEMA test_schema_cols")
    conn.execute("""
        CREATE TABLE test_schema_cols.wide_table (
            id INTEGER,
            col1 VARCHAR,
            col2 INTEGER,
            col3 DOUBLE,
            col4 VARCHAR
        )
    """)
    
    conn.execute("""
        INSERT INTO test_schema_cols.wide_table VALUES
        (1, 'a', 10, 1.1, 'x'),
        (2, 'b', 20, 2.2, 'y'),
        (3, 'c', 30, 3.3, 'z')
    """)
    
    # Read only specific columns with custom client
    dc_client_result = ducklake_client.read_table(
        "test_schema_cols", 
        "wide_table",
        columns=["id", "col2"]
    )
    
    # Read same columns with DuckDB
    duckdb_result = conn.execute("SELECT id, col2 FROM test_schema_cols.wide_table ORDER BY id").fetch_arrow_table()
    
    # Compare
    dc_result_sorted = dc_client_result.sort_by([("id", "ascending")])
    assert dc_result_sorted.equals(duckdb_result), "Partial column reads should be equal"
    assert dc_result_sorted.num_columns == 2, "Should have 2 columns"


# def test_empty_table(duckdb_conn, ducklake_client):
#     """Test reading an empty table."""
#     # TODO: fix problem with data type mapping or this test will never succeed.

#     conn = duckdb_conn
    
#     conn.execute("CREATE SCHEMA test_schema_empty")
#     conn.execute("""
#         CREATE TABLE test_schema_empty.empty_table (
#             id INTEGER,
#             name VARCHAR
#         )
#     """)
    
#     # Don't insert any data
    
#     # Read with custom client
#     dc_client_result = ducklake_client.read_table("test_schema_empty", "empty_table")
    
#     # Read with DuckDB
#     duckdb_result = conn.execute("SELECT * FROM test_schema_empty.empty_table").fetch_arrow_table()
    
#     # Compare
#     assert dc_client_result.equals(duckdb_result), "Empty tables should be equal"
#     assert dc_client_result.num_rows == 0, "Should have 0 rows"


def test_various_data_types(duckdb_conn, ducklake_client):
    """
    Test reading a table with various data types.
    Very good example of how stable type mapping is between DuckDB and Arrow, 
    which helps this very poor implementation.
    """
    conn = duckdb_conn
    
    conn.execute("CREATE SCHEMA test_schema_types")
    conn.execute("""
        CREATE TABLE test_schema_types.types_table (
            int_col INTEGER,
            bigint_col BIGINT,
            double_col DOUBLE,
            varchar_col VARCHAR,
            bool_col BOOLEAN,
            date_col DATE
        )
    """)
    
    conn.execute("""
        INSERT INTO test_schema_types.types_table VALUES
        (42, 1234567890, 3.14159, 'test string', true, DATE '2024-01-15'),
        (-100, -9876543210, -2.71828, 'another string', false, DATE '2023-12-25')
    """)
    
    # Read with custom client
    dc_result = ducklake_client.read_table("test_schema_types", "types_table")
    
    # Read with DuckDB
    duckdb_result = conn.execute("SELECT * FROM test_schema_types.types_table ORDER BY int_col").fetch_arrow_table()
    
    # Compare
    dc_result_sorted = dc_result.sort_by([("int_col", "ascending")])
    assert dc_result_sorted.equals(duckdb_result), "Tables with various types should be equal"


def test_list_operations(ducklake_client, duckdb_conn):
    """Test listing schemas and tables."""
    conn = duckdb_conn
    
    # Create multiple schemas and tables
    conn.execute("CREATE SCHEMA schema1")
    conn.execute("CREATE SCHEMA schema2")
    conn.execute("CREATE TABLE schema1.table1 (id INTEGER)")
    conn.execute("CREATE TABLE schema1.table2 (id INTEGER)")
    conn.execute("CREATE TABLE schema2.table3 (id INTEGER)")
    
    # List schemas
    schemas = ducklake_client.list_schemas()
    schema_names = [name for _, name in schemas]
    assert "schema1" in schema_names
    assert "schema2" in schema_names
    
    # Find schema1 id
    schema1_id = next(sid for sid, name in schemas if name == "schema1")
    
    # List tables in schema1
    tables = ducklake_client.list_tables(schema1_id)
    table_names = [name for _, name in tables]
    assert "table1" in table_names
    assert "table2" in table_names
    assert "table3" not in table_names  # This is in schema2


def test_get_table_columns(ducklake_client, duckdb_conn):
    """Test retrieving table column information."""
    conn = duckdb_conn
    
    conn.execute("CREATE SCHEMA test_schema_meta")
    conn.execute("""
        CREATE TABLE test_schema_meta.meta_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)
    
    # Get schema and table IDs
    schemas = ducklake_client.list_schemas()
    schema_id = next(sid for sid, name in schemas if name == "test_schema_meta")
    tables = ducklake_client.list_tables(schema_id)
    table_id = next(tid for tid, name in tables if name == "meta_table")
    
    # Get columns
    columns = ducklake_client.get_table_columns(table_id)
    
    assert len(columns) == 3
    column_names = [col.column_name for col in columns]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names

def test_read_with_delete_files(ducklake_client, duckdb_conn):
    """Test reading froma a table that has delete files."""
    conn = duckdb_conn
    
    # Create schema and table using DuckDB
    conn.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
    conn.execute("""
        CREATE TABLE test_schema.deletes_table (
            id INTEGER,
            name VARCHAR,
            value DOUBLE
        )
    """)
    
    # Insert test data
    conn.execute("""
        INSERT INTO test_schema.deletes_table VALUES
        (1, 'Alice', 10.5),
        (2, 'Bob', 20.3),
        (3, 'Charlie', 30.7)
    """)

    # Delete one row to create delete files
    conn.execute("""
        DELETE FROM test_schema.deletes_table WHERE id = 2
    """)
    
    # Read with our custom client
    dc_result = ducklake_client.read_table("test_schema", "deletes_table")

    # Read with DuckDB DuckLake client
    duckdb_result = conn.execute("SELECT * FROM test_schema.deletes_table ORDER BY id").fetch_arrow_table()
    # Compare results
    # Sort both tables by id to ensure consistent order
    dc_result_sorted = dc_result.sort_by([("id", "ascending")])

    assert dc_result_sorted.equals(duckdb_result), "Tables should be equal"