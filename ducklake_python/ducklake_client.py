"""
DuckLake Python Client

A client for reading from DuckLake tables using SQLAlchemy and PyArrow.
"""

from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class TableColumn:
    """Represents a DuckLake table column."""
    column_id: int
    column_name: str
    column_type: str
    column_order: int


@dataclass
class DataFile:
    """Represents a data file with optional delete file."""
    data_file_path: str
    delete_file_path: Optional[str]
    path_is_relative: bool


class DucklakeClient:
    """Client for reading from DuckLake tables."""

    # TODO: Types are now based on type inferencing from parquet files. This is just very wrong
    # and will potentially mess up with schema evolution. Also empty tables cannot have types since
    # there are no parquet files to infer from.
    
    def __init__(self, connection_string: str, data_path: Optional[str] = None):
        """
        Initialize DuckLake client.
        
        Args:
            connection_string: SQLAlchemy connection string for the catalog database
            data_path: Base path for data files (used for relative paths)
        """
        self.engine: Engine = create_engine(connection_string)
        self.data_path = Path(data_path) if data_path else None
        self._closed = False
    
    def close(self) -> None:
        """Close underlying resources (dispose engine / connection pool)."""
        if self._closed:
            return
        try:
            # Dispose of SQLAlchemy Engine to close pooled DBAPI connections
            self.engine.dispose()
        finally:
            self._closed = True

    def __enter__(self):
        if self._closed:
            raise RuntimeError("DucklakeClient is closed")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_current_snapshot_id(self) -> int:
        """Get the most recent snapshot ID."""
        query = text("""
            SELECT snapshot_id
            FROM ducklake_snapshot
            WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            if result is None:
                raise ValueError("No snapshots found in the catalog")
            return result[0]
    
    def list_schemas(self, snapshot_id: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        List all schemas available at a given snapshot.
        
        Args:
            snapshot_id: Snapshot ID to query (uses current if None)
            
        Returns:
            List of (schema_id, schema_name) tuples
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        query = text("""
            SELECT schema_id, schema_name
            FROM ducklake_schema
            WHERE
                :snapshot_id >= begin_snapshot AND
                (:snapshot_id < end_snapshot OR end_snapshot IS NULL)
        """)
        
        with self.engine.connect() as conn:
            results = conn.execute(query, {"snapshot_id": snapshot_id}).fetchall()
            return [(row[0], row[1]) for row in results]
    
    def list_tables(self, schema_id: int, snapshot_id: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        List all tables in a schema at a given snapshot.
        
        Args:
            schema_id: Schema ID to query
            snapshot_id: Snapshot ID to query (uses current if None)
            
        Returns:
            List of (table_id, table_name) tuples
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        query = text("""
            SELECT table_id, table_name
            FROM ducklake_table
            WHERE
                schema_id = :schema_id AND
                :snapshot_id >= begin_snapshot AND
                (:snapshot_id < end_snapshot OR end_snapshot IS NULL)
        """)
        
        with self.engine.connect() as conn:
            results = conn.execute(
                query, 
                {"schema_id": schema_id, "snapshot_id": snapshot_id}
            ).fetchall()
            return [(row[0], row[1]) for row in results]
    
    def get_table_columns(self, table_id: int, snapshot_id: Optional[int] = None) -> List[TableColumn]:
        """
        Get the column structure of a table.
        
        Args:
            table_id: Table ID to query
            snapshot_id: Snapshot ID to query (uses current if None)
            
        Returns:
            List of TableColumn objects
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        query = text("""
            SELECT column_id, column_name, column_type, column_order
            FROM ducklake_column
            WHERE
                table_id = :table_id AND
                parent_column IS NULL AND
                :snapshot_id >= begin_snapshot AND
                (:snapshot_id < end_snapshot OR end_snapshot IS NULL)
            ORDER BY column_order
        """)
        
        with self.engine.connect() as conn:
            results = conn.execute(
                query,
                {"table_id": table_id, "snapshot_id": snapshot_id}
            ).fetchall()
            
            return [
                TableColumn(
                    column_id=row[0],
                    column_name=row[1],
                    column_type=row[2],
                    column_order=row[3]
                )
                for row in results
            ]
    
    def get_data_files(self, table_id: int, snapshot_id: Optional[int] = None) -> List[DataFile]:
        """
        Get the list of data files and associated delete files for a table.
        
        Args:
            table_id: Table ID to query
            snapshot_id: Snapshot ID to query (uses current if None)
            
        Returns:
            List of DataFile objects
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        query = text("""
            SELECT 
                data.path AS data_file_path,
                data.path_is_relative AS data_is_relative,
                del.path AS delete_file_path
            FROM ducklake_data_file AS data
            LEFT JOIN (
                SELECT data_file_id, path
                FROM ducklake_delete_file
                WHERE
                    :snapshot_id >= begin_snapshot AND
                    (:snapshot_id < end_snapshot OR end_snapshot IS NULL)
            ) AS del
            ON data.data_file_id = del.data_file_id
            WHERE
                data.table_id = :table_id AND
                :snapshot_id >= data.begin_snapshot AND
                (:snapshot_id < data.end_snapshot OR data.end_snapshot IS NULL)
            ORDER BY data.file_order
        """)
        with self.engine.connect() as conn:
            results = conn.execute(
                query,
                {"table_id": table_id, "snapshot_id": snapshot_id}
            ).fetchall()
            
            return [
                DataFile(
                    data_file_path=row[0],
                    path_is_relative=row[1],
                    delete_file_path=row[2]
                )
                for row in results
            ]
    
    def _resolve_path(self, path: str, is_relative: bool, table_name: str, schema_name: str) -> Path:
        """Resolve a file path (relative or absolute)."""
        if is_relative:
            if self.data_path is None:
                raise ValueError("Relative path provided but no data_path configured")
            return (self.data_path / schema_name /table_name / path).absolute()
        return Path(path).absolute()
    
    def _read_delete_row_ids(self, delete_file_path: Path) -> List[int]:
        """Read positions to delete from a delete file."""
        table = pq.read_table(str(delete_file_path))
        # Delete files contain row indices to be removed
        # Assuming the column is named 'row_id' or similar
        return table['pos'].to_pylist()

    def _read_table(
        self, 
        table_id: int, 
        table_name: str,
        schema_name: str,
        snapshot_id: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> pa.Table:
        """
        Read a complete table into memory.
        
        Args:
            table_id: Table ID to read
            snapshot_id: Snapshot ID to query (uses current if None)
            columns: Optional list of column names to read (reads all if None)
            
        Returns:
            PyArrow Table containing the data
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        # Get data files
        data_files = self.get_data_files(table_id, snapshot_id)
        
        if not data_files:
            # Return empty table with schema
            if columns:
                table_columns = [c for c in table_columns if c.column_name in columns]
            else:
                table_columns = self.get_table_columns(table_id, snapshot_id)
            
            # Create empty schema 
            # TODO: Map DuckLake types to PyArrow types properly. This is pretty bad.
            schema = pa.schema([
                pa.field(col.column_name, pa.string())  # Simplified type handling
                for col in table_columns
            ])
            return pa.Table.from_pydict({f.name: [] for f in schema}, schema=schema)
        
        # Read all data files
        arrow_tables = []
        for df in data_files:
            data_path = self._resolve_path(df.data_file_path, df.path_is_relative, table_name, schema_name)
            # Read the data file (already does projection pushdown)
            table = pq.read_table(str(data_path), columns=columns)
            
            # Apply delete file if present
            if df.delete_file_path:
                delete_path = self._resolve_path(df.delete_file_path, df.path_is_relative, table_name, schema_name)
                delete_row_ids = self._read_delete_row_ids(delete_path)
                print(delete_row_ids)
                print(table)
                
                # Filter out deleted rows (by position)
                # This feels super flaky and it is based on the assumption that order is kept
                row_indices = list(range(len(table)))
                keep_mask = [i not in delete_row_ids for i in row_indices]
                table = table.filter(pa.array(keep_mask))
            
            arrow_tables.append(table)
        
        # Concatenate all tables
        if len(arrow_tables) == 1:
            return arrow_tables[0]
        return pa.concat_tables(arrow_tables)
    
    def read_table(
        self,
        schema_name: str,
        table_name: str,
        snapshot_id: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> pa.Table:
        """
        Read a table by schema and table name.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            snapshot_id: Snapshot ID to query (uses current if None)
            columns: Optional list of column names to read
            
        Returns:
            PyArrow Table containing the data
        """
        if snapshot_id is None:
            snapshot_id = self.get_current_snapshot_id()
        
        # Find schema
        schemas = self.list_schemas(snapshot_id)
        schema_id = None
        for sid, sname in schemas:
            if sname == schema_name:
                schema_id = sid
                break
        
        if schema_id is None:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        # Find table
        tables = self.list_tables(schema_id, snapshot_id)
        table_id = None
        for tid, tname in tables:
            if tname == table_name:
                table_id = tid
                break
        
        if table_id is None:
            raise ValueError(f"Table '{table_name}' not found in schema '{schema_name}'")
        
        return self._read_table(table_id, table_name, schema_name, snapshot_id, columns)

    