"""
DuckDB Connection Helper

Provides a standardized way to open connections to the persistent DuckDB database.
All Stage 2 jobs import this module to read pre-computed tables.
"""

import os
import duckdb
from pathlib import Path

# Default database path — sits alongside the duckdb-jobs folder
_BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = str(_BASE_DIR / "output" / "igot.duckdb")


def get_connection(db_path: str = None, read_only: bool = False,
                   memory_limit: str = "8GB", threads: int = 8) -> duckdb.DuckDBPyConnection:
    """
    Open a connection to the persistent DuckDB database.

    Args:
        db_path:      Path to the .duckdb file.  Defaults to output/igot.duckdb.
        read_only:    True for Stage 2 consumers (concurrent reads allowed).
        memory_limit: DuckDB memory budget.
        threads:      Number of DuckDB worker threads.

    Returns:
        A duckdb.DuckDBPyConnection that the caller must .close() when done.
    """
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)

    con = duckdb.connect(database=path, read_only=read_only)
    con.execute(f"SET memory_limit = '{memory_limit}';")
    con.execute(f"SET threads = {threads};")
    con.execute(f"SET temp_directory = '{os.path.dirname(path)}/duckdb_tmp';")
    con.execute("SET preserve_insertion_order = false;")
    return con


def get_read_connection(db_path: str = None, memory_limit: str = "8GB",
                        threads: int = 8) -> duckdb.DuckDBPyConnection:
    """Convenience wrapper — opens the database in read-only mode."""
    return get_connection(db_path=db_path, read_only=True,
                          memory_limit=memory_limit, threads=threads)
