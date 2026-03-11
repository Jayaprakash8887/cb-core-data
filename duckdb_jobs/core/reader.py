"""
DuckDB Reader Helper
====================
Provides a simple API for Stage 2 jobs to query the pre-built DuckDB
database (created by ``initializer.py``) and return results as
pandas DataFrames or PySpark DataFrames.

Usage:
    from duckdb_jobs.core.reader import DuckDBReader

    reader = DuckDBReader()                        # default DB path
    df = reader.table("user_org_computed")          # full table as pandas DF
    df = reader.query("SELECT ... FROM ...")        # custom query
    spark_df = reader.to_spark(spark, "user_org_computed")   # PySpark DF
    reader.close()
"""

import os
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[1] / "output" / "igot.duckdb")


class DuckDBReader:
    """Read-only accessor for the pre-built DuckDB database."""

    def __init__(self, db_path: str = None):
        self._db_path = db_path or DEFAULT_DB_PATH
        if not os.path.exists(self._db_path):
            raise FileNotFoundError(
                f"DuckDB database not found: {self._db_path}. "
                "Run the initializer first."
            )
        self._con = duckdb.connect(database=self._db_path, read_only=True)

    # ── query helpers ─────────────────────────────────────────────────

    def query(self, sql: str):
        """Run a SQL query and return a pandas DataFrame."""
        return self._con.execute(sql).fetchdf()

    def table(self, name: str):
        """Return the full contents of *name* as a pandas DataFrame."""
        return self.query(f'SELECT * FROM "{name}"')

    def table_names(self):
        """List all table names in the database."""
        rows = self._con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        return [r[0] for r in rows]

    def row_count(self, name: str) -> int:
        """Return row count for *name*."""
        return self._con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]

    # ── PySpark interop ───────────────────────────────────────────────

    def to_spark(self, spark, table_name: str, sql: str = None):
        """
        Load a DuckDB table (or query result) as a PySpark DataFrame.

        Parameters
        ----------
        spark : SparkSession
        table_name : str
            If *sql* is None, reads the entire table.
        sql : str, optional
            Custom SQL — overrides table_name.
        """
        pdf = self.query(sql) if sql else self.table(table_name)
        return spark.createDataFrame(pdf)

    # ── lifecycle ─────────────────────────────────────────────────────

    def close(self):
        if self._con:
            self._con.close()
            self._con = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        self.close()
