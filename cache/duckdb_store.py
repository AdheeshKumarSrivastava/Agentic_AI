from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
import pandas as pd


@dataclass
class DuckDBStore:
    """
    Maintains a local DuckDB catalog for cached Parquet snapshots.
    Enables offline analytics: you can query cached parquet files using DuckDB SQL.

    We keep a small metadata table in DuckDB:
      - cache_key (string)
      - parquet_path (string)
      - created_at (timestamp)

    Note: This does NOT mutate source DB. It's purely local.
    """

    duckdb_path: Path

    def __post_init__(self) -> None:
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.duckdb_path))

    def _init_db(self) -> None:
        con = self._conn()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_catalog (
                    cache_key VARCHAR PRIMARY KEY,
                    parquet_path VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        finally:
            con.close()

    def register_parquet(self, cache_key: str, parquet_path: Path) -> None:
        con = self._conn()
        try:
            # 1) Ensure table exists
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_catalog (
                    cache_key TEXT PRIMARY KEY,
                    parquet_path TEXT NOT NULL,
                    created_at TIMESTAMP
                );
                """
            )

            # 2) If an old table exists without created_at, add it (migration-safe)
            # DuckDB supports IF NOT EXISTS for ADD COLUMN in recent versions, but be safe with try/except.
            try:
                con.execute("ALTER TABLE cache_catalog ADD COLUMN created_at TIMESTAMP;")
            except Exception:
                # column already exists OR duckdb version differs -> ignore
                pass

            # 3) Correct UPSERT: CURRENT_TIMESTAMP is a VALUE, not a column
            con.execute(
                """
                INSERT INTO cache_catalog (cache_key, parquet_path, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (cache_key) DO UPDATE SET
                    parquet_path = excluded.parquet_path,
                    created_at = CURRENT_TIMESTAMP;
                """,
                [cache_key, str(parquet_path)],
            )

        finally:
            con.close()

    def get_parquet_path(self, cache_key: str) -> Optional[Path]:
        con = self._conn()
        try:
            row = con.execute(
                "SELECT parquet_path FROM cache_catalog WHERE cache_key = ?",
                [cache_key],
            ).fetchone()
            if not row:
                return None
            return Path(row[0])
        finally:
            con.close()

    def list_catalog(self) -> pd.DataFrame:
        con = self._conn()
        try:
            return con.execute("SELECT * FROM cache_catalog ORDER BY created_at DESC").df()
        finally:
            con.close()

    def query_cached(self, cache_key: str, duckdb_sql: str) -> pd.DataFrame:
        """
        Run DuckDB SQL against a cached parquet snapshot (offline mode).

        The parquet is exposed as a view called: cached
        Example duckdb_sql:
          SELECT col1, SUM(col2) FROM cached GROUP BY col1
        """
        parquet_path = self.get_parquet_path(cache_key)
        if parquet_path is None or not parquet_path.exists():
            raise FileNotFoundError(f"No cached parquet found for cache_key={cache_key}")

        con = self._conn()
        try:
            con.execute(f"CREATE OR REPLACE VIEW cached AS SELECT * FROM read_parquet('{parquet_path.as_posix()}')")
            return con.execute(duckdb_sql).df()
        finally:
            con.close()

    def health(self) -> Dict[str, Any]:
        return {
            "duckdb_path": str(self.duckdb_path),
            "exists": self.duckdb_path.exists(),
        }