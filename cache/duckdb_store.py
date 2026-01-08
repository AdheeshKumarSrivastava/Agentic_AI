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

    Metadata table:
      - cache_key (string)
      - parquet_path (string)
      - created_at (timestamp)
      - updated_at (timestamp)
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
            # Create table with safe defaults
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_catalog (
                    cache_key    VARCHAR PRIMARY KEY,
                    parquet_path VARCHAR NOT NULL,
                    created_at   TIMESTAMP DEFAULT now(),
                    updated_at   TIMESTAMP DEFAULT now()
                );
                """
            )

            # Migration-safe: add missing columns if DB existed earlier
            try:
                con.execute("ALTER TABLE cache_catalog ADD COLUMN created_at TIMESTAMP DEFAULT now();")
            except Exception:
                pass
            try:
                con.execute("ALTER TABLE cache_catalog ADD COLUMN updated_at TIMESTAMP DEFAULT now();")
            except Exception:
                pass
        finally:
            con.close()

    def register_parquet(self, cache_key: str, parquet_path: Path) -> None:
        con = self._conn()
        try:
            # Ensure schema exists (idempotent)
            self._init_db()

            # ✅ Use now() (avoid CURRENT_TIMESTAMP parsing/binding issues)
            # ✅ Keep created_at unchanged on updates
            con.execute(
                """
                INSERT INTO cache_catalog (cache_key, parquet_path, created_at, updated_at)
                VALUES (?, ?, now(), now())
                ON CONFLICT (cache_key) DO UPDATE
                SET parquet_path = excluded.parquet_path,
                    updated_at   = now();
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
            # Prefer updated_at if present, else fallback to created_at
            try:
                return con.execute("SELECT * FROM cache_catalog ORDER BY updated_at DESC").df()
            except Exception:
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
            con.execute(
                f"CREATE OR REPLACE VIEW cached AS "
                f"SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
            )
            return con.execute(duckdb_sql).df()
        finally:
            con.close()

    def health(self) -> Dict[str, Any]:
        return {
            "duckdb_path": str(self.duckdb_path),
            "exists": self.duckdb_path.exists(),
        }