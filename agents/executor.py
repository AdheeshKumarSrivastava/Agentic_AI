from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple
import time
import hashlib

import pandas as pd

from config import Settings
from db import run_sql_query
from cache.snapshot_cache import SnapshotCache
from cache.duckdb_store import DuckDBStore


@dataclass
class Executor:
    settings: Settings

    def __post_init__(self) -> None:
        self.cache = SnapshotCache(Path(self.settings.CACHE_DIR))

        # Safe fallback if DUCKDB_PATH not present (prevents crashes)
        duckdb_path = getattr(self.settings, "DUCKDB_PATH", str(Path(self.settings.CACHE_DIR) / "catalog.duckdb"))
        self.duckdb = DuckDBStore(Path(duckdb_path))

    def _cache_key(self, sql: str, params: Dict[str, Any]) -> str:
        payload = (sql + "|" + repr(sorted((params or {}).items()))).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def run(self, *, sql: str, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Executes SQL safely (SELECT-only assumed validated upstream).
        Uses Parquet snapshot caching.
        Registers cached snapshots into DuckDB catalog for offline dashboards.
        """
        start = time.time()
        cache_key = self._cache_key(sql, params or {})

        # 1) Try cache first
        cached = self.cache.get(cache_key)
        if cached is not None:
            df = cached
            parquet_path = self.cache.path_for_key(cache_key)
            if parquet_path and parquet_path.exists():
                self.duckdb.register_parquet(cache_key, parquet_path)

            meta = {
                "cache_key": cache_key,
                "cache_hit": True,
                "rows": int(len(df)),
                "seconds": round(time.time() - start, 4),
                "mode": "cache",
            }
            return df, meta

        # 2) Offline-only mode blocks DB calls
        if bool(getattr(self.settings, "OFFLINE_ONLY", False)):
            raise RuntimeError(
                "OFFLINE_ONLY is enabled and no cache snapshot exists for this query. "
                "Run once with OFFLINE_ONLY=false to populate cache."
            )

        # 3) Execute against DB (match YOUR Settings fields)
        timeout_seconds = int(getattr(self.settings, "STATEMENT_TIMEOUT_SECONDS", 3600))
        max_rows = int(getattr(self.settings, "MAX_RETURNED_ROWS", 200000))

        df = run_sql_query(
            sql=sql,
            params=params or {},
            timeout_seconds=timeout_seconds,
            max_rows=max_rows,
            settings=self.settings,  # pass settings so db layer uses same connection config
        )

        # 4) Cache to parquet
        self.cache.put(cache_key, df)
        parquet_path = self.cache.path_for_key(cache_key)
        if parquet_path and parquet_path.exists():
            self.duckdb.register_parquet(cache_key, parquet_path)

        meta = {
            "cache_key": cache_key,
            "cache_hit": False,
            "rows": int(len(df)),
            "seconds": round(time.time() - start, 4),
            "mode": "db",
        }
        return df, meta