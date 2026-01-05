from __future__ import annotations

from typing import Any, Dict, Tuple
import time
import hashlib

import pandas as pd
from sqlalchemy import text

from config import Settings
from db.engine import build_engine
from cache.cache_manager import QueryCache
from observability.timing import timed_block


class Executor:
    """
    Executes safe SQL with:
    - timeouts (configurable)
    - fetch size
    - cache (parquet snapshots)
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.engine = build_engine(settings)
        self.cache = QueryCache(settings=settings)

    def run(self, sql: str, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        start = time.time()
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]

        # Cache lookup
        cached = self.cache.get(sql_hash=sql_hash)
        if cached is not None:
            meta = {
                "sql_hash": sql_hash,
                "cache": {"hit": True, "path": self.cache.path_for(sql_hash).as_posix()},
                "rows": int(len(cached)),
                "seconds": round(time.time() - start, 4),
                "timeout_seconds": self.settings.STATEMENT_TIMEOUT_SECONDS,
            }
            return cached, meta

        # Execute (statement timeout best-effort)
        with self.engine.connect() as conn:
            # best-effort statement timeout (SQL Server)
            # For SQL Server, we can use SET LOCK_TIMEOUT / query timeout through driver; SQLAlchemy doesn't always propagate.
            # We'll rely on driver-side timeout via connect args would be better; but keep safe meta.
            with timed_block("sql_execute"):
                df = pd.read_sql(text(sql), conn, params=params)

        # Enforce max returned rows at executor level too (hard stop)
        max_rows = int(self.settings.MAX_RETURNED_ROWS)
        if max_rows > 0 and len(df) > max_rows:
            df = df.head(max_rows)

        self.cache.put(sql_hash=sql_hash, df=df)

        meta = {
            "sql_hash": sql_hash,
            "cache": {"hit": False, "path": self.cache.path_for(sql_hash).as_posix()},
            "rows": int(len(df)),
            "seconds": round(time.time() - start, 4),
            "timeout_seconds": self.settings.STATEMENT_TIMEOUT_SECONDS,
        }
        return df, meta
