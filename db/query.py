from __future__ import annotations

from typing import Any, Dict, Optional
import time

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text


def run_sql_query(
    *,
    engine: Engine,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 360000,
    max_rows: int = 200000,
    chunksize: int = 50000,
) -> pd.DataFrame:
    """
    Execute a SELECT query safely via SQLAlchemy engine.
    - Applies per-connection statement timeout when possible (SQL Server via SET LOCK_TIMEOUT is different;
      actual query timeout is usually controlled by driver; we still keep a soft wall-clock guard here).
    - Streams results in chunks up to max_rows.
    """
    params = params or {}
    t0 = time.time()

    # Soft guard: prevent unbounded reads
    collected = []
    rows_so_far = 0

    with engine.connect() as conn:
        # NOTE: For SQL Server, query timeout is usually set in pyodbc / driver.
        # We keep a soft wall-clock check and chunking + max_rows.

        result_iter = pd.read_sql_query(
            sql=text(sql),
            con=conn,
            params=params,
            chunksize=int(chunksize) if chunksize else None,
        )

        # pandas returns DataFrame if chunksize=None; iterator if chunksize provided
        if isinstance(result_iter, pd.DataFrame):
            df = result_iter.head(int(max_rows))
            return df

        for chunk in result_iter:
            if chunk is None or chunk.empty:
                continue

            collected.append(chunk)
            rows_so_far += len(chunk)

            # max rows guard
            if rows_so_far >= int(max_rows):
                break

            # soft timeout guard
            if (time.time() - t0) > float(timeout_seconds):
                break

    if not collected:
        return pd.DataFrame()

    df = pd.concat(collected, ignore_index=True)
    if len(df) > int(max_rows):
        df = df.head(int(max_rows))
    return df