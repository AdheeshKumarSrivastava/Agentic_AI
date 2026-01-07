from __future__ import annotations

from typing import Any, Dict, Optional
import time
import re

import pandas as pd
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text


_SELECT_ONLY_RE = re.compile(r"^\s*(?:--[^\n]*\n|\s|/\*.*?\*/)*select\b", re.IGNORECASE | re.DOTALL)


def _assert_select_only(sql: str) -> None:
    """
    Defense-in-depth: upstream guard should already enforce this,
    but we re-check here to avoid accidental non-SELECT execution.
    """
    if not sql or not _SELECT_ONLY_RE.search(sql):
        raise ValueError("Only SELECT queries are allowed in run_sql_query().")


def _apply_sqlserver_session_settings(
    conn: Connection,
    *,
    lock_timeout_ms: int,
    read_uncommitted: bool,
) -> None:
    """
    Applies session-level settings for SQL Server.
    These statements are safe and help avoid locking + chatty rowcount messages.
    """
    # NOCOUNT reduces "x rows affected" chatter
    conn.exec_driver_sql("SET NOCOUNT ON;")

    # Optional: avoid blocking forever on locks
    if lock_timeout_ms and lock_timeout_ms > 0:
        conn.exec_driver_sql(f"SET LOCK_TIMEOUT {int(lock_timeout_ms)};")

    # Optional: analytics-friendly isolation (avoid reader blocking writers)
    if read_uncommitted:
        conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")


def run_sql_query(
    *,
    engine: Engine,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 3600,
    max_rows: int = 200000,
    chunksize: int = 50000,
    # SQL Server-specific knobs (safe defaults)
    sqlserver_lock_timeout_ms: int = 30_000,
    sqlserver_read_uncommitted: bool = True,
) -> pd.DataFrame:
    """
    Execute a SELECT query safely via SQLAlchemy Engine.

    Guarantees:
    - SELECT-only (defense-in-depth)
    - Streamed reads with chunking
    - Hard cap on max_rows
    - Best-effort timeout (driver + wall-clock cutoff)

    Notes on timeout:
    - For SQL Server/pyodbc, query timeout is often controlled by the driver.
      SQLAlchemy `execution_options(timeout=...)` is respected by many drivers,
      but not all. We still keep a wall-clock cutoff to stop ingestion.
    """
    _assert_select_only(sql)

    params = params or {}

    timeout_seconds = int(timeout_seconds) if timeout_seconds and timeout_seconds > 0 else 3600
    max_rows = int(max_rows) if max_rows and max_rows > 0 else 200000
    chunksize = int(chunksize) if chunksize and chunksize > 0 else 50000

    t0 = time.time()
    collected: list[pd.DataFrame] = []
    rows_so_far = 0

    # Use a single connection, streamed, and returned to pool reliably.
    # execution_options(stream_results=True) helps avoid buffering huge result sets.
    with engine.connect() as conn:
        # SQL Server session settings (safe)
        try:
            _apply_sqlserver_session_settings(
                conn,
                lock_timeout_ms=sqlserver_lock_timeout_ms,
                read_uncommitted=sqlserver_read_uncommitted,
            )
        except Exception:
            # Don’t fail the query if these statements are not supported / permissions differ.
            pass

        # Best-effort driver-side timeout (often respected; harmless if ignored)
        conn = conn.execution_options(stream_results=True, timeout=timeout_seconds)

        # pandas: iterator when chunksize is set
        result_iter = pd.read_sql_query(
            sql=text(sql),
            con=conn,
            params=params,
            chunksize=chunksize,
        )

        # If pandas ever returns a DF (shouldn't with chunksize), handle anyway.
        if isinstance(result_iter, pd.DataFrame):
            return result_iter.head(max_rows)

        for chunk in result_iter:
            if chunk is None or chunk.empty:
                continue

            collected.append(chunk)
            rows_so_far += int(len(chunk))

            if rows_so_far >= max_rows:
                break

            # Soft wall-clock stop (does not cancel server query in all drivers)
            if (time.time() - t0) > float(timeout_seconds):
                break

    if not collected:
        return pd.DataFrame()

    df = pd.concat(collected, ignore_index=True)

    # Final hard cap
    if len(df) > max_rows:
        df = df.head(max_rows)

    return df