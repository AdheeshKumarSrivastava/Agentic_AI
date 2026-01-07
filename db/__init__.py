from __future__ import annotations

from typing import Any, Dict, Optional
import re
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# -----------------------------
# Global engine cache (single pool)
# -----------------------------
_ENGINE: Optional[Engine] = None


# -----------------------------
# SQL safety (defense-in-depth)
# -----------------------------
_SELECT_ONLY_RE = re.compile(
    r"^\s*(?:--[^\n]*\n|\s|/\*.*?\*/)*select\b",
    re.IGNORECASE | re.DOTALL,
)

_BANNED = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|truncate|create|grant|revoke|execute|exec)\b",
    re.IGNORECASE,
)


def _enforce_select_only(sql: str) -> None:
    s = (sql or "").strip()
    if not s:
        raise ValueError("Empty SQL")

    if _BANNED.search(s):
        raise ValueError("Unsafe SQL blocked at DB layer: non-SELECT keyword detected")

    # disallow stacked statements except optional trailing semicolon
    if ";" in s:
        parts = [p.strip() for p in s.split(";") if p.strip()]
        if len(parts) > 1:
            raise ValueError("Unsafe SQL blocked at DB layer: multiple statements detected")

    if not _SELECT_ONLY_RE.search(s):
        raise ValueError("Unsafe SQL blocked at DB layer: only SELECT allowed")


# -----------------------------
# Engine (STANDARDIZED: DB_* + ODBC_*)
# -----------------------------
def build_mssql_engine(settings) -> Engine:
    """
    SQL Server engine using SQLAlchemy + pyodbc.
    Uses DB_* + ODBC_* fields from Settings (matches your config.py screenshot).
    """
    db_dialect = getattr(settings, "DB_DIALECT", "mssql+pyodbc")
    driver = getattr(settings, "ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    host = settings.DB_HOST
    port = int(getattr(settings, "DB_PORT", 1433))
    db = settings.DB_NAME
    user = settings.DB_USERNAME
    pwd = settings.DB_PASSWORD

    # optional extras from config.py (example: "TrustServerCertificate=yes;Encrypt=no")
    extra = (getattr(settings, "ODBC_EXTRA_PARAMS", "") or "").strip()
    if extra and not extra.endswith(";"):
        extra += ";"

    # Always read-only intent
    odbc = (
        f"Driver={{{driver}}};"
        f"Server={host},{port};"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        f"ApplicationIntent=ReadOnly;"
        f"{extra}"
    )

    conn_url = f"{db_dialect}:///?odbc_connect={quote_plus(odbc)}"

    return create_engine(
        conn_url,
        pool_pre_ping=True,
        pool_recycle=int(getattr(settings, "DB_POOL_RECYCLE", 1800)),
        future=True,
    )


def get_engine(settings) -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = build_mssql_engine(settings)
    return _ENGINE


# -----------------------------
# Query execution (STREAMING + CHUNKS)
# -----------------------------
def run_sql_query(
    *,
    sql: str,
    params: Dict[str, Any],
    timeout_seconds: int,
    max_rows: int,
    settings=None,
) -> pd.DataFrame:
    """
    Executes SELECT-only SQL safely with:
      - SELECT-only enforcement
      - SQL Server session guards (NOCOUNT, LOCK_TIMEOUT, optional READ UNCOMMITTED)
      - streaming results (chunked) to avoid memory blowups
      - max_rows cutoff
      - best-effort timeout
    """
    if settings is None:
        from config import Settings  # lazy import
        settings = Settings()

    _enforce_select_only(sql)

    engine = get_engine(settings)

    timeout_seconds = int(timeout_seconds) if timeout_seconds and timeout_seconds > 0 else int(
        getattr(settings, "STATEMENT_TIMEOUT_SECONDS", 3600)
    )
    max_rows = int(max_rows) if max_rows and max_rows > 0 else int(
        getattr(settings, "MAX_RETURNED_ROWS", 200000)
    )

    chunksize = int(getattr(settings, "FETCH_CHUNK_SIZE", getattr(settings, "SQL_CHUNKSIZE", 50000)))
    chunksize = max(1000, min(chunksize, 200000))

    lock_timeout_ms = int(getattr(settings, "MSSQL_LOCK_TIMEOUT_MS", 30000))
    read_uncommitted = bool(getattr(settings, "MSSQL_READ_UNCOMMITTED", True))

    t0 = time.time()
    frames: list[pd.DataFrame] = []
    rows_so_far = 0

    with engine.connect() as conn:
        # Session-level safety (won’t break if not supported)
        try:
            conn.exec_driver_sql("SET NOCOUNT ON;")
            if lock_timeout_ms and lock_timeout_ms > 0:
                conn.exec_driver_sql(f"SET LOCK_TIMEOUT {lock_timeout_ms};")
            if read_uncommitted:
                conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
        except Exception:
            pass

        conn = conn.execution_options(stream_results=True, timeout=timeout_seconds)

        it = pd.read_sql_query(
            sql=text(sql),
            con=conn,
            params=params or {},
            chunksize=chunksize,
        )

        if isinstance(it, pd.DataFrame):
            return it.head(max_rows)

        for chunk in it:
            if chunk is None or chunk.empty:
                continue

            frames.append(chunk)
            rows_so_far += int(len(chunk))

            if rows_so_far >= max_rows:
                break

            if (time.time() - t0) > float(timeout_seconds):
                break

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if len(df) > max_rows:
        df = df.head(max_rows)

    return df