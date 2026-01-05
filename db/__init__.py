from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import os
import re
import time

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Settings


_ENGINE: Optional[Engine] = None


def build_mssql_engine(settings: Settings) -> Engine:
    """
    Build SQL Server engine using SQLAlchemy + pyodbc.
    Keeps it read-only at app-level; DB user should also be read-only.
    """
    driver = settings.MSSQL_DRIVER
    server = settings.MSSQL_SERVER
    database = settings.MSSQL_DATABASE
    username = settings.MSSQL_USERNAME
    password = settings.MSSQL_PASSWORD

    encrypt = "yes" if str(settings.MSSQL_ENCRYPT).lower() in ("1", "true", "yes") else "no"
    trust_cert = "yes" if str(settings.MSSQL_TRUST_CERT).lower() in ("1", "true", "yes") else "no"

    # NOTE: do not print this; treat as secret
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver}"
        f"&Encrypt={encrypt}"
        f"&TrustServerCertificate={trust_cert}"
    )
    return create_engine(conn_str, pool_pre_ping=True, fast_executemany=True)


def get_engine(settings: Settings) -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = build_mssql_engine(settings)
    return _ENGINE


def _enforce_select_only(sql: str) -> None:
    """
    Extra guard at DB layer: should already be validated by SQLSafetyGuard,
    but we enforce again to avoid accidental misuse.
    """
    s = (sql or "").strip()
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    s = re.sub(r"--.*?$", "", s, flags=re.MULTILINE)
    low = s.lower()

    banned = [
        "insert", "update", "delete", "merge", "drop", "alter", "truncate", "create",
        "grant", "revoke", "execute", "exec"
    ]
    for b in banned:
        if re.search(rf"\b{b}\b", low):
            raise ValueError(f"Unsafe SQL blocked at DB layer: contains '{b}'")

    # multi-statement / stacked queries
    if ";" in low:
        # allow only trailing semicolon
        parts = [p.strip() for p in low.split(";") if p.strip()]
        if len(parts) > 1:
            raise ValueError("Unsafe SQL blocked at DB layer: multiple statements detected")

    if not re.match(r"^\s*select\b", low):
        raise ValueError("Unsafe SQL blocked at DB layer: only SELECT allowed")


def run_sql_query(
    *,
    sql: str,
    params: Dict[str, Any],
    timeout_seconds: int,
    max_rows: int,
    settings: Optional[Settings] = None,
) -> pd.DataFrame:
    """
    Executes a SELECT-only SQL query safely with:
      - per-statement timeout
      - max rows cutoff
    """
    if settings is None:
        settings = Settings()

    _enforce_select_only(sql)

    engine = get_engine(settings)

    start = time.time()
    with engine.connect() as conn:
        # Set timeout for SQL Server (seconds). SQLAlchemy uses driver-specific.
        # For pyodbc, you can set it on the connection if needed.
        try:
            raw = conn.connection
            if hasattr(raw, "timeout"):
                raw.timeout = int(timeout_seconds)
        except Exception:
            pass

        result = conn.execute(text(sql), params or {})
        rows = result.fetchmany(max_rows + 1)

        if len(rows) > max_rows:
            rows = rows[:max_rows]

        df = pd.DataFrame(rows, columns=result.keys())

    return df