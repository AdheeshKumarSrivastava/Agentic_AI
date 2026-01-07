from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from typing import Optional

from config import Settings
from observability.redaction import redact_connection_string

# Streamlit-friendly singleton engine
_ENGINE_SINGLETON: Optional[Engine] = None


def build_mssql_connection_url(settings: Settings) -> str:
    """
    SQL Server via ODBC.
    Adds ApplicationIntent=ReadOnly when possible.
    """
    odbc = (
        f"Driver={{{settings.ODBC_DRIVER}}};"
        f"Server={settings.DB_HOST},{settings.DB_PORT};"
        f"Database={settings.DB_NAME};"
        f"Uid={settings.DB_USERNAME};"
        f"Pwd={settings.DB_PASSWORD};"
        f"ApplicationIntent=ReadOnly;"
    )
    extra = (settings.ODBC_EXTRA_PARAMS or "").strip()
    if extra:
        if not extra.endswith(";"):
            extra += ";"
        odbc += extra

    return f"{settings.DB_DIALECT}:///?odbc_connect={quote_plus(odbc)}"


def build_engine(settings: Settings) -> Engine:
    """
    Build a pooled SQLAlchemy engine (safe for Streamlit reruns).

    IMPORTANT:
    - pool_size/max_overflow prevent connection explosion
    - pool_timeout avoids hanging when pool is exhausted
    - pool_recycle reduces stale connections
    - pool_pre_ping checks connections before using them
    - pool_use_lifo improves behavior under bursty workloads (Streamlit reruns)
    """
    url = build_mssql_connection_url(settings)

    # ---- Pool knobs (add these to Settings; defaults here are safe) ----
    pool_size = int(getattr(settings, "SQL_POOL_SIZE", 5))
    max_overflow = int(getattr(settings, "SQL_MAX_OVERFLOW", 2))
    pool_timeout = int(getattr(settings, "SQL_POOL_TIMEOUT_SECONDS", 30))
    pool_recycle = int(getattr(settings, "SQL_POOL_RECYCLE_SECONDS", 1800))
    pool_use_lifo = bool(getattr(settings, "SQL_POOL_USE_LIFO", True))

    engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        pool_use_lifo=pool_use_lifo,
    )

    # never log url directly
    _ = redact_connection_string(url)
    return engine


def get_shared_engine(settings: Settings) -> Engine:
    """
    Returns a singleton Engine (best for Streamlit so reruns don't create new pools).
    """
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is None:
        _ENGINE_SINGLETON = build_engine(settings)
    return _ENGINE_SINGLETON


def dispose_shared_engine() -> None:
    """
    Optional: call this on app shutdown / debugging to force close pool connections.
    """
    global _ENGINE_SINGLETON
    if _ENGINE_SINGLETON is not None:
        try:
            _ENGINE_SINGLETON.dispose()
        finally:
            _ENGINE_SINGLETON = None