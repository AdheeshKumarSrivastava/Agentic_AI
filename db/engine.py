from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

from config import Settings
from observability.redaction import redact_connection_string


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
    url = build_mssql_connection_url(settings)
    # pool_pre_ping for reliability
    engine = create_engine(
        url,
        pool_pre_ping=True,
        future=True,
    )
    # never log url directly
    _ = redact_connection_string(url)
    return engine
