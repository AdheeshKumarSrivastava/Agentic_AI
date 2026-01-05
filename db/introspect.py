from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def fetch_tables(engine: Engine) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        s.name AS schema_name,
        t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.is_ms_shipped = 0
    ORDER BY s.name, t.name
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]


def fetch_columns(engine: Engine, schema: str, table: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable
    FROM sys.columns c
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = :schema AND t.name = :table
    ORDER BY c.column_id
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"schema": schema, "table": table}).mappings().all()
    return [dict(r) for r in rows]


def fetch_row_count(engine: Engine, schema: str, table: str) -> int:
    # Approx row count from sys.dm_db_partition_stats (faster than COUNT(*))
    sql = """
    SELECT SUM(ps.row_count) AS row_count
    FROM sys.dm_db_partition_stats ps
    JOIN sys.tables t ON ps.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = :schema AND t.name = :table
      AND ps.index_id IN (0,1)
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"schema": schema, "table": table}).mappings().first()
    return int(row["row_count"] or 0)


def sample_table(engine: Engine, schema: str, table: str, columns: List[str], top_n: int = 50) -> pd.DataFrame:
    # Explicit column list required
    col_list = ", ".join([f"[{c}]" for c in columns])
    sql = f"SELECT TOP ({int(top_n)}) {col_list} FROM [{schema}].[{table}]"
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df


def pk_fk_hints(engine: Engine, schema: str, table: str) -> Dict[str, Any]:
    # Best-effort PK and FK hints for planning (not mandatory for correctness).
    pk_sql = """
    SELECT c.name AS column_name
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN sys.tables t ON i.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE i.is_primary_key = 1 AND s.name = :schema AND t.name = :table
    ORDER BY ic.key_ordinal
    """
    fk_sql = """
    SELECT
      cpa.name AS parent_column,
      s2.name AS ref_schema,
      t2.name AS ref_table,
      cr.name AS ref_column
    FROM sys.foreign_key_columns fkc
    JOIN sys.tables t1 ON fkc.parent_object_id = t1.object_id
    JOIN sys.schemas s1 ON t1.schema_id = s1.schema_id
    JOIN sys.columns cpa ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
    JOIN sys.tables t2 ON fkc.referenced_object_id = t2.object_id
    JOIN sys.schemas s2 ON t2.schema_id = s2.schema_id
    JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
    WHERE s1.name = :schema AND t1.name = :table
    """
    with engine.connect() as conn:
        pk = conn.execute(text(pk_sql), {"schema": schema, "table": table}).mappings().all()
        fk = conn.execute(text(fk_sql), {"schema": schema, "table": table}).mappings().all()
    return {"primary_key": [r["column_name"] for r in pk], "foreign_keys": [dict(r) for r in fk]}
