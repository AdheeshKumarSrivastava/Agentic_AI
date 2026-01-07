from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import re
import pandas as pd

from config import Settings
from db.engine import build_engine
from db.introspect import (
    fetch_tables,
    fetch_columns,
    fetch_row_count,
    sample_table,
    pk_fk_hints,
)
from db.query import run_sql_query  # <-- ensure you have this (or import your existing db runner)
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry
from knowledge_graph.content_index import ContentIndexStore


class SchemaAgent:
    """
    Extracts schema + stats and persists:
    - knowledge_graph_data/schema.json
    - knowledge_graph_data/schema_registry.json
    - knowledge_graph_data/content_index.json  (NEW: content-driven signals)

    Content index is designed for table suggestion based on real data, WITHOUT loading entire tables.
    """

    def __init__(self, settings: Settings, kg: KnowledgeGraphStore, registry: SchemaRegistry):
        self.settings = settings
        self.kg = kg
        self.registry = registry
        self.engine = build_engine(settings)
        self.content_store = ContentIndexStore(Path(self.settings.KNOWLEDGE_GRAPH_DIR))

    def refresh(
        self,
        sample_rows: int = 20000,
        top_tables: int | None = None,
        build_content_index: bool = True,
        content_top_values: int = 2000,
        content_profile_cols_max: int = 250,
        skip_top_values_for_id_cols: bool = True,
    ) -> Dict[str, Any]:
        """
        Refresh schema + registry + optional content index.

        build_content_index:
          - True: reads real table content via aggregated DB queries
          - False: only schema + sample rows
        """
        tables = fetch_tables(self.engine)
        if top_tables is not None:
            tables = tables[: int(top_tables)]

        schema: Dict[str, Any] = {"tables": {}}
        registry: Dict[str, Any] = {"tables": {}}

        indexed_tables = 0

        for t in tables:
            schema_name = t["schema_name"]
            table_name = t["table_name"]
            key = f"{schema_name}.{table_name}"

            cols = fetch_columns(self.engine, schema=schema_name, table=table_name)
            row_count = fetch_row_count(self.engine, schema=schema_name, table=table_name)
            hints = pk_fk_hints(self.engine, schema=schema_name, table=table_name)

            col_names = [c["column_name"] for c in cols]

            # --- sample rows (explicit columns) ---
            df_sample = pd.DataFrame()
            if col_names:
                df_sample = sample_table(
                    self.engine,
                    schema=schema_name,
                    table=table_name,
                    columns=col_names[: min(30, len(col_names))],
                    top_n=sample_rows,
                )

            schema["tables"][key] = {
                "schema": schema_name,
                "name": table_name,
                "row_count": int(row_count),
                "columns": cols,
                "pk_fk_hints": hints,
                "sample": df_sample.head(sample_rows).to_dict(orient="records"),
            }

            registry["tables"][key] = {
                "schema": schema_name,
                "name": table_name,
                "row_count": int(row_count),
                "columns": [
                    {"name": c["column_name"], "type": c["data_type"], "nullable": bool(c["is_nullable"])}
                    for c in cols
                ],
                "pk_fk_hints": hints,
            }

            # --- NEW: Content Index ---
            if build_content_index:
                content_payload = self._build_table_content_profile(
                    schema=schema_name,
                    table=table_name,
                    table_key=key,
                    columns=col_names[: min(content_profile_cols_max, len(col_names))],
                    sample_df=df_sample,
                    row_count=int(row_count),
                    top_values=content_top_values,
                    skip_top_values_for_id_cols=skip_top_values_for_id_cols,
                )
                self.content_store.upsert_table(key, content_payload)
                indexed_tables += 1

        self.kg.save_schema(schema)
        self.registry.save(registry)

        return {
            "ok": True,
            "tables": len(schema["tables"]),
            "content_indexed_tables": indexed_tables,
            "note": "Schema refreshed from DB. Content index updated." if build_content_index else "Schema refreshed from DB.",
        }

    # ---------------------------------------------------------------------
    # Content profiling (reads REAL table content via aggregates + samples)
    # ---------------------------------------------------------------------

    def _build_table_content_profile(
        self,
        *,
        schema: str,
        table: str,
        table_key: str,
        columns: List[str],
        sample_df: pd.DataFrame,
        row_count: int,
        top_values: int,
        skip_top_values_for_id_cols: bool,
    ) -> Dict[str, Any]:
        """
        Builds content signals for table suggestion:
        - sample rows already obtained
        - top values counts per column (categorical)
        - min/max for numeric/datetime columns (lightweight)
        - keywords blob for planner matching
        """
        tbl = self._fmt_table(schema, table)

        # Pick candidate columns for top-values profiling
        topvals: Dict[str, Any] = {}
        minmax: Dict[str, Any] = {}

        # Heuristic: columns ending with id are usually huge cardinality
        def is_id_like(c: str) -> bool:
            c2 = c.lower()
            return c2 == "id" or c2.endswith("_id") or c2.endswith("id")

        # Use schema registry types if available
        reg = self.registry.load()
        reg_cols = {c["name"]: c.get("type", "") for c in reg.get("tables", {}).get(table_key, {}).get("columns", [])}

        for c in columns:
            if skip_top_values_for_id_cols and is_id_like(c):
                continue

            ctype = str(reg_cols.get(c, "")).lower()

            # min/max for numeric or datetime-ish types
            if any(x in ctype for x in ["int", "decimal", "numeric", "float", "real", "money", "date", "time"]):
                sql_mm = f"""
                SELECT
                  MIN([{c}]) AS [min_val],
                  MAX([{c}]) AS [max_val]
                FROM {tbl}
                WHERE [{c}] IS NOT NULL
                """
                df_mm = self._safe_sql(sql_mm)
                if not df_mm.empty:
                    minmax[c] = {
                        "min": self._json_safe(df_mm["min_val"].iloc[0]),
                        "max": self._json_safe(df_mm["max_val"].iloc[0]),
                    }

            # top values for categorical-ish columns
            sql_top = f"""
            SELECT TOP ({int(top_values)})
              CAST([{c}] AS NVARCHAR(4000)) AS [value],
              COUNT(1) AS [cnt]
            FROM {tbl}
            WHERE [{c}] IS NOT NULL
            GROUP BY CAST([{c}] AS NVARCHAR(4000))
            ORDER BY COUNT(1) DESC
            """
            df_top = self._safe_sql(sql_top)
            if not df_top.empty:
                topvals[c] = df_top.head(top_values).to_dict(orient="records")

        sample_rows = sample_df.head(50).to_dict(orient="records") if sample_df is not None else []

        # Create planner-friendly keyword blob
        text_blob = self._build_table_text_blob(
            table_key=table_key,
            columns=columns,
            top_values=topvals,
            sample_rows=sample_rows,
        )

        return {
            "table_key": table_key,
            "row_count": int(row_count),
            "profiled_columns": columns,
            "sample_rows": sample_rows,
            "top_values": topvals,
            "minmax": minmax,
            "table_text": text_blob,  # used for retrieval/scoring
        }

    def _build_table_text_blob(
        self,
        *,
        table_key: str,
        columns: List[str],
        top_values: Dict[str, Any],
        sample_rows: List[Dict[str, Any]],
    ) -> str:
        """
        Generates a compact text blob used by PlannerAgent for table suggestion scoring.
        """
        parts: List[str] = [table_key]
        parts.extend(columns[:50])

        # include top values (only values, not counts)
        for col, rows in (top_values or {}).items():
            parts.append(col)
            for r in rows[:10]:
                v = str(r.get("value", "")).strip()
                if v:
                    parts.append(v[:80])

        # include some sample row tokens
        for r in (sample_rows or [])[:10]:
            for k, v in r.items():
                if v is None:
                    continue
                vs = str(v)
                if vs and len(vs) <= 80:
                    parts.append(vs)

        blob = " ".join(parts)
        blob = blob.lower()
        blob = re.sub(r"[^a-z0-9_ ]+", " ", blob)
        blob = re.sub(r"\s+", " ", blob).strip()
        return blob[:20000]  # keep bounded

    def _fmt_table(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"

    def _safe_sql(self, sql: str) -> pd.DataFrame:
        """
        Executes read-only aggregate queries for profiling.
        """
        # NOTE: This assumes your db.query.run_sql_query is SELECT-only guarded already.
        return run_sql_query(
            engine=self.engine,
            sql=sql,
            params={},
            timeout_seconds=int(self.settings.STATEMENT_TIMEOUT_SECONDS),
            max_rows=50000,  # profiling output is small
        )

    def _json_safe(self, v: Any) -> Any:
        # ensure JSON serialization (timestamps, decimals etc.)
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (pd.Timestamp,)):
            return v.isoformat()
        return v