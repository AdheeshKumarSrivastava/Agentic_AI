from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

from config import Settings
from db.engine import build_engine
from db.introspect import fetch_tables, fetch_columns, fetch_row_count, sample_table, pk_fk_hints
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry


class SchemaAgent:
    """
    Extracts schema + lightweight stats and persists:
    - knowledge_graph/schema.json
    - knowledge_graph/schema_registry.json
    """

    def __init__(self, settings: Settings, kg: KnowledgeGraphStore, registry: SchemaRegistry):
        self.settings = settings
        self.kg = kg
        self.registry = registry
        self.engine = build_engine(settings)

    def refresh(self, sample_rows: int = 50, top_tables: int | None = None) -> Dict[str, Any]:
        tables = fetch_tables(self.engine)
        if top_tables is not None:
            tables = tables[: int(top_tables)]

        schema: Dict[str, Any] = {"tables": {}}
        registry: Dict[str, Any] = {"tables": {}}

        for t in tables:
            schema_name = t["schema_name"]
            table_name = t["table_name"]
            key = f"{schema_name}.{table_name}"

            cols = fetch_columns(self.engine, schema=schema_name, table=table_name)
            row_count = fetch_row_count(self.engine, schema=schema_name, table=table_name)
            hints = pk_fk_hints(self.engine, schema=schema_name, table=table_name)

            col_names = [c["column_name"] for c in cols]
            df_sample = pd.DataFrame()
            if col_names:
                # Sample requires explicit columns
                df_sample = sample_table(self.engine, schema=schema_name, table=table_name, columns=col_names[: min(30, len(col_names))], top_n=sample_rows)

            schema["tables"][key] = {
                "schema": schema_name,
                "name": table_name,
                "row_count": row_count,
                "columns": cols,
                "pk_fk_hints": hints,
                "sample": df_sample.head(sample_rows).to_dict(orient="records"),
            }

            registry["tables"][key] = {
                "schema": schema_name,
                "name": table_name,
                "row_count": row_count,
                "columns": [{"name": c["column_name"], "type": c["data_type"], "nullable": bool(c["is_nullable"])} for c in cols],
                "pk_fk_hints": hints,
            }

        self.kg.save_schema(schema)
        self.registry.save(registry)
        return {"ok": True, "tables": len(schema["tables"]), "note": "Schema refreshed from DB."}
