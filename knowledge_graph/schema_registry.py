from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import json


class SchemaRegistry:
    """
    Local registry derived from DB introspection.
    Used to validate that planner/sql-agent never invents names.
    """

    def __init__(self, kg_dir: str):
        self.kg_dir = Path(kg_dir)
        self.path = self.kg_dir / "schema_registry.json"

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"tables": {}}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, registry: Dict[str, Any]) -> None:
        self.path.write_text(json.dumps(registry, indent=2), encoding="utf-8")

    def list_tables(self) -> List[str]:
        reg = self.load()
        return sorted(reg.get("tables", {}).keys())

    def table_columns(self, table_key: str) -> List[str]:
        reg = self.load()
        t = reg.get("tables", {}).get(table_key, {})
        return [c["name"] for c in t.get("columns", [])]

    def has_table(self, table_key: str) -> bool:
        return table_key in self.load().get("tables", {})

    def has_column(self, table_key: str, col: str) -> bool:
        cols = set(self.table_columns(table_key))
        return col in cols
