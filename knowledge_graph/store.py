from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import json
import time


class KnowledgeGraphStore:
    """
    Simple JSON-based knowledge graph store:
    - schema.json: tables, columns, stats, pk/fk hints
    """

    def __init__(self, base_dir: str):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.schema_path = self.base / "schema.json"

    def load_schema(self) -> Dict[str, Any]:
        if not self.schema_path.exists():
            return {"updated_at": None, "tables": {}}
        return json.loads(self.schema_path.read_text(encoding="utf-8"))

    def save_schema(self, schema: Dict[str, Any]) -> None:
        schema = dict(schema)
        schema["updated_at"] = int(time.time())
        self.schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
