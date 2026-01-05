from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import json


@dataclass
class ContentIndexStore:
    base_dir: Path

    @property
    def path(self) -> Path:
        return self.base_dir / "content_index.json"

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"tables": {}}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, obj: Dict[str, Any]) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

    def upsert_table(self, table_key: str, payload: Dict[str, Any]) -> None:
        obj = self.load()
        obj.setdefault("tables", {})
        obj["tables"][table_key] = payload
        self.save(obj)