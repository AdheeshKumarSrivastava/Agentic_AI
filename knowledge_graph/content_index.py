from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import json
from utils.json_sanitize import json_sanitize


@dataclass
class ContentIndexStore:
    base_dir: Path

    @property
    def path(self) -> Path:
        return self.base_dir / "content_index.json"

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"tables": {}}

        raw = self.path.read_text(encoding="utf-8").strip()
        if not raw:
            return {"tables": {}}

        try:
            obj = json.loads(raw)
        except Exception:
            return {"tables": {}}

        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except Exception:
                return {"tables": {}}

        if not isinstance(obj, dict):
            return {"tables": {}}

        obj.setdefault("tables", {})
        if not isinstance(obj.get("tables"), dict):
            obj["tables"] = {}

        return obj

    def save(self, obj: Dict[str, Any]) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        safe_obj = json_sanitize(obj)
        self.path.write_text(json.dumps(safe_obj, indent=2), encoding="utf-8")

    def upsert_table(self, table_key: str, payload: Dict[str, Any]) -> None:
        obj = self.load()
        if not isinstance(obj, dict):
            obj = {"tables": {}}
        if "tables" not in obj or not isinstance(obj["tables"], dict):
            obj["tables"] = {}
        obj["tables"][table_key] = payload
        self.save(obj)