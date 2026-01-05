from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path
import json
import time


class QueryLogStore:
    def __init__(self, log_dir: str):
        self.path = Path(log_dir) / "query_logs.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, meta: Dict[str, Any]) -> None:
        row = dict(meta)
        row["ts"] = int(time.time())
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def read_recent(self, n: int = 200) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        out = []
        for ln in lines[-n:]:
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
        return out
