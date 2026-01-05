from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import json
import time
import uuid
import difflib


class TraceStore:
    """
    Persists traces to /traces as JSON. Survives restarts.
    Provides list/load/diff APIs.
    """

    def __init__(self, traces_dir: str):
        self.base = Path(traces_dir)
        self.base.mkdir(parents=True, exist_ok=True)

    def new_run(self) -> str:
        run_id = uuid.uuid4().hex[:12]
        doc = {
            "run_id": run_id,
            "created_at": int(time.time()),
            "status": "started",
            "nodes": {},
            "errors": [],
        }
        self._save(run_id, doc)
        return run_id

    def add_node(self, run_id: str, node: str, payload: Any) -> None:
        doc = self.load(run_id)
        nodes = doc.setdefault("nodes", {})
        nodes[node] = {
            "timestamp": int(time.time()),
            "payload": payload,
        }
        self._save(run_id, doc)

    def add_error(self, run_id: str, node: str, message: str, stack: str) -> None:
        doc = self.load(run_id)
        doc.setdefault("errors", []).append({
            "timestamp": int(time.time()),
            "node": node,
            "message": message,
            "stack": stack,
        })
        doc["status"] = "failed"
        self._save(run_id, doc)

    def finalize(self, run_id: str, status: str) -> None:
        doc = self.load(run_id)
        doc["status"] = status
        doc["finalized_at"] = int(time.time())
        self._save(run_id, doc)

    def list_runs(self) -> List[Dict[str, Any]]:
        runs = []
        for p in sorted(self.base.glob("run_*.json"), reverse=True):
            try:
                doc = json.loads(p.read_text(encoding="utf-8"))
                runs.append({
                    "run_id": doc.get("run_id"),
                    "created_at": doc.get("created_at"),
                    "status": doc.get("status"),
                    "path": p.as_posix(),
                })
            except Exception:
                continue
        return runs

    def load(self, run_id: str) -> Dict[str, Any]:
        p = self._path(run_id)
        if not p.exists():
            return {"run_id": run_id, "status": "missing", "nodes": {}, "errors": []}
        return json.loads(p.read_text(encoding="utf-8"))

    def get_node(self, run_id: str, node: str) -> Any:
        doc = self.load(run_id)
        return doc.get("nodes", {}).get(node)

    def diff_runs(self, run_a: str, run_b: str, keys: Optional[List[str]] = None) -> str:
        a = self.load(run_a)
        b = self.load(run_b)
        keys = keys or ["C_plan", "E_sql_generation", "I_insights", "J_dashboard"]
        sa = json.dumps({k: a.get("nodes", {}).get(k, {}) for k in keys}, indent=2, sort_keys=True)
        sb = json.dumps({k: b.get("nodes", {}).get(k, {}) for k in keys}, indent=2, sort_keys=True)
        diff = difflib.unified_diff(sa.splitlines(), sb.splitlines(), fromfile=run_a, tofile=run_b, lineterm="")
        return "\n".join(diff)

    def _path(self, run_id: str) -> Path:
        return self.base / f"run_{run_id}.json"

    def _save(self, run_id: str, doc: Dict[str, Any]) -> None:
        self._path(run_id).write_text(json.dumps(doc, indent=2), encoding="utf-8")
