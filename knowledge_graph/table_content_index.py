from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import time
import hashlib

import pandas as pd


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    return s[:200]


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class TableContentIndex:
    """
    Stores per-table content fingerprints and text summaries on disk.
    Used ONLY for table suggestion and NOT for generating hallucinated schema.
    """

    root: Path

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, table_key: str) -> Path:
        safe = table_key.replace(".", "__").replace("/", "_")
        return self.root / f"{safe}.json"

    def upsert(self, table_key: str, payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["table_key"] = table_key
        payload["updated_at_epoch"] = int(time.time())
        self.path_for(table_key).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def load(self, table_key: str) -> Optional[Dict[str, Any]]:
        p = self.path_for(table_key)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    def list_all(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for p in self.root.glob("*.json"):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                tk = obj.get("table_key") or p.stem
                out[str(tk)] = obj
            except Exception:
                continue
        return out

    @staticmethod
    def summarize_dataframe(df: pd.DataFrame, max_rows: int = 5000) -> Dict[str, Any]:
        """
        Creates a compact textual representation for semantic/keyword matching.
        Uses only first max_rows to avoid huge payloads.
        """
        if df is None or df.empty:
            return {"ok": True, "rows": 0, "text": "", "hash": _hash_text("")}

        df2 = df.head(max_rows)

        # Build token-like corpus: column names + example values
        chunks: List[str] = []
        chunks.extend([f"col:{c}" for c in df2.columns])

        # take limited values per col
        for c in df2.columns[: min(25, len(df2.columns))]:
            series = df2[c].dropna().astype(str)
            # take top few unique values
            vals = list(dict.fromkeys(series.head(200).tolist()))[:50]
            for v in vals:
                chunks.append(f"val:{c}={_safe_text(v)}")

        text = " | ".join(chunks)
        return {
            "ok": True,
            "rows": int(len(df)),
            "text": text[:200_000],  # cap stored text size
            "hash": _hash_text(text[:200_000]),
        }