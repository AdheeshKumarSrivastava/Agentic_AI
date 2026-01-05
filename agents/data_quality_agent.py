from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd


class DataQualityAgent:
    def run(self, df: pd.DataFrame, expected_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        if df is None:
            return {"ok": False, "reason": "DataFrame is None."}
        if df.empty:
            return {"ok": False, "reason": "Empty result set.", "rows": 0}

        cols = list(df.columns)
        if expected_columns:
            missing = [c for c in expected_columns if c not in cols]
            if missing:
                return {"ok": False, "reason": "Missing expected columns.", "missing": missing, "columns": cols}

        null_rates = {}
        for c in cols:
            try:
                null_rates[c] = float(df[c].isna().mean())
            except Exception:
                null_rates[c] = None

        dup_rows = int(df.duplicated().sum()) if len(df.columns) > 0 else 0

        return {
            "ok": True,
            "rows": int(len(df)),
            "columns": cols,
            "duplicate_rows": dup_rows,
            "null_rate": null_rates,
        }
