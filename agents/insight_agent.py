from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np


class InsightAgent:
    """
    Must not hallucinate: only use computed values from df.
    """

    def generate(self, df: pd.DataFrame, plan: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"kpis": [], "highlights": [], "tables": []}
        if df is None or df.empty:
            return {"kpis": [], "highlights": ["No data returned."], "tables": []}

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if df[c].dtype == "object" or pd.api.types.is_categorical_dtype(df[c])]

        # KPI ideas: numeric summaries
        for c in numeric_cols[:8]:
            s = df[c].dropna()
            if s.empty:
                continue
            out["kpis"].append({
                "name": f"{c} (avg)",
                "value": float(np.mean(s)),
            })
            out["kpis"].append({
                "name": f"{c} (sum)",
                "value": float(np.sum(s)),
            })

        # Highlights: top categories by count
        for c in cat_cols[:3]:
            vc = df[c].astype(str).value_counts().head(5)
            if vc.empty:
                continue
            top = [{"value": k, "count": int(v)} for k, v in vc.items()]
            out["tables"].append({"title": f"Top {c}", "rows": top})
            out["highlights"].append(f"Top values for {c}: " + ", ".join([f"{r['value']} ({r['count']})" for r in top[:3]]))

        # Data shape note
        out["highlights"].append(f"Returned {len(df):,} rows and {len(df.columns)} columns.")
        return out
