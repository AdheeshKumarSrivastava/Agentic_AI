from __future__ import annotations

import streamlit as st
from pathlib import Path
import pandas as pd

from config import Settings
from traces.trace_store import TraceStore


def render_export(settings: Settings, trace_store: TraceStore) -> None:
    st.header("Export")

    last = st.session_state.get("last_result")
    if not last or last.get("status") != "success":
        st.info("Run a successful pipeline first.")
        return

    # Dashboard HTML
    html = last.get("dashboard_html", "")
    st.download_button(
        "Download Dashboard HTML",
        data=html.encode("utf-8"),
        file_name="dashboard.html",
        mime="text/html",
    )

    # CSV from preview (or rehydrate from trace if needed)
    df_preview = last.get("df_preview", [])
    if df_preview:
        df = pd.DataFrame(df_preview)
        st.download_button(
            "Download Result CSV (preview rows)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="results_preview.csv",
            mime="text/csv",
        )

    # Trace JSON
    run_id = last.get("run_id")
    doc = trace_store.load(run_id)
    st.download_button(
        "Download Trace JSON",
        data=str(doc).encode("utf-8"),
        file_name=f"run_{run_id}.json",
        mime="application/json",
    )
