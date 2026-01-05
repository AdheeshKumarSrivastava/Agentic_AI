from __future__ import annotations

import streamlit as st
from config import Settings
from observability.query_log import QueryLogStore


def render_query_logs(settings: Settings) -> None:
    st.header("Query Logs (Audit)")
    store = QueryLogStore(settings.LOG_DIR)
    rows = store.read_recent(200)
    if not rows:
        st.info("No query logs yet.")
        return
    st.dataframe(rows, use_container_width=True)
