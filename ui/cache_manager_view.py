from __future__ import annotations

import streamlit as st
from config import Settings
from cache.cache_manager import QueryCache


def render_cache_manager(settings: Settings) -> None:
    st.header("Cache Manager")
    cache = QueryCache(settings)

    entries = cache.list_entries()
    st.dataframe(entries, use_container_width=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        key = st.text_input("Clear by key (sql_hash)", value="")
        if st.button("Clear Key"):
            removed = cache.clear(key=key.strip() or None)
            st.success(f"Removed {removed} entries.")
    with col2:
        if st.button("Clear ALL cache", type="primary"):
            removed = cache.clear()
            st.success(f"Removed {removed} entries.")
