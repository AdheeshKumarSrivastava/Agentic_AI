from __future__ import annotations

import streamlit as st

from config import Settings
from traces.trace_store import TraceStore
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry
from agents.schema_agent import SchemaAgent

from ui.schema_explorer import render_schema_explorer
from ui.ask_analytics import render_ask_analytics
from ui.trace_viewer import render_trace_viewer
from ui.query_logs_view import render_query_logs
from ui.cache_manager_view import render_cache_manager
from ui.export_view import render_export


def _bootstrap_schema_if_missing(settings: Settings) -> None:
    """
    Auto-introspect schema ONCE if no schema cache exists.
    This makes the platform actually "do it" by default.
    """
    kg = KnowledgeGraphStore(settings.KNOWLEDGE_GRAPH_DIR)
    schema = kg.load_schema()
    if schema.get("tables"):
        return

    registry = SchemaRegistry(settings.KNOWLEDGE_GRAPH_DIR)
    agent = SchemaAgent(settings=settings, kg=kg, registry=registry)

    with st.spinner("Bootstrapping: introspecting database schema (first run)..."):
        res = agent.refresh(sample_rows=50)
    st.success(f"Bootstrap complete: {res.get('tables', 0)} tables discovered.")


def render_app(settings: Settings) -> None:
    st.sidebar.title("Agentic Analytics Platform")

    trace_store = TraceStore(settings.TRACES_DIR)

    page = st.sidebar.radio(
        "Views",
        ["Schema Explorer", "Ask Analytics", "Run Traces", "Query Logs", "Cache Manager", "Export"],
        index=1,
    )

    dev_default = bool(settings.DEV_MODE_DEFAULT)
    developer_mode = st.sidebar.toggle("Developer Mode", value=dev_default)
    st.session_state["developer_mode"] = developer_mode

    # ✅ AUTO DO IT: schema bootstrap
    _bootstrap_schema_if_missing(settings)

    if page == "Schema Explorer":
        render_schema_explorer(settings)
    elif page == "Ask Analytics":
        render_ask_analytics(settings, trace_store=trace_store, developer_mode=developer_mode)
    elif page == "Run Traces":
        render_trace_viewer(settings, trace_store=trace_store, developer_mode=developer_mode)
    elif page == "Query Logs":
        render_query_logs(settings)
    elif page == "Cache Manager":
        render_cache_manager(settings)
    elif page == "Export":
        render_export(settings, trace_store=trace_store)
