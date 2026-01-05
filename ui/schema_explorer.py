from __future__ import annotations

import streamlit as st

from config import Settings
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry
from agents.schema_agent import SchemaAgent


def render_schema_explorer(settings: Settings) -> None:
    st.header("Schema Explorer")

    kg = KnowledgeGraphStore(settings.KNOWLEDGE_GRAPH_DIR)
    registry = SchemaRegistry(settings.KNOWLEDGE_GRAPH_DIR)
    agent = SchemaAgent(settings=settings, kg=kg, registry=registry)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Refresh Schema (introspect DB)", type="primary"):
            with st.spinner("Refreshing schema..."):
                res = agent.refresh(sample_rows=50)
            st.success(res.get("note", "Done."))

    schema = kg.load_schema()
    tables = list(schema.get("tables", {}).keys())

    if not tables:
        st.info("No schema cached yet. Click **Refresh Schema**.")
        return

    # Table allowlist selector (NEW mandatory requirement)
    st.subheader("Table Selection (Allowlist)")
    default_allowed = st.session_state.get("allowed_tables", tables)
    allowed = st.multiselect(
        "Select tables to allow the agents to use",
        options=tables,
        default=default_allowed,
        help="Agents will only plan/use these tables. This prevents unwanted table usage.",
    )
    st.session_state["allowed_tables"] = allowed

    st.divider()
    st.subheader("Tables")

    selected = st.selectbox("Pick a table", options=tables)
    t = schema["tables"][selected]

    st.markdown(f"**{selected}**  \nRows (approx): `{t.get('row_count')}`")

    st.markdown("### Columns")
    st.dataframe(t.get("columns", []), use_container_width=True)

    st.markdown("### PK/FK Hints (best-effort)")
    st.json(t.get("pk_fk_hints", {}))

    st.markdown("### Sample (explicit columns)")
    st.dataframe(t.get("sample", []), use_container_width=True)
