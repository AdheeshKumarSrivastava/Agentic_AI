from __future__ import annotations

import streamlit as st

from config import Settings
from traces.trace_store import TraceStore


TIMELINE = [
    "A_intent",
    "A_intent__critique",
    "B_schema_reasoning",
    "B_schema_reasoning__critique",
    "C_plan",
    "C_plan__critique",
    "D_human_review",
    "D_human_review__applied",
    "D_human_review__critique",
    "E_sql_generation",
    "E_sql_generation__critique",
    "F_sql_safety",
    "F_sql_safety__critique",
    "G_execute",
    "G_execute__critique",
    "H_data_validation",
    "H_data_validation__critique",
    "I_insights",
    "I_insights__critique",
    "J_dashboard",
    "J_dashboard__html",
    "J_dashboard__critique",
    "K_render",
    "K_render__critique",
    "L_critique_rollup",
]


def render_trace_viewer(settings: Settings, trace_store: TraceStore, developer_mode: bool) -> None:
    st.header("Run Trace Viewer / Node Outputs")

    runs = trace_store.list_runs()
    if not runs:
        st.info("No runs found yet.")
        return

    # Left sidebar list (mandatory requirement)
    with st.sidebar:
        st.subheader("Runs")
        options = [r["run_id"] for r in runs if r.get("run_id")]
        run_id = st.selectbox("Select run", options=options, index=0)
        st.divider()
        st.subheader("Compare runs")
        run_a = st.selectbox("Run A", options=options, index=0, key="cmp_a")
        run_b = st.selectbox("Run B", options=options, index=min(1, len(options)-1), key="cmp_b")
        if st.button("Show Diff"):
            diff = trace_store.diff_runs(run_a, run_b)
            st.session_state["run_diff"] = diff

    doc = trace_store.load(run_id)
    st.markdown(f"**Run:** `{run_id}` • **Status:** `{doc.get('status')}`")

    # Download trace
    trace_json = doc
    st.download_button(
        "Download trace JSON",
        data=str(trace_json).encode("utf-8"),
        file_name=f"run_{run_id}.json",
        mime="application/json",
    )

    st.divider()
    st.subheader("Timeline (A→L)")

    # Vertical timeline via expanders
    nodes = doc.get("nodes", {})
    for step in TIMELINE:
        with st.expander(step, expanded=False):
            if step in nodes:
                st.caption(f"timestamp: {nodes[step].get('timestamp')}")
                st.json(nodes[step].get("payload"))
            else:
                st.info("No output for this step.")

    if doc.get("errors"):
        st.subheader("Errors / Fallback Decisions")
        st.json(doc["errors"])

    if st.session_state.get("run_diff"):
        st.subheader("Compare Runs Diff (plan/sql/insights/dashboard)")
        st.code(st.session_state["run_diff"], language="diff")
