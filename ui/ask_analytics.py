from __future__ import annotations

import json
import streamlit as st
import streamlit.components.v1 as components

from config import Settings
from traces.trace_store import TraceStore
from core.run_pipeline import run_agentic_pipeline
from knowledge_graph.schema_registry import SchemaRegistry
from knowledge_graph.store import KnowledgeGraphStore
from agents.planner_agent import PlannerAgent


def render_ask_analytics(settings: Settings, trace_store: TraceStore, developer_mode: bool) -> None:
    st.header("Ask Analytics")

    kg = KnowledgeGraphStore(settings.KNOWLEDGE_GRAPH_DIR)
    registry = SchemaRegistry(settings.KNOWLEDGE_GRAPH_DIR)

    schema = kg.load_schema()
    all_tables = sorted(schema.get("tables", {}).keys())
    if not all_tables:
        st.warning("Schema not available yet. (Auto bootstrap should run.)")
        return

    # default allowlist = all tables
    if "allowed_tables" not in st.session_state:
        st.session_state["allowed_tables"] = all_tables

    allowed_tables = st.session_state["allowed_tables"]

    st.subheader("1) Ask your question")
    question = st.text_area(
        "Business question",
        value=st.session_state.get("question_tmp", ""),
        height=90,
        placeholder="e.g., Create a dashboard for weekly revenue trend by region and top customers.",
    )
    st.session_state["question_tmp"] = question

    st.divider()
    st.subheader("2) Table Selection (You control the final allowlist)")

    planner = PlannerAgent(settings=settings, kg=kg, registry=registry)

    col1, col2 = st.columns([1, 1])
    with col1:
        st.caption("Current allowlist (agents will only use these)")
        allowed_tables = st.multiselect(
            "Allowed tables",
            options=all_tables,
            default=allowed_tables,
            key="allowed_tables_picker",
        )

    with col2:
        st.caption("Agent suggestion (click to generate)")
        if st.button("Suggest tables from my question", type="secondary"):
            if not question.strip():
                st.error("Type a question first.")
            else:
                intent = planner.extract_intent(question, allowed_tables=all_tables)
                reasoning = planner.schema_reasoning(intent=intent, allowed_tables=all_tables)
                st.session_state["suggested_tables"] = reasoning.get("candidate_tables", [])
                st.session_state["suggested_intent"] = intent

        suggested = st.session_state.get("suggested_tables", [])
        if suggested:
            st.success(f"Suggested {len(suggested)} tables.")
            st.write(suggested)
            if st.button("Apply suggested tables as allowlist", type="primary"):
                allowed_tables = suggested
                st.session_state["allowed_tables_picker"] = suggested

    # persist final allowlist
    st.session_state["allowed_tables"] = allowed_tables

    large_mode = st.toggle("Large Query Mode (use max rows)",value=True)
    st.session_state['large_mode'] = large_mode

    st.divider()
    st.subheader("3) Run pipeline (Plan → HITL → Execute → Insights → Dashboard)")
    run_btn = st.button("Run", type="primary")

    if "last_result" not in st.session_state:
        st.session_state["last_result"] = None

    if run_btn:
        run_id = trace_store.new_run()
        result = run_agentic_pipeline(
            settings=settings,
            trace_store=trace_store,
            run_id=run_id,
            user_question=question,
            allowed_tables=allowed_tables,
            human_review=None,
            developer_mode=developer_mode,
        )
        st.session_state["last_result"] = result

    result = st.session_state.get("last_result")
    if not result:
        st.info("Run the pipeline to generate plan + dashboard.")
        return

    status = result.get("status")
    st.markdown(f"**Run ID:** `{result.get('run_id')}` • **Status:** `{status}`")

    if status == "needs_human_review":
        st.warning("Mode E: Human review required before executing SQL.")
        packet = result.get("human_review_packet", {})
        st.json(packet)

        st.subheader("Provide explicit edits (JSON)")
        default_edit = {"allowed_tables": allowed_tables, "plan": packet.get("proposed_plan", {})}
        edit_text = st.text_area("Edits JSON", value=json.dumps(default_edit, indent=2), height=260)

        if st.button("Approve & Continue", type="primary"):
            try:
                human_review = json.loads(edit_text)
            except Exception as e:
                st.error(f"Invalid JSON edits: {e}")
                return

            run_id = result["run_id"]
            result2 = run_agentic_pipeline(
                settings=settings,
                trace_store=trace_store,
                run_id=run_id,
                user_question=question,
                allowed_tables=allowed_tables,
                human_review=human_review,
                developer_mode=developer_mode,
            )
            st.session_state["last_result"] = result2
            result = result2
            status = result.get("status")

    if status in ("failed", "rejected", "failed_data_quality"):
        st.error(result.get("error") or result.get("rejection") or result.get("data_quality"))
        st.info("Open **Run Traces** to see node outputs & errors.")
        return

    if status == "success":
        st.subheader("Insights")
        st.json(result.get("insights", {}))

        st.subheader("Dashboard Preview")
        html = result.get("dashboard_html", "")
        if html:
            components.html(html, height=900, scrolling=True)
        else:
            st.warning("Dashboard HTML missing.")

        if developer_mode:
            st.subheader("Developer Outputs")
            st.json({"sql": result.get("sql"), "exec_meta": result.get("exec_meta")})
