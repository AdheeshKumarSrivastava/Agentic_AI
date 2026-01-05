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

    # ------------------------------------------------------------
    # Session defaults
    # ------------------------------------------------------------
    if "allowed_tables" not in st.session_state:
        st.session_state["allowed_tables"] = all_tables

    if "large_mode" not in st.session_state:
        st.session_state["large_mode"] = True

    if "last_result" not in st.session_state:
        st.session_state["last_result"] = None

    # ------------------------------------------------------------
    # 1) Question
    # ------------------------------------------------------------
    st.subheader("1) Ask your question")
    question = st.text_area(
        "Business question",
        value=st.session_state.get("question_tmp", ""),
        height=90,
        placeholder="e.g., Create a dashboard for weekly revenue trend by region and top customers.",
    )
    st.session_state["question_tmp"] = question

    # ------------------------------------------------------------
    # 2) Table selection + LLM suggestions
    # ------------------------------------------------------------
    st.divider()
    st.subheader("2) Table Selection (You control the final allowlist)")

    planner = PlannerAgent(settings=settings, kg=kg, registry=registry)

    # ✅ Apply pending updates BEFORE widget instantiation (Streamlit requirement)
    if "pending_allowed_tables" in st.session_state:
        st.session_state["allowed_tables"] = st.session_state.pop("pending_allowed_tables")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.caption("Current allowlist (agents will only use these)")

        # No widget-key mutation after instantiation:
        allowed_tables = st.multiselect(
            "Allowed tables",
            options=all_tables,
            default=st.session_state.get("allowed_tables", all_tables),
            key="allowed_tables_picker",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Reset allowlist to ALL tables"):
                st.session_state["pending_allowed_tables"] = all_tables
                st.rerun()

        with c2:
            st.write(f"Selected: **{len(allowed_tables)}**")

    with col2:
        st.caption("Agent suggestion (click to generate)")

        if st.button("Suggest tables from my question", type="secondary"):
            if not question.strip():
                st.error("Type a question first.")
            else:
                # Suggest from full schema (not current allowlist) so user can discover missing tables.
                intent = planner.extract_intent(question, allowed_tables=all_tables)
                reasoning = planner.schema_reasoning(intent=intent, allowed_tables=all_tables)

                st.session_state["suggested_tables"] = reasoning.get("candidate_tables", [])
                st.session_state["suggested_intent"] = intent
                st.session_state["suggested_reasoning"] = reasoning

        suggested = st.session_state.get("suggested_tables", [])
        if suggested:
            st.success(f"Suggested {len(suggested)} tables.")
            st.write(suggested)

            if st.button("Apply suggested tables as allowlist", type="primary"):
                st.session_state["pending_allowed_tables"] = suggested
                st.rerun()

            with st.expander("Show suggestion details (intent + scoring)"):
                st.json(
                    {
                        "intent": st.session_state.get("suggested_intent", {}),
                        "schema_reasoning": st.session_state.get("suggested_reasoning", {}),
                    }
                )

    # Persist final allowlist from widget to session state
    st.session_state["allowed_tables"] = allowed_tables

    # ------------------------------------------------------------
    # Large Query Mode (passed to pipeline only if your run_pipeline supports it)
    # ------------------------------------------------------------
    st.session_state["large_mode"] = st.toggle(
        "Large Query Mode (use MAX_RETURNED_ROWS)",
        value=bool(st.session_state["large_mode"]),
        help="ON: SQLAgent should use TOP(MAX_RETURNED_ROWS). OFF: uses TOP(DEFAULT_EXPLORATORY_TOP).",
    )

    # ------------------------------------------------------------
    # 3) Run
    # ------------------------------------------------------------
    st.divider()
    st.subheader("3) Run pipeline (Plan → HITL → Execute → Insights → Dashboard)")

    run_btn = st.button("Run", type="primary", disabled=not bool(question.strip()))

    if run_btn:
        run_id = trace_store.new_run()

        # ⚠️ If your run_agentic_pipeline signature does NOT include large_mode,
        # remove it from the call below to avoid TypeError.
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

    # ------------------------------------------------------------
    # Results
    # ------------------------------------------------------------
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

        # Download dashboard HTML
        if html:
            st.download_button(
                "Download Dashboard HTML",
                data=html.encode("utf-8"),
                file_name=f"dashboard_{result.get('run_id','run')}.html",
                mime="text/html",
            )

        if developer_mode:
            st.subheader("Developer Outputs")
            st.json(
                {
                    "sql": result.get("sql"),
                    "exec_meta": result.get("exec_meta"),
                    "large_mode": bool(st.session_state["large_mode"]),
                    "allowed_tables_count": len(allowed_tables),
                }
            )