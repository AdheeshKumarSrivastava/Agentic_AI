from __future__ import annotations

import json
from typing import Any, Dict, List

import streamlit as st
import streamlit.components.v1 as components

from config import Settings
from traces.trace_store import TraceStore
from knowledge_graph.schema_registry import SchemaRegistry
from knowledge_graph.store import KnowledgeGraphStore


# -----------------------------
# Internal helpers
# -----------------------------
def _ss_init(key: str, default: Any) -> None:
    if key not in st.session_state:
        st.session_state[key] = default


def _reset_suggestions() -> None:
    st.session_state.pop("suggested_tables", None)
    st.session_state.pop("suggested_intent", None)
    st.session_state.pop("suggested_reasoning", None)


def _suggest_tables(
    *,
    settings: Settings,
    kg: KnowledgeGraphStore,
    registry: SchemaRegistry,
    question: str,
) -> None:
    """
    Generate suggestions from full schema (not current allowlist).
    Uses PlannerAgent.schema_reasoning().
    Lazy import avoids circular imports.
    """
    from agents.planner_agent import PlannerAgent  # lazy import

    schema = kg.load_schema()
    all_tables = sorted(schema.get("tables", {}).keys())

    planner = PlannerAgent(settings=settings, kg=kg, registry=registry)

    intent = planner.extract_intent(user_question=question, allowed_tables=all_tables)
    reasoning = planner.schema_reasoning(intent=intent, allowed_tables=all_tables)

    st.session_state["suggested_tables"] = reasoning.get("candidate_tables", []) or []
    st.session_state["suggested_intent"] = intent
    st.session_state["suggested_reasoning"] = reasoning


def _run_pipeline(
    *,
    settings: Settings,
    trace_store: TraceStore,
    question: str,
    allowed_tables: List[str],
    developer_mode: bool,
    large_mode: bool,
    human_review: Dict[str, Any] | None,
    run_id: str | None = None,
) -> Dict[str, Any]:
    """
    Runs pipeline with safe lazy import (avoids circular import issues).
    """
    from core.run_pipeline import run_agentic_pipeline  # lazy import

    if run_id is None:
        run_id = trace_store.new_run()

    return run_agentic_pipeline(
        settings=settings,
        trace_store=trace_store,
        run_id=run_id,
        user_question=question,
        allowed_tables=allowed_tables,
        human_review=human_review,
        developer_mode=developer_mode,
        large_mode=bool(large_mode),
    )


# -----------------------------
# Main page
# -----------------------------
def render_ask_analytics(settings: Settings, trace_store: TraceStore, developer_mode: bool) -> None:
    st.header("Ask Analytics")

    kg = KnowledgeGraphStore(settings.KNOWLEDGE_GRAPH_DIR)
    registry = SchemaRegistry(settings.KNOWLEDGE_GRAPH_DIR)

    schema = kg.load_schema()
    all_tables = sorted(schema.get("tables", {}).keys())
    if not all_tables:
        st.warning("Schema not available yet. (Auto bootstrap should run.)")
        return

    # -----------------------------
    # Session defaults (ONLY ONCE)
    # -----------------------------
    _ss_init("question_tmp", "")
    _ss_init("last_result", None)

    # Canonical allowlist + widget backing state
    _ss_init("allowed_tables", list(all_tables))
    _ss_init("allowed_tables_picker", list(st.session_state["allowed_tables"]))

    # large_mode is widget-owned; initialize only once
    _ss_init("large_mode", True)

    # -----------------------------
    # 1) Question
    # -----------------------------
    st.subheader("1) Ask your question")
    question = st.text_area(
        "Business question",
        value=st.session_state["question_tmp"],
        height=90,
        placeholder="e.g., Create a dashboard for weekly revenue trend by region and top customers.",
    )
    st.session_state["question_tmp"] = question

    # ------------------------------------------------------------
    # 2) Table selection + suggestions (STREAMLIT SAFE)
    # ------------------------------------------------------------
    st.divider()
    st.subheader("2) Table Selection (You control the final allowlist)")

    # ✅ Apply pending allowlist BEFORE widget instantiation
    if "pending_allowed_tables" in st.session_state:
        st.session_state["allowed_tables_picker"] = list(st.session_state.pop("pending_allowed_tables"))

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        st.caption("Current allowlist (agents will only use these tables)")

        allowed_tables = st.multiselect(
            "Allowed tables",
            options=all_tables,
            key="allowed_tables_picker",
        )

        # Canonical copy
        st.session_state["allowed_tables"] = list(allowed_tables)

        a, b, c = st.columns([1, 1, 2])
        with a:
            if st.button("Reset to ALL", use_container_width=True):
                st.session_state["pending_allowed_tables"] = list(all_tables)
                st.rerun()

        with b:
            if st.button("Clear", use_container_width=True):
                st.session_state["pending_allowed_tables"] = []
                st.rerun()

        with c:
            st.write(f"Selected: **{len(allowed_tables)}** / {len(all_tables)}")

    with col2:
        st.caption("Agent suggestion")

        suggest_btn = st.button("Suggest from question")
        if suggest_btn:
            if not question.strip():
                st.error("Type a question first.")
            else:
                _suggest_tables(settings=settings, kg=kg, registry=registry, question=question)
                st.rerun()

        suggested = st.session_state.get("suggested_tables", []) or []
        if suggested:
            st.success(f"Suggested {len(suggested)} tables")
            st.write(suggested)

            if st.button("Apply suggested as allowlist", type="primary"):
                # ✅ apply via pending BEFORE widget on next rerun
                st.session_state["pending_allowed_tables"] = list(suggested)
                st.rerun()

            if st.button("Reset suggestion"):
                _reset_suggestions()
                st.rerun()

            with st.expander("Show suggestion details (intent + scoring)"):
                st.json(
                    {
                        "intent": st.session_state.get("suggested_intent", {}),
                        "schema_reasoning": st.session_state.get("suggested_reasoning", {}),
                    }
                )

    # -----------------------------
    # 3) Runtime controls
    # -----------------------------
    st.divider()
    st.subheader("3) Runtime controls")

    # ✅ widget-owned key ONLY (never assign st.session_state["large_mode"] after this)
    st.toggle(
        "Large Query Mode (use MAX_RETURNED_ROWS)",
        key="large_mode",
        help="ON: SQLAgent uses TOP(MAX_RETURNED_ROWS). OFF: TOP(DEFAULT_EXPLORATORY_TOP).",
    )

    # -----------------------------
    # 4) Run pipeline
    # -----------------------------
    st.divider()
    st.subheader("4) Run pipeline (Plan → HITL → Execute → Insights → Dashboard)")

    run_disabled = not bool(question.strip())
    run_btn = st.button("Run", type="primary", disabled=run_disabled)

    if run_btn:
        result = _run_pipeline(
            settings=settings,
            trace_store=trace_store,
            question=question,
            allowed_tables=list(st.session_state["allowed_tables"]),
            developer_mode=developer_mode,
            large_mode=bool(st.session_state["large_mode"]),
            human_review=None,
        )
        st.session_state["last_result"] = result
        st.rerun()

    # -----------------------------
    # Results
    # -----------------------------
    result = st.session_state.get("last_result")
    if not result:
        st.info("Run the pipeline to generate plan + insights + dashboard.")
        return

    status = result.get("status")
    st.markdown(f"**Run ID:** `{result.get('run_id')}` • **Status:** `{status}`")

    # -----------------------------
    # HITL
    # -----------------------------
    if status == "needs_human_review":
        st.warning("Human review required before executing SQL.")
        packet = result.get("human_review_packet", {}) or {}
        st.json(packet)

        st.subheader("Provide explicit edits (JSON)")
        default_edit = {
            "allowed_tables": list(st.session_state["allowed_tables"]),
            "plan": packet.get("proposed_plan", {}) or {},
            # optional override:
            # "large_mode": bool(st.session_state["large_mode"]),
        }
        edit_text = st.text_area("Edits JSON", value=json.dumps(default_edit, indent=2), height=280)

        if st.button("Approve & Continue", type="primary"):
            try:
                human_review = json.loads(edit_text)
                if not isinstance(human_review, dict):
                    raise ValueError("Edits JSON must be an object/dict.")
            except Exception as e:
                st.error(f"Invalid JSON edits: {e}")
                return

            result2 = _run_pipeline(
                settings=settings,
                trace_store=trace_store,
                question=question,
                allowed_tables=list(st.session_state["allowed_tables"]),
                developer_mode=developer_mode,
                large_mode=bool(st.session_state["large_mode"]),
                human_review=human_review,
                run_id=str(result.get("run_id")),
            )
            st.session_state["last_result"] = result2
            st.rerun()

        return

    # -----------------------------
    # Failure states
    # -----------------------------
    if status in ("failed", "rejected", "failed_data_quality"):
        st.error(result.get("error") or result.get("rejection") or result.get("data_quality") or "Unknown error")
        st.info("Open **Run Traces** to see node outputs & errors.")
        if developer_mode:
            st.subheader("Developer dump")
            st.json(result)
        return

    # -----------------------------
    # Success
    # -----------------------------
    if status == "success":
        st.subheader("Insights")
        st.json(result.get("insights", {}) or {})

        st.subheader("Dashboard Preview")
        html = (result.get("dashboard_html", "") or "").strip()
        if html:
            components.html(html, height=900, scrolling=True)
            st.download_button(
                "Download Dashboard HTML",
                data=html.encode("utf-8"),
                file_name=f"dashboard_{result.get('run_id','run')}.html",
                mime="text/html",
            )
        else:
            st.warning("Dashboard HTML missing.")

        if developer_mode:
            st.subheader("Developer Outputs")
            st.json(
                {
                    "large_mode": bool(st.session_state["large_mode"]),
                    "allowed_tables_count": len(st.session_state["allowed_tables"]),
                    "allowed_tables_preview": st.session_state["allowed_tables"][:50],
                    "sql": result.get("sql"),
                    "exec_meta": result.get("exec_meta"),
                    "rows": result.get("rows"),
                    "columns": result.get("columns"),
                }
            )