from __future__ import annotations

from typing import Any, Dict, Optional, List
import traceback


PIPELINE_STEPS = [
    "A_intent",
    "B_schema_reasoning",
    "C_plan",
    "D_human_review",
    "E_sql_generation",
    "F_sql_safety",
    "G_execute",
    "H_data_validation",
    "I_insights",
    "J_dashboard",
    "K_render",
    "L_critique_rollup",
]


def run_agentic_pipeline(
    *,
    settings,
    trace_store,
    run_id: str,
    user_question: str,
    allowed_tables: List[str],
    human_review: Optional[Dict[str, Any]],
    developer_mode: bool,
    large_mode: bool,
) -> Dict[str, Any]:
    """
    Runs A→L deterministically, persisting node outputs to TraceStore.
    Human review packet is applied ONLY if provided explicitly.

    large_mode:
      - True: SQLAgent uses TOP(MAX_RETURNED_ROWS)
      - False: SQLAgent uses TOP(DEFAULT_EXPLORATORY_TOP)
    """

    # -------------------------
    # LAZY IMPORTS (break circular imports)
    # -------------------------
    from knowledge_graph.schema_registry import SchemaRegistry
    from knowledge_graph.store import KnowledgeGraphStore

    from agents.planner_agent import PlannerAgent
    from agents.sql_agent import SQLAgent
    from guards.sql_safety import SQLSafetyGuard
    from agents.executor import Executor
    from agents.data_quality_agent import DataQualityAgent
    from agents.insight_agent import InsightAgent
    from agents.dashboard_agent import DashboardAgent
    from agents.critique_agent import CritiqueAgent

    from observability.query_log import QueryLogStore

    # -------------------------
    # Init shared stores/agents
    # -------------------------
    kg = KnowledgeGraphStore(settings.KNOWLEDGE_GRAPH_DIR)
    registry = SchemaRegistry(settings.KNOWLEDGE_GRAPH_DIR)

    planner = PlannerAgent(settings=settings, kg=kg, registry=registry)
    sql_agent = SQLAgent(settings=settings, registry=registry)
    guard = SQLSafetyGuard(settings=settings)
    executor = Executor(settings=settings)
    dq = DataQualityAgent()
    insight = InsightAgent()
    dashboard = DashboardAgent(settings=settings)
    critique = CritiqueAgent(settings=settings)
    query_logs = QueryLogStore(settings.LOG_DIR)

    final: Dict[str, Any] = {"run_id": run_id, "status": "started"}

    # Record run-level config into traces
    trace_store.add_node(
        run_id,
        "RUN_CONFIG",
        {
            "developer_mode": bool(developer_mode),
            "large_mode": bool(large_mode),
            "allowed_tables_count": len(allowed_tables),
            "allowed_tables_preview": allowed_tables[:50],
        },
    )

    # -------------------------
    # A) Intent extraction
    # -------------------------
    try:
        intent = planner.extract_intent(user_question=user_question, allowed_tables=allowed_tables)
        trace_store.add_node(run_id, "A_intent", intent)
        critique_a = critique.critique_step("A_intent", intent)
        trace_store.add_node(run_id, "A_intent__critique", critique_a)
    except Exception as e:
        trace_store.add_error(run_id, "A_intent", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Intent failed: {e}"}

    # -------------------------
    # B) Schema reasoning
    # -------------------------
    try:
        schema_reasoning = planner.schema_reasoning(intent=intent, allowed_tables=allowed_tables)
        trace_store.add_node(run_id, "B_schema_reasoning", schema_reasoning)
        critique_b = critique.critique_step("B_schema_reasoning", schema_reasoning)
        trace_store.add_node(run_id, "B_schema_reasoning__critique", critique_b)
    except Exception as e:
        trace_store.add_error(run_id, "B_schema_reasoning", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Schema reasoning failed: {e}"}

    # -------------------------
    # C) Plan generation
    # -------------------------
    try:
        plan = planner.build_plan(
            user_question=user_question,
            intent=intent,
            schema_reasoning=schema_reasoning,
            allowed_tables=allowed_tables,
        )

        # Attach large_mode to plan
        plan["large_mode"] = bool(large_mode)

        trace_store.add_node(run_id, "C_plan", plan)
        trace_store.add_node(run_id, "C_plan__large_mode", {"large_mode": bool(large_mode)})

        critique_c = critique.critique_step("C_plan", plan)
        trace_store.add_node(run_id, "C_plan__critique", critique_c)
    except Exception as e:
        trace_store.add_error(run_id, "C_plan", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Plan failed: {e}"}

    # -------------------------
    # D) Human review checkpoint
    # -------------------------
    try:
        review_packet = planner.build_human_review_packet(plan=plan, intent=intent, allowed_tables=allowed_tables)
        trace_store.add_node(run_id, "D_human_review", review_packet)

        if human_review is not None:
            applied = planner.apply_human_review(plan=plan, review=human_review, allowed_tables=allowed_tables)
            plan = applied["plan"]
            allowed_tables = applied["allowed_tables"]

            if isinstance(human_review, dict) and "large_mode" in human_review:
                plan["large_mode"] = bool(human_review["large_mode"])
            else:
                plan["large_mode"] = bool(large_mode)

            trace_store.add_node(run_id, "D_human_review__applied", applied)

        critique_d = critique.critique_step("D_human_review", {"review_packet": review_packet, "applied": human_review})
        trace_store.add_node(run_id, "D_human_review__critique", critique_d)

        if critique_d.get("force_hitl") and human_review is None:
            final.update({"status": "needs_human_review", "human_review_packet": review_packet})
            trace_store.finalize(run_id, status="needs_human_review")
            return final
    except Exception as e:
        trace_store.add_error(run_id, "D_human_review", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Human review failed: {e}"}

    # -------------------------
    # E) SQL generation
    # -------------------------
    try:
        sql_bundle = sql_agent.generate_sql(
            plan=plan,
            allowed_tables=allowed_tables,
            large_mode=bool(plan.get("large_mode", large_mode)),
        )
        trace_store.add_node(run_id, "E_sql_generation", sql_bundle)
        critique_e = critique.critique_step("E_sql_generation", sql_bundle)
        trace_store.add_node(run_id, "E_sql_generation__critique", critique_e)
    except Exception as e:
        trace_store.add_error(run_id, "E_sql_generation", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"SQL generation failed: {e}"}

    # -------------------------
    # F) SQL safety validation
    # -------------------------
    try:
        safety = guard.validate(sql_bundle["sql"])
        trace_store.add_node(run_id, "F_sql_safety", safety)
        critique_f = critique.critique_step("F_sql_safety", safety)
        trace_store.add_node(run_id, "F_sql_safety__critique", critique_f)
        if not safety["ok"]:
            final.update({"status": "rejected", "rejection": safety})
            trace_store.finalize(run_id, status="rejected")
            return final
    except Exception as e:
        trace_store.add_error(run_id, "F_sql_safety", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Safety validation failed: {e}"}

    # -------------------------
    # G) Execute SQL safely (with cache)
    # -------------------------
    try:
        df, exec_meta = executor.run(sql=sql_bundle["sql"], params=sql_bundle.get("params") or {})
        trace_store.add_node(run_id, "G_execute", exec_meta)
        query_logs.append(exec_meta)
        critique_g = critique.critique_step("G_execute", exec_meta)
        trace_store.add_node(run_id, "G_execute__critique", critique_g)
    except Exception as e:
        trace_store.add_error(run_id, "G_execute", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Execution failed: {e}"}

    # -------------------------
    # H) Data validation
    # -------------------------
    try:
        dq_report = dq.run(df, expected_columns=plan.get("expected_columns"))
        trace_store.add_node(run_id, "H_data_validation", dq_report)
        critique_h = critique.critique_step("H_data_validation", dq_report)
        trace_store.add_node(run_id, "H_data_validation__critique", critique_h)
        if not dq_report["ok"]:
            final.update({"status": "failed_data_quality", "data_quality": dq_report})
            trace_store.finalize(run_id, status="failed_data_quality")
            return final
    except Exception as e:
        trace_store.add_error(run_id, "H_data_validation", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Data validation failed: {e}"}

    # -------------------------
    # I) Insights
    # -------------------------
    try:
        insights = insight.generate(df=df, plan=plan)
        trace_store.add_node(run_id, "I_insights", insights)
        critique_i = critique.critique_step("I_insights", insights)
        trace_store.add_node(run_id, "I_insights__critique", critique_i)
    except Exception as e:
        trace_store.add_error(run_id, "I_insights", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Insights failed: {e}"}

    # -------------------------
    # J) Dashboard generation
    # -------------------------
    try:
        html_bundle = dashboard.build_dashboard(df=df, plan=plan, insights=insights)
        trace_store.add_node(run_id, "J_dashboard", {"dashboard_meta": html_bundle["meta"]})
        trace_store.add_node(run_id, "J_dashboard__html", {"html": html_bundle["html"][:5000], "note": "truncated"})
        critique_j = critique.critique_step("J_dashboard", html_bundle["meta"])
        trace_store.add_node(run_id, "J_dashboard__critique", critique_j)
    except Exception as e:
        trace_store.add_error(run_id, "J_dashboard", str(e), traceback.format_exc())
        return {"run_id": run_id, "status": "failed", "error": f"Dashboard failed: {e}"}

    # -------------------------
    # K) Render
    # -------------------------
    trace_store.add_node(run_id, "K_render", {"ok": True, "note": "Rendered in Streamlit UI"})
    critique_k = critique.critique_step("K_render", {"ok": True})
    trace_store.add_node(run_id, "K_render__critique", critique_k)

    # -------------------------
    # L) Critique rollup
    # -------------------------
    rollup = critique.rollup(
        [
            ("A", trace_store.get_node(run_id, "A_intent__critique")),
            ("B", trace_store.get_node(run_id, "B_schema_reasoning__critique")),
            ("C", trace_store.get_node(run_id, "C_plan__critique")),
            ("D", trace_store.get_node(run_id, "D_human_review__critique")),
            ("E", trace_store.get_node(run_id, "E_sql_generation__critique")),
            ("F", trace_store.get_node(run_id, "F_sql_safety__critique")),
            ("G", trace_store.get_node(run_id, "G_execute__critique")),
            ("H", trace_store.get_node(run_id, "H_data_validation__critique")),
            ("I", trace_store.get_node(run_id, "I_insights__critique")),
            ("J", trace_store.get_node(run_id, "J_dashboard__critique")),
            ("K", trace_store.get_node(run_id, "K_render__critique")),
        ]
    )
    trace_store.add_node(run_id, "L_critique_rollup", rollup)

    final.update(
        {
            "status": "success",
            "plan": plan,
            "sql": sql_bundle["sql"],
            "params": sql_bundle.get("params") or {},
            "exec_meta": exec_meta,
            "data_quality": dq_report,
            "insights": insights,
            "dashboard_html": html_bundle["html"],
            "dashboard_meta": html_bundle["meta"],
            "df_preview": df.head(50).to_dict(orient="records"),
            "columns": list(df.columns),
            "rows": int(len(df)),
        }
    )
    trace_store.finalize(run_id, status="success")
    return final