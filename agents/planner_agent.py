from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from config import Settings
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry
from core.orchestrator import build_orchestrator


def _keywordize(text: str) -> List[str]:
    toks = re.findall(r"[a-zA-Z0-9_]+", (text or "").lower())
    stop = {"the", "a", "an", "and", "or", "to", "of", "in", "for", "by", "with", "show", "give", "me", "create", "dashboard"}
    return [t for t in toks if t not in stop and len(t) > 2]


class PlannerAgent:
    """
    Deterministic pipeline helper + LLM assisted planner.
    Never invents table/column names: we validate plan candidates against SchemaRegistry.
    """

    def __init__(self, settings: Settings, kg: KnowledgeGraphStore, registry: SchemaRegistry):
        self.settings = settings
        self.kg = kg
        self.registry = registry
        self.orch = build_orchestrator(settings.OLLAMA_BASE_URL, settings.OLLAMA_MODEL)

    def extract_intent(self, user_question: str, allowed_tables: List[str]) -> Dict[str, Any]:
        # LLM prompt but structured; fallback ok.
        system = (
            "You are an analytics intent extractor. Output STRICT JSON only.\n"
            "Do NOT invent table/column names.\n"
            "Return keys: kpis (list), dimensions (list), time_range (string|null), granularity (string|null), "
            "segments (list), filters (list of {field, op, value}), confidence (0-1), notes (string)."
        )
        user = f"Question: {user_question}\nAllowed tables: {allowed_tables}"
        res = self.orch.generate_json(system=system, user=user)
        raw = res.raw if isinstance(res.raw, dict) else {}
        # Hard default
        intent = {
            "kpis": raw.get("kpis", []),
            "dimensions": raw.get("dimensions", []),
            "time_range": raw.get("time_range"),
            "granularity": raw.get("granularity"),
            "segments": raw.get("segments", []),
            "filters": raw.get("filters", []),
            "confidence": float(raw.get("confidence", 0.4)) if str(raw.get("confidence", "")).replace(".", "").isdigit() else 0.4,
            "notes": raw.get("notes", ""),
        }
        return intent

    def schema_reasoning(self, intent: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        # Deterministic matching: score tables by matching keywords to table/column names
        reg = self.registry.load()
        tables = [t for t in reg.get("tables", {}).keys() if (not allowed_tables or t in allowed_tables)]
        q_words = set(_keywordize(" ".join(intent.get("kpis", []) + intent.get("dimensions", []) + intent.get("segments", []) + [intent.get("notes", "")])))

        scored = []
        for t in tables:
            cols = [c["name"].lower() for c in reg["tables"][t].get("columns", [])]
            tname = t.lower()
            score = 0
            for w in q_words:
                if w in tname:
                    score += 3
                if any(w in c for c in cols):
                    score += 1
            scored.append((score, t))

        scored.sort(reverse=True)
        top = [t for s, t in scored if s > 0][:12]
        return {"candidate_tables": top, "scoring": scored[:20]}

    def build_plan(
        self,
        user_question: str,
        intent: Dict[str, Any],
        schema_reasoning: Dict[str, Any],
        allowed_tables: List[str],
    ) -> Dict[str, Any]:
        reg = self.registry.load()
        candidates = schema_reasoning.get("candidate_tables", [])
        if allowed_tables:
            candidates = [t for t in candidates if t in allowed_tables]

        # Ask LLM to propose plan using only candidate tables.
        system = (
            "You are a senior analytics planner. Output STRICT JSON only.\n"
            "IMPORTANT: You MUST only reference tables from candidate_tables and columns from schema.\n"
            "Plan JSON keys:\n"
            "tables (list of table keys), joins (list of {left_table,right_table,left_key,right_key,join_type}),\n"
            "metrics (list of {name, expr, depends_on}), dimensions (list), filters (list),\n"
            "time_field (string|null), time_granularity (string|null), visuals (list of {type,title,x,y,color,agg}),\n"
            "expected_columns (list), query_cost_risk (low|medium|high), notes.\n"
        )
        user = {
            "question": user_question,
            "intent": intent,
            "candidate_tables": candidates,
            "schema_registry_tables": {t: reg["tables"][t] for t in candidates if t in reg["tables"]},
        }
        res = self.orch.generate_json(system=system, user=str(user))
        plan = res.raw if isinstance(res.raw, dict) else {}

        # Validate: tables must exist and be allowed
        plan_tables = [t for t in plan.get("tables", []) if isinstance(t, str)]
        plan_tables = [t for t in plan_tables if t in reg.get("tables", {})]
        if allowed_tables:
            plan_tables = [t for t in plan_tables if t in allowed_tables]

        # If LLM failed, fallback to deterministic plan
        if not plan_tables:
            plan_tables = candidates[:2]

        plan["tables"] = plan_tables

        # Ensure expected_columns are explicit and valid; if missing, set later in SQLAgent
        plan.setdefault("joins", [])
        plan.setdefault("metrics", [])
        plan.setdefault("dimensions", [])
        plan.setdefault("filters", [])
        plan.setdefault("visuals", [])
        plan.setdefault("query_cost_risk", self._estimate_cost_risk(plan_tables))
        plan.setdefault("notes", "")
        plan.setdefault("expected_columns", [])
        return plan

    def build_human_review_packet(self, plan: Dict[str, Any], intent: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        return {
            "mode": "E_HUMAN_REVIEW",
            "instructions": [
                "Approve or edit ONLY what you want changed.",
                "If you remove tables, ensure joins/metrics still make sense.",
                "Edits must be explicit. System will not infer unstated changes.",
            ],
            "current_allowed_tables": allowed_tables,
            "proposed_plan": plan,
            "intent": intent,
            "editable_fields": [
                "allowed_tables",
                "plan.tables",
                "plan.joins",
                "plan.metrics",
                "plan.dimensions",
                "plan.filters",
                "plan.time_field",
                "plan.time_granularity",
                "plan.visuals",
            ],
            "edit_schema": {
                "allowed_tables": "list[str] (subset of available tables)",
                "plan": "object (same shape as proposed_plan)",
            },
        }

    def apply_human_review(self, plan: Dict[str, Any], review: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        # Apply only explicit fields
        new_allowed = allowed_tables
        if isinstance(review.get("allowed_tables"), list):
            new_allowed = [str(x) for x in review["allowed_tables"]]

        if isinstance(review.get("plan"), dict):
            # Replace only keys present
            for k, v in review["plan"].items():
                plan[k] = v

        # Validate against registry
        reg = self.registry.load()
        plan_tables = [t for t in plan.get("tables", []) if t in reg.get("tables", {})]
        if new_allowed:
            plan_tables = [t for t in plan_tables if t in new_allowed]
        plan["tables"] = plan_tables

        return {"ok": True, "allowed_tables": new_allowed, "plan": plan}

    def _estimate_cost_risk(self, tables: List[str]) -> str:
        reg = self.registry.load().get("tables", {})
        rows = [int(reg.get(t, {}).get("row_count", 0) or 0) for t in tables]
        if not rows:
            return "low"
        # simple heuristic
        total = 1
        for r in rows[:3]:
            total *= max(r, 1)
        if total > 10**18:
            return "high"
        if total > 10**14:
            return "medium"
        return "low"
