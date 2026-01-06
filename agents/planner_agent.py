from __future__ import annotations

from typing import Any, Dict, List, Tuple
from pathlib import Path
import re
import json

from config import Settings
from knowledge_graph.store import KnowledgeGraphStore
from knowledge_graph.schema_registry import SchemaRegistry
from core.orchestrator import build_orchestrator


def _keywordize(text: str) -> List[str]:
    toks = re.findall(r"[a-zA-Z0-9_]+", (text or "").lower())
    stop = {
        "the", "a", "an", "and", "or", "to", "of", "in", "for", "by", "with",
        "show", "give", "me", "create", "dashboard", "plot", "chart", "trend",
        "analysis", "report", "data", "details"
    }
    return [t for t in toks if t not in stop and len(t) > 2]


def _safe_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        return ""


class PlannerAgent:
    """
    Deterministic pipeline helper + LLM assisted planner.
    Never invents table/column names: we validate plan candidates against SchemaRegistry.

    Updated:
    - schema_reasoning() uses BOTH:
      (1) schema registry (table + column names)
      (2) content_index.json (sample rows + top values + content keywords)
    """

    def __init__(self, settings: Settings, kg: KnowledgeGraphStore, registry: SchemaRegistry):
        self.settings = settings
        self.kg [self.kg] = kg  # ✅ FIXED (was: self.kg [self.kg] [self.kg [self.kg]] = kg)
        self.registry = registry
        self.orch = build_orchestrator(settings.OLLAMA_BASE_URL, settings.OLLAMA_MODEL)

    # -----------------------------
    # A) Intent
    # -----------------------------
    def extract_intent(self, user_question: str, allowed_tables: List[str]) -> Dict[str, Any]:
        system = (
            "You are an analytics intent extractor. Output STRICT JSON only.\n"
            "Do NOT invent table/column names.\n"
            "Return keys: kpis (list), dimensions (list), time_range (string|null), granularity (string|null), "
            "segments (list), filters (list of {field, op, value}), confidence (0-1), notes (string)."
        )
        user = f"Question: {user_question}\nAllowed tables: {allowed_tables}"
        res = self.orch.generate_json(system=system, user=user)
        raw = res.raw if isinstance(res.raw, dict) else {}

        def _num(v: Any, default: float) -> float:
            try:
                return float(v)
            except Exception:
                return default

        intent = {
            "kpis": raw.get("kpis", []) if isinstance(raw.get("kpis", []), list) else [],
            "dimensions": raw.get("dimensions", []) if isinstance(raw.get("dimensions", []), list) else [],
            "time_range": raw.get("time_range"),
            "granularity": raw.get("granularity"),
            "segments": raw.get("segments", []) if isinstance(raw.get("segments", []), list) else [],
            "filters": raw.get("filters", []) if isinstance(raw.get("filters", []), list) else [],
            "confidence": _num(raw.get("confidence", 0.4), 0.4),
            "notes": raw.get("notes", "") if isinstance(raw.get("notes", ""), str) else "",
        }
        return intent

    # -----------------------------
    # B) Schema reasoning (content-aware)
    # -----------------------------
    def schema_reasoning(self, intent: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        """
        Scores candidate tables using:
        - table + column names (schema)
        - content index: top values + sample rows + keyword blob
        """
        reg = self.registry.load()
        all_reg_tables = list(reg.get("tables", {}).keys())

        # allowlist enforcement
        tables = [t for t in all_reg_tables if (not allowed_tables or t in allowed_tables)]

        # build query keywords from intent
        q_text = " ".join(
            (intent.get("kpis", []) or [])
            + (intent.get("dimensions", []) or [])
            + (intent.get("segments", []) or [])
            + [intent.get("notes", "") or ""]
        )
        q_words = set(_keywordize(q_text))

        # load content index
        content_path = Path(self.settings.KNOWLEDGE_GRAPH_DIR) / "content_index.json"
        content_obj: Dict[str, Any] = {}
        try:
            if content_path.exists():
                content_obj = json.loads(content_path.read_text(encoding="utf-8"))
        except Exception:
            content_obj = {}

        content_tables: Dict[str, Any] = {}
        if isinstance(content_obj, dict) and isinstance(content_obj.get("tables"), dict):
            content_tables = content_obj["tables"]

        scored: List[Tuple[float, str]] = []
        breakdown: Dict[str, Any] = {}

        for t in tables:
            tmeta = reg.get("tables", {}).get(t, {}) if isinstance(reg, dict) else {}
            cols = [
                (c.get("name", "") or "").lower()
                for c in (tmeta.get("columns", []) or [])
                if isinstance(c, dict)
            ]
            tname = (t or "").lower()

            # schema score
            schema_score = 0.0
            matched_schema: List[str] = []
            for w in q_words:
                if w in tname:
                    schema_score += 3.0
                    matched_schema.append(f"table:{w}")
                if any(w in c for c in cols):
                    schema_score += 1.0
                    matched_schema.append(f"col:{w}")

            # content score
            content_score = 0.0
            matched_content: List[str] = []

            ct = content_tables.get(t, {}) if isinstance(content_tables.get(t, {}), dict) else {}

            # 1) keyword blob
            blob = ct.get("table_text", "")
            if isinstance(blob, str) and blob:
                blob_l = blob.lower()
                for w in q_words:
                    if w in blob_l:
                        content_score += 1.5
                        matched_content.append(f"blob:{w}")

            # 2) top values
            top_vals = ct.get("top_values", {})
            if isinstance(top_vals, dict):
                for col, rows in top_vals.items():
                    if not isinstance(rows, list):
                        continue
                    for r in rows[:20]:
                        if not isinstance(r, dict):
                            continue
                        v = _safe_str(r.get("value", "")).lower()
                        if not v:
                            continue
                        for w in q_words:
                            if w in v:
                                content_score += 2.5
                                matched_content.append(f"top:{col}:{w}")
                                break

            # 3) sample rows
            samples = ct.get("sample_rows", [])
            if isinstance(samples, list) and samples:
                parts: List[str] = []
                for row in samples[:10]:
                    if isinstance(row, dict):
                        for _, v in row.items():
                            vs = _safe_str(v).lower()
                            if vs and len(vs) <= 80:
                                parts.append(vs)
                sample_text = " ".join(parts)
                for w in q_words:
                    if w in sample_text:
                        content_score += 1.0
                        matched_content.append(f"sample:{w}")

            # joinability bonus
            join_bonus = 0.0
            hints = tmeta.get("pk_fk_hints", {})
            if isinstance(hints, dict) and (hints.get("primary_keys") or hints.get("foreign_keys")):
                join_bonus = 0.5

            total = schema_score + content_score + join_bonus

            breakdown[t] = {
                "total_score": round(total, 3),
                "schema_score": round(schema_score, 3),
                "content_score": round(content_score, 3),
                "join_bonus": round(join_bonus, 3),
                "matched_schema": matched_schema[:30],
                "matched_content": matched_content[:40],
                "row_count": int(tmeta.get("row_count", 0) or 0),
                "has_content_index": bool(ct),
            }

            scored.append((total, t))

        scored.sort(key=lambda x: x[0], reverse=True)

        top = [t for s, t in scored if s > 0][:12]
        if not top:
            top = [t for _, t in scored[:8]]

        return {
            "candidate_tables": top,
            "scoring_top": [(round(s, 3), t) for s, t in scored[:30]],
            "score_breakdown": {t: breakdown[t] for t in top if t in breakdown},
            "used_content_index": bool(content_tables),
            "query_keywords": sorted(list(q_words))[:80],
        }

    # -----------------------------
    # C) Plan
    # -----------------------------
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

        system = (
            "You are a senior analytics planner. Output STRICT JSON only.\n"
            "IMPORTANT: You MUST only reference tables from candidate_tables and columns from schema.\n"
            "Plan JSON keys:\n"
            "tables (list of table keys), joins (list of {left_table,right_table,left_key,right_key,join_type}),\n"
            "metrics (list of {name, agg, field, depends_on}), dimensions (list), filters (list),\n"
            "time_field (string|null), time_grain (string|null), order_by (list of {field, dir}),\n"
            "visuals (list of {type,title,x,y,color,agg}),\n"
            "expected_columns (list), query_cost_risk (low|medium|high), notes.\n"
            "Rules:\n"
            "- Do NOT use SELECT *.\n"
            "- Prefer aggregated metrics with dimensions when user asks trend/summary.\n"
            "- Keep minimal set of tables/joins.\n"
        )

        user = {
            "question": user_question,
            "intent": intent,
            "candidate_tables": candidates,
            "schema_registry_tables": {t: reg["tables"][t] for t in candidates if t in reg.get("tables", {})},
        }

        res = self.orch.generate_json(system=system, user=str(user))
        plan = res.raw if isinstance(res.raw, dict) else {}

        plan_tables = [t for t in plan.get("tables", []) if isinstance(t, str)]
        plan_tables = [t for t in plan_tables if t in reg.get("tables", {})]
        if allowed_tables:
            plan_tables = [t for t in plan_tables if t in allowed_tables]

        if not plan_tables:
            plan_tables = candidates[:2]

        plan["tables"] = plan_tables

        plan.setdefault("joins", [])
        plan.setdefault("metrics", [])
        plan.setdefault("dimensions", [])
        plan.setdefault("filters", [])
        plan.setdefault("visuals", [])
        plan.setdefault("order_by", [])
        plan.setdefault("time_field", None)
        plan.setdefault("time_grain", intent.get("granularity"))
        plan.setdefault("query_cost_risk", self._estimate_cost_risk(plan_tables))
        plan.setdefault("notes", "")
        plan.setdefault("expected_columns", [])
        return plan

    # -----------------------------
    # HITL
    # -----------------------------
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
                "plan.time_grain",
                "plan.order_by",
                "plan.visuals",
            ],
            "edit_schema": {
                "allowed_tables": "list[str] (subset of available tables)",
                "plan": "object (same shape as proposed_plan)",
            },
        }

    def apply_human_review(self, plan: Dict[str, Any], review: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        new_allowed = allowed_tables
        if isinstance(review.get("allowed_tables"), list):
            new_allowed = [str(x) for x in review["allowed_tables"]]

        if isinstance(review.get("plan"), dict):
            for k, v in review["plan"].items():
                plan[k] = v

        reg = self.registry.load()
        plan_tables = [t for t in plan.get("tables", []) if t in reg.get("tables", {})]
        if new_allowed:
            plan_tables = [t for t in plan_tables if t in new_allowed]
        plan["tables"] = plan_tables

        return {"ok": True, "allowed_tables": new_allowed, "plan": plan}

    # -----------------------------
    # cost heuristic
    # -----------------------------
    def _estimate_cost_risk(self, tables: List[str]) -> str:
        reg = self.registry.load().get("tables", {})
        rows = [int(reg.get(t, {}).get("row_count", 0) or 0) for t in tables]
        if not rows:
            return "low"
        total = 1
        for r in rows[:3]:
            total *= max(r, 1)
        if total > 10**18:
            return "high"
        if total > 10**14:
            return "medium"
        return "low"