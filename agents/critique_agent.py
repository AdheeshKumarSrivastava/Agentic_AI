from __future__ import annotations

from typing import Any, Dict, List, Tuple

from config import Settings


class CritiqueAgent:
    """
    Self-review after each node.
    Can force HITL if confidence/risks are high.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def critique_step(self, step: str, payload: Any) -> Dict[str, Any]:
        # Deterministic critique rules to avoid "LLM hallucination critique"
        issues = []
        force_hitl = False
        confidence = 0.7

        if payload is None:
            issues.append("Payload is None.")
            confidence = 0.2
            force_hitl = True

        if step == "C_plan":
            # If plan has no tables/metrics/dimensions, risk
            if isinstance(payload, dict):
                if not payload.get("tables"):
                    issues.append("Plan missing tables.")
                    confidence = 0.2
                    force_hitl = True
                if payload.get("query_cost_risk") == "high":
                    issues.append("Query cost risk HIGH — recommend human review.")
                    force_hitl = True
                    confidence = min(confidence, 0.5)

        if step == "F_sql_safety":
            if isinstance(payload, dict) and not payload.get("ok", False):
                issues.append("Safety validation failed.")
                confidence = 0.1

        if step == "H_data_validation":
            if isinstance(payload, dict) and not payload.get("ok", False):
                issues.append("Data quality checks failed; cannot proceed.")
                confidence = 0.1
                force_hitl = True

        return {
            "step": step,
            "confidence": confidence,
            "force_hitl": force_hitl,
            "issues": issues,
            "recommendation": "Proceed" if not force_hitl else "Needs human review",
        }

    def rollup(self, critiques: List[Tuple[str, Any]]) -> Dict[str, Any]:
        issues = []
        min_conf = 1.0
        forced = False
        for _, c in critiques:
            if isinstance(c, dict):
                min_conf = min(min_conf, float(c.get("confidence", 1.0)))
                if c.get("force_hitl"):
                    forced = True
                issues.extend(c.get("issues", []))
        return {"min_confidence": min_conf, "force_hitl_any": forced, "issues": issues[:50]}
