from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

from config import Settings
from knowledge_graph.schema_registry import SchemaRegistry


class SQLAgent:
    """
    Generates SELECT-only SQL Server queries with explicit columns.
    Uses SchemaRegistry to ensure no hallucinated columns/tables.

    Key behavior:
    - Always explicit column list (no SELECT *).
    - Parameterized filters (:p0 style).
    - Respects Large Query Mode:
        - large_mode=True => TOP(MAX_RETURNED_ROWS)
        - else => TOP(DEFAULT_EXPLORATORY_TOP)
    - If plan indicates aggregation, we generate GROUP BY.
    """

    def __init__(self, settings: Settings, registry: SchemaRegistry):
        self.settings = settings
        self.registry = registry

    def generate_sql(
        self,
        plan: Dict[str, Any],
        allowed_tables: List[str],
        *,
        large_mode: Optional[bool] = None,
    ) -> Dict[str, Any]:
        reg = self.registry.load()

        # Validate planned tables exist in registry
        planned_tables = plan.get("tables", [])
        tables = [t for t in planned_tables if isinstance(t, str) and t in reg.get("tables", {})]

        # Apply allowlist
        if allowed_tables:
            tables = [t for t in tables if t in allowed_tables]

        if not tables:
            raise ValueError("No valid tables in plan after registry + allowlist validation.")

        primary = tables[0]
        joins = plan.get("joins", []) if isinstance(plan.get("joins", []), list) else []

        # FROM + JOIN clauses
        from_clause = f"FROM {self._fmt_table(primary)} AS t0"
        alias_map: Dict[str, str] = {primary: "t0"}
        join_clauses: List[str] = []
        alias_i = 1

        for j in joins:
            if not isinstance(j, dict):
                continue
            lt = j.get("left_table")
            rt = j.get("right_table")
            lk = j.get("left_key")
            rk = j.get("right_key")
            jt = (j.get("join_type") or "LEFT").upper()

            if not all(isinstance(x, str) for x in [lt, rt, lk, rk]):
                continue

            if lt not in tables or rt not in tables:
                continue
            if not self.registry.has_column(lt, lk) or not self.registry.has_column(rt, rk):
                continue

            if lt not in alias_map:
                alias_map[lt] = f"t{alias_i}"
                alias_i += 1
            if rt not in alias_map:
                alias_map[rt] = f"t{alias_i}"
                alias_i += 1

            if jt not in {"INNER", "LEFT", "RIGHT", "FULL"}:
                jt = "LEFT"

            join_clauses.append(
                f"{jt} JOIN {self._fmt_table(rt)} AS {alias_map[rt]} "
                f"ON {alias_map[lt]}.[{lk}] = {alias_map[rt]}.[{rk}]"
            )

        # Determine aggregation mode
        metrics = plan.get("metrics", []) if isinstance(plan.get("metrics", []), list) else []
        is_agg = bool(plan.get("aggregation") or plan.get("group_by") or any(self._is_metric_agg(m) for m in metrics))

        dims = [d for d in plan.get("dimensions", []) if isinstance(d, str)]
        time_field = plan.get("time_field") if isinstance(plan.get("time_field"), str) else None

        # SELECT columns (dimensions/time)
        dim_select_cols: List[str] = []
        group_by_cols: List[str] = []

        for d in dims:
            col_ref = self._resolve_column(d, tables, alias_map)
            if col_ref:
                dim_select_cols.append(col_ref)
                group_by_cols.append(col_ref.split(" AS ")[0].strip())

        if time_field:
            tf = self._resolve_column(time_field, tables, alias_map)
            if tf and tf not in dim_select_cols:
                dim_select_cols.append(tf)
                group_by_cols.append(tf.split(" AS ")[0].strip())

        # Metric SELECT columns
        metric_select_cols: List[str] = []
        metric_expected_names: List[str] = []

        for m in metrics:
            if not isinstance(m, dict):
                continue
            # Expected metric shape (PlannerAgent should produce):
            # {"name": "Revenue", "agg": "sum", "field": "amount"} or {"name": "...", "expression": "..."} (we ignore expression for safety)
            m_name = m.get("name")
            agg = (m.get("agg") or "").lower().strip()
            field = m.get("field")

            if not isinstance(m_name, str) or not m_name.strip():
                continue

            # If no agg, treat depends_on columns as raw select (non-agg)
            if not agg:
                dep = m.get("depends_on")
                if isinstance(dep, list):
                    for c in dep:
                        col_ref = self._resolve_column(str(c), tables, alias_map)
                        if col_ref and col_ref not in dim_select_cols and col_ref not in metric_select_cols:
                            metric_select_cols.append(col_ref)
                continue

            # Agg metric
            if not isinstance(field, str):
                continue
            base_col = self._resolve_column(field, tables, alias_map)
            if not base_col:
                continue
            base_left = base_col.split(" AS ")[0].strip()

            sql_agg = self._agg_sql(agg, base_left)
            alias = self._safe_alias(m_name)
            metric_select_cols.append(f"{sql_agg} AS [{alias}]")
            metric_expected_names.append(alias)

        # Fallback if nothing selected
        if not dim_select_cols and not metric_select_cols:
            # pick first N columns from primary
            cols = self.registry.table_columns(primary)
            cols = cols[: min(12, len(cols))]
            dim_select_cols = [f"{alias_map[primary]}.[{c}] AS [{c}]" for c in cols]
            # no GROUP BY; this becomes raw select

        # Deduplicate
        select_cols = self._dedupe_by_alias(dim_select_cols + metric_select_cols)

        # WHERE filters (parameterized)
        params: Dict[str, Any] = {}
        where_parts: List[str] = []

        filters = plan.get("filters", []) if isinstance(plan.get("filters", []), list) else []
        for idx, f in enumerate(filters):
            if not isinstance(f, dict):
                continue
            field = str(f.get("field", "")).strip()
            op = str(f.get("op", "=")).strip()
            value = f.get("value")

            col_ref = self._resolve_column(field, tables, alias_map)
            if not col_ref:
                continue
            left = col_ref.split(" AS ")[0].strip()

            p = f"p{idx}"
            op_l = op.lower()

            if op_l == "in":
                if not isinstance(value, list) or not value:
                    continue
                placeholders = []
                for j, vv in enumerate(value):
                    pj = f"{p}_{j}"
                    params[pj] = vv
                    placeholders.append(f":{pj}")
                where_parts.append(f"{left} IN ({', '.join(placeholders)})")
            else:
                safe_ops = {"=", "!=", "<>", ">", ">=", "<", "<=", "like"}
                if op_l not in safe_ops:
                    op = "="
                params[p] = value
                where_parts.append(f"{left} {op} :{p}")

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # GROUP BY if aggregation
        group_by_clause = ""
        if is_agg:
            # Only group by dimension/time fields
            gb = [c for c in group_by_cols if c]
            if gb:
                group_by_clause = "GROUP BY " + ", ".join(gb)

        # TOP selection (Large Query Mode)
        if large_mode is None:
            large_mode = bool(plan.get("large_mode", False))

        top = int(self.settings.MAX_RETURNED_ROWS if large_mode else self.settings.DEFAULT_EXPLORATORY_TOP)

        # In agg mode, TOP still helps if dimension cardinality is huge; keep it.
        select_prefix = f"SELECT TOP ({top}) "

        select_list = ",\n  ".join(select_cols)

        sql = "\n".join(
            [
                f"{select_prefix}\n  {select_list}",
                from_clause,
                *join_clauses,
                where_clause,
                group_by_clause,
            ]
        ).strip()

        # expected columns for downstream validation
        expected = [self._alias_name(c) for c in select_cols]
        plan["expected_columns"] = expected

        return {"sql": sql, "params": params, "expected_columns": expected, "is_aggregated": is_agg, "top": top}

    # -----------------------------
    # Helpers
    # -----------------------------

    def _fmt_table(self, table_key: str) -> str:
        schema, table = table_key.split(".", 1)
        return f"[{schema}].[{table}]"

    def _resolve_column(self, hint: str, tables: List[str], alias_map: Dict[str, str]) -> Optional[str]:
        """
        Resolve a field to an actual column expression:
        - If hint is "schema.table.col", use that exact.
        - If hint is "col", search across candidate tables for exact column name match (case-insensitive).
        """
        hint = (hint or "").strip()
        if not hint:
            return None

        # Accept schema.table.col (3-part)
        parts = hint.split(".")
        if len(parts) == 3:
            tpart = f"{parts[0]}.{parts[1]}"
            cpart = parts[2]
            if tpart in alias_map and self.registry.has_column(tpart, cpart):
                a = alias_map[tpart]
                return f"{a}.[{cpart}] AS [{cpart}]"
            return None

        # If hint is schema.table (invalid for column)
        if len(parts) == 2:
            # refuse ambiguous "table.col" because table might not be schema.table
            # we keep strict: only schema.table.col allowed
            return None

        # match by column name across tables
        for t in tables:
            for c in self.registry.table_columns(t):
                if c.lower() == hint.lower():
                    a = alias_map.get(t, "t0")
                    return f"{a}.[{c}] AS [{c}]"
        return None

    def _alias_name(self, col_expr: str) -> str:
        m = re.search(r"\s+AS\s+\[(.+?)\]\s*$", col_expr, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        return col_expr

    def _dedupe_by_alias(self, col_exprs: List[str]) -> List[str]:
        seen = set()
        cleaned: List[str] = []
        for c in col_exprs:
            alias = self._alias_name(c).strip()
            if alias and alias not in seen:
                seen.add(alias)
                cleaned.append(c)
        return cleaned

    def _safe_alias(self, name: str) -> str:
        # keep readable but safe for [alias]
        name = (name or "").strip()
        name = re.sub(r"[^a-zA-Z0-9 _-]", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name[:64] if name else "metric"

    def _is_metric_agg(self, m: Any) -> bool:
        return isinstance(m, dict) and bool((m.get("agg") or "").strip())

    def _agg_sql(self, agg: str, col_left: str) -> str:
        agg = (agg or "").lower().strip()
        if agg == "sum":
            return f"SUM({col_left})"
        if agg == "avg" or agg == "mean":
            return f"AVG({col_left})"
        if agg == "min":
            return f"MIN({col_left})"
        if agg == "max":
            return f"MAX({col_left})"
        if agg == "count":
            return "COUNT(1)"
        if agg == "count_distinct":
            return f"COUNT(DISTINCT {col_left})"
        # fallback safe
        return f"SUM({col_left})"
