from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from config import Settings
from knowledge_graph.schema_registry import SchemaRegistry


class SQLAgent:
    """
    Generates SELECT-only SQL Server queries with explicit columns.
    Uses SchemaRegistry to ensure no hallucinated columns/tables.

    Hardened behavior:
    - Never crashes if plan.tables invalid/empty -> auto-recovers from allowlist/registry.
    - Always explicit column list (no SELECT *).
    - Parameterized filters (:p0 style).
    - Respects Large Query Mode:
        - large_mode=True => TOP(MAX_RETURNED_ROWS)
        - else => TOP(DEFAULT_EXPLORATORY_TOP)
    - If plan indicates aggregation, generates GROUP BY.
    - Supports optional ORDER BY (safe).
    - Supports optional time bucketing for SQL Server.
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
        reg_tables = list((reg.get("tables") or {}).keys())

        # -------------------------
        # 1) Validate + recover tables safely
        # -------------------------
        planned_tables = plan.get("tables", [])
        planned_tables = planned_tables if isinstance(planned_tables, list) else []

        # keep only tables that exist in registry
        tables = [t for t in planned_tables if isinstance(t, str) and t in reg_tables]

        # apply allowlist
        if allowed_tables:
            allow_ok = [t for t in allowed_tables if isinstance(t, str) and t in reg_tables]
            tables = [t for t in tables if t in allow_ok]

        # recovery path (NEVER crash)
        if not tables:
            tables = self._recover_tables(reg_tables=reg_tables, allowed_tables=allowed_tables)
            plan["tables"] = list(tables)  # persist recovery so downstream nodes see it

        if not tables:
            # truly nothing available (empty registry)
            raise ValueError("Schema registry has no tables. Run schema bootstrap/ingestion first.")

        primary = tables[0]

        # -------------------------
        # 2) FROM + JOIN clauses
        # -------------------------
        joins = plan.get("joins", []) if isinstance(plan.get("joins", []), list) else []

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

            # must be in the chosen tables set
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

        # -------------------------
        # 3) Determine aggregation mode
        # -------------------------
        metrics = plan.get("metrics", []) if isinstance(plan.get("metrics", []), list) else []
        is_agg = bool(
            plan.get("aggregation")
            or plan.get("group_by")
            or any(self._is_metric_agg(m) for m in metrics)
        )

        dims = [d for d in plan.get("dimensions", []) if isinstance(d, str)]
        time_field = plan.get("time_field") if isinstance(plan.get("time_field"), str) else None
        time_grain = plan.get("time_grain") if isinstance(plan.get("time_grain"), str) else None  # day/week/month/year

        # -------------------------
        # 4) SELECT columns
        # -------------------------
        dim_select_cols: List[str] = []
        group_by_cols: List[str] = []

        for d in dims:
            col_ref = self._resolve_column(d, tables, alias_map)
            if col_ref:
                dim_select_cols.append(col_ref)
                group_by_cols.append(col_ref.split(" AS ")[0].strip())

        # time bucketing
        if time_field:
            tf = self._resolve_column(time_field, tables, alias_map)
            if tf:
                if time_grain and is_agg:
                    tf_left, tf_alias = self._split_expr_alias(tf)
                    bucket_left = self._time_bucket_sqlserver(tf_left, time_grain)
                    tf_bucket = f"{bucket_left} AS [{tf_alias}]"
                    dim_select_cols.append(tf_bucket)
                    group_by_cols.append(bucket_left)
                else:
                    if tf not in dim_select_cols:
                        dim_select_cols.append(tf)
                        group_by_cols.append(tf.split(" AS ")[0].strip())

        metric_select_cols: List[str] = []
        metric_expected_names: List[str] = []

        for m in metrics:
            if not isinstance(m, dict):
                continue

            m_name = m.get("name")
            agg = (m.get("agg") or "").lower().strip()
            field = m.get("field")

            if not isinstance(m_name, str) or not m_name.strip():
                continue

            # no agg => raw columns via depends_on
            if not agg:
                dep = m.get("depends_on")
                if isinstance(dep, list):
                    for c in dep:
                        col_ref = self._resolve_column(str(c), tables, alias_map)
                        if col_ref and col_ref not in dim_select_cols and col_ref not in metric_select_cols:
                            metric_select_cols.append(col_ref)
                continue

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

        # If nothing selected, fall back to first N columns from primary
        if not dim_select_cols and not metric_select_cols:
            cols = self.registry.table_columns(primary)
            cols = cols[: min(12, len(cols))]
            dim_select_cols = [f"{alias_map[primary]}.[{c}] AS [{c}]" for c in cols]
            is_agg = False

        select_cols = self._dedupe_by_alias(dim_select_cols + metric_select_cols)

        # -------------------------
        # 5) WHERE filters (parameterized)
        # -------------------------
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

        # -------------------------
        # 6) GROUP BY
        # -------------------------
        group_by_clause = ""
        if is_agg:
            gb = [c for c in group_by_cols if c]
            if gb:
                group_by_clause = "GROUP BY " + ", ".join(gb)

        # -------------------------
        # 7) ORDER BY (safe)
        # -------------------------
        order_by_clause = ""
        order_by = plan.get("order_by")
        if isinstance(order_by, list) and order_by:
            order_parts: List[str] = []
            for ob in order_by:
                if not isinstance(ob, dict):
                    continue
                ob_field = str(ob.get("field", "")).strip()
                ob_dir = (str(ob.get("dir", "asc")).strip().upper() or "ASC")
                if ob_dir not in {"ASC", "DESC"}:
                    ob_dir = "ASC"

                col_ref = self._resolve_column(ob_field, tables, alias_map)
                if col_ref:
                    left = col_ref.split(" AS ")[0].strip()
                    order_parts.append(f"{left} {ob_dir}")
                else:
                    safe_alias = self._safe_alias(ob_field)
                    if safe_alias in metric_expected_names:
                        order_parts.append(f"[{safe_alias}] {ob_dir}")

            if order_parts:
                order_by_clause = "ORDER BY " + ", ".join(order_parts)

        # -------------------------
        # 8) TOP selection (Large Query Mode)
        # -------------------------
        if large_mode is None:
            large_mode = bool(plan.get("large_mode", False))
        top = int(self.settings.MAX_RETURNED_ROWS if large_mode else self.settings.DEFAULT_EXPLORATORY_TOP)

        sql = "\n".join(
            [
                f"SELECT TOP ({top})",
                "  " + ",\n  ".join(select_cols),
                from_clause,
                *join_clauses,
                where_clause,
                group_by_clause,
                order_by_clause,
            ]
        ).strip()

        expected = [self._alias_name(c) for c in select_cols]
        plan["expected_columns"] = expected

        return {
            "sql": sql,
            "params": params,
            "expected_columns": expected,
            "is_aggregated": is_agg,
            "top": top,
            "order_by": order_by,
            "time_grain": time_grain,
            "recovered_tables": bool(planned_tables is None or planned_tables == [] or (planned_tables and planned_tables != tables)),
            "final_tables": list(tables),
        }

    # -----------------------------
    # Recovery logic
    # -----------------------------
    def _recover_tables(self, *, reg_tables: List[str], allowed_tables: List[str]) -> List[str]:
        # best: allowlist ∩ registry
        if allowed_tables:
            allow_ok = [t for t in allowed_tables if isinstance(t, str) and t in reg_tables]
            if allow_ok:
                return allow_ok[:2]  # keep minimal to reduce join risk
        # fallback: first registry tables
        return reg_tables[:2] if reg_tables else []

    # -----------------------------
    # Helpers
    # -----------------------------
    def _fmt_table(self, table_key: str) -> str:
        """
        Accepts:
          - "schema.table" -> "[schema].[table]"
          - "table"        -> "[table]"
        """
        table_key = (table_key or "").strip()
        if "." in table_key:
            schema, table = table_key.split(".", 1)
            return f"[{schema}].[{table}]"
        return f"[{table_key}]"

    def _resolve_column(self, hint: str, tables: List[str], alias_map: Dict[str, str]) -> Optional[str]:
        hint = (hint or "").strip()
        if not hint:
            return None

        parts = hint.split(".")

        # schema.table.col (3-part)
        if len(parts) == 3:
            tpart = f"{parts[0]}.{parts[1]}"
            cpart = parts[2]
            if tpart in alias_map and self.registry.has_column(tpart, cpart):
                a = alias_map[tpart]
                return f"{a}.[{cpart}] AS [{cpart}]"
            return None

        # reject ambiguous table.col
        if len(parts) == 2:
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
        return m.group(1) if m else col_expr

    def _split_expr_alias(self, col_expr: str) -> Tuple[str, str]:
        m = re.search(r"^(.*?)\s+AS\s+\[(.+?)\]\s*$", col_expr.strip(), flags=re.IGNORECASE)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return col_expr.strip(), self._alias_name(col_expr).strip()

    def _dedupe_by_alias(self, col_exprs: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for c in col_exprs:
            alias = self._alias_name(c).strip()
            if alias and alias not in seen:
                seen.add(alias)
                out.append(c)
        return out

    def _safe_alias(self, name: str) -> str:
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
        if agg in {"avg", "mean"}:
            return f"AVG({col_left})"
        if agg == "min":
            return f"MIN({col_left})"
        if agg == "max":
            return f"MAX({col_left})"
        if agg == "count":
            return "COUNT(1)"
        if agg == "count_distinct":
            return f"COUNT(DISTINCT {col_left})"
        return f"SUM({col_left})"

    def _time_bucket_sqlserver(self, col_left: str, grain: str) -> str:
        g = (grain or "").lower().strip()
        if g == "day":
            return f"DATEADD(day, DATEDIFF(day, 0, {col_left}), 0)"
        if g == "week":
            return f"DATEADD(week, DATEDIFF(week, 0, {col_left}), 0)"
        if g == "month":
            return f"DATEADD(month, DATEDIFF(month, 0, {col_left}), 0)"
        if g == "year":
            return f"DATEADD(year, DATEDIFF(year, 0, {col_left}), 0)"
        return col_left