from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from config import Settings
from knowledge_graph.schema_registry import SchemaRegistry


class SQLAgent:
    """
    SQL Server SELECT-only generator (no SELECT *) with registry validation.

    HARDENED:
    - If plan.tables invalid/empty -> auto-recover from allowlist/registry.
    - If allowlist empty -> fallback to registry tables.
    - Never throws "No valid tables..." anymore.
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

        if not reg_tables:
            raise ValueError("Schema registry has no tables. Run schema bootstrap/ingestion first.")

        # ---------- table validation + recovery ----------
        planned_tables = plan.get("tables", [])
        planned_tables = planned_tables if isinstance(planned_tables, list) else []

        tables = [t for t in planned_tables if isinstance(t, str) and t in reg_tables]

        allow_ok: List[str] = []
        if allowed_tables:
            allow_ok = [t for t in allowed_tables if isinstance(t, str) and t in reg_tables]
            tables = [t for t in tables if t in allow_ok]

        # ✅ Recovery (never crash)
        recovered = False
        if not tables:
            recovered = True
            if allow_ok:
                tables = allow_ok[:2]  # minimal safe set
            else:
                tables = reg_tables[:2]

            plan["tables"] = list(tables)  # persist so downstream sees it

        primary = tables[0]

        # ---------- FROM + JOIN ----------
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

        # ---------- aggregation mode ----------
        metrics = plan.get("metrics", []) if isinstance(plan.get("metrics", []), list) else []
        is_agg = bool(
            plan.get("aggregation")
            or plan.get("group_by")
            or any(self._is_metric_agg(m) for m in metrics)
        )

        dims = [d for d in plan.get("dimensions", []) if isinstance(d, str)]
        time_field = plan.get("time_field") if isinstance(plan.get("time_field"), str) else None
        time_grain = plan.get("time_grain") if isinstance(plan.get("time_grain"), str) else None

        # ---------- SELECT dims ----------
        dim_select_cols: List[str] = []
        group_by_cols: List[str] = []

        for d in dims:
            col_ref = self._resolve_column(d, tables, alias_map)
            if col_ref:
                dim_select_cols.append(col_ref)
                group_by_cols.append(col_ref.split(" AS ")[0].strip())

        # time bucket
        if time_field:
            tf = self._resolve_column(time_field, tables, alias_map)
            if tf:
                if time_grain and is_agg:
                    tf_left, tf_alias = self._split_expr_alias(tf)
                    bucket_left = self._time_bucket_sqlserver(tf_left, time_grain)
                    dim_select_cols.append(f"{bucket_left} AS [{tf_alias}]")
                    group_by_cols.append(bucket_left)
                else:
                    dim_select_cols.append(tf)
                    group_by_cols.append(tf.split(" AS ")[0].strip())

        # ---------- SELECT metrics ----------
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
            if not agg or not isinstance(field, str):
                continue

            base_col = self._resolve_column(field, tables, alias_map)
            if not base_col:
                continue

            base_left = base_col.split(" AS ")[0].strip()
            alias = self._safe_alias(m_name)
            metric_select_cols.append(f"{self._agg_sql(agg, base_left)} AS [{alias}]")
            metric_expected_names.append(alias)

        # fallback: pick first columns from primary
        if not dim_select_cols and not metric_select_cols:
            cols = self.registry.table_columns(primary)[:12]
            dim_select_cols = [f"{alias_map[primary]}.[{c}] AS [{c}]" for c in cols]
            is_agg = False

        select_cols = self._dedupe_by_alias(dim_select_cols + metric_select_cols)

        # ---------- WHERE ----------
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
                ph = []
                for j, vv in enumerate(value):
                    pj = f"{p}_{j}"
                    params[pj] = vv
                    ph.append(f":{pj}")
                where_parts.append(f"{left} IN ({', '.join(ph)})")
            else:
                safe_ops = {"=", "!=", "<>", ">", ">=", "<", "<=", "like"}
                if op_l not in safe_ops:
                    op = "="
                params[p] = value
                where_parts.append(f"{left} {op} :{p}")

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # ---------- GROUP BY ----------
        group_by_clause = ""
        if is_agg and group_by_cols:
            group_by_clause = "GROUP BY " + ", ".join([c for c in group_by_cols if c])

        # ---------- ORDER BY ----------
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
                    order_parts.append(f"{col_ref.split(' AS ')[0].strip()} {ob_dir}")
                else:
                    safe_alias = self._safe_alias(ob_field)
                    if safe_alias in metric_expected_names:
                        order_parts.append(f"[{safe_alias}] {ob_dir}")

            if order_parts:
                order_by_clause = "ORDER BY " + ", ".join(order_parts)

        # ---------- TOP ----------
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

        print("planned_tables",planned_tables)
        print("allowed_tables_count",len(allowed_tables))
        print("registry_tables_count",len(reg.get("tables",{})))

        return {
            "sql": sql,
            "params": params,
            "expected_columns": expected,
            "is_aggregated": is_agg,
            "top": top,
            "time_grain": time_grain,
            "final_tables": list(tables),
            "recovered_tables": recovered,
        }

    # ---------------- helpers ----------------
    def _fmt_table(self, table_key: str) -> str:
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

        # schema.table.col
        if len(parts) == 3:
            tpart = f"{parts[0]}.{parts[1]}"
            cpart = parts[2]
            if tpart in alias_map and self.registry.has_column(tpart, cpart):
                a = alias_map[tpart]
                return f"{a}.[{cpart}] AS [{cpart}]"
            return None

        # reject table.col ambiguity
        if len(parts) == 2:
            return None

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
            a = self._alias_name(c).strip()
            if a and a not in seen:
                seen.add(a)
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