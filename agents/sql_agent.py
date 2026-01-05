from __future__ import annotations

from typing import Any, Dict, List, Tuple
import re

from config import Settings
from knowledge_graph.schema_registry import SchemaRegistry


class SQLAgent:
    """
    Generates SELECT-only SQL Server queries with explicit columns.
    Uses SchemaRegistry to ensure no hallucinated columns/tables.
    """

    def __init__(self, settings: Settings, registry: SchemaRegistry):
        self.settings = settings
        self.registry = registry

    def generate_sql(self, plan: Dict[str, Any], allowed_tables: List[str]) -> Dict[str, Any]:
        reg = self.registry.load()
        tables = [t for t in plan.get("tables", []) if t in reg.get("tables", {})]
        if allowed_tables:
            tables = [t for t in tables if t in allowed_tables]
        if not tables:
            raise ValueError("No valid tables in plan after validation.")

        # Choose a "primary" table as first.
        primary = tables[0]
        joins = plan.get("joins", []) if isinstance(plan.get("joins", []), list) else []

        # Build FROM and JOIN clauses, validating join columns
        from_clause = f"FROM {self._fmt_table(primary)} AS t0"
        alias_map = {primary: "t0"}
        join_clauses = []
        alias_i = 1

        for j in joins:
            try:
                lt = j["left_table"]
                rt = j["right_table"]
                lk = j["left_key"]
                rk = j["right_key"]
                jt = (j.get("join_type") or "LEFT").upper()
            except Exception:
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

            join_clauses.append(
                f"{jt} JOIN {self._fmt_table(rt)} AS {alias_map[rt]} "
                f"ON {alias_map[lt]}.[{lk}] = {alias_map[rt]}.[{rk}]"
            )

        # Select columns: dimensions + metric deps + time field; fallback to a safe subset
        select_cols: List[str] = []
        dims = [d for d in plan.get("dimensions", []) if isinstance(d, str)]
        time_field = plan.get("time_field") if isinstance(plan.get("time_field"), str) else None

        # Attempt to map dimension strings to actual columns (best effort)
        for d in dims:
            col_ref = self._resolve_column(d, tables, alias_map)
            if col_ref:
                select_cols.append(col_ref)

        if time_field:
            tf = self._resolve_column(time_field, tables, alias_map)
            if tf and tf not in select_cols:
                select_cols.append(tf)

        # Metric dependencies (columns) if provided
        for m in plan.get("metrics", []):
            if not isinstance(m, dict):
                continue
            dep = m.get("depends_on")
            if isinstance(dep, list):
                for c in dep:
                    col_ref = self._resolve_column(str(c), tables, alias_map)
                    if col_ref and col_ref not in select_cols:
                        select_cols.append(col_ref)

        # If still empty, pick first N columns from primary
        if not select_cols:
            cols = self.registry.table_columns(primary)[: min(12, len(self.registry.table_columns(primary)))]
            select_cols = [f"{alias_map[primary]}.[{c}] AS [{c}]" for c in cols]

        # Ensure explicit cols and stable aliases
        # Remove duplicates by alias name
        seen = set()
        cleaned = []
        for c in select_cols:
            alias = c.split(" AS ")[-1].strip() if " AS " in c else c
            if alias not in seen:
                seen.add(alias)
                cleaned.append(c)
        select_cols = cleaned

        # WHERE filters - parameterized placeholders
        params: Dict[str, Any] = {}
        where = []
        for idx, f in enumerate(plan.get("filters", []) if isinstance(plan.get("filters", []), list) else []):
            if not isinstance(f, dict):
                continue
            field = str(f.get("field", "")).strip()
            op = str(f.get("op", "=")).strip()
            value = f.get("value")
            col_ref = self._resolve_column(field, tables, alias_map)
            if not col_ref:
                continue

            # col_ref is like "t0.[Col] AS [Col]" -> extract left part before AS
            left = col_ref.split(" AS ")[0].strip()
            p = f"p{idx}"
            if op.lower() in ("in",):
                # expect list
                if not isinstance(value, list) or not value:
                    continue
                placeholders = []
                for j, vv in enumerate(value):
                    pj = f"{p}_{j}"
                    params[pj] = vv
                    placeholders.append(f":{pj}")
                where.append(f"{left} IN ({', '.join(placeholders)})")
            else:
                params[p] = value
                # only allow simple ops
                safe_ops = {"=", "!=", "<>", ">", ">=", "<", "<=", "like"}
                if op.lower() not in safe_ops:
                    op = "="
                where.append(f"{left} {op} :{p}")

        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        # Add TOP for exploratory if not aggregated; safety guard can enforce too
        top = int(self.settings.DEFAULT_EXPLORATORY_TOP)
        select_prefix = f"SELECT TOP ({top}) "
        select_list = ",\n  ".join(select_cols)

        sql = "\n".join([
            f"{select_prefix}\n  {select_list}",
            from_clause,
            *join_clauses,
            where_clause,
        ]).strip()

        # Provide expected_columns (explicit)
        plan["expected_columns"] = [self._alias_name(c) for c in select_cols]
        return {"sql": sql, "params": params, "expected_columns": plan["expected_columns"]}

    def _fmt_table(self, table_key: str) -> str:
        # table_key like schema.table
        schema, table = table_key.split(".", 1)
        return f"[{schema}].[{table}]"

    def _resolve_column(self, hint: str, tables: List[str], alias_map: Dict[str, str]) -> str | None:
        """
        Resolve a user-friendly field name to an actual column:
        - if hint contains ".", treat as table_key.col
        - else search in candidate tables by column name match (case-insensitive)
        """
        hint = (hint or "").strip()
        if not hint:
            return None

        if "." in hint:
            tpart, cpart = hint.split(".", 1)
            # tpart might be schema.table or table alias-ish; we only accept schema.table
            if tpart in alias_map and self.registry.has_column(tpart, cpart):
                a = alias_map[tpart]
                return f"{a}.[{cpart}] AS [{cpart}]"
            return None

        # match by column name in any table
        for t in tables:
            cols = self.registry.table_columns(t)
            for c in cols:
                if c.lower() == hint.lower():
                    a = alias_map.get(t, "t0")
                    return f"{a}.[{c}] AS [{c}]"
        return None

    def _alias_name(self, col_expr: str) -> str:
        m = re.search(r"\s+AS\s+\[(.+?)\]\s*$", col_expr, flags=re.IGNORECASE)
        if m:
            return m.group(1)
        return col_expr
