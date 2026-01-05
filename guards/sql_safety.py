from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, DML, Whitespace, Comment

from config import Settings


DISALLOWED_KEYWORDS = [
    "insert", "update", "delete", "merge", "drop", "alter", "truncate", "create",
    "exec", "execute", "call", "grant", "revoke",
]

COMMENT_PATTERNS = [
    r"--",
    r"/\*",
    r"\*/",
]

# also block statement stacking
STACKING_PATTERN = r";\s*\S"


class SQLSafetyGuard:
    """
    Defensive SQL validator:
    - single statement only
    - only SELECT / WITH...SELECT
    - no comments tokens at all (prevents obfuscation)
    - block DDL/DML keywords
    - no SELECT *
    - enforce TOP / OFFSET-FETCH / LIMIT like behavior for exploratory queries
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def validate(self, sql: str) -> Dict[str, Any]:
        raw = sql or ""
        s = raw.strip()

        report: Dict[str, Any] = {
            "ok": False,
            "reasons": [],
            "normalized_sql": None,
            "enforced_limit": None,
        }

        if not s:
            report["reasons"].append("Empty SQL.")
            return report

        # Hard block comment markers (avoid obfuscation & stacked tricks)
        for pat in COMMENT_PATTERNS:
            if re.search(pat, s):
                report["reasons"].append("SQL contains comment tokens; rejected.")
                return report

        # Parse & ensure single statement
        stmts = [st for st in sqlparse.parse(s) if str(st).strip()]
        if len(stmts) != 1:
            report["reasons"].append("Multiple statements detected; rejected.")
            return report

        stmt = stmts[0]

        # Block semicolon stacking (allow optional trailing semicolon only)
        if re.search(STACKING_PATTERN, s):
            report["reasons"].append("Statement stacking via semicolon detected; rejected.")
            return report

        # Must start with SELECT or WITH
        start = self._first_non_ws_token(stmt)
        if start is None:
            report["reasons"].append("No tokens found.")
            return report

        start_val = start.value.strip().lower()
        if not (start_val.startswith("select") or start_val.startswith("with")):
            report["reasons"].append("Only SELECT / WITH statements are allowed.")
            return report

        # Disallow disallowed keywords anywhere (case-insensitive, word-boundary)
        lowered = s.lower()
        for kw in DISALLOWED_KEYWORDS:
            if re.search(rf"\b{re.escape(kw)}\b", lowered):
                report["reasons"].append(f"Disallowed keyword detected: {kw}")
                return report

        # Disallow SELECT *
        if self._has_select_star(stmt):
            report["reasons"].append("SELECT * is not allowed; use explicit column lists.")
            return report

        normalized = sqlparse.format(s, keyword_case="upper", strip_comments=True, reindent=True)
        report["normalized_sql"] = normalized

        # Enforce limit if no explicit TOP/OFFSET-FETCH/LIMIT detected
        enforced = self._enforce_row_limit_if_missing(normalized)
        if enforced != normalized:
            report["enforced_limit"] = {"applied": True, "max_rows": self.settings.MAX_RETURNED_ROWS}
            report["normalized_sql"] = enforced
        else:
            report["enforced_limit"] = {"applied": False}

        report["ok"] = True
        return report

    def _first_non_ws_token(self, stmt: TokenList):
        for t in stmt.flatten():
            if t.ttype in (Whitespace,):
                continue
            if t.ttype in Comment:
                return None
            if t.value and t.value.strip():
                return t
        return None

    def _has_select_star(self, stmt: TokenList) -> bool:
        # Very defensive: block any "SELECT *" or "SELECT DISTINCT *" etc.
        txt = " ".join([t.value for t in stmt.flatten() if t.value])
        return bool(re.search(r"\bselect\b\s+(distinct\s+)?\*", txt, flags=re.IGNORECASE))

    def _enforce_row_limit_if_missing(self, sql: str) -> str:
        """
        For SQL Server, enforce TOP if missing.
        We avoid changing queries that already have TOP or OFFSET-FETCH.
        """
        max_rows = int(self.settings.MAX_RETURNED_ROWS)

        if max_rows <= 0:
            # Still required by safety policy, but config might be wrong. Keep enforced anyway.
            max_rows = 200000

        lowered = sql.lower()
        has_top = re.search(r"\bselect\b\s+top\s*\(", lowered) or re.search(r"\bselect\b\s+top\s+\d+", lowered)
        has_offset_fetch = "offset" in lowered and "fetch" in lowered
        has_limit = re.search(r"\blimit\b\s+\d+", lowered)

        if has_top or has_offset_fetch or has_limit:
            return sql

        # Insert TOP (max_rows) after SELECT or SELECT DISTINCT
        def repl(m: re.Match) -> str:
            sel = m.group(0)
            return f"{sel} TOP ({max_rows}) "

        # match "SELECT " or "SELECT DISTINCT "
        out = re.sub(r"\bSELECT\s+(DISTINCT\s+)?", repl, sql, flags=re.IGNORECASE, count=1)
        return out
