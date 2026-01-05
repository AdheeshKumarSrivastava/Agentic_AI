from __future__ import annotations

from config import settings
from guards.sql_safety import SQLSafetyGuard


def test_blocks_dml():
    g = SQLSafetyGuard(settings)
    r = g.validate("UPDATE users SET a=1")
    assert not r["ok"]
    assert any("Disallowed keyword" in x for x in r["reasons"])


def test_single_statement_only():
    g = SQLSafetyGuard(settings)
    r = g.validate("SELECT 1; SELECT 2")
    assert not r["ok"]
