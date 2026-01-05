from __future__ import annotations

from config import settings
from guards.sql_safety import SQLSafetyGuard


def test_blocks_select_star():
    g = SQLSafetyGuard(settings)
    r = g.validate("SELECT * FROM dbo.Users")
    assert not r["ok"]
    assert any("SELECT *" in x for x in r["reasons"])
