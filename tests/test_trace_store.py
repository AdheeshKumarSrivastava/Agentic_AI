from __future__ import annotations

import tempfile
from traces.trace_store import TraceStore


def test_trace_persist_and_load():
    with tempfile.TemporaryDirectory() as d:
        ts = TraceStore(d)
        run_id = ts.new_run()
        ts.add_node(run_id, "A_intent", {"kpis": ["x"]})
        doc = ts.load(run_id)
        assert doc["run_id"] == run_id
        assert "A_intent" in doc["nodes"]
