# Agentic Analytics Platform (Read-Only, Safe, Traceable)

This repository provides a production-ready **Agentic Analytics Platform** that:
- Connects to a SQL database in **STRICT READ-ONLY** mode
- **Introspects schema** and builds a local **Schema Registry** (no hallucinated tables/columns)
- Plans analytics, generates **SELECT-only** SQL (explicit columns; no `SELECT *`)
- Validates SQL with hard guardrails (blocks DDL/DML, obfuscation attempts, stacked statements)
- Executes safely and caches results to Parquet/DuckDB
- Produces business insights and generates a **single HTML dashboard** rendered inside Streamlit
- Persists **run traces** to disk with a **Run Trace Viewer** and **Compare Runs** diff UI

---

## Quickstart

### 1) Create venv & install
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
