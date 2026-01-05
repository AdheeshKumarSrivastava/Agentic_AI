from __future__ import annotations

from typing import Optional, List, Dict, Any
from pathlib import Path
import duckdb
import pandas as pd

from config import Settings


class QueryCache:
    """
    Parquet snapshots keyed by sql_hash.
    Uses DuckDB for fast reads if needed.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.base = Path(settings.CACHE_DIR)
        self.base.mkdir(parents=True, exist_ok=True)

    def path_for(self, sql_hash: str) -> Path:
        return self.base / f"{sql_hash}.parquet"

    def get(self, sql_hash: str) -> Optional[pd.DataFrame]:
        p = self.path_for(sql_hash)
        if not p.exists():
            return None
        try:
            con = duckdb.connect(database=":memory:")
            df = con.execute(f"SELECT * FROM read_parquet('{p.as_posix()}')").df()
            con.close()
            return df
        except Exception:
            # fallback
            return pd.read_parquet(p)

    def put(self, sql_hash: str, df: pd.DataFrame) -> None:
        p = self.path_for(sql_hash)
        df.to_parquet(p, index=False)

    def list_entries(self) -> List[Dict[str, Any]]:
        out = []
        for p in sorted(self.base.glob("*.parquet")):
            out.append({"key": p.stem, "path": p.as_posix(), "size_bytes": p.stat().st_size})
        return out

    def clear(self, key: Optional[str] = None) -> int:
        removed = 0
        if key:
            p = self.path_for(key)
            if p.exists():
                p.unlink()
                removed += 1
            return removed
        for p in self.base.glob("*.parquet"):
            p.unlink()
            removed += 1
        return removed
