from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # App
    APP_ENV: str = "dev"
    APP_NAME: str = "Agentic Analytics Platform"
    DEV_MODE_DEFAULT: bool = True

    # LLM
    OLLAMA_BASE_URL: str = "http://127.0.0.1:11434"
    #OLLAMA_MODEL: str = "qwen2.5:7b"
    OLLAMA_MODEL: str ="deepseek-r1:8b"

    # DB
    DB_DIALECT: str = "mssql+pyodbc"
    DB_HOST: str = "DRWNWSQATSI12.amer.dell.com"
    DB_PORT: int = 1433
    DB_NAME: str = "DellRewardsStorage"
    DB_USERNAME: str = "DellRewardsUser"
    DB_PASSWORD: str = "@Reward123"
    ODBC_DRIVER: str = "ODBC Driver 18 for SQL Server"
    ODBC_EXTRA_PARAMS: str = "TrustServerCertificate=yes;Encrypt=no"

    # Safety & performance
    MAX_RETURNED_ROWS: int = 200000
    DEFAULT_EXPLORATORY_TOP: int = 10000
    FETCH_CHUNK_SIZE: int = 50000
    STATEMENT_TIMEOUT_SECONDS: int = 360000  # keep large if you want

    # Storage
    DATA_DIR: str = "./data"
    KNOWLEDGE_GRAPH_DIR: str = "./knowledge_graph_data"
    CACHE_DIR: str = "./cache_data"
    DUCKDB_PATH: str ="./cache_data/catalog.duckdb"
    TRACES_DIR: str = "./traces_data"
    LOG_DIR: str = "./logs"

    OFFLINE_ONLY: bool = False
    # Vendor
    PLOTLY_VENDOR_PATH: str = "./vendor/plotly-3.3.0.min.js"

    def ensure_dirs(self) -> None:
        for p in [
            self.DATA_DIR,
            self.KNOWLEDGE_GRAPH_DIR,
            self.CACHE_DIR,
            self.TRACES_DIR,
            self.LOG_DIR,
        ]:
            Path(p).mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.ensure_dirs()
