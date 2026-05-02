"""Backend configuration.

All secrets and environment-specific values come from environment variables
(typically loaded from a local ``.env`` via ``python-dotenv``). Never hardcode
tokens. ``DATABRICKS_CATALOG`` and ``DATABRICKS_SCHEMA`` constrain the only
namespace the chatbot is ever allowed to query.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    openai_api_key: str
    openai_model: str
    databricks_host: str
    databricks_token: str
    databricks_warehouse_id: str
    databricks_catalog: str
    databricks_schema: str
    sql_row_limit: int
    sql_timeout_seconds: int
    semantic_registry_path: Path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        databricks_host=os.getenv("DATABRICKS_HOST", ""),
        databricks_token=os.getenv("DATABRICKS_TOKEN", ""),
        databricks_warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
        databricks_catalog=os.getenv("DATABRICKS_CATALOG", "03_gold"),
        databricks_schema=os.getenv("DATABRICKS_SCHEMA", "sc_analytics"),
        sql_row_limit=int(os.getenv("SQL_ROW_LIMIT", "500")),
        sql_timeout_seconds=int(os.getenv("SQL_TIMEOUT_SECONDS", "30")),
        semantic_registry_path=Path(
            os.getenv(
                "SEMANTIC_REGISTRY_PATH",
                str(Path(__file__).resolve().parent / "semantic_registry.json"),
            )
        ),
    )
