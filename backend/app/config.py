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

    # Prefer backend/.env, fall back to repo-root .env so credentials shared
    # with the pipeline / publishing scripts work without duplication.
    _BACKEND_ENV = Path(__file__).resolve().parent.parent / ".env"
    _REPO_ENV = Path(__file__).resolve().parents[2] / ".env"
    if _BACKEND_ENV.exists():
        load_dotenv(_BACKEND_ENV)
    if _REPO_ENV.exists():
        load_dotenv(_REPO_ENV, override=False)
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
    # Chatbot-scoped catalog/schema. The repo-root .env may set DATABRICKS_CATALOG
    # / DATABRICKS_SCHEMA for the ingestion pipeline (e.g. 04_sdp / warcraftlogs);
    # the chatbot must read the governed gold layer instead, so we prefer
    # CHATBOT_-prefixed vars and only fall back to the generic ones.
    catalog = os.getenv("CHATBOT_DATABRICKS_CATALOG") or (
        "03_gold" if os.getenv("DATABRICKS_CATALOG") in {None, ""} else "03_gold"
    )
    schema = os.getenv("CHATBOT_DATABRICKS_SCHEMA") or (
        "sc_analytics" if os.getenv("DATABRICKS_SCHEMA") in {None, ""} else "sc_analytics"
    )
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        databricks_host=os.getenv("DATABRICKS_HOST", ""),
        databricks_token=os.getenv("DATABRICKS_TOKEN", ""),
        databricks_warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
        databricks_catalog=catalog,
        databricks_schema=schema,
        sql_row_limit=int(os.getenv("SQL_ROW_LIMIT", "500")),
        sql_timeout_seconds=int(os.getenv("SQL_TIMEOUT_SECONDS", "30")),
        semantic_registry_path=Path(
            os.getenv(
                "SEMANTIC_REGISTRY_PATH",
                str(Path(__file__).resolve().parent / "semantic_registry.json"),
            )
        ),
    )
