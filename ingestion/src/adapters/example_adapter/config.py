"""Load config for the example adapter from environment / secret scope."""

import os

from ingestion.src.adapters.example_adapter.adapter import ExampleAdapterConfig


def load_config() -> ExampleAdapterConfig:
    """
    Load adapter config from environment variables.
    In Databricks, use dbutils.secrets instead of os.environ for secrets.
    """
    return ExampleAdapterConfig(
        base_url=os.environ["SOURCE_API_BASE_URL"],
        api_key=os.environ["SOURCE_API_KEY"],
    )
