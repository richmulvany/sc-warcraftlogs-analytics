"""
Example adapter — demonstrates the full adapter pattern.

Copy this directory and replace the implementation to add a new data source.
"""

import logging
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.src.adapters.base import AdapterConfig, BaseAdapter, FetchResult

logger = logging.getLogger(__name__)


class ExampleAdapterConfig(AdapterConfig):
    """Configuration for the example adapter."""

    api_key: str
    api_version: str = "v1"


class ExampleAdapter(BaseAdapter):
    """
    Example implementation of BaseAdapter.

    Replace the HTTP calls below with your actual API client.
    """

    def __init__(self, config: ExampleAdapterConfig) -> None:
        super().__init__(config)
        self.config: ExampleAdapterConfig = config
        self._client: httpx.Client | None = None

    def authenticate(self) -> None:
        """Create an authenticated HTTP client."""
        self._client = httpx.Client(
            base_url=self.config.base_url,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Accept": "application/json",
            },
            timeout=self.config.timeout_seconds,
        )
        logger.info("Authenticated with example API")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> FetchResult:
        """Fetch data from the example API with automatic retry."""
        if self._client is None:
            raise RuntimeError("authenticate() must be called before fetch()")

        response = self._client.get(f"/{endpoint}", params=params or {})
        response.raise_for_status()
        data = response.json()

        records = data.get("data", [])
        logger.info("Fetched %d records from endpoint '%s'", len(records), endpoint)

        return FetchResult(
            source="example",
            endpoint=endpoint,
            records=records,
            total_records=data.get("total", len(records)),
            page=data.get("page", 1),
            has_more=data.get("has_more", False),
        )

    def validate(self, result: FetchResult) -> bool:
        """Validate that we received at least one record."""
        if not result.records:
            logger.warning("No records returned from endpoint '%s'", result.endpoint)
            return False
        return True
