"""
Base adapter interface for all data source adapters.

To add a new data source:
  1. Create a new directory under adapters/
  2. Implement BaseAdapter in an adapter.py file
  3. See adapters/example_adapter/ for a complete example
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class AdapterConfig(BaseModel):
    """Base configuration model — extend with source-specific fields."""

    base_url: str
    timeout_seconds: int = 30
    max_retries: int = 3


class FetchResult(BaseModel):
    """Standard result returned by all adapters."""

    source: str
    endpoint: str
    records: list[dict[str, Any]]
    total_records: int
    page: int = 1
    has_more: bool = False
    metadata: dict[str, Any] = {}


class BaseAdapter(ABC):
    """
    Abstract base class for all data source adapters.

    All adapters must implement authenticate(), fetch(), and validate().
    The pipeline calls these methods in order for each ingestion run.
    """

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config

    @abstractmethod
    def authenticate(self) -> None:
        """
        Authenticate with the data source.
        Called once before any fetch() calls.
        Store credentials on self for use in fetch().
        """
        ...

    @abstractmethod
    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> FetchResult:
        """
        Fetch data from an endpoint.

        Args:
            endpoint: The logical endpoint name (e.g. 'reports', 'rankings')
            params: Optional query parameters / pagination state

        Returns:
            FetchResult with records and pagination metadata
        """
        ...

    @abstractmethod
    def validate(self, result: FetchResult) -> bool:
        """
        Validate a FetchResult before it is written to Bronze.

        Args:
            result: The result to validate

        Returns:
            True if valid, False to skip writing (warning is logged)
        """
        ...
