"""Tests for the example adapter."""

from unittest.mock import MagicMock

import pytest

from ingestion.src.adapters.base import FetchResult
from ingestion.src.adapters.example_adapter.adapter import ExampleAdapter, ExampleAdapterConfig


@pytest.fixture
def config() -> ExampleAdapterConfig:
    return ExampleAdapterConfig(
        base_url="https://test-api.example.com",
        api_key="test-key-123",
    )


@pytest.fixture
def adapter(config: ExampleAdapterConfig) -> ExampleAdapter:
    a = ExampleAdapter(config)
    a.authenticate()
    return a


def test_fetch_returns_records(adapter: ExampleAdapter) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [{"id": 1, "name": "Test"}],
        "total": 1,
        "has_more": False,
    }
    mock_response.raise_for_status = MagicMock()
    adapter._client.get = MagicMock(return_value=mock_response)  # type: ignore[union-attr]

    result = adapter.fetch("entities")
    assert isinstance(result, FetchResult)
    assert len(result.records) == 1
    assert result.records[0]["id"] == 1


def test_fetch_handles_pagination(adapter: ExampleAdapter) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [{"id": 1}],
        "total": 2,
        "page": 1,
        "has_more": True,
    }
    mock_response.raise_for_status = MagicMock()
    adapter._client.get = MagicMock(return_value=mock_response)  # type: ignore[union-attr]

    result = adapter.fetch("entities", params={"page": 1})
    assert result.has_more is True


def test_validate_empty_result_returns_false(adapter: ExampleAdapter) -> None:
    result = FetchResult(source="example", endpoint="entities", records=[], total_records=0)
    assert adapter.validate(result) is False


def test_validate_non_empty_result_returns_true(adapter: ExampleAdapter) -> None:
    result = FetchResult(
        source="example",
        endpoint="entities",
        records=[{"id": 1}],
        total_records=1,
    )
    assert adapter.validate(result) is True
