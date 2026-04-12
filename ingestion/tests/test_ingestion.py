"""Tests for the ingestion layer."""

import pytest

from ingestion.src.adapters.wcl.client import WarcraftLogsAdapter
from ingestion.src.utils.helpers import RateLimiter, add_ingestion_metadata


class TestWarcraftLogsAdapter:
    """Tests for WarcraftLogs adapter using mock HTTP responses."""

    @pytest.fixture
    def adapter(self, monkeypatch: pytest.MonkeyPatch) -> WarcraftLogsAdapter:
        monkeypatch.setenv("SOURCE_API_CLIENT_ID", "test_id")
        monkeypatch.setenv("SOURCE_API_CLIENT_SECRET", "test_secret")
        return WarcraftLogsAdapter()

    def test_get_source_name(self, adapter: WarcraftLogsAdapter) -> None:
        assert adapter.get_source_name() == "wcl"

    def test_rate_limit_config_has_required_keys(self, adapter: WarcraftLogsAdapter) -> None:
        config = adapter.get_rate_limit_config()
        assert "requests_per_minute" in config
        assert "requests_per_hour" in config
        assert config["requests_per_minute"] > 0

    def test_fetch_raises_without_auth(self, adapter: WarcraftLogsAdapter) -> None:
        with pytest.raises((RuntimeError, Exception), match="authenticate|RetryError"):
            adapter.fetch("test", {"query": "{ test }"})


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_allows_requests_within_limit(self) -> None:
        limiter = RateLimiter(requests_per_minute=10)
        # Should not raise or sleep for first 5 requests
        for _ in range(5):
            async with limiter:
                pass


class TestHelpers:
    def test_add_ingestion_metadata_adds_fields(self) -> None:
        record = {"id": 1, "name": "test"}
        result = add_ingestion_metadata(record, source="wcl")
        assert result["_source"] == "wcl"
        assert "_ingested_at" in result
        assert result["id"] == 1

    def test_add_ingestion_metadata_does_not_mutate_input(self) -> None:
        record = {"id": 1}
        add_ingestion_metadata(record, source="wcl")
        assert "_source" not in record
