"""Tests for the Raider.IO adapter."""

from __future__ import annotations

import httpx
import pytest

from ingestion.src.adapters.raiderio.client import RaiderIoAdapter, RaiderIoNotFoundError


def test_fetch_character_profile_returns_payload() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "name": "Televisions",
                "realm": "Twisting Nether",
                "profile_url": "https://raider.io/characters/eu/twisting-nether/Televisions",
                "mythic_plus_scores_by_season": [
                    {"season": "season-tww-3", "scores": {"all": 1234.5}},
                ],
            },
        )

    adapter = RaiderIoAdapter(region="EU")
    adapter.close()
    adapter._http = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://raider.io/api/v1"
    )  # noqa: SLF001

    result = adapter.fetch_character_profile(
        name="Televisions",
        realm_slug="twisting-nether",
        season="current",
    )

    assert result.source == "raiderio"
    assert result.total_records == 1
    assert result.records[0]["name"] == "Televisions"
    assert requests[0].url.params["region"] == "eu"
    assert requests[0].url.params["realm"] == "twisting-nether"
    assert requests[0].url.params["name"] == "Televisions"
    assert "mythic_plus_scores_by_season:current" in requests[0].url.params["fields"]

    adapter.close()


def test_fetch_character_profile_raises_not_found() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "not found"})

    adapter = RaiderIoAdapter()
    adapter.close()
    adapter._http = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://raider.io/api/v1"
    )  # noqa: SLF001

    with pytest.raises(RaiderIoNotFoundError):
        adapter.fetch_character_profile(name="Missing", realm_slug="twisting-nether")

    adapter.close()
