"""Raider.IO character profile API adapter."""

from __future__ import annotations

import time
from typing import Any, cast

import httpx
import structlog

from ingestion.src.adapters.base import FetchResult

log = structlog.get_logger(__name__)

BASE_URL = "https://raider.io/api/v1"
DEFAULT_FIELDS = ",".join(
    [
        "mythic_plus_scores_by_season:current",
        "mythic_plus_ranks",
        "mythic_plus_recent_runs",
        "mythic_plus_best_runs",
    ]
)


class RaiderIoNotFoundError(Exception):
    """Raised when Raider.IO has no profile for a character."""


class RaiderIoTransientError(Exception):
    """Raised when Raider.IO keeps returning transient errors after retries."""


class RaiderIoAdapter:
    """Small unauthenticated adapter for Raider.IO's public character profile API."""

    def __init__(
        self,
        *,
        region: str = "eu",
        timeout_seconds: float = 30.0,
        request_sleep_seconds: float = 0.25,
    ) -> None:
        self.region = region.lower()
        self.request_sleep_seconds = request_sleep_seconds
        self._http = httpx.Client(
            base_url=BASE_URL,
            follow_redirects=True,
            timeout=timeout_seconds,
            headers={"User-Agent": "sc-warcraftlogs-analytics/1.0"},
        )

    def authenticate(self) -> None:
        """Raider.IO's public character profile endpoint does not require auth."""
        log.info("raiderio.authenticated", mode="public", region=self.region)

    def fetch_character_profile(
        self,
        *,
        name: str,
        realm_slug: str,
        season: str = "current",
        fields: str = DEFAULT_FIELDS,
    ) -> FetchResult:
        """Fetch one character profile from Raider.IO."""
        fields_for_season = fields.replace(":current", f":{season}")
        response = self._get_with_retry(
            "/characters/profile",
            {
                "region": self.region,
                "realm": realm_slug,
                "name": name,
                "fields": fields_for_season,
            },
        )
        if response.status_code in {400, 404}:
            raise RaiderIoNotFoundError(
                f"Raider.IO profile unavailable for {name}-{realm_slug}: HTTP {response.status_code}"
            )
        if response.status_code == 429 or response.status_code >= 500:
            raise RaiderIoTransientError(
                f"Raider.IO transient error for {name}-{realm_slug}: HTTP {response.status_code}"
            )
        response.raise_for_status()

        payload = cast(dict[str, Any], response.json())
        log.info("raiderio.character_profile", name=name, realm=realm_slug)
        return FetchResult(
            source="raiderio",
            endpoint="character_profile",
            records=[payload],
            total_records=1,
            has_more=False,
        )

    def _get_with_retry(self, path: str, params: dict[str, str]) -> httpx.Response:
        """Retry transient transport failures without losing static typing."""
        last_error: httpx.TimeoutException | httpx.TransportError | None = None
        last_response: httpx.Response | None = None
        for attempt in range(3):
            try:
                response = self._http.get(path, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    last_response = response
                    retry_after = float(response.headers.get("Retry-After") or 0)
                    time.sleep(max(retry_after, self.request_sleep_seconds, min(2**attempt, 10)))
                    continue
                return response
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt == 2:
                    break
                time.sleep(min(2**attempt, 10))

        if last_response is not None:
            return last_response
        if last_error is not None:
            raise last_error
        raise RuntimeError("Raider.IO request retry loop exited without a response.")

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()
