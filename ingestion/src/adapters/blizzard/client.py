"""Blizzard Game Data API adapter for guild roster."""

from typing import Any

import httpx
import structlog

from ingestion.src.adapters.base import FetchResult

log = structlog.get_logger(__name__)

OAUTH_URL = "https://oauth.battle.net/token"


class BlizzardAdapter:
    """
    Adapter for the Blizzard Game Data API.

    Authentication uses OAuth2 client credentials flow with HTTP basic auth
    (client_id:client_secret).  The token is regional — pass the region that
    matches your guild's realm (e.g. "eu", "us").

    This adapter does NOT extend BaseAdapter because the Blizzard API uses a
    different authentication pattern (basic auth + regional endpoints) rather
    than the WCL GraphQL pattern.
    """

    def __init__(self) -> None:
        self._http: httpx.Client | None = None
        self._region: str = "eu"

    def authenticate(self, client_id: str, client_secret: str, region: str = "eu") -> None:
        """
        Obtain an OAuth2 bearer token via client credentials with HTTP basic auth.

        Args:
            client_id: Blizzard API client ID from developer.battle.net
            client_secret: Blizzard API client secret
            region: API region — "eu", "us", "kr", or "tw" (default "eu")
        """
        self._region = region.lower()
        response = httpx.post(
            OAUTH_URL,
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )
        log.info("blizzard.authenticated", region=self._region)

    def fetch_guild_roster(
        self,
        realm_slug: str,
        guild_slug: str,
        locale: str = "en_GB",
    ) -> FetchResult:
        """
        Fetch the guild roster from the Blizzard Profile API.

        Args:
            realm_slug: Lowercase hyphenated realm name (e.g. "twisting-nether")
            guild_slug: Lowercase hyphenated guild name (e.g. "student-council")
            locale: Response locale (default "en_GB")

        Returns:
            FetchResult whose records is the members array, each flattened to:
            {name, realm_slug, rank, class_id, class_name, level}
        """
        if self._http is None:
            raise RuntimeError("Call authenticate() before making API requests.")

        url = (
            f"https://{self._region}.api.blizzard.com"
            f"/data/wow/guild/{realm_slug}/{guild_slug}/roster"
        )
        response = self._http.get(
            url,
            params={
                "namespace": f"profile-{self._region}",
                "locale": locale,
            },
        )
        response.raise_for_status()

        raw: dict[str, Any] = response.json()
        members_raw: list[dict[str, Any]] = raw.get("members", [])

        records: list[dict[str, Any]] = []
        for entry in members_raw:
            character = entry.get("character", {})
            playable_class = character.get("playable_class", {})
            realm_info = character.get("realm", {})
            records.append(
                {
                    "name": character.get("name"),
                    "realm_slug": realm_info.get("slug"),
                    "rank": entry.get("rank"),
                    "class_id": playable_class.get("id"),
                    "class_name": playable_class.get("name"),
                    "level": character.get("level"),
                }
            )

        log.info("blizzard.guild_roster", realm=realm_slug, guild=guild_slug, members=len(records))
        return FetchResult(
            source="blizzard",
            endpoint="guild_roster",
            records=records,
            total_records=len(records),
            has_more=False,
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http:
            self._http.close()
            self._http = None
