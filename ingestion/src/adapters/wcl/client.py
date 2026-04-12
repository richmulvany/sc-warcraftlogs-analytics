"""WarcraftLogs v2 GraphQL API adapter."""

import os
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.src.adapters.base import AdapterConfig, BaseAdapter, FetchResult

log = structlog.get_logger(__name__)

# WarcraftLogs v2 uses OAuth2 client credentials
TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
API_URL = "https://www.warcraftlogs.com/api/v2/client"


class WarcraftLogsConfig(AdapterConfig):
    """Configuration for the WarcraftLogs adapter."""

    client_id: str
    client_secret: str
    base_url: str = API_URL


class WarcraftLogsAdapter(BaseAdapter):
    """
    Adapter for the WarcraftLogs v2 GraphQL API.

    Authentication:
        Requires SOURCE_API_CLIENT_ID and SOURCE_API_CLIENT_SECRET
        environment variables (or Databricks secret scope equivalents).

    Rate limits:
        WCL uses a point-based system. Each query costs points; the budget
        resets hourly. Conservative defaults are set below.
    """

    def __init__(self, config: WarcraftLogsConfig | None = None) -> None:
        if config is None:
            config = WarcraftLogsConfig(
                client_id=os.environ["SOURCE_API_CLIENT_ID"],
                client_secret=os.environ["SOURCE_API_CLIENT_SECRET"],
            )
        super().__init__(config)
        self.config: WarcraftLogsConfig = config
        self._access_token: str | None = None
        self._http: httpx.Client | None = None

    def authenticate(self) -> None:
        """Obtain an OAuth2 bearer token via client credentials flow."""
        with httpx.Client() as client:
            response = client.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config.client_id,
                    "client_secret": self.config.client_secret,
                },
            )
            response.raise_for_status()
            self._access_token = response.json()["access_token"]
            log.info("wcl.authenticated")

        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {self._access_token}"},
            timeout=30.0,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
    )
    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> FetchResult:
        """
        Execute a GraphQL query against the WCL API.

        Args:
            endpoint: Descriptive label for the query (e.g. 'guild_reports').
            params: Must contain 'query' (str) and optionally 'variables' (dict).

        Returns:
            FetchResult with records extracted from the GraphQL response.
        """
        if self._http is None:
            raise RuntimeError("Call authenticate() before fetch()")

        params = params or {}
        query = params.get("query", "")
        variables = params.get("variables", {})

        log.debug("wcl.query", endpoint=endpoint, variables=variables)

        response = self._http.post(
            API_URL,
            json={"query": query, "variables": variables},
        )
        response.raise_for_status()

        payload = response.json()
        if "errors" in payload:
            log.error("wcl.graphql_errors", errors=payload["errors"])
            raise ValueError(f"GraphQL errors: {payload['errors']}")

        data = payload.get("data", {})

        # Flatten nested GraphQL response into a list of records
        records = self._extract_records(data)

        return FetchResult(
            source="wcl",
            endpoint=endpoint,
            records=records,
            total_records=len(records),
            has_more=False,
        )

    def validate(self, result: FetchResult) -> bool:
        """Validate that we received at least one record."""
        if not result.records:
            log.warning("wcl.empty_result", endpoint=result.endpoint)
            return False
        return True

    def get_source_name(self) -> str:
        return "wcl"

    def get_rate_limit_config(self) -> dict[str, int]:
        return {
            "requests_per_minute": 30,
            "requests_per_hour": 300,
        }

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http:
            self._http.close()

    # ── Convenience query methods ─────────────────────────────────────────────

    def fetch_guild_reports(
        self, guild_name: str, server_slug: str, server_region: str
    ) -> FetchResult:
        """Fetch recent raid reports for a guild."""
        query = """
        query GuildReports($guildName: String!, $serverSlug: String!, $serverRegion: String!) {
          guildData {
            guild(name: $guildName, serverSlug: $serverSlug, serverRegion: $serverRegion) {
              id
              name
              server { slug region }
              attendance(limit: 16) {
                data {
                  code startTime title zone { name }
                }
              }
            }
          }
        }
        """
        return self.fetch(
            "guild_reports",
            {
                "query": query,
                "variables": {
                    "guildName": guild_name,
                    "serverSlug": server_slug,
                    "serverRegion": server_region,
                },
            },
        )

    def fetch_report_fights(self, report_code: str) -> FetchResult:
        """Fetch fight breakdown for a specific report."""
        query = """
        query ReportFights($code: String!) {
          reportData {
            report(code: $code) {
              code
              title
              startTime
              endTime
              fights(killType: Encounters) {
                id
                name
                kill
                startTime
                endTime
                difficulty
                fightPercentage
                bossPercentage
              }
            }
          }
        }
        """
        return self.fetch(
            "report_fights",
            {"query": query, "variables": {"code": report_code}},
        )

    @staticmethod
    def _extract_records(data: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten a nested GraphQL response into a flat list of records."""
        if not data:
            return []
        # Walk into the first nested value until we find a list
        current: Any = data
        while isinstance(current, dict):
            keys = list(current.keys())
            if not keys:
                return []
            current = current[keys[0]]
        if isinstance(current, list):
            return current
        return [data]
