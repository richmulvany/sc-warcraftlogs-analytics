"""WarcraftLogs v2 GraphQL API adapter."""

from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.src.adapters.base import AdapterConfig, BaseAdapter, FetchResult

log = structlog.get_logger(__name__)

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

    Authentication uses OAuth2 client credentials flow.  Credentials are
    passed in via WarcraftLogsConfig (typically loaded from a Databricks
    Secret Scope in the ingestion job).

    Rate limits: WCL uses a point budget that resets hourly.  The retry
    decorator on _graphql_query handles transient 429/5xx errors.
    """

    def __init__(self, config: WarcraftLogsConfig) -> None:
        super().__init__(config)
        self.config: WarcraftLogsConfig = config
        self._http: httpx.Client | None = None

    def authenticate(self) -> None:
        """Obtain an OAuth2 bearer token via client credentials flow."""
        response = httpx.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )
        log.info("wcl.authenticated")

    def close(self) -> None:
        if self._http:
            self._http.close()
            self._http = None

    # ── BaseAdapter contract ──────────────────────────────────────────────────

    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> FetchResult:
        """Generic GraphQL fetch.  params must contain 'query' and optionally 'variables'."""
        params = params or {}
        data = self._graphql_query(params.get("query", ""), params.get("variables", {}))
        return FetchResult(source="wcl", endpoint=endpoint, records=[data], total_records=1)

    def validate(self, result: FetchResult) -> bool:
        if not result.records:
            log.warning("wcl.empty_result", endpoint=result.endpoint)
            return False
        return True

    def get_source_name(self) -> str:
        return "wcl"

    def get_rate_limit_config(self) -> dict[str, int]:
        return {"requests_per_minute": 30, "requests_per_hour": 300}

    # ── Internal GraphQL execution ────────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30))  # type: ignore[misc]
    def _graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return the ``data`` payload."""
        if self._http is None:
            raise RuntimeError("Call authenticate() before making API requests.")

        response = self._http.post(
            API_URL,
            json={"query": query, "variables": variables or {}},
        )
        response.raise_for_status()

        payload: dict[str, Any] = response.json()
        if "errors" in payload:
            log.error("wcl.graphql_errors", errors=payload["errors"])
            raise ValueError(f"GraphQL errors: {payload['errors']}")

        data: dict[str, Any] = payload.get("data", {})
        return data

    # ── Public query methods ──────────────────────────────────────────────────

    def fetch_guild_reports(
        self,
        guild_name: str,
        server_slug: str,
        server_region: str,
        page: int = 1,
    ) -> FetchResult:
        """
        Fetch one page of guild raid reports.

        Uses reportData.reports with guild filter arguments — the Guild type
        does not expose a reports field directly.

        Returns a FetchResult whose ``records`` is a list of report dicts, each
        containing ``code``, ``title``, ``startTime``, ``endTime``, and
        ``zone {id, name}``.
        """
        query = """
        query GuildReports(
          $guildName: String!
          $guildServerSlug: String!
          $guildServerRegion: String!
          $page: Int
        ) {
          reportData {
            reports(
              guildName: $guildName
              guildServerSlug: $guildServerSlug
              guildServerRegion: $guildServerRegion
              limit: 25
              page: $page
            ) {
              data {
                code
                title
                startTime
                endTime
                zone { id name }
              }
              has_more_pages
            }
          }
        }
        """
        data = self._graphql_query(
            query,
            {
                "guildName": guild_name,
                "guildServerSlug": server_slug,
                "guildServerRegion": server_region,
                "page": page,
            },
        )
        reports_page = data["reportData"]["reports"]
        records = reports_page.get("data", [])
        log.info("wcl.guild_reports", page=page, count=len(records))
        return FetchResult(
            source="wcl",
            endpoint="guild_reports",
            records=records,
            total_records=len(records),
            page=page,
            has_more=reports_page.get("has_more_pages", False),
        )

    def fetch_report_fights(self, report_code: str) -> FetchResult:
        """
        Fetch boss fight breakdown for a specific report.

        Returns a FetchResult with a single record: the full report object
        including a nested ``fights`` list (boss encounters only).
        """
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
                lastPhase
              }
            }
          }
        }
        """
        data = self._graphql_query(query, {"code": report_code})
        report = data["reportData"]["report"]
        log.info(
            "wcl.report_fights",
            code=report_code,
            fights=len(report.get("fights") or []),
        )
        return FetchResult(
            source="wcl",
            endpoint="report_fights",
            records=[report],
            total_records=1,
            has_more=False,
        )

    def fetch_raid_attendance(
        self,
        guild_name: str,
        server_slug: str,
        server_region: str,
        page: int = 1,
    ) -> FetchResult:
        """
        Fetch one page of raid attendance records.

        Returns a FetchResult whose ``records`` is a list of dicts, each
        containing ``code`` (report code) and a nested ``players`` list with
        ``name``, ``presence`` (1=present, 2=benched, 3=absent), and ``type``
        (class name).
        """
        query = """
        query GuildAttendance(
          $guildName: String!
          $serverSlug: String!
          $serverRegion: String!
          $page: Int
        ) {
          guildData {
            guild(name: $guildName, serverSlug: $serverSlug, serverRegion: $serverRegion) {
              attendance(limit: 25, page: $page) {
                data {
                  code
                  players {
                    name
                    presence
                    type
                  }
                }
                has_more_pages
              }
            }
          }
        }
        """
        data = self._graphql_query(
            query,
            {
                "guildName": guild_name,
                "serverSlug": server_slug,
                "serverRegion": server_region,
                "page": page,
            },
        )
        att_page = data["guildData"]["guild"]["attendance"]
        records = att_page.get("data", [])
        log.info("wcl.raid_attendance", page=page, count=len(records))
        return FetchResult(
            source="wcl",
            endpoint="raid_attendance",
            records=records,
            total_records=len(records),
            page=page,
            has_more=att_page.get("has_more_pages", False),
        )
