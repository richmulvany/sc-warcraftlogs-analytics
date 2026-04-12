"""WarcraftLogs v2 GraphQL API adapter."""

import json
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
        Fetch one page of guild raid reports via reportData.reports.

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
        including ``zone``, ``masterData.actors`` (player roster for this
        report), and a nested ``fights`` list (boss encounters only, filtered
        server-side via killType: Encounters).

        Fields added vs earlier version: ``encounterID``, ``size``,
        ``friendlyPlayers`` (actor IDs), ``zone {id, name}`` at report level.
        """
        query = """
        query ReportFights($code: String!) {
          reportData {
            report(code: $code) {
              code
              title
              startTime
              endTime
              zone { id name }
              masterData {
                actors(type: "Player") {
                  id
                  name
                  type
                  subType
                  server
                }
              }
              fights(killType: Encounters) {
                id
                name
                encounterID
                kill
                startTime
                endTime
                difficulty
                fightPercentage
                bossPercentage
                lastPhase
                size
                friendlyPlayers
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

    def fetch_player_details(self, report_code: str, fight_id: int) -> FetchResult:
        """
        Fetch per-player performance breakdown for a single kill fight.

        ``playerDetails`` is a JSON scalar in the WCL schema — the API returns
        a nested blob rather than a typed GraphQL object.  We serialise it to a
        string (``player_details_json``) for safe storage in the bronze JSONL
        file; the silver layer parses it with an explicit schema.

        Returns a FetchResult with a single record containing
        ``report_code``, ``fight_id``, and ``player_details_json``.
        """
        query = """
        query PlayerDetails($code: String!, $fightIDs: [Int]) {
          reportData {
            report(code: $code) {
              playerDetails(fightIDs: $fightIDs, includeCombatantInfo: true)
            }
          }
        }
        """
        data = self._graphql_query(query, {"code": report_code, "fightIDs": [fight_id]})
        raw_pd = data["reportData"]["report"]["playerDetails"]

        # Serialise to string — playerDetails is an opaque JSON scalar and its
        # nested structure is complex enough that Auto Loader schema inference
        # is unreliable.  Silver parses it with an explicit StructType.
        pd_json_str = json.dumps(raw_pd) if not isinstance(raw_pd, str) else raw_pd

        log.info("wcl.player_details", code=report_code, fight_id=fight_id)
        return FetchResult(
            source="wcl",
            endpoint="player_details",
            records=[
                {
                    "report_code": report_code,
                    "fight_id": fight_id,
                    "player_details_json": pd_json_str,
                }
            ],
            total_records=1,
            has_more=False,
        )

    def fetch_actor_roster(self, report_code: str) -> FetchResult:
        """
        Fetch the player actor roster for a report from masterData.

        Actor IDs are report-scoped and match the ``friendlyPlayers`` lists
        in fight objects.  ``subType`` on a Player actor is the WoW class name.

        Returns a FetchResult with a single record containing
        ``report_code`` and ``actors`` (list of actor dicts).
        """
        query = """
        query ActorRoster($code: String!) {
          reportData {
            report(code: $code) {
              masterData {
                actors(type: "Player") {
                  id
                  name
                  type
                  subType
                  server
                }
              }
            }
          }
        }
        """
        data = self._graphql_query(query, {"code": report_code})
        actors = data["reportData"]["report"]["masterData"]["actors"]
        log.info("wcl.actor_roster", code=report_code, actors=len(actors or []))
        return FetchResult(
            source="wcl",
            endpoint="actor_roster",
            records=[{"report_code": report_code, "actors": actors or []}],
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

        Each record contains ``code`` (report code), ``startTime`` (ms epoch),
        ``zone {id, name}``, and a nested ``players`` list with ``name``,
        ``presence`` (1=present, 2=benched, 3=absent), and ``type`` (class).
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
                  startTime
                  zone { id name }
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

    def fetch_zone_catalog(self) -> FetchResult:
        """
        Fetch the full WCL zone catalog from worldData.

        Returns all zones with their encounters and difficulty tiers.  Used to
        build ``silver_zone_catalog`` (a stable reference table) and to
        distinguish raid zones from M+ dungeon zones.

        This is a small query (dozens of zones) — no pagination needed.
        """
        query = """
        query ZoneCatalog {
          worldData {
            zones {
              id
              name
              frozen
              encounters { id name }
              difficulties { id name sizes }
            }
          }
        }
        """
        data = self._graphql_query(query)
        zones = data["worldData"]["zones"]
        log.info("wcl.zone_catalog", zones=len(zones or []))
        return FetchResult(
            source="wcl",
            endpoint="zone_catalog",
            records=zones or [],
            total_records=len(zones or []),
            has_more=False,
        )

    def fetch_report_rankings(self, report_code: str, fight_ids: list[int]) -> FetchResult:
        """
        Fetch WCL parse rankings for specific kill fights within a report.

        The ``rankings`` field is an opaque JSON scalar in the WCL schema.  We
        serialise it to a string (``rankings_json``) for safe storage in bronze;
        the silver layer parses it with an explicit schema.

        Args:
            report_code: WCL report code (e.g. "aAbBcC1234")
            fight_ids: List of fight IDs to include in the rankings query

        Returns:
            FetchResult with one record: {report_code, rankings_json}
        """
        query = """
        query ReportRankings($code: String!, $fightIDs: [Int]) {
          reportData {
            report(code: $code) {
              rankings(fightIDs: $fightIDs, compare: Parses, timeframe: Historical)
            }
          }
        }
        """
        data = self._graphql_query(query, {"code": report_code, "fightIDs": fight_ids})
        raw = data["reportData"]["report"]["rankings"]
        rankings_json = json.dumps(raw) if not isinstance(raw, str) else raw

        log.info("wcl.report_rankings", code=report_code, fight_count=len(fight_ids))
        return FetchResult(
            source="wcl",
            endpoint="report_rankings",
            records=[
                {
                    "report_code": report_code,
                    "rankings_json": rankings_json,
                }
            ],
            total_records=1,
            has_more=False,
        )

    def fetch_fight_deaths(self, report_code: str, fight_ids: list[int]) -> FetchResult:
        """
        Fetch death events for boss fights within a report via the table API.

        The ``table`` field is an opaque JSON scalar in the WCL schema.  We
        serialise it to a string (``table_json``) for safe storage in bronze;
        the silver layer parses it with an explicit schema.

        Args:
            report_code: WCL report code
            fight_ids: List of boss fight IDs (kills + wipes) to aggregate deaths for

        Returns:
            FetchResult with one record: {report_code, fight_ids, table_json}
        """
        query = """
        query FightDeaths($code: String!, $fightIDs: [Int]) {
          reportData {
            report(code: $code) {
              table(dataType: Deaths, fightIDs: $fightIDs)
            }
          }
        }
        """
        data = self._graphql_query(query, {"code": report_code, "fightIDs": fight_ids})
        raw = data["reportData"]["report"]["table"]
        table_json = json.dumps(raw) if not isinstance(raw, str) else raw

        log.info("wcl.fight_deaths", code=report_code, fight_count=len(fight_ids))
        return FetchResult(
            source="wcl",
            endpoint="fight_deaths",
            records=[
                {
                    "report_code": report_code,
                    "fight_ids": fight_ids,
                    "table_json": table_json,
                }
            ],
            total_records=1,
            has_more=False,
        )
