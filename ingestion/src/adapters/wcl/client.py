"""WarcraftLogs v2 GraphQL API adapter."""

import json
import time
from typing import Any

import httpx
import structlog

from ingestion.src.adapters.base import AdapterConfig, BaseAdapter, FetchResult

log = structlog.get_logger(__name__)

TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
API_URL = "https://www.warcraftlogs.com/api/v2/client"

# How many seconds before token expiry we proactively re-authenticate.
_TOKEN_REFRESH_BUFFER_SECS = 300  # 5 minutes


class ArchivedReportError(Exception):
    """Raised when WCL rejects a query because the report has been archived.

    WCL archives older reports for non-subscribing users.  These reports cannot
    be fetched and should be skipped permanently rather than retried.
    """


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

    Retry behaviour
    ---------------
    * **429 Too Many Requests** — waits for the ``Retry-After`` header value
      (default 60 s) and retries up to ``_MAX_429_ATTEMPTS`` times.  The WCL
      point budget resets on an hourly window so a single long sleep is usually
      enough.
    * **5xx Server Errors** — exponential back-off starting at 4 s, up to
      ``_MAX_5XX_ATTEMPTS`` times.
    * **Archived report GraphQL error** — raises ``ArchivedReportError``
      immediately (no retry); the caller should write a skip marker and move on.
    * **Other GraphQL errors** — raises ``ValueError`` immediately.

    Token refresh
    -------------
    ``authenticate()`` records the token expiry time.  ``_graphql_query``
    calls ``_maybe_refresh_token()`` before every request so long-running
    ingestion jobs never hit a stale token mid-run.
    """

    _MAX_429_ATTEMPTS = 5
    _MAX_5XX_ATTEMPTS = 3
    _DEFAULT_429_WAIT = 60  # seconds, used when Retry-After header is absent

    def __init__(self, config: WarcraftLogsConfig) -> None:
        super().__init__(config)
        self.config: WarcraftLogsConfig = config
        self._http: httpx.Client | None = None
        self._token_expiry: float = 0.0

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
        token_data = response.json()
        token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 3600))
        self._token_expiry = time.time() + expires_in - _TOKEN_REFRESH_BUFFER_SECS

        # Close any existing session before creating a new one.
        if self._http:
            self._http.close()
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )
        log.info("wcl.authenticated", expires_in=expires_in, refresh_at=self._token_expiry)

    def _maybe_refresh_token(self) -> None:
        """Re-authenticate if the token is within the refresh buffer window."""
        if self._token_expiry and time.time() >= self._token_expiry:
            log.info("wcl.token_refresh_triggered")
            self.authenticate()

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

    def _graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return the ``data`` payload.

        Handles 429s, 5xx errors, token refresh, and archived-report GraphQL
        errors without leaking retries to the caller.

        Raises
        ------
        ArchivedReportError
            When WCL signals that the report has been archived.
        ValueError
            On any other GraphQL-level error.
        httpx.HTTPStatusError
            On unrecoverable HTTP errors (4xx other than 429, or 5xx after
            all retry attempts are exhausted).
        RuntimeError
            If called before ``authenticate()``.
        """
        if self._http is None:
            raise RuntimeError("Call authenticate() before making API requests.")

        self._maybe_refresh_token()

        attempt_429 = 0
        attempt_5xx = 0

        while True:
            try:
                response = self._http.post(
                    API_URL,
                    json={"query": query, "variables": variables or {}},
                )
            except httpx.RequestError as exc:
                # Network-level error (timeout, connection reset, etc.)
                attempt_5xx += 1
                if attempt_5xx >= self._MAX_5XX_ATTEMPTS:
                    log.error("wcl.request_error_exhausted", error=str(exc))
                    raise
                wait = min(4 * (2 ** (attempt_5xx - 1)), 30)
                log.warning(
                    "wcl.request_error_retrying", attempt=attempt_5xx, wait=wait, error=str(exc)
                )
                time.sleep(wait)
                continue

            # ── 429 Too Many Requests ─────────────────────────────────────
            if response.status_code == 429:
                attempt_429 += 1
                retry_after = int(response.headers.get("Retry-After", self._DEFAULT_429_WAIT))
                log.warning(
                    "wcl.rate_limited",
                    attempt=attempt_429,
                    max_attempts=self._MAX_429_ATTEMPTS,
                    wait_seconds=retry_after,
                )
                if attempt_429 >= self._MAX_429_ATTEMPTS:
                    log.error("wcl.rate_limit_exhausted")
                    response.raise_for_status()
                time.sleep(retry_after)
                # Re-check token after a long sleep
                self._maybe_refresh_token()
                continue

            # ── 5xx Server Error ──────────────────────────────────────────
            if response.status_code >= 500:
                attempt_5xx += 1
                wait = min(4 * (2 ** (attempt_5xx - 1)), 30)
                log.warning(
                    "wcl.server_error",
                    status=response.status_code,
                    attempt=attempt_5xx,
                    wait=wait,
                )
                if attempt_5xx >= self._MAX_5XX_ATTEMPTS:
                    log.error("wcl.server_error_exhausted", status=response.status_code)
                    response.raise_for_status()
                time.sleep(wait)
                continue

            # ── All other HTTP errors (4xx except 429) ────────────────────
            response.raise_for_status()

            # ── GraphQL-level errors (HTTP 200 but errors in payload) ─────
            payload: dict[str, Any] = response.json()
            errors = payload.get("errors", [])
            if errors:
                messages = [e.get("message", "") for e in errors]
                if any("archived" in m.lower() for m in messages):
                    raise ArchivedReportError(
                        f"Report is archived and cannot be fetched: {messages[0]}"
                    )
                log.error("wcl.graphql_errors", errors=errors)
                raise ValueError(f"GraphQL errors: {errors}")

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

        Raises ArchivedReportError if the report has been archived.
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

        Raises ArchivedReportError if the report has been archived.
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

        Raises ArchivedReportError if the report has been archived.
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

        WCL truncates multi-fight Deaths table responses on long reports. To
        avoid silently dropping later pulls, fetch one fight at a time and
        persist one bronze record per fight.

        The ``table`` field is an opaque JSON scalar in the WCL schema. We
        serialise it to a string (``table_json``) for safe storage in bronze;
        the silver layer parses it with an explicit schema.

        Raises ArchivedReportError if the report has been archived.
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
        records: list[dict[str, Any]] = []
        for fight_id in fight_ids:
            data = self._graphql_query(query, {"code": report_code, "fightIDs": [fight_id]})
            raw = data["reportData"]["report"]["table"]
            table_json = json.dumps(raw) if not isinstance(raw, str) else raw
            records.append(
                {
                    "report_code": report_code,
                    "fight_ids": [fight_id],
                    "table_json": table_json,
                }
            )

        log.info(
            "wcl.fight_deaths",
            code=report_code,
            fight_count=len(fight_ids),
            record_count=len(records),
        )
        return FetchResult(
            source="wcl",
            endpoint="fight_deaths",
            records=records,
            total_records=len(records),
            has_more=False,
        )

    def _fetch_report_events(
        self,
        report_code: str,
        fight_id: int,
        data_type: str,
    ) -> list[dict[str, Any]]:
        query = """
        query FightEvents($code: String!, $fightIDs: [Int], $dataType: EventDataType, $startTime: Float) {
          reportData {
            report(code: $code) {
              events(
                dataType: $dataType
                fightIDs: $fightIDs
                startTime: $startTime
                limit: 10000
                useActorIDs: true
              ) {
                nextPageTimestamp
                data
              }
            }
          }
        }
        """

        events: list[dict[str, Any]] = []
        start_time: float | None = None

        while True:
            data = self._graphql_query(
                query,
                {
                    "code": report_code,
                    "fightIDs": [fight_id],
                    "dataType": data_type,
                    "startTime": start_time,
                },
            )
            page = data["reportData"]["report"]["events"]
            page_events = page.get("data") or []
            for event in page_events:
                event.setdefault("fight", fight_id)
            events.extend(page_events)

            next_page = page.get("nextPageTimestamp")
            if next_page is None or next_page == start_time:
                break
            start_time = next_page

        return events

    def fetch_fight_casts(self, report_code: str, fight_ids: list[int]) -> FetchResult:
        """
        Fetch player cast events for boss fights within a report.

        This is intentionally event-based rather than ``table(dataType: Casts)``:
        defensive and health-potion analysis needs fight/player/ability-level
        rows, and the table endpoint can truncate broad cast tables. The events
        paginator gives us the full cast stream for the selected boss pulls.

        Raises ArchivedReportError if the report has been archived.
        """
        events: list[dict[str, Any]] = []
        buff_events: list[dict[str, Any]] = []
        combatant_info_events: list[dict[str, Any]] = []
        for fight_id in fight_ids:
            events.extend(self._fetch_report_events(report_code, fight_id, "Casts"))
            buff_events.extend(self._fetch_report_events(report_code, fight_id, "Buffs"))
            combatant_info_events.extend(
                self._fetch_report_events(report_code, fight_id, "CombatantInfo")
            )

        casts_json = json.dumps({"data": events})
        buffs_json = json.dumps({"data": buff_events})
        combatant_info_json = json.dumps({"data": combatant_info_events})

        log.info(
            "wcl.fight_casts",
            code=report_code,
            fight_count=len(fight_ids),
            event_count=len(events),
            buff_event_count=len(buff_events),
            combatant_info_count=len(combatant_info_events),
        )
        return FetchResult(
            source="wcl",
            endpoint="fight_casts",
            records=[
                {
                    "report_code": report_code,
                    "fight_ids": fight_ids,
                    "events_json": casts_json,
                    "buffs_json": buffs_json,
                    "combatant_info_json": combatant_info_json,
                }
            ],
            total_records=1,
            has_more=False,
        )
