# Databricks notebook source
# WarcraftLogs ingestion job — fetches guild reports, fight details, player
# performance, actor rosters, raid attendance, and the zone catalog from the
# WCL v2 GraphQL API and lands them as JSONL files in a Unity Catalog Volume
# for the DLT pipeline to pick up via Auto Loader.
#
# Third-party libraries are installed at notebook startup via %pip below.
# Free Edition serverless does not support the job-level environments/client spec.

# COMMAND ----------
%pip install structlog httpx tenacity
dbutils.library.restartPython()

# COMMAND ----------
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime

# Derive bundle root from notebook workspace path — __file__ is undefined in
# Databricks notebooks.  The notebook lives at:
#   .../files/ingestion/jobs/ingest_primary
# so stripping the last 3 segments gives the bundle "files/" root.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb_path = _ctx.notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from ingestion.src.adapters.wcl.client import ArchivedReportError, WarcraftLogsAdapter, WarcraftLogsConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# ── Configuration ─────────────────────────────────────────────────────────────
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "04_sdp"  # noqa: F821
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "warcraftlogs"  # noqa: F821
guild_name = (
    dbutils.widgets.get("guild_name") if dbutils.widgets.get("guild_name") else "Student Council"  # noqa: F821
)
server_slug = (
    dbutils.widgets.get("guild_server_slug")  # noqa: F821
    if dbutils.widgets.get("guild_server_slug")  # noqa: F821
    else "twisting-nether"
)
server_region = (
    dbutils.widgets.get("guild_server_region")  # noqa: F821
    if dbutils.widgets.get("guild_server_region")  # noqa: F821
    else "EU"
)

logger.info(
    "Ingesting guild=%s server=%s region=%s → %s.%s",
    guild_name, server_slug, server_region, catalog, schema,
)

# COMMAND ----------
# ── Authentication ────────────────────────────────────────────────────────────
client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")  # noqa: F821
client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")  # noqa: F821

adapter = WarcraftLogsAdapter(WarcraftLogsConfig(client_id=client_id, client_secret=client_secret))
adapter.authenticate()

# COMMAND ----------
# ── Volume setup ──────────────────────────────────────────────────────────────
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.landing")  # noqa: F821

landing = f"/Volumes/{catalog}/{schema}/landing"
for subdir in (
    "guild_reports",
    "report_fights",
    "raid_attendance",
    "actor_roster",
    "player_details",
    "zone_catalog",
    "guild_members",
    "fight_rankings",
    "fight_deaths",
    "archived",       # skip-marker directory — one empty file per archived report code
):
    os.makedirs(f"{landing}/{subdir}", exist_ok=True)


def _is_archived(report_code: str) -> bool:
    """Return True if this report has a permanent archived skip marker."""
    return os.path.exists(f"{landing}/archived/{report_code}")


def _mark_archived(report_code: str) -> None:
    """Write an empty skip marker so future runs bypass this report immediately."""
    open(f"{landing}/archived/{report_code}", "w").close()
    logger.warning("report_archived_marked: %s — will skip permanently", report_code)

run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
ingested_at = datetime.now(UTC).isoformat()

# Helper: throttle between API calls to stay within 30 req/min
def _sleep() -> None:
    time.sleep(2)

# COMMAND ----------
# ── 1. Zone Catalog ───────────────────────────────────────────────────────────
# Fetch once per run — zones rarely change but always refresh so new raid tiers
# appear automatically.
logger.info("Fetching zone catalog …")
zone_result = adapter.fetch_zone_catalog()
with open(f"{landing}/zone_catalog/{run_ts}_zones.jsonl", "w") as fh:
    for zone in zone_result.records:
        fh.write(json.dumps({**zone, "_source": "wcl", "_ingested_at": ingested_at}) + "\n")
logger.info("zone_catalog → %d zones", zone_result.total_records)
_sleep()

# COMMAND ----------
# ── 2. Guild Reports ──────────────────────────────────────────────────────────
# Fetch all pages and write each as a JSONL file keyed by run timestamp + page.
# The silver layer deduplicates on report code, so re-runs are safe.
logger.info("Fetching guild reports …")
page, has_more = 1, True
all_report_codes: list[str] = []

while has_more:
    result = adapter.fetch_guild_reports(guild_name, server_slug, server_region, page=page)
    if not result.records:
        logger.warning("No records returned for guild_reports page %d — stopping", page)
        break

    records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]
    all_report_codes.extend(r["code"] for r in result.records if r.get("code"))

    out_path = f"{landing}/guild_reports/{run_ts}_p{page}.jsonl"
    with open(out_path, "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    logger.info("guild_reports page %d → %d records", page, len(records))
    has_more = result.has_more
    page += 1
    _sleep()

logger.info("Total report codes collected: %d", len(all_report_codes))

# COMMAND ----------
# ── 3. Report Fights + Actor Roster + Player Details ─────────────────────────
# For each report:
#   a) Fetch fight details (boss encounters, zone, masterData actors)
#   b) Fetch the actor roster separately (cached per report code)
#   c) For each boss kill (difficulty 3/4/5): fetch per-player performance
#
# Files are named by report code so already-fetched reports are skipped on
# re-runs.  Delete the relevant subdirectory in the Volume to force a refresh
# (e.g. when the fight schema was extended).

RAID_DIFFICULTIES = {3, 4, 5}  # Normal, Heroic, Mythic

for report_code in all_report_codes:

    # Skip any report already known to be archived from a previous run.
    if _is_archived(report_code):
        logger.info("report_fights: %s is archived — skipping", report_code)
        continue

    # ── 3a: Fight details ──────────────────────────────────────────────────
    fight_file = f"{landing}/report_fights/{report_code}.jsonl"
    if not os.path.exists(fight_file):
        try:
            fight_result = adapter.fetch_report_fights(report_code)
        except ArchivedReportError:
            _mark_archived(report_code)
            continue
        if fight_result.records:
            report_data = fight_result.records[0]
            with open(fight_file, "w") as fh:
                fh.write(
                    json.dumps({**report_data, "_source": "wcl", "_ingested_at": ingested_at})
                    + "\n"
                )
            logger.info(
                "report_fights: %s → %d fights",
                report_code,
                len(report_data.get("fights") or []),
            )
        _sleep()
    else:
        logger.info("report_fights: %s already fetched — skipping", report_code)
        with open(fight_file) as fh:
            report_data = json.loads(fh.readline())

    fights = report_data.get("fights") or []

    # ── 3b: Actor roster ──────────────────────────────────────────────────
    roster_file = f"{landing}/actor_roster/{report_code}.jsonl"
    if not os.path.exists(roster_file):
        try:
            roster_result = adapter.fetch_actor_roster(report_code)
        except ArchivedReportError:
            _mark_archived(report_code)
            continue
        if roster_result.records:
            with open(roster_file, "w") as fh:
                fh.write(
                    json.dumps(
                        {**roster_result.records[0], "_source": "wcl", "_ingested_at": ingested_at}
                    )
                    + "\n"
                )
            logger.info(
                "actor_roster: %s → %d actors",
                report_code,
                len(roster_result.records[0].get("actors") or []),
            )
        _sleep()
    else:
        logger.info("actor_roster: %s already fetched — skipping", report_code)

    # ── 3c: Player details for kill fights ────────────────────────────────
    kill_fights = [
        f for f in fights
        if f.get("kill")
        and f.get("difficulty", 0) in RAID_DIFFICULTIES
        and (f.get("encounterID") or 0) > 0
    ]
    logger.info("report %s: %d kill fights to process for player details", report_code, len(kill_fights))

    for fight in kill_fights:
        fight_id = fight["id"]
        details_file = f"{landing}/player_details/{report_code}_{fight_id}.jsonl"
        if os.path.exists(details_file):
            logger.info("player_details: %s fight %d already fetched — skipping", report_code, fight_id)
            continue

        try:
            pd_result = adapter.fetch_player_details(report_code, fight_id)
        except ArchivedReportError:
            _mark_archived(report_code)
            break  # all fights in this report are archived; move to next report
        if pd_result.records:
            record = {
                **pd_result.records[0],
                "boss_name": fight.get("name"),
                "encounter_id": fight.get("encounterID"),
                "difficulty": fight.get("difficulty"),
                "is_kill": fight.get("kill"),
                "duration_ms": (fight.get("endTime", 0) or 0) - (fight.get("startTime", 0) or 0),
                "zone_id": report_data.get("zone", {}).get("id") if report_data.get("zone") else None,
                "zone_name": report_data.get("zone", {}).get("name") if report_data.get("zone") else None,
                "_source": "wcl",
                "_ingested_at": ingested_at,
            }
            with open(details_file, "w") as fh:
                fh.write(json.dumps(record) + "\n")
            logger.info("player_details: %s fight %d written", report_code, fight_id)
        _sleep()

# COMMAND ----------
# ── 4. Raid Attendance ────────────────────────────────────────────────────────
# Fetch paginated attendance (players present/benched/absent per report).
# The attendance API returns zone {id, name} directly on each record.
logger.info("Fetching raid attendance …")
page, has_more = 1, True

while has_more:
    result = adapter.fetch_raid_attendance(guild_name, server_slug, server_region, page=page)
    if not result.records:
        logger.warning("No records returned for raid_attendance page %d — stopping", page)
        break

    records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]
    out_path = f"{landing}/raid_attendance/{run_ts}_p{page}.jsonl"
    with open(out_path, "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    logger.info("raid_attendance page %d → %d records", page, len(records))
    has_more = result.has_more
    page += 1
    _sleep()

logger.info("WCL ingestion complete.")

# COMMAND ----------
# ── 5. Guild Members (Blizzard API) ───────────────────────────────────────────
# Fetches the live guild roster from the Blizzard Profile API.
# Only runs when Blizzard credentials are configured in the secret scope.
# The roster changes frequently (member joins/leaves/rank changes), so we always
# overwrite rather than skip on re-run.
try:
    bz_client_id = dbutils.secrets.get(scope="warcraftlogs", key="blizzard_client_id")  # noqa: F821
    bz_client_secret = dbutils.secrets.get(scope="warcraftlogs", key="blizzard_client_secret")  # noqa: F821

    from ingestion.src.adapters.blizzard.client import BlizzardAdapter  # noqa: PLC0415

    bz_region = server_region.lower()
    # Build guild slug from guild name: lowercase, spaces → hyphens
    guild_slug = guild_name.lower().replace(" ", "-")

    bz_adapter = BlizzardAdapter()
    bz_adapter.authenticate(bz_client_id, bz_client_secret, region=bz_region)
    roster_result = bz_adapter.fetch_guild_roster(
        realm_slug=server_slug,
        guild_slug=guild_slug,
    )
    bz_adapter.close()
    _sleep()

    records = [
        {
            **r,
            "_source": "blizzard",
            "_ingested_at": ingested_at,
        }
        for r in roster_result.records
    ]

    members_file = f"{landing}/guild_members/{run_ts}.jsonl"
    with open(members_file, "w") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")

    logger.info("guild_members → %d members", len(records))

except Exception as e:
    logger.warning(
        "Blizzard API not configured or failed: %s — skipping guild members", e
    )

# COMMAND ----------
# ── 6. Fight Rankings ─────────────────────────────────────────────────────────
# Fetches WCL parse rankings for kill fights in each report.
# Rankings are stable once a report is cleared — skip if already fetched.
# Only kill fights on raid difficulties (3/4/5) with a valid encounterID are
# included; fights with no qualifying kills are skipped entirely.
logger.info("Fetching fight rankings …")

for report_code in all_report_codes:
    if _is_archived(report_code):
        logger.info("fight_rankings: %s is archived — skipping", report_code)
        continue

    rankings_file = f"{landing}/fight_rankings/{report_code}.jsonl"
    if os.path.exists(rankings_file):
        logger.info("fight_rankings: %s already fetched — skipping", report_code)
        continue

    fight_file = f"{landing}/report_fights/{report_code}.jsonl"
    if not os.path.exists(fight_file):
        logger.warning("fight_rankings: no fight file for %s — skipping", report_code)
        continue

    with open(fight_file) as fh:
        report_data_local = json.loads(fh.readline())

    fights_local = report_data_local.get("fights") or []
    kill_fight_ids = [
        f["id"]
        for f in fights_local
        if f.get("kill")
        and f.get("difficulty", 0) in RAID_DIFFICULTIES
        and (f.get("encounterID") or 0) > 0
    ]

    if not kill_fight_ids:
        logger.info("fight_rankings: %s has no qualifying kill fights — skipping", report_code)
        continue

    try:
        rankings_result = adapter.fetch_report_rankings(report_code, kill_fight_ids)
    except ArchivedReportError:
        _mark_archived(report_code)
        continue
    if rankings_result.records:
        record = {
            **rankings_result.records[0],
            "_source": "wcl",
            "_ingested_at": ingested_at,
        }
        with open(rankings_file, "w") as fh:
            fh.write(json.dumps(record) + "\n")
        logger.info("fight_rankings: %s written (%d kill fights)", report_code, len(kill_fight_ids))
    _sleep()

# COMMAND ----------
# ── 7. Fight Deaths ───────────────────────────────────────────────────────────
# Fetches death events for ALL boss fights (kills + wipes) per report via the
# WCL table API.  Deaths are aggregated across all requested fights — per-fight
# attribution is not available from this endpoint (limitation of the table API).
# Skip if already fetched.
logger.info("Fetching fight deaths …")

for report_code in all_report_codes:
    if _is_archived(report_code):
        logger.info("fight_deaths: %s is archived — skipping", report_code)
        continue

    deaths_file = f"{landing}/fight_deaths/{report_code}.jsonl"
    if os.path.exists(deaths_file):
        logger.info("fight_deaths: %s already fetched — skipping", report_code)
        continue

    fight_file = f"{landing}/report_fights/{report_code}.jsonl"
    if not os.path.exists(fight_file):
        logger.warning("fight_deaths: no fight file for %s — skipping", report_code)
        continue

    with open(fight_file) as fh:
        report_data_local = json.loads(fh.readline())

    fights_local = report_data_local.get("fights") or []
    all_boss_fight_ids = [
        f["id"]
        for f in fights_local
        if f.get("difficulty", 0) in RAID_DIFFICULTIES
        and (f.get("encounterID") or 0) > 0
    ]

    if not all_boss_fight_ids:
        logger.info("fight_deaths: %s has no qualifying boss fights — skipping", report_code)
        continue

    try:
        deaths_result = adapter.fetch_fight_deaths(report_code, all_boss_fight_ids)
    except ArchivedReportError:
        _mark_archived(report_code)
        continue
    if deaths_result.records:
        record = {
            **deaths_result.records[0],
            "_source": "wcl",
            "_ingested_at": ingested_at,
        }
        with open(deaths_file, "w") as fh:
            fh.write(json.dumps(record) + "\n")
        logger.info(
            "fight_deaths: %s written (%d boss fights)", report_code, len(all_boss_fight_ids)
        )
    _sleep()

logger.info("Ingestion complete.")
