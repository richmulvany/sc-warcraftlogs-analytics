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
from glob import glob
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

# DBTITLE 1,Configuration
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


def _config_value(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        value = ""
    return value or os.environ.get(name.upper(), default)


logger.info(
    "Ingesting guild=%s server=%s region=%s → %s.%s",
    guild_name, server_slug, server_region, catalog, schema,
)

# COMMAND ----------

# DBTITLE 1,Authentication
client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")  # noqa: F821
client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")  # noqa: F821

adapter = WarcraftLogsAdapter(WarcraftLogsConfig(client_id=client_id, client_secret=client_secret))
adapter.authenticate()

# COMMAND ----------

# DBTITLE 1,Volume setup
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
    "raiderio_character_profiles",
    "fight_rankings",
    "fight_deaths",
    "fight_casts",
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
guild_member_records: list[dict[str, object]] = []

# Helper: throttle between API calls to stay within 30 req/min
def _sleep() -> None:
    time.sleep(2)

# COMMAND ----------

# DBTITLE 1,Zone Catalog
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

# DBTITLE 1,Guild Reports
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

# DBTITLE 1,Report Fights + Actor Roster + Player Details
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

    # ── 3d: Cast events for consumable + defensive analysis ───────────────
    raid_fights = [
        f for f in fights
        if f.get("difficulty", 0) in RAID_DIFFICULTIES
        and (f.get("encounterID") or 0) > 0
    ]
    raid_fight_ids = [int(f["id"]) for f in raid_fights if f.get("id") is not None]
    casts_file_pattern = f"{landing}/fight_casts/{report_code}*.jsonl"
    cast_files = sorted(glob(casts_file_pattern))
    casts_file = cast_files[-1] if cast_files else f"{landing}/fight_casts/{report_code}.jsonl"
    should_fetch_casts = bool(raid_fight_ids) and not cast_files
    if raid_fight_ids and cast_files:
        try:
            with open(casts_file) as fh:
                existing_cast_record = json.loads(fh.readline() or "{}")
            existing_fight_ids = {
                int(fight_id) for fight_id in existing_cast_record.get("fight_ids", [])
                if fight_id is not None
            }
            should_fetch_casts = (
                "combatant_info_json" not in existing_cast_record
                or "buffs_json" not in existing_cast_record
                or not set(raid_fight_ids).issubset(existing_fight_ids)
            )
            if should_fetch_casts:
                logger.info("fight_casts: %s stale/incomplete — refetching", report_code)
                casts_file = f"{landing}/fight_casts/{report_code}_{run_ts}.jsonl"
        except Exception as exc:
            logger.warning("fight_casts: %s unreadable (%s) — refetching", report_code, exc)
            should_fetch_casts = True
            casts_file = f"{landing}/fight_casts/{report_code}_{run_ts}.jsonl"

    if raid_fight_ids and should_fetch_casts:
        try:
            casts_result = adapter.fetch_fight_casts(report_code, raid_fight_ids)
        except ArchivedReportError:
            _mark_archived(report_code)
            continue
        if casts_result.records:
            with open(casts_file, "w") as fh:
                fh.write(
                    json.dumps({**casts_result.records[0], "_source": "wcl", "_ingested_at": ingested_at})
                    + "\n"
                )
            logger.info(
                "fight_casts: %s → %d fights",
                report_code,
                len(raid_fight_ids),
            )
        _sleep()
    elif raid_fight_ids:
        logger.info("fight_casts: %s already fetched — skipping", report_code)

# COMMAND ----------

# DBTITLE 1,Raid Attendance
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

# DBTITLE 1,Guild Members (Blizzard API)
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
    guild_member_records = records

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

# DBTITLE 1,Raider.IO Character Profiles
# Fetches current-season Mythic+ profile data for all guild members and lands
# the raw profile payload as opaque JSON.  DLT owns parsing and product shaping.
RAIDER_IO_EXPORT_ENABLED = _config_value("raider_io_export_enabled", "true").lower() == "true"
RAIDER_IO_REGION = _config_value("raider_io_region", server_region).lower()
RAIDER_IO_SEASON = _config_value("raider_io_season", "current")
RAIDER_IO_PROFILE_EXPORT_CAP = int(_config_value("raider_io_profile_export_cap", "0"))
RAIDER_IO_REQUEST_SLEEP_SECONDS = float(_config_value("raider_io_request_sleep_seconds", "0.25"))


def _realm_to_slug(value: object) -> str:
    text = str(value or server_slug or "").strip()
    if not text:
        return server_slug
    cleaned = text.replace("'", "").replace("_", "-").replace(" ", "-")
    cleaned = "".join(
        f"-{char.lower()}" if char.isupper() else char
        for char in cleaned
    ).lower()
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    overrides = {
        "twistingnether": "twisting-nether",
        "twisting-nether": "twisting-nether",
        "defiasbrotherhood": "defias-brotherhood",
        "defias-brotherhood": "defias-brotherhood",
        "argentdawn": "argent-dawn",
        "argent-dawn": "argent-dawn",
    }
    return overrides.get(cleaned.replace("-", ""), cleaned.strip("-"))


def _raiderio_candidates_from_current_roster() -> list[dict[str, str]]:
    """All guild members from the current Blizzard API response."""
    candidates: list[dict[str, str]] = []
    for member in guild_member_records:
        name = str(member.get("name") or "").strip()
        if not name:
            continue
        candidates.append(
            {
                "player_name": name,
                "realm_slug": _realm_to_slug(member.get("realm_slug")),
                "region": RAIDER_IO_REGION,
            }
        )
    return candidates


def _raiderio_candidates_from_table(
    table_name: str,
    name_expr: str,
    realm_expr: str,
    where_clause: str,
) -> list[dict[str, str]]:
    """Seed Raider.IO candidates from a previously materialised identity table."""
    try:
        rows = spark.sql(  # noqa: F821
            f"""
            SELECT
              {name_expr} AS player_name,
              COALESCE(NULLIF({realm_expr}, ''), '{server_slug}') AS realm_slug
            FROM `{catalog}`.`{schema}`.{table_name}
            WHERE {where_clause}
            """
        ).collect()
    except Exception as exc:
        logger.info("raiderio: %s unavailable for candidate seed: %s", table_name, exc)
        return []

    candidates = [
        {
            "player_name": str(row["player_name"]).strip(),
            "realm_slug": _realm_to_slug(row["realm_slug"]),
            "region": RAIDER_IO_REGION,
        }
        for row in rows
        if str(row["player_name"] or "").strip()
    ]
    logger.info("raiderio: %s contributed %d candidate rows", table_name, len(candidates))
    return candidates


def _raiderio_candidates_from_existing_player_tables() -> list[dict[str, str]]:
    """Include known active/cross-realm players that are not in Blizzard guild membership."""
    return (
        _raiderio_candidates_from_table(
            table_name="silver_guild_members",
            name_expr="name",
            realm_expr="realm_slug",
            where_clause="name IS NOT NULL AND name != ''",
        )
        + _raiderio_candidates_from_table(
            table_name="gold_guild_roster",
            name_expr="name",
            realm_expr="realm",
            where_clause="""
              name IS NOT NULL
              AND name != ''
              AND COALESCE(CAST(is_active AS BOOLEAN), false) = true
            """,
        )
        + _raiderio_candidates_from_table(
            table_name="gold_raid_team",
            name_expr="name",
            realm_expr="realm",
            where_clause="name IS NOT NULL AND name != ''",
        )
    )


if RAIDER_IO_EXPORT_ENABLED:
    try:
        from ingestion.src.adapters.raiderio.client import (  # noqa: PLC0415
            RaiderIoAdapter,
            RaiderIoNotFoundError,
            RaiderIoTransientError,
        )

        seen_candidates: set[tuple[str, str, str]] = set()
        raiderio_candidates: list[dict[str, str]] = []
        for candidate in (
            _raiderio_candidates_from_current_roster()
            + _raiderio_candidates_from_existing_player_tables()
        ):
            key = (
                candidate["player_name"].casefold(),
                candidate["realm_slug"],
                candidate["region"],
            )
            if key in seen_candidates:
                continue
            seen_candidates.add(key)
            raiderio_candidates.append(candidate)

        if RAIDER_IO_PROFILE_EXPORT_CAP > 0:
            raiderio_candidates = raiderio_candidates[:RAIDER_IO_PROFILE_EXPORT_CAP]
            logger.info(
                "Fetching Raider.IO profiles for %d characters (cap=%d)",
                len(raiderio_candidates),
                RAIDER_IO_PROFILE_EXPORT_CAP,
            )
        else:
            logger.info(
                "Fetching Raider.IO profiles for all %d guild characters",
                len(raiderio_candidates),
            )

        rio_adapter = RaiderIoAdapter(
            region=RAIDER_IO_REGION,
            request_sleep_seconds=RAIDER_IO_REQUEST_SLEEP_SECONDS,
        )
        rio_adapter.authenticate()

        profiles_file = f"{landing}/raiderio_character_profiles/{run_ts}.jsonl"
        rows_written = 0
        with open(profiles_file, "w") as fh:
            for candidate in raiderio_candidates:
                player_name = candidate["player_name"]
                realm_slug = candidate["realm_slug"]
                try:
                    result = rio_adapter.fetch_character_profile(
                        name=player_name,
                        realm_slug=realm_slug,
                        season=RAIDER_IO_SEASON,
                    )
                except RaiderIoNotFoundError:
                    logger.info("Raider.IO profile not found for %s-%s", player_name, realm_slug)
                    time.sleep(RAIDER_IO_REQUEST_SLEEP_SECONDS)
                    continue
                except RaiderIoTransientError as exc:
                    logger.warning(
                        "Raider.IO transient error for %s-%s after retries: %s",
                        player_name,
                        realm_slug,
                        exc,
                    )
                    time.sleep(RAIDER_IO_REQUEST_SLEEP_SECONDS)
                    continue

                profile = result.records[0] if result.records else {}
                record = {
                    "player_name": player_name,
                    "realm_slug": realm_slug,
                    "region": RAIDER_IO_REGION,
                    "profile_url": profile.get("profile_url"),
                    "profile_json": json.dumps(profile),
                    "_source": "raiderio",
                    "_ingested_at": ingested_at,
                }
                fh.write(json.dumps(record) + "\n")
                rows_written += 1
                time.sleep(RAIDER_IO_REQUEST_SLEEP_SECONDS)

        rio_adapter.close()
        logger.info("raiderio_character_profiles → %d profiles", rows_written)
    except Exception as e:
        logger.warning("Raider.IO API failed: %s — skipping Mythic+ profile ingestion", e)
else:
    logger.info("Skipping Raider.IO character profiles: RAIDER_IO_EXPORT_ENABLED=false")

# COMMAND ----------

# DBTITLE 1,Fight Rankings
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

# DBTITLE 1,Fight Deaths
# Fetches death events for ALL boss fights (kills + wipes) per report via the
# WCL table API. Multi-fight Deaths responses can truncate on long reports, so
# fetches are done one fight at a time and written as JSONL records.
# Skip if already fetched.
logger.info("Fetching fight deaths …")

for report_code in all_report_codes:
    if _is_archived(report_code):
        logger.info("fight_deaths: %s is archived — skipping", report_code)
        continue

    deaths_file_pattern = f"{landing}/fight_deaths/{report_code}*.jsonl"
    death_files = sorted(glob(deaths_file_pattern))
    deaths_file = death_files[-1] if death_files else f"{landing}/fight_deaths/{report_code}.jsonl"

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

    should_fetch_deaths = not death_files
    if death_files:
        try:
            line_count = 0
            existing_fight_ids: set[int] = set()
            has_legacy_multi_fight_record = False
            with open(deaths_file) as fh:
                for line in fh:
                    raw_record = json.loads(line)
                    line_count += 1
                    record_fight_ids = {
                        int(fight_id)
                        for fight_id in raw_record.get("fight_ids", [])
                        if fight_id is not None
                    }
                    existing_fight_ids.update(record_fight_ids)
                    if len(record_fight_ids) > 1:
                        has_legacy_multi_fight_record = True
            should_fetch_deaths = (
                has_legacy_multi_fight_record
                or line_count < len(all_boss_fight_ids)
                or not set(all_boss_fight_ids).issubset(existing_fight_ids)
            )
            if should_fetch_deaths:
                logger.info("fight_deaths: %s stale/incomplete — refetching", report_code)
                deaths_file = f"{landing}/fight_deaths/{report_code}_{run_ts}.jsonl"
        except Exception as exc:
            logger.warning("fight_deaths: %s unreadable (%s) — refetching", report_code, exc)
            should_fetch_deaths = True
            deaths_file = f"{landing}/fight_deaths/{report_code}_{run_ts}.jsonl"

    if not should_fetch_deaths:
        logger.info("fight_deaths: %s already fetched — skipping", report_code)
        continue

    try:
        deaths_result = adapter.fetch_fight_deaths(report_code, all_boss_fight_ids)
    except ArchivedReportError:
        _mark_archived(report_code)
        continue
    if deaths_result.records:
        with open(deaths_file, "w") as fh:
            for raw_record in deaths_result.records:
                record = {
                    **raw_record,
                    "_source": "wcl",
                    "_ingested_at": ingested_at,
                }
                fh.write(json.dumps(record) + "\n")
        logger.info(
            "fight_deaths: %s written (%d boss fights, %d records)",
            report_code,
            len(all_boss_fight_ids),
            len(deaths_result.records),
        )
    _sleep()

logger.info("Ingestion complete.")
