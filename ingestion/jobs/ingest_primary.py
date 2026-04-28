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
from ingestion.src.utils.paths import LANDING_ROOTS, SOURCE_SUBDIRS
from ingestion.src.utils.wcl_rankings import (
    RANKINGS_BACKFILL_MAX_AGE_DAYS,
    RANKINGS_INCOMPLETE_NULL_FRACTION,
    rankings_completeness,
)

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
    "Ingesting guild=%s server=%s region=%s",
    guild_name, server_slug, server_region,
)

ACTIVE_STAGE = _config_value("stage", "all").strip().lower() or "all"


def _stage_enabled(*names: str) -> bool:
    return ACTIVE_STAGE == "all" or ACTIVE_STAGE in {name.strip().lower() for name in names}


logger.info("Active ingestion stage: %s", ACTIVE_STAGE)

# COMMAND ----------

# DBTITLE 1,Authentication
adapter: WarcraftLogsAdapter | None = None
try:
    client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")  # noqa: F821
    client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")  # noqa: F821
    adapter = WarcraftLogsAdapter(WarcraftLogsConfig(client_id=client_id, client_secret=client_secret))
    adapter.authenticate()
except Exception as exc:
    logger.warning("WarcraftLogs API not configured or failed: %s — skipping WCL ingestion", exc)

# COMMAND ----------

# DBTITLE 1,Volume setup
for source_catalog, source_schema in (
    ("01_bronze", "warcraftlogs"),
    ("01_bronze", "blizzard"),
    ("01_bronze", "raiderio"),
    ("01_bronze", "google_sheets"),
):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{source_catalog}`.`{source_schema}`.landing")  # noqa: F821

for source_name, landing_root in LANDING_ROOTS.items():
    for subdir in SOURCE_SUBDIRS[source_name]:
        os.makedirs(f"{landing_root}/{subdir}", exist_ok=True)

wcl_landing = LANDING_ROOTS["warcraftlogs"]
blizzard_landing = LANDING_ROOTS["blizzard"]
raiderio_landing = LANDING_ROOTS["raiderio"]
google_sheets_landing = LANDING_ROOTS["google_sheets"]


def _is_archived(report_code: str) -> bool:
    """Return True if this report has a permanent archived skip marker."""
    return os.path.exists(f"{wcl_landing}/archived/{report_code}")


def _mark_archived(report_code: str) -> None:
    """Write an empty skip marker so future runs bypass this report immediately."""
    open(f"{wcl_landing}/archived/{report_code}", "w").close()
    logger.warning("report_archived_marked: %s — will skip permanently", report_code)

run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
ingested_at = datetime.now(UTC).isoformat()
all_report_codes: list[str] = []

# COMMAND ----------

# DBTITLE 1,Zone Catalog
# Fetch once per run — zones rarely change but always refresh so new raid tiers
# appear automatically.
if _stage_enabled("wcl") and adapter is not None:
    try:
        logger.info("Fetching zone catalog …")
        zone_result = adapter.fetch_zone_catalog()
        with open(f"{wcl_landing}/zone_catalog/{run_ts}_zones.jsonl", "w") as fh:
            for zone in zone_result.records:
                fh.write(json.dumps({**zone, "_source": "wcl", "_ingested_at": ingested_at}) + "\n")
        logger.info("zone_catalog → %d zones", zone_result.total_records)
    except Exception as exc:
        logger.warning("WCL zone catalog ingestion failed: %s — skipping", exc)

# COMMAND ----------

# DBTITLE 1,Guild Reports
# Fetch all pages and write each as a JSONL file keyed by run timestamp + page.
# The silver layer deduplicates on report code, so re-runs are safe.
if _stage_enabled("wcl") and adapter is not None:
    try:
        logger.info("Fetching guild reports …")
        page, has_more = 1, True

        while has_more:
            result = adapter.fetch_guild_reports(guild_name, server_slug, server_region, page=page)
            if not result.records:
                logger.warning("No records returned for guild_reports page %d — stopping", page)
                break

            records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]
            all_report_codes.extend(r["code"] for r in result.records if r.get("code"))

            out_path = f"{wcl_landing}/guild_reports/{run_ts}_p{page}.jsonl"
            with open(out_path, "w") as fh:
                for record in records:
                    fh.write(json.dumps(record) + "\n")

            logger.info("guild_reports page %d → %d records", page, len(records))
            has_more = result.has_more
            page += 1

        logger.info("Total report codes collected: %d", len(all_report_codes))
    except Exception as exc:
        logger.warning("WCL guild reports ingestion failed: %s — skipping", exc)

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

if _stage_enabled("wcl") and adapter is not None:
    try:
        for report_code in all_report_codes:

            if _is_archived(report_code):
                logger.info("report_fights: %s is archived — skipping", report_code)
                continue

            fight_file = f"{wcl_landing}/report_fights/{report_code}.jsonl"
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
            else:
                logger.info("report_fights: %s already fetched — skipping", report_code)
                with open(fight_file) as fh:
                    report_data = json.loads(fh.readline())

            fights = report_data.get("fights") or []

            roster_file = f"{wcl_landing}/actor_roster/{report_code}.jsonl"
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
            else:
                logger.info("actor_roster: %s already fetched — skipping", report_code)

            kill_fights = [
                f for f in fights
                if f.get("kill")
                and f.get("difficulty", 0) in RAID_DIFFICULTIES
                and (f.get("encounterID") or 0) > 0
            ]
            logger.info("report %s: %d kill fights to process for player details", report_code, len(kill_fights))

            for fight in kill_fights:
                fight_id = fight["id"]
                details_file = f"{wcl_landing}/player_details/{report_code}_{fight_id}.jsonl"
                if os.path.exists(details_file):
                    logger.info("player_details: %s fight %d already fetched — skipping", report_code, fight_id)
                    continue

                try:
                    pd_result = adapter.fetch_player_details(report_code, fight_id)
                except ArchivedReportError:
                    _mark_archived(report_code)
                    break
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

            raid_fights = [
                f for f in fights
                if f.get("difficulty", 0) in RAID_DIFFICULTIES
                and (f.get("encounterID") or 0) > 0
            ]
            raid_fight_ids = [int(f["id"]) for f in raid_fights if f.get("id") is not None]
            casts_file_pattern = f"{wcl_landing}/fight_casts/{report_code}*.jsonl"
            cast_files = sorted(glob(casts_file_pattern))
            casts_file = cast_files[-1] if cast_files else f"{wcl_landing}/fight_casts/{report_code}.jsonl"
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
                        casts_file = f"{wcl_landing}/fight_casts/{report_code}_{run_ts}.jsonl"
                except Exception as exc:
                    logger.warning("fight_casts: %s unreadable (%s) — refetching", report_code, exc)
                    should_fetch_casts = True
                    casts_file = f"{wcl_landing}/fight_casts/{report_code}_{run_ts}.jsonl"

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
            elif raid_fight_ids:
                logger.info("fight_casts: %s already fetched — skipping", report_code)
    except Exception as exc:
        logger.warning("WCL report/fight ingestion failed: %s — skipping remaining WCL report stages", exc)

# COMMAND ----------

# DBTITLE 1,Raid Attendance
# Fetch paginated attendance (players present/benched/absent per report).
# The attendance API returns zone {id, name} directly on each record.
if _stage_enabled("wcl") and adapter is not None:
    try:
        logger.info("Fetching raid attendance …")
        page, has_more = 1, True

        while has_more:
            result = adapter.fetch_raid_attendance(guild_name, server_slug, server_region, page=page)
            if not result.records:
                logger.warning("No records returned for raid_attendance page %d — stopping", page)
                break

            records = [{**r, "_source": "wcl", "_ingested_at": ingested_at} for r in result.records]
            out_path = f"{wcl_landing}/raid_attendance/{run_ts}_p{page}.jsonl"
            with open(out_path, "w") as fh:
                for record in records:
                    fh.write(json.dumps(record) + "\n")

            logger.info("raid_attendance page %d → %d records", page, len(records))
            has_more = result.has_more
            page += 1

        logger.info("WCL ingestion complete.")
    except Exception as exc:
        logger.warning("WCL raid attendance ingestion failed: %s — skipping", exc)

# COMMAND ----------

# DBTITLE 1,Guild Members (Blizzard API)
# Fetches the live guild roster from the Blizzard Profile API.
# Only runs when Blizzard credentials are configured in the secret scope.
# The roster changes frequently (member joins/leaves/rank changes), so we always
# overwrite rather than skip on re-run.
if _stage_enabled("blizzard"):
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

        records = [
            {
                **r,
                "_source": "blizzard",
                "_ingested_at": ingested_at,
            }
            for r in roster_result.records
        ]

        members_file = f"{blizzard_landing}/guild_members/{run_ts}.jsonl"
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


def _load_realm_overrides() -> dict[str, str]:
    config_path = os.path.join(_bundle_root, "ingestion", "config", "realm_overrides.yml")
    try:
        import yaml  # noqa: PLC0415
    except ImportError:
        logger.warning("realm_overrides: pyyaml unavailable, using no overrides")
        return {}

    try:
        with open(config_path) as fh:
            config = yaml.safe_load(fh) or {}
    except (OSError, ValueError) as exc:
        logger.warning("realm_overrides: failed to load %s: %s", config_path, exc)
        return {}

    overrides = config.get("overrides")
    if not isinstance(overrides, dict):
        logger.warning("realm_overrides: invalid overrides payload in %s", config_path)
        return {}

    return {
        str(key): str(value)
        for key, value in overrides.items()
    }


REALM_SLUG_OVERRIDES = _load_realm_overrides()


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
    return REALM_SLUG_OVERRIDES.get(cleaned.replace("-", ""), cleaned.strip("-"))


def _log_fight_rankings_decision(
    report_code: str,
    incomplete_fights: int,
    total_fights: int,
    null_chars: int,
    total_chars: int,
    file_age_days: float,
    decision: str,
) -> None:
    logger.info(
        "fight_rankings_decision decision=%s report_code=%s incomplete_fights=%d total_fights=%d null_chars=%d total_chars=%d file_age_days=%.1f",
        decision,
        report_code,
        incomplete_fights,
        total_fights,
        null_chars,
        total_chars,
        file_age_days,
    )


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


if _stage_enabled("raiderio") and RAIDER_IO_EXPORT_ENABLED:
    try:
        from ingestion.src.adapters.raiderio.client import (  # noqa: PLC0415
            RaiderIoAdapter,
            RaiderIoNotFoundError,
            RaiderIoTransientError,
        )

        seen_candidates: set[tuple[str, str, str]] = set()
        raiderio_candidates: list[dict[str, str]] = []
        for candidate in _raiderio_candidates_from_existing_player_tables():
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

        profiles_file = f"{raiderio_landing}/raiderio_character_profiles/{run_ts}.jsonl"
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
elif _stage_enabled("raiderio"):
    logger.info("Skipping Raider.IO character profiles: RAIDER_IO_EXPORT_ENABLED=false")

# COMMAND ----------

# DBTITLE 1,Fight Rankings
# Fetches WCL parse rankings for kill fights in each report.
# WCL computes parse rankings asynchronously after a report is uploaded — for
# fresh reports, the rankings response can come back with character rows but
# null `rankPercent` for entire fights. We re-fetch any landing file whose
# stored rankings response still contains incomplete fights, until either all
# rankings are populated or the file ages past RANKINGS_BACKFILL_MAX_AGE_DAYS
# (after which we assume WCL is never going to rank those fights — e.g. exotic
# off-spec, partition split, or guild-private report).
if _stage_enabled("wcl") and adapter is not None:
    try:
        logger.info("Fetching fight rankings …")

        for report_code in all_report_codes:
            if _is_archived(report_code):
                logger.info("fight_rankings: %s is archived — skipping", report_code)
                continue

            rankings_file = f"{wcl_landing}/fight_rankings/{report_code}.jsonl"
            if os.path.exists(rankings_file):
                incomplete, total, null_chars, total_chars = rankings_completeness(rankings_file)
                file_age_days = (time.time() - os.path.getmtime(rankings_file)) / 86400
                if incomplete == 0:
                    _log_fight_rankings_decision(
                        report_code=report_code,
                        incomplete_fights=incomplete,
                        total_fights=total,
                        null_chars=null_chars,
                        total_chars=total_chars,
                        file_age_days=file_age_days,
                        decision="skip_complete",
                    )
                    continue
                if file_age_days >= RANKINGS_BACKFILL_MAX_AGE_DAYS:
                    _log_fight_rankings_decision(
                        report_code=report_code,
                        incomplete_fights=incomplete,
                        total_fights=total,
                        null_chars=null_chars,
                        total_chars=total_chars,
                        file_age_days=file_age_days,
                        decision="accept_final",
                    )
                    continue
                _log_fight_rankings_decision(
                    report_code=report_code,
                    incomplete_fights=incomplete,
                    total_fights=total,
                    null_chars=null_chars,
                    total_chars=total_chars,
                    file_age_days=file_age_days,
                    decision="refetch",
                )

            fight_file = f"{wcl_landing}/report_fights/{report_code}.jsonl"
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
    except Exception as exc:
        logger.warning("WCL fight rankings ingestion failed: %s — skipping", exc)

# COMMAND ----------

# DBTITLE 1,Fight Deaths
# Fetches death events for ALL boss fights (kills + wipes) per report via the
# WCL table API. Multi-fight Deaths responses can truncate on long reports, so
# fetches are done one fight at a time and written as JSONL records.
# Skip if already fetched.
if _stage_enabled("wcl") and adapter is not None:
    try:
        logger.info("Fetching fight deaths …")

        for report_code in all_report_codes:
            if _is_archived(report_code):
                logger.info("fight_deaths: %s is archived — skipping", report_code)
                continue

            deaths_file_pattern = f"{wcl_landing}/fight_deaths/{report_code}*.jsonl"
            death_files = sorted(glob(deaths_file_pattern))
            deaths_file = death_files[-1] if death_files else f"{wcl_landing}/fight_deaths/{report_code}.jsonl"

            fight_file = f"{wcl_landing}/report_fights/{report_code}.jsonl"
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
                        deaths_file = f"{wcl_landing}/fight_deaths/{report_code}_{run_ts}.jsonl"
                except Exception as exc:
                    logger.warning("fight_deaths: %s unreadable (%s) — refetching", report_code, exc)
                    should_fetch_deaths = True
                    deaths_file = f"{wcl_landing}/fight_deaths/{report_code}_{run_ts}.jsonl"

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
    except Exception as exc:
        logger.warning("WCL fight deaths ingestion failed: %s — skipping", exc)


# COMMAND ----------

# DBTITLE 1,Guild Zone Ranks (WCL)
# Fetches guildData.zoneRanking.progress per raid zone. The candidate zone list
# is sourced from gold_boss_progression in the configured candidate catalog/schema
# (parameterised so cutover remains a config flip — defaults can still point at
# the legacy gold location, but should normally target 03_gold.sc_analytics).
GUILD_ZONE_RANKS_ENABLED = _config_value("guild_zone_ranks_enabled", "true").lower() == "true"
PROFILE_CANDIDATE_CATALOG = _config_value("profile_candidate_catalog", "03_gold")
PROFILE_CANDIDATE_SCHEMA = _config_value("profile_candidate_schema", "sc_analytics")
EXCLUDED_ZONE_NAMES = [
    name.strip()
    for name in _config_value("excluded_zone_names", "").split(",")
    if name.strip()
]

if _stage_enabled("wcl") and GUILD_ZONE_RANKS_ENABLED and adapter is not None:
    try:
        zones_query = f"""
            SELECT DISTINCT CAST(zone_id AS BIGINT) AS zone_id, zone_name
            FROM `{PROFILE_CANDIDATE_CATALOG}`.`{PROFILE_CANDIDATE_SCHEMA}`.gold_boss_progression
            WHERE zone_id IS NOT NULL AND zone_name IS NOT NULL
        """
        if EXCLUDED_ZONE_NAMES:
            quoted = ", ".join(f"'{z}'" for z in EXCLUDED_ZONE_NAMES)
            zones_query += f" AND zone_name NOT IN ({quoted})"

        zone_rows = spark.sql(zones_query).collect()  # noqa: F821
        zone_ranks_file = f"{wcl_landing}/guild_zone_ranks/{run_ts}.jsonl"
        rank_count = 0
        with open(zone_ranks_file, "w") as fh:
            for row in zone_rows:
                zone_id = int(row["zone_id"])
                zone_name = str(row["zone_name"])
                try:
                    rank_result = adapter.fetch_guild_zone_ranks(
                        guild_name=guild_name,
                        server_slug=server_slug,
                        server_region=server_region,
                        zone_id=zone_id,
                    )
                except Exception as exc:
                    logger.warning("guild_zone_ranks: zone %s failed (%s) — skipping", zone_id, exc)
                    continue
                if rank_result.records:
                    record = {
                        **rank_result.records[0],
                        "zone_name": zone_name,
                        "_source": "wcl",
                        "_ingested_at": ingested_at,
                    }
                    fh.write(json.dumps(record) + "\n")
                    rank_count += 1
        logger.info("guild_zone_ranks → %d zones", rank_count)
    except Exception as exc:
        logger.warning("Guild zone ranks ingestion failed: %s — skipping", exc)
elif _stage_enabled("wcl") and GUILD_ZONE_RANKS_ENABLED:
    logger.info("Skipping guild zone ranks: WarcraftLogs adapter unavailable")
elif _stage_enabled("wcl"):
    logger.info("Skipping guild zone ranks: GUILD_ZONE_RANKS_ENABLED=false")

# COMMAND ----------

# DBTITLE 1,Live Raid Roster (Google Sheets)
# Pulls the manually-maintained raid roster sheet as raw CSV text for bronze.
# Silver parses the CSV with an explicit schema. The sheet must be shared with
# "anyone with the link" — no auth is performed.
LIVE_ROSTER_SHEET_ID = _config_value("live_roster_sheet_id", "")
LIVE_ROSTER_SHEET_GID = _config_value("live_roster_sheet_gid", "0")

if _stage_enabled("google_sheets") and LIVE_ROSTER_SHEET_ID:
    try:
        from ingestion.src.adapters.google_sheets.client import GoogleSheetsAdapter  # noqa: PLC0415

        gs_adapter = GoogleSheetsAdapter()
        gs_adapter.authenticate()
        sheet_result = gs_adapter.fetch_sheet_csv(LIVE_ROSTER_SHEET_ID, LIVE_ROSTER_SHEET_GID)
        gs_adapter.close()

        roster_file = f"{google_sheets_landing}/live_raid_roster/{run_ts}.jsonl"
        with open(roster_file, "w") as fh:
            for r in sheet_result.records:
                fh.write(
                    json.dumps({**r, "_source": "google_sheets", "_ingested_at": ingested_at})
                    + "\n"
                )
        logger.info("live_raid_roster → 1 csv payload (%d bytes)",
                    len(sheet_result.records[0].get("csv_text", "")))
    except Exception as exc:
        logger.warning("Live raid roster ingestion failed: %s — skipping", exc)
elif _stage_enabled("google_sheets"):
    logger.info("Skipping live raid roster: live_roster_sheet_id not configured")

# COMMAND ----------

# DBTITLE 1,Blizzard Character Profiles (media + equipment + achievements)
# Fetches per-character Blizzard Profile API payloads for guild raiders.
# Candidates come from the configured gold tables (gold_raid_team and
# gold_boss_kill_roster). The export script previously did this work locally;
# this section moves it into ingestion. Silver shapes the raw JSON.
BLIZZARD_PROFILE_EXPORT_ENABLED = (
    _config_value("blizzard_profile_export_enabled", "true").lower() == "true"
)
BLIZZARD_PROFILE_EXPORT_CAP = int(_config_value("blizzard_profile_export_cap", "0"))
BLIZZARD_PROFILE_SLEEP_SECONDS = float(_config_value("blizzard_profile_sleep_seconds", "0.05"))

if _stage_enabled("blizzard") and BLIZZARD_PROFILE_EXPORT_ENABLED:
    try:
        bz_client_id = dbutils.secrets.get(scope="warcraftlogs", key="blizzard_client_id")  # noqa: F821
        bz_client_secret = dbutils.secrets.get(scope="warcraftlogs", key="blizzard_client_secret")  # noqa: F821

        from ingestion.src.adapters.blizzard.client import BlizzardAdapter  # noqa: PLC0415

        candidates_query = f"""
            WITH candidates AS (
              SELECT
                name AS player_name,
                COALESCE(NULLIF(realm, ''), '{server_slug}') AS realm,
                true AS is_raid_team,
                0 AS kills_tracked,
                last_raid_date AS latest_seen_date
              FROM `{PROFILE_CANDIDATE_CATALOG}`.`{PROFILE_CANDIDATE_SCHEMA}`.gold_raid_team
              WHERE name IS NOT NULL AND name != ''

              UNION ALL

              SELECT
                player_name,
                '{server_slug}' AS realm,
                false AS is_raid_team,
                COUNT(*) AS kills_tracked,
                MAX(raid_night_date) AS latest_seen_date
              FROM `{PROFILE_CANDIDATE_CATALOG}`.`{PROFILE_CANDIDATE_SCHEMA}`.gold_boss_kill_roster
              WHERE player_name IS NOT NULL AND player_name != ''
              GROUP BY player_name
            )
            SELECT player_name, realm
            FROM candidates
            GROUP BY player_name, realm
            ORDER BY MAX(is_raid_team) DESC,
                     MAX(kills_tracked) DESC,
                     MAX(latest_seen_date) DESC NULLS LAST,
                     player_name
        """
        if BLIZZARD_PROFILE_EXPORT_CAP > 0:
            candidates_query += f"\nLIMIT {BLIZZARD_PROFILE_EXPORT_CAP}"

        candidate_rows = spark.sql(candidates_query).collect()  # noqa: F821
        seen: set[tuple[str, str]] = set()
        candidates_list: list[dict[str, str]] = []
        for row in candidate_rows:
            player_name = str(row["player_name"] or "").strip()
            realm = _realm_to_slug(row["realm"])
            key = (player_name.casefold(), realm)
            if not player_name or key in seen:
                continue
            seen.add(key)
            candidates_list.append({"player_name": player_name, "realm_slug": realm})

        logger.info("blizzard_profiles: %d candidates queued", len(candidates_list))

        bz_adapter = BlizzardAdapter()
        bz_adapter.authenticate(bz_client_id, bz_client_secret, region=server_region.lower())

        media_file = f"{blizzard_landing}/character_media/{run_ts}.jsonl"
        equipment_file = f"{blizzard_landing}/character_equipment/{run_ts}.jsonl"
        achievements_file = f"{blizzard_landing}/character_achievements/{run_ts}.jsonl"

        media_count = equipment_count = achievement_count = 0
        equipped_item_ids: set[int] = set()
        with (
            open(media_file, "w") as media_fh,
            open(equipment_file, "w") as equip_fh,
            open(achievements_file, "w") as ach_fh,
        ):
            for candidate in candidates_list:
                player_name = candidate["player_name"]
                realm_slug = candidate["realm_slug"]
                try:
                    media_res = bz_adapter.fetch_character_media(player_name, realm_slug)
                except Exception as exc:
                    logger.warning(
                        "blizzard_profiles: %s-%s media failed (%s)",
                        player_name, realm_slug, exc,
                    )
                    media_res = None

                # If media is missing the character is likely transferred/renamed;
                # skip the rest of the calls for that character.
                if not media_res or not media_res.records:
                    time.sleep(BLIZZARD_PROFILE_SLEEP_SECONDS)
                    continue

                for r in media_res.records:
                    media_fh.write(
                        json.dumps({**r, "_source": "blizzard", "_ingested_at": ingested_at}) + "\n"
                    )
                    media_count += 1

                try:
                    equip_res = bz_adapter.fetch_character_equipment(player_name, realm_slug)
                    for r in equip_res.records:
                        equip_fh.write(
                            json.dumps({**r, "_source": "blizzard", "_ingested_at": ingested_at})
                            + "\n"
                        )
                        equipment_count += 1
                        try:
                            equipment_payload = json.loads(r.get("equipment_json") or "{}")
                            for equipped_item in equipment_payload.get("equipped_items", []):
                                item_block = equipped_item.get("item") or {}
                                item_id = item_block.get("id")
                                if isinstance(item_id, int):
                                    equipped_item_ids.add(item_id)
                        except (TypeError, ValueError, json.JSONDecodeError):
                            pass
                except Exception as exc:
                    logger.warning(
                        "blizzard_profiles: %s-%s equipment failed (%s)",
                        player_name, realm_slug, exc,
                    )

                try:
                    ach_res = bz_adapter.fetch_character_achievements(player_name, realm_slug)
                    for r in ach_res.records:
                        ach_fh.write(
                            json.dumps({**r, "_source": "blizzard", "_ingested_at": ingested_at})
                            + "\n"
                        )
                        achievement_count += 1
                except Exception as exc:
                    logger.warning(
                        "blizzard_profiles: %s-%s achievements failed (%s)",
                        player_name, realm_slug, exc,
                    )

                time.sleep(BLIZZARD_PROFILE_SLEEP_SECONDS)

        # Item media: fetch icon URLs for any equipped item we haven't seen yet.
        # One file per item id under blizzard landing/item_media/{item_id}.jsonl
        # means reruns skip already-known items cheaply.
        item_media_count = 0
        item_media_dir = f"{blizzard_landing}/item_media"
        for item_id in sorted(equipped_item_ids):
            item_file = f"{item_media_dir}/{item_id}.jsonl"
            if os.path.exists(item_file):
                continue
            try:
                media_res = bz_adapter.fetch_item_media(item_id)
            except Exception as exc:
                logger.warning("blizzard_profiles: item %s media failed (%s)", item_id, exc)
                continue
            if not media_res.records:
                continue
            with open(item_file, "w") as fh:
                for r in media_res.records:
                    fh.write(
                        json.dumps({**r, "_source": "blizzard", "_ingested_at": ingested_at})
                        + "\n"
                    )
                    item_media_count += 1
            time.sleep(BLIZZARD_PROFILE_SLEEP_SECONDS)

        bz_adapter.close()
        logger.info(
            "blizzard_profiles → media=%d equipment=%d achievements=%d item_media=%d",
            media_count, equipment_count, achievement_count, item_media_count,
        )
    except Exception as exc:
        logger.warning("Blizzard profile ingestion failed: %s — skipping", exc)
elif _stage_enabled("blizzard"):
    logger.info("Skipping Blizzard profiles: BLIZZARD_PROFILE_EXPORT_ENABLED=false")

# COMMAND ----------

logger.info("Ingestion complete.")
