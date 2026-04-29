# Databricks notebook source
# Bronze layer — WarcraftLogs source
#
# All tables publish to 01_bronze.warcraftlogs via fully-qualified `name=`
# (Free Edition serverless DLT does not accept catalog=/schema= kwargs).
# Pre-create the schema once: CREATE SCHEMA IF NOT EXISTS 01_bronze.warcraftlogs;
#
# Reads from the source-matched bronze landing volume at
#   /Volumes/01_bronze/warcraftlogs/landing/<subdir>/

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

LANDING = "/Volumes/01_bronze/warcraftlogs/landing"
TARGET_SCHEMA = "01_bronze.warcraftlogs"


def _autoload(subdir: str, schema: StructType, allow_overwrites: bool = False):
    """Standard Auto Loader configuration for a WCL bronze landing subdir.

    allow_overwrites=True lets re-landed files with the same path be re-ingested.
    Required for sources whose ingestion job overwrites payloads in place
    (e.g. fight_rankings, where WCL fills in previously-null rankPercent values
    asynchronously and the ingestion job rewrites the same JSONL file).
    """
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .schema(schema)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING}/{subdir}/_schema")
    )
    if allow_overwrites:
        reader = reader.option("cloudFiles.allowOverwrites", "true")
    return reader.load(f"{LANDING}/{subdir}/").withColumn(
        "_file_path", F.col("_metadata.file_path")
    )


# ── Shared sub-schemas ─────────────────────────────────────────────────────────

_ZONE_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

_ACTOR_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("subType", StringType(), True),
        StructField("server", StringType(), True),
    ]
)

_FIGHT_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("encounterID", LongType(), True),
        StructField("kill", BooleanType(), True),
        StructField("startTime", LongType(), True),
        StructField("endTime", LongType(), True),
        StructField("difficulty", LongType(), True),
        StructField("fightPercentage", DoubleType(), True),
        StructField("bossPercentage", DoubleType(), True),
        StructField("lastPhase", LongType(), True),
        StructField("size", LongType(), True),
        StructField("friendlyPlayers", ArrayType(LongType()), True),
    ]
)

_PLAYER_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("presence", LongType(), True),
        StructField("type", StringType(), True),
    ]
)

_ENCOUNTER_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

_DIFFICULTY_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("sizes", ArrayType(LongType()), True),
    ]
)


# ── Guild Reports ──────────────────────────────────────────────────────────────

_GUILD_REPORTS_SCHEMA = StructType(
    [
        StructField("code", StringType(), True),
        StructField("title", StringType(), True),
        StructField("startTime", LongType(), True),
        StructField("endTime", LongType(), True),
        StructField("zone", _ZONE_STRUCT, True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_guild_reports",
    comment="Raw guild report records from WarcraftLogs API.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_guild_reports():
    return _autoload("guild_reports", _GUILD_REPORTS_SCHEMA)


# ── Report Fights ──────────────────────────────────────────────────────────────

_REPORT_FIGHTS_SCHEMA = StructType(
    [
        StructField("code", StringType(), True),
        StructField("title", StringType(), True),
        StructField("startTime", LongType(), True),
        StructField("endTime", LongType(), True),
        StructField("zone", _ZONE_STRUCT, True),
        StructField(
            "masterData",
            StructType(
                [
                    StructField("actors", ArrayType(_ACTOR_STRUCT), True),
                ]
            ),
            True,
        ),
        StructField("fights", ArrayType(_FIGHT_STRUCT), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_report_fights",
    comment="Raw fight-level data per raid report.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_report_fights():
    return _autoload("report_fights", _REPORT_FIGHTS_SCHEMA)


# ── Raid Attendance ────────────────────────────────────────────────────────────

_RAID_ATTENDANCE_SCHEMA = StructType(
    [
        StructField("code", StringType(), True),
        StructField("startTime", LongType(), True),
        StructField("zone", _ZONE_STRUCT, True),
        StructField("players", ArrayType(_PLAYER_STRUCT), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_raid_attendance",
    comment="Raw raid attendance records per report.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_raid_attendance():
    return _autoload("raid_attendance", _RAID_ATTENDANCE_SCHEMA)


# ── Actor Roster ───────────────────────────────────────────────────────────────

_ACTOR_ROSTER_SCHEMA = StructType(
    [
        StructField("report_code", StringType(), True),
        StructField("actors", ArrayType(_ACTOR_STRUCT), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_actor_roster",
    comment="Raw player actor roster per report from masterData.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_actor_roster():
    return _autoload("actor_roster", _ACTOR_ROSTER_SCHEMA)


# ── Player Details ─────────────────────────────────────────────────────────────

_PLAYER_DETAILS_SCHEMA = StructType(
    [
        StructField("report_code", StringType(), True),
        StructField("fight_id", LongType(), True),
        StructField("boss_name", StringType(), True),
        StructField("encounter_id", LongType(), True),
        StructField("difficulty", LongType(), True),
        StructField("is_kill", BooleanType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("zone_id", LongType(), True),
        StructField("zone_name", StringType(), True),
        StructField("player_details_json", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_player_details",
    comment="Raw per-player performance JSON per boss kill fight.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_fight_id", "fight_id IS NOT NULL")
@dlt.expect("has_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_player_details():
    return _autoload("player_details", _PLAYER_DETAILS_SCHEMA)


# ── Zone Catalog ───────────────────────────────────────────────────────────────

_ZONE_CATALOG_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("frozen", BooleanType(), True),
        StructField("encounters", ArrayType(_ENCOUNTER_STRUCT), True),
        StructField("difficulties", ArrayType(_DIFFICULTY_STRUCT), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_zone_catalog",
    comment="WCL zone and encounter reference catalog.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_zone_id", "id IS NOT NULL")
def bronze_zone_catalog():
    return _autoload("zone_catalog", _ZONE_CATALOG_SCHEMA)


# ── Fight Rankings ─────────────────────────────────────────────────────────────

_FIGHT_RANKINGS_SCHEMA = StructType(
    [
        StructField("report_code", StringType(), True),
        # rankings_json: WCL rankings(playerMetric: dps) — used for dps/tank rows.
        StructField("rankings_json", StringType(), True),
        # rankings_hps_json: WCL rankings(playerMetric: hps) — used for healer rows.
        # Nullable so old bronze landings (which only carry rankings_json) still load;
        # silver gracefully nulls healer parses for those legacy reports until
        # re-ingested.
        StructField("rankings_hps_json", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_fight_rankings",
    comment="Raw WCL parse rankings per report (opaque JSON scalar).",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_rankings_data", "rankings_json IS NOT NULL")
def bronze_fight_rankings():
    # allow_overwrites=True: the ingestion job re-lands fight_rankings/<report>.jsonl
    # in place when WCL has filled in previously-null `rankPercent` values.
    # silver_player_rankings dedupes per report_code keeping the latest _ingested_at.
    return _autoload("fight_rankings", _FIGHT_RANKINGS_SCHEMA, allow_overwrites=True)


# ── Fight Deaths ───────────────────────────────────────────────────────────────

_FIGHT_DEATHS_SCHEMA = StructType(
    [
        StructField("report_code", StringType(), True),
        StructField("fight_ids", ArrayType(LongType()), True),
        StructField("table_json", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_fight_deaths",
    comment="Raw WCL death table per report (opaque JSON scalar).",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_deaths_data", "table_json IS NOT NULL")
def bronze_fight_deaths():
    return _autoload("fight_deaths", _FIGHT_DEATHS_SCHEMA)


# ── Fight Casts ────────────────────────────────────────────────────────────────

_FIGHT_CASTS_SCHEMA = StructType(
    [
        StructField("report_code", StringType(), True),
        StructField("fight_ids", ArrayType(LongType()), True),
        StructField("events_json", StringType(), True),
        StructField("buffs_json", StringType(), True),
        StructField("combatant_info_json", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_fight_casts",
    comment="Raw WCL cast events for raid boss pulls.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_report_code", "report_code IS NOT NULL")
@dlt.expect("has_cast_data", "events_json IS NOT NULL")
def bronze_fight_casts():
    return _autoload("fight_casts", _FIGHT_CASTS_SCHEMA)


# ── Guild Zone Ranks (NEW — migrated from export script) ──────────────────────

_GUILD_ZONE_RANKS_SCHEMA = StructType(
    [
        StructField("zone_id", LongType(), True),
        StructField("zone_name", StringType(), True),
        StructField("progress_json", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


@dlt.table(
    name=f"{TARGET_SCHEMA}.bronze_guild_zone_ranks",
    comment="Raw guildData.zoneRanking.progress per raid zone (opaque JSON scalar).",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_zone_id", "zone_id IS NOT NULL")
def bronze_guild_zone_ranks():
    return _autoload("guild_zone_ranks", _GUILD_ZONE_RANKS_SCHEMA)
