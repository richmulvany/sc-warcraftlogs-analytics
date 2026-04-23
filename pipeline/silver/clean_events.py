# Databricks notebook source
# Silver layer — parsed WCL fight death events
#
# silver_player_deaths — one row per death event per player per report.
#
# Actual WCL table(dataType: Deaths) response structure:
# {
#   "data": {
#     "entries": [
#       {
#         "name": "PlayerName",
#         "id": 7,
#         "type": "DeathKnight",     <- WoW class
#         "icon": "DeathKnight-Blood",
#         "timestamp": 1073684,      <- death timestamp (ms from report start)
#         "fight": 4,                <- fight ID (available per death!)
#         "overkill": 0,
#         "events": [                <- damage window leading to death, newest first
#           {
#             "timestamp": 1073346,
#             "type": "damage",
#             "sourceIsFriendly": false,
#             "ability": {"name": "Caustic Phlegm", "guid": 1246653},
#             ...
#           },
#           ...
#         ]
#       }
#     ]
#   }
# }
#
# Key findings vs original assumptions:
#   - Each entry is ONE death event (not a player summary with a deathEvents array)
#   - fight_id IS available on each entry via the "fight" field
#   - events[] are sorted newest-first; events[0] from a non-friendly source = killing blow
#   - No "killingBlow" top-level field — derived from events[0]

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema for the table(dataType: Deaths) JSON scalar ────────────────────────

_ABILITY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("guid", LongType(),   True),
])

_EVENT_STRUCT = StructType([
    StructField("timestamp",        LongType(),    True),
    StructField("type",             StringType(),  True),  # "damage", "heal", etc.
    StructField("sourceIsFriendly", BooleanType(), True),
    StructField("ability",          _ABILITY_STRUCT, True),
])

_ENTRY_STRUCT = StructType([
    StructField("name",       StringType(),           True),  # player name
    StructField("id",         LongType(),             True),  # actor ID
    StructField("type",       StringType(),           True),  # WoW class
    StructField("icon",       StringType(),           True),  # "ClassName-Spec"
    StructField("timestamp",  LongType(),             True),  # ms from report start
    StructField("fight",      LongType(),             True),  # fight ID
    StructField("overkill",   LongType(),             True),
    StructField("events",     ArrayType(_EVENT_STRUCT), True),
])

_TABLE_DATA_STRUCT = StructType([
    StructField("entries", ArrayType(_ENTRY_STRUCT), True),
])

_TABLE_SCHEMA = StructType([
    StructField("data", _TABLE_DATA_STRUCT, True),
])


# ── Parsed Player Death Events ─────────────────────────────────────────────────

@dlt.table(
    name="silver_player_deaths",
    comment=(
        "WCL death events per player per report. "
        "One row per death event with fight_id and killing blow. "
        "fight_id is available — each entry in the Deaths table corresponds "
        "to a single death in a specific fight."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_deaths():
    raw = dlt.read("bronze_fight_deaths")  # batch — stable once ingested

    # Legacy bronze death files stored many fights in one record; WCL truncates
    # those responses on long reports. Once a report has any single-fight death
    # records, prefer those and ignore the older multi-fight payloads.
    report_modes = (
        raw
        .groupBy("report_code")
        .agg(
            F.max(
                F.when(F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1, F.lit(1))
                .otherwise(F.lit(0))
            ).alias("has_single_fight_records")
        )
    )

    filtered_raw = (
        raw
        .join(report_modes, on="report_code", how="left")
        .filter(
            (F.col("has_single_fight_records") == 0)
            | (F.size(F.coalesce(F.col("fight_ids"), F.array().cast("array<long>"))) == 1)
        )
        .drop("has_single_fight_records")
    )

    return (
        filtered_raw
        .withColumn("parsed", F.from_json(F.col("table_json"), _TABLE_SCHEMA))
        # Drop rows where JSON parsing failed
        .filter(F.col("parsed").isNotNull())
        # Explode entries: one row per death event
        .withColumn("entry", F.explode("parsed.data.entries"))
        .select(
            F.col("report_code"),
            F.col("entry.fight").alias("fight_id"),
            F.col("entry.name").alias("player_name"),
            F.col("entry.type").alias("player_class"),
            F.col("entry.timestamp").alias("death_timestamp_ms"),
            F.col("entry.overkill").alias("overkill"),
            # Killing blow: filter events to non-friendly damage sources (newest first),
            # take element [0] — the hit closest to the death timestamp.
            # FILTER() is a Spark SQL higher-order function available in Spark 3.x+.
            F.expr(
                "FILTER(entry.events, e -> e.type = 'damage' AND e.sourceIsFriendly = false)[0]"
            ).alias("kb_event"),
        )
        .withColumn("killing_blow_name", F.col("kb_event.ability.name"))
        .withColumn("killing_blow_id",   F.col("kb_event.ability.guid"))
        .drop("kb_event")
        .filter(F.col("player_name").isNotNull())
    )


# ── Schema for events(dataType: Casts) JSON scalar ────────────────────────────

_CAST_ABILITY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("guid", LongType(), True),
    StructField("type", LongType(), True),
    StructField("abilityIcon", StringType(), True),
])

_CAST_EVENT_STRUCT = StructType([
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("sourceID", LongType(), True),
    StructField("targetID", LongType(), True),
    StructField("fight", LongType(), True),
    StructField("abilityGameID", LongType(), True),
    StructField("ability", _CAST_ABILITY_STRUCT, True),
])

_CAST_EVENTS_SCHEMA = StructType([
    StructField("data", ArrayType(_CAST_EVENT_STRUCT), True),
])


# ── Parsed Player Cast Events ─────────────────────────────────────────────────

@dlt.table(
    name="silver_player_cast_events",
    comment=(
        "WCL player cast events for raid boss pulls. "
        "Used for health potion, healthstone, and defensive usage analysis."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_cast_events():
    raw = dlt.read("bronze_fight_casts")
    actors = dlt.read("silver_actor_roster")
    fights = dlt.read("silver_fight_events")

    parsed = (
        raw
        .withColumn("parsed", F.from_json(F.col("events_json"), _CAST_EVENTS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        .withColumn("event", F.explode("parsed.data"))
        .select(
            F.col("report_code"),
            F.col("event.fight").alias("event_fight_id"),
            F.col("event.timestamp").alias("cast_timestamp_ms"),
            F.col("event.sourceID").alias("actor_id"),
            F.col("event.targetID").alias("target_actor_id"),
            F.col("event.ability.name").alias("ability_name"),
            F.coalesce(F.col("event.ability.guid"), F.col("event.abilityGameID")).alias("ability_id"),
            F.col("event.ability.abilityIcon").alias("ability_icon"),
            F.col("_ingested_at"),
        )
        .filter(F.col("actor_id").isNotNull())
        .filter(F.col("ability_name").isNotNull() | F.col("ability_id").isNotNull())
    )

    actor_lookup = actors.select(
        F.col("report_code").alias("_a_report_code"),
        F.col("actor_id").alias("_a_actor_id"),
        "player_name",
        "player_class",
    )

    fight_lookup = fights.select(
        F.col("report_code").alias("_f_report_code"),
        F.col("fight_id").alias("_f_fight_id"),
        F.col("fight_start_ms").alias("_f_start_ms"),
        (F.col("fight_start_ms") + (F.col("duration_seconds") * 1000)).alias("_f_end_ms"),
    )

    with_fight = (
        parsed
        .join(
            fight_lookup,
            (parsed.report_code == fight_lookup._f_report_code)
            & (
                (parsed.event_fight_id == fight_lookup._f_fight_id)
                | (
                    parsed.event_fight_id.isNull()
                    & (parsed.cast_timestamp_ms >= fight_lookup._f_start_ms)
                    & (parsed.cast_timestamp_ms <= fight_lookup._f_end_ms)
                )
            ),
            "left",
        )
        .withColumn("fight_id", F.coalesce(F.col("event_fight_id"), F.col("_f_fight_id")))
        .drop("_f_report_code", "_f_fight_id", "_f_start_ms", "_f_end_ms", "event_fight_id")
    )

    return (
        with_fight
        .join(
            actor_lookup,
            (with_fight.report_code == actor_lookup._a_report_code)
            & (with_fight.actor_id == actor_lookup._a_actor_id),
            "left",
        )
        .drop("_a_report_code", "_a_actor_id")
        .filter(F.col("player_name").isNotNull())
        .dropDuplicates(["report_code", "fight_id", "cast_timestamp_ms", "actor_id", "ability_id"])
    )
