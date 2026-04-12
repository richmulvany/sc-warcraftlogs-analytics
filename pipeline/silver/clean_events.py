# Databricks notebook source
# Silver layer — parsed WCL fight death events
#
# silver_player_deaths — one row per death event per player per report.
#
# Note: The WCL table(dataType: Deaths) endpoint aggregates deaths across ALL
# requested fights in a single call.  There is no per-fight attribution
# available from this endpoint — fight_id is not present in the death entries.
# The fight_ids column on the bronze record records which fights were covered,
# but individual deaths cannot be assigned to specific fight IDs.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Schema for the table(dataType: Deaths) JSON scalar ────────────────────────
# WCL response structure:
# {
#   "data": {
#     "entries": [
#       {
#         "id": 12,
#         "name": "PlayerName",
#         "type": "Warrior",
#         "deaths": 2,
#         "deathEvents": [
#           {
#             "timestamp": 123456,
#             "killingBlow": {"name": "Ability Name", "id": 123}
#           }
#         ]
#       }
#     ]
#   }
# }

_KILLING_BLOW_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("id", LongType(), True),
])

_DEATH_EVENT_STRUCT = StructType([
    StructField("timestamp", LongType(), True),
    StructField("killingBlow", _KILLING_BLOW_STRUCT, True),
])

_ENTRY_STRUCT = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("deaths", LongType(), True),
    StructField("deathEvents", ArrayType(_DEATH_EVENT_STRUCT), True),
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
        "One row per death event with killing blow details. "
        "Note: fight_id is NOT available — deaths are aggregated across all boss "
        "fights in the report by the WCL table API."
    ),
    table_properties={"quality": "silver"},
)
def silver_player_deaths():
    # Read as batch — death records are per-report and stable once ingested.
    raw = dlt.read("bronze_fight_deaths")

    parsed = (
        raw
        .withColumn("parsed", F.from_json(F.col("table_json"), _TABLE_SCHEMA))
        # Drop rows where JSON parsing failed (malformed or unexpected structure)
        .filter(F.col("parsed").isNotNull())
        # Explode entries: one row per player
        .withColumn("entry", F.explode("parsed.data.entries"))
        # deathEvents may be null or empty — coerce to empty array before explode
        .withColumn(
            "death_events_safe",
            F.coalesce(F.col("entry.deathEvents"), F.array()),
        )
        # Explode death events: one row per death
        .withColumn("death_event", F.explode("death_events_safe"))
    )

    return (
        parsed
        .select(
            F.col("report_code"),
            F.col("entry.name").alias("player_name"),
            F.col("entry.type").alias("player_class"),
            F.col("death_event.timestamp").alias("death_timestamp_ms"),
            F.col("death_event.killingBlow.name").alias("killing_blow_name"),
            F.col("death_event.killingBlow.id").alias("killing_blow_id"),
        )
        .filter(F.col("player_name").isNotNull())
    )
