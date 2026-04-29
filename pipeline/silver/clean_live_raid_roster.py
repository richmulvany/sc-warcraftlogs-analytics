# Databricks notebook source
# Silver layer — live raid roster from Google Sheets
#
# silver_live_raid_roster — one row per active roster member parsed from the
# raw csv_text in bronze.  Mirrors the column layout that
# scripts/export_gold_tables.py used to produce live_raid_roster.csv.

import csv
import io

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

_NAME_IDX = 0
_RANK_IDX = 3
_CLASS_IDX = 5
_RACE_IDX = 119
_NOTE_IDX = 120


_ROSTER_ROW_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("roster_rank", StringType(), True),
        StructField("player_class", StringType(), True),
        StructField("race", StringType(), True),
        StructField("note", StringType(), True),
        StructField("source_refreshed_at", StringType(), True),
    ]
)


def _parse_roster_csv(csv_text: str | None):
    if not csv_text:
        return []
    rows = list(csv.reader(io.StringIO(csv_text)))
    if len(rows) < 3:
        return []
    refreshed_at = rows[1][1].strip() if len(rows[1]) > 1 else ""
    out = []
    seen: set[str] = set()
    for row in rows[2:]:
        if not row:
            continue
        name = row[_NAME_IDX].strip() if len(row) > _NAME_IDX else ""
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            (
                name,
                row[_RANK_IDX].strip() if len(row) > _RANK_IDX else "",
                row[_CLASS_IDX].strip() if len(row) > _CLASS_IDX else "",
                row[_RACE_IDX].strip() if len(row) > _RACE_IDX else "",
                row[_NOTE_IDX].strip() if len(row) > _NOTE_IDX else "",
                refreshed_at,
            )
        )
    return out


_parse_roster_udf = F.udf(_parse_roster_csv, ArrayType(_ROSTER_ROW_SCHEMA))


@dlt.table(
    name="02_silver.sc_analytics_google_sheets.silver_live_raid_roster",
    comment="Active raid roster parsed from the live Google Sheets CSV export.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_name", "name IS NOT NULL AND length(name) > 0")
def silver_live_raid_roster():
    raw = spark.read.table("01_bronze.google_sheets.bronze_live_raid_roster")  # noqa: F821
    w = Window.partitionBy("sheet_id", "gid").orderBy(F.col("_ingested_at").desc())
    latest = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return (
        latest.withColumn("rows", _parse_roster_udf(F.col("csv_text")))
        .withColumn("row", F.explode("rows"))
        .select(
            F.col("row.name").alias("name"),
            F.col("row.roster_rank").alias("roster_rank"),
            F.col("row.player_class").alias("player_class"),
            F.col("row.race").alias("race"),
            F.col("row.note").alias("note"),
            F.col("row.source_refreshed_at").alias("source_refreshed_at"),
            F.to_timestamp("_ingested_at").alias("snapshot_at"),
            "_ingested_at",
        )
    )
