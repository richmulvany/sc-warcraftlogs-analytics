# Databricks notebook source
# ruff: noqa: E402, I001
# Silver layer — parsed WCL fight rankings
#
# silver_player_rankings — one row per player per kill fight with WCL parse
#                          percentiles, role-aware.  Bronze persists two
#                          payloads per report:
#
#   * rankings_json     — WCL rankings(playerMetric: dps), used for dps/tank rows.
#   * rankings_hps_json — WCL rankings(playerMetric: hps), used for healer rows.
#
# We explode the role buckets from the *correct* payload per role so a healer's
# `amount` and `rankPercent` reflect HPS, not DPS.  Tanks fall back to the dps
# payload because WCL has no first-class tank parse comparator.
#
# WCL rankings(compare: Parses) response structure (per metric):
# {
#   "data": [
#     {
#       "fightID": 1,
#       "encounter": {"id": 2587, "name": "Eranog"},
#       "difficulty": 5,
#       "size": 20,
#       "roles": {
#         "tanks":   {"characters": [{name, class, spec, amount, rankPercent, bracketPercent, rank, ...}]},
#         "healers": {"characters": [...]},
#         "dps":     {"characters": [...]}
#       }
#     }
#   ]
# }
#
# Note: there is NO top-level array — the root is an object with a "data" key.
# Note: there is NO "medal" field — rankings are expressed via rankPercent only.
# Note: "rank" is a string in "~1265" format, not a number.

import dlt
import os
import sys
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def _ensure_repo_root_on_syspath() -> None:
    candidates = [os.getcwd()]

    module_file = globals().get("__file__")
    if module_file:
        candidates.append(os.path.abspath(module_file))

    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()  # noqa: F821
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        candidates.append(notebook_path)
    except Exception:
        pass

    for candidate in candidates:
        current = candidate if os.path.isdir(candidate) else os.path.dirname(candidate)
        while current and current != os.path.dirname(current):
            pipeline_dir = current if os.path.basename(current) == "pipeline" else os.path.join(current, "pipeline")
            if os.path.isfile(os.path.join(pipeline_dir, "__init__.py")):
                repo_root = os.path.dirname(pipeline_dir)
                if repo_root not in sys.path:
                    sys.path.insert(0, repo_root)
                return
            current = os.path.dirname(current)


_ensure_repo_root_on_syspath()

from pipeline.expectations.common_expectations import INGESTED_AT_PRESENT, REPORT_FIGHT_PLAYER_UNIQUE

# ── Schemas ────────────────────────────────────────────────────────────────────

_CHARACTER_STRUCT = StructType([
    StructField("id",             LongType(),   True),
    StructField("name",           StringType(), True),
    StructField("class",          StringType(), True),
    StructField("spec",           StringType(), True),
    StructField("amount",         DoubleType(), True),
    StructField("rankPercent",    DoubleType(), True),
    StructField("bracketPercent", DoubleType(), True),
    StructField("rank",           StringType(), True),   # "~1265" — keep as string
    StructField("totalParses",    LongType(),   True),
])

_ROLE_STRUCT = StructType([
    StructField("name",       StringType(),              True),
    StructField("characters", ArrayType(_CHARACTER_STRUCT), True),
])

_ROLES_STRUCT = StructType([
    StructField("tanks",   _ROLE_STRUCT, True),
    StructField("healers", _ROLE_STRUCT, True),
    StructField("dps",     _ROLE_STRUCT, True),
])

_ENCOUNTER_STRUCT = StructType([
    StructField("id",   LongType(),   True),
    StructField("name", StringType(), True),
])

_FIGHT_ENTRY_STRUCT = StructType([
    StructField("fightID",    LongType(),   True),
    StructField("difficulty", LongType(),   True),
    StructField("zone",       LongType(),   True),
    StructField("encounter",  _ENCOUNTER_STRUCT, True),
    StructField("size",       LongType(),   True),
    StructField("duration",   LongType(),   True),
    StructField("roles",      _ROLES_STRUCT, True),
])

_RANKINGS_SCHEMA = StructType([
    StructField("data", ArrayType(_FIGHT_ENTRY_STRUCT), True),
])


# ── Parsed Player Rankings ─────────────────────────────────────────────────────

@dlt.table(
    name="02_silver.sc_analytics_warcraftlogs.silver_player_rankings",
    comment=(
        "WCL parse rankings per player per kill fight. "
        "One row per player per fight with percentile, spec, and class. "
        "Sourced from rankings(compare: Parses) — roles-based structure."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_ranking_ref", "report_code IS NOT NULL AND fight_id IS NOT NULL")
@dlt.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
@dlt.expect(*REPORT_FIGHT_PLAYER_UNIQUE)
@dlt.expect(*INGESTED_AT_PRESENT)
# Warn-only: tracks WCL parse-rankings completeness over time. Nulls are
# legitimate (unrankable specs, archived/private reports, async compute lag),
# but a sustained spike usually indicates an ingestion or Auto Loader issue.
@dlt.expect("rank_percent_present", "rank_percent IS NOT NULL")
def silver_player_rankings():
    raw = spark.read.table("01_bronze.warcraftlogs.bronze_fight_rankings")  # noqa: F821

    # If the ingestion job re-landed a rankings file (e.g. to backfill rankings
    # WCL hadn't computed yet), bronze can have multiple rows per report_code.
    # Keep only the most recently ingested payload per report.
    latest_window = Window.partitionBy("report_code").orderBy(F.col("_ingested_at").desc_nulls_last())
    raw_latest = (
        raw
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Parse both rankings payloads (DPS metric, HPS metric).  rankings_hps_json
    # may be missing on legacy bronze rows that pre-date the dual-metric fetch;
    # in that case healer rows simply fall through with null parses until those
    # reports are re-ingested.
    has_hps_col = "rankings_hps_json" in raw_latest.columns
    fights_dps = (
        raw_latest
        .withColumn("parsed", F.from_json(F.col("rankings_json"), _RANKINGS_SCHEMA))
        .filter(F.col("parsed").isNotNull())
        .withColumn("fight_entry", F.explode("parsed.data"))
    )
    if has_hps_col:
        fights_hps = (
            raw_latest
            .filter(F.col("rankings_hps_json").isNotNull())
            .withColumn("parsed", F.from_json(F.col("rankings_hps_json"), _RANKINGS_SCHEMA))
            .filter(F.col("parsed").isNotNull())
            .withColumn("fight_entry", F.explode("parsed.data"))
        )
    else:
        fights_hps = None

    # Extract characters from a specific role bucket of a specific payload.
    # Role-payload pairing:
    #   tanks   ← DPS payload (no first-class tank parse comparator in WCL)
    #   dps     ← DPS payload
    #   healers ← HPS payload (so amount/rankPercent are HPS-based)
    def _role_df(fights_df, role_key: str, role_label: str):
        chars_col = f"fight_entry.roles.{role_key}.characters"
        return (
            fights_df
            .filter(F.col(chars_col).isNotNull())
            .filter(F.size(F.col(chars_col)) > 0)
            .select(
                F.col("report_code"),
                F.col("fight_entry.fightID").alias("fight_id"),
                F.col("fight_entry.encounter.id").alias("encounter_id"),
                F.col("fight_entry.encounter.name").alias("encounter_name"),
                F.col("fight_entry.difficulty").alias("difficulty"),
                F.col("fight_entry.size").alias("fight_size"),
                F.col("_ingested_at"),
                F.explode(chars_col).alias("character"),
                F.lit(role_label).alias("role"),
            )
        )

    role_dfs = [
        _role_df(fights_dps, "tanks", "tank"),
        _role_df(fights_dps, "dps", "dps"),
    ]
    if fights_hps is not None:
        role_dfs.append(_role_df(fights_hps, "healers", "healer"))
    all_players = role_dfs[0]
    for df in role_dfs[1:]:
        all_players = all_players.union(df)

    return (
        all_players
        .select(
            F.col("report_code"),
            F.col("fight_id"),
            F.col("encounter_id"),
            F.col("encounter_name"),
            F.col("difficulty"),
            F.col("fight_size"),
            F.col("role"),
            F.col("character.name").alias("player_name"),
            F.col("character.class").alias("player_class"),
            F.col("character.spec").alias("spec"),
            F.col("character.amount").alias("amount"),
            F.col("character.rankPercent").alias("rank_percent"),
            F.col("character.bracketPercent").alias("bracket_percent"),
            F.col("character.rank").alias("rank_string"),   # "~1265" approximate rank
            F.col("character.totalParses").alias("total_parses"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "_duplicate_count",
            F.count(F.lit(1)).over(Window.partitionBy("report_code", "fight_id", "player_name")),
        )
        .filter(F.col("player_name").isNotNull())
        .dropDuplicates(["report_code", "fight_id", "player_name"])
    )
