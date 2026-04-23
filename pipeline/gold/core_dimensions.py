# Databricks notebook source
# Gold layer — core dimension tables
#
# dim_player       — canonical player identity across all logs, enriched with
#                    guild membership and rank from silver_guild_members
# dim_guild_member — full guild roster from Blizzard API, enriched with
#                    attendance stats and activity flag

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Player Dimension ───────────────────────────────────────────────────────────
# Canonical player identity table combining actor roster (class, realm) and
# attendance data (first/last seen dates).
# Guild membership and rank are joined in from silver_guild_members using a
# case-insensitive name match to handle capitalisation differences.

@dlt.table(
    name="dim_player",
    comment=(
        "Canonical player identity across all WCL logs. "
        "One row per player name with class, realm, guild membership, and rank."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def dim_player():
    actors = dlt.read("silver_actor_roster")
    attendance = dlt.read("silver_raid_attendance")
    guild_members = dlt.read("silver_guild_members")
    perf = dlt.read("silver_player_performance")

    # Most recent class + realm snapshot per player from actor logs
    w_actor = Window.partitionBy("player_name").orderBy(F.col("_ingested_at").desc())
    latest_actor = (
        actors
        .filter(F.col("player_name").isNotNull() & (F.col("player_name") != ""))
        .withColumn("_rn", F.row_number().over(w_actor))
        .filter(F.col("_rn") == 1)
        .select("player_name", "player_class", "realm")
    )

    # Fallback class source: silver_player_performance reliably has player.type (WoW class).
    # Use this to fill in player_class when actor roster has null/blank class.
    w_perf = Window.partitionBy("player_name").orderBy(F.col("_ingested_at").desc())
    perf_class = (
        perf
        .filter(F.col("player_name").isNotNull() & (F.col("player_name") != ""))
        .filter(F.col("player_class").isNotNull() & (F.col("player_class") != ""))
        .withColumn("_rn", F.row_number().over(w_perf))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("player_name").alias("_pc_player_name"),
            F.col("player_class").alias("_pc_player_class"),
        )
    )

    # All distinct player names from attendance (catches players not in actor roster)
    att_players = (
        attendance
        .filter(F.col("player_name").isNotNull() & (F.col("player_name") != ""))
        .select("player_name")
        .distinct()
    )

    # Attendance date range per player
    att_dates = (
        attendance
        .groupBy("player_name")
        .agg(
            F.min("raid_night_date").alias("first_seen_date"),
            F.max("raid_night_date").alias("last_seen_date"),
        )
    )

    # Union actor roster names with attendance-only names, then join actor info
    all_players = (
        latest_actor
        .select("player_name")
        .union(att_players)
        .distinct()
        # Exclude blank names that can slip in through attendance data
        .filter(F.col("player_name").isNotNull() & (F.col("player_name") != ""))
    )

    # Bring in actor class/realm for all players (null for attendance-only)
    players_with_actor = (
        all_players
        .join(latest_actor, "player_name", "left")
        # Fill missing player_class from performance data (more consistently populated)
        .join(perf_class, F.col("player_name") == perf_class._pc_player_name, "left")
        .withColumn(
            "player_class",
            F.coalesce(F.col("player_class"), F.col("_pc_player_class")),
        )
        .drop("_pc_player_name", "_pc_player_class")
    )

    # Join attendance date range
    players_with_dates = (
        players_with_actor
        .join(att_dates, "player_name", "left")
    )

    # Join guild membership using case-insensitive name matching
    guild_enriched = (
        guild_members
        .select(
            F.lower("name").alias("_member_lower"),
            F.col("name").alias("_member_name"),
            F.col("rank"),
            F.col("rank_label"),
            F.col("rank_category"),
            F.col("is_raid_team"),
        )
    )

    return (
        players_with_dates
        .join(
            guild_enriched,
            F.lower(F.col("player_name")) == F.col("_member_lower"),
            "left",
        )
        .withColumn("is_guild_member", F.col("_member_name").isNotNull())
        .withColumn("is_raid_team", F.coalesce(F.col("is_raid_team"), F.lit(False)))  # ranks 0-5,8
        .drop("_member_lower", "_member_name")
        .select(
            "player_name",
            "player_class",
            "realm",
            "is_guild_member",
            "rank",
            "rank_label",
            "rank_category",
            "is_raid_team",
            "first_seen_date",
            "last_seen_date",
        )
        .dropDuplicates(["player_name"])
        .orderBy("player_name")
    )


# ── Guild Member Dimension ─────────────────────────────────────────────────────
# Authoritative guild roster from Blizzard API enriched with attendance stats.
# is_active = attendance_rate_pct >= 25.0 OR rank IN (0, 1) so GMs and Officers
# are always considered active regardless of recent attendance.

@dlt.table(
    name="dim_guild_member",
    comment=(
        "Guild roster from Blizzard API enriched with WCL attendance statistics. "
        "One row per guild member with activity flag."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "name",
    },
)
def dim_guild_member():
    members = dlt.read("silver_guild_members")
    attendance = dlt.read("silver_raid_attendance")

    # Attendance stats per player (case-insensitive join to handle capitalisation)
    att_stats = (
        attendance
        .groupBy(F.lower("player_name").alias("_player_lower"))
        .agg(
            F.count("*").alias("total_raids_tracked"),
            F.sum(F.when(F.col("presence") == 1, 1).otherwise(0)).alias("raids_present"),
            F.max("raid_night_date").alias("last_raid_date"),
            F.min("raid_night_date").alias("first_raid_date"),
        )
        .withColumn(
            "attendance_rate_pct",
            F.round(
                F.col("raids_present")
                / F.greatest(F.col("total_raids_tracked"), F.lit(1))
                * 100,
                1,
            ),
        )
    )

    return (
        members
        .join(
            att_stats,
            F.lower(F.col("name")) == F.col("_player_lower"),
            "left",
        )
        .drop("_player_lower")
        # Default to zero for members who have never appeared in a WCL log
        .withColumn("total_raids_tracked", F.coalesce(F.col("total_raids_tracked"), F.lit(0)))
        .withColumn("raids_present", F.coalesce(F.col("raids_present"), F.lit(0)))
        .withColumn(
            "attendance_rate_pct",
            F.coalesce(F.col("attendance_rate_pct"), F.lit(0.0)),
        )
        .withColumn(
            "is_active",
            (F.col("attendance_rate_pct") >= 25.0) | F.col("rank").isin(0, 1, 2),
        )
        # possible_main is the character's own name; alt resolution is done in gold_raid_team
        .withColumn("possible_main", F.col("name"))
        .select(
            "name",
            "realm_slug",
            "rank",
            "rank_label",
            "rank_category",
            "is_raid_team",
            "class_id",
            "class_name",
            "level",
            "total_raids_tracked",
            "raids_present",
            "attendance_rate_pct",
            "last_raid_date",
            "first_raid_date",
            "is_active",
            "possible_main",
            "_ingested_at",
        )
        .orderBy("rank", "name")
    )
