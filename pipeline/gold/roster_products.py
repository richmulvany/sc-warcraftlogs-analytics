# Databricks notebook source
# Gold layer — roster and player profile data products
#
# gold_guild_roster     — authoritative guild roster from Blizzard API
# gold_raid_team        — active raid team with attendance and alt detection
# gold_player_profile   — comprehensive per-player identity + performance summary

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Guild Roster ───────────────────────────────────────────────────────────────
# The authoritative guild roster sourced from the Blizzard Profile API via
# dim_guild_member.  Shows all members at all ranks.
#
# NOTE: If Blizzard API credentials are not configured, dim_guild_member will be
# empty (the ingestion job skips the Blizzard section) and this table will also
# be empty.  Configure blizzard_client_id and blizzard_client_secret in the
# "warcraftlogs" Databricks Secret Scope to populate this table.

@dlt.table(
    name="gold_guild_roster",
    comment=(
        "Authoritative guild roster from Blizzard API enriched with WCL attendance. "
        "All members at all ranks. Empty if Blizzard API is not configured."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "name",
    },
)
def gold_guild_roster():
    members = dlt.read("dim_guild_member")
    return (
        members
        .select(
            F.col("name"),
            F.col("class_name").alias("player_class"),
            F.col("realm_slug").alias("realm"),
            "rank",
            "rank_label",
            "rank_category",
            "is_raid_team",
            "is_active",
            "total_raids_tracked",
            "raids_present",
            "attendance_rate_pct",
            "last_raid_date",
            "first_raid_date",
        )
        .orderBy("rank", "name")
    )


# ── Raid Team ──────────────────────────────────────────────────────────────────
# Active raid team members (rank IN 0,1,2,3) with recent attendance breakdown
# and simple alt detection.
#
# Alt detection logic:
# Players who appear in WCL logs (dim_player) but are NOT in the guild roster
# (is_guild_member = false) are cross-referenced against raid team member name
# prefixes.  A non-member is flagged as possible_alt when their lowercase name
# starts with the first 5 characters of a raid team member's lowercase name,
# AND the raid team member name is at least 5 characters long.
# This catches common alt naming patterns (e.g. "Rahmiel" → "Rahmieldk").

@dlt.table(
    name="gold_raid_team",
    comment=(
        "Active raid team (ranks 0-3) with attendance stats and possible alt flags. "
        "Includes alt detection for non-guild players found in WCL logs."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "name",
    },
)
def gold_raid_team():
    members = dlt.read("dim_guild_member")
    players = dlt.read("dim_player")

    # Active raid team members only
    raid_team = members.filter(F.col("is_raid_team") == True)  # noqa: E712

    # Non-guild players who appeared in WCL logs
    non_members = (
        players
        .filter(F.col("is_guild_member") == False)  # noqa: E712
        .select(
            F.col("player_name"),
            F.lower(F.col("player_name")).alias("_name_lower"),
        )
    )

    # Build a list of raid team name prefixes (first 5 chars of lowercase name,
    # only for members whose name is at least 5 characters long)
    raid_team_prefixes = (
        raid_team
        .filter(F.length("name") >= 5)
        .select(
            F.lower(F.col("name")).substr(1, 5).alias("_prefix"),
        )
        .distinct()
    )

    # Cross-join non-members with raid team prefixes to find possible alts.
    # A non-member player is a possible alt of a raid team member if their
    # lowercase name starts with the first 5 chars of the raid team member's name.
    # We then join back to the raid team to flag members who have a possible alt.
    # A non-member is a possible alt if the first 5 chars of their lowercase name
    # match the prefix of a raid team member.  Column.startswith() only accepts
    # string literals in PySpark, so we compare substr(1,5) directly instead.
    possible_alts = (
        non_members
        .crossJoin(raid_team_prefixes)
        .filter(F.col("_name_lower").substr(1, 5) == F.col("_prefix"))
        .select(F.col("_prefix").alias("_matched_prefix"))
        .distinct()
    )

    # Re-join to raid team to mark members whose name prefix matched a non-member
    raid_team_with_prefix = (
        raid_team
        .withColumn("_rt_prefix", F.lower(F.col("name")).substr(1, 5))
    )

    raid_team_flagged = (
        raid_team_with_prefix
        .join(
            possible_alts.withColumnRenamed("_matched_prefix", "_alt_prefix"),
            raid_team_with_prefix._rt_prefix == F.col("_alt_prefix"),
            "left",
        )
        .withColumn("has_possible_alt_in_logs", F.col("_alt_prefix").isNotNull())
        .drop("_rt_prefix", "_alt_prefix")
    )

    return (
        raid_team_flagged
        .select(
            "name",
            F.col("class_name").alias("player_class"),
            F.col("realm_slug").alias("realm"),
            "rank",
            "rank_label",
            "rank_category",
            "is_active",
            "total_raids_tracked",
            "raids_present",
            "attendance_rate_pct",
            "last_raid_date",
            "first_raid_date",
            "possible_main",
            "has_possible_alt_in_logs",
        )
        .orderBy("rank", "name")
    )


# ── Player Profile ─────────────────────────────────────────────────────────────
# Comprehensive per-player summary combining identity, rank, performance
# aggregations, and attendance.  One row per player across all logs.

@dlt.table(
    name="gold_player_profile",
    comment=(
        "Comprehensive per-player profile combining identity, guild rank, "
        "WCL performance aggregates, and attendance statistics."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "player_name",
    },
)
def gold_player_profile():
    players = dlt.read("dim_player")
    perf_facts = dlt.read("fact_player_fight_performance")
    guild_members = dlt.read("dim_guild_member")

    # Performance aggregations per player across all kill fights
    w_spec = Window.partitionBy("player_name").orderBy(F.col("spec_count").desc())
    w_role = Window.partitionBy("player_name").orderBy(F.col("role_count").desc())

    spec_counts = (
        perf_facts
        .filter(F.col("spec").isNotNull())
        .groupBy("player_name", "spec")
        .agg(F.count("*").alias("spec_count"))
    )
    primary_specs = (
        spec_counts
        .withColumn("_rn", F.row_number().over(w_spec))
        .filter(F.col("_rn") == 1)
        .select("player_name", F.col("spec").alias("primary_spec"))
    )

    role_counts = (
        perf_facts
        .filter(F.col("role").isNotNull())
        .groupBy("player_name", "role")
        .agg(F.count("*").alias("role_count"))
    )
    primary_roles = (
        role_counts
        .withColumn("_rn", F.row_number().over(w_role))
        .filter(F.col("_rn") == 1)
        .select("player_name", F.col("role").alias("primary_role"))
    )

    perf_agg = (
        perf_facts
        .groupBy("player_name")
        .agg(
            F.count("*").alias("kills_tracked"),
            F.avg("throughput_per_second").cast("long").alias("avg_throughput_per_second"),
            F.avg("rank_percent").alias("avg_rank_percent"),
            F.avg("bracket_percent").alias("avg_bracket_percent"),
            F.max("rank_percent").alias("best_rank_percent"),
            F.avg("avg_item_level").alias("avg_item_level"),
            F.avg("active_time_pct").alias("avg_active_time_pct"),
            F.collect_set("zone_name").alias("zones_raided"),
            F.min("raid_night_date").alias("first_kill_date"),
            F.max("raid_night_date").alias("latest_kill_date"),
        )
    )

    # Attendance from guild member dimension (covers non-guild players too via dim_player join)
    guild_att = (
        guild_members
        .select(
            F.col("name").alias("_gm_name"),
            "total_raids_tracked",
            "raids_present",
            "attendance_rate_pct",
        )
    )

    return (
        players
        .join(perf_agg, "player_name", "left")
        .join(primary_specs, "player_name", "left")
        .join(primary_roles, "player_name", "left")
        .join(
            guild_att,
            F.lower(F.col("player_name")) == F.lower(F.col("_gm_name")),
            "left",
        )
        .drop("_gm_name")
        .withColumn("total_raids_tracked", F.coalesce(F.col("total_raids_tracked"), F.lit(0)))
        .withColumn("raids_present", F.coalesce(F.col("raids_present"), F.lit(0)))
        .withColumn(
            "attendance_rate_pct",
            F.coalesce(F.col("attendance_rate_pct"), F.lit(0.0)),
        )
        .select(
            "player_name",
            "player_class",
            "realm",
            "is_guild_member",
            # rank is included for ordering; NULL for non-guild players
            "rank",
            "rank_label",
            "rank_category",
            "is_raid_team",
            "primary_role",
            "primary_spec",
            F.coalesce(F.col("kills_tracked"), F.lit(0)).alias("kills_tracked"),
            "avg_throughput_per_second",
            F.round("avg_rank_percent", 1).alias("avg_rank_percent"),
            F.round("avg_bracket_percent", 1).alias("avg_bracket_percent"),
            F.round("best_rank_percent", 1).alias("best_rank_percent"),
            F.round("avg_item_level", 1).alias("avg_item_level"),
            F.round("avg_active_time_pct", 1).alias("avg_active_time_pct"),
            "zones_raided",
            "first_kill_date",
            "latest_kill_date",
            "total_raids_tracked",
            "raids_present",
            F.round("attendance_rate_pct", 1).alias("attendance_rate_pct"),
        )
        .orderBy(
            F.col("is_raid_team").desc(),
            F.col("rank").asc_nulls_last(),
            F.col("player_name").asc(),
        )
    )
