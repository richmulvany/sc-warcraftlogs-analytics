# Databricks notebook source
# Copies the existing shared landing volume into the source-matched bronze
# landing volumes so ingestion does not need to be rerun to repopulate them.
#
# Run this once in Databricks before switching the bronze pipeline notebooks to
# the new landing paths, or rerun it to backfill any missed subdirectories.

# COMMAND ----------
COPY_MAP = {
    "/Volumes/04_sdp/warcraftlogs/landing/guild_reports": "/Volumes/01_bronze/warcraftlogs/landing/guild_reports",
    "/Volumes/04_sdp/warcraftlogs/landing/report_fights": "/Volumes/01_bronze/warcraftlogs/landing/report_fights",
    "/Volumes/04_sdp/warcraftlogs/landing/raid_attendance": "/Volumes/01_bronze/warcraftlogs/landing/raid_attendance",
    "/Volumes/04_sdp/warcraftlogs/landing/actor_roster": "/Volumes/01_bronze/warcraftlogs/landing/actor_roster",
    "/Volumes/04_sdp/warcraftlogs/landing/player_details": "/Volumes/01_bronze/warcraftlogs/landing/player_details",
    "/Volumes/04_sdp/warcraftlogs/landing/zone_catalog": "/Volumes/01_bronze/warcraftlogs/landing/zone_catalog",
    "/Volumes/04_sdp/warcraftlogs/landing/fight_rankings": "/Volumes/01_bronze/warcraftlogs/landing/fight_rankings",
    "/Volumes/04_sdp/warcraftlogs/landing/fight_deaths": "/Volumes/01_bronze/warcraftlogs/landing/fight_deaths",
    "/Volumes/04_sdp/warcraftlogs/landing/fight_casts": "/Volumes/01_bronze/warcraftlogs/landing/fight_casts",
    "/Volumes/04_sdp/warcraftlogs/landing/guild_zone_ranks": "/Volumes/01_bronze/warcraftlogs/landing/guild_zone_ranks",
    "/Volumes/04_sdp/warcraftlogs/landing/archived": "/Volumes/01_bronze/warcraftlogs/landing/archived",
    "/Volumes/04_sdp/warcraftlogs/landing/guild_members": "/Volumes/01_bronze/blizzard/landing/guild_members",
    "/Volumes/04_sdp/warcraftlogs/landing/character_media": "/Volumes/01_bronze/blizzard/landing/character_media",
    "/Volumes/04_sdp/warcraftlogs/landing/character_equipment": "/Volumes/01_bronze/blizzard/landing/character_equipment",
    "/Volumes/04_sdp/warcraftlogs/landing/character_achievements": "/Volumes/01_bronze/blizzard/landing/character_achievements",
    "/Volumes/04_sdp/warcraftlogs/landing/item_media": "/Volumes/01_bronze/blizzard/landing/item_media",
    "/Volumes/04_sdp/warcraftlogs/landing/raiderio_character_profiles": "/Volumes/01_bronze/raiderio/landing/raiderio_character_profiles",
    "/Volumes/04_sdp/warcraftlogs/landing/live_raid_roster": "/Volumes/01_bronze/google_sheets/landing/live_raid_roster",
}

for volume_path in (
    "`01_bronze`.`warcraftlogs`.landing",
    "`01_bronze`.`blizzard`.landing",
    "`01_bronze`.`raiderio`.landing",
    "`01_bronze`.`google_sheets`.landing",
):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")  # noqa: F821


def ensure_dir(path: str) -> None:
    dbutils.fs.mkdirs(path)  # noqa: F821


results = []
for src, dst in COPY_MAP.items():
    ensure_dir(dst)
    try:
        copied = dbutils.fs.cp(src, dst, recurse=True)  # noqa: F821
        results.append((src, dst, copied, "ok"))
    except Exception as exc:
        results.append((src, dst, False, str(exc)))

display(spark.createDataFrame(results, ["src", "dst", "copied", "status"]))  # noqa: F821

# COMMAND ----------
# After the copy succeeds, deploy the bundle and run the DLT pipeline as a
# FULL REFRESH so the bronze streaming tables rebuild from the new source paths.
