"""Shared landing-path constants for ingestion jobs."""

LANDING_ROOTS = {
    "warcraftlogs": "/Volumes/01_bronze/warcraftlogs/landing",
    "blizzard": "/Volumes/01_bronze/blizzard/landing",
    "raiderio": "/Volumes/01_bronze/raiderio/landing",
    "google_sheets": "/Volumes/01_bronze/google_sheets/landing",
}

SOURCE_SUBDIRS = {
    "warcraftlogs": (
        "guild_reports",
        "report_fights",
        "raid_attendance",
        "actor_roster",
        "player_details",
        "zone_catalog",
        "fight_rankings",
        "fight_deaths",
        "fight_casts",
        "guild_zone_ranks",
        "archived",
    ),
    "blizzard": (
        "guild_members",
        "character_media",
        "character_equipment",
        "character_achievements",
        "item_media",
    ),
    "raiderio": ("raiderio_character_profiles",),
    "google_sheets": ("live_raid_roster",),
}
