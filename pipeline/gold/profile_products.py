# Databricks notebook source
# Gold layer — flattened character profile data products
#
# These tables flatten the JSON payloads stored in the Blizzard / WCL / Google
# Sheets silver tables so the frontend CSV exports can be plain SELECT * reads
# from gold.  No transformation logic should remain in the export script.
#
#   gold_live_raid_roster          — pass-through of silver_live_raid_roster.
#   gold_guild_zone_ranks          — flattens progress_json → world/region/server rank.
#   gold_player_character_media    — pass-through of silver_character_media.
#   gold_player_character_equipment— explodes equipped_items[] and joins silver_item_media
#                                    for icon URLs.  Re-serialises enchantments/sockets/
#                                    stats/spells as compact JSON strings to match the
#                                    legacy CSV schema.
#   gold_player_raid_achievements  — explodes achievements_json filtered to raid feats
#                                    (Cutting Edge / Famed Slayer / Famed Bane).

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


def _profile_player_identity_key() -> F.Column:
    return F.concat_ws(
        ":",
        F.lower(F.trim(F.col("player_name"))),
        F.lit("unknown"),
        F.lower(F.trim(F.coalesce(F.col("realm_slug"), F.lit("unknown")))),
    )


# ── gold_live_raid_roster ──────────────────────────────────────────────────────


@dlt.table(
    name="03_gold.sc_analytics.gold_live_raid_roster",
    comment="Active raid roster from the live Google Sheets export (pass-through of silver).",
    table_properties={"quality": "gold"},
)
def gold_live_raid_roster():
    return spark.read.table("02_silver.sc_analytics_google_sheets.silver_live_raid_roster").select(  # noqa: F821
        "name",
        "roster_rank",
        "player_class",
        "race",
        "note",
        "source_refreshed_at",
    )


# ── gold_guild_zone_ranks ──────────────────────────────────────────────────────

_RANK_STRUCT = StructType([StructField("number", LongType(), True)])

_PROGRESS_SCHEMA = StructType(
    [
        StructField("worldRank", _RANK_STRUCT, True),
        StructField("regionRank", _RANK_STRUCT, True),
        StructField("serverRank", _RANK_STRUCT, True),
    ]
)


@dlt.table(
    name="03_gold.sc_analytics.gold_guild_zone_ranks",
    comment="Guild progression rank per raid zone (world/region/server).",
    table_properties={"quality": "gold"},
)
def gold_guild_zone_ranks():
    raw = spark.read.table("02_silver.sc_analytics_warcraftlogs.silver_guild_zone_ranks")  # noqa: F821
    parsed = raw.withColumn("p", F.from_json("progress_json", _PROGRESS_SCHEMA))
    return parsed.select(
        "zone_id",
        "zone_name",
        F.col("p.worldRank.number").alias("world_rank"),
        F.col("p.regionRank.number").alias("region_rank"),
        F.col("p.serverRank.number").alias("server_rank"),
    )


# ── gold_player_character_media ────────────────────────────────────────────────


@dlt.table(
    name="03_gold.sc_analytics.gold_player_character_media",
    comment="Per-character avatar / inset / main artwork URLs from the Blizzard media payload.",
    table_properties={"quality": "gold"},
)
def gold_player_character_media():
    return spark.read.table("02_silver.sc_analytics_blizzard.silver_character_media").select(  # noqa: F821
        _profile_player_identity_key().alias("player_identity_key"),
        "player_name",
        "realm_slug",
        "avatar_url",
        "inset_url",
        "main_url",
        "main_raw_url",
    )


# ── gold_player_character_equipment ────────────────────────────────────────────

_NAMED_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

_ITEM_REF_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

_LEVEL_STRUCT = StructType([StructField("value", LongType(), True)])

_QUALITY_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

_TRANSMOG_STRUCT = StructType(
    [
        StructField("item", _ITEM_REF_STRUCT, True),
    ]
)

_ENCHANT_STRUCT = StructType(
    [
        StructField("display_string", StringType(), True),
        StructField("source_item", _ITEM_REF_STRUCT, True),
        StructField("enchantment_id", LongType(), True),
        StructField("id", LongType(), True),
    ]
)

_SOCKET_STRUCT = StructType(
    [
        StructField("socket_type", _NAMED_STRUCT, True),
        StructField("item", _ITEM_REF_STRUCT, True),
        StructField("display_string", StringType(), True),
    ]
)

_STAT_DISPLAY_STRUCT = StructType(
    [
        StructField("display_string", StringType(), True),
    ]
)

_STAT_STRUCT = StructType(
    [
        StructField("type", _NAMED_STRUCT, True),
        StructField("value", LongType(), True),
        StructField("display", _STAT_DISPLAY_STRUCT, True),
        StructField("is_negated", BooleanType(), True),
    ]
)

_SPELL_REF_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

_SPELL_STRUCT = StructType(
    [
        StructField("spell", _SPELL_REF_STRUCT, True),
        StructField("description", StringType(), True),
    ]
)

_NAME_DESC_STRUCT = StructType(
    [
        StructField("display_string", StringType(), True),
    ]
)

_REQUIREMENTS_STRUCT = StructType(
    [
        StructField("level", StructType([StructField("display_string", StringType(), True)]), True),
    ]
)

_DURABILITY_STRUCT = StructType(
    [
        StructField("display_string", StringType(), True),
        StructField("value", LongType(), True),
    ]
)

_LIMIT_CATEGORY_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
    ]
)

_EQUIPPED_ITEM_STRUCT = StructType(
    [
        StructField("slot", _NAMED_STRUCT, True),
        StructField("item", _ITEM_REF_STRUCT, True),
        StructField("name", StringType(), True),
        StructField("level", _LEVEL_STRUCT, True),
        StructField("quality", _QUALITY_STRUCT, True),
        StructField("inventory_type", _NAMED_STRUCT, True),
        StructField("item_subclass", _NAMED_STRUCT, True),
        StructField("binding", _NAMED_STRUCT, True),
        StructField("transmog", _TRANSMOG_STRUCT, True),
        StructField("enchantments", ArrayType(_ENCHANT_STRUCT), True),
        StructField("sockets", ArrayType(_SOCKET_STRUCT), True),
        StructField("stats", ArrayType(_STAT_STRUCT), True),
        StructField("spells", ArrayType(_SPELL_STRUCT), True),
        StructField("name_description", _NAME_DESC_STRUCT, True),
        StructField("requirements", _REQUIREMENTS_STRUCT, True),
        StructField("durability", _DURABILITY_STRUCT, True),
        StructField("limit_category", _LIMIT_CATEGORY_STRUCT, True),
    ]
)

_EQUIPMENT_FULL_SCHEMA = StructType(
    [
        StructField("equipped_items", ArrayType(_EQUIPPED_ITEM_STRUCT), True),
    ]
)


@dlt.table(
    name="03_gold.sc_analytics.gold_player_character_equipment",
    comment=(
        "Per-equipped-slot item details from the Blizzard equipment payload, joined "
        "to silver_item_media for icon URLs.  Sub-arrays (enchantments / sockets / "
        "stats / spells) are re-serialised as compact JSON strings."
    ),
    table_properties={"quality": "gold"},
)
def gold_player_character_equipment():
    raw = spark.read.table("02_silver.sc_analytics_blizzard.silver_character_equipment")  # noqa: F821
    item_media = spark.read.table("02_silver.sc_analytics_blizzard.silver_item_media").select(  # noqa: F821
        "item_id", "icon_url"
    )

    parsed = raw.withColumn("eq", F.from_json("equipment_json", _EQUIPMENT_FULL_SCHEMA))
    exploded = parsed.select(
        _profile_player_identity_key().alias("player_identity_key"),
        "player_name",
        "realm_slug",
        F.explode("eq.equipped_items").alias("it"),
    )

    enchant_items = F.expr(
        "TRANSFORM(it.enchantments, e -> named_struct("
        "'display_string', COALESCE(e.display_string, ''),"
        "'source_item_name', COALESCE(e.source_item.name, ''),"
        "'enchantment_id', COALESCE(e.enchantment_id, e.id)"
        "))"
    )

    socket_items = F.expr(
        "TRANSFORM(it.sockets, s -> named_struct("
        "'socket_type', COALESCE(s.socket_type.name, ''),"
        "'item_id', s.item.id,"
        "'item_name', COALESCE(s.item.name, ''),"
        "'display_string', COALESCE(s.display_string, '')"
        "))"
    )

    stat_items = F.expr(
        "TRANSFORM(it.stats, s -> named_struct("
        "'type', COALESCE(s.type.name, ''),"
        "'value', s.value,"
        "'display', COALESCE(s.display.display_string, ''),"
        "'is_negated', COALESCE(s.is_negated, false)"
        "))"
    )

    spell_items = F.expr(
        "TRANSFORM(it.spells, sp -> named_struct("
        "'spell_id', sp.spell.id,"
        "'spell_name', COALESCE(sp.spell.name, ''),"
        "'description', COALESCE(sp.description, '')"
        "))"
    )

    raw_details = F.expr(
        "named_struct("
        "'name_description', COALESCE(it.name_description.display_string, ''),"
        "'requirements_level', COALESCE(it.requirements.level.display_string, ''),"
        "'durability_display', COALESCE(it.durability.display_string, ''),"
        "'durability_value', it.durability.value,"
        "'limit_category', COALESCE(it.limit_category.name, '')"
        ")"
    )

    flattened = exploded.select(
        "player_identity_key",
        "player_name",
        "realm_slug",
        F.col("it.slot.type").alias("slot_type"),
        F.col("it.slot.name").alias("slot_name"),
        F.col("it.item.id").alias("item_id"),
        F.col("it.name").alias("item_name"),
        F.col("it.quality.name").alias("quality"),
        F.col("it.level.value").alias("item_level"),
        F.col("it.inventory_type.name").alias("inventory_type"),
        F.col("it.item_subclass.name").alias("item_subclass"),
        F.col("it.binding.name").alias("binding"),
        F.col("it.transmog.item.name").alias("transmog_name"),
        F.to_json(enchant_items).alias("enchantments_json"),
        F.to_json(socket_items).alias("sockets_json"),
        F.to_json(stat_items).alias("stats_json"),
        F.to_json(spell_items).alias("spells_json"),
        F.to_json(raw_details).alias("raw_details_json"),
    )

    return (
        flattened.alias("e")
        .join(item_media.alias("m"), F.col("e.item_id") == F.col("m.item_id"), "left")
        .select(
            "e.player_name",
            "e.player_identity_key",
            "e.realm_slug",
            "e.slot_type",
            "e.slot_name",
            "e.item_id",
            "e.item_name",
            F.coalesce(F.col("m.icon_url"), F.lit("")).alias("icon_url"),
            "e.quality",
            "e.item_level",
            "e.inventory_type",
            "e.item_subclass",
            "e.binding",
            "e.transmog_name",
            "e.enchantments_json",
            "e.sockets_json",
            "e.stats_json",
            "e.spells_json",
            "e.raw_details_json",
        )
    )


# ── gold_player_raid_achievements ──────────────────────────────────────────────

_ACHIEVEMENT_INFO_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

_CRITERIA_STRUCT = StructType(
    [
        StructField("is_completed", BooleanType(), True),
    ]
)

_ACHIEVEMENT_ROW_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("achievement", _ACHIEVEMENT_INFO_STRUCT, True),
        StructField("criteria", _CRITERIA_STRUCT, True),
        StructField("completed_timestamp", LongType(), True),
    ]
)

_ACHIEVEMENTS_SCHEMA = StructType(
    [
        StructField("achievements", ArrayType(_ACHIEVEMENT_ROW_STRUCT), True),
    ]
)


@dlt.table(
    name="03_gold.sc_analytics.gold_player_raid_achievements",
    comment=(
        "Per-character raid feat-of-strength achievements (Cutting Edge / Famed "
        "Slayer / Famed Bane).  Filtered to completed achievements only."
    ),
    table_properties={"quality": "gold"},
)
def gold_player_raid_achievements():
    raw = spark.read.table("02_silver.sc_analytics_blizzard.silver_character_achievements")  # noqa: F821
    parsed = raw.withColumn("a", F.from_json("achievements_json", _ACHIEVEMENTS_SCHEMA))
    exploded = parsed.select(
        _profile_player_identity_key().alias("player_identity_key"),
        "player_name",
        "realm_slug",
        F.explode("a.achievements").alias("row"),
    )
    return (
        exploded.withColumn(
            "achievement_id",
            F.coalesce(F.col("row.achievement.id"), F.col("row.id")),
        )
        .withColumn("achievement_name", F.col("row.achievement.name"))
        .withColumn("completed_timestamp", F.col("row.completed_timestamp"))
        .withColumn("is_completed", F.col("row.criteria.is_completed"))
        .filter((F.col("is_completed") == F.lit(True)) | F.col("completed_timestamp").isNotNull())
        .filter(F.lower(F.col("achievement_name")).rlike(r"cutting edge:|famed slayer|famed bane"))
        .select(
            "player_identity_key",
            "player_name",
            "realm_slug",
            "achievement_id",
            "achievement_name",
            "completed_timestamp",
        )
    )
