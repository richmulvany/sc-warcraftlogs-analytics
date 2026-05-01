# Databricks notebook source
# ruff: noqa: I001
# Gold layer — raid preparation products
#
# gold_preparation_readiness — current-tier raid-team readiness metrics and
#                              score semantics for the Preparation dashboard.

import dlt


@dlt.table(
    name="03_gold.sc_analytics.gold_preparation_readiness",
    comment=(
        "Current-tier raid-team preparation readiness. Applies published "
        "preparation identity overrides, computes consumable/attendance rates, "
        "readiness score, label, notes, and weakest signal in Gold."
    ),
    table_properties={"quality": "gold"},
)
def gold_preparation_readiness():
    return spark.sql(  # noqa: F821
        """
        WITH included_raids AS (
          SELECT *
          FROM 03_gold.sc_analytics.gold_raid_summary
          WHERE report_code IS NOT NULL
            AND raid_night_date IS NOT NULL
            AND zone_name IS NOT NULL
            AND zone_name != 'Blackrock Depths'
        ),
        current_tier AS (
          SELECT MAX_BY(zone_name, raid_night_date) AS zone_name
          FROM included_raids
        ),
        current_tier_raids AS (
          SELECT r.*
          FROM included_raids r
          INNER JOIN current_tier t ON r.zone_name = t.zone_name
        ),
        current_tier_raid_dates AS (
          SELECT COUNT(DISTINCT raid_night_date) AS total_raids_tracked
          FROM current_tier_raids
        ),
        live_count AS (
          SELECT COUNT(*) AS row_count
          FROM 03_gold.sc_analytics.gold_live_raid_roster
          WHERE name IS NOT NULL AND name != ''
        ),
        team_members AS (
          SELECT
            l.name,
            COALESCE(NULLIF(l.player_class, ''), 'Unknown') AS player_class,
            COALESCE(NULLIF(l.roster_rank, ''), 'Raider') AS rank_label,
            TRUE AS is_active,
            'live_raid_roster' AS roster_source
          FROM 03_gold.sc_analytics.gold_live_raid_roster l
          CROSS JOIN live_count c
          WHERE c.row_count > 0
            AND l.name IS NOT NULL
            AND l.name != ''

          UNION ALL

          SELECT
            r.name,
            COALESCE(NULLIF(r.player_class, ''), 'Unknown') AS player_class,
            COALESCE(NULLIF(r.rank_label, ''), 'Raider') AS rank_label,
            COALESCE(r.is_active, false) AS is_active,
            'gold_raid_team' AS roster_source
          FROM 03_gold.sc_analytics.gold_raid_team r
          CROSS JOIN live_count c
          WHERE c.row_count = 0
            AND r.name IS NOT NULL
            AND r.name != ''
        ),
        active_overrides AS (
          SELECT *
          FROM 00_governance.warcraftlogs_admin.preparation_identity_overrides
          WHERE COALESCE(enabled, true) = true
        ),
        override_characters AS (
          SELECT
            CONCAT('override:', id) AS identity_key,
            COALESCE(NULLIF(display_name, ''), NULLIF(source_character, ''), NULLIF(target_character, ''), character_name) AS display_name,
            mode,
            character_name
          FROM active_overrides
          LATERAL VIEW EXPLODE(
            CASE
              WHEN mode = 'replace' THEN ARRAY(source_character, target_character)
              ELSE SPLIT(COALESCE(characters, ''), '\\\\|')
            END
          ) exploded AS character_name
          WHERE character_name IS NOT NULL AND TRIM(character_name) != ''
        ),
        team_identities AS (
          SELECT
            COALESCE(o.identity_key, CONCAT('character:', LOWER(t.name))) AS identity_key,
            COALESCE(MAX(o.display_name), MAX(t.name)) AS player_name,
            COALESCE(MAX_BY(NULLIF(t.player_class, 'Unknown'), t.name), MAX(t.player_class), 'Unknown') AS player_class,
            COALESCE(MAX(t.rank_label), 'Raider') AS rank_label,
            MAX(t.is_active) AS is_active,
            CONCAT_WS('|', ARRAY_SORT(COLLECT_SET(t.name))) AS character_names,
            CASE WHEN MAX(o.identity_key) IS NOT NULL THEN MAX(o.mode) ELSE '' END AS override_label,
            MAX(t.roster_source) AS roster_source
          FROM team_members t
          LEFT JOIN override_characters o
            ON LOWER(t.name) = LOWER(TRIM(o.character_name))
          GROUP BY COALESCE(o.identity_key, CONCAT('character:', LOWER(t.name)))
        ),
        identity_characters AS (
          SELECT
            ti.identity_key,
            exploded.character_name
          FROM team_identities ti
          LATERAL VIEW EXPLODE(SPLIT(ti.character_names, '\\\\|')) exploded AS character_name
        ),
        current_tier_kills AS (
          SELECT
            k.*,
            COALESCE(i.identity_key, CONCAT('character:', LOWER(k.player_name))) AS identity_key
          FROM 03_gold.sc_analytics.gold_boss_kill_roster k
          INNER JOIN current_tier t ON k.zone_name = t.zone_name
          INNER JOIN identity_characters i ON LOWER(k.player_name) = LOWER(i.character_name)
        ),
        prepared_kills AS (
          SELECT
            *,
            LOWER(COALESCE(player_class, '')) AS player_class_lc,
            LOWER(COALESCE(spec, '')) AS spec_lc,
            LOWER(COALESCE(weapon_enhancement_names, '')) AS weapon_names_lc,
            CASE
              WHEN LOWER(COALESCE(player_class, '')) = 'shaman'
                AND LOWER(COALESCE(spec, '')) = 'enhancement'
              THEN CASE
                WHEN LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%flametongue weapon%'
                  AND LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%windfury weapon%'
                THEN 1 ELSE 0
              END
              WHEN LOWER(COALESCE(player_class, '')) = 'shaman'
                AND LOWER(COALESCE(spec, '')) = 'restoration'
              THEN CASE
                WHEN LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%earthliving weapon%'
                THEN 1 ELSE 0
              END
              WHEN LOWER(COALESCE(player_class, '')) = 'shaman'
                AND LOWER(COALESCE(spec, '')) = 'elemental'
              THEN CASE
                WHEN LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%flametongue weapon%'
                  OR COALESCE(has_weapon_enhancement, 0) > 0
                THEN 1 ELSE 0
              END
              WHEN LOWER(COALESCE(player_class, '')) = 'shaman'
              THEN CASE
                WHEN LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%flametongue weapon%'
                  OR LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%windfury weapon%'
                  OR LOWER(COALESCE(weapon_enhancement_names, '')) LIKE '%earthliving weapon%'
                  OR COALESCE(has_weapon_enhancement, 0) > 0
                THEN 1 ELSE 0
              END
              ELSE COALESCE(has_weapon_enhancement, 0)
            END AS has_required_weapon_enhancement
          FROM current_tier_kills
        ),
        kill_agg AS (
          SELECT
            identity_key,
            MAX_BY(player_class, raid_night_date) AS player_class,
            CASE
              WHEN LOWER(MAX_BY(role, raid_night_date)) IN ('healer', 'healing') THEN 'healer'
              WHEN LOWER(MAX_BY(role, raid_night_date)) = 'tank' THEN 'tank'
              WHEN LOWER(MAX_BY(role, raid_night_date)) = 'dps' THEN 'dps'
              ELSE 'unknown'
            END AS role,
            MAX_BY(spec, raid_night_date) AS spec,
            MAX_BY(avg_item_level, raid_night_date) AS latest_avg_item_level,
            MAX(raid_night_date) AS latest_kill_date,
            COUNT(*) AS kills_tracked,
            SUM(CASE WHEN COALESCE(has_food_buff, 0) > 0 THEN 1 ELSE 0 END) AS kills_with_food,
            SUM(CASE WHEN COALESCE(has_flask_or_phial_buff, 0) > 0 THEN 1 ELSE 0 END) AS kills_with_flask,
            SUM(CASE WHEN COALESCE(has_required_weapon_enhancement, 0) > 0 THEN 1 ELSE 0 END) AS kills_with_weapon,
            SUM(CASE WHEN COALESCE(potion_use, 0) > 0 THEN 1 ELSE 0 END) AS kills_with_combat_potion,
            COUNT(DISTINCT raid_night_date) AS raids_present,
            MAX_BY(NULLIF(TRIM(food_buff_names), ''), CASE WHEN NULLIF(TRIM(food_buff_names), '') IS NOT NULL THEN raid_night_date END) AS recent_food_names,
            MAX_BY(NULLIF(TRIM(flask_or_phial_names), ''), CASE WHEN NULLIF(TRIM(flask_or_phial_names), '') IS NOT NULL THEN raid_night_date END) AS recent_flask_names,
            MAX_BY(NULLIF(TRIM(weapon_enhancement_names), ''), CASE WHEN NULLIF(TRIM(weapon_enhancement_names), '') IS NOT NULL THEN raid_night_date END) AS recent_weapon_names,
            MAX_BY(NULLIF(TRIM(combat_potion_names), ''), CASE WHEN NULLIF(TRIM(combat_potion_names), '') IS NOT NULL THEN raid_night_date END) AS recent_combat_potion_names
          FROM prepared_kills
          GROUP BY identity_key
        ),
        base AS (
          SELECT
            i.identity_key,
            i.player_name,
            COALESCE(k.player_class, i.player_class, 'Unknown') AS player_class,
            COALESCE(k.role, 'unknown') AS role,
            i.rank_label,
            i.is_active,
            t.zone_name AS current_tier,
            i.roster_source,
            k.identity_key IS NOT NULL AS has_current_tier_data,
            COALESCE(k.raids_present, 0) AS raids_present,
            d.total_raids_tracked,
            COALESCE(k.kills_tracked, 0) AS kills_tracked,
            CASE WHEN d.total_raids_tracked > 0 THEN COALESCE(k.raids_present, 0) / d.total_raids_tracked * 100 ELSE 0 END AS attendance_rate_pct,
            CASE WHEN COALESCE(k.kills_tracked, 0) > 0 THEN COALESCE(k.kills_with_food, 0) / k.kills_tracked * 100 ELSE 0 END AS food_rate,
            CASE WHEN COALESCE(k.kills_tracked, 0) > 0 THEN COALESCE(k.kills_with_flask, 0) / k.kills_tracked * 100 ELSE 0 END AS flask_rate,
            CASE WHEN COALESCE(k.kills_tracked, 0) > 0 THEN COALESCE(k.kills_with_weapon, 0) / k.kills_tracked * 100 ELSE 0 END AS weapon_rate,
            CASE WHEN COALESCE(k.kills_tracked, 0) > 0 THEN COALESCE(k.kills_with_combat_potion, 0) / k.kills_tracked * 100 ELSE 0 END AS combat_potion_rate,
            COALESCE(k.spec, '—') AS spec,
            COALESCE(k.latest_avg_item_level, 0) AS latest_avg_item_level,
            COALESCE(CAST(k.latest_kill_date AS STRING), '') AS latest_kill_date,
            COALESCE(k.recent_food_names, '') AS recent_food_names,
            COALESCE(k.recent_flask_names, '') AS recent_flask_names,
            COALESCE(k.recent_weapon_names, '') AS recent_weapon_names,
            COALESCE(
              k.recent_combat_potion_names,
              CASE WHEN COALESCE(k.kills_with_combat_potion, 0) > 0 THEN 'Combat potion used' ELSE '' END
            ) AS recent_combat_potion_names,
            i.character_names,
            CASE
              WHEN i.override_label = 'replace' THEN 'Replace'
              WHEN i.override_label = 'pool' THEN 'Pool'
              ELSE ''
            END AS override_label
          FROM team_identities i
          CROSS JOIN current_tier t
          CROSS JOIN current_tier_raid_dates d
          LEFT JOIN kill_agg k ON i.identity_key = k.identity_key
        ),
        scored AS (
          SELECT
            *,
            role = 'dps' AS include_combat_potion,
            CASE
              WHEN role = 'dps' THEN LEAST(food_rate, flask_rate, weapon_rate, combat_potion_rate)
              ELSE LEAST(food_rate, flask_rate, weapon_rate)
            END AS weakest_signal_score,
            CASE
              WHEN role = 'dps' AND combat_potion_rate <= food_rate AND combat_potion_rate <= flask_rate AND combat_potion_rate <= weapon_rate THEN 'Combat Potion'
              WHEN food_rate <= flask_rate AND food_rate <= weapon_rate THEN 'Food'
              WHEN flask_rate <= weapon_rate THEN 'Flask'
              ELSE 'Weapon'
            END AS weakest_signal_label,
            CASE
              WHEN total_raids_tracked > 0 AND kills_tracked > 0 AND role = 'dps'
              THEN attendance_rate_pct * 0.25 + food_rate * 0.25 + flask_rate * 0.20 + weapon_rate * 0.15 + combat_potion_rate * 0.15
              WHEN total_raids_tracked > 0 AND kills_tracked > 0
              THEN (attendance_rate_pct * 0.25 + food_rate * 0.25 + flask_rate * 0.20 + weapon_rate * 0.15) / 0.85
              WHEN total_raids_tracked > 0
              THEN attendance_rate_pct
              ELSE 0
            END AS readiness_score_raw
          FROM base
        ),
        labelled AS (
          SELECT
            *,
            FILTER(ARRAY(
              CASE WHEN has_current_tier_data = false THEN 'no current-tier logs' END,
              CASE WHEN attendance_rate_pct > 0 AND attendance_rate_pct < 70 THEN 'attendance risk' END,
              CASE WHEN kills_tracked > 0 AND food_rate < 80 THEN 'food coverage low' END,
              CASE WHEN kills_tracked > 0 AND flask_rate < 80 THEN 'flask/phial coverage low' END,
              CASE WHEN kills_tracked > 0 AND weapon_rate < 80 THEN 'weapon enhancement coverage low' END,
              CASE WHEN include_combat_potion AND kills_tracked > 0 AND combat_potion_rate < 50 THEN 'combat potion usage low' END,
              CASE WHEN has_current_tier_data AND kills_tracked = 0 THEN 'no tracked boss kills' END
            ), note -> note IS NOT NULL) AS readiness_notes_array
          FROM scored
        )
        SELECT
          identity_key,
          player_name,
          player_class,
          role,
          rank_label,
          is_active,
          current_tier,
          roster_source,
          has_current_tier_data,
          ROUND(attendance_rate_pct, 1) AS attendance_rate_pct,
          raids_present,
          total_raids_tracked,
          kills_tracked,
          ROUND(food_rate, 1) AS food_rate,
          ROUND(flask_rate, 1) AS flask_rate,
          ROUND(weapon_rate, 1) AS weapon_rate,
          ROUND(combat_potion_rate, 1) AS combat_potion_rate,
          ROUND(readiness_score_raw, 1) AS readiness_score,
          CASE
            WHEN readiness_score_raw >= 85 AND SIZE(readiness_notes_array) <= 1 THEN 'strong'
            WHEN readiness_score_raw < 65 OR SIZE(readiness_notes_array) >= 3 THEN 'watch'
            ELSE 'steady'
          END AS readiness_label,
          COALESCE(CONCAT_WS('|', readiness_notes_array), '') AS readiness_notes,
          COALESCE(spec, '—') AS spec,
          ROUND(latest_avg_item_level, 1) AS latest_avg_item_level,
          COALESCE(latest_kill_date, '') AS latest_kill_date,
          COALESCE(weakest_signal_label, '') AS weakest_signal_label,
          COALESCE(recent_food_names, '') AS recent_food_names,
          COALESCE(recent_flask_names, '') AS recent_flask_names,
          COALESCE(recent_weapon_names, '') AS recent_weapon_names,
          COALESCE(recent_combat_potion_names, '') AS recent_combat_potion_names,
          COALESCE(character_names, '') AS character_names,
          COALESCE(override_label, '') AS override_label
        FROM labelled
        ORDER BY readiness_score ASC, player_name ASC
        """
    )
