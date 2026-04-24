# Data Dictionary — Gold Layer

All gold tables are produced by the DLT pipeline and are the source of truth for the frontend dashboard. Updated on each pipeline run (nightly after ingestion).

**Unity Catalog path**: `03_gold.sc_analytics.<table_name>`

---

## Core Facts

### fact_player_fight_performance

One row per player per boss kill fight. The primary fact table for all performance analysis.

**Source**: `silver_player_performance` ⋈ `silver_fight_events` ⋈ `silver_player_rankings`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | STRING | WCL report identifier |
| `fight_id` | LONG | Fight number within the report |
| `encounter_id` | LONG | Stable WCL encounter identifier |
| `boss_name` | STRING | Boss name |
| `zone_name` | STRING | Raid zone name |
| `difficulty` | LONG | Difficulty integer (3=Normal, 4=Heroic, 5=Mythic) |
| `difficulty_label` | STRING | Human-readable difficulty |
| `raid_night_date` | DATE | Date of the raid night |
| `is_kill` | BOOLEAN | Always true (table filters to kills only) |
| `duration_seconds` | LONG | Fight duration in seconds |
| `player_name` | STRING | Player character name |
| `player_class` | STRING | WoW class (from WCL actor data) |
| `role` | STRING | dps / healer / tank |
| `spec` | STRING | Specialisation name |
| `avg_item_level` | DOUBLE | Average item level of equipped gear |
| `potion_use` | LONG | Number of potions used (from combatantInfo) |
| `healthstone_use` | LONG | Number of healthstones used |
| `crit_rating` | LONG | Critical strike rating |
| `haste_rating` | LONG | Haste rating |
| `mastery_rating` | LONG | Mastery rating |
| `versatility_rating` | LONG | Versatility rating |
| `throughput_per_second` | LONG | DPS/HPS from WCL rankings.amount (nullable — null when no WCL ranking for this fight/player) |
| `rank_percent` | DOUBLE | WCL parse percentile (0–100) |
| `bracket_percent` | DOUBLE | WCL bracket (item level bracket) percentile |
| `rank_string` | STRING | Approximate rank position (e.g. "~1265") |

**Limitations**:
- `throughput_per_second` is null when WCL has no ranking entry for the player/fight combination (e.g. fight was not ranked, player was offline, or boss has no ranking support)
- `potion_use` / `healthstone_use` are 0 when not used, but may be null for some older reports where combatantInfo was not available

---

### fact_player_events

One row per death event across all reports.

**Source**: `silver_player_deaths` ⋈ `silver_guild_reports`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | STRING | WCL report identifier |
| `fight_id` | LONG | Fight the death occurred in |
| `player_name` | STRING | Player who died |
| `player_class` | STRING | Player's WoW class |
| `death_timestamp_ms` | LONG | Fight-relative timestamp in milliseconds |
| `overkill` | LONG | Overkill damage amount |
| `killing_blow_name` | STRING | Name of the ability that killed the player (nullable — null if killing blow was from a friendly or events array was empty) |
| `killing_blow_id` | LONG | Spell ID of the killing blow ability |
| `zone_name` | STRING | Raid zone (from report-level context) |
| `zone_id` | LONG | WCL zone identifier |
| `raid_night_date` | DATE | Date of the raid night |

---

## Dimensions

### dim_encounter

Boss encounter reference table. One row per encounter ID.

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | LONG | Stable WCL encounter identifier |
| `encounter_name` | STRING | Boss name |
| `zone_id` | LONG | Zone identifier |
| `zone_name` | STRING | Raid zone name |
| `zone_frozen` | BOOLEAN | Whether the zone is a previous tier (frozen = older content) |
| `difficulty_names` | ARRAY<STRING> | List of available difficulty names for this encounter |

**Note**: Only includes active (non-frozen) zones. Filter `zone_frozen = false` for current tier.

---

### dim_player

Canonical player identity table. One row per player name across all WCL logs.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name (from WCL actor data) |
| `player_class` | STRING | WoW class (from actor roster; falls back to performance data) |
| `realm` | STRING | Realm name |
| `is_guild_member` | BOOLEAN | Whether matched in Blizzard guild roster |
| `rank` | LONG | Guild rank ID (null for non-guild players) |
| `rank_label` | STRING | Human-readable rank (Guild Master, Officer, etc.) |
| `rank_category` | STRING | Rank group (GM, Officer, Raider, etc.) |
| `is_raid_team` | BOOLEAN | Whether rank is in the active raid team (ranks 0–5, 8) |
| `first_seen_date` | DATE | First attendance record date |
| `last_seen_date` | DATE | Most recent attendance record date |

---

### dim_guild_member

Authoritative guild roster from Blizzard API. One row per guild member.

| Column | Type | Description |
|--------|------|-------------|
| `name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug (lowercase, hyphenated) |
| `rank` | LONG | Guild rank ID (0–9) |
| `rank_label` | STRING | Human-readable rank label |
| `rank_category` | STRING | Rank category grouping |
| `is_raid_team` | BOOLEAN | Active raid team member flag |
| `class_id` | LONG | Blizzard class ID (1–13) |
| `class_name` | STRING | Derived WoW class name |
| `level` | LONG | Character level |
| `total_raids_tracked` | LONG | Total WCL raid reports in attendance data |
| `raids_present` | LONG | Reports where player was marked present |
| `attendance_rate_pct` | DOUBLE | Attendance % (raids_present / total_raids_tracked × 100) |
| `last_raid_date` | DATE | Most recent raid attendance date |
| `first_raid_date` | DATE | First raid attendance date |
| `is_active` | BOOLEAN | Attendance ≥ 25% OR rank IN (0, 1, 2) |
| `possible_main` | STRING | Character's own name (alt resolution handled in gold_raid_team) |

---

## Player Products

### gold_player_attendance

Per-player attendance rates across all tracked raids.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | |
| `player_class` | STRING | |
| `total_raids_tracked` | LONG | Total report count |
| `raids_present` | LONG | Reports with presence = 1 |
| `raids_benched` | LONG | Reports with presence = 2 |
| `raids_absent` | LONG | Reports with presence = 3 |
| `last_raid_date` | DATE | |
| `first_raid_date` | DATE | |
| `zones_attended` | ARRAY<STRING> | Distinct zones attended |
| `attendance_rate_pct` | DOUBLE | raids_present / total × 100 |

---

### gold_weekly_activity

Raid activity aggregated by ISO week.

| Column | Type | Description |
|--------|------|-------------|
| `week_start` | TIMESTAMP | Monday of the ISO week |
| `raid_nights` | LONG | Number of unique reports that week |
| `total_boss_kills` | LONG | Sum of boss kills across all reports |
| `total_wipes` | LONG | Sum of wipes |
| `total_pulls` | LONG | Total pulls (kills + wipes) |
| `total_raid_seconds` | LONG | Total active fight time in seconds |
| `zones_raided` | ARRAY<STRING> | Distinct zones raided that week |

---

### gold_player_performance_summary

Aggregated performance per player across all kill fights.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | |
| `player_class` | STRING | |
| `realm` | STRING | |
| `role` | STRING | dps / healer / tank |
| `primary_spec` | STRING | Most frequently played spec |
| `kills_tracked` | LONG | Number of kill fights included |
| `avg_throughput_per_second` | LONG | Average DPS/HPS (from WCL rankings) |
| `best_throughput_per_second` | LONG | Single-fight best DPS/HPS |
| `avg_rank_percent` | DOUBLE | Average WCL parse percentile |
| `best_rank_percent` | DOUBLE | Highest single-fight parse |
| `avg_item_level` | DOUBLE | Average equipped item level |
| `last_seen_date` | DATE | Most recent kill date |

---

### gold_boss_kill_roster

Per-player stats on every boss kill. Queryable by boss, player, or date.

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | STRING | |
| `fight_id` | LONG | |
| `boss_name` | STRING | |
| `encounter_id` | LONG | |
| `difficulty` | LONG | |
| `difficulty_label` | STRING | |
| `zone_name` | STRING | |
| `raid_night_date` | DATE | |
| `duration_seconds` | LONG | |
| `player_name` | STRING | |
| `player_class` | STRING | |
| `role` | STRING | |
| `spec` | STRING | |
| `avg_item_level` | DOUBLE | |
| `potion_use` | LONG | |
| `healthstone_use` | LONG | |
| `throughput_per_second` | LONG | From WCL rankings (nullable) |
| `rank_percent` | DOUBLE | |
| `bracket_percent` | DOUBLE | |
| `rank_string` | STRING | |

---

### gold_player_boss_performance

Per-player per-boss aggregation across all kills. Includes trend indicator.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | |
| `player_class` | STRING | |
| `role` | STRING | |
| `primary_spec` | STRING | |
| `encounter_id` | LONG | |
| `boss_name` | STRING | |
| `zone_name` | STRING | |
| `difficulty` | LONG | |
| `difficulty_label` | STRING | |
| `kills_on_boss` | LONG | Number of kills included |
| `avg_throughput_per_second` | LONG | |
| `best_throughput_per_second` | LONG | |
| `latest_throughput_per_second` | LONG | Most recent kill performance |
| `throughput_trend` | DOUBLE | (latest − avg) / avg × 100. Positive = improving |
| `avg_rank_percent` | DOUBLE | |
| `best_rank_percent` | DOUBLE | |
| `avg_item_level` | DOUBLE | |
| `first_kill_date` | DATE | |
| `latest_kill_date` | DATE | |

---

## Progression / Summary

### gold_boss_progression
Kill/wipe counts per encounter across all raids. `wipe_to_kill_ratio`, `best_kill_seconds`, first/last attempt dates.

### gold_raid_summary
One row per raid night. Boss kill count, wipe count, total pulls, active fight seconds, zones.

### gold_progression_timeline
First-kill dates per encounter per difficulty, with cumulative kill counter for progression charts.

### gold_best_kills
Fastest recorded kill per encounter per difficulty. Includes `best_kill_mm_ss` string (e.g. "3m 42s").

### gold_boss_wipe_analysis
Wipe breakdown per boss: closest wipe %, avg wipe %, phase progression, duration stats, raid nights attempted.

### gold_encounter_catalog
Zone/encounter reference for frontend dropdowns. Active tiers only.

---

## Survivability

### gold_player_survivability

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | |
| `player_class` | STRING | |
| `total_deaths` | LONG | Deaths across all raids |
| `kills_tracked` | LONG | Kill fights player participated in |
| `deaths_per_kill` | DOUBLE | total_deaths / kills_tracked |
| `most_common_killing_blow` | STRING | Ability that killed this player most |
| `most_common_killing_blow_count` | LONG | How many times |
| `zones_died_in` | ARRAY<STRING> | |
| `last_death_timestamp_ms` | LONG | |

---

### gold_boss_mechanics

Enhanced wipe analysis per boss: phase bucket breakdown (Phase 1/2/3+), duration bucket breakdown (< 1 min / 1–3 min / 3–5 min / 5+ min), weekly pull counts, progress trend (last week avg boss% vs overall avg).

---

## Preparation

### gold_player_consumables

Per-player consumable compliance. `potion_use_rate` and `healthstone_use_rate` are fractions 0.0–1.0. Includes `boss_consumable_details` array with per-boss breakdown.

### gold_player_combat_stats

Per-player stat ratings. `latest_*` columns = most recent kill snapshot. `avg_*` columns = all-time average across kills. Grouped by (player, spec).

### gold_boss_ability_deaths

What is killing players on each boss. `death_rank` orders abilities by deaths. Splits `deaths_on_kills` vs `deaths_on_wipes`. Includes `unique_players_killed` and `reports_with_deaths`.

### Current Preparation page inputs

The live frontend `Preparation` page does **not** currently use `gold_player_consumables`
or `gold_player_combat_stats` as its main source of truth.

Instead it is built from:
- `gold_raid_summary` to identify the current raid tier and its raid nights
- `gold_boss_kill_roster` for current-tier preparation signals and latest prep names
- `live_raid_roster` with `gold_raid_team` fallback for the current team scope
- `preparation_overrides.csv` for same-raider character replacement/pooling

The page is intentionally current-tier only. Historical all-time aggregates are not
used there unless the page is explicitly redesigned to support mixed-tier views.

`gold_boss_kill_roster` now carries these preparation-facing columns used directly
by the frontend:
- `has_food_buff`, `food_buff_names`
- `has_flask_or_phial_buff`, `flask_or_phial_names`
- `has_weapon_enhancement`, `weapon_enhancement_names`
- `potion_use`, `combat_potion_casts`, `combat_potion_names`

Role-specific scoring rule:
- combat potion usage is displayed for all roles
- combat potion usage contributes to readiness scoring only for DPS

---

## Roster

### gold_guild_roster
Full Blizzard guild roster with class, realm, rank, attendance stats, `is_active` flag.

### gold_raid_team
Active raid team (is_raid_team = true) with `has_possible_alt_in_logs` flag (name-prefix heuristic).

### gold_player_profile
Comprehensive per-player summary: identity + guild rank + WCL performance aggregates (throughput, parse %, item level) + attendance stats.

### gold_roster
Active player roster from WCL actor logs (not Blizzard) with latest class/realm and attendance summary.

---

## Mythic+

Raider.IO data is ingested into Databricks through `ingest_primary.py` and transformed by `pipeline/silver/clean_raiderio.py` and `pipeline/gold/mplus_products.py`. These tables are current-season only in v1. True score-over-time begins from the first successful Raider.IO ingestion.

### gold_player_mplus_summary

Latest Raider.IO Mythic+ summary per player.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug |
| `region` | STRING | Raider.IO region, usually `eu` |
| `profile_url` | STRING | Raider.IO character profile URL |
| `season` | STRING | Raider.IO season identifier |
| `snapshot_at` | TIMESTAMP | Ingestion timestamp for the latest score snapshot |
| `score_all` | DOUBLE | Overall Raider.IO score |
| `score_dps` | DOUBLE | DPS role score |
| `score_healer` | DOUBLE | Healer role score |
| `score_tank` | DOUBLE | Tank role score |
| `world_rank` | LONG | Overall world rank if returned by Raider.IO |
| `region_rank` | LONG | Overall region rank |
| `realm_rank` | LONG | Overall realm rank |
| `total_runs` | LONG | Deduped recent/best runs exported for this character |
| `timed_runs` | LONG | Runs completed in time |
| `untimed_runs` | LONG | Runs not completed in time |
| `highest_timed_level` | LONG | Highest timed key level |
| `highest_untimed_level` | LONG | Highest untimed key level |
| `most_common_key_level` | LONG | Most common key level in exported run payload |
| `most_common_key_count` | LONG | Count at the most common key level |
| `best_run_dungeon` | STRING | Dungeon for the selected best run |
| `best_run_short_name` | STRING | Short dungeon name |
| `best_run_level` | LONG | Key level for the selected best run |
| `best_run_score` | DOUBLE | Raider.IO run score |
| `best_run_timed` | BOOLEAN | Whether the selected best run was timed |
| `best_run_completed_at` | TIMESTAMP | Completion timestamp |
| `best_run_url` | STRING | Raider.IO run URL |

### gold_player_mplus_score_history

Nightly Raider.IO score snapshots for trend charts.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug |
| `region` | STRING | Raider.IO region |
| `profile_url` | STRING | Raider.IO character profile URL |
| `season` | STRING | Raider.IO season identifier |
| `snapshot_at` | TIMESTAMP | Ingestion timestamp |
| `snapshot_date` | DATE | Snapshot date |
| `score_all` | DOUBLE | Overall score |
| `score_dps` | DOUBLE | DPS role score |
| `score_healer` | DOUBLE | Healer role score |
| `score_tank` | DOUBLE | Tank role score |
| `world_rank` | LONG | Overall world rank |
| `region_rank` | LONG | Overall region rank |
| `realm_rank` | LONG | Overall realm rank |
| `raiderio_last_crawled_at` | STRING | Raider.IO crawl timestamp from the source payload |

### gold_player_mplus_run_history

Governed run-level table from Raider.IO recent/best run arrays.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug |
| `region` | STRING | Raider.IO region |
| `season` | STRING | Season identifier |
| `source` | STRING | `recent` or `best` source array |
| `dungeon` | STRING | Dungeon name |
| `short_name` | STRING | Short dungeon name |
| `mythic_level` | LONG | Keystone level |
| `score` | DOUBLE | Raider.IO run score |
| `completed_at` | TIMESTAMP | Completion timestamp |
| `completed_date` | DATE | Completion date |
| `clear_time_ms` | LONG | Clear time in ms |
| `par_time_ms` | LONG | Timer/par time in ms |
| `num_keystone_upgrades` | LONG | Keystone upgrades awarded |
| `timed` | BOOLEAN | Whether the run was completed in time |
| `url` | STRING | Raider.IO run URL |
| `snapshot_at` | TIMESTAMP | Ingestion timestamp |

### gold_player_mplus_weekly_activity

Weekly Mythic+ activity derived from the exported run payload.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug |
| `region` | STRING | Raider.IO region |
| `season` | STRING | Season identifier |
| `week_start` | DATE | Week start date |
| `total_runs` | LONG | Runs in week |
| `timed_runs` | LONG | Timed runs in week |
| `untimed_runs` | LONG | Untimed runs in week |
| `highest_key_level` | LONG | Highest key level completed that week |
| `unique_dungeons` | LONG | Distinct dungeons completed that week |
| `most_common_key_level` | LONG | Most common key level that week |

### gold_player_mplus_dungeon_breakdown

Per-player per-dungeon Mythic+ summary.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `realm_slug` | STRING | Realm slug |
| `region` | STRING | Raider.IO region |
| `season` | STRING | Season identifier |
| `dungeon` | STRING | Dungeon name |
| `best_short_name` | STRING | Short dungeon name for the best run |
| `highest_key_level` | LONG | Highest key level in exported payload |
| `highest_timed_level` | LONG | Highest timed key level |
| `total_runs` | LONG | Runs for this dungeon |
| `timed_runs` | LONG | Timed runs for this dungeon |
| `untimed_runs` | LONG | Untimed runs for this dungeon |
| `latest_completed_at` | TIMESTAMP | Latest completion timestamp |
| `best_key_level` | LONG | Key level for selected best run |
| `best_score` | DOUBLE | Raider.IO best run score |
| `best_timed` | BOOLEAN | Whether selected best run was timed |
| `best_completed_at` | TIMESTAMP | Best run completion timestamp |
| `best_run_url` | STRING | Raider.IO run URL |

---

## Rank Reference

| Rank ID | Label | Category | Is Raid Team |
|---------|-------|----------|--------------|
| 0 | Guild Master | GM | ✓ |
| 1 | GM Alt | GM | ✓ |
| 2 | Officer | Officer | ✓ |
| 3 | Officer Alt | Officer | ✓ |
| 4 | Officer Alt | Officer | ✓ |
| 5 | Raider | Raider | ✓ |
| 6 | Raider Alt | Raider Alt | ✗ |
| 7 | Bestie | Bestie | ✗ |
| 8 | Trial | Trial | ✓ |
| 9 | Social | Social | ✗ |
