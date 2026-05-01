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
| `throughput_per_second` | LONG | Role-aware throughput from WCL rankings.amount: DPS for dps/tank rows, HPS for healer rows (nullable — null when no WCL ranking for this fight/player) |
| `rank_percent` | DOUBLE | WCL parse percentile (0–100), role-aware (DPS parse for dps/tank, HPS parse for healer) |
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

Player-keyed Gold and dashboard-facing products use `player_identity_key` as the
relational character key. The key is deterministic and lowercase:
`player_name:player_class:realm` when class is available, or
`player_name:unknown:realm_slug` for Raider.IO/Blizzard profile products that do
not carry class. Realmless legacy rows use the explicit `unknown` sentinel.

### gold_player_attendance

Per-player attendance rates across all tracked raids. Grain is `player_name`
plus `player_class` plus `player_realm`. Realm is resolved from the per-report
actor roster when available; older/incomplete rows use the explicit sentinel
`unknown`. `player_identity_key` is a deterministic lowercase convenience key
for this table's identity grain.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | STRING | Lowercase `player_name:player_class:player_realm` identity key |
| `player_name` | STRING | |
| `player_class` | STRING | |
| `player_realm` | STRING | Realm resolved from WCL actor roster, or `unknown` |
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
| `player_identity_key` | STRING | Lowercase `player_name:player_class:realm` identity fingerprint |
| `player_name` | STRING | |
| `player_class` | STRING | |
| `realm` | STRING | Realm resolved from WCL actor roster, or `unknown` |
| `role` | STRING | dps / healer / tank |
| `primary_spec` | STRING | Most frequently played spec |
| `kills_tracked` | LONG | Number of kill fights included |
| `avg_throughput_per_second` | LONG | Average role-aware throughput (DPS for dps/tank, HPS for healer); null when WCL ranking amount is unavailable |
| `best_throughput_per_second` | LONG | Single-fight best role-aware throughput (DPS for dps/tank, HPS for healer); null when WCL ranking amount is unavailable |
| `avg_rank_percent` | DOUBLE | Average WCL parse percentile, role-aware (DPS parse / HPS parse) |
| `best_rank_percent` | DOUBLE | Highest single-fight parse, role-aware (DPS parse / HPS parse) |
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

### gold_player_survivability_rankings

Scoped player-detail survivability rank product. Grain is one row per
`player_name`, `zone_name`, `boss_name`, and `difficulty_label`; scope columns may
use `All` for pre-aggregated dashboard scopes. Rank is calculated within each
scope by lowest `deaths_per_kill` first, with `survivability_rank_percentile`
provided as a 0-100 display scale where higher is better.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `player_class` | STRING | WarcraftLogs class |
| `zone_name` | STRING | Raid zone, or `All` |
| `encounter_id` | LONG | Encounter id when the display boss scope maps to one encounter; null for all-boss or ambiguous all-zone scopes |
| `boss_name` | STRING | Boss name, or `All` |
| `difficulty` | LONG | Difficulty id when the display difficulty scope maps to one id; null for all-difficulty or ambiguous scopes |
| `difficulty_label` | STRING | Difficulty label, or `All` |
| `deaths` | LONG | Death count in scope |
| `kills` | LONG | Kill roster rows in scope |
| `deaths_per_kill` | DOUBLE | Deaths divided by kills |
| `survivability_rank` | LONG | Rank in scope; 1 is best survivability |
| `survivability_rank_total` | LONG | Number of ranked players in scope |
| `survivability_rank_percentile` | DOUBLE | 0-100 percentile derived from rank |

Downstream consumer: `frontend/src/features/player-detail/index.tsx`.

---

### gold_wipe_survival_discipline

Gold-owned Wipe Analysis survival-discipline product. Grain is one row per
`player_name`, `zone_name`, `boss_name`, and `difficulty_label`; scope columns may
use the literal `All` for pre-aggregated static dashboard views. Primary key:
`player_name`, `zone_name`, `boss_name`, `difficulty_label`.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | STRING | Character name |
| `player_class` | STRING | WarcraftLogs class |
| `role` | STRING | Latest known kill-role when available |
| `zone_name` | STRING | Raid zone, or `All` |
| `encounter_id` | LONG | Encounter id; null for all-boss scopes |
| `boss_name` | STRING | Boss name, or `All` |
| `difficulty` | LONG | WCL difficulty; null for all-difficulty scopes |
| `difficulty_label` | STRING | Difficulty label, or `All` |
| `wipe_pulls_tracked` | LONG | Instrumented wipe pulls where the player was present |
| `wipe_deaths` | LONG | Player deaths on tracked wipe pulls |
| `first_deaths` | LONG | Wipe deaths that were first in the pull |
| `early_deaths` | LONG | Wipe deaths within the first minute |
| `kill_deaths` | LONG | Player deaths on kill pulls in scope |
| `kills_tracked` | LONG | Kill pulls where the player was present |
| `deaths_per_kill` | DOUBLE | `kill_deaths / kills_tracked`; null when no kills are tracked |
| `deaths_per_wipe` | DOUBLE | `wipe_deaths / wipe_pulls_tracked` |
| `pulls_with_tracked_defensive_capacity` | LONG | Wipe pulls with scored personal defensive capacity |
| `tracked_defensive_capacity` | LONG | Scored possible personal defensive casts |
| `defensive_casts` | LONG | Scored personal defensive casts used |
| `defensive_missed_casts` | LONG | Scored possible defensive casts left unused |
| `healthstone_uses` | LONG | Wipe deaths with healthstone use before death |
| `potion_uses` | LONG | Wipe deaths with health potion use before death |
| `defensive_usage_rate` | DOUBLE | `defensive_casts / tracked_defensive_capacity * 100`; null when no defensive capacity is tracked |
| `healthstone_usage_rate` | DOUBLE | `healthstone_uses / wipe_deaths * 100`; null when no wipe deaths exist |
| `potion_usage_rate` | DOUBLE | `potion_uses / wipe_deaths * 100`; null when no wipe deaths exist |
| `death_pressure_score` | DOUBLE | 0–100 component score for avoiding wipe deaths |
| `defensive_component_score` | DOUBLE | 0–100 class-baselined defensive component; unknown and no-capacity states receive a neutral component while `defensive_tracking_status` preserves the distinction |
| `healthstone_component_score` | DOUBLE | 0–100 healthstone discipline component |
| `potion_component_score` | DOUBLE | 0–100 potion discipline component |
| `defensive_class_baseline_pct` | DOUBLE | Scope/class median defensive usage rate for tracked-capacity players |
| `survival_discipline_score` | DOUBLE | Absolute 0–100 consistency score; higher is better |
| `survival_failure_score` | DOUBLE | Back-compatible alias for `survival_discipline_score` |
| `top_improvement_area` | STRING | Lowest component label, or `—` when all components are strong |
| `defensive_tracking_status` | STRING | `tracked_used`, `tracked_zero_usage`, `no_tracked_capacity`, or `unknown` |

Null/zero semantics:
- `tracked_zero_usage` means the player had tracked defensive capacity and `defensive_usage_rate = 0`.
- `no_tracked_capacity` means the player was present on tracked wipe pulls but no scored personal defensive capacity existed for the observed class/spec rules.
- `unknown` is reserved for rows where Gold cannot establish defensive capacity state.
- Null rates mean no valid denominator or unknown state; they must not be coerced to zero by consumers.

Downstream consumer: `frontend/src/features/wipe-analysis/index.tsx` via the
`wipe_survival_discipline` dashboard JSON asset. The frontend may assign
relative letter grades for the currently visible rows, but must not redefine the
absolute component or score formula.

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

### gold_preparation_readiness

Current-tier raid-team preparation readiness. Grain is one row per preparation
identity (`identity_key`). Gold applies published preparation identity overrides,
uses `gold_live_raid_roster` when present and `gold_raid_team` otherwise, derives
current tier from latest included `gold_raid_summary`, and computes preparation
rates and readiness semantics from `gold_boss_kill_roster`.

| Column | Type | Description |
|--------|------|-------------|
| `identity_key` | STRING | Character or published override identity key |
| `player_name` | STRING | Display name |
| `player_class` | STRING | Latest known class or roster class |
| `role` | STRING | dps / healer / tank / unknown |
| `rank_label` | STRING | Roster rank label |
| `is_active` | BOOLEAN | Active roster flag |
| `current_tier` | STRING | Current raid tier |
| `roster_source` | STRING | `live_raid_roster` or `gold_raid_team` |
| `has_current_tier_data` | BOOLEAN | Whether current-tier kill rows exist |
| `attendance_rate_pct` | DOUBLE | Current-tier raid nights present / raid nights tracked × 100 |
| `raids_present` | LONG | Current-tier raid nights with at least one kill row |
| `total_raids_tracked` | LONG | Current-tier raid nights tracked |
| `kills_tracked` | LONG | Current-tier boss kill rows |
| `food_rate` | DOUBLE | Kill coverage for food buff |
| `flask_rate` | DOUBLE | Kill coverage for flask/phial |
| `weapon_rate` | DOUBLE | Kill coverage for weapon enhancement |
| `combat_potion_rate` | DOUBLE | Kill coverage for combat potion usage |
| `readiness_score` | DOUBLE | Gold-owned 0-100 readiness score |
| `readiness_label` | STRING | `watch`, `steady`, or `strong` |
| `readiness_notes` | STRING | Pipe-delimited Gold-owned notes |
| `weakest_signal_label` | STRING | Lowest preparation component label |
| `recent_food_names` | STRING | Latest non-empty current-tier food buff names, or empty string |
| `recent_flask_names` | STRING | Latest non-empty current-tier flask/phial names, or empty string |
| `recent_weapon_names` | STRING | Latest non-empty current-tier weapon enhancement names, or empty string |
| `recent_combat_potion_names` | STRING | Latest non-empty classified combat potion names. If potion usage exists but the event name is unavailable, `Combat potion used`; otherwise empty string |
| `character_names` | STRING | Pipe-delimited characters included in the identity |
| `override_label` | STRING | `Replace`, `Pool`, or empty string |

Score semantics:
- attendance contributes 25% when raid nights exist
- food contributes 25%, flask/phial 20%, and weapon enhancement 15% when kills exist
- Shaman weapon enhancement is spec-aware: Enhancement requires both `Flametongue Weapon` and `Windfury Weapon`, Restoration requires `Earthliving Weapon`, and Elemental accepts either `Flametongue Weapon` or a tracked weapon oil/enhancement
- combat potion contributes 15% only for DPS identities
- non-DPS readiness renormalises the non-potion weights
- `strong` is score >= 85 with at most one note; `watch` is score < 65 or at least three notes; otherwise `steady`

Downstream consumer: `frontend/src/pages/Preparation.tsx` via the
`preparation_readiness` dashboard JSON asset. The frontend owns filtering,
sorting, colours, override-edit UI, and display formatting only.

---

## Roster

### gold_guild_roster
Full Blizzard guild roster with class, realm, rank, attendance stats, `is_active` flag.

### gold_raid_team
Active raid team (is_raid_team = true) with `has_possible_alt_in_logs` flag (name-prefix heuristic).

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
| `guild_mplus_rank` | LONG | Guild rank by current-season overall score |
| `guild_mplus_rank_total` | LONG | Number of guild players with non-zero current-season score |
| `guild_mplus_rank_percentile` | DOUBLE | 0-100 percentile derived from guild rank |
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
