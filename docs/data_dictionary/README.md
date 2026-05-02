# Data Dictionary — Gold Layer

Generated from `pipeline/contracts/gold/*.yml` by `scripts/generate_data_dictionary.py`. Do not edit by hand — update the contracts and re-run the generator.

**Unity Catalog path**: `03_gold.sc_analytics.<table_name>`

---

## Conventions and freshness

All gold tables are produced by the DLT pipeline and are the source of truth
for the frontend dashboard. They are refreshed nightly after ingestion.

### Player identity

Player-keyed gold and dashboard products use `player_identity_key` as the
relational character key. The key is deterministic and lowercase:

- `player_name:player_class:realm` when class is available (most WCL-derived products)
- `player_name:unknown:realm_slug` for Raider.IO and Blizzard profile products that do not carry class
- legacy realmless rows fall back to the explicit `unknown` sentinel

`dim_player.player_name` is sufficient for joining facts that only carry a
character name; use `player_identity_key` for joins that need to disambiguate
across realms or class changes.

### Mythic+ caveat

Raider.IO data is current-season only. Score history (`gold_player_mplus_score_history`)
begins at the first successful Raider.IO ingestion — it is **not** true
season-start history unless ingestion began at season start. Trend charts
should be clear that they show captured snapshots only.

### Throughput

`throughput_per_second` on `fact_player_fight_performance` and the `*_throughput_per_second`
fields on player summary products come from WCL `rankings.amount`. The value is
already DPS for dps/tank rows or HPS for healer rows — do not recompute from
`playerDetails`. The field is null when WCL has no ranking entry for the
fight/player; do not bucket null as zero.

---

## Primary tier (chatbot-facing)

### dim_player

Canonical per-character dimension. One row per player_name observed in WCL logs, enriched with Blizzard guild rank when the character matches the guild roster. Use this as the join target for any fact table that has only a player_name. For Mythic+ identity (which keys on realm) use the player_identity_key on the Mythic+ products instead.

**Grain**: One row per canonical player name observed in WCL logs, enriched with guild metadata when available.

**Primary key**: `player_name`

**Summary**: Player identity dimension. Use to filter to guild members (`is_guild_member = true`) or the active raid team (`is_raid_team = true`) and to look up rank labels for display. Joins to almost every fact table on `player_name`.

| Column | Type | Description |
|--------|------|-------------|
| `player_name` | string | Character name (from WCL actor data) |
| `player_class` | string | WoW class (from actor roster; falls back to performance data) (nullable) |
| `realm` | string | Realm name (nullable) |
| `is_guild_member` | boolean | Whether matched in Blizzard guild roster |
| `rank` | integer | Guild rank ID (null for non-guild players) Allowed: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9. (nullable) |
| `rank_label` | string | Human-readable rank (Guild Master, Officer, etc.) (nullable) |
| `rank_category` | string | Rank group (GM, Officer, Raider, etc.) Allowed: GM, Officer, Raider, Raider Alt, Bestie, Trial, Social. (nullable) |
| `is_raid_team` | boolean | Whether rank is in the active raid team (ranks 0–5, 8) |
| `first_seen_date` | date | First attendance record date (nullable) |
| `last_seen_date` | date | Most recent attendance record date (nullable) |

**Example questions**:
- Who is on the active raid team?
- List all guild members and their ranks.
- Which characters first appeared in raid logs after a given date?
- How many players are in each rank category?

**Avoid using for**:
- Mythic+ joins — Mythic+ products use `player_identity_key`, not `player_name` alone.
- Counting raid attendance — use `gold_player_attendance` (presence-aware) instead.

---

### fact_player_events

One row per player death event across all WCL reports. The grain is the death itself, not the fight, so a player can have many rows in a single fight. Includes the killing-blow ability when WCL recorded one. Use this for "who dies most", "what kills us most often", and per-ability death breakdowns. Wipe rate / pull counts live in gold_boss_wipe_analysis.

**Grain**: One row per player death event.

**Primary key**: `report_code, fight_id, player_name, death_timestamp_ms`

**Summary**: Death-event fact. Each row is a single death; group by player_name to count deaths, by killing_blow_name to find which mechanics kill most, by encounter to find dangerous fights. Joins to dim_player on player_name and to gold_encounter_catalog on (zone_id, encounter_id) — but encounter_id is not on this table directly, derive from a join on (report_code, fight_id) via fact_player_fight_performance if needed.

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string | WCL report identifier |
| `fight_id` | integer | Fight the death occurred in |
| `player_name` | string | Player who died |
| `player_class` | string | Player's WoW class (nullable) |
| `death_timestamp_ms` | integer | Fight-relative timestamp in milliseconds Unit: `milliseconds`. |
| `overkill` | number | Overkill damage amount on the killing blow Unit: `count`. (nullable) |
| `killing_blow_name` | string | Name of the ability that killed the player (nullable — null if killing blow was from a friendly or events array was empty) (nullable) |
| `killing_blow_id` | integer | Spell ID of the killing blow ability (nullable) |
| `zone_name` | string | Raid zone (from report-level context) (nullable) |
| `zone_id` | integer | WCL zone identifier (nullable) |
| `raid_night_date` | date | Date of the raid night (nullable) |

**Metrics**:
- `death_count` — Count of rows per (player, fight) or per (boss, ability).
- `most_common_killing_blow` — killing_blow_name with the highest count for the chosen scope.

**Example questions**:
- Who dies most often on each boss?
- What are our most common killing blows?
- Which abilities killed players the most last raid night?
- Show death counts per player on Mythic <boss>.

**Avoid using for**:
- Counting wipes — a wipe and a death are different things; use gold_boss_wipe_analysis.
- Computing parse percentiles — use fact_player_fight_performance.
- Treating null killing_blow_name as zero — exclude or label as "unknown".

---

### fact_player_fight_performance

Primary fact table for raid performance analysis. One row per player per boss-kill fight, joining WCL player performance, fight events, and player rankings. Includes throughput, parse percentiles, item level, consumables, and secondary stats. Wipe pulls are not included here — use gold_boss_wipe_analysis or gold_wipe_survival_discipline for wipes.

**Grain**: One row per player on each tracked kill fight.

**Primary key**: `report_code, fight_id, player_name`

**Summary**: Per-player, per-kill performance facts. Use for "best players on boss X", "parse percentiles", "throughput trends", "consumable coverage on kills". For wipes / deaths / survival, use gold_boss_wipe_analysis or gold_wipe_survival_discipline instead — this table only contains kill rows.

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string | WCL report identifier |
| `fight_id` | integer | Fight number within the report |
| `encounter_id` | integer | Stable WCL encounter identifier |
| `boss_name` | string | Boss name |
| `zone_name` | string | Raid zone name (nullable) |
| `difficulty` | integer | Difficulty integer (3=Normal, 4=Heroic, 5=Mythic) Allowed: 3, 4, 5. (nullable) |
| `difficulty_label` | string | Human-readable difficulty (nullable) |
| `raid_night_date` | date | Date of the raid night (nullable) |
| `is_kill` | boolean | Always true (table filters to kills only) |
| `duration_seconds` | number | Fight duration in seconds Unit: `seconds`. (nullable) |
| `player_name` | string | Player character name |
| `player_class` | string | WoW class (from WCL actor data) (nullable) |
| `role` | string | Player role for this fight. Allowed: dps, healer, tank. (nullable) |
| `spec` | string | Specialisation name (nullable) |
| `avg_item_level` | number | Average item level of equipped gear (nullable) |
| `potion_use` | integer | Number of potions used (from combatantInfo) |
| `combat_potion_casts` | integer | Number of damage/health combat-potion casts (DPS/healer combat usage signal). (nullable) |
| `combat_potion_names` | string | Comma-separated list of combat-potion names cast (raw aura/cast names). (nullable) |
| `healthstone_use` | integer | Number of healthstones used (nullable) |
| `has_food_buff` | boolean | Whether a recognised raid food buff was active during the fight. |
| `food_buff_names` | string | Comma-separated raid food buff names that were active. (nullable) |
| `has_flask_or_phial_buff` | boolean | Whether a flask or phial buff was active during the fight. |
| `flask_or_phial_names` | string | Comma-separated flask/phial buff names that were active. (nullable) |
| `has_weapon_enhancement` | boolean | Whether a weapon enhancement (e.g. rune/stone/oil/imbue) was applied. |
| `weapon_enhancement_names` | string | Comma-separated weapon enhancement names that were applied. (nullable) |
| `crit_rating` | integer | Critical strike rating (nullable) |
| `haste_rating` | integer | Haste rating (nullable) |
| `mastery_rating` | integer | Mastery rating (nullable) |
| `versatility_rating` | integer | Versatility rating (nullable) |
| `throughput_per_second` | number | Role-aware throughput from WCL rankings.amount: DPS for dps/tank rows, HPS for healer rows (nullable — null when no WCL ranking for this fight/player) Unit: `per_second`. (nullable) |
| `rank_percent` | number | WCL parse percentile (0–100), role-aware (DPS parse for dps/tank, HPS parse for healer) Unit: `percent_0_100`. (nullable) |
| `bracket_percent` | number | WCL bracket (item level bracket) percentile Unit: `percent_0_100`. (nullable) |
| `rank_string` | string | Approximate rank position (e.g. "~1265") (nullable) |

**Metrics**:
- `throughput_per_second` — Role-aware DPS for dps/tank or HPS for healer, sourced from WCL rankings.amount.
- `rank_percent` — WCL parse percentile (0–100), role-aware.

**Example questions**:
- Which players have the best parse percentiles on Mythic <boss>?
- What is the average DPS per fight on heroic <boss> by player?
- Who is improving over time on <boss> by parse percentile?
- Which players consistently use flasks and food on kill fights?

**Avoid using for**:
- Counting wipes — this table is filtered to kills (is_kill=true).
- Death analysis — use fact_player_events for deaths.
- Bucketing nullable parse fields as zero.

---

### gold_boss_mechanics

Per-(encounter, difficulty) wipe-mechanics breakdown. Reports the share of wipes by phase bucket (P1, P2, P3+), wipes by duration bucket (<1m, 1–3m, 3–5m, 5m+), and a progress_trend that compares the last week average wipe HP% to the all-time average. Use for "where are we wiping on this boss" (phase) and "are we improving" (trend).

**Grain**: One row per boss encounter, boss name, zone, and difficulty with wipe mechanics metrics.

**Primary key**: `encounter_id, boss_name, zone_name, difficulty, difficulty_label`

**Summary**: Per-encounter wipe diagnosis. Use for "which phase do we wipe in most", "are we trending closer to a kill on <boss>", or "do we have lots of fast/early wipes". Pair with gold_boss_wipe_analysis for HP% and best-pull detail.

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer | Stable WCL encounter identifier. |
| `boss_name` | string | Boss name. |
| `zone_name` | string | Raid zone name. |
| `difficulty` | integer | Difficulty integer (3=Normal, 4=Heroic, 5=Mythic). Allowed: 3, 4, 5. |
| `difficulty_label` | string | Human-readable difficulty. Allowed: Normal, Heroic, Mythic. |
| `total_wipes` | integer | Total wipes counted in this rollup. Unit: `count`. |
| `avg_boss_pct` | number | Average boss HP% remaining at wipe (closer to 0 = closer to a kill). Unit: `percent_0_100`. (nullable) |
| `pct_wipes_phase_1` | number | Share of wipes (0–100) that ended in phase 1. Unit: `percent_0_100`. |
| `pct_wipes_phase_2` | number | Share of wipes (0–100) that ended in phase 2. Unit: `percent_0_100`. |
| `pct_wipes_phase_3_plus` | number | Share of wipes (0–100) that ended in phase 3 or later. Unit: `percent_0_100`. |
| `wipes_lt_1min` | integer | Wipes shorter than 1 minute. Unit: `count`. |
| `wipes_1_3min` | integer | Wipes between 1 and 3 minutes. Unit: `count`. |
| `wipes_3_5min` | integer | Wipes between 3 and 5 minutes. Unit: `count`. |
| `wipes_5plus_min` | integer | Wipes 5 minutes or longer. Unit: `count`. |
| `last_week_avg_boss_pct` | number | Average wipe boss HP% over the last 7 days; null when no recent wipes. Unit: `percent_0_100`. (nullable) |
| `progress_trend` | number | all-time avg_boss_pct minus last_week_avg_boss_pct; positive = recent wipes are closer to kill. Unit: `percent_0_100`. (nullable) |

**Metrics**:
- `progress_trend` — avg_boss_pct minus last_week_avg_boss_pct.

**Example questions**:
- Which phase are we wiping in most on Mythic <boss>?
- Are we improving on <boss> this week?
- Which bosses have the most fast (<1 minute) wipes?
- What share of <boss> wipes reach phase 3?

**Avoid using for**:
- Counting kills — this is wipe-only.
- Player-level analysis — no player columns.
- Comparing absolute wipe counts across bosses with vastly different attempt counts (use ratios).

---

### gold_boss_progression

Per-encounter, per-difficulty progression rollup. Includes total pulls, kills, wipes, best kill time, and a wipe-to-kill ratio for each boss the guild has engaged. Use this as the primary progression scoreboard. For raid-night-level rollups use gold_raid_summary; for per-pull detail use gold_boss_pull_history; for wipe-only diagnosis use gold_boss_wipe_analysis.

**Grain**: One row per encounter, boss, zone, and difficulty.

**Primary key**: `encounter_id, boss_name, zone_id, zone_name, difficulty`

**Summary**: Per-encounter progression scoreboard. Rows show how many pulls/kills/wipes per boss-difficulty pair, with best kill time and wipe ratio. Filter by difficulty_label and zone_name for tier-specific views.

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer | Stable WCL encounter identifier. |
| `boss_name` | string | Boss name. |
| `zone_id` | integer | WCL zone identifier. |
| `zone_name` | string | Raid zone name. |
| `difficulty` | integer | Difficulty integer (3=Normal, 4=Heroic, 5=Mythic). Allowed: 3, 4, 5. |
| `difficulty_label` | string | Human-readable difficulty. Allowed: Normal, Heroic, Mythic. |
| `total_pulls` | integer | Total pulls (kills + wipes). Unit: `count`. |
| `total_kills` | integer | Number of successful kills. Unit: `count`. |
| `total_wipes` | integer | Number of wipes. Unit: `count`. |
| `best_kill_seconds` | number | Fastest kill duration recorded; null if never killed. Unit: `seconds`. (nullable) |
| `avg_pull_duration_seconds` | number | Average pull duration across all pulls (kills and wipes). Unit: `seconds`. (nullable) |
| `is_killed` | boolean | Whether the boss has ever been killed at this difficulty. (nullable) |
| `first_kill_date` | date | Date of the first kill at this difficulty; null if never killed. (nullable) |
| `last_attempt_date` | date | Date of the most recent attempt (kill or wipe). (nullable) |
| `wipe_to_kill_ratio` | number | total_wipes / max(total_kills, 1) — higher means more attempts per kill. Unit: `ratio`. |

**Metrics**:
- `wipe_to_kill_ratio` — total_wipes / max(total_kills, 1).
- `best_kill_seconds` — Fastest successful kill at this difficulty.

**Example questions**:
- Which bosses have we killed on Mythic?
- Which bosses are we wiping on most?
- Show progression on the current tier ordered by wipe-to-kill ratio.
- When did we first kill <boss> on Heroic?
- Which Mythic bosses have we attempted but not killed?

**Avoid using for**:
- Per-night attendance — use gold_raid_summary or gold_player_attendance.
- Per-pull mechanics analysis — use gold_boss_pull_history or gold_boss_mechanics.
- Player-level diagnosis — this table has no player columns.

---

### gold_boss_wipe_analysis

Per-(encounter, difficulty) wipe rollup. Includes how many wipes, best (lowest remaining HP%) wipe, average wipe HP%, max phase reached, and wipe durations. Use this to answer "where are we wiping?" and "are we improving on this boss?". For per-pull mechanics use gold_boss_mechanics; for player survival diagnosis use gold_wipe_survival_discipline.

**Grain**: One row per boss encounter and difficulty with tracked wipes.

**Primary key**: `encounter_id, zone_name, difficulty`

**Summary**: Wipe-side counterpart to gold_boss_progression. Use for "where are we wiping?", "what is our best pull on <boss>?", "are we making progress (avg_wipe_pct trending down)?". Lower best_wipe_pct = closer to killing.

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer | Stable WCL encounter identifier. |
| `boss_name` | string | Boss name. |
| `zone_name` | string | Raid zone name. |
| `difficulty` | integer | Difficulty integer (3=Normal, 4=Heroic, 5=Mythic). Allowed: 3, 4, 5. |
| `difficulty_label` | string | Human-readable difficulty. Allowed: Normal, Heroic, Mythic. |
| `total_wipes` | integer | Number of wipes recorded against this (encounter, difficulty). Unit: `count`. |
| `best_wipe_pct` | number | Lowest remaining boss HP% across all wipes (closer to 0 = closer to a kill). Unit: `percent_0_100`. (nullable) |
| `avg_wipe_pct` | number | Average remaining boss HP% across wipes. Unit: `percent_0_100`. (nullable) |
| `avg_last_phase` | number | Average final phase reached across wipes. (nullable) |
| `max_phase_reached` | integer | Furthest phase ever reached on a wipe. (nullable) |
| `avg_wipe_duration_seconds` | number | Average wipe duration. Unit: `seconds`. (nullable) |
| `longest_wipe_seconds` | number | Longest wipe duration ever recorded. Unit: `seconds`. (nullable) |
| `first_wipe_date` | date | Date of the first recorded wipe at this (encounter, difficulty). (nullable) |
| `latest_wipe_date` | date | Date of the most recent wipe. (nullable) |
| `raid_nights_attempted` | integer | Distinct raid nights with at least one pull on this (encounter, difficulty). Unit: `count`. |
| `avg_wipe_pct_rounded` | number | avg_wipe_pct rounded for display use. Unit: `percent_0_100`. (nullable) |

**Metrics**:
- `best_wipe_pct` — Lowest boss HP% remaining across all wipes — primary "how close are we" signal.
- `avg_wipe_pct` — Mean boss HP% remaining across wipes.

**Example questions**:
- Which bosses are we wiping on most this tier?
- What is our best pull (lowest HP%) on Mythic <boss>?
- How many raid nights have we spent on <boss>?
- Which Mythic bosses are we farthest from killing?

**Avoid using for**:
- Counting kills — use gold_boss_progression.total_kills.
- Per-pull or per-player analysis — use gold_boss_pull_history or gold_wipe_survival_discipline.
- Computing parse percentiles — wipes don't generate parses.

---

### gold_encounter_catalog

Reference dimension of active raid encounters. One row per (zone_id, encounter_id). Use to translate encounter_id ↔ encounter_name, list bosses in a zone, or filter facts to current-tier encounters. Only active (non-frozen) zones are included.

**Grain**: One row per active raid-zone encounter reference.

**Primary key**: `zone_id, encounter_id`

**Summary**: Encounter dimension. Join on encounter_id to get the boss name when a fact table only has encounter_id, or join on zone_id to filter to a specific raid zone. Active zones only — old tier encounters are excluded.

| Column | Type | Description |
|--------|------|-------------|
| `zone_id` | integer | WCL zone identifier. |
| `zone_name` | string | Raid zone name. |
| `encounter_id` | integer | Stable WCL encounter identifier. |
| `encounter_name` | string | Boss name. |
| `difficulty_names` | array | Available difficulty names for this encounter (e.g. [Normal, Heroic, Mythic]). (nullable) |

**Example questions**:
- List all bosses in the current raid zone.
- What is the boss name for encounter_id = X?
- Which encounters are available on Mythic difficulty?

**Avoid using for**:
- Old tier / frozen encounter lookups — those are excluded.
- Difficulty-specific keys — difficulty is not part of the primary key here; join on encounter_id only.

---

### gold_player_mplus_score_history

Time series of Raider.IO score snapshots per character. One row per nightly snapshot per (player_identity_key, region, season). History starts at the first successful ingestion — it is NOT season-start history unless ingestion began at season start. Use this for score-trend questions; for the latest snapshot only, use gold_player_mplus_summary.

**Grain**: One row per Raider.IO player score snapshot.

**Primary key**: `player_identity_key, region, season, snapshot_at`

**Summary**: Mythic+ score time series. Use for "who is improving in M+", "score trend for player X", or "rate of score change over the last N days". Snapshots are nightly when ingestion runs.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase player_name:unknown:realm_slug identity key. |
| `player_name` | string | Character name |
| `realm_slug` | string | Realm slug |
| `region` | string | Raider.IO region code. Allowed: eu, us, kr, tw. |
| `profile_url` | string | Raider.IO character profile URL (nullable) |
| `season` | string | Raider.IO season identifier |
| `snapshot_at` | timestamp | Ingestion timestamp |
| `snapshot_date` | date | Snapshot date |
| `score_all` | number | Overall Raider.IO score at this snapshot. Unit: `score`. (nullable) |
| `score_dps` | number | DPS role score at this snapshot. Unit: `score`. (nullable) |
| `score_healer` | number | Healer role score at this snapshot. Unit: `score`. (nullable) |
| `score_tank` | number | Tank role score at this snapshot. Unit: `score`. (nullable) |
| `world_rank` | integer | Overall world rank (nullable) |
| `region_rank` | integer | Overall region rank (nullable) |
| `realm_rank` | integer | Overall realm rank (nullable) |
| `raiderio_last_crawled_at` | timestamp | Raider.IO crawl timestamp from the source payload (nullable) |

**Metrics**:
- `score_delta` — score_all minus the prior snapshot's score_all for the same (player_identity_key, season).

**Example questions**:
- Who has the largest M+ score gain in the last 30 days?
- Show player X's score trend this season.
- Which players have stopped improving (no score change in two weeks)?
- When did player X first cross 3000 score?

**Avoid using for**:
- Latest snapshot lookups — use gold_player_mplus_summary which already filters to latest.
- Run-level analysis — this is score-only; for runs use gold_player_mplus_run_history.
- Treating start-of-history as start-of-season — history begins at first ingestion.

---

### gold_player_mplus_summary

Per-player current Mythic+ snapshot derived from Raider.IO. One row per (player_identity_key, region, season). Includes overall and role scores, world/region/realm/guild ranks, run counts (timed and untimed), highest key levels, and a single "best run" highlight. For score-over-time use gold_player_mplus_score_history; for per-dungeon coverage use gold_player_mplus_dungeon_breakdown; for full run logs use gold_player_mplus_run_history.

**Grain**: One row per player identity, region, and Raider.IO season.

**Primary key**: `player_identity_key, region, season`

**Summary**: Per-player current Mythic+ scoreboard from Raider.IO. Use for "who has the highest M+ score", "guild M+ ranking", or per-role score comparisons. Filter to non-zero score_all to scope to active players. For trend questions use gold_player_mplus_score_history.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase player_name:unknown:realm_slug identity key (Mythic+ has no class context). |
| `player_name` | string | Character name |
| `realm_slug` | string | Realm slug |
| `region` | string | Raider.IO region code. Allowed: eu, us, kr, tw. |
| `profile_url` | string | Raider.IO character profile URL (nullable) |
| `season` | string | Raider.IO season identifier |
| `snapshot_at` | timestamp | Ingestion timestamp for the latest score snapshot |
| `score_all` | number | Overall Raider.IO score Unit: `score`. (nullable) |
| `score_dps` | number | DPS role score Unit: `score`. (nullable) |
| `score_healer` | number | Healer role score Unit: `score`. (nullable) |
| `score_tank` | number | Tank role score Unit: `score`. (nullable) |
| `world_rank` | integer | Overall world rank if returned by Raider.IO (nullable) |
| `region_rank` | integer | Overall region rank (nullable) |
| `realm_rank` | integer | Overall realm rank (nullable) |
| `guild_mplus_rank` | integer | Guild rank by current-season overall score (nullable) |
| `guild_mplus_rank_total` | integer | Number of guild players with non-zero current-season score |
| `guild_mplus_rank_percentile` | number | 0-100 percentile derived from guild rank (nullable) |
| `total_runs` | integer | Deduped recent/best runs exported for this character |
| `timed_runs` | integer | Runs completed in time |
| `untimed_runs` | integer | Runs not completed in time |
| `highest_timed_level` | integer | Highest timed key level (nullable) |
| `highest_untimed_level` | integer | Highest untimed key level (nullable) |
| `most_common_key_level` | integer | Most common key level in exported run payload (nullable) |
| `most_common_key_count` | integer | Count at the most common key level |
| `best_run_dungeon` | string | Dungeon for the selected best run (nullable) |
| `best_run_short_name` | string | Short dungeon name (nullable) |
| `best_run_level` | integer | Key level for the selected best run (nullable) |
| `best_run_score` | number | Raider.IO run score (nullable) |
| `best_run_timed` | boolean | Whether the selected best run was timed (nullable) |
| `best_run_completed_at` | timestamp | Completion timestamp (nullable) |
| `best_run_url` | string | Raider.IO run URL (nullable) |

**Metrics**:
- `score_all` — Overall Raider.IO season score for the character.
- `highest_timed_level` — Highest key level the character timed in this season.

**Example questions**:
- Who has the highest current Mythic+ score?
- List the guild Mythic+ ranking by overall score.
- Which players have the highest tank role score?
- What is the best timed key level recorded by any guild member?

**Avoid using for**:
- Joining to raid facts on player_name alone — Mythic+ identity uses (player_name, realm_slug).
- Score trend over time — use gold_player_mplus_score_history.
- Per-dungeon coverage — use gold_player_mplus_dungeon_breakdown.

---

### gold_player_performance_summary

Per-player, per-role aggregate of all kill fights tracked. Use this for "best throughput by player", "average parse for player X", and ranked leaderboards. The grain is (player_identity_key, role) — players who raid as multiple roles will have one row per role.

**Grain**: One row per player identity and role.

**Primary key**: `player_identity_key, role`

**Summary**: Aggregate per-player throughput and parse leaderboard. Use for "best DPS overall", "average parse for player X", or "top healers by HPS". For per-boss leaderboards, prefer gold_player_boss_performance. For per-fight detail, use fact_player_fight_performance directly.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase `player_name:player_class:realm` identity fingerprint |
| `player_name` | string | Character name |
| `player_class` | string | WoW class |
| `realm` | string | Realm resolved from WCL actor roster, or `unknown` |
| `role` | string | Player role for this row. Allowed: dps, healer, tank. (nullable) |
| `primary_spec` | string | Most frequently played spec (nullable) |
| `kills_tracked` | integer | Number of kill fights included |
| `avg_throughput_per_second` | number | Average role-aware throughput (DPS for dps/tank, HPS for healer); null when WCL ranking amount is unavailable Unit: `per_second`. (nullable) |
| `best_throughput_per_second` | number | Single-fight best role-aware throughput (DPS for dps/tank, HPS for healer); null when WCL ranking amount is unavailable Unit: `per_second`. (nullable) |
| `avg_rank_percent` | number | Average WCL parse percentile, role-aware (DPS parse / HPS parse) Unit: `percent_0_100`. (nullable) |
| `best_rank_percent` | number | Highest single-fight parse, role-aware (DPS parse / HPS parse) Unit: `percent_0_100`. (nullable) |
| `avg_item_level` | number | Average equipped item level (nullable) |
| `last_seen_date` | date | Most recent kill date (nullable) |

**Metrics**:
- `avg_throughput_per_second` — Mean of role-aware per-second throughput across all kill fights for this (player, role).
- `avg_rank_percent` — Mean WCL parse percentile (0–100) across kill fights.

**Example questions**:
- Who has the best average parse percentile this tier?
- List the top 10 DPS by best single-fight throughput.
- What is player X's average HPS across all kill fights?
- Which healers have the highest average rank percent?

**Avoid using for**:
- Per-fight analysis — this is already aggregated, use fact_player_fight_performance for fights.
- Counting attendance — use gold_player_attendance.
- Comparing players across roles directly — DPS and HPS are not interchangeable.

---

### gold_player_survivability_rankings

Per-player survivability rank within a display scope (zone, boss, difficulty), where rank 1 is the best (lowest deaths-per-kill). Use this for "who has the best survival rate", "where does player X rank in survivability", or "top 5 survival players this tier". Pair with gold_wipe_survival_discipline for the wipe-discipline view (deaths on wipes, defensives, healthstones).

**Grain**: One row per player and stable player-detail display scope.

**Primary key**: `player_identity_key, zone_name, boss_name, difficulty_label`

**Summary**: Player survivability leaderboard within a display scope. Rank 1 = best (lowest deaths_per_kill); the percentile field is derived from rank. Filter on (zone_name, boss_name, difficulty_label) to scope; use the literal string `All` for cross-scope rows.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase player_name:player_class:realm identity key. |
| `player_name` | string | Character name |
| `player_class` | string | WarcraftLogs class |
| `zone_name` | string | Raid zone, or `All` |
| `encounter_id` | integer | Encounter id when the display boss scope maps to one encounter; null for all-boss or ambiguous all-zone scopes (nullable) |
| `boss_name` | string | Boss name, or `All` |
| `difficulty` | integer | Difficulty id when the display difficulty scope maps to one id; null for all-difficulty or ambiguous scopes (nullable) |
| `difficulty_label` | string | Difficulty label, or `All` |
| `deaths` | integer | Death count in scope. Unit: `count`. |
| `kills` | integer | Kill roster rows in scope. Unit: `count`. |
| `deaths_per_kill` | number | Deaths divided by kills; lower is better. Unit: `ratio`. |
| `survivability_rank` | integer | Rank in scope; 1 is best survivability. |
| `survivability_rank_total` | integer | Number of ranked players in scope. Unit: `count`. |
| `survivability_rank_percentile` | number | 0-100 percentile derived from rank (100 = best). Unit: `percent_0_100`. |

**Metrics**:
- `deaths_per_kill` — deaths / max(kills, 1) within scope.
- `survivability_rank_percentile` — 100 * (1 - (survivability_rank - 1) / max(survivability_rank_total - 1, 1)).

**Example questions**:
- Who has the best survivability this tier?
- List the top 5 most survivable players on Mythic <boss>.
- Where does player X rank in survivability?
- Which players have the worst deaths-per-kill ratio?

**Avoid using for**:
- Wipe-side analysis — use gold_wipe_survival_discipline.
- Per-fight death events — use fact_player_events.
- Comparing scopes without a (zone_name, boss_name, difficulty_label) filter.

---

### gold_preparation_readiness

Per-raider current-tier preparation scoreboard. Combines food, flask/phial, weapon-enhancement, and combat-potion coverage on kill fights into a 0–100 readiness_score with a categorical readiness_label (watch/steady/strong) and a `weakest_signal_label` indicating where to improve. Combat-potion usage is displayed for all roles but contributes to scoring only for DPS (per project convention). Scope is current tier only — historical consumable usage lives in fact_player_fight_performance.

**Grain**: One row per current raid-team preparation identity.

**Primary key**: `identity_key`

**Summary**: Current-tier preparation scoreboard. Per-raider 0–100 readiness_score composed of food, flask/phial, weapon-enhancement, and (DPS-only) combat potion coverage on kill fights. Use for "who has poor consumable usage", "who is least prepared", or "what is player X's weakest consumable signal". Combat potions display for all roles but only contribute to scoring for DPS.

| Column | Type | Description |
|--------|------|-------------|
| `identity_key` | string | Character or published override identity key |
| `player_name` | string | Display name |
| `player_class` | string | Latest known class or roster class |
| `role` | string | Player role at current tier. Allowed: dps, healer, tank, unknown. |
| `rank_label` | string | Roster rank label (nullable) |
| `is_active` | boolean | Active roster flag |
| `current_tier` | string | Current raid tier |
| `roster_source` | string | Source roster used to generate this row. Allowed: live_raid_roster, gold_raid_team. |
| `has_current_tier_data` | boolean | Whether current-tier kill rows exist |
| `attendance_rate_pct` | number | Current-tier raid nights present / raid nights tracked × 100 Unit: `percent_0_100`. |
| `raids_present` | integer | Current-tier raid nights with at least one kill row |
| `total_raids_tracked` | integer | Current-tier raid nights tracked |
| `kills_tracked` | integer | Current-tier boss kill rows |
| `food_rate` | number | Share (0–1) of current-tier kill fights with a recognised food buff. Unit: `ratio_0_1`. |
| `flask_rate` | number | Share (0–1) of current-tier kill fights with a flask or phial. Unit: `ratio_0_1`. |
| `weapon_rate` | number | Share (0–1) of current-tier kill fights with a weapon enhancement applied. Unit: `ratio_0_1`. |
| `combat_potion_rate` | number | Share (0–1) of current-tier kill fights with a combat-potion cast (DPS-only signal for scoring; displayed for all roles). Unit: `ratio_0_1`. |
| `readiness_score` | number | 0–100 readiness composite owned by Gold. Unit: `score`. |
| `readiness_label` | string | Categorical readiness bucket. Allowed: watch, steady, strong. |
| `readiness_notes` | string | Pipe-delimited Gold-owned notes |
| `spec` | string | Most recent specialisation observed for this identity, or empty. |
| `latest_avg_item_level` | number | Most recent equipped average item level. (nullable) |
| `latest_kill_date` | string | ISO date of the most recent current-tier kill, or empty if none. |
| `weakest_signal_label` | string | Lowest preparation component label |
| `recent_food_names` | string | Latest non-empty current-tier food buff names, or empty string |
| `recent_flask_names` | string | Latest non-empty current-tier flask/phial names, or empty string |
| `recent_weapon_names` | string | Latest non-empty current-tier weapon enhancement names, or empty string |
| `recent_combat_potion_names` | string | Latest non-empty classified combat potion names. If potion usage exists but the event name is unavailable, `Combat potion used`; otherwise empty string |
| `character_names` | string | Pipe-delimited characters included in the identity |
| `override_label` | string | Identity-pool override applied to this row. Allowed: , Replace, Pool. |

**Metrics**:
- `readiness_score` — 0–100 composite of food, flask, weapon, and (DPS-only) combat-potion coverage on current-tier kills.
- `food_rate` — Share of current-tier kills with a recognised food buff active.
- `flask_rate` — Share of current-tier kills with a flask or phial active.

**Example questions**:
- Who has poor consumable usage on the current tier?
- Which raiders need to improve their flask/phial coverage?
- List players in the `watch` readiness bucket.
- What is player X's weakest preparation signal?
- Who has the highest readiness_score this tier?

**Avoid using for**:
- Historical (cross-tier) consumable usage — use fact_player_fight_performance.
- Wipe-side discipline — use gold_wipe_survival_discipline (defensives, healthstones, potions on wipes).
- Counting raid attendance independently — use gold_player_attendance.

---

### gold_raid_summary

Per-raid-night rollup. One row per WCL report — covers report timing, zone, primary difficulty, total pulls, kills, wipes, and unique bosses engaged. Use this for "what happened on raid night X" or weekly activity questions. For per-encounter progression use gold_boss_progression; for per-week rollups use gold_weekly_activity.

**Grain**: One row per WarcraftLogs report.

**Primary key**: `report_code`

**Summary**: Raid-night summary table. Use to ask "how did raid go on date X", "how many pulls did we do this week", or "what's our kill count over time". Joins to fact_player_fight_performance and fact_player_events on report_code.

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string | WCL report identifier. |
| `report_title` | string | Report title as set in WCL. |
| `start_time_utc` | timestamp | Report start time in UTC. (nullable) |
| `end_time_utc` | timestamp | Report end time in UTC. (nullable) |
| `zone_id` | integer | WCL zone identifier. (nullable) |
| `zone_name` | string | Raid zone name. |
| `raid_night_date` | date | Date the raid took place (UTC). |
| `primary_difficulty` | string | Most-attempted difficulty on this report. Allowed: Normal, Heroic, Mythic. (nullable) |
| `total_pulls` | integer | Total pulls (kills + wipes) on this report. Unit: `count`. |
| `boss_kills` | integer | Number of successful boss kills on this report. Unit: `count`. |
| `total_wipes` | integer | Number of wipes on this report. Unit: `count`. |
| `total_fight_seconds` | number | Sum of active fight durations across all pulls. Unit: `seconds`. |
| `unique_bosses_engaged` | integer | Distinct bosses pulled on this report. Unit: `count`. |
| `unique_bosses_killed` | integer | Distinct bosses killed on this report. Unit: `count`. |

**Metrics**:
- `kills_per_pull` — boss_kills / max(total_pulls, 1).

**Example questions**:
- How many pulls did we do last raid night?
- Show kill count by raid night for the last month.
- Which raid night had the most wipes?
- How many unique bosses did we engage this week?

**Avoid using for**:
- Per-encounter progression — use gold_boss_progression.
- Player-level analysis — this table has no player columns.
- Weekly rollups — use gold_weekly_activity for ISO-week aggregation.

---

### gold_wipe_survival_discipline

Per-(player, scope) wipe-discipline diagnosis. Each row evaluates a player on one stable scope (typically all-zones, a specific zone, or a specific (zone, boss, difficulty)). It scores how often the player dies on wipes, whether they used personal defensives, healthstones, and combat health potions, and combines those into a 0–100 survival_discipline_score. Use this to answer "who is dying most on wipes", "who has poor cooldown discipline", "what should player X improve". For raid-team unused cooldown capacity (raid CDs / externals) use gold_wipe_cooldown_utilization.

**Grain**: One row per player and stable Wipe Analysis dashboard scope.

**Primary key**: `player_identity_key, zone_name, boss_name, difficulty_label`

**Summary**: Player-side wipe diagnosis with a 0–100 survival_discipline_score (higher is better). Use for "who has the worst survival discipline", "who is not using defensives", "what should player X improve". Pair with fact_player_events for raw death detail and with gold_wipe_cooldown_utilization for raid-team-level cooldown coverage.

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase player_name:player_class:realm identity key. |
| `player_name` | string | Character name |
| `player_class` | string | WarcraftLogs class |
| `realm` | string | Realm resolved from WCL actor roster, or `unknown`. |
| `role` | string | Latest known kill-role when available. Allowed: dps, healer, tank. (nullable) |
| `zone_name` | string | Raid zone, or `All` |
| `encounter_id` | integer | Encounter id; null for all-boss scopes (nullable) |
| `boss_name` | string | Boss name, or `All` |
| `difficulty` | integer | WCL difficulty; null for all-difficulty scopes (nullable) |
| `difficulty_label` | string | Difficulty label, or `All` |
| `wipe_pulls_tracked` | integer | Instrumented wipe pulls where the player was present |
| `wipe_deaths` | integer | Player deaths on tracked wipe pulls |
| `first_deaths` | integer | Wipe deaths that were first in the pull |
| `early_deaths` | integer | Wipe deaths within the first minute |
| `kill_deaths` | integer | Player deaths on kill pulls in scope |
| `kills_tracked` | integer | Kill pulls where the player was present |
| `deaths_per_kill` | number | `kill_deaths / kills_tracked`; null when no kills are tracked (nullable) |
| `deaths_per_wipe` | number | `wipe_deaths / wipe_pulls_tracked` |
| `pulls_with_tracked_defensive_capacity` | integer | Wipe pulls with scored personal defensive capacity |
| `tracked_defensive_capacity` | integer | Scored possible personal defensive casts |
| `defensive_casts` | integer | Scored personal defensive casts used |
| `defensive_missed_casts` | integer | Scored possible defensive casts left unused |
| `healthstone_uses` | integer | Wipe deaths with healthstone use before death |
| `potion_uses` | integer | Wipe deaths with health potion use before death |
| `no_healthstone_deaths` | integer | Wipe deaths where the player did NOT use a healthstone before dying. Unit: `count`. |
| `no_health_potion_deaths` | integer | Wipe deaths where the player did NOT use a health potion before dying. Unit: `count`. |
| `defensive_usage_rate` | number | `defensive_casts / tracked_defensive_capacity * 100`; null when no defensive capacity is tracked (nullable) |
| `healthstone_usage_rate` | number | `healthstone_uses / wipe_deaths * 100`; null when no wipe deaths exist (nullable) |
| `potion_usage_rate` | number | `potion_uses / wipe_deaths * 100`; null when no wipe deaths exist (nullable) |
| `no_healthstone_pct` | number | no_healthstone_deaths / max(wipe_deaths, 1) * 100. Unit: `percent_0_100`. |
| `no_health_potion_pct` | number | no_health_potion_deaths / max(wipe_deaths, 1) * 100. Unit: `percent_0_100`. |
| `death_pressure_score` | number | 0–100 component score for avoiding wipe deaths |
| `defensive_component_score` | number | 0–100 class-baselined defensive component; unknown and no-capacity states receive a neutral component while `defensive_tracking_status` preserves the distinction |
| `healthstone_component_score` | number | 0–100 healthstone discipline component |
| `potion_component_score` | number | 0–100 potion discipline component |
| `defensive_class_baseline_pct` | number | Scope/class median defensive usage rate for tracked-capacity players (nullable) |
| `defensive_class_delta_pct` | number | Player's defensive_usage_rate minus the class baseline; positive = above class median. Unit: `percent_0_100`. (nullable) |
| `weighted_failure_points` | number | Internal weighted sum of survival failure components used to derive survival_discipline_score; lower is better. |
| `survival_failure_score` | number | Back-compatible alias for `survival_discipline_score` |
| `survival_discipline_score` | number | Absolute 0–100 consistency score; higher is better |
| `top_improvement_area` | string | Lowest component label, or `—` when all components are strong |
| `top_missing_category` | string | Component label most responsible for the failure score (e.g. defensives, healthstones, potions, deaths). |
| `defensive_tracking_status` | string | `tracked_used`, `tracked_zero_usage`, `no_tracked_capacity`, or `unknown` |
| `has_defensive_capacity_tracked` | boolean | True when the player has scoreable defensive capacity for this scope; false when class/spec has no tracked defensives or no pulls had capacity tracking. |
| `most_common_killing_blow` | string | Most frequent killing-blow ability against this player on wipes in scope (empty when no wipe deaths). |
| `most_common_killing_blow_count` | integer | Count for most_common_killing_blow. Unit: `count`. |
| `survival_grade` | string | Per-scope relative letter grade derived from survival_discipline_score within the same (zone_name, boss_name, difficulty_label) partition; S = best, F = worst. Allowed: S, A, B, C, D, E, F. |

**Metrics**:
- `survival_discipline_score` — 0–100 composite of personal defensives, healthstones, potions, and death pressure on wipes; higher is better.
- `deaths_per_wipe` — wipe_deaths / wipe_pulls_tracked.
- `defensive_usage_rate` — defensive_casts / tracked_defensive_capacity * 100.
- `survival_grade` — S/A/B/C/D/E/F letter grade for survival_discipline_score within the same (zone_name, boss_name, difficulty_label) partition. Computed as a dense-rank-based relative bucketing — single-distinct-score partitions receive S; otherwise the highest distinct score gets S, the lowest F, and the middle ranks are bucketed by middle_relative = (rank-2)/(n-3) with thresholds 0.8/0.6/0.4/0.2 mapping to A/B/C/D, else E.

**Example questions**:
- Who has the worst survival discipline this tier?
- Which players never use healthstones on wipes?
- What is player X's top improvement area?
- Who has poor defensive cooldown usage relative to their class?
- Which abilities most often kill player X on wipes?

**Avoid using for**:
- Counting kills — this is wipe-discipline, not kill performance.
- Raid-team cooldown coverage — use gold_wipe_cooldown_utilization.
- Per-pull death events — use fact_player_events.
- Comparing across all-scope and specific-scope rows in the same query without a scope filter.

---

## Secondary tier

### dim_guild_member

**Grain**: One row per Blizzard guild member character.

**Primary key**: `name, realm_slug`

| Column | Type | Description |
|--------|------|-------------|
| `name` | string | Character name |
| `realm_slug` | string | Realm slug (lowercase, hyphenated) |
| `rank` | integer | Guild rank ID (0–9) |
| `rank_label` | string | Human-readable rank label (nullable) |
| `rank_category` | string | Rank category grouping (nullable) |
| `is_raid_team` | boolean | Active raid team member flag |
| `class_id` | integer | Blizzard class ID (1–13) (nullable) |
| `class_name` | string | Derived WoW class name (nullable) |
| `level` | integer | Character level (nullable) |
| `total_raids_tracked` | integer | Total WCL raid reports in attendance data |
| `raids_present` | integer | Reports where player was marked present |
| `attendance_rate_pct` | number | Attendance % (raids_present / total_raids_tracked × 100) |
| `last_raid_date` | date | Most recent raid attendance date (nullable) |
| `first_raid_date` | date | First raid attendance date (nullable) |
| `is_active` | boolean | Attendance ≥ 25% OR rank IN (0, 1, 2) |
| `possible_main` | string | Character's own name (alt resolution handled in gold_raid_team) (nullable) |
| `_ingested_at` | timestamp | (nullable) |

---

### gold_best_kills

**Grain**: One row per encounter, boss, zone, and difficulty with at least one kill.

**Primary key**: `encounter_id, boss_name, zone_name, difficulty`

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `best_kill_seconds` | number |  |
| `avg_kill_seconds` | number |  |
| `total_kills` | integer |  |
| `first_kill_date` | date |  |
| `latest_kill_date` | date |  |
| `best_kill_mm_ss` | string |  |

---

### gold_boss_kill_roster

**Grain**: One row per player on each tracked boss kill.

**Primary key**: `report_code, fight_id, player_identity_key`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string |  |
| `fight_id` | integer |  |
| `boss_name` | string |  |
| `encounter_id` | integer |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `zone_name` | string |  |
| `raid_night_date` | date |  |
| `duration_seconds` | number | (nullable) |
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string |  |
| `realm` | string |  |
| `role` | string | (nullable) |
| `spec` | string | (nullable) |
| `avg_item_level` | number | (nullable) |
| `potion_use` | integer |  |
| `combat_potion_casts` | integer | (nullable) |
| `combat_potion_names` | string | (nullable) |
| `healthstone_use` | integer | (nullable) |
| `has_food_buff` | boolean |  |
| `food_buff_names` | string | (nullable) |
| `has_flask_or_phial_buff` | boolean |  |
| `flask_or_phial_names` | string | (nullable) |
| `has_weapon_enhancement` | boolean |  |
| `weapon_enhancement_names` | string | (nullable) |
| `throughput_per_second` | number | From WCL rankings (nullable) (nullable) |
| `rank_percent` | number | (nullable) |
| `bracket_percent` | number | (nullable) |
| `rank_string` | string | (nullable) |

---

### gold_boss_progress_history

**Grain**: One row per report, boss encounter, and difficulty with nightly progression summary.

**Primary key**: `encounter_id, difficulty, report_code`

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date |  |
| `report_code` | string |  |
| `report_title` | string | (nullable) |
| `start_time_utc` | timestamp | (nullable) |
| `end_time_utc` | timestamp | (nullable) |
| `pulls_on_night` | integer |  |
| `kills_on_night` | integer |  |
| `wipes_on_night` | integer |  |
| `best_wipe_pct_on_night` | number | (nullable) |
| `avg_wipe_pct_on_night` | number | (nullable) |
| `best_boss_hp_remaining` | number | (nullable) |
| `is_kill_on_night` | boolean |  |
| `kill_duration_seconds` | number | (nullable) |
| `longest_pull_seconds` | number | (nullable) |

---

### gold_boss_pull_history

**Grain**: One row per tracked boss pull.

**Primary key**: `report_code, fight_id`

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date |  |
| `report_code` | string |  |
| `report_title` | string | (nullable) |
| `start_time_utc` | timestamp | (nullable) |
| `end_time_utc` | timestamp | (nullable) |
| `fight_id` | integer |  |
| `is_kill` | boolean |  |
| `boss_percentage` | number | (nullable) |
| `boss_hp_remaining` | number | (nullable) |
| `duration_seconds` | number | (nullable) |
| `last_phase` | integer | (nullable) |

---

### gold_guild_roster

**Grain**: One row per guild roster character.

**Primary key**: `name, realm`

| Column | Type | Description |
|--------|------|-------------|
| `name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `rank` | integer |  |
| `rank_label` | string | (nullable) |
| `rank_category` | string | (nullable) |
| `is_raid_team` | boolean |  |
| `is_active` | boolean |  |
| `total_raids_tracked` | integer |  |
| `raids_present` | integer |  |
| `attendance_rate_pct` | number |  |
| `last_raid_date` | date | (nullable) |
| `first_raid_date` | date | (nullable) |

---

### gold_guild_zone_ranks

**Grain**: One row per raid zone with guild progression ranks.

**Primary key**: `zone_id`

| Column | Type | Description |
|--------|------|-------------|
| `zone_id` | integer |  |
| `zone_name` | string |  |
| `world_rank` | integer | (nullable) |
| `region_rank` | integer | (nullable) |
| `server_rank` | integer | (nullable) |

---

### gold_live_raid_roster

**Grain**: One row per active live raid roster entry from Google Sheets.

**Primary key**: `name`

| Column | Type | Description |
|--------|------|-------------|
| `name` | string |  |
| `roster_rank` | string | (nullable) |
| `player_class` | string | (nullable) |
| `race` | string | (nullable) |
| `note` | string | (nullable) |
| `source_refreshed_at` | timestamp | (nullable) |

---

### gold_player_attendance

**Grain**: One row per player identity for attendance across tracked raids.

**Primary key**: `player_identity_key`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string | Lowercase `player_name:player_class:player_realm` identity key |
| `player_name` | string |  |
| `player_class` | string |  |
| `player_realm` | string | Realm resolved from WCL actor roster, or `unknown` |
| `total_raids_tracked` | integer | Total report count |
| `raids_present` | integer | Reports with presence = 1 |
| `raids_benched` | integer | Reports with presence = 2 |
| `raids_absent` | integer | Reports with presence = 3 |
| `last_raid_date` | date | (nullable) |
| `first_raid_date` | date | (nullable) |
| `zones_attended` | array | Distinct zones attended (nullable) |
| `attendance_rate_pct` | number | raids_present / total × 100 |

---

### gold_player_boss_performance

**Grain**: One row per player identity, role, boss encounter, zone, and difficulty across kill pulls.

**Primary key**: `player_identity_key, role, encounter_id, boss_name, zone_name, difficulty, difficulty_label`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string |  |
| `realm` | string |  |
| `role` | string |  |
| `primary_spec` | string | (nullable) |
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `kills_on_boss` | integer | Number of kills included |
| `avg_throughput_per_second` | number | (nullable) |
| `best_throughput_per_second` | number | (nullable) |
| `latest_throughput_per_second` | number | Most recent kill performance (nullable) |
| `throughput_trend` | number | (latest − avg) / avg × 100. Positive = improving (nullable) |
| `avg_rank_percent` | number | (nullable) |
| `best_rank_percent` | number | (nullable) |
| `avg_item_level` | number | (nullable) |
| `first_kill_date` | date | (nullable) |
| `latest_kill_date` | date | (nullable) |

---

### gold_player_character_equipment

**Grain**: One row per player character equipped item slot.

**Primary key**: `player_identity_key, slot_type`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `realm_slug` | string |  |
| `slot_type` | string |  |
| `slot_name` | string | (nullable) |
| `item_id` | integer | (nullable) |
| `item_name` | string | (nullable) |
| `icon_url` | string |  |
| `quality` | string | (nullable) |
| `item_level` | integer | (nullable) |
| `inventory_type` | string | (nullable) |
| `item_subclass` | string | (nullable) |
| `binding` | string | (nullable) |
| `transmog_name` | string | (nullable) |
| `enchantments_json` | string | (nullable) |
| `sockets_json` | string | (nullable) |
| `stats_json` | string | (nullable) |
| `spells_json` | string | (nullable) |
| `raw_details_json` | string | (nullable) |

---

### gold_player_character_media

**Grain**: One row per player character media payload.

**Primary key**: `player_identity_key`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `realm_slug` | string |  |
| `avatar_url` | string | (nullable) |
| `inset_url` | string | (nullable) |
| `main_url` | string | (nullable) |
| `main_raw_url` | string | (nullable) |

---

### gold_player_death_events

**Grain**: One row per player death joined to boss fight context.

**Primary key**: `death_event_key`

| Column | Type | Description |
|--------|------|-------------|
| `death_event_key` | string |  |
| `report_code` | string |  |
| `fight_id` | integer |  |
| `encounter_id` | integer |  |
| `boss_name` | string | (nullable) |
| `zone_name` | string | (nullable) |
| `zone_id` | integer | (nullable) |
| `difficulty` | integer | (nullable) |
| `difficulty_label` | string | (nullable) |
| `raid_night_date` | date | (nullable) |
| `is_kill` | boolean | (nullable) |
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `death_timestamp_ms` | integer |  |
| `fight_start_ms` | integer | (nullable) |
| `overkill` | number | (nullable) |
| `killing_blow_name` | string | (nullable) |
| `killing_blow_id` | integer | (nullable) |

---

### gold_player_mplus_dungeon_breakdown

**Grain**: One row per player, region, season, and Mythic+ dungeon.

**Primary key**: `player_identity_key, region, season, dungeon`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string | Character name |
| `realm_slug` | string | Realm slug |
| `region` | string | Raider.IO region |
| `season` | string | Season identifier |
| `dungeon` | string | Dungeon name |
| `best_short_name` | string | Short dungeon name for the best run (nullable) |
| `highest_key_level` | integer | Highest key level in exported payload (nullable) |
| `highest_timed_level` | integer | Highest timed key level (nullable) |
| `total_runs` | integer | Runs for this dungeon |
| `timed_runs` | integer | Timed runs for this dungeon |
| `untimed_runs` | integer | Untimed runs for this dungeon |
| `latest_completed_at` | timestamp | Latest completion timestamp (nullable) |
| `best_key_level` | integer | Key level for selected best run (nullable) |
| `best_score` | number | Raider.IO best run score (nullable) |
| `best_timed` | boolean | Whether selected best run was timed (nullable) |
| `best_clear_time_ms` | integer | (nullable) |
| `best_par_time_ms` | integer | (nullable) |
| `best_completed_at` | timestamp | Best run completion timestamp (nullable) |
| `best_run_url` | string | Raider.IO run URL (nullable) |

---

### gold_player_mplus_run_history

**Grain**: One row per Raider.IO Mythic+ run record.

**Primary key**: `player_identity_key, region, season, dungeon, completed_at, url`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string | Character name |
| `realm_slug` | string | Realm slug |
| `region` | string | Raider.IO region |
| `season` | string | Season identifier |
| `source` | string | `recent` or `best` source array (nullable) |
| `dungeon` | string | Dungeon name |
| `short_name` | string | Short dungeon name (nullable) |
| `mythic_level` | integer | Keystone level (nullable) |
| `score` | number | Raider.IO run score (nullable) |
| `completed_at` | timestamp | Completion timestamp |
| `completed_date` | date | Completion date (nullable) |
| `clear_time_ms` | integer | Clear time in ms (nullable) |
| `par_time_ms` | integer | Timer/par time in ms (nullable) |
| `num_keystone_upgrades` | integer | Keystone upgrades awarded (nullable) |
| `timed` | boolean | Whether the run was completed in time (nullable) |
| `url` | string | Raider.IO run URL |
| `snapshot_at` | timestamp | Ingestion timestamp (nullable) |

---

### gold_player_mplus_weekly_activity

**Grain**: One row per player, region, season, and Mythic+ week.

**Primary key**: `player_identity_key, region, season, week_start`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string | Character name |
| `realm_slug` | string | Realm slug |
| `region` | string | Raider.IO region |
| `season` | string | Season identifier |
| `week_start` | date | Week start date |
| `total_runs` | integer | Runs in week |
| `timed_runs` | integer | Timed runs in week |
| `untimed_runs` | integer | Untimed runs in week |
| `highest_key_level` | integer | Highest key level completed that week (nullable) |
| `unique_dungeons` | integer | Distinct dungeons completed that week |
| `most_common_key_level` | integer | Most common key level that week (nullable) |

---

### gold_player_raid_achievements

**Grain**: One row per completed raid feat achievement for a character.

**Primary key**: `player_identity_key, achievement_id, completed_timestamp`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `realm_slug` | string |  |
| `achievement_id` | integer |  |
| `achievement_name` | string |  |
| `completed_timestamp` | integer |  |

---

### gold_player_survivability

**Grain**: One row per player identity with all-time death statistics.

**Primary key**: `player_identity_key`

| Column | Type | Description |
|--------|------|-------------|
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string |  |
| `realm` | string |  |
| `total_deaths` | integer | Deaths across all raids |
| `kills_tracked` | integer | Kill fights player participated in |
| `deaths_per_kill` | number | total_deaths / kills_tracked |
| `most_common_killing_blow` | string | Ability that killed this player most (nullable) |
| `most_common_killing_blow_count` | integer | How many times (nullable) |
| `top_killing_blows_json` | string | (nullable) |
| `zones_died_in` | array | (nullable) |
| `last_death_timestamp_ms` | integer | (nullable) |

---

### gold_player_utility_by_pull

**Grain**: One row per player on an instrumented pull with utility usage counts.

**Primary key**: `report_code, fight_id, player_identity_key`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string |  |
| `fight_id` | integer |  |
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date | (nullable) |
| `is_kill` | boolean |  |
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `health_potion_uses` | integer |  |
| `healthstone_casts` | integer |  |
| `defensive_casts` | integer |  |
| `defensive_abilities` | string | (nullable) |

---

### gold_progression_timeline

**Grain**: One row per first kill contributing to cumulative progression timeline.

**Primary key**: `encounter_id, difficulty`

| Column | Type | Description |
|--------|------|-------------|
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date |  |
| `cumulative_kills` | integer |  |

---

### gold_raid_team

**Grain**: One row per active raid team member.

**Primary key**: `name, realm`

| Column | Type | Description |
|--------|------|-------------|
| `name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `rank` | integer |  |
| `rank_label` | string | (nullable) |
| `rank_category` | string | (nullable) |
| `is_active` | boolean |  |
| `total_raids_tracked` | integer |  |
| `raids_present` | integer |  |
| `attendance_rate_pct` | number |  |
| `last_raid_date` | date | (nullable) |
| `first_raid_date` | date | (nullable) |
| `possible_main` | string | (nullable) |
| `has_possible_alt_in_logs` | boolean |  |

---

### gold_weekly_activity

**Grain**: One row per raid activity week.

**Primary key**: `week_start`

| Column | Type | Description |
|--------|------|-------------|
| `week_start` | timestamp | Monday of the ISO week |
| `raid_nights` | integer | Number of unique reports that week |
| `total_boss_kills` | integer | Sum of boss kills across all reports |
| `total_wipes` | integer | Sum of wipes |
| `total_pulls` | integer | Total pulls (kills + wipes) |
| `total_raid_seconds` | number | Total active fight time in seconds |
| `zones_raided` | array | Distinct zones raided that week (nullable) |

---

### gold_wipe_cooldown_utilization

**Grain**: One row per player, pull, and tracked cooldown ability with scored capacity and usage.

**Primary key**: `report_code, fight_id, player_identity_key, ability_id`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string |  |
| `fight_id` | integer |  |
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date | (nullable) |
| `is_kill` | boolean |  |
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `cooldown_category` | string |  |
| `ability_id` | integer |  |
| `ability_name` | string |  |
| `possible_casts` | integer |  |
| `observed_casts` | integer |  |
| `actual_casts` | integer |  |
| `over_capacity_casts` | integer |  |
| `missed_casts` | integer |  |

---

### gold_wipe_survival_events

**Grain**: One row per player death on an instrumented wipe pull.

**Primary key**: `report_code, fight_id, player_identity_key, death_timestamp_ms`

| Column | Type | Description |
|--------|------|-------------|
| `report_code` | string |  |
| `fight_id` | integer |  |
| `encounter_id` | integer |  |
| `boss_name` | string |  |
| `zone_name` | string |  |
| `zone_id` | integer | (nullable) |
| `difficulty` | integer |  |
| `difficulty_label` | string |  |
| `raid_night_date` | date | (nullable) |
| `player_identity_key` | string |  |
| `player_name` | string |  |
| `player_class` | string | (nullable) |
| `realm` | string |  |
| `spec_id` | integer | (nullable) |
| `death_timestamp_ms` | integer |  |
| `fight_start_ms` | integer | (nullable) |
| `killing_blow_name` | string | (nullable) |
| `killing_blow_id` | integer | (nullable) |
| `defensive_active_at_death` | integer |  |
| `defensive_available_at_death` | integer |  |
| `available_defensive_unused_at_death` | integer |  |
| `last_defensive_cast_before_death_ms` | integer | (nullable) |
| `active_defensives` | string | (nullable) |
| `available_defensives` | string | (nullable) |
| `healthstone_before_death` | integer |  |
| `health_potion_before_death` | integer |  |

---

## Excluded from chatbot

### gold_parse_completeness_daily

**Grain**: One row per raid night and role with WCL parse null-rate monitoring.

**Primary key**: `raid_night_date, role`

| Column | Type | Description |
|--------|------|-------------|
| `raid_night_date` | date |  |
| `role` | string |  |
| `player_rows` | integer |  |
| `null_rank_rows` | integer |  |
| `null_rank_pct` | number |  |

---

## Rank Reference

Blizzard guild rank IDs are 0-indexed. The "Raid Team" column reflects the
membership rule used by `dim_player.is_raid_team` and `gold_raid_team`.

| Rank ID | Label | Category | Raid Team |
|---------|-------|----------|-----------|
| 0 | Guild Master | GM | yes |
| 1 | GM Alt | GM | yes |
| 2 | Officer | Officer | yes |
| 3 | Officer Alt | Officer | yes |
| 4 | Officer Alt | Officer | yes |
| 5 | Raider | Raider | yes |
| 6 | Raider Alt | Raider Alt | no |
| 7 | Bestie | Bestie | no |
| 8 | Trial | Trial | yes |
| 9 | Social | Social | no |

## WoW Class Reference

Blizzard class IDs (used by `gold_player_mplus_summary` and Raider.IO products
where they appear):

| ID | Class |
|----|-------|
| 1 | Warrior |
| 2 | Paladin |
| 3 | Hunter |
| 4 | Rogue |
| 5 | Priest |
| 6 | Death Knight |
| 7 | Shaman |
| 8 | Mage |
| 9 | Warlock |
| 10 | Monk |
| 11 | Druid |
| 12 | Demon Hunter |
| 13 | Evoker |
