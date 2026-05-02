// ─── Gold table row types ────────────────────────────────────────────────────

export interface RaidSummary {
  report_code: string
  report_title: string
  start_time_utc: string
  end_time_utc: string
  zone_id: string
  zone_name: string
  raid_night_date: string
  primary_difficulty: string
  total_pulls: number
  boss_kills: number
  total_wipes: number
  total_fight_seconds: number
  unique_bosses_engaged: number
  unique_bosses_killed: number
}

export interface PlayerPerformanceSummary {
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  role: string
  primary_spec: string
  kills_tracked: number
  avg_throughput_per_second: number | null
  best_throughput_per_second: number | null
  avg_rank_percent: number
  best_rank_percent: number
  avg_item_level: number
  last_seen_date: string
}

export interface BossProgression {
  encounter_id: string
  boss_name: string
  zone_id: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  total_pulls: number
  total_kills: number
  total_wipes: number
  best_kill_seconds: number
  avg_pull_duration_seconds: number
  is_killed: string
  first_kill_date: string
  last_attempt_date: string
  wipe_to_kill_ratio: number
}

export interface EncounterCatalog {
  zone_id: string
  zone_name: string
  encounter_id: string
  encounter_name: string
  difficulty_names: string
}

export interface BossKillRosterRow {
  report_code: string
  fight_id: string
  boss_name: string
  encounter_id: string
  difficulty: string
  difficulty_label: string
  zone_name: string
  raid_night_date: string
  duration_seconds: number
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  role: string
  spec: string
  avg_item_level: number
  potion_use: number
  combat_potion_casts: number
  combat_potion_names: string
  healthstone_use: number
  has_food_buff: boolean
  food_buff_names: string
  has_flask_or_phial_buff: boolean
  flask_or_phial_names: string
  has_weapon_enhancement: boolean
  weapon_enhancement_names: string
  throughput_per_second: number
  rank_percent: number
  bracket_percent: number
  rank_string: string
}

export interface PreparationOverrideRow {
  id: string
  mode: 'replace' | 'pool'
  source_character?: string
  target_character?: string
  characters?: string
  display_name?: string
  enabled?: boolean | string
  notes?: string
  updated_by?: string
  updated_at?: string
}

export interface PlayerAttendance {
  player_identity_key: string
  player_name: string
  player_class: string
  player_realm: string
  total_raids_tracked: number
  raids_present: number
  raids_benched: number
  raids_absent: number
  last_raid_date: string
  first_raid_date: string
  zones_attended: string
  attendance_rate_pct: number
}

export interface PlayerUtilityByPull {
  report_code: string
  fight_id: string
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  is_kill: string | boolean
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  health_potion_uses: number
  healthstone_casts: number
  defensive_casts: number
  defensive_abilities: string
}

export interface WipeSurvivalEvent {
  report_code: string
  fight_id: string
  encounter_id: string
  boss_name: string
  zone_name: string
  zone_id: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  spec_id: string | number
  death_timestamp_ms: number
  fight_start_ms: number
  killing_blow_name: string
  killing_blow_id: string
  defensive_active_at_death: number
  defensive_available_at_death: number
  available_defensive_unused_at_death: number
  last_defensive_cast_before_death_ms: number
  active_defensives: string
  available_defensives: string
  healthstone_before_death: number
  health_potion_before_death: number
}

export interface WipeCooldownUtilization {
  report_code: string
  fight_id: string
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  duration_seconds: number
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  spec_id: string | number
  cooldown_category: 'personal' | 'personal_spec' | 'raid' | 'external'
  ability_id: string
  ability_name: string
  cooldown_seconds: number
  active_seconds: number
  possible_casts: number
  actual_casts: number
  missed_casts: number
  cast_efficiency_pct: number
}

export interface WipeSurvivalDiscipline {
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  role: string
  zone_name: string
  encounter_id: string | number | null
  boss_name: string
  difficulty: string | number | null
  difficulty_label: string
  wipe_pulls_tracked: number
  wipe_deaths: number
  first_deaths: number
  early_deaths: number
  kill_deaths: number
  kills_tracked: number
  deaths_per_kill: number | null
  deaths_per_wipe: number
  pulls_with_tracked_defensive_capacity: number
  tracked_defensive_capacity: number
  defensive_casts: number
  defensive_missed_casts: number
  healthstone_uses: number
  potion_uses: number
  no_healthstone_deaths: number
  no_health_potion_deaths: number
  defensive_usage_rate: number | null
  healthstone_usage_rate: number | null
  potion_usage_rate: number | null
  no_healthstone_pct: number
  no_health_potion_pct: number
  death_pressure_score: number
  defensive_component_score: number | null
  healthstone_component_score: number
  potion_component_score: number
  defensive_class_baseline_pct: number | null
  defensive_class_delta_pct: number | null
  weighted_failure_points: number
  survival_failure_score: number
  survival_discipline_score: number
  top_improvement_area: string
  top_missing_category: string
  defensive_tracking_status: 'tracked_used' | 'tracked_zero_usage' | 'no_tracked_capacity' | 'unknown'
  has_defensive_capacity_tracked: boolean | string
  most_common_killing_blow: string
  most_common_killing_blow_count: number
  survival_grade?: 'S' | 'A' | 'B' | 'C' | 'D' | 'E' | 'F'
}

export interface GuildRoster {
  name: string
  player_class: string
  realm: string
  rank: string
  rank_label: string
  rank_category: string
  is_raid_team: string
  is_active: string
  total_raids_tracked: number
  raids_present: number
  attendance_rate_pct: number
  last_raid_date: string
  first_raid_date: string
}

export interface WeeklyActivity {
  week_start: string
  raid_nights: number
  total_boss_kills: number
  total_wipes: number
  total_pulls: number
  total_raid_seconds: number
  zones_raided: string
}

export interface GuildZoneRank {
  zone_id: string
  zone_name: string
  world_rank: number
  region_rank: number
  server_rank: number
}

export interface BossWipeAnalysis {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  total_wipes: number
  best_wipe_pct: number
  avg_wipe_pct: number
  avg_last_phase: number
  max_phase_reached: number
  avg_wipe_duration_seconds: number
  longest_wipe_seconds: number
  first_wipe_date: string
  latest_wipe_date: string
  raid_nights_attempted: number
  avg_wipe_pct_rounded: number
}

export interface BossProgressHistoryRow {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  report_code: string
  report_title: string
  start_time_utc: string
  end_time_utc: string
  pulls_on_night: number
  kills_on_night: number
  wipes_on_night: number
  best_wipe_pct_on_night: number
  avg_wipe_pct_on_night: number
  best_boss_hp_remaining: number
  is_kill_on_night: string | boolean
  kill_duration_seconds: number
  longest_pull_seconds: number
}

export interface BossPullHistoryRow {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  report_code: string
  report_title: string
  start_time_utc: string
  end_time_utc: string
  fight_id: string
  is_kill: string | boolean
  boss_percentage: number
  boss_hp_remaining: number
  duration_seconds: number
  last_phase: number
}

export interface PlayerSurvivability {
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  total_deaths: number
  kills_tracked: number
  deaths_per_kill: number
  most_common_killing_blow: string
  most_common_killing_blow_count: number
  top_killing_blows_json?: string
  zones_died_in: string
  last_death_timestamp_ms: number
}

export interface PlayerSurvivabilityRanking {
  player_identity_key: string
  player_name: string
  player_class: string
  zone_name: string
  encounter_id: string | number | null
  boss_name: string
  difficulty: string | number | null
  difficulty_label: string
  deaths: number
  kills: number
  deaths_per_kill: number
  survivability_rank: number
  survivability_rank_total: number
  survivability_rank_percentile: number
}

export interface PlayerDeathEvent {
  death_event_key: string
  report_code: string
  fight_id: string
  encounter_id: string
  boss_name: string
  zone_name: string
  zone_id: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  is_kill: string | boolean
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  death_timestamp_ms: number
  fight_start_ms: number
  overkill: number
  killing_blow_name: string
  killing_blow_id: string
}

export interface ProgressionTimeline {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  raid_night_date: string
  cumulative_kills: number
}

export interface RaidTeamMember {
  name: string
  player_class: string
  realm: string
  rank: string
  rank_label: string
  rank_category: string
  is_active: string
  total_raids_tracked: number
  raids_present: number
  attendance_rate_pct: number
  last_raid_date: string
  first_raid_date: string
  possible_main: string
  has_possible_alt_in_logs: string
}

export interface PreparationReadiness {
  identity_key: string
  player_name: string
  player_class: string
  role: string
  rank_label: string
  is_active: boolean | string
  current_tier: string
  roster_source: 'live_raid_roster' | 'gold_raid_team' | string
  has_current_tier_data: boolean | string
  attendance_rate_pct: number
  raids_present: number
  total_raids_tracked: number
  kills_tracked: number
  food_rate: number
  flask_rate: number
  weapon_rate: number
  combat_potion_rate: number
  readiness_score: number
  readiness_label: 'watch' | 'steady' | 'strong'
  readiness_notes: string
  spec: string
  latest_avg_item_level: number
  latest_kill_date: string
  weakest_signal_label: string
  recent_food_names: string
  recent_flask_names: string
  recent_weapon_names: string
  recent_combat_potion_names: string
  character_names: string
  override_label: string
}

export interface LiveRaidRosterEntry {
  name: string
  roster_rank: string
  player_class: string
  race: string
  note: string
  source_refreshed_at: string
}

export interface PlayerCharacterMedia {
  player_identity_key: string
  player_name: string
  realm_slug: string
  avatar_url: string
  inset_url: string
  main_url: string
  main_raw_url: string
}

export interface PlayerCharacterEquipment {
  player_identity_key: string
  player_name: string
  realm_slug: string
  slot_type: string
  slot_name: string
  item_id: string
  item_name: string
  icon_url: string
  quality: string
  item_level: number
  inventory_type: string
  item_subclass: string
  binding: string
  transmog_name: string
  enchantments_json: string
  sockets_json: string
  stats_json: string
  spells_json: string
  raw_details_json: string
}

export interface PlayerRaidAchievement {
  player_identity_key: string
  player_name: string
  realm_slug: string
  achievement_id: string
  achievement_name: string
  completed_timestamp: number
}

export interface BestKill {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  best_kill_seconds: number
  avg_kill_seconds: number
  total_kills: number
  first_kill_date: string
  latest_kill_date: string
  best_kill_mm_ss: string
}

export interface BossMechanics {
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  total_wipes: number
  avg_boss_pct: number
  pct_wipes_phase_1: number
  pct_wipes_phase_2: number
  pct_wipes_phase_3_plus: number
  wipes_lt_1min: number
  wipes_1_3min: number
  wipes_3_5min: number
  wipes_5plus_min: number
  last_week_avg_boss_pct: number
  progress_trend: number
}

export interface PlayerBossPerformance {
  player_identity_key: string
  player_name: string
  player_class: string
  realm: string
  role: string
  primary_spec: string
  encounter_id: string
  boss_name: string
  zone_name: string
  difficulty: string
  difficulty_label: string
  kills_on_boss: number
  avg_throughput_per_second: number
  best_throughput_per_second: number
  latest_throughput_per_second: number
  throughput_trend: number
  avg_rank_percent: number
  best_rank_percent: number
  avg_item_level: number
  first_kill_date: string
  latest_kill_date: string
}

export interface PlayerMplusSummary {
  player_identity_key: string
  player_name: string
  realm_slug: string
  region: string
  profile_url: string
  season: string
  snapshot_at: string
  score_all: number
  score_dps: number
  score_healer: number
  score_tank: number
  world_rank: number
  region_rank: number
  realm_rank: number
  guild_mplus_rank: number | null
  guild_mplus_rank_total: number
  guild_mplus_rank_percentile: number | null
  total_runs: number
  timed_runs: number
  untimed_runs: number
  highest_timed_level: number
  highest_untimed_level: number
  most_common_key_level: number
  most_common_key_count: number
  best_run_dungeon: string
  best_run_short_name: string
  best_run_level: number
  best_run_score: number
  best_run_timed: string | boolean
  best_run_completed_at: string
  best_run_url: string
}

export interface PlayerMplusScoreHistory {
  player_identity_key: string
  player_name: string
  realm_slug: string
  region: string
  profile_url: string
  season: string
  snapshot_at: string
  snapshot_date: string
  score_all: number
  score_dps: number
  score_healer: number
  score_tank: number
  world_rank: number
  region_rank: number
  realm_rank: number
  raiderio_last_crawled_at: string
}

export interface PlayerMplusRunHistory {
  player_identity_key: string
  player_name: string
  realm_slug: string
  region: string
  season: string
  source: string
  dungeon: string
  short_name: string
  mythic_level: number
  score: number
  completed_at: string
  completed_date: string
  clear_time_ms: number
  par_time_ms: number
  num_keystone_upgrades: number
  timed: string | boolean
  url: string
  snapshot_at: string
}

export interface PlayerMplusWeeklyActivity {
  player_identity_key: string
  player_name: string
  realm_slug: string
  region: string
  season: string
  week_start: string
  total_runs: number
  timed_runs: number
  untimed_runs: number
  highest_key_level: number
  unique_dungeons: number
  most_common_key_level: number
}

export interface PlayerConsumables {
  player_name: string
  player_class: string
  role: string
  kills_tracked: number
  kills_with_potion: number
  kills_with_healthstone: number
  potion_use_rate: number
  healthstone_use_rate: number
  boss_consumable_details: string
}

export interface PlayerCombatStats {
  player_name: string
  player_class: string
  role: string
  spec: string
  kills_tracked: number
  latest_avg_item_level: number
  latest_crit_rating: number
  latest_haste_rating: number
  latest_mastery_rating: number
  latest_versatility_rating: number
  avg_item_level: number
  avg_crit_rating: number
  avg_haste_rating: number
  avg_mastery_rating: number
  avg_versatility_rating: number
  latest_kill_date: string
}

export interface PlayerMplusDungeonBreakdown {
  player_identity_key: string
  player_name: string
  realm_slug: string
  region: string
  season: string
  dungeon: string
  best_short_name: string
  highest_key_level: number
  highest_timed_level: number
  total_runs: number
  timed_runs: number
  untimed_runs: number
  latest_completed_at: string
  best_key_level: number
  best_score: number
  best_timed: string | boolean
  best_clear_time_ms: number
  best_par_time_ms: number
  best_completed_at: string
  best_run_url: string
}
