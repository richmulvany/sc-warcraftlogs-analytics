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
  player_name: string
  player_class: string
  realm: string
  role: string
  primary_spec: string
  kills_tracked: number
  avg_throughput_per_second: number
  best_throughput_per_second: number
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
  player_name: string
  player_class: string
  role: string
  spec: string
  avg_item_level: number
  potion_use: number
  healthstone_use: number
  throughput_per_second: number
  rank_percent: number
  bracket_percent: number
  rank_string: string
}

export interface PlayerAttendance {
  player_name: string
  player_class: string
  total_raids_tracked: number
  raids_present: number
  raids_benched: number
  raids_absent: number
  last_raid_date: string
  first_raid_date: string
  zones_attended: string
  attendance_rate_pct: number
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
  player_name: string
  player_class: string
  total_deaths: number
  kills_tracked: number
  deaths_per_kill: number
  most_common_killing_blow: string
  most_common_killing_blow_count: number
  top_killing_blows_json?: string
  zones_died_in: string
  last_death_timestamp_ms: number
}

export interface PlayerDeathEvent {
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
  player_name: string
  player_class: string
  death_timestamp_ms: number
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

export interface LiveRaidRosterEntry {
  name: string
  roster_rank: string
  player_class: string
  race: string
  note: string
  source_refreshed_at: string
}

export interface PlayerCharacterMedia {
  player_name: string
  realm_slug: string
  avatar_url: string
  inset_url: string
  main_url: string
  main_raw_url: string
}

export interface PlayerCharacterEquipment {
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
  player_name: string
  player_class: string
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

export interface PlayerMplusDungeonBreakdown {
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
  best_completed_at: string
  best_run_url: string
}
