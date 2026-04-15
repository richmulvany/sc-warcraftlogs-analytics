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

export interface PlayerSurvivability {
  player_name: string
  player_class: string
  total_deaths: number
  kills_tracked: number
  deaths_per_kill: number
  most_common_killing_blow: string
  most_common_killing_blow_count: number
  zones_died_in: string
  last_death_timestamp_ms: number
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
