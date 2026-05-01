export type SortDirection = 'asc' | 'desc'

export type WipeSurvivalSortKey =
  | 'player'
  | 'grade'
  | 'wipePullsTracked'
  | 'wipeDeaths'
  | 'deathsPerWipePull'
  | 'defensiveCapacityUsedPct'
  | 'noHealthstonePct'
  | 'noHealthPotionPct'
  | 'survivalFailureScore'
  | 'topMissing'
  | 'mostKilledBy'
  | 'killDeaths'
  | 'killsTracked'
  | 'deathsPerKill'

export interface WipeSurvivalFailureRow {
  player_identity_key?: string
  player_name: string
  player_class: string
  realm?: string
  role?: string
  zone_name?: string
  encounter_id?: string | number | null
  boss_name?: string
  difficulty?: string | number | null
  difficulty_label?: string
  wipe_pulls_tracked: number
  wipe_deaths: number
  first_deaths: number
  early_deaths: number
  kill_deaths: number
  kills_tracked: number
  deaths_per_kill: number | null
  deaths_per_wipe: number
  deaths_per_wipe_pull: number
  no_healthstone_deaths: number
  no_health_potion_deaths: number
  pulls_with_tracked_defensive_capacity: number
  tracked_defensive_capacity: number
  defensive_casts: number
  defensive_missed_casts: number
  defensive_possible_casts: number
  defensive_actual_casts: number
  defensive_capacity_used_pct: number | null
  defensive_class_baseline_pct: number | null
  defensive_class_delta_pct: number | null
  has_defensive_capacity_tracked: boolean
  defensive_tracking_status: 'tracked_used' | 'tracked_zero_usage' | 'no_tracked_capacity' | 'unknown'
  healthstone_uses: number
  potion_uses: number
  defensive_usage_rate: number | null
  healthstone_usage_rate: number | null
  potion_usage_rate: number | null
  death_pressure_score: number
  defensive_component_score: number | null
  healthstone_component_score: number
  potion_component_score: number
  no_healthstone_pct: number
  no_health_potion_pct: number
  weighted_failure_points: number
  survival_failure_score: number
  survival_discipline_score: number
  survival_grade: 'S' | 'A' | 'B' | 'C' | 'D' | 'E' | 'F'
  top_improvement_area: string
  top_missing_category: string
  most_common_killing_blow: string
  most_common_killing_blow_count: number
}

export interface CooldownCapacityRow {
  key: string
  player_name: string
  player_class: string
  ability_name: string
  possible_casts: number
  actual_casts: number
  missed_casts: number
  cast_efficiency_pct: number
  pulls_tracked: number
}

export interface ScopedSurvivabilityRow {
  player_name: string
  player_class: string
  total_deaths: number
  wipe_deaths: number
  kill_deaths: number
  kills_tracked: number
  pulls_tracked: number
  deaths_per_kill: number | null
  deaths_per_pull: number | null
  most_common_killing_blow: string
  most_common_killing_blow_count: number
}

export interface DeathTimingSummary {
  count: number
  min: number
  q1: number
  median: number
  q3: number
  max: number
  lowerWhisker: number
  upperWhisker: number
  outliers: number[]
}

export interface ProgressSnapshotDatum {
  boss_name: string
  difficulty_label?: string | null
  label: string
  subLabel: string
  open: number
  close: number
  high: number
  low: number
  currentNight: string
  previousNight: string | null
  pullCount: number
}

export interface WipeWallRow {
  boss: string; fullName: string; diff: string; wipes: number
  bestPct: number; avgPct: number; nights: number; isCleared: boolean
}

export interface RecurringKillerRow {
  name: string; fullName: string; deaths: number
  uniquePlayers: number; uniqueBosses: number
}
