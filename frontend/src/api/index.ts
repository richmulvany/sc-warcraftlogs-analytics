import { DashboardManifest, fetchDataset, fetchManifest } from '../lib/dashboardDataClient'

export interface ExportEnvelope<T> {
  exported_at: string
  record_count: number
  data: T[]
}

async function fetchExport<T>(tableName: string): Promise<ExportEnvelope<T>> {
  const data = await fetchDataset<T>(tableName)
  const manifest = await fetchManifest()
  return {
    exported_at: manifest.generated_at,
    record_count: data.length,
    data,
  }
}

export interface EntitySummaryRow {
  category: string
  total_count: number
  unique_count: number
  latest_created_at: string
  earliest_created_at: string
  _gold_generated_at: string
}

export interface BossProgressionRow {
  boss_name: string
  difficulty: number
  total_pulls: number
  total_kills: number
  total_wipes: number
  best_kill_seconds: number | null
  avg_pull_duration_seconds: number
  is_killed: boolean | null
  wipe_to_kill_ratio: number
}

export interface RaidSummaryRow {
  report_code: string
  report_title: string
  zone_name: string
  start_time_utc: string
  total_pulls: number | null
  boss_kills: number | null
  total_wipes: number | null
  total_time_seconds: number | null
  unique_bosses_engaged: number | null
}

export interface ProgressionTimelineRow {
  boss_name: string
  difficulty: number
  start_time_utc: string
  cumulative_kills: number
}

export type ExportManifest = DashboardManifest

export const api = {
  fetchEntitySummary: () => fetchExport<EntitySummaryRow>('entity_summary'),
  fetchBossProgression: () => fetchExport<BossProgressionRow>('boss_progression'),
  fetchRaidSummary: () => fetchExport<RaidSummaryRow>('raid_summary'),
  fetchProgressionTimeline: () => fetchExport<ProgressionTimelineRow>('progression_timeline'),
  fetchManifest: () => fetchManifest(),
}
