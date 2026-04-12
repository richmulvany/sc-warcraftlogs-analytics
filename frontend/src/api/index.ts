/**
 * API layer — fetches static JSON exports from the data directory.
 * Add one function per gold table export.
 */

const BASE_URL = import.meta.env.VITE_DATA_BASE_URL ?? '/data'

export interface ExportEnvelope<T> {
  exported_at: string
  record_count: number
  data: T[]
}

async function fetchExport<T>(tableName: string): Promise<ExportEnvelope<T>> {
  const res = await fetch(`${BASE_URL}/${tableName}.json`)
  if (!res.ok) throw new Error(`Failed to fetch ${tableName}: ${res.status} ${res.statusText}`)
  return res.json() as Promise<ExportEnvelope<T>>
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

export interface ExportManifest {
  exported_at: string
  tables: string[]
  total_records: number
}

export const api = {
  fetchEntitySummary: () => fetchExport<EntitySummaryRow>('entity_summary'),
  fetchBossProgression: () => fetchExport<BossProgressionRow>('boss_progression'),
  fetchRaidSummary: () => fetchExport<RaidSummaryRow>('raid_summary'),
  fetchProgressionTimeline: () => fetchExport<ProgressionTimelineRow>('progression_timeline'),
  fetchManifest: () =>
    fetch(`${BASE_URL}/manifest.json`).then((r) => r.json()) as Promise<ExportManifest>,
}
