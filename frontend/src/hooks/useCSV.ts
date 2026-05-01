import { useState, useEffect } from 'react'
import { fetchDataset } from '../lib/dashboardDataClient'

export interface CSVResult<T> {
  data: T[]
  loading: boolean
  error: string | null
}

interface UseCSVOptions {
  optional?: boolean
}

const CSV_FILENAME_TO_DATASET_KEY: Record<string, string> = {
  'gold_raid_summary.csv': 'raid_summary',
  'gold_player_performance_summary.csv': 'player_performance_summary',
  'gold_boss_progression.csv': 'boss_progression',
  'gold_encounter_catalog.csv': 'encounter_catalog',
  'gold_boss_kill_roster.csv': 'boss_kill_roster',
  'gold_player_attendance.csv': 'player_attendance',
  'gold_player_utility_by_pull.csv': 'player_utility_by_pull',
  'gold_wipe_survival_events.csv': 'wipe_survival_events',
  'gold_wipe_cooldown_utilization.csv': 'wipe_cooldown_utilization',
  'gold_wipe_survival_discipline.csv': 'wipe_survival_discipline',
  'gold_guild_roster.csv': 'guild_roster',
  'gold_weekly_activity.csv': 'weekly_activity',
  'guild_zone_ranks.csv': 'guild_zone_ranks',
  'gold_boss_wipe_analysis.csv': 'boss_wipe_analysis',
  'gold_boss_progress_history.csv': 'boss_progress_history',
  'gold_boss_pull_history.csv': 'boss_pull_history',
  'gold_player_survivability.csv': 'player_survivability',
  'gold_player_survivability_rankings.csv': 'player_survivability_rankings',
  'gold_player_death_events.csv': 'player_death_events',
  'gold_progression_timeline.csv': 'progression_timeline',
  'gold_raid_team.csv': 'raid_team',
  'gold_preparation_readiness.csv': 'preparation_readiness',
  'live_raid_roster.csv': 'live_raid_roster',
  'player_character_media.csv': 'player_character_media',
  'player_character_equipment.csv': 'player_character_equipment',
  'player_raid_achievements.csv': 'player_raid_achievements',
  'gold_best_kills.csv': 'best_kills',
  'gold_boss_mechanics.csv': 'boss_mechanics',
  'gold_player_boss_performance.csv': 'player_boss_performance',
  'gold_player_mplus_summary.csv': 'player_mplus_summary',
  'gold_player_mplus_score_history.csv': 'player_mplus_score_history',
  'gold_player_mplus_run_history.csv': 'player_mplus_run_history',
  'gold_player_mplus_weekly_activity.csv': 'player_mplus_weekly_activity',
  'gold_player_mplus_dungeon_breakdown.csv': 'player_mplus_dungeon_breakdown',
  'preparation_overrides.csv': 'preparation_overrides',
}

function normaliseValue(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString()
  if (typeof value !== 'string') return value

  const trimmed = value.trim()
  if (trimmed === '') return ''
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed)
  if (trimmed === 'true') return true
  if (trimmed === 'false') return false
  return trimmed
}

export function useCSV<T extends object>(filename: string, options: UseCSVOptions = {}): CSVResult<T> {
  const [data, setData] = useState<T[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { optional = false } = options

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    const datasetKey = CSV_FILENAME_TO_DATASET_KEY[filename]
    if (!datasetKey) {
      if (!cancelled) {
        setError(`No dataset key found for "${filename}"`)
        setLoading(false)
      }
      return
    }

    fetchDataset<T>(datasetKey)
      .then(rows =>
        rows.map(row =>
          Object.fromEntries(
            Object.entries(row as Record<string, unknown>).map(([k, v]) => [
              k,
              Array.isArray(v) || (v && typeof v === 'object') ? JSON.stringify(v) : normaliseValue(v),
            ])
          ) as T
        )
      )
      .then(payload => {
        if (!cancelled) {
          setData(payload)
          setLoading(false)
        }
      })
      .catch(err => {
        if (!cancelled) {
          if (optional) {
            setData([])
            setLoading(false)
          } else {
            setError(String(err))
            setLoading(false)
          }
        }
      })

    return () => { cancelled = true }
  }, [filename, optional])

  return { data, loading, error }
}
