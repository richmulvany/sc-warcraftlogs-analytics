import type { WipeSurvivalFailureRow } from './types'

export function quantile(sorted: number[], q: number) {
  if (!sorted.length) return 0
  const pos = (sorted.length - 1) * q
  const base = Math.floor(pos)
  const rest = pos - base
  const next = sorted[base + 1]
  return next !== undefined
    ? sorted[base] + rest * (next - sorted[base])
    : sorted[base]
}

export function formatTimingLabel(ms: number) {
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`
  const mins = ms / 60_000
  return Number.isInteger(mins) ? `${mins}m` : `${mins.toFixed(1)}m`
}

export function buildTimingTicks(domainMax: number) {
  const maxMins = domainMax / 60_000
  const candidates = [0.5, 1, 2, 5, 10, 15, 20, 30]
  const interval = candidates.find(c => maxMins / c <= 5) ?? 30

  const ticks: number[] = []
  for (let m = 0; m * 60_000 <= domainMax; m += interval) {
    ticks.push(m * 60_000)
  }

  return ticks
}

export function formatAxisTick(ms: number) {
  const mins = ms / 60_000
  return Number.isInteger(mins) ? `${mins}m` : `${mins.toFixed(1)}m`
}

export function isKillRow(row: { is_kill?: boolean | string | null }) {
  return row.is_kill === true || row.is_kill === 'true'
}

export function isPositiveFlag(value: unknown) {
  return value === true || value === 'true' || Number(value) > 0
}

export function sectionTotal<T extends { [k: string]: unknown }>(rows: T[], key: keyof T) {
  return rows.reduce((sum, row) => sum + Number(row[key] ?? 0), 0)
}

export function pct(numerator: number, denominator: number) {
  return denominator > 0 ? (numerator / denominator) * 100 : 0
}

export function gradeForRelativeDisciplineScore(
  score: number,
  distinctScoresAscending: number[]
): WipeSurvivalFailureRow['survival_grade'] {
  if (distinctScoresAscending.length <= 1) return 'S'

  const index = distinctScoresAscending.findIndex(candidate => candidate === score)
  if (index === distinctScoresAscending.length - 1) return 'S'
  if (index === 0) return 'F'

  if (distinctScoresAscending.length <= 3) return 'A'

  const middleRelative = (index - 1) / (distinctScoresAscending.length - 3)
  if (middleRelative >= 0.8) return 'A'
  if (middleRelative >= 0.6) return 'B'
  if (middleRelative >= 0.4) return 'C'
  if (middleRelative >= 0.2) return 'D'
  return 'E'
}

export function gradeClassName(grade: WipeSurvivalFailureRow['survival_grade']) {
  switch (grade) {
    case 'S': return 'border-ctp-green/30 bg-ctp-green/10 text-ctp-green'
    case 'A': return 'border-ctp-teal/30 bg-ctp-teal/10 text-ctp-teal'
    case 'B': return 'border-ctp-blue/30 bg-ctp-blue/10 text-ctp-blue'
    case 'C': return 'border-ctp-overlay1/30 bg-ctp-surface1/40 text-ctp-overlay1'
    case 'D': return 'border-ctp-yellow/30 bg-ctp-yellow/10 text-ctp-yellow'
    case 'E': return 'border-ctp-peach/30 bg-ctp-peach/10 text-ctp-peach'
    case 'F': return 'border-ctp-red/30 bg-ctp-red/10 text-ctp-red'
  }
}

export function clampPct(value: number) {
  if (!Number.isFinite(value)) return 0
  return Math.max(0, Math.min(100, value))
}

export function shortDateLabel(date: string) {
  const parsed = new Date(date)
  if (Number.isNaN(parsed.getTime())) return date
  return parsed.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}
