import { HEATMAP_EMPTY_CELL } from './constants'

export function getMplusCellColor(level: number): string {
  if (level <= 0) return HEATMAP_EMPTY_CELL
  if (level < 7)  return '#313244'
  if (level < 12) return '#45475a'
  if (level < 17) return '#89b4fa'
  if (level < 20) return '#cba6f7'
  return '#f38ba8'
}

export function getMplusQuantityCellColor(count: number): string {
  if (count <= 0) return HEATMAP_EMPTY_CELL
  if (count === 1) return '#313244'
  if (count === 2) return '#89b4fa'
  if (count === 3) return '#cba6f7'
  return '#f38ba8'
}

export function formatKeyLevel(value: unknown): string {
  const number = Number(value)
  return Number.isFinite(number) && number > 0 ? `+${number}` : '—'
}

export function formatRunTime(valueMs: unknown): string {
  const ms = Number(valueMs)
  if (!Number.isFinite(ms) || ms <= 0) return '—'
  const totalSeconds = Math.round(ms / 1000)
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  return `${minutes}:${String(seconds).padStart(2, '0')}`
}

export function timerUsedPct(clearTimeMs: unknown, parTimeMs: unknown): number | null {
  const clearMs = Number(clearTimeMs)
  const parMs = Number(parTimeMs)
  if (!Number.isFinite(clearMs) || !Number.isFinite(parMs) || clearMs <= 0 || parMs <= 0) return null
  return (clearMs / parMs) * 100
}

export function isTimed(value: unknown): boolean {
  return value === true || String(value).toLowerCase() === 'true'
}

export function dateKey(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

export function dateFromKey(value: string): Date {
  const [year, month, day] = value.split('-').map(Number)
  return new Date(year, (month || 1) - 1, day || 1)
}

export function addDays(date: Date, days: number): Date {
  const next = new Date(date)
  next.setDate(next.getDate() + days)
  return next
}
