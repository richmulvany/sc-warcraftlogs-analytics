const intl = new Intl.NumberFormat('en-GB')

export function formatNumber(n: number | string): string {
  const v = Number(n)
  if (isNaN(v)) return '—'
  return intl.format(v)
}

export function formatPct(n: number | string, decimals = 1): string {
  const v = Number(n)
  if (isNaN(v) || n === '' || n == null) return '—'
  return `${v.toFixed(decimals)}%`
}

export function formatDate(iso: string): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (isNaN(d.getTime())) return iso
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
}

export function formatDateShort(iso: string): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (isNaN(d.getTime())) return iso
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: '2-digit' })
}

export function formatRelativeTime(iso: string): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (isNaN(d.getTime())) return iso
  const diff = Date.now() - d.getTime()
  const mins = Math.floor(diff / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  if (days === 1) return 'yesterday'
  if (days < 7) return `${days}d ago`
  return formatDate(iso)
}

export function capitalise(s: string): string {
  if (!s) return s
  return s.charAt(0).toUpperCase() + s.slice(1)
}

// Returns null for null/undefined/empty/NaN. Use this instead of `Number(x) || 0`
// when missing data should be excluded from averages, not pulled toward zero.
export function toFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null
  const n = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(n) ? n : null
}

export function meanIgnoringNulls(values: Array<number | null | undefined>): number {
  let sum = 0
  let count = 0
  for (const v of values) {
    if (v === null || v === undefined || !Number.isFinite(v)) continue
    sum += v
    count += 1
  }
  return count === 0 ? 0 : sum / count
}

export function getRelativeScoreDomain(
  values: Array<number | null | undefined>,
  options?: {
    minPadding?: number
    flatPadding?: number
    step?: number
  },
): [number, number] | null {
  const scores = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  if (scores.length === 0) return null

  const minPadding = options?.minPadding ?? 25
  const flatPadding = options?.flatPadding ?? 50
  const step = options?.step ?? 25

  const minScore = Math.min(...scores)
  const maxScore = Math.max(...scores)
  const range = maxScore - minScore
  const padding = range === 0 ? flatPadding : Math.max(minPadding, range * 0.15)
  const yMin = Math.max(0, Math.floor((minScore - padding) / step) * step)
  const yMax = Math.ceil((maxScore + padding) / step) * step

  if (yMax <= yMin) return [yMin, yMin + Math.max(step, flatPadding * 2)]
  return [yMin, yMax]
}
