import { useMemo } from 'react'
import { ComposedChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, Line } from 'recharts'
import type { BossKillRosterRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatNumber, formatPct } from '../../utils/format'
import type { ChartTooltipProps } from './types'
import { CHART_TICK_STYLE } from '../../utils/chartStyle'

interface ParseBin {
  label: string
  count: number
  midpoint: number
  curve: number
}

interface Props {
  data: BossKillRosterRow[]
  showCurve?: boolean
}

export interface ParseDistributionSummary {
  type: 'Normal' | 'Left-skew' | 'Right-skew' | 'Bimodal' | 'Uniform'
  probability: number
  standardDeviation: number
  skewness: number
  kurtosis: number
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

function toFiniteParse(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null
  const n = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(n) || n < 0 || n > 100) return null
  return n
}

export function getParseDistributionSummary(data: BossKillRosterRow[]): ParseDistributionSummary | null {
  const values = data
    .map(row => toFiniteParse(row.rank_percent))
    .filter((value): value is number => value !== null)

  if (values.length < 10) return null

  const n = values.length
  const mean = values.reduce((sum, value) => sum + value, 0) / n
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / n
  const sd = Math.sqrt(variance)
  if (sd === 0) {
    return {
      type: 'Uniform',
      probability: 100,
      standardDeviation: 0,
      skewness: 0,
      kurtosis: 0,
    }
  }

  const m3 = values.reduce((sum, value) => sum + (value - mean) ** 3, 0) / n
  const m4 = values.reduce((sum, value) => sum + (value - mean) ** 4, 0) / n
  const skew = m3 / (sd ** 3)
  const kurtosis = m4 / (sd ** 4)
  const bimodalityCoefficient = ((skew ** 2) + 1) / Math.max(kurtosis, 0.001)

  const counts = Array(20).fill(0) as number[]
  values.forEach(value => {
    counts[Math.min(Math.floor(value / 5), 19)]++
  })
  const smoothed = counts.map((count, i) => ((counts[i - 1] ?? count) + count + (counts[i + 1] ?? count)) / 3)
  const localPeaks = smoothed.filter((value, i) =>
    i > 0 &&
    i < smoothed.length - 1 &&
    value > smoothed[i - 1] &&
    value > smoothed[i + 1] &&
    value >= n * 0.025
  ).length

  const rawScores: Array<[ParseDistributionSummary['type'], number]> = [
    ['Normal', clamp(1 - (Math.abs(skew) / 1.25) - (Math.abs(kurtosis - 3) / 4) - (localPeaks > 1 ? 0.2 : 0), 0.02, 0.98)],
    ['Left-skew', clamp(skew < 0 ? (Math.abs(skew) / 1.15) + (kurtosis > 3 ? 0.08 : 0) : 0.03, 0.02, 0.98)],
    ['Right-skew', clamp(skew > 0 ? (Math.abs(skew) / 1.15) + (kurtosis > 3 ? 0.08 : 0) : 0.03, 0.02, 0.98)],
    ['Bimodal', clamp(localPeaks >= 2 ? 0.48 + ((localPeaks - 1) * 0.12) + Math.max(0, bimodalityCoefficient - 0.55) : 0.02, 0.02, 0.98)],
    ['Uniform', clamp(1 - (Math.abs(skew) / 0.8) - (Math.abs(kurtosis - 1.8) / 1.4) - (localPeaks > 2 ? 0.2 : 0), 0.02, 0.82)],
  ]

  const weights = rawScores.map(([type, score]) => [type, Math.exp(score * 4)] as const)
  const totalWeight = weights.reduce((sum, [, weight]) => sum + weight, 0)
  const [type, weight] = [...weights].sort((a, b) => b[1] - a[1])[0]

  return {
    type,
    probability: (weight / totalWeight) * 100,
    standardDeviation: sd,
    skewness: skew,
    kurtosis,
  }
}

function Tip({ active, payload, label }: ChartTooltipProps<ParseBin>) {
  const { getParseColor } = useColourBlind()
  if (!active || !payload?.length) return null
  const countPayload = payload.find(item => item.dataKey === 'count') ?? payload[0]
  const row = countPayload.payload
  if (!row) return null
  const midpoint = Number(row.midpoint ?? label)
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2 text-xs font-mono shadow-xl">
      <p style={{ color: getParseColor(midpoint) }} className="font-semibold mb-0.5">{row.label ?? label}</p>
      <p className="text-ctp-subtext1">{countPayload.value} parses</p>
    </div>
  )
}

export function ParseHistogramChart({ data, showCurve = false }: Props) {
  const { getParseColor } = useColourBlind()

  const { buckets, totalParses, avgParse, medianParse, eliteCount, greyCount, tierMix } = useMemo(() => {
    const counts = Array(20).fill(0) as number[]
    const values: number[] = []
    for (const row of data) {
      const pct = toFiniteParse(row.rank_percent)
      if (pct === null) continue
      values.push(pct)
      const idx = Math.min(Math.floor(pct / 5), 19)
      counts[idx]++
    }
    const sortedValues = [...values].sort((a, b) => a - b)
    const medianIndex = Math.floor(sortedValues.length / 2)
    const medianParse = sortedValues.length === 0
      ? 0
      : sortedValues.length % 2 === 0
        ? (sortedValues[medianIndex - 1] + sortedValues[medianIndex]) / 2
        : sortedValues[medianIndex]

    const totalParses = sortedValues.length
    const mean = totalParses > 0 ? sortedValues.reduce((sum, value) => sum + value, 0) / totalParses : 0
    const variance = totalParses > 1
      ? sortedValues.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (totalParses - 1)
      : 0
    const sd = Math.sqrt(variance)
    const silvermanBandwidth = totalParses > 1 ? 1.06 * sd * (totalParses ** -0.2) : 5
    const bandwidth = clamp(silvermanBandwidth || 5, 2.5, 15)
    const normaliser = totalParses * bandwidth * Math.sqrt(2 * Math.PI)
    const buckets = counts.map((count, i) => {
      const midpoint = i * 5 + 2.5
      const curve = totalParses > 0
        ? (() => {
        const reflectedDensity = sortedValues.reduce((sum, value) => {
          const z = (sample: number) => (midpoint - sample) / bandwidth
          return sum +
            Math.exp(-0.5 * z(value) ** 2) +
            Math.exp(-0.5 * z(-value) ** 2) +
            Math.exp(-0.5 * z(200 - value) ** 2)
        }, 0) / normaliser

          return reflectedDensity * totalParses * 5
        })()
        : 0

      return {
        label: i === 19 ? '95–100' : `${i * 5}–${i * 5 + 4}`,
        count,
        midpoint,
        curve,
      }
    })

    const tierMix = [
      { label: 'Grey', count: counts.slice(0, 5).reduce((sum, value) => sum + value, 0), midpoint: 12.5 },
      { label: 'Green', count: counts.slice(5, 10).reduce((sum, value) => sum + value, 0), midpoint: 37.5 },
      { label: 'Blue', count: counts.slice(10, 15).reduce((sum, value) => sum + value, 0), midpoint: 62.5 },
      { label: 'Epic', count: counts.slice(15, 19).reduce((sum, value) => sum + value, 0), midpoint: 85 },
      { label: 'Legendary', count: counts[19], midpoint: 97.5 },
    ]

    return {
      buckets,
      totalParses,
      avgParse: mean,
      medianParse,
      eliteCount: sortedValues.filter(value => value >= 95).length,
      greyCount: sortedValues.filter(value => value < 25).length,
      tierMix,
    }
  }, [data])

  return (
    <div className="flex h-full w-full flex-col">
      <div className="min-h-[280px] flex-1">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={buckets} margin={{ top: 4, right: 4, left: -22, bottom: 0 }}>
            <XAxis
              dataKey="label"
              tick={CHART_TICK_STYLE}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              tick={CHART_TICK_STYLE}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip content={<Tip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
            <Bar dataKey="count" radius={[4, 4, 0, 0]} barSize={22}>
              {buckets.map((b, i) => (
                <Cell key={i} fill={getParseColor(b.midpoint)} fillOpacity={0.85} />
              ))}
            </Bar>
            {showCurve && (
              <Line
                type="monotone"
                dataKey="curve"
                stroke="#f38ba8"
                strokeWidth={4}
                dot={false}
                activeDot={false}
                isAnimationActive={false}
              />
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className="grid grid-cols-2 xl:grid-cols-4 gap-3 mt-4">
        <div className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 px-3 py-2.5">
          <p className="text-[10px] font-mono text-ctp-overlay0">Total Parses</p>
          <p className="text-sm font-semibold text-ctp-text mt-1">{formatNumber(totalParses)}</p>
        </div>
        <div className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 px-3 py-2.5">
          <p className="text-[10px] font-mono text-ctp-overlay0">Average Parse</p>
          <p className="text-sm font-semibold mt-1" style={{ color: getParseColor(avgParse) }}>{formatPct(avgParse, 1)}</p>
        </div>
        <div className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 px-3 py-2.5">
          <p className="text-[10px] font-mono text-ctp-overlay0">Median Parse</p>
          <p className="text-sm font-semibold mt-1" style={{ color: getParseColor(medianParse) }}>{formatPct(medianParse, 0)}</p>
        </div>
        <div className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 px-3 py-2.5">
          <p className="text-[10px] font-mono text-ctp-overlay0">95+ / &lt;25</p>
          <p className="text-sm font-semibold text-ctp-text mt-1">
            <span style={{ color: getParseColor(95) }}>{formatNumber(eliteCount)}</span>
            <span className="text-ctp-overlay0"> / </span>
            <span style={{ color: getParseColor(15) }}>{formatNumber(greyCount)}</span>
          </p>
        </div>
      </div>

      <div className="mt-4">
        <div className="flex items-center justify-between gap-3 mb-2">
          <p className="text-[10px] font-mono text-ctp-overlay0">Tier Share</p>
          <p className="text-[10px] font-mono text-ctp-overlay0">All tracked fight parses</p>
        </div>
        <div className="h-3 w-full rounded-full overflow-hidden bg-ctp-surface1 flex">
          {tierMix.map(tier => (
            <div
              key={tier.label}
              style={{
                width: `${totalParses > 0 ? (tier.count / totalParses) * 100 : 0}%`,
                backgroundColor: getParseColor(tier.midpoint),
              }}
            />
          ))}
        </div>
        <div className="grid grid-cols-2 xl:grid-cols-5 gap-2 mt-3">
          {tierMix.map(tier => (
            <div key={tier.label} className="rounded-lg border border-ctp-surface1 bg-ctp-surface0/50 px-2.5 py-2">
              <p className="text-[10px] font-mono" style={{ color: getParseColor(tier.midpoint) }}>{tier.label}</p>
              <p className="text-xs font-semibold text-ctp-text mt-0.5">{formatNumber(tier.count)}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
