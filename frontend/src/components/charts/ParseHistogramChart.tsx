import { useMemo } from 'react'
import { ComposedChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, Line } from 'recharts'
import type { BossKillRosterRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatNumber, formatPct } from '../../utils/format'

interface Props {
  data: BossKillRosterRow[]
  showCurve?: boolean
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload, label }: any) {
  const { getParseColor } = useColourBlind()
  if (!active || !payload?.length) return null
  const countPayload = payload.find((item: { dataKey?: string }) => item.dataKey === 'count') ?? payload[0]
  const start = Number(label.split('–')[0])
  const midpoint = start + 2.5
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2 text-xs font-mono shadow-xl">
      <p style={{ color: getParseColor(midpoint) }} className="font-semibold mb-0.5">{label}</p>
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
      const pct = Number(row.rank_percent)
      if (!isFinite(pct) || pct < 0) continue
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

    const bandwidth = Math.max(6, Math.min(12, 100 / Math.sqrt(Math.max(values.length, 1))))
    const rawDensity = counts.map((_, i) => {
      const x = i * 5 + 2.5
      return values.reduce((sum, value) => {
        const z = (x - value) / bandwidth
        return sum + Math.exp(-0.5 * z * z)
      }, 0)
    })
    const maxDensity = Math.max(...rawDensity, 0)
    const maxCount = Math.max(...counts, 0)

    const buckets = counts.map((count, i) => ({
      label: i === 19 ? '95–100' : `${i * 5}–${i * 5 + 4}`,
      count,
      midpoint: i * 5 + 2.5,
      curve: maxDensity > 0 ? (rawDensity[i] / maxDensity) * maxCount : 0,
    }))

    const totalParses = sortedValues.length
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
      avgParse: totalParses > 0 ? sortedValues.reduce((sum, value) => sum + value, 0) / totalParses : 0,
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
              tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip content={<Tip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
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
