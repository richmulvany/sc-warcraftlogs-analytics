import { useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import type { BossKillRosterRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'

interface Props { data: BossKillRosterRow[] }

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload, label }: any) {
  const { getParseColor } = useColourBlind()
  if (!active || !payload?.length) return null
  const midpoint = Number(label.split('–')[0]) + 5
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2 text-xs font-mono shadow-xl">
      <p style={{ color: getParseColor(midpoint) }} className="font-semibold mb-0.5">{label}</p>
      <p className="text-ctp-subtext1">{payload[0].value} parses</p>
    </div>
  )
}

export function ParseHistogramChart({ data }: Props) {
  const { getParseColor } = useColourBlind()

  const buckets = useMemo(() => {
    const counts = Array(10).fill(0) as number[]
    for (const row of data) {
      const pct = Number(row.rank_percent)
      if (!isFinite(pct) || pct < 0) continue
      const idx = Math.min(Math.floor(pct / 10), 9)
      counts[idx]++
    }
    return counts.map((count, i) => ({
      label: i === 9 ? '90–100' : `${i * 10}–${i * 10 + 9}`,
      count,
      midpoint: i * 10 + 5,
    }))
  }, [data])

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={buckets} margin={{ top: 4, right: 4, left: -22, bottom: 0 }}>
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
      </BarChart>
    </ResponsiveContainer>
  )
}
