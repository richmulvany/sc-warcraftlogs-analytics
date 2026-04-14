import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts'
import type { WeeklyActivity } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'

interface Props { data: WeeklyActivity[] }

function fmt(d: string) {
  return new Date(d).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: '2-digit' })
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="text-ctp-overlay1 mb-2">{fmt(label)}</p>
      {payload.map((p: { name: string; value: number; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>{p.name}: <span className="font-semibold">{p.value}</span></p>
      ))}
    </div>
  )
}

export function WeeklyActivityChart({ data }: Props) {
  const { chartColors } = useColourBlind()
  const killC = chartColors.primary
  const wipeC = chartColors.secondary
  const sorted = [...data].sort((a, b) => a.week_start.localeCompare(b.week_start))

  return (
    <ResponsiveContainer width="100%" height={210}>
      <AreaChart data={sorted} margin={{ top: 4, right: 4, left: -22, bottom: 0 }}>
        <defs>
          <linearGradient id="killsGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor={killC} stopOpacity={0.4} />
            <stop offset="95%" stopColor={killC} stopOpacity={0} />
          </linearGradient>
          <linearGradient id="wipesGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%"  stopColor={wipeC} stopOpacity={0.3} />
            <stop offset="95%" stopColor={wipeC} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
        <XAxis
          dataKey="week_start"
          tickFormatter={fmt}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<Tip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
        <Area
          type="monotoneX"
          dataKey="total_boss_kills"
          name="Boss Kills"
          stroke={killC}
          strokeWidth={2}
          fill="url(#killsGrad)"
          dot={false}
          activeDot={{ r: 4, fill: killC, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
        <Area
          type="monotoneX"
          dataKey="total_wipes"
          name="Wipes"
          stroke={wipeC}
          strokeWidth={1.5}
          fill="url(#wipesGrad)"
          dot={false}
          activeDot={{ r: 4, fill: wipeC, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
