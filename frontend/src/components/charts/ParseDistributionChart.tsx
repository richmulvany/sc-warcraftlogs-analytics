import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import type { PlayerPerformanceSummary } from '../../types'
import { getClassColor } from '../../constants/wow'
import { useColourBlind } from '../../context/ColourBlindContext'
import type { ChartTooltipProps } from './types'
import { CHART_TICK_STYLE } from '../../utils/chartStyle'

interface Props { data: PlayerPerformanceSummary[]; limit?: number }

// Tooltip rendered by Recharts — must be a component that can use hooks
function Tip({ active, payload }: ChartTooltipProps<PlayerPerformanceSummary>) {
  const { getParseColor } = useColourBlind()
  if (!active || !payload?.length) return null
  const d = payload[0].payload as PlayerPerformanceSummary
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl min-w-[160px]">
      <p style={{ color: getClassColor(d.player_class) }} className="font-semibold mb-1">{d.player_name}</p>
      <p className="text-ctp-overlay1">{d.player_class} · {d.primary_spec}</p>
      <p className="text-ctp-subtext1 mt-1">
        Avg: <span style={{ color: getParseColor(d.avg_rank_percent) }}>{d.avg_rank_percent?.toFixed(1)}%</span>
      </p>
      <p className="text-ctp-subtext1">
        Best: <span style={{ color: getParseColor(d.best_rank_percent) }}>{d.best_rank_percent?.toFixed(1)}%</span>
      </p>
    </div>
  )
}

export function ParseDistributionChart({ data, limit = 15 }: Props) {
  const { getParseColor } = useColourBlind()
  const sorted = [...data]
    .filter(d => d.avg_rank_percent != null && d.avg_rank_percent > 0)
    .sort((a, b) => b.avg_rank_percent - a.avg_rank_percent)
    .slice(0, limit)

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={sorted} margin={{ top: 4, right: 4, left: -22, bottom: 36 }}>
        <XAxis
          dataKey="player_name"
          tick={CHART_TICK_STYLE}
          axisLine={false}
          tickLine={false}
          angle={-40}
          textAnchor="end"
          interval={0}
        />
        <YAxis
          domain={[0, 100]}
          tick={CHART_TICK_STYLE}
          axisLine={false}
          tickLine={false}
          tickFormatter={v => `${v}%`}
        />
        <Tooltip content={<Tip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
        <Bar dataKey="avg_rank_percent" radius={[4, 4, 0, 0]}>
          {sorted.map((entry, i) => (
            <Cell key={i} fill={getParseColor(entry.avg_rank_percent)} fillOpacity={0.85} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
