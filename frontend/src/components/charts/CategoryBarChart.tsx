import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { EntitySummaryRow } from '../../api'
import { formatNumber, capitalise } from '../../utils/format'

interface CategoryBarChartProps {
  data: EntitySummaryRow[]
}

const COLORS = ['#6b96f5', '#22c9a0', '#e8a020', '#f06050', '#a78bfa', '#38bdf8']

interface TooltipPayload {
  name: string
  value: number
}

function CustomTooltip({ active, payload, label }: {
  active?: boolean
  payload?: TooltipPayload[]
  label?: string
}) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-surface-3 border border-white/10 rounded-lg px-3 py-2 text-xs font-mono shadow-lg">
      <p className="text-slate-300 mb-1">{capitalise(label ?? '')}</p>
      <p className="text-brand-400">{formatNumber(payload[0].value)} entities</p>
    </div>
  )
}

export function CategoryBarChart({ data }: CategoryBarChartProps) {
  const sorted = [...data].sort((a, b) => b.total_count - a.total_count)

  return (
    <div className="bg-surface-2 rounded-xl p-5 shadow-card border border-white/5">
      <p className="section-heading">entities by category</p>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={sorted} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
          <XAxis
            dataKey="category"
            tickFormatter={capitalise}
            tick={{ fill: '#64748b', fontSize: 11, fontFamily: 'IBM Plex Mono' }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: '#64748b', fontSize: 11, fontFamily: 'IBM Plex Mono' }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => formatNumber(v)}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
          <Bar dataKey="total_count" radius={[4, 4, 0, 0]}>
            {sorted.map((_, index) => (
              <Cell key={index} fill={COLORS[index % COLORS.length]} opacity={0.85} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
