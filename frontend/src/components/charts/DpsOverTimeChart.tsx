import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from 'recharts'
import { formatThroughput } from '../../constants/wow'
import { getClassColor } from '../../constants/wow'

interface DataPoint {
  date: string
  throughput: number
  boss: string
  parse: number | null
}

interface Props {
  data: DataPoint[]
  playerClass: string
  avgThroughput?: number
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  const d = payload[0].payload as DataPoint
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl min-w-[160px]">
      <p className="text-ctp-overlay1 mb-1.5">{label}</p>
      <p className="text-ctp-subtext1 truncate mb-1">{d.boss}</p>
      <p className="text-ctp-text font-semibold">{formatThroughput(d.throughput)}</p>
      {d.parse !== null && d.parse > 0 && (
        <p className="text-ctp-overlay1 mt-0.5">Parse: {d.parse.toFixed(1)}%</p>
      )}
    </div>
  )
}

export function DpsOverTimeChart({ data, playerClass, avgThroughput }: Props) {
  const color = getClassColor(playerClass)
  const sorted = [...data].sort((a, b) => a.date.localeCompare(b.date))

  if (!sorted.length) {
    return (
      <div className="h-48 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No fight data available
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={sorted} margin={{ top: 4, right: 12, left: -10, bottom: 0 }}>
        <defs>
          <linearGradient id="dpsDot" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.8} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
        <XAxis
          dataKey="date"
          tickFormatter={d => new Date(d).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          tickFormatter={v => formatThroughput(v)}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<Tip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
        {avgThroughput && avgThroughput > 0 && (
          <ReferenceLine
            y={avgThroughput}
            stroke={color}
            strokeDasharray="4 4"
            strokeOpacity={0.4}
          />
        )}
        <Line
          type="monotone"
          dataKey="throughput"
          name="DPS / HPS"
          stroke={color}
          strokeWidth={2}
          dot={{ r: 3, fill: color, stroke: '#1e1e2e', strokeWidth: 2 }}
          activeDot={{ r: 5, fill: color, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
