import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { BossProgressHistoryRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatDuration } from '../../constants/wow'

interface Props {
  data: BossProgressHistoryRow[]
}

function parseDate(value: string): Date {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? new Date(0) : date
}

function formatDateLabel(value: string) {
  const date = parseDate(value)
  return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

function slugify(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'boss'
}

function isKill(value: string | boolean) {
  return value === true || value === 'true' || value === 'True'
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload as BossProgressHistoryRow
  const killed = isKill(row.is_kill_on_night)

  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl max-w-72">
      <p className="text-ctp-text font-semibold mb-0.5">{formatDateLabel(row.raid_night_date)}</p>
      <p className="text-ctp-overlay1 truncate">{row.report_title || row.report_code}</p>
      <div className="mt-2 space-y-1">
        <p className="text-ctp-subtext1">
          Best HP: <span className="font-semibold">{killed ? 'Kill (0.0%)' : `${Number(row.best_boss_hp_remaining).toFixed(1)}%`}</span>
        </p>
        <p className="text-ctp-subtext1">
          Pulls: <span className="font-semibold">{row.pulls_on_night}</span>
          <span className="text-ctp-overlay0"> · </span>
          Wipes: <span className="font-semibold">{row.wipes_on_night}</span>
        </p>
        {killed && Number(row.kill_duration_seconds) > 0 && (
          <p className="text-ctp-subtext1">
            Kill time: <span className="font-semibold">{formatDuration(Number(row.kill_duration_seconds))}</span>
          </p>
        )}
      </div>
    </div>
  )
}

export function BossProgressHistoryChart({ data }: Props) {
  const { chartColors } = useColourBlind()

  if (data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No boss progress history exported yet
      </div>
    )
  }

  const gradId = slugify(`${data[0].encounter_id}-${data[0].difficulty}`)

  return (
    <ResponsiveContainer width="100%" height={260}>
      <AreaChart data={data} margin={{ top: 8, right: 8, left: -24, bottom: 0 }}>
        <defs>
          <linearGradient id={`boss-progress-${gradId}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={chartColors.secondary} stopOpacity={0.1} />
            <stop offset="100%" stopColor={chartColors.secondary} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
        <XAxis
          dataKey="raid_night_date"
          tickFormatter={formatDateLabel}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          domain={[0, 100]}
          tickFormatter={value => `${value}%`}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<Tip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
        <Area
          type="monotoneX"
          dataKey="best_boss_hp_remaining"
          name="Best HP"
          stroke={chartColors.secondary}
          strokeWidth={2}
          fill={`url(#boss-progress-${gradId})`}
          dot={{ r: 2.5, fill: chartColors.secondary, stroke: '#1e1e2e', strokeWidth: 1 }}
          activeDot={{ r: 4, fill: chartColors.secondary, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
