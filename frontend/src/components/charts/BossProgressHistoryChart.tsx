import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { BossPullHistoryRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatDuration } from '../../constants/wow'

interface Props {
  data: BossPullHistoryRow[]
}

interface ChartPoint extends BossPullHistoryRow {
  pull_index: number
  best_so_far_hp: number
}

function parseDate(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? new Date(0) : date
}

function formatDateLabel(value: string) {
  return parseDate(value).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

function isKill(value: string | boolean) {
  return value === true || value === 'true' || value === 'True'
}

function buildChartData(rows: BossPullHistoryRow[]): ChartPoint[] {
  let bestSoFar = 100

  return [...rows]
    .sort((a, b) => {
      const byDate = String(a.raid_night_date).localeCompare(String(b.raid_night_date))
      if (byDate !== 0) return byDate
      const byStart = String(a.start_time_utc ?? '').localeCompare(String(b.start_time_utc ?? ''))
      if (byStart !== 0) return byStart
      const byReport = String(a.report_code).localeCompare(String(b.report_code))
      if (byReport !== 0) return byReport
      return Number(a.fight_id) - Number(b.fight_id)
    })
    .map((row, index) => {
      const hp = Number(row.boss_hp_remaining)
      if (Number.isFinite(hp)) bestSoFar = Math.min(bestSoFar, hp)
      return {
        ...row,
        pull_index: index + 1,
        best_so_far_hp: bestSoFar,
      }
    })
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload as ChartPoint
  const killed = isKill(row.is_kill)

  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl max-w-80">
      <p className="text-ctp-text font-semibold">Pull {row.pull_index}</p>
      <p className="text-ctp-overlay1 mt-0.5">{formatDateLabel(row.raid_night_date)} · {row.report_title || row.report_code}</p>
      <div className="mt-2 space-y-1">
        <p className="text-ctp-subtext1">
          Pull HP: <span className="font-semibold">{killed ? 'Kill (0.0%)' : `${Number(row.boss_hp_remaining).toFixed(1)}%`}</span>
        </p>
        <p className="text-ctp-subtext1">
          Best so far: <span className="font-semibold">{Number(row.best_so_far_hp).toFixed(1)}%</span>
        </p>
        <p className="text-ctp-subtext1">
          Duration: <span className="font-semibold">{formatDuration(Number(row.duration_seconds))}</span>
        </p>
        <p className="text-ctp-subtext1">
          Result: <span className="font-semibold">{killed ? 'Kill' : 'Wipe'}</span>
        </p>
      </div>
    </div>
  )
}

export function BossProgressHistoryChart({ data }: Props) {
  const { chartColors } = useColourBlind()

  if (data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No boss pull history exported yet
      </div>
    )
  }

  const chartData = buildChartData(data)

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={chartData} margin={{ top: 8, right: 12, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
        <XAxis
          dataKey="pull_index"
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          domain={[0, 100]}
          reversed
          tickFormatter={value => `${value}%`}
          tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<Tip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
        <Legend
          verticalAlign="top"
          align="right"
          wrapperStyle={{ fontSize: 11, fontFamily: 'IBM Plex Mono, monospace', color: '#6c7086' }}
        />
        <Line
          type="linear"
          dataKey="boss_hp_remaining"
          name="Pull HP"
          stroke={chartColors.primary}
          strokeWidth={1.5}
          dot={{ r: 2, fill: chartColors.primary }}
          activeDot={{ r: 4, fill: chartColors.primary, stroke: '#1e1e2e', strokeWidth: 2 }}
          connectNulls={false}
          strokeOpacity={0.7}
        />
        <Line
          type="stepAfter"
          dataKey="best_so_far_hp"
          name="Best So Far"
          stroke={chartColors.secondary}
          strokeWidth={2.25}
          dot={false}
          activeDot={{ r: 4, fill: chartColors.secondary, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
