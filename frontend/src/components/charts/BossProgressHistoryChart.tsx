import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { BossPullHistoryRow } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatDuration } from '../../constants/wow'
import { toFiniteNumber } from '../../utils/format'
import type { ChartTooltipProps } from './types'
import { CHART_TICK_STYLE } from '../../utils/chartStyle'

interface Props {
  data: BossPullHistoryRow[]
}

interface ChartPoint extends BossPullHistoryRow {
  pull_index: number
  best_so_far_hp: number
  max_phase_so_far: number
}

interface PhaseSegment {
  phase: number
  start: number
  end: number
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
  let maxPhaseSoFar = 0

  const sortedRows = [...rows]
    .sort((a, b) => {
      const byDate = String(a.raid_night_date).localeCompare(String(b.raid_night_date))
      if (byDate !== 0) return byDate
      const byStart = String(a.start_time_utc ?? '').localeCompare(String(b.start_time_utc ?? ''))
      if (byStart !== 0) return byStart
      const byReport = String(a.report_code).localeCompare(String(b.report_code))
      if (byReport !== 0) return byReport
      return Number(a.fight_id) - Number(b.fight_id)
    })

  const firstKillIndex = sortedRows.findIndex(row => isKill(row.is_kill))
  const truncatedRows = firstKillIndex >= 0 ? sortedRows.slice(0, firstKillIndex + 1) : sortedRows

  const points = truncatedRows
    .map((row, index) => {
      const hp = Number(row.boss_hp_remaining)
      if (Number.isFinite(hp)) bestSoFar = Math.min(bestSoFar, hp)
      maxPhaseSoFar = Math.max(maxPhaseSoFar, toFiniteNumber(row.last_phase) ?? 0)
      return {
        ...row,
        pull_index: index + 1,
        best_so_far_hp: bestSoFar,
        max_phase_so_far: maxPhaseSoFar,
      }
    })

  const first = points[0]
  if (!first) return []

  return [
    {
      ...first,
      pull_index: 0,
      boss_hp_remaining: 100,
      boss_percentage: 100,
      best_so_far_hp: 100,
      max_phase_so_far: 0,
      is_kill: false,
    },
    ...points,
  ]
}

function buildPhaseSegments(points: ChartPoint[]): PhaseSegment[] {
  const realPoints = points.slice(1)
  if (realPoints.length === 0) return []

  const highestPhase = Math.max(...realPoints.map(point => toFiniteNumber(point.max_phase_so_far) ?? 0))
  if (highestPhase <= 1) return []

  const segments: PhaseSegment[] = []
  let currentPhase = Math.max(1, toFiniteNumber(realPoints[0].max_phase_so_far) ?? 1)
  let start = 0

  for (let i = 1; i < realPoints.length; i++) {
    const phase = Math.max(1, toFiniteNumber(realPoints[i].max_phase_so_far) ?? 1)
    if (phase !== currentPhase) {
      segments.push({ phase: currentPhase, start, end: realPoints[i - 1].pull_index })
      start = realPoints[i - 1].pull_index
      currentPhase = phase
    }
  }

  segments.push({ phase: currentPhase, start, end: realPoints[realPoints.length - 1].pull_index })
  return segments
}

function Tip({ active, payload }: ChartTooltipProps<ChartPoint>) {
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
          Best pull: <span className="font-semibold">{Number(row.best_so_far_hp).toFixed(1)}%</span>
        </p>
        <p className="text-ctp-subtext1">
          Duration: <span className="font-semibold">{formatDuration(Number(row.duration_seconds))}</span>
        </p>
        <p className="text-ctp-subtext1">
          Phase reached: <span className="font-semibold">P{Math.max(1, toFiniteNumber(row.last_phase) ?? 1)}</span>
        </p>
        <p className="text-ctp-subtext1">
          Result: <span className="font-semibold">{killed ? 'Kill' : 'Wipe'}</span>
        </p>
      </div>
    </div>
  )
}

export function BossProgressHistoryChart({ data }: Props) {
  const { chartColors, phaseColors } = useColourBlind()

  if (data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No boss pull history exported yet
      </div>
    )
  }

  const chartData = buildChartData(data)
  const phaseSegments = buildPhaseSegments(chartData)

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={chartData} margin={{ top: 8, right: 12, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
        <XAxis
          dataKey="pull_index"
          tick={CHART_TICK_STYLE}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          domain={[0, 100]}
          tickFormatter={value => `${value}%`}
          tick={CHART_TICK_STYLE}
          axisLine={false}
          tickLine={false}
        />
        {phaseSegments.map((segment, index) => (
          <ReferenceArea
            key={`${segment.phase}-${segment.start}-${segment.end}-${index}`}
            x1={segment.start}
            x2={segment.end}
            y1={0}
            y2={100}
            fill={phaseColors[Math.min(Math.max(segment.phase - 1, 0), phaseColors.length - 1)]}
            fillOpacity={0.12}
            ifOverflow="extendDomain"
          />
        ))}
        {phaseSegments.map((segment, index) => (
          <ReferenceLine
            key={`phase-boundary-${segment.phase}-${segment.start}-${index}`}
            x={segment.start}
            stroke={phaseColors[Math.min(Math.max(segment.phase - 1, 0), phaseColors.length - 1)]}
            strokeOpacity={0.45}
            strokeDasharray="4 4"
            ifOverflow="extendDomain"
            label={{
              value: `P${segment.phase}`,
              position: 'insideBottomLeft',
              fill: phaseColors[Math.min(Math.max(segment.phase - 1, 0), phaseColors.length - 1)],
              fontSize: 10,
              fontFamily: 'IBM Plex Mono, monospace',
            }}
          />
        ))}
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
          name="Best Pull"
          stroke={chartColors.secondary}
          strokeWidth={2.25}
          dot={false}
          activeDot={{ r: 4, fill: chartColors.secondary, stroke: '#1e1e2e', strokeWidth: 2 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
