import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts'
import type { RaidSummary } from '../../types'
import { useColourBlind } from '../../context/ColourBlindContext'
import { formatNumber } from '../../utils/format'

interface Props {
  raids: RaidSummary[]
  mode?: 'tiers' | 'compare'
  difficulty?: 'All' | 'Mythic' | 'Heroic' | 'Normal'
}

interface TierWeekPoint {
  week_start: string
  bossKills: number
  wipes: number
  pulls: number
  week_index: number
}

type RawTierWeekPoint = Omit<TierWeekPoint, 'week_index'>

interface TierSeries {
  label: string
  data: TierWeekPoint[]
}

function slugify(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'tier'
}

function parseDate(value: string): Date {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? new Date(0) : date
}

function formatWeekLabel(value: string) {
  const date = parseDate(value)
  return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

function getRaidWeekStart(value: string): string {
  const date = parseDate(value)
  date.setHours(0, 0, 0, 0)
  const day = date.getDay()
  const daysSinceWednesday = (day - 3 + 7) % 7
  date.setDate(date.getDate() - daysSinceWednesday)
  return date.toISOString()
}

function addDays(value: string, days: number): string {
  const date = parseDate(value)
  date.setDate(date.getDate() + days)
  return date.toISOString()
}

function isRealZoneName(value: unknown): value is string {
  return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
}

function matchesDifficulty(raid: RaidSummary, difficulty: Props['difficulty']): boolean {
  if (!difficulty || difficulty === 'All') return true
  return String(raid.primary_difficulty ?? '').trim().toLowerCase() === difficulty.toLowerCase()
}

function buildTierSeries(
  raids: RaidSummary[],
  zoneName: string,
  difficulty: Props['difficulty']
): TierSeries {
  const zoneRaids = raids.filter(raid => raid.zone_name === zoneName)
  const filteredRaids = zoneRaids.filter(raid => matchesDifficulty(raid, difficulty))

  const firstAnchorWeek = getRaidWeekStart(
    String(
      [...zoneRaids]
        .sort((a, b) => String(a.raid_night_date ?? '').localeCompare(String(b.raid_night_date ?? '')))[0]?.raid_night_date ?? ''
    )
  )

  const lastRelevantWeek = getRaidWeekStart(
    String(
      [...zoneRaids]
        .sort((a, b) => String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? '')))[0]?.raid_night_date ?? ''
    )
  )

  const weekly = new Map<string, RawTierWeekPoint>()

  filteredRaids.forEach(raid => {
    const weekStart = getRaidWeekStart(String(raid.raid_night_date ?? ''))
    const current = weekly.get(weekStart) ?? {
      week_start: weekStart,
      bossKills: 0,
      wipes: 0,
      pulls: 0,
    }

    current.bossKills += Number(raid.boss_kills) || 0
    current.wipes += Number(raid.total_wipes) || 0
    current.pulls += Number(raid.total_pulls) || 0
    weekly.set(weekStart, current)
  })

  const points: TierWeekPoint[] = []
  if (firstAnchorWeek && lastRelevantWeek) {
    let cursor = firstAnchorWeek
    let weekIndex = 1
    while (cursor <= lastRelevantWeek) {
      const point = weekly.get(cursor) ?? {
        week_start: cursor,
        bossKills: 0,
        wipes: 0,
        pulls: 0,
      }
      points.push({ ...point, week_index: weekIndex })
      cursor = addDays(cursor, 7)
      weekIndex += 1
    }
  }

  return {
    label: zoneName,
    data: points,
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload as TierWeekPoint
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="text-ctp-overlay1 mb-2">Week of {formatWeekLabel(label)}</p>
      {payload.map((p: { name: string; value: number; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}: <span className="font-semibold">{formatNumber(p.value)}</span>
        </p>
      ))}
      <p className="text-ctp-overlay0 mt-2">Pulls: <span className="font-semibold">{formatNumber(row.pulls)}</span></p>
    </div>
  )
}

function TierChart({ series, showDivider = false }: { series: TierSeries; showDivider?: boolean }) {
  const { chartColors } = useColourBlind()
  const gradId = slugify(series.label)

  return (
    <div className={showDivider ? 'pt-4 border-t border-ctp-surface1' : ''}>
      <div className="mb-3">
        <p className="text-xs font-semibold text-ctp-text">{series.label}</p>
        <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5">All available Wednesday-start raid weeks</p>
      </div>
      <ResponsiveContainer width="100%" height={170}>
        <AreaChart data={series.data} margin={{ top: 4, right: 4, left: -22, bottom: 0 }}>
          <defs>
            <linearGradient id={`killsGrad-${gradId}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={chartColors.primary} stopOpacity={0.16} />
              <stop offset="95%" stopColor={chartColors.primary} stopOpacity={0} />
            </linearGradient>
            <linearGradient id={`wipesGrad-${gradId}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={chartColors.secondary} stopOpacity={0.12} />
              <stop offset="95%" stopColor={chartColors.secondary} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
          <XAxis
            dataKey="week_start"
            tickFormatter={formatWeekLabel}
            tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
            axisLine={false}
            tickLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
            axisLine={false}
            tickLine={false}
            allowDecimals={false}
          />
          <Tooltip content={<Tip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
          <Area
            type="monotoneX"
            dataKey="bossKills"
            name="Boss Kills"
            stroke={chartColors.primary}
            strokeWidth={2}
            fill={`url(#killsGrad-${gradId})`}
            dot={false}
            activeDot={{ r: 4, fill: chartColors.primary, stroke: '#1e1e2e', strokeWidth: 2 }}
          />
          <Area
            type="monotoneX"
            dataKey="wipes"
            name="Wipes"
            stroke={chartColors.secondary}
            strokeWidth={1.5}
            fill={`url(#wipesGrad-${gradId})`}
            dot={false}
            activeDot={{ r: 4, fill: chartColors.secondary, stroke: '#1e1e2e', strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

interface ComparePoint {
  week_index: number
  label: string
  currentTier: number | null
  previousTier: number | null
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CompareTip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="text-ctp-overlay1 mb-2">{label}</p>
      {payload.map((p: { name: string; value: number; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}: <span className="font-semibold">{formatNumber(p.value)}</span>
        </p>
      ))}
    </div>
  )
}

function buildComparisonData(
  currentTier: TierSeries | undefined,
  previousTier: TierSeries | undefined,
  metric: 'bossKills' | 'wipes'
): ComparePoint[] {
  const maxWeeks = Math.max(currentTier?.data.length ?? 0, previousTier?.data.length ?? 0)
  return Array.from({ length: maxWeeks }, (_, index) => ({
    week_index: index + 1,
    label: `Week ${index + 1}`,
    currentTier: currentTier?.data[index]?.[metric] ?? null,
    previousTier: previousTier?.data[index]?.[metric] ?? null,
  }))
}

function CompareChart({
  title,
  currentTier,
  previousTier,
  metric,
  showDivider = false,
}: {
  title: string
  currentTier?: TierSeries
  previousTier?: TierSeries
  metric: 'bossKills' | 'wipes'
  showDivider?: boolean
}) {
  const { topTierColor, chartColors } = useColourBlind()
  const data = buildComparisonData(currentTier, previousTier, metric)
  const currentColor = topTierColor
  const previousColor = chartColors.primary

  return (
    <div className={showDivider ? 'pt-4 border-t border-ctp-surface1' : ''}>
      <div className="mb-3">
        <p className="text-xs font-semibold text-ctp-text">{title}</p>
        <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5">
          Current tier vs previous tier by raid week number
        </p>
      </div>
      <ResponsiveContainer width="100%" height={170}>
        <AreaChart data={data} margin={{ top: 4, right: 4, left: -22, bottom: 0 }}>
          <defs>
            <linearGradient id={`compare-previous-${metric}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={previousColor} stopOpacity={0.1} />
              <stop offset="95%" stopColor={previousColor} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
            axisLine={false}
            tickLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
            axisLine={false}
            tickLine={false}
            allowDecimals={false}
          />
          <Tooltip content={<CompareTip />} cursor={{ stroke: '#45475a', strokeWidth: 1 }} />
          <Area
            type="monotone"
            dataKey="currentTier"
            name={currentTier?.label ?? 'Current Tier'}
            stroke={currentColor}
            strokeWidth={2}
            fillOpacity={0}
            dot={false}
            activeDot={{ r: 4, fill: currentColor, stroke: '#1e1e2e', strokeWidth: 2 }}
            connectNulls
          />
          <Area
            type="monotone"
            dataKey="previousTier"
            name={previousTier?.label ?? 'Previous Tier'}
            stroke={previousColor}
            strokeWidth={2}
            fill={`url(#compare-previous-${metric})`}
            dot={false}
            activeDot={{ r: 4, fill: previousColor, stroke: '#1e1e2e', strokeWidth: 2 }}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

export function WeeklyActivityChart({ raids, mode = 'tiers', difficulty = 'All' }: Props) {
  const tierNames = [...new Set(
    [...raids]
      .filter(raid => isRealZoneName(raid.zone_name))
      .sort((a, b) => parseDate(String(b.raid_night_date ?? '')).getTime() - parseDate(String(a.raid_night_date ?? '')).getTime())
      .map(raid => raid.zone_name)
  )].slice(0, 2)

  const tiers = tierNames.map(name => buildTierSeries(raids, name, difficulty))
  const [currentTier, previousTier] = tiers

  if (tiers.length === 0) {
    return (
      <div className="h-40 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No weekly raid activity data
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {mode === 'compare' ? (
        <>
          <CompareChart title="Boss Kills" currentTier={currentTier} previousTier={previousTier} metric="bossKills" />
          <CompareChart title="Boss Wipes" currentTier={currentTier} previousTier={previousTier} metric="wipes" showDivider />
        </>
      ) : (
        tiers.map((tier, index) => (
          <TierChart key={tier.label} series={tier} showDivider={index > 0} />
        ))
      )}
    </div>
  )
}
