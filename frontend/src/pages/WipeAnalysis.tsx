import { useEffect, useMemo, useState, type ReactNode } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { FilterSelect } from '../components/ui/FilterSelect'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { DiffBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot } from '../components/ui/ClassLabel'
import {
  useRaidSummary,
  useBossMechanics,
  usePlayerDeathEvents,
  useBossWipeAnalysis,
  useBossPullHistory,
  useBossKillRoster,
  usePlayerUtilityByPull,
  useWipeSurvivalEvents,
  useWipeCooldownUtilization,
} from '../hooks/useGoldData'
import { formatDate, formatNumber, formatPct } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal'] as const

type SurvivabilitySortKey =
  | 'deathsPerKill'
  | 'deathsPerPull'
  | 'deaths'
  | 'wipeDeath'
  | 'killDeath'
  | 'kills'
  | 'pulls'
type SortDirection = 'asc' | 'desc'
type WipeSurvivalSortKey =
  | 'survivalFailureScore'
  | 'wipeDeaths'
  | 'defensiveCapacityUsedPct'
  | 'noHealthstonePct'
  | 'noHealthPotionPct'

interface WipeSurvivalFailureRow {
  player_name: string
  player_class: string
  wipe_pulls_tracked: number
  wipe_deaths: number
  deaths_per_wipe_pull: number
  no_healthstone_deaths: number
  no_health_potion_deaths: number
  defensive_possible_casts: number
  defensive_actual_casts: number
  defensive_missed_casts: number
  defensive_capacity_used_pct: number
  no_healthstone_pct: number
  no_health_potion_pct: number
  weighted_failure_points: number
  survival_failure_score: number
  survival_grade: 'S' | 'A' | 'B' | 'C' | 'D' | 'E' | 'F'
  top_missing_category: string
  most_common_killing_blow: string
  most_common_killing_blow_count: number
}

interface CooldownCapacityRow {
  key: string
  player_name: string
  player_class: string
  ability_name: string
  possible_casts: number
  actual_casts: number
  missed_casts: number
  cast_efficiency_pct: number
  pulls_tracked: number
}

interface ScopedSurvivabilityRow {
  player_name: string
  player_class: string
  total_deaths: number
  wipe_deaths: number
  kill_deaths: number
  kills_tracked: number
  pulls_tracked: number
  deaths_per_kill: number | null
  deaths_per_pull: number | null
  most_common_killing_blow: string
  most_common_killing_blow_count: number
}

interface DeathTimingSummary {
  count: number
  min: number
  q1: number
  median: number
  q3: number
  max: number
  lowerWhisker: number
  upperWhisker: number
  outliers: number[]
}

function quantile(sorted: number[], q: number) {
  if (!sorted.length) return 0
  const pos = (sorted.length - 1) * q
  const base = Math.floor(pos)
  const rest = pos - base
  const next = sorted[base + 1]
  return next !== undefined
    ? sorted[base] + rest * (next - sorted[base])
    : sorted[base]
}

function formatTimingLabel(ms: number) {
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`
  const mins = ms / 60_000
  return Number.isInteger(mins) ? `${mins}m` : `${mins.toFixed(1)}m`
}

function buildTimingTicks(domainMax: number) {
  const maxMins = domainMax / 60_000
  const candidates = [0.5, 1, 2, 5, 10, 15, 20, 30]
  const interval = candidates.find(c => maxMins / c <= 5) ?? 30

  const ticks: number[] = []
  for (let m = 0; m * 60_000 <= domainMax; m += interval) {
    ticks.push(m * 60_000)
  }

  return ticks
}

function formatAxisTick(ms: number) {
  const mins = ms / 60_000
  return Number.isInteger(mins) ? `${mins}m` : `${mins.toFixed(1)}m`
}

// Case-insensitive normaliser — WCL / pipeline may export role as 'tank', 'HEALER', '1', etc.
function normalizeRole(raw: string | null | undefined): 'Tank' | 'Healer' | 'DPS' | 'Unknown' {
  if (!raw) return 'Unknown'
  const s = raw.toLowerCase().trim()
  if (s === 'tank' || s === '1') return 'Tank'
  if (s === 'healer' || s === 'heal' || s === '2') return 'Healer'
  if (s === 'dps' || s === 'damage' || s === 'ranged' || s === 'melee' || s === '3') return 'DPS'
  return 'Unknown'
}

// Fallback for players not in the kill roster (pure-DPS classes only; hybrids → Unknown)
const CLASS_ROLE_FALLBACK: Record<string, 'DPS'> = {
  Hunter: 'DPS',
  Mage: 'DPS',
  Rogue: 'DPS',
  Warlock: 'DPS',
}

function DeathTimingBoxPlot({
  summary,
  boxColor,
  lineColor,
  axisColor,
  labelColor,
}: {
  summary: DeathTimingSummary
  boxColor: string
  lineColor: string
  axisColor: string
  labelColor: string
}) {
  const domainMax = Math.max(summary.upperWhisker * 1.1, 60_000)
  const ticks = buildTimingTicks(domainMax)

  const pct = (value: number) => Math.max(0, Math.min(100, (value / domainMax) * 100))

  const lower = pct(summary.lowerWhisker)
  const q1Pos = pct(summary.q1)
  const medPos = pct(summary.median)
  const q3Pos = pct(summary.q3)
  const upper = pct(summary.upperWhisker)
  const boxW = Math.max(q3Pos - q1Pos, 1.5)
  const boxLeft = q1Pos
  const boxRight = q1Pos + boxW

  return (
    <div className="w-full">
      <svg
        viewBox="0 0 100 52"
        className="h-52 w-full overflow-visible"
        aria-label="Death timing box plot"
      >
        <line
          x1="0"
          y1="40"
          x2="100"
          y2="40"
          stroke={axisColor}
          strokeOpacity="0.2"
          strokeWidth="0.35"
        />

        <line
          x1={lower}
          y1="22"
          x2={boxLeft}
          y2="22"
          stroke={lineColor}
          strokeOpacity="0.48"
          strokeWidth="0.55"
        />
        <line
          x1={boxRight}
          y1="22"
          x2={upper}
          y2="22"
          stroke={lineColor}
          strokeOpacity="0.48"
          strokeWidth="0.55"
        />
        <line
          x1={lower}
          y1="18"
          x2={lower}
          y2="26"
          stroke={lineColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />
        <line
          x1={upper}
          y1="18"
          x2={upper}
          y2="26"
          stroke={lineColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />

        <rect
          x={q1Pos}
          y="16"
          width={boxW}
          height="12"
          rx="1.2"
          fill={boxColor}
          fillOpacity="0.1"
          stroke={boxColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />

        <line
          x1={medPos}
          y1="13"
          x2={medPos}
          y2="31"
          stroke={boxColor}
          strokeWidth="1.1"
          strokeLinecap="round"
        />

        {summary.outliers.slice(0, 40).map((value, index) => (
          <circle
            key={`${value}-${index}`}
            cx={pct(value)}
            cy="22"
            r="1"
            fill={boxColor}
            fillOpacity="0.7"
          />
        ))}

        {ticks.map((tick, index) => {
          const x = pct(tick)
          const anchor =
            index === 0 ? 'start' : index === ticks.length - 1 ? 'end' : 'middle'

          return (
            <g key={index}>
              <line
                x1={x}
                y1="40"
                x2={x}
                y2="43.5"
                stroke={axisColor}
                strokeOpacity="0.35"
                strokeWidth="0.35"
              />
              <text
                x={x}
                y="50"
                textAnchor={anchor}
                fontSize="4.5"
                fill={labelColor}
                style={{ fontFamily: 'IBM Plex Mono, monospace' }}
              >
                {formatAxisTick(tick)}
              </text>
            </g>
          )
        })}
      </svg>

      <div className="mt-2 grid grid-cols-4 gap-2">
        {[
          { label: 'Q1', value: formatTimingLabel(summary.q1) },
          { label: 'Median', value: formatTimingLabel(summary.median) },
          { label: 'Q3', value: formatTimingLabel(summary.q3) },
          { label: 'Samples', value: formatNumber(summary.count) },
        ].map(({ label, value }) => (
          <div
            key={label}
            className="rounded-lg border border-ctp-surface1 bg-ctp-surface1/20 px-2.5 py-2.5 text-center"
          >
            <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
              {label}
            </p>
            <p className="mt-1 font-mono text-sm font-semibold text-ctp-text">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}

function isKillRow(row: { is_kill?: boolean | string | null }) {
  return row.is_kill === true || row.is_kill === 'true'
}

function isPositiveFlag(value: unknown) {
  return value === true || value === 'true' || Number(value) > 0
}

function sectionTotal<T extends { [k: string]: unknown }>(rows: T[], key: keyof T) {
  return rows.reduce((sum, row) => sum + Number(row[key] ?? 0), 0)
}

function getDefaultSortDirection(key: SurvivabilitySortKey): SortDirection {
  return key === 'deathsPerKill' || key === 'deathsPerPull' ? 'asc' : 'desc'
}

function pct(numerator: number, denominator: number) {
  return denominator > 0 ? (numerator / denominator) * 100 : 0
}

function gradeForPercentile(percentile: number): WipeSurvivalFailureRow['survival_grade'] {
  if (percentile <= 0.1) return 'S'
  if (percentile <= 0.25) return 'A'
  if (percentile <= 0.4) return 'B'
  if (percentile <= 0.55) return 'C'
  if (percentile <= 0.7) return 'D'
  if (percentile <= 0.85) return 'E'
  return 'F'
}

function gradeClassName(grade: WipeSurvivalFailureRow['survival_grade']) {
  switch (grade) {
    case 'S':
      return 'border-ctp-green/30 bg-ctp-green/10 text-ctp-green'
    case 'A':
      return 'border-ctp-teal/30 bg-ctp-teal/10 text-ctp-teal'
    case 'B':
      return 'border-ctp-blue/30 bg-ctp-blue/10 text-ctp-blue'
    case 'C':
      return 'border-ctp-overlay1/30 bg-ctp-surface1/40 text-ctp-overlay1'
    case 'D':
      return 'border-ctp-yellow/30 bg-ctp-yellow/10 text-ctp-yellow'
    case 'E':
      return 'border-ctp-peach/30 bg-ctp-peach/10 text-ctp-peach'
    case 'F':
      return 'border-ctp-red/30 bg-ctp-red/10 text-ctp-red'
  }
}

function MiniNote({ children }: { children: ReactNode }) {
  return <p className="text-[10px] font-mono text-ctp-overlay0">{children}</p>
}

function StatusPill({
  label,
  active = false,
  compact = false,
}: {
  label: string
  active?: boolean
  compact?: boolean
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full border font-mono uppercase ${
        compact ? 'px-1.5 py-0.5 text-[9px] tracking-[0.1em]' : 'px-2.5 py-1 text-[10px] tracking-[0.18em]'
      } ${
        active
          ? 'border-ctp-mauve/30 bg-ctp-mauve/10 text-ctp-mauve'
          : 'border-ctp-surface2 bg-ctp-surface1/50 text-ctp-overlay0'
      }`}
    >
      {label}
    </span>
  )
}

function SignalTile({
  label,
  value,
  detail,
  accentClass = 'text-ctp-text',
}: {
  label: string
  value: ReactNode
  detail: ReactNode
  accentClass?: string
}) {
  return (
    <div className="rounded-2xl border border-ctp-surface1/60 bg-ctp-surface1/30 p-3.5">
      <p className="mb-2 text-[10px] font-mono uppercase tracking-[0.18em] text-ctp-overlay0">
        {label}
      </p>
      <div className={`text-sm font-semibold leading-tight ${accentClass}`}>{value}</div>
      <p className="mt-1 text-[10px] font-mono leading-relaxed text-ctp-overlay0">{detail}</p>
    </div>
  )
}

interface ProgressSnapshotDatum {
  boss_name: string
  difficulty_label?: string | null
  label: string
  subLabel: string
  open: number
  close: number
  high: number
  low: number
  currentNight: string
  previousNight: string | null
  pullCount: number
}

function clampPct(value: number) {
  if (!Number.isFinite(value)) return 0
  return Math.max(0, Math.min(100, value))
}

function shortDateLabel(date: string) {
  const parsed = new Date(date)
  if (Number.isNaN(parsed.getTime())) return date
  return parsed.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

function ProgressSnapshotCandles({
  data,
  improvedColor,
  worseColor,
  neutralColor,
}: {
  data: ProgressSnapshotDatum[]
  improvedColor: string
  worseColor: string
  neutralColor: string
}) {
  const width = 720
  const chartHeight = 220
  const chartTop = 12
  const chartBottom = 46
  const plotHeight = chartHeight - chartTop - chartBottom
  const leftPad = 36
  const rightPad = 18
  const slotWidth = (width - leftPad - rightPad) / Math.max(data.length, 1)
  const candleWidth = Math.max(3, Math.min(18, slotWidth * 0.46))
  const wickWidth = data.length > 24 ? 1 : 1.5
  const labelEvery = data.length <= 10 ? 1 : Math.ceil(data.length / 10)
  const yForPct = (value: number) => chartTop + ((100 - clampPct(value)) / 100) * plotHeight
  const ticks = [100, 75, 50, 25, 0]

  return (
    <div className="pb-1">
      <svg
        viewBox={`0 0 ${width} ${chartHeight}`}
        className="h-[250px] w-full"
        role="img"
        aria-label="Progress snapshot candle chart"
        preserveAspectRatio="none"
      >
        {ticks.map(tick => {
          const y = yForPct(tick)
          return (
            <g key={tick}>
              <line
                x1={leftPad}
                x2={width - rightPad}
                y1={y}
                y2={y}
                stroke="#45475a"
                strokeDasharray={tick === 0 ? undefined : '4 6'}
                strokeOpacity={tick === 0 ? 0.55 : 0.42}
              />
              <text
                x={leftPad - 10}
                y={y + 3}
                textAnchor="end"
                className="fill-ctp-overlay0 font-mono text-[10px]"
              >
                {tick}%
              </text>
            </g>
          )
        })}

        {data.map((row, index) => {
          const x = leftPad + index * slotWidth + slotWidth / 2
          const open = clampPct(Number(row.open))
          const close = clampPct(Number(row.close))
          const high = clampPct(Number(row.high))
          const low = clampPct(Number(row.low))
          const highY = yForPct(high)
          const lowY = yForPct(low)
          const openY = yForPct(open)
          const closeY = yForPct(close)
          const bodyTop = Math.min(openY, closeY)
          const bodyHeight = Math.max(7, Math.abs(openY - closeY))
          const improved = close < open
          const regressed = close > open
          const bodyColor = improved ? improvedColor : regressed ? worseColor : neutralColor
          const showLabel = index % labelEvery === 0 || index === data.length - 1
          const tooltip = [
            `${row.boss_name}${row.difficulty_label ? ` (${row.difficulty_label})` : ''}`,
            row.currentNight,
            row.previousNight ? `Open (${row.previousNight} final pull): ${open.toFixed(1)}%` : `Open: ${open.toFixed(1)}%`,
            `Close (${row.currentNight} final pull): ${close.toFixed(1)}%`,
            `High: ${high.toFixed(1)}%`,
            `Low: ${low.toFixed(1)}%`,
            `Pulls: ${formatNumber(row.pullCount)}`,
          ]
            .filter(Boolean)
            .join('\n')

          return (
            <g key={`${row.boss_name}-${row.difficulty_label ?? 'unknown'}-${row.currentNight}-${index}`}>
              <title>{tooltip}</title>
              <line
                x1={x}
                x2={x}
                y1={highY}
                y2={lowY}
                stroke={bodyColor}
                strokeWidth={wickWidth}
                strokeLinecap="round"
                opacity={0.95}
              />
              <rect
                x={x - candleWidth / 2}
                y={bodyTop}
                width={candleWidth}
                height={bodyHeight}
                rx={1.5}
                fill={bodyColor}
                fillOpacity={improved ? 0.04 : regressed ? 0.78 : 0.18}
                stroke={bodyColor}
                strokeWidth={1.5}
              />
              {showLabel ? (
                <>
                  <text
                    x={x}
                    y={chartHeight - 27}
                    textAnchor="middle"
                    className="fill-ctp-subtext1 font-mono text-[10px]"
                  >
                    {row.label}
                  </text>
                  {data.length <= 14 ? (
                    <text
                      x={x}
                      y={chartHeight - 12}
                      textAnchor="middle"
                      className="fill-ctp-overlay0 font-mono text-[9px]"
                    >
                      {row.subLabel}
                    </text>
                  ) : null}
                </>
              ) : null}
            </g>
          )
        })}
      </svg>
    </div>
  )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CtpTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="min-w-[170px] rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      {label ? <p className="mb-2 text-ctp-overlay1">{label}</p> : null}
      {payload.map((p: { name: string; value: number | string; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}:{' '}
          <span className="font-semibold">
            {typeof p.value === 'number' ? formatNumber(p.value) : p.value}
          </span>
        </p>
      ))}
    </div>
  )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function WipeWallTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  const bestPct = Number(row.bestPct)
  const avgPct = Number(row.avgPct)
  return (
    <div className="min-w-[230px] space-y-1 rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="mb-1 font-semibold text-ctp-text">{row.fullName}</p>
      <div className="flex items-center gap-2">
        <p className="text-ctp-overlay1">{row.diff}</p>
        <StatusPill label={row.isCleared ? 'Cleared' : 'Active'} active={!row.isCleared} />
      </div>
      <p style={{ color: payload[0].color }}>
        Wipes: <span className="font-semibold">{formatNumber(row.wipes)}</span>
      </p>
      <p className="text-ctp-subtext1">
        Best pull: <span className="font-semibold">{bestPct > 0 ? formatPct(bestPct) : '—'}</span>
      </p>
      <p className="text-ctp-subtext1">
        Avg wipe: <span className="font-semibold">{avgPct > 0 ? formatPct(avgPct) : '—'}</span>
      </p>
      <p className="text-ctp-overlay1">
        Nights attempted: <span className="font-semibold">{formatNumber(row.nights)}</span>
      </p>
    </div>
  )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function RecurringKillerTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  return (
    <div className="min-w-[220px] space-y-1 rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="mb-1 font-semibold text-ctp-text">{row.fullName}</p>
      <p style={{ color: payload[0].color }}>
        Unique players killed: <span className="font-semibold">{row.uniquePlayers}</span>
      </p>
      <p className="text-ctp-subtext1">
        Wipe deaths: <span className="font-semibold">{row.deaths}</span>
      </p>
      <p className="text-ctp-overlay1">
        Bosses affected: <span className="font-semibold">{row.uniqueBosses}</span>
      </p>
    </div>
  )
}

export function WipeAnalysis() {
  const {
    wipeColor,
    phaseColors,
    chartColors,
    getDeathRateColor,
    getParseColor,
    topTierColor,
  } = useColourBlind()

  const mechs = useBossMechanics()
  const deathEvents = usePlayerDeathEvents()
  const wipes = useBossWipeAnalysis()
  const pullHistory = useBossPullHistory()
  const roster = useBossKillRoster()
  const raidSummary = useRaidSummary()
  const utilityByPull = usePlayerUtilityByPull()
  const wipeSurvivalEvents = useWipeSurvivalEvents()
  const wipeCooldownUtilization = useWipeCooldownUtilization()

  const [diff, setDiff] = useState<string>('Mythic')
  const [selectedTier, setSelectedTier] = useState<string>('All')
  const [selectedBoss, setSelectedBoss] = useState<string>('All')
  const [search, setSearch] = useState('')
  const [survivabilitySort, setSurvivabilitySort] = useState<{
    key: SurvivabilitySortKey
    direction: SortDirection
  }>({
    key: 'deathsPerPull',
    direction: 'desc',
  })
  const [wipeSurvivalSort, setWipeSurvivalSort] = useState<{
    key: WipeSurvivalSortKey
    direction: SortDirection
  }>({
    key: 'survivalFailureScore',
    direction: 'desc',
  })

  const loading =
    mechs.loading ||
    deathEvents.loading ||
    wipes.loading ||
    pullHistory.loading ||
    roster.loading ||
    raidSummary.loading ||
    utilityByPull.loading ||
    wipeSurvivalEvents.loading ||
    wipeCooldownUtilization.loading
  const error =
    mechs.error ||
    deathEvents.error ||
    wipes.error ||
    pullHistory.error ||
    roster.error ||
    raidSummary.error ||
    utilityByPull.error ||
    wipeSurvivalEvents.error ||
    wipeCooldownUtilization.error

  const tierOptions = useMemo(() => {
    const values = [...wipes.data]
      .sort((a, b) =>
        String(b.latest_wipe_date ?? '').localeCompare(String(a.latest_wipe_date ?? ''))
      )
      .map(row => row.zone_name)
      .filter(isIncludedZoneName)

    return ['All', ...new Set(values)]
  }, [wipes.data])

  const currentTier = tierOptions[1] ?? 'All'

  useEffect(() => {
    if (selectedTier === 'All' && currentTier !== 'All') {
      setSelectedTier(currentTier)
    }
  }, [selectedTier, currentTier])

  const bossOptions = useMemo(() => {
    const values = [
      ...new Set(
        wipes.data
          .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
          .filter(row => isIncludedZoneName(row.zone_name))
          .map(row => row.boss_name)
          .filter(Boolean)
      ),
    ].sort()
    return ['All', ...values]
  }, [wipes.data, selectedTier])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const filteredWipes = useMemo(
    () =>
      wipes.data
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
        .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes)),
    [wipes.data, diff, selectedTier, selectedBoss, search]
  )

  const filteredMechanics = useMemo(
    () =>
      mechs.data
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
        .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes)),
    [mechs.data, diff, selectedTier, selectedBoss, search]
  )

  const mechanicsMap = useMemo(() => {
    const map = new Map<string, (typeof mechs.data)[number]>()
    for (const row of mechs.data) {
      map.set(`${row.encounter_id}-${row.difficulty}`, row)
    }
    return map
  }, [mechs.data])

  const scopedDeathRows = useMemo(
    () =>
      deathEvents.data
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase())),
    [deathEvents.data, diff, selectedTier, selectedBoss, search]
  )

  const scopedRaidSummaryByReport = useMemo(() => {
    const map = new Map<string, (typeof raidSummary.data)[number]>()
    for (const row of raidSummary.data) {
      if (!isIncludedZoneName(row.zone_name)) continue
      if (selectedTier !== 'All' && row.zone_name !== selectedTier) continue
      if (diff !== 'All' && row.primary_difficulty !== diff) continue
      map.set(row.report_code, row)
    }
    return map
  }, [raidSummary.data, selectedTier, diff])

  const killedEncounterKeys = useMemo(
    () =>
      new Set(
        roster.data
          .filter(row => diff === 'All' || row.difficulty_label === diff)
          .filter(row => isIncludedZoneName(row.zone_name))
          .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
          .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
          .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
          .map(row => `${row.encounter_id}-${row.difficulty}`)
      ),
    [roster.data, diff, selectedTier, selectedBoss, search]
  )

  const unresolvedWipes = useMemo(
    () =>
      filteredWipes.filter(row => !killedEncounterKeys.has(`${row.encounter_id}-${row.difficulty}`)),
    [filteredWipes, killedEncounterKeys]
  )

  const scopedPlayerNames = useMemo(() => {
    const encounterKeys = new Set(filteredWipes.map(row => `${row.encounter_id}-${row.difficulty}`))
    return new Set(
      roster.data
        .filter(row => encounterKeys.has(`${row.encounter_id}-${row.difficulty}`))
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
        .map(row => row.player_name)
    )
  }, [filteredWipes, roster.data, selectedTier, selectedBoss, search])

  const scopedSurvivability = useMemo((): ScopedSurvivabilityRow[] => {
    const playerNames = new Set(scopedPlayerNames)
    for (const row of scopedDeathRows) {
      if (row.player_name) playerNames.add(row.player_name)
    }

    const rows = new Map<string, ScopedSurvivabilityRow>()
    const killingBlowsByPlayer = new Map<string, Map<string, number>>()
    const attendedReportsByPlayer = new Map<string, Set<string>>()

    function ensurePlayer(playerName: string, playerClass = 'Unknown') {
      const existing = rows.get(playerName)
      if (existing) {
        if (existing.player_class === 'Unknown' && playerClass) existing.player_class = playerClass
        return existing
      }

      const row: ScopedSurvivabilityRow = {
        player_name: playerName,
        player_class: playerClass || 'Unknown',
        total_deaths: 0,
        wipe_deaths: 0,
        kill_deaths: 0,
        kills_tracked: 0,
        pulls_tracked: 0,
        deaths_per_kill: null,
        deaths_per_pull: null,
        most_common_killing_blow: '',
        most_common_killing_blow_count: 0,
      }
      rows.set(playerName, row)
      return row
    }

    for (const row of roster.data) {
      if (!playerNames.has(row.player_name)) continue
      if (!isIncludedZoneName(row.zone_name)) continue
      if (selectedTier !== 'All' && row.zone_name !== selectedTier) continue
      if (diff !== 'All' && row.difficulty_label !== diff) continue
      if (selectedBoss !== 'All' && row.boss_name !== selectedBoss) continue
      if (search.trim() && !row.boss_name.toLowerCase().includes(search.toLowerCase())) continue
      ensurePlayer(row.player_name, row.player_class).kills_tracked += 1
      if (!attendedReportsByPlayer.has(row.player_name)) {
        attendedReportsByPlayer.set(row.player_name, new Set())
      }
      attendedReportsByPlayer.get(row.player_name)!.add(row.report_code)
    }

    for (const row of scopedDeathRows) {
      const player = ensurePlayer(row.player_name, row.player_class)
      player.total_deaths += 1

      if (isKillRow(row)) player.kill_deaths += 1
      else player.wipe_deaths += 1

      if (!row.killing_blow_name) continue
      if (!killingBlowsByPlayer.has(row.player_name)) {
        killingBlowsByPlayer.set(row.player_name, new Map())
      }
      const playerBlows = killingBlowsByPlayer.get(row.player_name)!
      playerBlows.set(row.killing_blow_name, (playerBlows.get(row.killing_blow_name) ?? 0) + 1)
    }

    killingBlowsByPlayer.forEach((counts, playerName) => {
      const top = [...counts.entries()].sort((a, b) => b[1] - a[1])[0]
      const row = rows.get(playerName)
      if (row && top) {
        row.most_common_killing_blow = top[0]
        row.most_common_killing_blow_count = top[1]
      }
    })

    return [...rows.values()].map(row => {
      const participatedWipes = [...(attendedReportsByPlayer.get(row.player_name) ?? new Set<string>())]
        .reduce((sum, reportCode) => {
          const report = scopedRaidSummaryByReport.get(reportCode)
          return sum + Number(report?.total_wipes ?? 0)
        }, 0)
      const totalPulls = row.kills_tracked + participatedWipes
      return {
        ...row,
        pulls_tracked: totalPulls,
        deaths_per_kill: row.kills_tracked > 0 ? row.total_deaths / row.kills_tracked : null,
        deaths_per_pull: totalPulls > 0 ? row.total_deaths / totalPulls : null,
      }
    })
  }, [
    scopedPlayerNames,
    scopedDeathRows,
    roster.data,
    selectedTier,
    diff,
    selectedBoss,
    search,
    scopedRaidSummaryByReport,
  ])

  const playersWithTrackedKills = useMemo(
    () => scopedSurvivability.filter(row => Number(row.kills_tracked) > 0),
    [scopedSurvivability]
  )

  const wipeOnlyDeaths = useMemo(() => scopedDeathRows.filter(row => !isKillRow(row)), [scopedDeathRows])

  const scopedWipeUtilityRows = useMemo(
    () =>
      utilityByPull.data
        .filter(row => !isKillRow(row))
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase())),
    [utilityByPull.data, diff, selectedTier, selectedBoss, search]
  )

  const scopedWipeSurvivalEventRows = useMemo(
    () =>
      wipeSurvivalEvents.data
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase())),
    [wipeSurvivalEvents.data, diff, selectedTier, selectedBoss, search]
  )

  const scopedCooldownUtilizationRows = useMemo(
    () =>
      wipeCooldownUtilization.data
        .filter(row => diff === 'All' || row.difficulty_label === diff)
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
        .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase())),
    [wipeCooldownUtilization.data, diff, selectedTier, selectedBoss, search]
  )

  const cooldownCapacityRows = useMemo(() => {
    const groups = new Map<string, CooldownCapacityRow>()

    for (const row of scopedCooldownUtilizationRows) {
      const key = `${row.cooldown_category}:${row.player_name}:${row.ability_id}`
      const existing = groups.get(key) ?? {
        key,
        player_name: row.player_name,
        player_class: row.player_class,
        ability_name: row.ability_name,
        possible_casts: 0,
        actual_casts: 0,
        missed_casts: 0,
        cast_efficiency_pct: 0,
        pulls_tracked: 0,
      }

      existing.possible_casts += Number(row.possible_casts) || 0
      existing.actual_casts += Number(row.actual_casts) || 0
      existing.missed_casts += Number(row.missed_casts) || 0
      existing.pulls_tracked += 1
      groups.set(key, existing)
    }

    return [...groups.values()].map(row => ({
      ...row,
      cast_efficiency_pct:
        row.possible_casts > 0 ? (row.actual_casts / row.possible_casts) * 100 : 0,
    }))
  }, [scopedCooldownUtilizationRows])

  const raidCooldownRows = useMemo(
    () =>
      cooldownCapacityRows
        .filter(row => row.key.startsWith('raid:'))
        .sort((a, b) => b.missed_casts - a.missed_casts || b.possible_casts - a.possible_casts)
        .slice(0, 8),
    [cooldownCapacityRows]
  )

  const personalDefensiveCapacityByPlayer = useMemo(() => {
    const rows = new Map<
      string,
      {
        possible_casts: number
        actual_casts: number
        missed_casts: number
      }
    >()

    for (const row of cooldownCapacityRows) {
      if (!row.key.startsWith('personal:') && !row.key.startsWith('personal_spec:')) continue

      const key = row.player_name.toLowerCase()
      const current = rows.get(key) ?? {
        possible_casts: 0,
        actual_casts: 0,
        missed_casts: 0,
      }

      current.possible_casts += row.possible_casts
      current.actual_casts += row.actual_casts
      current.missed_casts += row.missed_casts
      rows.set(key, current)
    }

    return rows
  }, [cooldownCapacityRows])

  const externalCooldownRows = useMemo(
    () =>
      cooldownCapacityRows
        .filter(row => row.key.startsWith('external:'))
        .sort((a, b) => b.missed_casts - a.missed_casts || b.possible_casts - a.possible_casts)
        .slice(0, 8),
    [cooldownCapacityRows]
  )

  function CooldownCapacityList({
    rows,
    accent,
  }: {
    rows: CooldownCapacityRow[]
    accent: string
  }) {
    return (
      <div className="space-y-3">
        {rows.map((row, index) => {
          const visibleTotal = Math.max(row.possible_casts, row.actual_casts, 1)
          const actualPct = (row.actual_casts / visibleTotal) * 100
          const missedPct = (row.missed_casts / visibleTotal) * 100

          return (
            <div key={row.key} className="space-y-1.5">
              <div className="flex items-center justify-between gap-3">
                <div className="flex min-w-0 items-center gap-2">
                  <span className="w-5 text-right font-mono text-[10px] text-ctp-overlay0">
                    {index + 1}
                  </span>
                  <ClassDot className={row.player_class} />
                  <span className="truncate font-mono text-xs font-medium text-ctp-text">
                    {row.player_name}
                  </span>
                </div>
                <span className="shrink-0 font-mono text-[10px] text-ctp-overlay1">
                  {formatNumber(row.missed_casts)} unused
                </span>
              </div>

              <div className="pl-7">
                <div className="h-2 overflow-hidden rounded-full bg-ctp-surface1">
                  <div
                    className="flex h-full overflow-hidden rounded-full"
                    title={`${row.ability_name}: ${formatNumber(row.actual_casts)} cast / ${formatNumber(row.missed_casts)} unused`}
                  >
                    <div
                      className="h-full"
                      style={{ width: `${actualPct}%`, backgroundColor: '#a6e3a1', opacity: 0.9 }}
                    />
                    <div
                      className="h-full"
                      style={{ width: `${missedPct}%`, backgroundColor: accent, opacity: 0.82 }}
                    />
                  </div>
                </div>
                <div className="mt-1 flex items-center justify-between gap-3">
                  <span className="truncate font-mono text-[10px] text-ctp-overlay0">
                    {row.ability_name}
                  </span>
                  <span className="shrink-0 font-mono text-[10px] text-ctp-overlay0">
                    {formatNumber(row.actual_casts)} / {formatNumber(row.possible_casts)} cast
                  </span>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    )
  }

  const wipeSurvivalFailures = useMemo((): WipeSurvivalFailureRow[] => {
    const wipePullsByPlayer = new Map<string, Set<string>>()
    const classByPlayer = new Map<string, string>()
    const displayNameByPlayer = new Map<string, string>()

    for (const row of scopedWipeUtilityRows) {
      const playerKey = row.player_name.toLowerCase()
      const pullKey = `${row.report_code}:${row.fight_id}`

      if (!wipePullsByPlayer.has(playerKey)) wipePullsByPlayer.set(playerKey, new Set())
      wipePullsByPlayer.get(playerKey)!.add(pullKey)
      if (!classByPlayer.has(playerKey) && row.player_class) classByPlayer.set(playerKey, row.player_class)
      if (!displayNameByPlayer.has(playerKey) && row.player_name) displayNameByPlayer.set(playerKey, row.player_name)
    }

    const rows = new Map<string, WipeSurvivalFailureRow>()
    const killingBlowsByPlayer = new Map<string, Map<string, number>>()

    function ensurePlayer(playerName: string, playerClass: string) {
      const playerKey = playerName.toLowerCase()
      const existing = rows.get(playerKey)
      if (existing) {
        if (existing.player_class === 'Unknown' && playerClass) existing.player_class = playerClass
        return existing
      }

      const row: WipeSurvivalFailureRow = {
        player_name: displayNameByPlayer.get(playerKey) ?? playerName,
        player_class: playerClass || 'Unknown',
        wipe_pulls_tracked: 0,
        wipe_deaths: 0,
        deaths_per_wipe_pull: 0,
        no_healthstone_deaths: 0,
        no_health_potion_deaths: 0,
        defensive_possible_casts: 0,
        defensive_actual_casts: 0,
        defensive_missed_casts: 0,
        defensive_capacity_used_pct: 0,
        no_healthstone_pct: 0,
        no_health_potion_pct: 0,
        weighted_failure_points: 0,
        survival_failure_score: 0,
        survival_grade: 'S',
        top_missing_category: '—',
        most_common_killing_blow: '',
        most_common_killing_blow_count: 0,
      }
      rows.set(playerKey, row)
      return row
    }

    for (const [playerKey, pulls] of wipePullsByPlayer.entries()) {
      const playerName = displayNameByPlayer.get(playerKey) ?? playerKey
      const player = ensurePlayer(playerName, classByPlayer.get(playerKey) ?? 'Unknown')
      player.wipe_pulls_tracked = pulls.size

      const defensiveCapacity = personalDefensiveCapacityByPlayer.get(playerKey)
      if (defensiveCapacity) {
        player.defensive_possible_casts = defensiveCapacity.possible_casts
        player.defensive_actual_casts = defensiveCapacity.actual_casts
        player.defensive_missed_casts = defensiveCapacity.missed_casts
      }
    }

    for (const death of scopedWipeSurvivalEventRows) {
      const playerKey = death.player_name.toLowerCase()
      const player = ensurePlayer(death.player_name, death.player_class)

      player.wipe_deaths += 1
      if (!isPositiveFlag(death.healthstone_before_death)) player.no_healthstone_deaths += 1
      if (!isPositiveFlag(death.health_potion_before_death)) player.no_health_potion_deaths += 1

      if (death.killing_blow_name) {
        if (!killingBlowsByPlayer.has(playerKey)) {
          killingBlowsByPlayer.set(playerKey, new Map())
        }
        const blows = killingBlowsByPlayer.get(playerKey)!
        blows.set(death.killing_blow_name, (blows.get(death.killing_blow_name) ?? 0) + 1)
      }
    }

    killingBlowsByPlayer.forEach((counts, playerKey) => {
      const top = [...counts.entries()].sort((a, b) => b[1] - a[1])[0]
      const row = rows.get(playerKey)
      if (row && top) {
        row.most_common_killing_blow = top[0]
        row.most_common_killing_blow_count = top[1]
      }
    })

    const scoredRows = [...rows.values()]
      .map(row => {
        const missing = [
          { label: 'Defensive capacity', count: row.defensive_missed_casts },
          { label: 'Healthstone', count: row.no_healthstone_deaths },
          { label: 'Health potion', count: row.no_health_potion_deaths },
        ].sort((a, b) => b.count - a.count)
        const wipePulls = Math.max(row.wipe_pulls_tracked, row.wipe_deaths)
        const weightedFailurePoints =
          row.defensive_missed_casts * 0.5 +
          row.no_healthstone_deaths * 0.3 +
          row.no_health_potion_deaths * 0.2

        return {
          ...row,
          wipe_pulls_tracked: wipePulls,
          deaths_per_wipe_pull: wipePulls > 0 ? row.wipe_deaths / wipePulls : 0,
          defensive_capacity_used_pct:
            row.defensive_possible_casts > 0
              ? (row.defensive_actual_casts / row.defensive_possible_casts) * 100
              : 0,
          no_healthstone_pct: pct(row.no_healthstone_deaths, row.wipe_deaths),
          no_health_potion_pct: pct(row.no_health_potion_deaths, row.wipe_deaths),
          weighted_failure_points: weightedFailurePoints,
          // Presence-normalised score: weighted missing-tool deaths per wipe pull, scaled to 100.
          // This keeps low-pull and high-pull boss scopes comparable.
          survival_failure_score: wipePulls > 0 ? (weightedFailurePoints / wipePulls) * 100 : 0,
          top_missing_category: missing[0]?.count > 0 ? missing[0].label : '—',
        }
      })

    const sortedForGrade = [...scoredRows].sort(
      (a, b) => a.survival_failure_score - b.survival_failure_score || a.wipe_deaths - b.wipe_deaths
    )
    const distinctScores = new Set(sortedForGrade.map(row => row.survival_failure_score))
    const gradeByPlayer = new Map<string, WipeSurvivalFailureRow['survival_grade']>()

    for (const row of sortedForGrade) {
      if (distinctScores.size <= 1) {
        gradeByPlayer.set(row.player_name.toLowerCase(), 'S')
        continue
      }

      const rank = sortedForGrade.findIndex(candidate => candidate === row)
      const percentile = sortedForGrade.length <= 1 ? 0 : rank / (sortedForGrade.length - 1)
      gradeByPlayer.set(row.player_name.toLowerCase(), gradeForPercentile(percentile))
    }

    return scoredRows
      .map(row => ({ ...row, survival_grade: gradeByPlayer.get(row.player_name.toLowerCase()) ?? 'S' }))
      .sort((a, b) => b.survival_failure_score - a.survival_failure_score || b.wipe_deaths - a.wipe_deaths)
  }, [personalDefensiveCapacityByPlayer, scopedWipeUtilityRows, scopedWipeSurvivalEventRows])

  const historicalClosestPull = useMemo(
    () =>
      [...filteredWipes]
        .filter(row => Number(row.best_wipe_pct) > 0)
        .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))[0],
    [filteredWipes]
  )

  const activeClosestPull = useMemo(
    () =>
      [...unresolvedWipes]
        .filter(row => Number(row.best_wipe_pct) > 0)
        .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))[0],
    [unresolvedWipes]
  )

  const stats = useMemo(() => {
    const totalWipes = filteredWipes.reduce((sum, row) => sum + Number(row.total_wipes), 0)
    const earlyWipes = filteredMechanics.reduce((sum, row) => sum + Number(row.wipes_lt_1min), 0)

    const topBlocker = [...filteredWipes].sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes))[0]

    const avgDeathsPerKill = playersWithTrackedKills.length
      ? playersWithTrackedKills.reduce((sum, row) => sum + Number(row.deaths_per_kill ?? 0), 0) /
        playersWithTrackedKills.length
      : 0

    const lateWipes = filteredMechanics.reduce((sum, row) => sum + Number(row.wipes_5plus_min), 0)
    const lateWipeRate = totalWipes > 0 ? lateWipes / totalWipes : 0
    const earlyWipeRate = totalWipes > 0 ? earlyWipes / totalWipes : 0
    const wipeDeathsPerWipe = totalWipes > 0 ? wipeOnlyDeaths.length / totalWipes : 0

    return {
      totalWipes,
      earlyWipes,
      lateWipes,
      earlyWipeRate,
      lateWipeRate,
      avgDeathsPerKill,
      bossesInScope: filteredWipes.length,
      activeProgressBosses: unresolvedWipes.length,
      closestPull: activeClosestPull ?? historicalClosestPull,
      topBlocker,
      wipeDeathsPerWipe,
      wipeDeathCount: wipeOnlyDeaths.length,
    }
  }, [
    filteredWipes,
    filteredMechanics,
    playersWithTrackedKills,
    unresolvedWipes,
    activeClosestPull,
    historicalClosestPull,
    wipeOnlyDeaths,
  ])

  const durationBuckets = useMemo(() => {
    const totals = { lt1: 0, one3: 0, three5: 0, gt5: 0 }

    for (const row of filteredMechanics) {
      totals.lt1 += Number(row.wipes_lt_1min)
      totals.one3 += Number(row.wipes_1_3min)
      totals.three5 += Number(row.wipes_3_5min)
      totals.gt5 += Number(row.wipes_5plus_min)
    }

    return [
      { label: '<1 min', wipes: totals.lt1, fill: phaseColors[0] },
      { label: '1–3 min', wipes: totals.one3, fill: phaseColors[1] },
      { label: '3–5 min', wipes: totals.three5, fill: phaseColors[2] },
      { label: '5+ min', wipes: totals.gt5, fill: phaseColors[3] },
    ]
  }, [filteredMechanics, phaseColors])

  const dominantDurationBucket = useMemo(() => {
    if (stats.totalWipes <= 0) return null

    const bucket = [...durationBuckets].sort((a, b) => b.wipes - a.wipes)[0]
    if (!bucket || bucket.wipes <= 0) return null

    const rate = bucket.wipes / stats.totalWipes
    return rate >= 0.4 ? { ...bucket, rate } : null
  }, [durationBuckets, stats.totalWipes])

  const topWipeBosses = useMemo(
    () =>
      filteredWipes.slice(0, 10).map(row => {
        const mech = mechanicsMap.get(`${row.encounter_id}-${row.difficulty}`)
        return {
          boss: row.boss_name.length > 20 ? `${row.boss_name.slice(0, 19)}…` : row.boss_name,
          fullName: row.boss_name,
          diff: row.difficulty_label,
          wipes: Number(row.total_wipes),
          bestPct: Number(row.best_wipe_pct),
          avgPct: Number(mech?.avg_boss_pct ?? row.avg_wipe_pct),
          nights: Number(row.raid_nights_attempted),
          isCleared: killedEncounterKeys.has(`${row.encounter_id}-${row.difficulty}`),
        }
      }),
    [filteredWipes, mechanicsMap, killedEncounterKeys]
  )

  const historicalProgressRows = useMemo(
    () =>
      filteredWipes
        .map(row => {
          const mech = mechanicsMap.get(`${row.encounter_id}-${row.difficulty}`)
          return {
            ...row,
            avgBossPct: Number(mech?.avg_boss_pct ?? row.avg_wipe_pct),
            lastWeek: Number(mech?.last_week_avg_boss_pct ?? 0),
            trend: Number(mech?.progress_trend ?? 0),
            maxPhase: Number(row.max_phase_reached ?? 0),
            isCleared: killedEncounterKeys.has(`${row.encounter_id}-${row.difficulty}`),
          }
        })
        .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))
        .slice(0, 12),
    [filteredWipes, mechanicsMap, killedEncounterKeys]
  )

  const activeProgressRows = useMemo(
    () =>
      unresolvedWipes
        .map(row => {
          const mech = mechanicsMap.get(`${row.encounter_id}-${row.difficulty}`)
          return {
            ...row,
            avgBossPct: Number(mech?.avg_boss_pct ?? row.avg_wipe_pct),
            lastWeek: Number(mech?.last_week_avg_boss_pct ?? 0),
            trend: Number(mech?.progress_trend ?? 0),
            maxPhase: Number(row.max_phase_reached ?? 0),
          }
        })
        .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))
        .slice(0, 12),
    [unresolvedWipes, mechanicsMap]
  )

  const currentProgressTarget = activeProgressRows[0] ?? null
  const currentProgressTargetKey = currentProgressTarget
    ? `${currentProgressTarget.encounter_id}-${currentProgressTarget.difficulty}`
    : null

  const progressCandles = useMemo(() => {
    if (!currentProgressTarget || !currentProgressTargetKey) return []

    const matchingPulls = pullHistory.data.filter(row => {
      const key = `${row.encounter_id}-${row.difficulty}`
      if (key !== currentProgressTargetKey) return false
      if (!isIncludedZoneName(row.zone_name)) return false
      if (selectedTier !== 'All' && row.zone_name !== selectedTier) return false
      return true
    })
    const ordered = [...matchingPulls].sort((a, b) => {
      const date = String(a.raid_night_date).localeCompare(String(b.raid_night_date))
      if (date !== 0) return date
      const start = String(a.start_time_utc ?? '').localeCompare(String(b.start_time_utc ?? ''))
      if (start !== 0) return start
      return Number(a.fight_id) - Number(b.fight_id)
    })
    const byNight = new Map<string, typeof pullHistory.data>()

    for (const pull of ordered) {
      const night = String(pull.raid_night_date)
      if (!night) continue
      const rows = byNight.get(night) ?? []
      rows.push(pull)
      byNight.set(night, rows)
    }

    const candles: ProgressSnapshotDatum[] = []
    let previousClosePull: (typeof pullHistory.data)[number] | null = null

    for (const [currentNight, currentPulls] of byNight.entries()) {
      const openPull = previousClosePull ?? currentPulls[0]
      const closePull = currentPulls[currentPulls.length - 1]
      if (!openPull || !closePull) continue

      const currentValues = currentPulls
        .map(row => clampPct(Number(row.boss_hp_remaining ?? row.boss_percentage)))
        .filter(Number.isFinite)
      const open = clampPct(Number(openPull.boss_hp_remaining ?? openPull.boss_percentage))
      const close = clampPct(Number(closePull.boss_hp_remaining ?? closePull.boss_percentage))
      const rangeValues = [...currentValues, open]

      candles.push({
        boss_name: closePull.boss_name,
        difficulty_label: closePull.difficulty_label,
        label: shortDateLabel(currentNight),
        subLabel: `${currentPulls.length} pull${currentPulls.length === 1 ? '' : 's'}`,
        open,
        close,
        high: Math.max(...rangeValues),
        low: Math.min(...rangeValues),
        currentNight,
        previousNight: previousClosePull ? String(previousClosePull.raid_night_date) : null,
        pullCount: currentPulls.length,
      })

      previousClosePull = closePull
    }

    return candles
  }, [pullHistory.data, currentProgressTarget, currentProgressTargetKey, selectedTier])

  const progressImprovedColor = '#a6e3a1'
  const progressWorseColor = '#f38ba8'
  const progressNeutralColor = '#9399b2'

  const killingBlows = useMemo(() => {
    const counts = new Map<string, number>()
    for (const row of scopedDeathRows) {
      if (!row.killing_blow_name) continue
      counts.set(row.killing_blow_name, (counts.get(row.killing_blow_name) ?? 0) + 1)
    }

    return [...counts.entries()]
      .filter(([name]) => name && name !== 'null')
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([name, count]) => ({
        name: name.length > 24 ? `${name.slice(0, 23)}…` : name,
        fullName: name,
        count,
      }))
  }, [scopedDeathRows])

  const phaseBreakdown = useMemo(() => {
    const counts = new Map<number, number>()
    for (const row of filteredWipes) {
      const phase = Number(row.max_phase_reached) || 0
      if (phase <= 0) continue
      counts.set(phase, (counts.get(phase) ?? 0) + Number(row.total_wipes))
    }

    return [...counts.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([phase, wipes], i) => ({
        label: `Phase ${phase}`,
        phase,
        wipes,
        fill: phaseColors[i % phaseColors.length],
      }))
  }, [filteredWipes, phaseColors])

  const recurringKillers = useMemo(() => {
    const killerStats = new Map<
      string,
      { deaths: number; players: Set<string>; bosses: Set<string> }
    >()

    for (const row of scopedDeathRows) {
      if (!row.killing_blow_name || row.killing_blow_name === 'null') continue
      if (isKillRow(row)) continue

      if (!killerStats.has(row.killing_blow_name)) {
        killerStats.set(row.killing_blow_name, {
          deaths: 0,
          players: new Set(),
          bosses: new Set(),
        })
      }

      const stat = killerStats.get(row.killing_blow_name)!
      stat.deaths += 1
      stat.players.add(row.player_name)
      stat.bosses.add(row.boss_name)
    }

    return [...killerStats.entries()]
      .map(([name, stat]) => ({
        name: name.length > 26 ? `${name.slice(0, 25)}…` : name,
        fullName: name,
        deaths: stat.deaths,
        uniquePlayers: stat.players.size,
        uniqueBosses: stat.bosses.size,
      }))
      .sort((a, b) => b.uniquePlayers - a.uniquePlayers || b.deaths - a.deaths)
      .slice(0, 10)
  }, [scopedDeathRows])

  const deathTimingSummary = useMemo<DeathTimingSummary | null>(() => {
    const values = wipeOnlyDeaths
      .map(row => Number(row.death_timestamp_ms) - Number(row.fight_start_ms))
      .filter(v => Number.isFinite(v) && v >= 0 && v < 3_600_000)
      .sort((a, b) => a - b)

    if (values.length < 5) return null

    const min = values[0]
    const max = values[values.length - 1]
    const q1 = quantile(values, 0.25)
    const median = quantile(values, 0.5)
    const q3 = quantile(values, 0.75)
    const iqr = q3 - q1

    const lowFence = q1 - 1.5 * iqr
    const highFence = q3 + 1.5 * iqr

    const lowerWhisker = values.find(v => v >= lowFence) ?? min
    const upperWhisker = [...values].reverse().find(v => v <= highFence) ?? max
    const outliers = values.filter(v => v < lowerWhisker || v > upperWhisker)

    return {
      count: values.length,
      min,
      q1,
      median,
      q3,
      max,
      lowerWhisker,
      upperWhisker,
      outliers,
    }
  }, [wipeOnlyDeaths])

  const firstDeathLeaders = useMemo(() => {
    const fightMap = new Map<string, { player_name: string; ts: number; player_class: string }[]>()

    for (const row of wipeOnlyDeaths) {
      const ts = Number(row.death_timestamp_ms)
      if (!ts || ts <= 0) continue
      const key = `${row.report_code}:${row.fight_id}:${row.boss_name}`
      if (!fightMap.has(key)) fightMap.set(key, [])
      fightMap.get(key)!.push({
        player_name: row.player_name,
        ts,
        player_class: row.player_class,
      })
    }

    const counts = new Map<string, { count: number; player_class: string }>()

    for (const deaths of fightMap.values()) {
      const first = deaths.reduce((a, b) => (a.ts <= b.ts ? a : b))
      const existing = counts.get(first.player_name)
      if (existing) existing.count += 1
      else counts.set(first.player_name, { count: 1, player_class: first.player_class })
    }

    return [...counts.entries()]
      .map(([player_name, data]) => ({
        player_name,
        player_class: data.player_class,
        count: data.count,
      }))
      .sort((a, b) => b.count - a.count)
  }, [wipeOnlyDeaths])

  const firstDeathProfile = useMemo(() => {
    const roleByPlayer = new Map<string, string | undefined>()
    for (const row of roster.data) {
      if (!roleByPlayer.has(row.player_name)) roleByPlayer.set(row.player_name, row.role)
    }

    const fightFirstDeath = new Map<
      string,
      {
        player_name: string
        player_class: string
        ts: number
        fight_start_ms: number
        killing_blow_name: string
      }
    >()

    for (const row of wipeOnlyDeaths) {
      const ts = Number(row.death_timestamp_ms)
      if (!ts || ts <= 0) continue
      const key = `${row.report_code}:${row.fight_id}:${row.boss_name}`
      const existing = fightFirstDeath.get(key)

      if (!existing || ts < existing.ts) {
        fightFirstDeath.set(key, {
          player_name: row.player_name,
          player_class: row.player_class,
          ts,
          fight_start_ms: Number(row.fight_start_ms),
          killing_blow_name: row.killing_blow_name,
        })
      }
    }

    const roles: Record<'Tank' | 'Healer' | 'DPS' | 'Unknown', number> = {
      Tank: 0,
      Healer: 0,
      DPS: 0,
      Unknown: 0,
    }

    const timing = { lt30s: 0, s30_60: 0, m1_2: 0, gt2m: 0 }
    let timingAvailable = false
    const blowCounts = new Map<string, number>()

    for (const d of fightFirstDeath.values()) {
      const rosterRole = normalizeRole(roleByPlayer.get(d.player_name))
      const role =
        rosterRole !== 'Unknown'
          ? rosterRole
          : (CLASS_ROLE_FALLBACK[d.player_class] ?? 'Unknown')

      roles[role] += 1

      if (d.fight_start_ms > 0) {
        const elapsed = d.ts - d.fight_start_ms
        if (elapsed >= 0) {
          timingAvailable = true
          if (elapsed < 30_000) timing.lt30s += 1
          else if (elapsed < 60_000) timing.s30_60 += 1
          else if (elapsed < 120_000) timing.m1_2 += 1
          else timing.gt2m += 1
        }
      }

      if (d.killing_blow_name) {
        blowCounts.set(d.killing_blow_name, (blowCounts.get(d.killing_blow_name) ?? 0) + 1)
      }
    }

    if (import.meta.env.DEV && roles.Unknown > 0) {
      const sampleRaw = [...fightFirstDeath.values()]
        .filter(
          d =>
            normalizeRole(roleByPlayer.get(d.player_name)) === 'Unknown' &&
            !CLASS_ROLE_FALLBACK[d.player_class]
        )
        .slice(0, 5)
        .map(
          d =>
            `${d.player_name}(${d.player_class}) raw="${roleByPlayer.get(d.player_name) ?? 'missing'}"`
        )

      console.warn('[FirstDeathProfile] Unknown roles:', roles.Unknown, sampleRaw)
    }

    const topBlows = [...blowCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 4)
      .map(([name, count]) => ({ name, count }))

    return {
      total: fightFirstDeath.size,
      roles,
      timing: timingAvailable ? timing : null,
      topBlows,
    }
  }, [wipeOnlyDeaths, roster.data])

  const bossSpotlight = useMemo(() => {
    const row = activeProgressRows[0]
    if (!row) return null

    const trendDirection =
      Number(row.trend) < 0 ? 'improving' : Number(row.trend) > 0 ? 'regressing' : 'flat'

    return {
      ...row,
      trendDirection,
    }
  }, [activeProgressRows])

  const wipeCauseMatrix = useMemo(() => {
    return recurringKillers.slice(0, 5).map(killer => {
      const matching = scopedDeathRows.filter(
        row => !isKillRow(row) && row.killing_blow_name === killer.fullName
      )
      const byBoss = new Map<string, number>()
      for (const row of matching) {
        byBoss.set(row.boss_name, (byBoss.get(row.boss_name) ?? 0) + 1)
      }
      const topBoss = [...byBoss.entries()].sort((a, b) => b[1] - a[1])[0]
      return {
        ...killer,
        topBoss: topBoss?.[0] ?? '—',
        topBossDeaths: topBoss?.[1] ?? 0,
      }
    })
  }, [recurringKillers, scopedDeathRows])

  const mostPunishedPlayers = useMemo(
    () =>
      [...scopedSurvivability]
        .filter(row => row.wipe_deaths > 0)
        .sort((a, b) => b.wipe_deaths - a.wipe_deaths)
        .slice(0, 10),
    [scopedSurvivability]
  )

  const wipePressureSummary = useMemo(() => {
    const uniquePlayers = new Set(wipeOnlyDeaths.map(r => r.player_name)).size
    const distinctCauses = new Set(
      wipeOnlyDeaths.filter(r => r.killing_blow_name).map(r => r.killing_blow_name)
    ).size

    return { uniquePlayers, distinctCauses }
  }, [wipeOnlyDeaths])

  const overviewSignals = useMemo(() => {
    const topKiller = recurringKillers[0]
    const firstDeath = firstDeathLeaders[0]

    return [
      {
        label: 'Wipe concentration',
        value: stats.topBlocker
          ? `${Math.round((Number(stats.topBlocker.total_wipes) / Math.max(stats.totalWipes, 1)) * 100)}% on ${stats.topBlocker.boss_name}`
          : 'No dominant blocker',
        detail: stats.topBlocker
          ? `${formatNumber(stats.topBlocker.total_wipes)} wipes across ${stats.topBlocker.raid_nights_attempted} nights`
          : 'No wipe history in scope',
        accentClass: 'text-ctp-text',
      },
      {
        label: 'Active progression',
        value:
          stats.activeProgressBosses > 0
            ? `${formatNumber(stats.activeProgressBosses)} unresolved boss${stats.activeProgressBosses !== 1 ? 'es' : ''}`
            : 'All cleared',
        detail:
          stats.activeProgressBosses > 0
            ? 'Spotlight and progress panels only use unresolved bosses'
            : 'Progression panels are intentionally suppressed in this scope',
        accentClass: stats.activeProgressBosses > 0 ? 'text-ctp-mauve' : 'text-ctp-overlay1',
      },
      {
        label: 'Top recurring killer',
        value: topKiller ? topKiller.fullName : 'No wipe killer signal',
        detail: topKiller
          ? `${topKiller.uniquePlayers} players hit · ${topKiller.deaths} wipe deaths`
          : 'No wipe-only death events in scope',
        accentClass: topKiller ? 'text-ctp-pink' : 'text-ctp-overlay1',
      },
      {
        label: 'First-death pattern',
        value: firstDeath ? firstDeath.player_name : 'No timing signal',
        detail: firstDeath
          ? `${firstDeath.count} wipe pulls opened by this death`
          : 'Not enough timestamped wipe deaths to rank first deaths',
        accentClass: firstDeath ? 'text-ctp-text' : 'text-ctp-overlay1',
      },
      {
        label: 'Late wipe pressure',
        value: stats.totalWipes ? `${(stats.lateWipeRate * 100).toFixed(0)}% 5m+` : '—',
        detail: stats.totalWipes
          ? `${formatNumber(stats.lateWipes)} long wipes in the current scope`
          : 'No wipe history in scope',
        accentClass: stats.lateWipeRate >= 0.3 ? 'text-ctp-peach' : 'text-ctp-text',
      },
      {
        label: 'Wipe death load',
        value: stats.totalWipes ? `${stats.wipeDeathsPerWipe.toFixed(1)} deaths/wipe` : '—',
        detail: stats.totalWipes
          ? `${formatNumber(stats.wipeDeathCount)} wipe-only deaths across ${formatNumber(stats.totalWipes)} wipes`
          : 'No wipe history in scope',
        accentClass: 'text-ctp-text',
      },
    ]
  }, [stats, recurringKillers, firstDeathLeaders])

  const playerRows = useMemo(() => {
    return [...scopedSurvivability]
      .sort((a, b) => {
        const valueFor = (row: ScopedSurvivabilityRow) => {
          switch (survivabilitySort.key) {
            case 'deathsPerKill':
              return row.deaths_per_kill ?? Number.NEGATIVE_INFINITY
            case 'deathsPerPull':
              return row.deaths_per_pull ?? Number.NEGATIVE_INFINITY
            case 'deaths':
              return row.total_deaths
            case 'wipeDeath':
              return row.kill_deaths
            case 'killDeath':
              return row.wipe_deaths
            case 'kills':
              return row.kills_tracked
            case 'pulls':
              return row.pulls_tracked
          }
        }

        const av = valueFor(a)
        const bv = valueFor(b)

        const comparison = survivabilitySort.direction === 'asc' ? av - bv : bv - av

        return comparison || a.player_name.localeCompare(b.player_name)
      })
      .slice(0, 10)
  }, [scopedSurvivability, survivabilitySort])

  const wipeSurvivalRows = useMemo(() => {
    const valueFor = (row: WipeSurvivalFailureRow) => {
      switch (wipeSurvivalSort.key) {
        case 'survivalFailureScore':
          return row.survival_failure_score
        case 'wipeDeaths':
          return row.wipe_deaths
        case 'defensiveCapacityUsedPct':
          return row.defensive_capacity_used_pct
        case 'noHealthstonePct':
          return row.no_healthstone_pct
        case 'noHealthPotionPct':
          return row.no_health_potion_pct
      }
    }

    return [...wipeSurvivalFailures]
      .sort((a, b) => {
        const av = valueFor(a)
        const bv = valueFor(b)
        const comparison = wipeSurvivalSort.direction === 'asc' ? av - bv : bv - av
        return comparison || b.wipe_deaths - a.wipe_deaths || a.player_name.localeCompare(b.player_name)
      })
      .slice(0, 12)
  }, [wipeSurvivalFailures, wipeSurvivalSort])

  function sortSurvivabilityBy(key: SurvivabilitySortKey) {
    setSurvivabilitySort(current =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: getDefaultSortDirection(key) }
    )
  }

  function sortWipeSurvivalBy(key: WipeSurvivalSortKey) {
    setWipeSurvivalSort(current =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: 'desc' }
    )
  }

  function WipeSurvivalSortButton({
    k,
    children,
  }: {
    k: WipeSurvivalSortKey
    children: ReactNode
  }) {
    return (
      <button
        type="button"
        onClick={() => sortWipeSurvivalBy(k)}
        className="transition-colors hover:text-ctp-mauve"
      >
        {children}
        {wipeSurvivalSort.key === k
          ? wipeSurvivalSort.direction === 'asc'
            ? ' ↑'
            : ' ↓'
          : ''}
      </button>
    )
  }

  const hasBossData = filteredWipes.length > 0
  const totalPhaseBreakdownWipes = sectionTotal(phaseBreakdown, 'wipes')

  return (
    <AppLayout title="Wipe Analysis" subtitle="where progression stalls and what tends to kill raids">
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {loading ? (
          Array(4)
            .fill(null)
            .map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4">
            <ErrorState message={error} />
          </div>
        ) : (
          <>
            <StatCard
              label="Total Wipes"
              value={formatNumber(stats.totalWipes)}
              subValue={selectedTier === 'All' ? 'all tiers in scope' : selectedTier}
              icon="✗"
              valueColor={wipeColor}
              accent="none"
            />
            <StatCard
              label="Top Blocker"
              value={stats.topBlocker?.boss_name ?? '—'}
              subValue={
                stats.topBlocker
                  ? `${formatNumber(stats.topBlocker.total_wipes)} wipes · ${stats.topBlocker.raid_nights_attempted} nights`
                  : 'no wipe data'
              }
              icon="◈"
              accent="mauve"
            />
            <StatCard
              label="Closest Pull"
              value={stats.closestPull ? formatPct(stats.closestPull.best_wipe_pct) : '—'}
              subValue={
                stats.closestPull
                  ? `${stats.closestPull.boss_name} boss HP remaining`
                  : 'no wipes in scope'
              }
              valueColor={
                stats.closestPull
                  ? getParseColor(100 - Number(stats.closestPull.best_wipe_pct))
                  : undefined
              }
              accent="none"
            />
            <StatCard
              label="Early Wipe Rate"
              value={stats.totalWipes ? `${(stats.earlyWipeRate * 100).toFixed(0)}%` : '—'}
              subValue={
                stats.totalWipes
                  ? `${formatNumber(stats.earlyWipes)} of ${formatNumber(stats.totalWipes)} wipes under 1 min`
                  : 'no wipe data'
              }
              valueColor={wipeColor}
              accent="none"
            />
          </>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <FilterTabs options={[...DIFFS]} value={diff} onChange={setDiff} />
        <FilterSelect
          value={selectedTier}
          onChange={setSelectedTier}
          options={tierOptions}
          className="min-w-48"
        />
        <FilterSelect
          value={selectedBoss}
          onChange={setSelectedBoss}
          options={bossOptions}
          className="min-w-52"
        />
        <input
          type="text"
          placeholder="Filter boss name…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="w-48 rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-1.5 font-mono text-xs text-ctp-subtext1 placeholder-ctp-overlay0 transition-colors focus:border-ctp-mauve/40 focus:outline-none"
        />
      </div>

      {!loading && !error && !hasBossData ? (
        <Card>
          <CardBody>
            <p className="py-10 text-center font-mono text-xs text-ctp-overlay0">
              No wipe-analysis rows match the current filters.
            </p>
          </CardBody>
        </Card>
      ) : null}

      {hasBossData && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
            <Card className="h-full xl:col-span-2">
              <CardHeader>
                <CardTitle>Signal Board</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Compact reads from the current scope.
                </p>
              </CardHeader>
              <CardBody className="h-full">
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  {overviewSignals.map(signal => (
                    <SignalTile
                      key={signal.label}
                      label={signal.label}
                      value={signal.value}
                      detail={signal.detail}
                      accentClass={signal.accentClass}
                    />
                  ))}
                </div>
              </CardBody>
            </Card>

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Boss Spotlight</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  The unresolved boss currently closest to becoming a kill.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col space-y-4">
                {!bossSpotlight ? (
                  <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/30 p-4">
                    <p className="text-sm font-semibold text-ctp-text">No active progression target</p>
                    <p className="mt-2 text-xs text-ctp-overlay0">
                      All bosses in this scope already have recorded kills, so the spotlight is intentionally blank.
                    </p>
                  </div>
                ) : (
                  <>
                    <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/35 p-3.5">
                      <div className="mb-2 flex items-center justify-between gap-3">
                        <p className="section-label">Target</p>
                        <StatusPill label="Active" active />
                      </div>
                      <p className="text-base font-semibold text-ctp-text">{bossSpotlight.boss_name}</p>
                      <p className="mt-1 text-xs text-ctp-overlay0">
                        {bossSpotlight.difficulty_label} · {formatNumber(bossSpotlight.total_wipes)} wipes
                      </p>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/35 p-3">
                        <p className="section-label mb-1">Best Pull</p>
                        <p
                          className="text-base font-semibold"
                          style={{ color: getParseColor(100 - Number(bossSpotlight.best_wipe_pct)) }}
                        >
                          {formatPct(bossSpotlight.best_wipe_pct)}
                        </p>
                        <MiniNote>boss HP remaining</MiniNote>
                      </div>

                      <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/35 p-3">
                        <p className="section-label mb-1">Trend</p>
                        <p
                          className="text-base font-semibold"
                          style={{
                            color:
                              bossSpotlight.trendDirection === 'improving'
                                ? topTierColor
                                : bossSpotlight.trendDirection === 'regressing'
                                  ? wipeColor
                                  : chartColors.secondary,
                          }}
                        >
                          {bossSpotlight.trendDirection}
                        </p>
                        <MiniNote>
                          {Number(bossSpotlight.trend) > 0 ? '+' : ''}
                          {Number(bossSpotlight.trend).toFixed(1)}% vs last week
                        </MiniNote>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/35 p-3.5">
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <p className="section-label">Kill proximity</p>
                        <p
                          className="text-sm font-semibold"
                          style={{ color: getParseColor(100 - Number(bossSpotlight.best_wipe_pct)) }}
                        >
                          {Math.max(0, 100 - Number(bossSpotlight.best_wipe_pct)).toFixed(0)}%
                        </p>
                      </div>
                      <div className="h-2 overflow-hidden rounded-full bg-ctp-surface0">
                        <div
                          className="h-full rounded-full"
                          style={{
                            width: `${Math.max(4, 100 - Number(bossSpotlight.best_wipe_pct))}%`,
                            backgroundColor: getParseColor(100 - Number(bossSpotlight.best_wipe_pct)),
                          }}
                        />
                      </div>
                      <p className="mt-3 text-xs text-ctp-subtext1">
                        {Number(bossSpotlight.best_wipe_pct) <= 10
                          ? 'This boss is close. The biggest gains are likely consistency and fewer preventable deaths.'
                          : Number(bossSpotlight.best_wipe_pct) <= 30
                            ? 'This is a real progression target now. Study later-phase failures and repeat killer mechanics.'
                            : 'This boss is still some distance away. Early pull stability and cleaner openings should move the needle most.'}
                      </p>
                    </div>
                  </>
                )}
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
            <Card className="h-full xl:col-span-2">
              <CardHeader>
                <CardTitle>Top Wipe Walls</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Historical wipe walls stay visible even after a boss is cleared, but cleared bosses are visually softened.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {wipes.loading ? (
                  <LoadingState rows={4} />
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={280}>
                      <BarChart data={topWipeBosses} margin={{ top: 4, right: 8, left: -20, bottom: 48 }}>
                        <XAxis
                          dataKey="boss"
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                            dx: 24,
                          }}
                          axisLine={false}
                          tickLine={false}
                          tickMargin={6}
                          angle={-25}
                          textAnchor="end"
                          interval={0}
                        />
                        <YAxis
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <Tooltip content={<WipeWallTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                        <Bar dataKey="wipes" name="Wipes" radius={[4, 4, 0, 0]}>
                          {topWipeBosses.map((row, index) => (
                            <Cell
                              key={index}
                              fill={row.bestPct > 0 ? getParseColor(100 - row.bestPct) : chartColors.secondary}
                              fillOpacity={row.isCleared ? 0.5 : 0.9}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>

                    <div className="mt-3 flex flex-wrap gap-2">
                      <StatusPill label={`${formatNumber(stats.activeProgressBosses)} active`} active />
                      <StatusPill
                        label={`${formatNumber(topWipeBosses.filter(row => row.isCleared).length)} cleared shown`}
                      />
                    </div>
                  </>
                )}
              </CardBody>
            </Card>

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Wipe Phase Breakdown</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Wipes grouped by the furthest phase reached for each boss in scope.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {wipes.loading ? (
                  <LoadingState rows={4} />
                ) : phaseBreakdown.length === 0 ? (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No phase data available for this scope.
                  </p>
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={200}>
                      <BarChart data={phaseBreakdown} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                        <XAxis
                          dataKey="label"
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                        <Bar dataKey="wipes" name="Wipes" radius={[4, 4, 0, 0]}>
                          {phaseBreakdown.map((row, i) => (
                            <Cell key={i} fill={row.fill} fillOpacity={0.88} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>

                    <div className="mt-3 space-y-1.5">
                      {phaseBreakdown.map(row => (
                        <div key={row.phase} className="flex items-center gap-2 text-[10px] font-mono">
                          <span className="h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: row.fill }} />
                          <span className="w-16 font-semibold text-ctp-subtext1">{row.label}</span>
                          <span className="text-ctp-overlay1">{formatNumber(row.wipes)} wipes</span>
                          {totalPhaseBreakdownWipes > 0 ? (
                            <span className="text-ctp-overlay0">
                              ({((row.wipes / totalPhaseBreakdownWipes) * 100).toFixed(0)}%)
                            </span>
                          ) : null}
                        </div>
                      ))}
                    </div>

                    <div className="mt-auto pt-3">
                      <MiniNote>
                        This uses max phase reached from available boss-level data, so it is directional rather than a perfect per-pull phase distribution.
                      </MiniNote>
                    </div>
                  </>
                )}
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            <Card className="h-full">
              <CardHeader>
                <CardTitle>Wipe Duration Breakdown</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Early wipes suggest opener or positioning issues; late wipes suggest deeper execution problems.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {mechs.loading ? (
                  <LoadingState rows={4} />
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={200}>
                      <BarChart data={durationBuckets} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                        <XAxis
                          dataKey="label"
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                        <Bar dataKey="wipes" name="Wipes" radius={[4, 4, 0, 0]}>
                          {durationBuckets.map((bucket, index) => (
                            <Cell key={index} fill={bucket.fill} fillOpacity={0.88} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>

                    <div className="mt-3 grid grid-cols-2 gap-2">
                      {durationBuckets.map(bucket => (
                        <div
                          key={bucket.label}
                          className="flex items-start gap-2 text-[10px] font-mono text-ctp-overlay1"
                        >
                          <span
                            className="mt-0.5 h-2 w-2 shrink-0 rounded-full"
                            style={{ backgroundColor: bucket.fill }}
                          />
                          <span>
                            <span className="font-semibold text-ctp-subtext1">{bucket.label}</span> ·{' '}
                            {formatNumber(bucket.wipes)} wipes
                            {stats.totalWipes > 0
                              ? ` (${((bucket.wipes / stats.totalWipes) * 100).toFixed(0)}%)`
                              : ''}
                          </span>
                        </div>
                      ))}
                    </div>

                    <div className="mt-0 pt-3">
                      <MiniNote>
                        {dominantDurationBucket
                          ? dominantDurationBucket.label === '<1 min'
                            ? `${(dominantDurationBucket.rate * 100).toFixed(0)}% of wipes end before 1 minute — opener and positioning problems are a meaningful theme.`
                            : dominantDurationBucket.label === '1–3 min'
                              ? `${(dominantDurationBucket.rate * 100).toFixed(0)}% of wipes land in the 1–3 minute window — early mechanics and first transition consistency are likely the pressure point.`
                              : dominantDurationBucket.label === '3–5 min'
                                ? `${(dominantDurationBucket.rate * 100).toFixed(0)}% of wipes land in the 3–5 minute window — mid-fight execution is the main concentration.`
                                : `${(dominantDurationBucket.rate * 100).toFixed(0)}% of wipes last beyond 5 minutes — the bigger problem is likely later-phase consistency.`
                          : 'Wipes are spread across the pull rather than concentrated in a single timing window.'}
                      </MiniNote>
                    </div>
                  </>
                )}
              </CardBody>
            </Card>

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Progress Snapshot</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  {currentProgressTarget
                    ? `${currentProgressTarget.boss_name} ${currentProgressTarget.difficulty_label} across all available raid nights. Lower boss HP is better.`
                    : 'Lower boss HP is better. Candles show progression across raid nights.'}
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {wipes.loading || pullHistory.loading ? (
                  <LoadingState rows={4} />
                ) : progressCandles.length === 0 ? (
                  <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/30 p-4">
                    <p className="text-sm font-semibold text-ctp-text">Nothing left to progress here</p>
                    <p className="mt-2 text-xs text-ctp-overlay0">
                      No unresolved bosses in this filter scope have enough pull history for a raid-night candle.
                    </p>
                  </div>
                ) : (
                  <>
                    <div className="mb-3 flex flex-wrap items-center justify-end gap-4 text-[11px] font-mono">
                      <span
                        className="inline-flex items-center gap-1.5"
                        style={{ color: progressNeutralColor }}
                      >
                        <span
                          className="h-px w-4"
                          style={{ backgroundColor: progressNeutralColor }}
                        />
                        Open
                      </span>
                      <span className="inline-flex items-center gap-1.5" style={{ color: progressImprovedColor }}>
                        <span
                          className="h-2.5 w-3 rounded-[3px] border"
                          style={{ borderColor: progressImprovedColor, backgroundColor: 'transparent' }}
                        />
                        Improved Close
                      </span>
                      <span className="inline-flex items-center gap-1.5" style={{ color: progressWorseColor }}>
                        <span
                          className="h-2.5 w-3 rounded-[3px]"
                          style={{ backgroundColor: progressWorseColor }}
                        />
                        Regressed Close
                      </span>
                    </div>

                    <ProgressSnapshotCandles
                      key={
                        currentProgressTarget
                          ? `${currentProgressTarget.zone_name}-${currentProgressTarget.encounter_id}-${currentProgressTarget.difficulty}`
                          : 'no-progress-target'
                      }
                      data={progressCandles}
                      improvedColor={progressImprovedColor}
                      worseColor={progressWorseColor}
                      neutralColor={progressNeutralColor}
                    />
                  </>
                )}
              </CardBody>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Boss Progress Table</CardTitle>
            </CardHeader>
            {wipes.loading ? (
              <CardBody>
                <LoadingState rows={8} />
              </CardBody>
            ) : (
              <CardBody>
                <div className="table-row-hover">
                  <table className="w-full table-fixed border-collapse text-xs">
                    <colgroup>
                      <col className="w-[25%]" />
                      <col className="w-[7%]" />
                      <col className="w-[7%]" />
                      <col className="w-[6%]" />
                      <col className="w-[7%]" />
                      <col className="w-[7%]" />
                      <col className="w-[6%]" />
                      <col className="w-[9%]" />
                      <col className="w-[7%]" />
                      <col className="w-[6%]" />
                      <col className="w-[13%]" />
                    </colgroup>
                  <thead className="border-b border-ctp-surface1">
                    <tr>
                      <th className="px-1.5 py-2 text-left font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Boss</th>
                      <th className="px-1.5 py-2 text-left font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Diff</th>
                      <th className="px-1.5 py-2 text-left font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Status</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Wipes</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Best</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Avg</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Phase</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Dur</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Trend</th>
                      <th className="px-1.5 py-2 text-right font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Nights</th>
                      <th className="px-1.5 py-2 text-left font-mono text-[9px] font-semibold uppercase tracking-[0.08em] text-ctp-overlay0">Last</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-ctp-surface0">
                    {historicalProgressRows.map(row => {
                      const bestProgressPct = row.isCleared ? 0 : Number(row.best_wipe_pct)

                      return (
                      <tr key={`${row.encounter_id}-${row.difficulty}`} className="transition-colors duration-100">
                        <td className="min-w-0 px-1.5 py-2.5">
                          <div className="truncate font-medium text-ctp-text">{row.boss_name}</div>
                          <div className="truncate text-[10px] text-ctp-overlay0">{row.zone_name}</div>
                        </td>
                        <td className="overflow-hidden px-1.5 py-2.5">
                          <DiffBadge label={row.difficulty_label} />
                        </td>
                        <td className="overflow-hidden px-1.5 py-2.5">
                          <StatusPill
                            label={row.isCleared ? 'Done' : 'Active'}
                            active={!row.isCleared}
                            compact
                          />
                        </td>
                        <td className="px-1.5 py-2.5 text-right font-mono" style={{ color: wipeColor }}>
                          {formatNumber(row.total_wipes)}
                        </td>
                        <td
                          className="px-1.5 py-2.5 text-right font-mono"
                          style={{ color: getParseColor(100 - bestProgressPct) }}
                        >
                          {formatPct(bestProgressPct)}
                        </td>
                        <td className="px-1.5 py-2.5 text-right font-mono text-ctp-overlay1">
                          {formatPct(row.avgBossPct)}
                        </td>
                        <td className="px-1.5 py-2.5 text-right font-mono text-ctp-overlay1">
                          {row.maxPhase || '—'}
                        </td>
                        <td className="px-1.5 py-2.5 text-right font-mono text-ctp-overlay1">
                          {formatDuration(Number(row.avg_wipe_duration_seconds))}
                        </td>
                        <td
                          className="px-1.5 py-2.5 text-right font-mono"
                          style={{ color: Number(row.trend) < 0 ? topTierColor : wipeColor }}
                        >
                          {Number(row.trend) > 0 ? '+' : ''}
                          {Number(row.trend).toFixed(1)}%
                        </td>
                        <td className="px-1.5 py-2.5 text-right font-mono text-ctp-overlay1">
                          {formatNumber(row.raid_nights_attempted)}
                        </td>
                        <td className="truncate px-1.5 py-2.5 text-xs text-ctp-overlay0">{formatDate(row.latest_wipe_date)}</td>
                      </tr>
                      )
                    })}
                  </tbody>
                </table>
                </div>
              </CardBody>
            )}
          </Card>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            <Card className="h-full">
              <CardHeader>
                <CardTitle>Recurring Killers</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Abilities ranked by how many different players they kill on wipe pulls. Breadth is often a better signal than raw count.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {deathEvents.loading ? (
                  <LoadingState rows={5} />
                ) : recurringKillers.length > 0 ? (
                  <ResponsiveContainer width="100%" height={240}>
                    <BarChart data={recurringKillers} layout="vertical" margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                      <XAxis
                        type="number"
                        tick={{
                          fontSize: 10,
                          fill: '#6c7086',
                          fontFamily: 'IBM Plex Mono, monospace',
                        }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        dataKey="name"
                        type="category"
                        width={160}
                        tick={{
                          fontSize: 10,
                          fill: '#a6adc8',
                          fontFamily: 'IBM Plex Mono, monospace',
                        }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<RecurringKillerTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                      <Bar
                        dataKey="uniquePlayers"
                        name="Unique Players Killed"
                        radius={[0, 4, 4, 0]}
                        fill={wipeColor}
                        fillOpacity={0.85}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No wipe-only death events in the current scope.
                  </p>
                )}
              </CardBody>
            </Card>

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Most Common Killing Blows</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Raw death count by ability across all pulls. This is wider than recurring killers and includes kill pulls.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {deathEvents.loading ? (
                  <LoadingState rows={5} />
                ) : killingBlows.length > 0 ? (
                  <ResponsiveContainer width="100%" height={240}>
                    <BarChart data={killingBlows} layout="vertical" margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                      <XAxis
                        type="number"
                        tick={{
                          fontSize: 10,
                          fill: '#6c7086',
                          fontFamily: 'IBM Plex Mono, monospace',
                        }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        dataKey="name"
                        type="category"
                        width={150}
                        tick={{
                          fontSize: 10,
                          fill: '#a6adc8',
                          fontFamily: 'IBM Plex Mono, monospace',
                        }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                      <Bar
                        dataKey="count"
                        name="Deaths"
                        radius={[0, 4, 4, 0]}
                        fill={chartColors.secondary}
                        fillOpacity={0.78}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No death events in the current scope.
                  </p>
                )}
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            {deathTimingSummary ? (
              <Card className="h-full">
                <CardHeader>
                  <CardTitle>Death Timing</CardTitle>
                  <p className="mt-0.5 text-xs text-ctp-overlay1">
                    Distribution of wipe-death timestamps. The box shows the interquartile range, the center line is the median, and dots are outliers.
                  </p>
                </CardHeader>
                <CardBody className="flex flex-col pt-1">
                  {deathEvents.loading ? (
                    <LoadingState rows={4} />
                  ) : (
                    <>
                      <DeathTimingBoxPlot
                        summary={deathTimingSummary}
                        boxColor={chartColors.secondary}
                        lineColor={topTierColor}
                        axisColor="#6c7086"
                        labelColor="#a6adc8"
                      />
                      <p className="mt-3 font-mono text-[10px] text-ctp-overlay0">
                        Earlier medians suggest opener failures. A wider box or long upper whisker suggests more variable wipes and deeper pull collapses.
                      </p>
                    </>
                  )}
                </CardBody>
              </Card>
            ) : (
              <Card className="h-full">
                <CardHeader>
                  <CardTitle>Death Timing</CardTitle>
                  <p className="mt-0.5 text-xs text-ctp-overlay1">Fight-relative timing of wipe deaths.</p>
                </CardHeader>
                <CardBody className="flex h-full items-center">
                  <p className="w-full py-8 text-center font-mono text-xs text-ctp-overlay0">
                    Not enough wipe death events in this scope to plot timing.
                  </p>
                </CardBody>
              </Card>
            )}

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Wipe Cause Matrix</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  A compact read of which recurring killers are widest, deadliest, and most associated with a specific boss.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {wipeCauseMatrix.length === 0 ? (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No wipe-cause patterns available in this scope.
                  </p>
                ) : (
                  <div className="space-y-2.5">
                    {wipeCauseMatrix.map(row => (
                      <div
                        key={row.fullName}
                        className="grid grid-cols-[minmax(0,1.4fr)_80px_72px_minmax(0,1fr)] items-center gap-3 rounded-2xl border border-ctp-surface1 bg-ctp-surface1/35 px-3 py-2.5"
                      >
                        <div className="min-w-0">
                          <p className="truncate text-xs font-semibold text-ctp-text">{row.fullName}</p>
                          <p className="text-[10px] font-mono text-ctp-overlay0">Top recurring killer</p>
                        </div>
                        <div className="text-right">
                          <p className="text-xs font-semibold text-ctp-subtext1">{row.uniquePlayers}</p>
                          <p className="text-[10px] font-mono text-ctp-overlay0">players</p>
                        </div>
                        <div className="text-right">
                          <p className="text-xs font-semibold" style={{ color: wipeColor }}>
                            {row.deaths}
                          </p>
                          <p className="text-[10px] font-mono text-ctp-overlay0">deaths</p>
                        </div>
                        <div className="min-w-0 text-right">
                          <p className="truncate text-xs text-ctp-subtext1">{row.topBoss}</p>
                          <p className="text-[10px] font-mono text-ctp-overlay0">
                            {row.topBossDeaths} on that boss
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            <Card className="h-full">
              <CardHeader>
                <CardTitle>First Death Leaders</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Players most often recorded as the earliest death on wipe pulls. This is approximate and depends on available death events.
                </p>
              </CardHeader>
              <CardBody className="pt-1">
                {deathEvents.loading ? (
                  <LoadingState rows={5} />
                ) : firstDeathLeaders.length === 0 ? (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No timing data available for first-death analysis.
                  </p>
                ) : (
                  <>
                    <div className="mt-2.5 flex min-h-[248px] flex-col justify-evenly gap-2">
                      {firstDeathLeaders.slice(0, 10).map((row, i) => {
                        const max = firstDeathLeaders[0].count
                        const pct = max > 0 ? (row.count / max) * 100 : 0
                        return (
                          <div
                            key={row.player_name}
                            className="grid min-h-[20px] grid-cols-[24px_18px_minmax(0,100px)_1fr_48px] items-center gap-3"
                          >
                            <span className="text-right font-mono text-[10px] text-ctp-overlay0">
                              {i + 1}
                            </span>
                            <ClassDot className={row.player_class} />
                            <span className="truncate font-mono text-xs text-ctp-text">
                              {row.player_name}
                            </span>
                            <div className="h-1.5 overflow-hidden rounded-full bg-ctp-surface1">
                              <div
                                className="h-full rounded-full"
                                style={{
                                  width: `${pct}%`,
                                  backgroundColor: wipeColor,
                                  opacity: 0.82,
                                }}
                              />
                            </div>
                            <span className="text-right font-mono text-[10px] text-ctp-overlay1">
                              {row.count}
                            </span>
                          </div>
                        )
                      })}
                    </div>

                    <div className="mx-1 mt-5 space-y-3 border-t border-ctp-surface1/40 pt-3.5">
                      <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
                        First Death Breakdown · {firstDeathProfile.total} pulls with a recorded first death
                      </p>

                      {(() => {
                        const segments = (
                          [
                            { key: 'Tank', color: '#89b4fa' },
                            { key: 'Healer', color: '#a6e3a1' },
                            { key: 'DPS', color: wipeColor },
                            { key: 'Unknown', color: '#6c7086' },
                          ] as const
                        ).filter(s => (firstDeathProfile.roles[s.key] ?? 0) > 0)

                        const total = firstDeathProfile.total

                        return (
                          <div>
                            <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-ctp-surface1">
                              {segments.map(s => (
                                <div
                                  key={s.key}
                                  style={{
                                    width: `${((firstDeathProfile.roles[s.key] ?? 0) / total) * 100}%`,
                                    backgroundColor: s.color,
                                    opacity: 0.85,
                                  }}
                                />
                              ))}
                            </div>
                            <div className="mt-1.5 flex flex-wrap gap-x-3 gap-y-0.5">
                              {segments.map(s => {
                                const count = firstDeathProfile.roles[s.key] ?? 0
                                const pct = total > 0 ? Math.round((count / total) * 100) : 0
                                return (
                                  <span key={s.key} className="font-mono text-[10px] text-ctp-overlay1">
                                    <span style={{ color: s.color }}>■ </span>
                                    {s.key}{' '}
                                    <span className="font-semibold text-ctp-subtext1">{count}</span>
                                    <span className="text-ctp-overlay0"> ({pct}%)</span>
                                  </span>
                                )
                              })}
                            </div>
                          </div>
                        )
                      })()}

                      {firstDeathProfile.timing &&
                        (() => {
                          const buckets = [
                            { label: '< 30s', val: firstDeathProfile.timing.lt30s },
                            { label: '30–60s', val: firstDeathProfile.timing.s30_60 },
                            { label: '1–2m', val: firstDeathProfile.timing.m1_2 },
                            { label: '2m+', val: firstDeathProfile.timing.gt2m },
                          ]

                          const maxBucket = Math.max(...buckets.map(b => b.val), 1)

                          return (
                            <div className="space-y-1.5">
                              <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
                                When in the pull
                              </p>
                              {buckets.map(({ label, val }) => {
                                const pct =
                                  firstDeathProfile.total > 0
                                    ? Math.round((val / firstDeathProfile.total) * 100)
                                    : 0

                                return (
                                  <div
                                    key={label}
                                    className="grid grid-cols-[40px_1fr_52px] items-center gap-2"
                                  >
                                    <span className="font-mono text-[10px] text-ctp-overlay1">
                                      {label}
                                    </span>
                                    <div className="h-1.5 overflow-hidden rounded-full bg-ctp-surface1">
                                      <div
                                        className="h-full rounded-full"
                                        style={{
                                          width: `${(val / maxBucket) * 100}%`,
                                          backgroundColor: wipeColor,
                                          opacity: 0.7,
                                        }}
                                      />
                                    </div>
                                    <span className="text-right font-mono text-[10px] text-ctp-overlay1">
                                      {val}{' '}
                                      <span className="text-ctp-overlay0">({pct}%)</span>
                                    </span>
                                  </div>
                                )
                              })}
                            </div>
                          )
                        })()}

                      {firstDeathProfile.topBlows.length > 0 && (
                        <div className="space-y-1">
                          <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
                            Top first-death abilities
                          </p>
                          {firstDeathProfile.topBlows.map(blow => (
                            <div
                              key={blow.name}
                              className="flex items-center justify-between gap-3"
                            >
                              <span className="truncate font-mono text-[10px] text-ctp-subtext1">
                                {blow.name.length > 34
                                  ? `${blow.name.slice(0, 33)}…`
                                  : blow.name}
                              </span>
                              <span className="shrink-0 font-mono text-[10px] text-ctp-overlay1">
                                ×{blow.count}
                              </span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </>
                )}
              </CardBody>
            </Card>

            <Card className="h-full">
              <CardHeader>
                <CardTitle>Most Punished Players</CardTitle>
                <p className="mt-0.5 text-xs text-ctp-overlay1">
                  Players with the most deaths on wipe pulls in the current scope, and what&apos;s killing them.
                </p>
              </CardHeader>
              <CardBody>
                {deathEvents.loading || roster.loading ? (
                  <LoadingState rows={5} />
                ) : mostPunishedPlayers.length === 0 ? (
                  <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                    No wipe-death events in this scope.
                  </p>
                ) : (
                  <>
                    <div className="space-y-2">
                      {mostPunishedPlayers.map((row, i) => {
                        const max = mostPunishedPlayers[0].wipe_deaths
                        const pct = max > 0 ? (row.wipe_deaths / max) * 100 : 0

                        return (
                          <div key={row.player_name} className="space-y-1">
                            <div className="grid grid-cols-[20px_minmax(0,1fr)_1fr_40px] items-center gap-2">
                              <span className="text-right font-mono text-[10px] text-ctp-overlay0">
                                {i + 1}
                              </span>
                              <div className="flex min-w-0 items-center gap-2">
                                <ClassDot className={row.player_class} />
                                <span className="truncate font-mono text-xs font-medium text-ctp-text">
                                  {row.player_name}
                                </span>
                              </div>
                              <div className="h-1.5 overflow-hidden rounded-full bg-ctp-surface1">
                                <div
                                  className="h-full rounded-full"
                                  style={{
                                    width: `${pct}%`,
                                    backgroundColor: wipeColor,
                                    opacity: 0.8,
                                  }}
                                />
                              </div>
                              <span className="text-right font-mono text-[10px] text-ctp-overlay1">
                                {row.wipe_deaths}
                              </span>
                            </div>

                            {row.most_common_killing_blow ? (
                              <p className="pl-[52px] font-mono text-[10px] text-ctp-overlay0">
                                <span className="text-ctp-overlay1">↳ </span>
                                {row.most_common_killing_blow.length > 34
                                  ? `${row.most_common_killing_blow.slice(0, 33)}…`
                                  : row.most_common_killing_blow}
                                {row.most_common_killing_blow_count > 1 && (
                                  <span className="ml-1">×{row.most_common_killing_blow_count}</span>
                                )}
                              </p>
                            ) : null}
                          </div>
                        )
                      })}
                    </div>

                    <div className="mt-4 grid grid-cols-2 gap-2 border-t border-ctp-surface1/40 pt-4">
                      {[
                        { label: 'Wipe deaths', value: formatNumber(stats.wipeDeathCount) },
                        { label: 'Players dying', value: wipePressureSummary.uniquePlayers },
                        { label: 'Deaths / wipe', value: stats.wipeDeathsPerWipe.toFixed(1) },
                        { label: 'Distinct causes', value: wipePressureSummary.distinctCauses },
                      ].map(({ label, value }) => (
                        <div
                          key={label}
                          className="rounded-lg border border-ctp-surface1 bg-ctp-surface1/20 px-3 py-2.5"
                        >
                          <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
                            {label}
                          </p>
                          <p className="mt-1 font-mono text-sm font-semibold text-ctp-text">
                            {value}
                          </p>
                        </div>
                      ))}
                    </div>
                  </>
                )}
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            {[
              {
                title: 'Raid Defensive Capacity',
                detail: 'Mass defensives ranked by unused cast capacity across wipe pulls in scope.',
                rows: raidCooldownRows,
                accent: wipeColor,
              },
              {
                title: 'External Cooldown Capacity',
                detail: 'Single-target externals ranked by unused cast capacity across wipe pulls in scope.',
                rows: externalCooldownRows,
                accent: '#fab387',
              },
            ].map(panel => {
              const totalMissed = panel.rows.reduce((sum, row) => sum + row.missed_casts, 0)
              const totalActual = panel.rows.reduce((sum, row) => sum + row.actual_casts, 0)

              return (
                <Card key={panel.title} className="h-full">
                  <CardHeader>
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <CardTitle>{panel.title}</CardTitle>
                        <p className="mt-0.5 text-xs text-ctp-overlay1">{panel.detail}</p>
                      </div>
                      <StatusPill label={`${formatNumber(totalMissed)} unused`} active={totalMissed > 0} />
                    </div>
                  </CardHeader>
                  <CardBody className="flex h-full flex-col">
                    {wipeCooldownUtilization.loading ? (
                      <LoadingState rows={5} />
                    ) : panel.rows.length > 0 ? (
                      <>
                        <CooldownCapacityList rows={panel.rows} accent={panel.accent} />
                        <div className="mt-3 flex flex-wrap gap-2">
                          <StatusPill label={`${formatNumber(totalActual)} casts`} active />
                          <StatusPill label={`${formatNumber(totalMissed)} unused capacity`} active={totalMissed > 0} />
                        </div>
                        <p className="mt-3 font-mono text-[10px] text-ctp-overlay0">
                          Capacity uses pull duration and base cooldown: one opener cast plus one additional possible cast per full cooldown elapsed.
                        </p>
                      </>
                    ) : (
                      <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                        No tracked cooldown capacity in the current wipe scope.
                      </p>
                    )}
                  </CardBody>
                </Card>
              )
            })}
          </div>

          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle>Survival Discipline on Wipes</CardTitle>
                  <p className="mt-0.5 text-xs text-ctp-overlay1">
                    Wipe pulls only. Score blends missed personal defensive capacity with deaths lacking healthstones or health potions; grade is relative to this scope.
                  </p>
                </div>
                <StatusPill label={`${formatNumber(wipeSurvivalFailures.length)} players`} active />
              </div>
            </CardHeader>

            {deathEvents.loading || utilityByPull.loading || wipeSurvivalEvents.loading || wipeCooldownUtilization.loading ? (
              <CardBody>
                <LoadingState rows={7} />
              </CardBody>
            ) : wipeSurvivalRows.length === 0 ? (
              <CardBody>
                <p className="py-8 text-center font-mono text-xs text-ctp-overlay0">
                  No wipe deaths with utility context in this scope.
                </p>
              </CardBody>
            ) : (
              <CardBody className="overflow-x-auto">
                <div className="mb-3 grid grid-cols-1 gap-2 md:grid-cols-3">
                  {[
                    {
                      label: 'Player wipe pulls',
                      value: formatNumber(
                        wipeSurvivalFailures.reduce((sum, row) => sum + row.wipe_pulls_tracked, 0)
                      ),
                      detail: 'presence denominator in scope',
                    },
                    {
                      label: 'Missed def capacity',
                      value: formatNumber(
                        wipeSurvivalFailures.reduce((sum, row) => sum + row.defensive_missed_casts, 0)
                      ),
                      detail: 'possible defensive casts left unused',
                    },
                    {
                      label: 'Recovery gaps',
                      value: formatNumber(
                        wipeSurvivalFailures.reduce(
                          (sum, row) => sum + row.no_healthstone_deaths + row.no_health_potion_deaths,
                          0
                        )
                      ),
                      detail: 'missing stone plus potion death flags',
                    },
                  ].map(item => (
                    <div
                      key={item.label}
                      className="rounded-xl border border-ctp-surface1 bg-ctp-surface1/25 px-3 py-2.5"
                    >
                      <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
                        {item.label}
                      </p>
                      <p className="mt-1 font-mono text-base font-semibold text-ctp-text">
                        {item.value}
                      </p>
                      <p className="mt-0.5 font-mono text-[10px] text-ctp-overlay0">{item.detail}</p>
                    </div>
                  ))}
                </div>

                <Table>
                  <THead>
                    <tr>
                      <Th>Player</Th>
                      <Th right>Grade</Th>
                      <Th right>Wipe Pulls</Th>
                      <Th right>
                        <WipeSurvivalSortButton k="wipeDeaths">Wipe Death Events</WipeSurvivalSortButton>
                      </Th>
                      <Th right>Deaths/Pull</Th>
                      <Th right>
                        Def Casts
                      </Th>
                      <Th right>
                        <WipeSurvivalSortButton k="defensiveCapacityUsedPct">Def Cap Used</WipeSurvivalSortButton>
                      </Th>
                      <Th right>No Stone</Th>
                      <Th right>
                        <WipeSurvivalSortButton k="noHealthstonePct">No Stone Rate</WipeSurvivalSortButton>
                      </Th>
                      <Th right>No HPot</Th>
                      <Th right>
                        <WipeSurvivalSortButton k="noHealthPotionPct">No HPot Rate</WipeSurvivalSortButton>
                      </Th>
                      <Th right>
                        <WipeSurvivalSortButton k="survivalFailureScore">Score</WipeSurvivalSortButton>
                      </Th>
                      <Th>Top Missing</Th>
                      <Th>Most Killed By</Th>
                    </tr>
                  </THead>
                  <TBody>
                    {wipeSurvivalRows.map(row => {
                      const scoreColor =
                        row.survival_failure_score >= 30
                          ? wipeColor
                          : row.survival_failure_score >= 15
                            ? '#fab387'
                            : '#a6adc8'

                      return (
                        <Tr key={row.player_name}>
                          <Td>
                            <div className="flex items-center gap-2">
                              <ClassDot className={row.player_class} />
                              <span className="text-xs font-medium text-ctp-text">{row.player_name}</span>
                            </div>
                          </Td>
                          <Td right>
                            <span
                              className={`inline-flex min-w-7 justify-center rounded-md border px-2 py-1 font-mono text-[10px] font-semibold ${gradeClassName(row.survival_grade)}`}
                            >
                              {row.survival_grade}
                            </span>
                          </Td>
                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.wipe_pulls_tracked)}
                          </Td>
                          <Td right mono style={{ color: wipeColor }}>
                            {formatNumber(row.wipe_deaths)}
                          </Td>
                          <Td right mono style={{ color: getDeathRateColor(row.deaths_per_wipe_pull) }}>
                            {(row.deaths_per_wipe_pull * 100).toFixed(1)}%
                          </Td>
                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.defensive_actual_casts)}
                          </Td>
                          <Td
                            right
                            mono
                            style={{ color: getDeathRateColor(Math.max(0, 1 - row.defensive_capacity_used_pct / 100)) }}
                          >
                            {row.defensive_possible_casts > 0 ? `${row.defensive_capacity_used_pct.toFixed(0)}%` : '—'}
                          </Td>
                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.no_healthstone_deaths)}
                          </Td>
                          <Td right mono style={{ color: getDeathRateColor(row.no_healthstone_pct / 100) }}>
                            {row.no_healthstone_pct.toFixed(0)}%
                          </Td>
                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.no_health_potion_deaths)}
                          </Td>
                          <Td right mono style={{ color: getDeathRateColor(row.no_health_potion_pct / 100) }}>
                            {row.no_health_potion_pct.toFixed(0)}%
                          </Td>
                          <Td right mono>
                            <span className="font-semibold" style={{ color: scoreColor }}>
                              {row.survival_failure_score.toFixed(1)}
                            </span>
                          </Td>
                          <Td>
                            <span className="rounded-md border border-ctp-surface2 bg-ctp-surface1/40 px-2 py-1 font-mono text-[10px] text-ctp-overlay1">
                              {row.top_missing_category}
                            </span>
                          </Td>
                          <Td className="max-w-[220px] font-mono text-[10px] text-ctp-overlay0">
                            {row.most_common_killing_blow ? (
                              <span title={row.most_common_killing_blow} className="block truncate">
                                {row.most_common_killing_blow.length > 30
                                  ? `${row.most_common_killing_blow.slice(0, 29)}…`
                                  : row.most_common_killing_blow}
                                {row.most_common_killing_blow_count > 1 ? (
                                  <span className="ml-1 text-ctp-overlay0">
                                    ×{row.most_common_killing_blow_count}
                                  </span>
                                ) : null}
                              </span>
                            ) : (
                              '—'
                            )}
                          </Td>
                        </Tr>
                      )
                    })}
                  </TBody>
                </Table>

                <p className="mt-3 font-mono text-[10px] text-ctp-overlay0">
                  Score = ((missed personal defensive capacity × 0.5 + no healthstone before death × 0.3 + no health potion before death × 0.2) / player wipe pulls) × 100.
                  Death columns count logged death events on wipe pulls; stone and potion rates use player wipe deaths as the denominator; defensive capacity is limited to tracked abilities for the spec seen on the pull.
                </p>
              </CardBody>
            )}
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Player Survivability</CardTitle>
              <p className="mt-0.5 text-xs text-ctp-overlay1">
                Top 10 in the current scope. Wipe deaths matter more than kill deaths when reading this table.
              </p>
            </CardHeader>

            {deathEvents.loading || roster.loading ? (
              <CardBody>
                <LoadingState rows={6} />
              </CardBody>
            ) : (
              <CardBody className="overflow-x-auto">
                <Table>
                  <THead>
                    <tr>
                      <Th>Player</Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('deaths')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Deaths
                          {survivabilitySort.key === 'deaths'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('killDeath')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          On Kills
                          {survivabilitySort.key === 'killDeath'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('wipeDeath')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          On Wipes
                          {survivabilitySort.key === 'wipeDeath'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('kills')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Kills
                          {survivabilitySort.key === 'kills'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('pulls')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Pulls
                          {survivabilitySort.key === 'pulls'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('deathsPerKill')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Deaths/Kill
                          {survivabilitySort.key === 'deathsPerKill'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('deathsPerPull')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Deaths/Pull
                          {survivabilitySort.key === 'deathsPerPull'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th>Most Killed By</Th>
                    </tr>
                  </THead>

                  <TBody>
                    {playerRows.map(row => {
                      const deathPerKill = row.deaths_per_kill
                      const deathPerPull = row.deaths_per_pull
                      const onWipesDeaths = row.kill_deaths
                      const onKillsDeaths = row.wipe_deaths

                      return (
                        <Tr key={row.player_name}>
                          <Td>
                            <div className="flex items-center gap-2">
                              <ClassDot className={row.player_class} />
                              <span className="text-xs font-medium text-ctp-text">{row.player_name}</span>
                            </div>
                          </Td>

                          <Td right mono style={{ color: wipeColor }}>
                            {formatNumber(row.total_deaths)}
                          </Td>

                          <Td right mono>
                            <span
                              style={{ color: onWipesDeaths > 0 ? wipeColor : undefined }}
                              className={onWipesDeaths === 0 ? 'text-ctp-overlay0' : undefined}
                            >
                              {formatNumber(onWipesDeaths)}
                            </span>
                            {row.total_deaths > 0 ? (
                              <span className="ml-1 text-[10px] text-ctp-overlay0">
                                ({((onWipesDeaths / row.total_deaths) * 100).toFixed(0)}%)
                              </span>
                            ) : null}
                          </Td>

                          <Td right mono>
                            <span
                              style={{ color: onKillsDeaths > 0 ? topTierColor : undefined }}
                              className={onKillsDeaths === 0 ? 'text-ctp-overlay0' : undefined}
                            >
                              {formatNumber(onKillsDeaths)}
                            </span>
                            {row.total_deaths > 0 ? (
                              <span className="ml-1 text-[10px] text-ctp-overlay0">
                                ({((onKillsDeaths / row.total_deaths) * 100).toFixed(0)}%)
                              </span>
                            ) : null}
                          </Td>

                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.kills_tracked)}
                          </Td>

                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(row.pulls_tracked)}
                          </Td>

                          <Td
                            right
                            mono
                            style={deathPerKill != null ? { color: getDeathRateColor(deathPerKill) } : undefined}
                            className={deathPerKill == null ? 'text-ctp-overlay0' : undefined}
                          >
                            {deathPerKill != null ? deathPerKill.toFixed(1) : '—'}
                          </Td>

                          <Td
                            right
                            mono
                            style={deathPerPull != null ? { color: getDeathRateColor(deathPerPull) } : undefined}
                            className={deathPerPull == null ? 'text-ctp-overlay0' : undefined}
                          >
                            {deathPerPull != null ? deathPerPull.toFixed(1) : '—'}
                          </Td>

                          <Td className="max-w-[220px] font-mono text-[10px] text-ctp-overlay0">
                            {row.most_common_killing_blow ? (
                              <span title={row.most_common_killing_blow} className="block truncate">
                                {row.most_common_killing_blow.length > 30
                                  ? `${row.most_common_killing_blow.slice(0, 29)}…`
                                  : row.most_common_killing_blow}
                                {row.most_common_killing_blow_count > 1 ? (
                                  <span className="ml-1 text-ctp-overlay0">
                                    ×{row.most_common_killing_blow_count}
                                  </span>
                                ) : null}
                              </span>
                            ) : (
                              '—'
                            )}
                          </Td>
                        </Tr>
                      )
                    })}
                  </TBody>
                </Table>
              </CardBody>
            )}
          </Card>
        </div>
      )}
    </AppLayout>
  )
}
