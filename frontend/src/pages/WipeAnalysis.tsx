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
  useBossMechanics,
  usePlayerDeathEvents,
  useBossWipeAnalysis,
  useBossKillRoster,
} from '../hooks/useGoldData'
import { formatDate, formatNumber, formatPct } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal'] as const

type SurvivabilityBasis = 'kill' | 'pull'
type SurvivabilitySortKey = 'deathRate' | 'deaths' | 'wipeDeath' | 'attempts'
type SortDirection = 'asc' | 'desc'

const SURVIVABILITY_BASIS_OPTIONS: readonly { value: SurvivabilityBasis; label: string }[] = [
  { value: 'kill', label: 'Kills' },
  { value: 'pull', label: 'Pulls' },
]

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
          x2={upper}
          y2="22"
          stroke={lineColor}
          strokeOpacity="0.55"
          strokeWidth="1.0"
        />
        <line
          x1={lower}
          y1="16"
          x2={lower}
          y2="28"
          stroke={lineColor}
          strokeOpacity="0.65"
          strokeWidth="0.9"
        />
        <line
          x1={upper}
          y1="16"
          x2={upper}
          y2="28"
          stroke={lineColor}
          strokeOpacity="0.65"
          strokeWidth="0.9"
        />

        <rect
          x={q1Pos}
          y="13"
          width={boxW}
          height="18"
          rx="1.8"
          fill={boxColor}
          fillOpacity="0.14"
          stroke={boxColor}
          strokeOpacity="0.6"
          strokeWidth="0.9"
        />

        <line
          x1={medPos}
          y1="9"
          x2={medPos}
          y2="35"
          stroke={boxColor}
          strokeWidth="2"
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

function sectionTotal<T extends { [k: string]: unknown }>(rows: T[], key: keyof T) {
  return rows.reduce((sum, row) => sum + Number(row[key] ?? 0), 0)
}

function getDefaultSortDirection(key: SurvivabilitySortKey): SortDirection {
  return key === 'deathRate' ? 'asc' : 'desc'
}

function MiniNote({ children }: { children: ReactNode }) {
  return <p className="text-[10px] font-mono text-ctp-overlay0">{children}</p>
}

function StatusPill({
  label,
  active = false,
}: {
  label: string
  active?: boolean
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[10px] font-mono uppercase tracking-[0.18em] ${
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
  const roster = useBossKillRoster()

  const [diff, setDiff] = useState<string>('Mythic')
  const [selectedTier, setSelectedTier] = useState<string>('All')
  const [selectedBoss, setSelectedBoss] = useState<string>('All')
  const [search, setSearch] = useState('')
  const [survivabilityBasis, setSurvivabilityBasis] = useState<SurvivabilityBasis>('kill')
  const [survivabilitySort, setSurvivabilitySort] = useState<{
    key: SurvivabilitySortKey
    direction: SortDirection
  }>({
    key: 'deathRate',
    direction: 'asc',
  })

  const loading = mechs.loading || deathEvents.loading || wipes.loading || roster.loading
  const error = mechs.error || deathEvents.error || wipes.error || roster.error

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
    }

    for (const row of scopedDeathRows) {
      const player = ensurePlayer(row.player_name, row.player_class)
      player.total_deaths += 1
      player.pulls_tracked += 1

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
      const totalPulls = row.kills_tracked + row.pulls_tracked
      return {
        ...row,
        pulls_tracked: totalPulls,
        deaths_per_kill: row.kills_tracked > 0 ? row.total_deaths / row.kills_tracked : null,
        deaths_per_pull: totalPulls > 0 ? row.total_deaths / totalPulls : null,
      }
    })
  }, [scopedPlayerNames, scopedDeathRows, roster.data, selectedTier, diff, selectedBoss, search])

  const playersWithTrackedKills = useMemo(
    () => scopedSurvivability.filter(row => Number(row.kills_tracked) > 0),
    [scopedSurvivability]
  )

  const wipeOnlyDeaths = useMemo(() => scopedDeathRows.filter(row => !isKillRow(row)), [scopedDeathRows])

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

  const progressCurrentColor = topTierColor
  const progressPreviousColor =
    chartColors.secondary !== progressCurrentColor
      ? chartColors.secondary
      : chartColors.primary !== progressCurrentColor
        ? chartColors.primary
        : '#89b4fa'

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
        const av =
          survivabilitySort.key === 'deathRate'
            ? ((survivabilityBasis === 'kill' ? a.deaths_per_kill : a.deaths_per_pull) ??
              Number.NEGATIVE_INFINITY)
            : survivabilitySort.key === 'deaths'
              ? a.total_deaths
              : survivabilitySort.key === 'wipeDeath'
                ? a.wipe_deaths
                : survivabilityBasis === 'kill'
                  ? a.kills_tracked
                  : a.pulls_tracked

        const bv =
          survivabilitySort.key === 'deathRate'
            ? ((survivabilityBasis === 'kill' ? b.deaths_per_kill : b.deaths_per_pull) ??
              Number.NEGATIVE_INFINITY)
            : survivabilitySort.key === 'deaths'
              ? b.total_deaths
              : survivabilitySort.key === 'wipeDeath'
                ? b.wipe_deaths
                : survivabilityBasis === 'kill'
                  ? b.kills_tracked
                  : b.pulls_tracked

        const comparison = survivabilitySort.direction === 'asc' ? av - bv : bv - av

        return comparison || a.player_name.localeCompare(b.player_name)
      })
      .slice(0, 10)
  }, [scopedSurvivability, survivabilitySort, survivabilityBasis])

  function sortSurvivabilityBy(key: SurvivabilitySortKey) {
    setSurvivabilitySort(current =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: getDefaultSortDirection(key) }
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
                      <BarChart data={topWipeBosses} margin={{ top: 4, right: 8, left: -20, bottom: 68 }}>
                        <XAxis
                          dataKey="boss"
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                          tickMargin={10}
                          angle={-35}
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

                    <div className="mt-auto pt-3">
                      <MiniNote>
                        {stats.earlyWipeRate >= 0.3
                          ? `${(stats.earlyWipeRate * 100).toFixed(0)}% of wipes end before 1 minute — opener and positioning problems are a meaningful theme.`
                          : stats.lateWipeRate >= 0.3
                            ? `${(stats.lateWipeRate * 100).toFixed(0)}% of wipes last beyond 5 minutes — the bigger problem is likely later-phase consistency.`
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
                  Lower boss HP is better. This chart only includes unresolved progression targets.
                </p>
              </CardHeader>
              <CardBody className="flex h-full flex-col">
                {wipes.loading ? (
                  <LoadingState rows={4} />
                ) : activeProgressRows.length === 0 ? (
                  <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface1/30 p-4">
                    <p className="text-sm font-semibold text-ctp-text">Nothing left to progress here</p>
                    <p className="mt-2 text-xs text-ctp-overlay0">
                      All bosses in this filter scope already have recorded kills, so week-over-week progression is not shown.
                    </p>
                  </div>
                ) : (
                  <>
                    <div className="mb-3 flex flex-wrap items-center justify-end gap-4 text-[11px] font-mono">
                      <span
                        className="inline-flex items-center gap-1.5"
                        style={{ color: progressPreviousColor }}
                      >
                        <span
                          className="h-2 w-2 rounded-full"
                          style={{ backgroundColor: progressPreviousColor }}
                        />
                        Last Week Avg %
                      </span>
                      <span className="inline-flex items-center gap-1.5" style={{ color: progressCurrentColor }}>
                        <span
                          className="h-2 w-2 rounded-full"
                          style={{ backgroundColor: progressCurrentColor }}
                        />
                        Current Avg %
                      </span>
                    </div>

                    <ResponsiveContainer width="100%" height={260}>
                      <BarChart data={activeProgressRows} margin={{ top: 4, right: 8, left: -20, bottom: 70 }}>
                        <XAxis
                          dataKey="boss_name"
                          tickFormatter={(value: string) =>
                            value.length > 14 ? `${value.slice(0, 13)}…` : value
                          }
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                          tickMargin={10}
                          angle={-35}
                          textAnchor="end"
                          interval={0}
                        />
                        <YAxis
                          tickFormatter={value => `${value}%`}
                          tick={{
                            fontSize: 10,
                            fill: '#6c7086',
                            fontFamily: 'IBM Plex Mono, monospace',
                          }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                        <Bar
                          dataKey="lastWeek"
                          name="Last Week Avg %"
                          radius={[4, 4, 0, 0]}
                          fill={progressPreviousColor}
                          fillOpacity={0.45}
                        />
                        <Bar
                          dataKey="avgBossPct"
                          name="Current Avg %"
                          radius={[4, 4, 0, 0]}
                          fill={progressCurrentColor}
                          fillOpacity={0.9}
                        />
                      </BarChart>
                    </ResponsiveContainer>
                  </>
                )}
              </CardBody>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Boss Progress Table</CardTitle>
              <p className="mt-0.5 text-xs text-ctp-overlay1">
                Best pull, average wipe HP, phase reached, duration, trend, and raid-night spread. Cleared bosses stay visible here as historical context.
              </p>
            </CardHeader>
            {wipes.loading ? (
              <CardBody>
                <LoadingState rows={8} />
              </CardBody>
            ) : (
              <CardBody className="overflow-x-auto">
                <Table>
                  <THead>
                    <tr>
                      <Th>Boss</Th>
                      <Th>Zone</Th>
                      <Th>Diff</Th>
                      <Th>Status</Th>
                      <Th right>Wipes</Th>
                      <Th right>Best %</Th>
                      <Th right>Avg %</Th>
                      <Th right>Max Phase</Th>
                      <Th right>Avg Duration</Th>
                      <Th right>Trend</Th>
                      <Th right>Nights</Th>
                      <Th>Last Wipe</Th>
                    </tr>
                  </THead>
                  <TBody>
                    {historicalProgressRows.map(row => (
                      <Tr key={`${row.encounter_id}-${row.difficulty}`}>
                        <Td className="font-medium text-ctp-text">{row.boss_name}</Td>
                        <Td className="max-w-[150px] truncate text-xs text-ctp-overlay1">{row.zone_name}</Td>
                        <Td>
                          <DiffBadge label={row.difficulty_label} />
                        </Td>
                        <Td>
                          <StatusPill label={row.isCleared ? 'Cleared' : 'Active'} active={!row.isCleared} />
                        </Td>
                        <Td right mono style={{ color: wipeColor }}>
                          {formatNumber(row.total_wipes)}
                        </Td>
                        <Td
                          right
                          mono
                          style={{ color: getParseColor(100 - Number(row.best_wipe_pct)) }}
                        >
                          {formatPct(row.best_wipe_pct)}
                        </Td>
                        <Td right mono className="text-ctp-overlay1">
                          {formatPct(row.avgBossPct)}
                        </Td>
                        <Td right mono className="text-ctp-overlay1">
                          {row.maxPhase || '—'}
                        </Td>
                        <Td right mono className="text-ctp-overlay1">
                          {formatDuration(Number(row.avg_wipe_duration_seconds))}
                        </Td>
                        <Td
                          right
                          mono
                          style={{ color: Number(row.trend) < 0 ? topTierColor : wipeColor }}
                        >
                          {Number(row.trend) > 0 ? '+' : ''}
                          {Number(row.trend).toFixed(1)}%
                        </Td>
                        <Td right mono className="text-ctp-overlay1">
                          {formatNumber(row.raid_nights_attempted)}
                        </Td>
                        <Td className="text-xs text-ctp-overlay0">{formatDate(row.latest_wipe_date)}</Td>
                      </Tr>
                    ))}
                  </TBody>
                </Table>
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
                    <div className="space-y-2.5">
                      {firstDeathLeaders.slice(0, 10).map((row, i) => {
                        const max = firstDeathLeaders[0].count
                        const pct = max > 0 ? (row.count / max) * 100 : 0
                        return (
                          <div
                            key={row.player_name}
                            className="grid grid-cols-[24px_18px_minmax(0,100px)_1fr_48px] items-center gap-3"
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

          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle>Player Survivability</CardTitle>
                  <p className="mt-0.5 text-xs text-ctp-overlay1">
                    Top 10 in the current scope. Wipe deaths matter more than kill deaths when reading this table.
                  </p>
                </div>
                <FilterTabs
                  options={SURVIVABILITY_BASIS_OPTIONS}
                  value={survivabilityBasis}
                  onChange={setSurvivabilityBasis}
                  className="flex-shrink-0"
                  buttonClassName="px-3 py-1.5"
                />
              </div>
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
                          onClick={() => sortSurvivabilityBy('attempts')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          {survivabilityBasis === 'kill' ? 'Kills' : 'Pulls'}
                          {survivabilitySort.key === 'attempts'
                            ? survivabilitySort.direction === 'asc'
                              ? ' ↑'
                              : ' ↓'
                            : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('deathRate')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Deaths/{survivabilityBasis === 'kill' ? 'Kill' : 'Pull'}
                          {survivabilitySort.key === 'deathRate'
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
                      const rate =
                        survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull

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
                              style={{ color: row.wipe_deaths > 0 ? wipeColor : undefined }}
                              className={row.wipe_deaths === 0 ? 'text-ctp-overlay0' : undefined}
                            >
                              {formatNumber(row.wipe_deaths)}
                            </span>
                            {row.total_deaths > 0 ? (
                              <span className="ml-1 text-[10px] text-ctp-overlay0">
                                ({((row.wipe_deaths / row.total_deaths) * 100).toFixed(0)}%)
                              </span>
                            ) : null}
                          </Td>

                          <Td right mono className="text-ctp-overlay1">
                            {formatNumber(
                              survivabilityBasis === 'kill' ? row.kills_tracked : row.pulls_tracked
                            )}
                          </Td>

                          <Td
                            right
                            mono
                            style={rate != null ? { color: getDeathRateColor(rate) } : undefined}
                            className={rate == null ? 'text-ctp-overlay0' : undefined}
                          >
                            {rate != null ? rate.toFixed(1) : '—'}
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
