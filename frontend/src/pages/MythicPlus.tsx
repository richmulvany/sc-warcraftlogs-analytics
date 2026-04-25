import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { ChevronDown, ChevronUp, Maximize2 } from 'lucide-react'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import {
  usePlayerMplusSummary,
  usePlayerMplusScoreHistory,
  usePlayerMplusWeeklyActivity,
  usePlayerMplusDungeonBreakdown,
  useGuildRoster,
  useLiveRaidRoster,
  useRaidTeam,
} from '../hooks/useGoldData'
import { formatNumber, formatDate, getRelativeScoreDomain } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { useColourBlind } from '../context/ColourBlindContext'
import { SectionNav, useActiveSection } from '../components/ui/SectionNav'

type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'
type ScopeFilter = 'guild' | 'raid-team'

const ROLES: { value: RoleFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'dps', label: 'DPS' },
  { value: 'healer', label: 'Healer' },
  { value: 'tank', label: 'Tank' },
]
const SCOPES: { value: ScopeFilter; label: string }[] = [
  { value: 'guild', label: 'Guild' },
  { value: 'raid-team', label: 'Raid Team' },
]

const SECTIONS = [
  { id: 'leaderboard', label: 'Leaderboard' },
  { id: 'vault',       label: 'Vault' },
  { id: 'coverage',    label: 'Coverage' },
  { id: 'trajectory',  label: 'Trajectory' },
  { id: 'push',        label: 'Push' },
] as const

const PREVIEW_ROWS = 5

function safeNumber(value: unknown): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

function deriveRole(s: { score_dps: number; score_healer: number; score_tank: number }): RoleFilter {
  const dps = safeNumber(s.score_dps)
  const healer = safeNumber(s.score_healer)
  const tank = safeNumber(s.score_tank)
  if (healer >= dps && healer >= tank && healer > 0) return 'healer'
  if (tank >= dps && tank >= healer && tank > 0) return 'tank'
  return 'dps'
}

function keyLevelColor(
  level: number,
  {
    topTierColor,
    killColor,
    getParseColor,
  }: {
    topTierColor: string
    killColor: string
    getParseColor: (pct: number) => string
  }
): string {
  if (level >= 12) return topTierColor
  if (level >= 10) return killColor
  if (level >= 7) return getParseColor(95)
  if (level >= 1) return getParseColor(75)
  return '#45475a'
}

function keyLevelHeatmapColor(
  level: number,
  {
    getParseColor,
    range,
  }: {
    getParseColor: (pct: number) => string
    range: { min: number; max: number } | null
  }
): string {
  if (level <= 0) return '#45475a'
  if (!range) return getParseColor(50)
  const clamped = Math.min(Math.max(level, range.min), range.max)
  const span = Math.max(1, range.max - range.min)
  const pct = 15 + ((clamped - range.min) / span) * 84
  return getParseColor(pct)
}

const VAULT_TIERS = [
  { count: 1, label: '1 key' },
  { count: 4, label: '4 keys' },
  { count: 8, label: '8 keys' },
] as const

function ExpandToggle({
  open, onClick, hiddenCount, label = 'rows',
}: { open: boolean; onClick: () => void; hiddenCount?: number; label?: string }) {
  return (
    <button
      onClick={onClick}
      className="w-full flex items-center justify-center gap-1.5 px-4 py-2.5 text-[11px] font-mono text-ctp-overlay1 hover:text-ctp-text border-t border-ctp-surface1 transition-colors"
    >
      {open
        ? <>collapse <ChevronUp className="w-3 h-3" /></>
        : <>show all{hiddenCount !== undefined ? ` ${hiddenCount} ${label}` : ''} <ChevronDown className="w-3 h-3" /></>
      }
    </button>
  )
}


export function MythicPlus() {
  const { topTierColor, chartColors, killColor, getParseColor, getRoleColor } = useColourBlind()
  const summary = usePlayerMplusSummary()
  const history = usePlayerMplusScoreHistory()
  const weekly = usePlayerMplusWeeklyActivity()
  const dungeons = usePlayerMplusDungeonBreakdown()
  const roster = useGuildRoster()
  const liveRoster = useLiveRaidRoster()
  const raidTeam = useRaidTeam()

  const [role, setRole] = useState<RoleFilter>('all')
  const [scope, setScope] = useState<ScopeFilter>('guild')
  const [search, setSearch] = useState('')
  const [minScore, setMinScore] = useState(1)

  const [showAllLeaderboard, setShowAllLeaderboard] = useState(false)
  const [showAllVault, setShowAllVault] = useState(false)
  const [showFullMatrix, setShowFullMatrix] = useState(false)
  const [showAllPush, setShowAllPush] = useState(false)

  const activeSectionId = useActiveSection(SECTIONS)

  const classByPlayer = useMemo(() => {
    const map = new Map<string, string>()
    for (const r of roster.data) {
      if (r.name) map.set(r.name, r.player_class || 'Unknown')
    }
    return map
  }, [roster.data])

  const teamMemberNames = useMemo(() => {
    if (liveRoster.data.length > 0) {
      return new Set(
        liveRoster.data
          .map(row => row.name?.trim().toLowerCase())
          .filter((name): name is string => Boolean(name))
      )
    }

    const names = new Set<string>()
    for (const row of raidTeam.data) {
      if (row.name) names.add(row.name.toLowerCase())
    }
    return names
  }, [liveRoster.data, raidTeam.data])

  // Active raiders = summary rows with any score > 0
  const activeSummary = useMemo(() =>
    summary.data
      .filter(r => r.player_name && safeNumber(r.score_all) > 0)
      .map(r => ({
        ...r,
        role: deriveRole(r),
        player_class: classByPlayer.get(r.player_name) || 'Unknown',
      })),
    [summary.data, classByPlayer]
  )

  const activeRaidTeamSummary = useMemo(() =>
    activeSummary.filter(r => teamMemberNames.has(r.player_name.toLowerCase())),
    [activeSummary, teamMemberNames]
  )

  const scopeSummary = useMemo(() =>
    scope === 'raid-team' ? activeRaidTeamSummary : activeSummary,
    [scope, activeRaidTeamSummary, activeSummary]
  )

  function filterSummaryRows<T extends { score_all: unknown; role: RoleFilter; player_name: string; player_class: string }>(rows: T[]): T[] {
    let next = rows.filter(r => safeNumber(r.score_all) >= minScore)
    if (role !== 'all') next = next.filter(r => r.role === role)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      next = next.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class)
      )
    }
    return [...next].sort((a, b) => safeNumber(b.score_all) - safeNumber(a.score_all))
  }

  const filteredSummary = useMemo(() => {
    return filterSummaryRows(scopeSummary)
  }, [scopeSummary, role, search, minScore])

  const heroSummary = useMemo(() => {
    return filterSummaryRows(activeRaidTeamSummary)
  }, [activeRaidTeamSummary, role, search, minScore])

  // Current reset week = most recent week_start in weekly_activity
  const currentWeek = useMemo(() => {
    const weeks = [...new Set(weekly.data.map(r => r.week_start))]
      .filter(Boolean)
      .sort()
      .reverse()
    return weeks[0] ?? null
  }, [weekly.data])

  const currentWeekActivity = useMemo(() => {
    if (!currentWeek) return new Map<string, number>()
    const map = new Map<string, number>()
    for (const r of weekly.data) {
      if (r.week_start === currentWeek) {
        map.set(r.player_name, safeNumber(r.total_runs))
      }
    }
    return map
  }, [weekly.data, currentWeek])

  const summaryStats = useMemo(() => {
    if (!heroSummary.length) return null
    const avgScore = heroSummary.reduce((s, r) => s + safeNumber(r.score_all), 0) / heroSummary.length
    const top = [...heroSummary].sort((a, b) => safeNumber(b.score_all) - safeNumber(a.score_all))[0]
    const keysThisWeek = heroSummary.reduce((sum, row) => sum + (currentWeekActivity.get(row.player_name) ?? 0), 0)
    return {
      active: heroSummary.length,
      avgScore,
      top,
      keysThisWeek,
    }
  }, [heroSummary, currentWeekActivity])

  // Vault progress this reset
  const vaultRows = useMemo(() => {
    if (!currentWeek) return []
    return filteredSummary
      .map(r => {
        const runs = currentWeekActivity.get(r.player_name) ?? 0
        return { ...r, runs_this_week: runs }
      })
      .sort((a, b) => b.runs_this_week - a.runs_this_week)
  }, [filteredSummary, currentWeekActivity, currentWeek])

  const vaultGuildSummary = useMemo(() => {
    if (vaultRows.length === 0) return null
    const counts = VAULT_TIERS.map(t => ({
      ...t,
      filled: vaultRows.filter(r => r.runs_this_week >= t.count).length,
    }))
    const total = vaultRows.length
    return { counts, total, totalRuns: vaultRows.reduce((s, r) => s + r.runs_this_week, 0) }
  }, [vaultRows])

  // Dungeon coverage matrix
  const dungeonNames = useMemo(() => {
    return [...new Set(dungeons.data.map(r => r.dungeon))].filter(Boolean).sort()
  }, [dungeons.data])

  const dungeonMatrix = useMemo(() => {
    const playerSet = new Set(filteredSummary.map(r => r.player_name))
    const map = new Map<string, Map<string, { best_timed: number; best_key: number }>>()
    for (const r of dungeons.data) {
      if (!playerSet.has(r.player_name)) continue
      if (!map.has(r.player_name)) map.set(r.player_name, new Map())
      map.get(r.player_name)!.set(r.dungeon, {
        best_timed: safeNumber(r.highest_timed_level),
        best_key: safeNumber(r.highest_key_level),
      })
    }
    return filteredSummary.map(r => ({
      player_name: r.player_name,
      player_class: r.player_class,
      role: r.role,
      cells: dungeonNames.map(d => map.get(r.player_name)?.get(d) ?? { best_timed: 0, best_key: 0 }),
    }))
  }, [filteredSummary, dungeons.data, dungeonNames])

  const matrixSummary = useMemo(() => {
    if (dungeonMatrix.length === 0 || dungeonNames.length === 0) return null
    let totalCells = 0
    let timedCells = 0
    let bestKey = 0
    let sumBest = 0
    let sumBestCount = 0
    const timedLevels: number[] = []
    for (const row of dungeonMatrix) {
      for (const cell of row.cells) {
        totalCells += 1
        if (cell.best_timed > 0) {
          timedCells += 1
          sumBest += cell.best_timed
          sumBestCount += 1
          timedLevels.push(cell.best_timed)
        }
        if (cell.best_key > bestKey) bestKey = cell.best_key
      }
    }
    const sortedTimedLevels = [...timedLevels].sort((a, b) => a - b)
    const medianTimed =
      sortedTimedLevels.length === 0
        ? 0
        : sortedTimedLevels.length % 2 === 1
          ? sortedTimedLevels[Math.floor(sortedTimedLevels.length / 2)]
          : (sortedTimedLevels[sortedTimedLevels.length / 2 - 1] + sortedTimedLevels[sortedTimedLevels.length / 2]) / 2
    return {
      coverage: totalCells === 0 ? 0 : (timedCells / totalCells) * 100,
      avgTimed: sumBestCount === 0 ? 0 : sumBest / sumBestCount,
      bestKey,
      medianTimed,
      totalCells,
    }
  }, [dungeonMatrix, dungeonNames])

  const heatmapLevelRange = useMemo(() => {
    const levels = dungeonMatrix
      .flatMap(row => row.cells.map(cell => cell.best_timed))
      .filter(level => level > 0)
    if (levels.length === 0) return null
    return {
      min: Math.min(...levels),
      max: Math.max(...levels),
    }
  }, [dungeonMatrix])

  // Score trajectory: avg non-zero score per role per snapshot_date
  const trajectoryData = useMemo(() => {
    const playerRole = new Map(filteredSummary.map(r => [r.player_name, r.role]))
    const byDate = new Map<string, { dps: number[]; healer: number[]; tank: number[] }>()
    for (const r of history.data) {
      const score = safeNumber(r.score_all)
      if (score <= 0) continue
      const date = r.snapshot_date
      if (!date) continue
      const pRole = playerRole.get(r.player_name)
      if (!pRole || pRole === 'all') continue
      if (!byDate.has(date)) byDate.set(date, { dps: [], healer: [], tank: [] })
      byDate.get(date)![pRole as 'dps' | 'healer' | 'tank'].push(score)
    }
    const points = [...byDate.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, buckets]) => ({
        date,
        dps:    buckets.dps.length    ? buckets.dps.reduce((s, v) => s + v, 0)    / buckets.dps.length    : null,
        healer: buckets.healer.length ? buckets.healer.reduce((s, v) => s + v, 0) / buckets.healer.length : null,
        tank:   buckets.tank.length   ? buckets.tank.reduce((s, v) => s + v, 0)   / buckets.tank.length   : null,
        all:    [...buckets.dps, ...buckets.healer, ...buckets.tank].length
          ? [...buckets.dps, ...buckets.healer, ...buckets.tank].reduce((s, v) => s + v, 0) /
            [...buckets.dps, ...buckets.healer, ...buckets.tank].length
          : null,
      }))
    return points
  }, [history.data, filteredSummary])

  const trajectoryYDomain = useMemo(() => {
    return getRelativeScoreDomain(
      trajectoryData.flatMap(row => [row.all, row.dps, row.healer, row.tank]),
      { minPadding: 25, flatPadding: 50, step: 25 }
    )
  }, [trajectoryData])

  // Push candidates: dungeon_breakdown rows where untimed key beats best timed
  const pushCandidates = useMemo(() => {
    const playerRole = new Map(filteredSummary.map(r => [r.player_name, r.role]))
    return dungeons.data
      .filter(r => safeNumber(r.highest_key_level) > safeNumber(r.highest_timed_level))
      .filter(r => safeNumber(r.highest_key_level) > 0)
      .map(r => ({
        player_name: r.player_name,
        player_class: classByPlayer.get(r.player_name) || 'Unknown',
        role: playerRole.get(r.player_name) ?? 'dps',
        dungeon: r.dungeon,
        short_name: r.best_short_name,
        highest_untimed: safeNumber(r.highest_key_level),
        highest_timed: safeNumber(r.highest_timed_level),
        gap: safeNumber(r.highest_key_level) - safeNumber(r.highest_timed_level),
        latest: r.latest_completed_at,
      }))
      .filter(r => role === 'all' || r.role === role)
      .filter(r => {
        if (!search.trim()) return true
        const q = normaliseSearchText(search)
        return matchesLooseSearch(q, r.player_name) ||
               matchesLooseSearch(q, r.player_class) ||
               matchesLooseSearch(q, r.dungeon)
      })
      .sort((a, b) => b.highest_untimed - a.highest_untimed || b.gap - a.gap)
  }, [dungeons.data, filteredSummary, classByPlayer, role, search])

  const loading = summary.loading || history.loading || weekly.loading || dungeons.loading || roster.loading || liveRoster.loading || raidTeam.loading
  const error = summary.error || history.error || weekly.error || dungeons.error || roster.error || liveRoster.error || raidTeam.error

  const visibleLeaderboard = showAllLeaderboard ? filteredSummary : filteredSummary.slice(0, PREVIEW_ROWS)
  const visibleVault = showAllVault ? vaultRows : vaultRows.slice(0, PREVIEW_ROWS)
  const visiblePush = showAllPush ? pushCandidates : pushCandidates.slice(0, PREVIEW_ROWS)
  const scopePlayerSingular = scope === 'raid-team' ? 'raid-team player' : 'guild player'
  const scopePlayersLabel = scope === 'raid-team' ? 'raid-team players' : 'guild players'
  const scopeContainerLabel = scope === 'raid-team' ? 'the raid team' : 'the guild'
  const scopeCandidateLabel = scope === 'raid-team' ? 'raid-team candidates' : 'guild candidates'

  return (
    <AppLayout
      title="Mythic+"
      subtitle="guild dashboard · score · vault · coverage · push"
      nav={<SectionNav sections={SECTIONS} activeId={activeSectionId} />}
    >
      {summaryStats && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard
            label="Active Raiders"
            value={summaryStats.active}
            subValue="raid-team members with non-zero current score"
            accent="none"
          />
          <StatCard
            label="Avg Score"
            value={Math.round(summaryStats.avgScore)}
            subValue="across active raiders"
            accent="none"
          />
          <StatCard
            label="Top Scorer"
            value={Math.round(safeNumber(summaryStats.top.score_all))}
            subValue={summaryStats.top.player_name}
            valueColor={topTierColor}
            accent="none"
          />
          <StatCard
            label="Keys This Reset"
            value={summaryStats.keysThisWeek}
            subValue={currentWeek ? `week of ${formatDate(currentWeek)}` : 'no current week data'}
            accent="green"
          />
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <FilterTabs options={SCOPES} value={scope} onChange={(value) => setScope(value as ScopeFilter)} />
        <FilterTabs options={ROLES} value={role} onChange={setRole} />
        <input
          type="text"
          placeholder="Search player or class…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-blue/40 w-56"
        />
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono text-ctp-overlay0">Min score:</span>
          <select
            value={minScore}
            onChange={e => setMinScore(Number(e.target.value))}
            className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-2 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
          >
            {[1, 500, 1000, 1500, 2000, 2500, 3000].map(n => <option key={n} value={n}>{n}+</option>)}
          </select>
        </div>
      </div>

      {/* ── Leaderboard ───────────────────────────────────────────── */}
      <section id="leaderboard" className="scroll-mt-20">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between gap-3">
              <div>
                <CardTitle>Roster Leaderboard</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Current-season Raider.IO score per {scopePlayerSingular}.{' '}
                  {showAllLeaderboard ? 'Showing all players.' : `Showing top ${Math.min(PREVIEW_ROWS, filteredSummary.length)} of ${filteredSummary.length}.`}
                </p>
              </div>
            </div>
          </CardHeader>
          {loading ? (
            <div className="p-5"><LoadingState rows={PREVIEW_ROWS} /></div>
          ) : error ? (
            <div className="p-5"><ErrorState message={error} /></div>
          ) : filteredSummary.length === 0 ? (
            <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
              No M+ data matches the current filters.
            </div>
          ) : (
            <>
              <Table>
                <THead>
                  <tr>
                    <Th>#</Th>
                    <Th>Player</Th>
                    <Th>Role</Th>
                    <Th right>Score</Th>
                    <Th right>Region Rank</Th>
                    <Th right>Realm Rank</Th>
                    <Th right>Total Runs</Th>
                    <Th right>Timed / Untimed</Th>
                    <Th right>Best Key</Th>
                    <Th>Last Snapshot</Th>
                  </tr>
                </THead>
                <TBody>
                  {visibleLeaderboard.map((p, i) => (
                    <Tr key={p.player_name}>
                      <Td mono className="text-ctp-surface2 text-xs">{i + 1}</Td>
                      <Td>
                        <Link to={`/players/${encodeURIComponent(p.player_name)}`} className="flex items-center gap-2 hover:text-ctp-mauve">
                          <ClassDot className={p.player_class} />
                          <div>
                            <p className="text-sm font-medium text-ctp-text">{p.player_name}</p>
                            <ClassLabel className={p.player_class} size="sm" />
                          </div>
                        </Link>
                      </Td>
                      <Td><RoleBadge role={p.role} /></Td>
                      <Td right mono className="font-semibold" style={{ color: i === 0 && !showAllLeaderboard ? topTierColor : undefined }}>
                        {Math.round(safeNumber(p.score_all))}
                      </Td>
                      <Td right mono className="text-ctp-overlay1">
                        {safeNumber(p.region_rank) > 0 ? `#${formatNumber(p.region_rank)}` : '—'}
                      </Td>
                      <Td right mono className="text-ctp-overlay1">
                        {safeNumber(p.realm_rank) > 0 ? `#${formatNumber(p.realm_rank)}` : '—'}
                      </Td>
                      <Td right mono className="text-ctp-subtext1">{formatNumber(p.total_runs)}</Td>
                      <Td right mono className="text-ctp-overlay1">
                        {formatNumber(p.timed_runs)} / {formatNumber(p.untimed_runs)}
                      </Td>
                      <Td
                        right
                        mono
                        className="font-semibold"
                        style={{ color: keyLevelColor(safeNumber(p.highest_timed_level), { topTierColor, killColor, getParseColor }) }}
                      >
                        +{safeNumber(p.highest_timed_level) || '—'}
                      </Td>
                      <Td className="text-xs text-ctp-overlay0">{formatDate(p.snapshot_at)}</Td>
                    </Tr>
                  ))}
                </TBody>
              </Table>
              {filteredSummary.length > PREVIEW_ROWS && (
                <ExpandToggle
                  open={showAllLeaderboard}
                  onClick={() => setShowAllLeaderboard(v => !v)}
                  hiddenCount={filteredSummary.length - PREVIEW_ROWS}
                  label="players"
                />
              )}
            </>
          )}
        </Card>
      </section>

      {/* ── Vault Progress ───────────────────────────────────────── */}
      <section id="vault" className="scroll-mt-20">
        <Card>
          <CardHeader>
            <CardTitle>Vault Progress · This Reset</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Keys completed by {scopePlayersLabel} in the current week ({currentWeek ? formatDate(currentWeek) : 'no data'}).
              Vault tiers fill at 1 / 4 / 8 keys.
            </p>
          </CardHeader>
          {loading ? (
            <div className="p-5"><LoadingState rows={PREVIEW_ROWS} /></div>
          ) : !currentWeek || vaultRows.length === 0 ? (
            <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
              No weekly activity for the current reset in {scopeContainerLabel}.
            </div>
          ) : (
            <>
              {vaultGuildSummary && (
                <div className="px-5 py-4 grid grid-cols-3 gap-3 border-b border-ctp-surface1">
                  {vaultGuildSummary.counts.map(t => {
                    const pct = vaultGuildSummary.total === 0 ? 0 : (t.filled / vaultGuildSummary.total) * 100
                    return (
                      <div key={t.count} className="rounded-lg border border-ctp-surface1 bg-ctp-surface0/40 px-3 py-2.5">
                        <div className="flex items-baseline justify-between mb-1.5">
                          <span className="text-[10px] font-mono text-ctp-overlay0 uppercase tracking-wide">{t.label}</span>
                          <span className="text-xs font-mono text-ctp-text font-semibold">{t.filled}<span className="text-ctp-overlay0">/{vaultGuildSummary.total}</span></span>
                        </div>
                        <div className="h-1.5 rounded-full bg-ctp-surface1 overflow-hidden">
                          <div className="h-full" style={{ width: `${pct}%`, backgroundColor: killColor }} />
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
              <div className="px-4">
              <Table>
                <THead>
                  <tr>
                    <Th>Player</Th>
                    <Th>Role</Th>
                    <Th right>Runs</Th>
                    {VAULT_TIERS.map(t => <Th key={t.count} right>{t.label}</Th>)}
                  </tr>
                </THead>
                <TBody>
                  {visibleVault.map(p => (
                    <Tr key={p.player_name}>
                      <Td>
                        <div className="flex items-center gap-2">
                          <ClassDot className={p.player_class} />
                          <p className="text-sm font-medium text-ctp-text">{p.player_name}</p>
                        </div>
                      </Td>
                      <Td><RoleBadge role={p.role} /></Td>
                      <Td right mono className="font-semibold text-ctp-text">{p.runs_this_week}</Td>
                      {VAULT_TIERS.map(t => {
                        const filled = p.runs_this_week >= t.count
                        return (
                          <Td key={t.count} right>
                            <span
                              className="inline-block w-3.5 h-3.5 rounded-sm border"
                              style={{
                                backgroundColor: filled ? killColor : 'transparent',
                                borderColor: filled ? killColor : '#45475a',
                              }}
                              title={filled ? `${t.label} unlocked` : `${t.count - p.runs_this_week} more for ${t.label}`}
                            />
                          </Td>
                        )
                      })}
                    </Tr>
                  ))}
                </TBody>
              </Table>
              </div>
              {vaultRows.length > PREVIEW_ROWS && (
                <ExpandToggle
                  open={showAllVault}
                  onClick={() => setShowAllVault(v => !v)}
                  hiddenCount={vaultRows.length - PREVIEW_ROWS}
                  label="players"
                />
              )}
            </>
          )}
        </Card>
      </section>

      {/* ── Coverage Matrix ──────────────────────────────────────── */}
      <section id="coverage" className="scroll-mt-20">
        <Card>
          <CardHeader>
            <CardTitle>Dungeon Coverage Matrix</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Best timed key per dungeon across {scopePlayersLabel}.{showFullMatrix ? ' Hover cells for untimed-best.' : ''}
            </p>
          </CardHeader>
          {loading ? (
            <div className="p-5"><LoadingState rows={6} /></div>
          ) : dungeonMatrix.length === 0 || dungeonNames.length === 0 ? (
            <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
              No dungeon coverage data.
            </div>
          ) : (
            <>
              {showFullMatrix ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="border-b border-ctp-surface1">
                        <th className="px-3 py-2 text-left font-mono text-[10px] uppercase tracking-wide text-ctp-overlay0 sticky left-0 bg-ctp-surface0">Player</th>
                        {dungeonNames.map(d => (
                          <th key={d} className="px-2 py-2 text-center font-mono text-[10px] uppercase tracking-wide text-ctp-overlay0" title={d}>
                            {d.length > 6 ? d.slice(0, 6) + '…' : d}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-ctp-surface0">
                      {dungeonMatrix.map(row => (
                        <tr key={row.player_name}>
                          <td className="px-3 py-1.5 sticky left-0 bg-ctp-surface0">
                            <div className="flex items-center gap-2">
                              <ClassDot className={row.player_class} />
                              <span className="text-sm text-ctp-text">{row.player_name}</span>
                            </div>
                          </td>
                          {row.cells.map((cell, i) => (
                            <td
                              key={i}
                              className="px-2 py-1.5 text-center font-mono"
                              style={{
                                backgroundColor: cell.best_timed > 0 ? `${keyLevelHeatmapColor(cell.best_timed, { getParseColor, range: heatmapLevelRange })}22` : undefined,
                                color: cell.best_timed > 0 ? keyLevelHeatmapColor(cell.best_timed, { getParseColor, range: heatmapLevelRange }) : '#45475a',
                              }}
                              title={cell.best_key > cell.best_timed
                                ? `Timed +${cell.best_timed || 0}, untimed +${cell.best_key}`
                                : cell.best_timed > 0 ? `Timed +${cell.best_timed}` : 'No completed runs'}
                            >
                              {cell.best_timed > 0 ? cell.best_timed : '—'}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <MatrixThumbnail
                  matrix={dungeonMatrix}
                  dungeonNames={dungeonNames}
                  summary={matrixSummary}
                  onExpand={() => setShowFullMatrix(true)}
                />
              )}
              <ExpandToggle
                open={showFullMatrix}
                onClick={() => setShowFullMatrix(v => !v)}
              />
            </>
          )}
        </Card>
      </section>

      {/* ── Score Trajectory ─────────────────────────────────────── */}
      <section id="trajectory" className="scroll-mt-20">
        <Card>
          <CardHeader>
            <CardTitle>Score Trajectory</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Average current-season score per role across {scopePlayersLabel}, by snapshot date.
              Note: Raider.IO history starts from first ingestion.
            </p>
          </CardHeader>
          {loading ? (
            <div className="p-5"><LoadingState rows={6} /></div>
          ) : trajectoryData.length === 0 ? (
            <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
              No score history available.
            </div>
          ) : (
            <div className="p-5 pt-2">
              {trajectoryYDomain && (
                <p className="mb-2 text-[10px] font-mono text-ctp-overlay0">
                  Y-axis is scaled to the visible score range.
                </p>
              )}
              <div className="h-56 md:h-72 2xl:h-80"><ResponsiveContainer width="100%" height="100%">
                <LineChart data={trajectoryData} margin={{ top: 4, right: 12, left: -10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#45475a" vertical={false} />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false}
                    tickLine={false}
                    tickFormatter={(v) => formatDate(v)}
                    interval="preserveStartEnd"
                    minTickGap={40}
                  />
                  <YAxis
                    domain={trajectoryYDomain ?? undefined}
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false}
                    tickLine={false}
                    allowDecimals={false}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#1e1e2e',
                      border: '1px solid #45475a',
                      borderRadius: '0.75rem',
                      fontSize: 12,
                      fontFamily: 'IBM Plex Mono, monospace',
                    }}
                    labelFormatter={(v) => formatDate(String(v))}
                    formatter={(val) => {
                      const n = Number(val)
                      return Number.isFinite(n) ? Math.round(n) : '—'
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11, fontFamily: 'IBM Plex Mono, monospace' }} />
                  <Line type="monotone" dataKey="all"    name="All"    stroke={chartColors.primary}   strokeWidth={2}   dot={false} connectNulls />
                  <Line type="monotone" dataKey="dps"    name="DPS"    stroke={getRoleColor('dps')}    strokeWidth={1.5} dot={false} connectNulls />
                  <Line type="monotone" dataKey="healer" name="Healer" stroke={getRoleColor('healer')} strokeWidth={1.5} dot={false} connectNulls />
                  <Line type="monotone" dataKey="tank"   name="Tank"   stroke={getRoleColor('tank')}   strokeWidth={1.5} dot={false} connectNulls />
                </LineChart>
              </ResponsiveContainer></div>
            </div>
          )}
        </Card>
      </section>

      {/* ── Push Candidates ──────────────────────────────────────── */}
      <section id="push" className="scroll-mt-20">
        <Card>
          <CardHeader>
            <CardTitle>Push Candidates</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Players whose highest attempted key in a dungeon exceeds their best timed —
              close enough to push if reattempted with a tuned group. Showing top {Math.min(PREVIEW_ROWS, pushCandidates.length)} of {pushCandidates.length} {scopeCandidateLabel}.
            </p>
          </CardHeader>
          {loading ? (
            <div className="p-5"><LoadingState rows={PREVIEW_ROWS} /></div>
          ) : pushCandidates.length === 0 ? (
            <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
              No untimed runs that beat a player's best timed level.
            </div>
          ) : (
            <>
              <Table>
                <THead>
                  <tr>
                    <Th>Player</Th>
                    <Th>Role</Th>
                    <Th>Dungeon</Th>
                    <Th right>Best Timed</Th>
                    <Th right>Highest Untimed</Th>
                    <Th right>Gap</Th>
                    <Th>Last Attempt</Th>
                  </tr>
                </THead>
                <TBody>
                  {visiblePush.map((p, i) => (
                    <Tr key={`${p.player_name}-${p.dungeon}-${i}`}>
                      <Td>
                        <div className="flex items-center gap-2">
                          <ClassDot className={p.player_class} />
                          <span className="text-sm font-medium text-ctp-text">{p.player_name}</span>
                        </div>
                      </Td>
                      <Td><RoleBadge role={p.role} /></Td>
                      <Td className="text-xs text-ctp-subtext0">{p.dungeon}</Td>
                      <Td right mono style={{ color: keyLevelColor(p.highest_timed, { topTierColor, killColor, getParseColor }) }}>
                        {p.highest_timed > 0 ? `+${p.highest_timed}` : '—'}
                      </Td>
                      <Td right mono className="font-semibold" style={{ color: keyLevelColor(p.highest_untimed, { topTierColor, killColor, getParseColor }) }}>
                        +{p.highest_untimed}
                      </Td>
                      <Td right mono style={{ color: getParseColor(95) }}>+{p.gap}</Td>
                      <Td className="text-xs text-ctp-overlay0">{formatDate(p.latest)}</Td>
                    </Tr>
                  ))}
                </TBody>
              </Table>
              {pushCandidates.length > PREVIEW_ROWS && (
                <ExpandToggle
                  open={showAllPush}
                  onClick={() => setShowAllPush(v => !v)}
                  hiddenCount={pushCandidates.length - PREVIEW_ROWS}
                  label="candidates"
                />
              )}
            </>
          )}
        </Card>
      </section>
    </AppLayout>
  )
}

interface MatrixThumbProps {
  matrix: { player_name: string; cells: { best_timed: number; best_key: number }[] }[]
  dungeonNames: string[]
  summary: {
    coverage: number
    avgTimed: number
    bestKey: number
    medianTimed: number
    totalCells: number
  } | null
  onExpand: () => void
}

function MatrixThumbnail({ matrix, dungeonNames, summary, onExpand }: MatrixThumbProps) {
  const { getParseColor } = useColourBlind()
  const CELL_PX = 6
  const MAX_PLAYERS = 99

  // Players sorted highest peak key descending — highest scorer on the left
  const sortedPlayers = useMemo(() =>
    [...matrix]
      .sort((a, b) => {
        const aMax = a.cells.reduce((m, c) => Math.max(m, c.best_key), 0)
        const bMax = b.cells.reduce((m, c) => Math.max(m, c.best_key), 0)
        return bMax - aMax
      })
      .slice(0, MAX_PLAYERS),
    [matrix]
  )
  const hiddenPlayers = Math.max(0, matrix.length - MAX_PLAYERS)

  const heatmapLevelRange = useMemo(() => {
    const levels = matrix
      .flatMap(row => row.cells.map(cell => cell.best_timed))
      .filter(level => level > 0)
    if (levels.length === 0) return null
    return { min: Math.min(...levels), max: Math.max(...levels) }
  }, [matrix])

  return (
    <div className="flex flex-col gap-4 p-5">
      {/* Matrix heatmap — full width, legend inline */}
      <button
        onClick={onExpand}
        className="group relative rounded-xl border border-ctp-surface1 bg-ctp-surface0/30 hover:border-ctp-mauve/40 transition-colors p-3 text-left overflow-x-auto"
        aria-label="Expand full coverage matrix"
      >
        {/* Horizontal layout: dungeons = rows, players = columns */}
        <div
          className="grid gap-[2px]"
          style={{
            gridTemplateColumns: `repeat(${sortedPlayers.length}, ${CELL_PX}px)`,
            gridAutoRows: `${CELL_PX}px`,
          }}
        >
          {dungeonNames.flatMap((_, dIdx) =>
            sortedPlayers.map((row, pIdx) => {
              const cell = row.cells[dIdx]
              return (
                <div
                  key={`${dIdx}-${pIdx}`}
                  className="rounded-[1px]"
                  style={{
                    backgroundColor: cell.best_timed > 0
                      ? keyLevelHeatmapColor(cell.best_timed, { getParseColor, range: heatmapLevelRange })
                      : '#313244',
                    opacity: cell.best_timed > 0 ? 0.85 : 0.4,
                  }}
                />
              )
            })
          )}
        </div>
        <div className="mt-2 flex items-center justify-between text-[10px] font-mono text-ctp-overlay0">
          <span>
            {dungeonNames.length} dungeons × {sortedPlayers.length} players
            {hiddenPlayers > 0 && <span className="text-ctp-overlay0/70"> · top {MAX_PLAYERS} shown</span>}
          </span>
          <span className="opacity-60 group-hover:opacity-100 transition-opacity flex items-center gap-1 text-ctp-mauve">
            expand <Maximize2 className="w-3 h-3" />
          </span>
        </div>
        <div className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1">
          {([
            { label: '+12 push', level: 12 },
            { label: '+10 vault', level: 10 },
            { label: '+7 mid',   level: 7 },
            { label: '+1 low',   level: 1 },
            { label: 'untimed',  level: 0 },
          ] as const).map(t => (
            <div key={t.label} className="flex items-center gap-1.5">
              <span
                className="inline-block w-2.5 h-2.5 rounded-sm flex-shrink-0"
                style={{ backgroundColor: t.level === 0 ? '#313244' : keyLevelHeatmapColor(t.level, { getParseColor, range: { min: 1, max: 12 } }), opacity: t.level === 0 ? 0.45 : 0.85 }}
              />
              <span className="text-[10px] font-mono text-ctp-overlay1">{t.label}</span>
            </div>
          ))}
        </div>
      </button>

      {/* Stat tiles below the matrix */}
      {summary && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
          <ThumbStat label="Coverage" value={`${summary.coverage.toFixed(0)}%`} sub={`${summary.totalCells} cells`} />
          <ThumbStat label="Avg Timed" value={`+${summary.avgTimed.toFixed(1)}`} sub="across timed cells" />
          <ThumbStat
            label="Top Key"
            value={`+${summary.bestKey || '—'}`}
            sub="best in roster"
            valueColor={keyLevelHeatmapColor(summary.bestKey, { getParseColor, range: heatmapLevelRange })}
          />
          <ThumbStat
            label="Median Best Key"
            value={summary.medianTimed > 0 ? `+${summary.medianTimed.toFixed(summary.medianTimed % 1 === 0 ? 0 : 1)}` : '—'}
            sub={`middle timed cell of ${summary.totalCells}`}
            valueColor={summary.medianTimed > 0 ? keyLevelHeatmapColor(summary.medianTimed, { getParseColor, range: heatmapLevelRange }) : undefined}
          />
        </div>
      )}
    </div>
  )
}

function ThumbStat({
  label, value, sub, valueColor,
}: { label: string; value: string; sub?: string; valueColor?: string }) {
  return (
    <div className="rounded-lg border border-ctp-surface1 bg-ctp-surface0/50 px-3 py-2">
      <p className="text-[10px] font-mono text-ctp-overlay0 uppercase tracking-wide">{label}</p>
      <p className="text-sm font-semibold mt-0.5 text-ctp-text" style={valueColor ? { color: valueColor } : undefined}>{value}</p>
      {sub && <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5">{sub}</p>}
    </div>
  )
}
