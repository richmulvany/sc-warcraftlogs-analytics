import { useEffect, useMemo, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
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
import { useBossMechanics, usePlayerDeathEvents, useBossWipeAnalysis, useBossKillRoster } from '../hooks/useGoldData'
import { formatDate, formatNumber, formatPct } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CtpTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl">
      {label ? <p className="text-ctp-overlay1 mb-2">{label}</p> : null}
      {payload.map((p: { name: string; value: number; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}: <span className="font-semibold">{typeof p.value === 'number' ? formatNumber(p.value) : p.value}</span>
        </p>
      ))}
    </div>
  )
}

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal']
type SurvivabilityBasis = 'kill' | 'pull'
type SurvivabilitySortKey = 'deathRate' | 'deaths' | 'attempts'
type SortDirection = 'asc' | 'desc'
const SURVIVABILITY_BASIS_OPTIONS: readonly { value: SurvivabilityBasis; label: string }[] = [
  { value: 'kill', label: 'Kills' },
  { value: 'pull', label: 'Pulls' },
]

interface ScopedSurvivabilityRow {
  player_name: string
  player_class: string
  total_deaths: number
  kills_tracked: number
  pulls_tracked: number
  deaths_per_kill: number | null
  deaths_per_pull: number | null
  most_common_killing_blow: string
}

export function WipeAnalysis() {
  const { wipeColor, phaseColors, chartColors, getDeathRateColor, getParseColor, topTierColor } = useColourBlind()
  const mechs = useBossMechanics()
  const deathEvents = usePlayerDeathEvents()
  const wipes = useBossWipeAnalysis()
  const roster = useBossKillRoster()

  const [diff, setDiff] = useState('Mythic')
  const [selectedTier, setSelectedTier] = useState('All')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [search, setSearch] = useState('')
  const [survivabilityBasis, setSurvivabilityBasis] = useState<SurvivabilityBasis>('kill')
  const [survivabilitySort, setSurvivabilitySort] = useState<{ key: SurvivabilitySortKey; direction: SortDirection }>({
    key: 'deathRate',
    direction: 'asc',
  })

  const tierOptions = useMemo(() => {
    const values = [...wipes.data]
      .sort((a, b) => String(b.latest_wipe_date ?? '').localeCompare(String(a.latest_wipe_date ?? '')))
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
    const values = [...new Set(
      wipes.data
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .filter(row => isIncludedZoneName(row.zone_name))
        .map(row => row.boss_name)
        .filter(Boolean)
    )].sort()
    return ['All', ...values]
  }, [wipes.data, selectedTier])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const filteredWipes = useMemo(() =>
    wipes.data
      .filter(row => diff === 'All' || row.difficulty_label === diff)
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
      .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
      .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes)),
    [wipes.data, diff, selectedTier, selectedBoss, search]
  )

  const mechanicsMap = useMemo(() => {
    const map = new Map<string, typeof mechs.data[number]>()
    for (const row of mechs.data) {
      map.set(`${row.encounter_id}-${row.difficulty}`, row)
    }
    return map
  }, [mechs.data])

  const filteredMechanics = useMemo(() =>
    mechs.data
      .filter(row => diff === 'All' || row.difficulty_label === diff)
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
      .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
      .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes)),
    [mechs.data, diff, selectedTier, selectedBoss, search]
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

  const scopedDeathRows = useMemo(() =>
    deathEvents.data
      .filter(row => diff === 'All' || row.difficulty_label === diff)
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
      .filter(row => selectedBoss === 'All' || row.boss_name === selectedBoss)
      .filter(row => !search.trim() || row.boss_name.toLowerCase().includes(search.toLowerCase())),
    [deathEvents.data, diff, selectedTier, selectedBoss, search]
  )

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
        kills_tracked: 0,
        pulls_tracked: 0,
        deaths_per_kill: null,
        deaths_per_pull: null,
        most_common_killing_blow: '',
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
      if (!row.killing_blow_name) continue
      if (!killingBlowsByPlayer.has(row.player_name)) killingBlowsByPlayer.set(row.player_name, new Map())
      const playerBlows = killingBlowsByPlayer.get(row.player_name)!
      playerBlows.set(row.killing_blow_name, (playerBlows.get(row.killing_blow_name) ?? 0) + 1)
    }

    killingBlowsByPlayer.forEach((counts, playerName) => {
      const top = [...counts.entries()].sort((a, b) => b[1] - a[1])[0]
      const row = rows.get(playerName)
      if (row && top) row.most_common_killing_blow = top[0]
    })

    return [...rows.values()].map(row => ({
      ...row,
      deaths_per_kill: row.kills_tracked > 0 ? row.total_deaths / row.kills_tracked : null,
      pulls_tracked: row.kills_tracked + row.pulls_tracked,
      deaths_per_pull: (row.kills_tracked + row.pulls_tracked) > 0
        ? row.total_deaths / (row.kills_tracked + row.pulls_tracked)
        : null,
    }))
  }, [scopedPlayerNames, scopedDeathRows, roster.data, selectedTier, diff, selectedBoss, search])

  const playersWithTrackedKills = useMemo(() =>
    scopedSurvivability.filter(row => Number(row.kills_tracked) > 0),
    [scopedSurvivability]
  )

  const stats = useMemo(() => {
    const totalWipes = filteredWipes.reduce((sum, row) => sum + Number(row.total_wipes), 0)
    const totalDeaths = scopedDeathRows.length
    const earlyWipes = filteredMechanics.reduce((sum, row) => sum + Number(row.wipes_lt_1min), 0)
    const closestPull = [...filteredWipes]
      .filter(row => Number(row.best_wipe_pct) > 0)
      .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))[0]
    const topBlocker = [...filteredWipes]
      .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes))[0]
    const avgDeathsPerKill = playersWithTrackedKills.length
      ? playersWithTrackedKills.reduce((sum, row) => sum + Number(row.deaths_per_kill ?? 0), 0) / playersWithTrackedKills.length
      : 0

    return {
      totalWipes,
      totalDeaths,
      earlyWipes,
      avgDeathsPerKill,
      bossesInScope: filteredWipes.length,
      closestPull,
      topBlocker,
    }
  }, [filteredWipes, filteredMechanics, scopedDeathRows, playersWithTrackedKills])

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
      { label: '1-3 min', wipes: totals.one3, fill: phaseColors[1] },
      { label: '3-5 min', wipes: totals.three5, fill: phaseColors[2] },
      { label: '5+ min', wipes: totals.gt5, fill: phaseColors[3] },
    ]
  }, [filteredMechanics, phaseColors])

  const topWipeBosses = useMemo(() =>
    filteredWipes.slice(0, 10).map(row => ({
      boss: row.boss_name.length > 20 ? `${row.boss_name.slice(0, 19)}…` : row.boss_name,
      wipes: Number(row.total_wipes),
      bestPct: Number(row.best_wipe_pct),
      fullName: row.boss_name,
      diff: row.difficulty_label,
    })),
    [filteredWipes]
  )

  const progressRows = useMemo(() =>
    filteredWipes
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
    [filteredWipes, mechanicsMap]
  )

  const progressCurrentColor = topTierColor
  const progressPreviousColor = chartColors.secondary !== progressCurrentColor
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
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([name, count]) => ({
        name: name.length > 24 ? `${name.slice(0, 23)}…` : name,
        fullName: name,
        count,
      }))
  }, [scopedDeathRows])

  const playerRows = useMemo(() => {
    const direction = survivabilitySort.direction === 'asc' ? 1 : -1
    return [...scopedSurvivability]
      .sort((a, b) => {
        const av = survivabilitySort.key === 'deathRate'
          ? ((survivabilityBasis === 'kill' ? a.deaths_per_kill : a.deaths_per_pull) ?? Number.POSITIVE_INFINITY)
          : survivabilitySort.key === 'deaths'
            ? a.total_deaths
            : survivabilityBasis === 'kill' ? a.kills_tracked : a.pulls_tracked
        const bv = survivabilitySort.key === 'deathRate'
          ? ((survivabilityBasis === 'kill' ? b.deaths_per_kill : b.deaths_per_pull) ?? Number.POSITIVE_INFINITY)
          : survivabilitySort.key === 'deaths'
            ? b.total_deaths
            : survivabilityBasis === 'kill' ? b.kills_tracked : b.pulls_tracked
        return ((av - bv) * direction) || a.player_name.localeCompare(b.player_name)
      })
      .slice(0, 5)
  }, [scopedSurvivability, survivabilitySort, survivabilityBasis])

  function sortSurvivabilityBy(key: SurvivabilitySortKey) {
    setSurvivabilitySort(current =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: 'asc' }
    )
  }

  const loading = mechs.loading || deathEvents.loading || wipes.loading || roster.loading
  const error = mechs.error || deathEvents.error || wipes.error || roster.error
  const hasBossData = filteredWipes.length > 0

  return (
    <AppLayout title="Wipe Analysis" subtitle="where progression stalls and what tends to kill raids">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4"><ErrorState message={error} /></div>
        ) : (
          <>
            <StatCard
              label="Bosses In Scope"
              value={stats.bossesInScope}
              subValue={diff === 'All' ? 'all tracked difficulties' : diff}
              icon="◈"
              accent="mauve"
            />
            <StatCard
              label="Total Wipes"
              value={formatNumber(stats.totalWipes)}
              subValue={selectedTier === 'All' ? 'all tiers in scope' : selectedTier}
              icon="✗"
              valueColor={wipeColor}
              accent="none"
            />
            <StatCard
              label="Closest Pull"
              value={stats.closestPull ? formatPct(stats.closestPull.best_wipe_pct) : '—'}
              subValue={stats.closestPull?.boss_name ?? 'no wipes in scope'}
              valueColor={stats.closestPull ? getParseColor(100 - Number(stats.closestPull.best_wipe_pct)) : undefined}
              accent="none"
            />
            <StatCard
              label="Early Wipes"
              value={formatNumber(stats.earlyWipes)}
              subValue={stats.totalWipes ? `${((stats.earlyWipes / stats.totalWipes) * 100).toFixed(0)}% of wipes` : 'no wipe data'}
              valueColor={wipeColor}
              accent="none"
            />
          </>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <FilterTabs options={DIFFS} value={diff} onChange={setDiff} />
        <FilterSelect value={selectedTier} onChange={setSelectedTier} options={tierOptions} className="min-w-48" />
        <FilterSelect value={selectedBoss} onChange={setSelectedBoss} options={bossOptions} className="min-w-52" />
        <input
          type="text"
          placeholder="Filter boss name…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-48"
        />
      </div>

      {!loading && !error && !hasBossData ? (
        <Card>
          <CardBody>
            <p className="text-xs text-ctp-overlay0 font-mono text-center py-10">
              No wipe-analysis rows match the current filters.
            </p>
          </CardBody>
        </Card>
      ) : null}

      {hasBossData && (
        <>
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
            <Card className="xl:col-span-2">
              <CardHeader>
                <CardTitle>Top Wipe Walls</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Bosses generating the most wipes in the current scope. Best boss % shows how close the best pull got.
                </p>
              </CardHeader>
              <CardBody>
                {wipes.loading ? <LoadingState rows={4} /> : (
                  <ResponsiveContainer width="100%" height={280}>
                    <BarChart data={topWipeBosses} margin={{ top: 4, right: 8, left: -20, bottom: 68 }}>
                      <XAxis
                        dataKey="boss"
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                        tickMargin={10}
                        angle={-35}
                        textAnchor="end"
                        interval={0}
                      />
                      <YAxis
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                      <Bar dataKey="wipes" name="Wipes" radius={[4, 4, 0, 0]}>
                        {topWipeBosses.map((row, index) => (
                          <Cell key={index} fill={row.bestPct > 0 ? getParseColor(100 - row.bestPct) : chartColors.secondary} fillOpacity={0.9} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Current Read</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">Quick interpretation of the filtered wipe set</p>
              </CardHeader>
              <CardBody className="space-y-4">
                <div className="rounded-xl bg-ctp-surface1/40 p-3">
                  <p className="section-label mb-1">Top Blocker</p>
                  <p className="text-sm font-semibold text-ctp-text">{stats.topBlocker?.boss_name ?? '—'}</p>
                  <p className="text-xs text-ctp-overlay0 mt-1">
                    {stats.topBlocker ? `${formatNumber(stats.topBlocker.total_wipes)} wipes across ${stats.topBlocker.raid_nights_attempted} raid nights` : 'No wipe data'}
                  </p>
                </div>
                <div className="rounded-xl bg-ctp-surface1/40 p-3">
                  <p className="section-label mb-1">Best Recovery Point</p>
                  <p className="text-sm font-semibold" style={{ color: stats.closestPull ? getParseColor(100 - Number(stats.closestPull.best_wipe_pct)) : topTierColor }}>
                    {stats.closestPull ? `${stats.closestPull.boss_name} · ${formatPct(stats.closestPull.best_wipe_pct)}` : '—'}
                  </p>
                  <p className="text-xs text-ctp-overlay0 mt-1">
                    {stats.closestPull ? `${stats.closestPull.difficulty_label} · ${formatDuration(Number(stats.closestPull.longest_wipe_seconds))} longest pull` : 'No qualifying pull'}
                  </p>
                </div>
                <div className="rounded-xl bg-ctp-surface1/40 p-3">
                  <p className="section-label mb-1">Guild Survivability</p>
                  <p className="text-sm font-semibold" style={{ color: playersWithTrackedKills.length ? getDeathRateColor(stats.avgDeathsPerKill) : undefined }}>
                    {playersWithTrackedKills.length ? stats.avgDeathsPerKill.toFixed(1) : '—'}
                  </p>
                  <p className="text-xs text-ctp-overlay0 mt-1">
                    Average deaths per kill across {playersWithTrackedKills.length} players with tracked kills
                  </p>
                </div>
              </CardBody>
            </Card>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Wipe Duration Breakdown</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Early wipes usually indicate opener or positioning failures. Long wipes suggest later-phase execution issues.
                </p>
              </CardHeader>
              <CardBody>
                {mechs.loading ? <LoadingState rows={4} /> : (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={durationBuckets} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                      <XAxis
                        dataKey="label"
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
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
                )}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Progress Snapshot</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Lower boss HP is better. Trend compares current average wipe HP to the previous week when available.
                </p>
              </CardHeader>
              <CardBody>
                {wipes.loading ? <LoadingState rows={4} /> : (
                  <>
                    <div className="mb-3 flex flex-wrap items-center justify-end gap-4 text-[11px] font-mono">
                      <span className="inline-flex items-center gap-1.5" style={{ color: progressPreviousColor }}>
                        <span className="h-2 w-2 rounded-full" style={{ backgroundColor: progressPreviousColor }} />
                        Last Week Avg %
                      </span>
                      <span className="inline-flex items-center gap-1.5" style={{ color: progressCurrentColor }}>
                        <span className="h-2 w-2 rounded-full" style={{ backgroundColor: progressCurrentColor }} />
                        Current Avg %
                      </span>
                    </div>
                    <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={progressRows} margin={{ top: 4, right: 8, left: -20, bottom: 70 }}>
                      <XAxis
                        dataKey="boss_name"
                        tickFormatter={(value: string) => value.length > 14 ? `${value.slice(0, 13)}…` : value}
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                        tickMargin={10}
                        angle={-35}
                        textAnchor="end"
                        interval={0}
                      />
                      <YAxis
                        tickFormatter={value => `${value}%`}
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                      <Bar dataKey="lastWeek" name="Last Week Avg %" radius={[4, 4, 0, 0]} fill={progressPreviousColor} fillOpacity={0.45} />
                      <Bar dataKey="avgBossPct" name="Current Avg %" radius={[4, 4, 0, 0]} fill={progressCurrentColor} fillOpacity={0.9} />
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
              <p className="text-xs text-ctp-overlay1 mt-0.5">Best pull, average wipe HP, duration, phase reached, and raid-night spread for each boss in scope</p>
            </CardHeader>
            {wipes.loading ? (
              <CardBody><LoadingState rows={8} /></CardBody>
            ) : (
              <Table>
                <THead>
                  <tr>
                    <Th>Boss</Th>
                    <Th>Zone</Th>
                    <Th>Diff</Th>
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
                  {progressRows.map(row => (
                    <Tr key={`${row.encounter_id}-${row.difficulty}`}>
                      <Td className="font-medium text-ctp-text">{row.boss_name}</Td>
                      <Td className="text-ctp-overlay1 text-xs max-w-[150px] truncate">{row.zone_name}</Td>
                      <Td><DiffBadge label={row.difficulty_label} /></Td>
                      <Td right mono style={{ color: wipeColor }}>{formatNumber(row.total_wipes)}</Td>
                      <Td right mono style={{ color: getParseColor(100 - Number(row.best_wipe_pct)) }}>{formatPct(row.best_wipe_pct)}</Td>
                      <Td right mono className="text-ctp-overlay1">{formatPct(row.avgBossPct)}</Td>
                      <Td right mono className="text-ctp-overlay1">{row.maxPhase || '—'}</Td>
                      <Td right mono className="text-ctp-overlay1">{formatDuration(Number(row.avg_wipe_duration_seconds))}</Td>
                      <Td right mono style={{ color: Number(row.trend) < 0 ? topTierColor : wipeColor }}>
                        {Number(row.trend) > 0 ? '+' : ''}{Number(row.trend).toFixed(1)}%
                      </Td>
                      <Td right mono className="text-ctp-overlay1">{formatNumber(row.raid_nights_attempted)}</Td>
                      <Td className="text-xs text-ctp-overlay0">{formatDate(row.latest_wipe_date)}</Td>
                    </Tr>
                  ))}
                </TBody>
              </Table>
            )}
          </Card>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Most Common Killing Blows</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Death events in the current filter scope only.
                </p>
              </CardHeader>
              <CardBody>
                {deathEvents.loading ? <LoadingState rows={5} /> : killingBlows.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={killingBlows} layout="vertical" margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                      <XAxis
                        type="number"
                        tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        dataKey="name"
                        type="category"
                        width={150}
                        tick={{ fontSize: 10, fill: '#a6adc8', fontFamily: 'IBM Plex Mono, monospace' }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                      <Bar dataKey="count" name="Deaths" radius={[0, 4, 4, 0]} fill={wipeColor} fillOpacity={0.85} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No survivability rows match the current analysis scope</p>
                )}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <CardTitle>Player Survivability</CardTitle>
                    <p className="text-xs text-ctp-overlay1 mt-0.5">
                      Top 5 in the current filter scope. Click metric headers to change order.
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
                <CardBody><LoadingState rows={6} /></CardBody>
              ) : (
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
                          Deaths{survivabilitySort.key === 'deaths' ? (survivabilitySort.direction === 'asc' ? ' ↑' : ' ↓') : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('attempts')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          {survivabilityBasis === 'kill' ? 'Kills' : 'Pulls'}{survivabilitySort.key === 'attempts' ? (survivabilitySort.direction === 'asc' ? ' ↑' : ' ↓') : ''}
                        </button>
                      </Th>
                      <Th right>
                        <button
                          type="button"
                          onClick={() => sortSurvivabilityBy('deathRate')}
                          className="transition-colors hover:text-ctp-mauve"
                        >
                          Deaths/{survivabilityBasis === 'kill' ? 'Kill' : 'Pull'}{survivabilitySort.key === 'deathRate' ? (survivabilitySort.direction === 'asc' ? ' ↑' : ' ↓') : ''}
                        </button>
                      </Th>
                      <Th>Most Killed By</Th>
                    </tr>
                  </THead>
                  <TBody>
                    {playerRows.map(row => (
                      <Tr key={row.player_name}>
                        <Td>
                          <div className="flex items-center gap-2">
                            <ClassDot className={row.player_class} />
                            <span className="text-xs font-medium text-ctp-text">{row.player_name}</span>
                          </div>
                        </Td>
                        <Td right mono style={{ color: wipeColor }}>{formatNumber(row.total_deaths)}</Td>
                        <Td right mono className="text-ctp-overlay1">
                          {formatNumber(survivabilityBasis === 'kill' ? row.kills_tracked : row.pulls_tracked)}
                        </Td>
                        <Td
                          right
                          mono
                          style={(survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull) != null
                            ? { color: getDeathRateColor((survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull) ?? 0) }
                            : undefined}
                          className={(survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull) == null ? 'text-ctp-overlay0' : undefined}
                        >
                          {(survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull) != null
                            ? ((survivabilityBasis === 'kill' ? row.deaths_per_kill : row.deaths_per_pull) ?? 0).toFixed(1)
                            : '—'}
                        </Td>
                        <Td className="text-[10px] font-mono text-ctp-overlay0 max-w-[180px] truncate">
                          {row.most_common_killing_blow || '—'}
                        </Td>
                      </Tr>
                    ))}
                  </TBody>
                </Table>
              )}
            </Card>
          </div>
        </>
      )}
    </AppLayout>
  )
}
