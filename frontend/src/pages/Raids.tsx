import { useState, useMemo, useEffect } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useRaidSummary, useBossKillRoster } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

export function Raids() {
  const { killColor, wipeColor } = useColourBlind()
  const raids = useRaidSummary()
  const killRoster = useBossKillRoster()

  function getReportUrl(reportCode: string): string {
    return `https://www.warcraftlogs.com/reports/${reportCode}`
  }

  const [search,   setSearch]   = useState('')
  const [diff,     setDiff]     = useState('Mythic')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [sortDesc, setSortDesc] = useState(true)

  function hasRealText(value: unknown): value is string {
    return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
  }

  const validRaidRows = useMemo(() =>
    raids.data.filter(r =>
      hasRealText(r.report_code) &&
      hasRealText(r.zone_name) &&
      hasRealText(r.raid_night_date) &&
      hasRealText(r.primary_difficulty)
    ),
    [raids.data]
  )

  const tierOptions = useMemo(() => {
    const tiers = ['All', ...new Set(
      [...validRaidRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
    )]
    return tiers
  }, [validRaidRows])

  const currentTier = useMemo(() => tierOptions[1] ?? '', [tierOptions])

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const diffs = useMemo(() => {
    const ds = [...new Set(validRaidRows.map(r => r.primary_difficulty))].sort()
    return ['All', ...ds]
  }, [validRaidRows])

  const bossOptions = useMemo(() => {
    const bosses = [...new Set(
      killRoster.data
        .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
        .map(row => row.boss_name)
        .filter(hasRealText)
    )].sort()
    return ['All', ...bosses]
  }, [killRoster.data, selectedTier])

  const scopedReportBossStats = useMemo(() => {
    const stats = new Map<string, { bossKills: number; uniqueBossesKilled: number }>()
    const byReport = new Map<string, Set<string>>()

    killRoster.data.forEach(row => {
      if (!hasRealText(row.report_code) || !hasRealText(row.zone_name)) return
      const raid = validRaidRows.find(r => r.report_code === row.report_code)
      if (!raid || row.zone_name !== raid.zone_name) return

      const fightKey = `${row.encounter_id}-${row.fight_id}`
      const reportFightKey = `${row.report_code}:${fightKey}`
      if (!byReport.has(row.report_code)) byReport.set(row.report_code, new Set())
      const fights = byReport.get(row.report_code)!
      fights.add(reportFightKey)

      const current = stats.get(row.report_code) ?? { bossKills: 0, uniqueBossesKilled: 0 }
      current.bossKills = fights.size
      current.uniqueBossesKilled = new Set(
        killRoster.data
          .filter(r => r.report_code === row.report_code && r.zone_name === raid.zone_name)
          .map(r => r.encounter_id)
      ).size
      stats.set(row.report_code, current)
    })

    return stats
  }, [killRoster.data, validRaidRows])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const filtered = useMemo(() => {
    let rows = validRaidRows
    if (selectedTier && selectedTier !== 'All') rows = rows.filter(r => r.zone_name === selectedTier)
    if (diff !== 'All') rows = rows.filter(r => r.primary_difficulty === diff)
    if (selectedBoss !== 'All') {
      const matchingReports = new Set(
        killRoster.data
          .filter(row => selectedTier === 'All' || row.zone_name === selectedTier)
          .filter(row => diff === 'All' || row.difficulty_label === diff)
          .filter(row => row.boss_name === selectedBoss)
          .map(row => row.report_code)
      )
      rows = rows.filter(r => matchingReports.has(r.report_code))
    }
    if (search.trim()) {
      const q = search.toLowerCase()
      rows = rows.filter(r =>
        r.zone_name.toLowerCase().includes(q) ||
        r.report_title.toLowerCase().includes(q) ||
        r.raid_night_date.includes(q)
      )
    }
    return [...rows].sort((a, b) =>
      sortDesc
        ? b.raid_night_date.localeCompare(a.raid_night_date)
        : a.raid_night_date.localeCompare(b.raid_night_date)
    )
  }, [validRaidRows, selectedTier, diff, selectedBoss, search, sortDesc, killRoster.data])

  const stats = useMemo(() => {
    if (!filtered.length) return null
    const totalKills  = filtered.reduce((s, r) => s + Number(r.boss_kills), 0)
    const totalWipes  = filtered.reduce((s, r) => s + Number(r.total_wipes), 0)
    const totalSecs   = filtered.reduce((s, r) => s + Number(r.total_fight_seconds), 0)
    const avgKills    = totalKills / filtered.length
    return { totalKills, totalWipes, totalSecs, avgKills, count: filtered.length }
  }, [filtered])

  // Calendar-like view: group by zone
  const byZone = useMemo(() => {
    const m: Record<string, typeof raids.data> = {}
    filtered.forEach(r => {
      if (!m[r.zone_name]) m[r.zone_name] = []
      m[r.zone_name].push(r)
    })
    return m
  }, [filtered])

  return (
    <AppLayout title="Raids" subtitle="session history">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {raids.loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard
              label="Raid Sessions"
              value={formatNumber(stats?.count ?? 0)}
              subValue="tracked nights"
              icon="◷"
            />
            <StatCard
              label="Total Boss Kills"
              value={formatNumber(stats?.totalKills ?? 0)}
              subValue={`${stats?.avgKills.toFixed(1)} avg per night`}
              icon="⚔"
              accent="mauve"
            />
            <StatCard
              label="Total Wipes"
              value={formatNumber(stats?.totalWipes ?? 0)}
              subValue="all sessions"
              icon="✗"
              accent="red"
            />
            <StatCard
              label="Total Raid Time"
              value={formatDuration(stats?.totalSecs ?? 0)}
              subValue="fight time only"
              icon="◈"
            />
          </>
        )}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {diffs.map(d => (
            <button
              key={d}
              onClick={() => setDiff(d)}
              className={clsx(
                'px-3 py-2 rounded-lg text-xs font-medium transition-all duration-150',
                diff === d
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {d}
            </button>
          ))}
        </div>
        <select
          value={selectedTier}
          onChange={e => setSelectedTier(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-48"
        >
          {tierOptions.map(tier => <option key={tier} value={tier}>{tier}</option>)}
        </select>
        <select
          value={selectedBoss}
          onChange={e => setSelectedBoss(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-52"
        >
          {bossOptions.map(boss => <option key={boss} value={boss}>{boss}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search raid, zone, date…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-52"
        />
        <button
          onClick={() => setSortDesc(!sortDesc)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs font-mono text-ctp-overlay1 hover:text-ctp-subtext1 transition-colors"
        >
          Date {sortDesc ? '↓' : '↑'}
        </button>
        <span className="ml-auto text-xs font-mono text-ctp-overlay0">{filtered.length} sessions</span>
      </div>

      {/* Zone-grouped cards */}
      {raids.loading ? (
        <div className="space-y-6">
          {Array(3).fill(null).map((_, i) => (
            <div key={i}>
              <div className="h-4 w-40 bg-ctp-surface1 rounded-lg mb-3 animate-pulse" />
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {Array(3).fill(null).map((_, j) => <SkeletonCard key={j} />)}
              </div>
            </div>
          ))}
        </div>
      ) : raids.error ? (
        <ErrorState message={raids.error} />
      ) : (
        <div className="space-y-8 max-h-[42rem] overflow-y-auto pr-2">
          {Object.entries(byZone).map(([zoneName, zoneRaids]) => (
            <div key={zoneName}>
              <h2 className="section-label mb-3">{zoneName}</h2>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {zoneRaids.map(r => {
                  const scopedBossStats = scopedReportBossStats.get(r.report_code)

                  return (
                    <a
                      key={r.report_code}
                      href={getReportUrl(r.report_code)}
                      target="_blank"
                      rel="noreferrer"
                      className="bg-ctp-mauve/8 rounded-2xl border border-ctp-mauve/20 p-4 hover:border-ctp-mauve/35 hover:bg-ctp-mauve/10 hover:-translate-y-0.5 transition-all duration-200 shadow-card"
                    >
                      {/* Header row */}
                      <div className="flex items-start justify-between gap-2 mb-3">
                        <div>
                          <p className="text-xs font-semibold text-ctp-text leading-tight">
                            {formatDate(r.raid_night_date)}
                          </p>
                          <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5 truncate max-w-[160px]">
                            {r.report_title}
                          </p>
                        </div>
                        <DiffBadge label={r.primary_difficulty} />
                      </div>

                      {/* Stats grid */}
                      <div className="grid grid-cols-3 gap-2 mb-3">
                        <div className="text-center bg-ctp-mauve/10 rounded-xl py-1.5">
                          <p className="text-base font-semibold leading-none" style={{ color: killColor }}>{scopedBossStats?.bossKills ?? r.boss_kills}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">kills</p>
                        </div>
                        <div className="text-center bg-ctp-mauve/10 rounded-xl py-1.5">
                          <p className="text-base font-semibold leading-none" style={{ color: wipeColor }}>{r.total_wipes}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">wipes</p>
                        </div>
                        <div className="text-center bg-ctp-mauve/10 rounded-xl py-1.5">
                          <p className="text-base font-semibold text-ctp-subtext1 leading-none">{r.total_pulls}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">pulls</p>
                        </div>
                      </div>

                      {/* Duration + bosses */}
                      <div className="flex items-center justify-between text-[10px] font-mono text-ctp-overlay1">
                        <span>{formatDuration(Number(r.total_fight_seconds))} fight time</span>
                        <span>{scopedBossStats?.uniqueBossesKilled ?? r.unique_bosses_killed}/{r.unique_bosses_engaged} bosses</span>
                      </div>
                    </a>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Full table view */}
      <Card>
        <CardHeader>
          <CardTitle>All Sessions</CardTitle>
        </CardHeader>
        {raids.loading ? (
          <CardBody><LoadingState rows={8} /></CardBody>
        ) : raids.error ? (
          <CardBody><ErrorState message={raids.error} /></CardBody>
        ) : (
          <div className="max-h-[36rem] overflow-auto">
            <Table>
            <THead>
              <tr>
                <Th>Date</Th>
                <Th>Zone</Th>
                <Th>Diff</Th>
                <Th right>Kills</Th>
                <Th right>Wipes</Th>
                <Th right>Pulls</Th>
                <Th right>Bosses</Th>
                <Th right>Fight Time</Th>
                <Th>Report</Th>
              </tr>
            </THead>
            <TBody>
              {filtered.map(r => {
                const scopedBossStats = scopedReportBossStats.get(r.report_code)
                return (
                <Tr key={r.report_code}>
                  <Td className="font-medium text-ctp-text">{formatDate(r.raid_night_date)}</Td>
                  <Td className="text-ctp-overlay1 text-xs">{r.zone_name}</Td>
                  <Td><DiffBadge label={r.primary_difficulty} /></Td>
                  <Td right mono style={{ color: killColor }}>{scopedBossStats?.bossKills ?? r.boss_kills}</Td>
                  <Td right mono style={{ color: wipeColor }}>{r.total_wipes}</Td>
                  <Td right mono className="text-ctp-overlay1">{r.total_pulls}</Td>
                  <Td right mono className="text-ctp-overlay1">
                    {scopedBossStats?.uniqueBossesKilled ?? r.unique_bosses_killed}/{r.unique_bosses_engaged}
                  </Td>
                  <Td right mono className="text-ctp-overlay1">
                    {formatDuration(Number(r.total_fight_seconds))}
                  </Td>
                  <Td>
                    <a
                      href={getReportUrl(r.report_code)}
                      target="_blank"
                      rel="noreferrer"
                      className="text-[10px] font-mono text-ctp-overlay0 hover:text-ctp-mauve transition-colors"
                    >
                      {r.report_code}
                    </a>
                  </Td>
                </Tr>
              )})}
            </TBody>
            </Table>
          </div>
        )}
      </Card>
    </AppLayout>
  )
}
