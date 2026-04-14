import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useRaidSummary } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

export function Raids() {
  const { killColor, wipeColor } = useColourBlind()
  const raids = useRaidSummary()

  const [search,   setSearch]   = useState('')
  const [diff,     setDiff]     = useState('All')
  const [sortDesc, setSortDesc] = useState(true)


  const diffs = useMemo(() => {
    const ds = [...new Set(raids.data.map(r => r.primary_difficulty))].sort()
    return ['All', ...ds]
  }, [raids.data])

  const filtered = useMemo(() => {
    let rows = raids.data
    if (diff !== 'All') rows = rows.filter(r => r.primary_difficulty === diff)
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
  }, [raids.data, diff, search, sortDesc])

  const stats = useMemo(() => {
    if (!raids.data.length) return null
    const totalKills  = raids.data.reduce((s, r) => s + Number(r.boss_kills), 0)
    const totalWipes  = raids.data.reduce((s, r) => s + Number(r.total_wipes), 0)
    const totalSecs   = raids.data.reduce((s, r) => s + Number(r.total_fight_seconds), 0)
    const avgKills    = totalKills / raids.data.length
    return { totalKills, totalWipes, totalSecs, avgKills, count: raids.data.length }
  }, [raids.data])

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
              accent="blue"
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
              accent="teal"
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
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-150',
                diff === d
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {d}
            </button>
          ))}
        </div>
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
        <div className="space-y-8">
          {Object.entries(byZone).map(([zoneName, zoneRaids]) => (
            <div key={zoneName}>
              <h2 className="section-label mb-3">{zoneName}</h2>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {zoneRaids.map(r => {


                  return (
                    <div
                      key={r.report_code}
                      className="bg-ctp-surface0 rounded-2xl border border-ctp-surface1 p-4 hover:border-ctp-surface2 hover:-translate-y-0.5 transition-all duration-200 shadow-card"
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
                        <div className="text-center bg-ctp-surface1/40 rounded-xl py-1.5">
                          <p className="text-base font-semibold leading-none" style={{ color: killColor }}>{r.boss_kills}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">kills</p>
                        </div>
                        <div className="text-center bg-ctp-surface1/40 rounded-xl py-1.5">
                          <p className="text-base font-semibold leading-none" style={{ color: wipeColor }}>{r.total_wipes}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">wipes</p>
                        </div>
                        <div className="text-center bg-ctp-surface1/40 rounded-xl py-1.5">
                          <p className="text-base font-semibold text-ctp-subtext1 leading-none">{r.total_pulls}</p>
                          <p className="text-[9px] font-mono text-ctp-overlay0 mt-0.5">pulls</p>
                        </div>
                      </div>

                      {/* Duration + bosses */}
                      <div className="flex items-center justify-between text-[10px] font-mono text-ctp-overlay1">
                        <span>{formatDuration(Number(r.total_fight_seconds))} fight time</span>
                        <span>{r.unique_bosses_killed}/{r.unique_bosses_engaged} bosses</span>
                      </div>
                    </div>
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
              {filtered.map(r => (
                <Tr key={r.report_code}>
                  <Td className="font-medium text-ctp-text">{formatDate(r.raid_night_date)}</Td>
                  <Td className="text-ctp-overlay1 text-xs">{r.zone_name}</Td>
                  <Td><DiffBadge label={r.primary_difficulty} /></Td>
                  <Td right mono style={{ color: killColor }}>{r.boss_kills}</Td>
                  <Td right mono style={{ color: wipeColor }}>{r.total_wipes}</Td>
                  <Td right mono className="text-ctp-overlay1">{r.total_pulls}</Td>
                  <Td right mono className="text-ctp-overlay1">
                    {r.unique_bosses_killed}/{r.unique_bosses_engaged}
                  </Td>
                  <Td right mono className="text-ctp-overlay1">
                    {formatDuration(Number(r.total_fight_seconds))}
                  </Td>
                  <Td>
                    <span className="text-[10px] font-mono text-ctp-overlay0">{r.report_code}</span>
                  </Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
