import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { Badge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { useGuildRoster, useLiveRaidRoster, useRaidTeam } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { getRankColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

type TabKey = 'full' | 'team'
type SortKey = 'attendance_rate_pct' | 'raids_present' | 'name' | 'rank'

interface TeamRow {
  name: string
  player_class: string
  realm: string
  rank_label: string
  rank_category: string
  is_active: string | boolean
  total_raids_tracked: number | string
  raids_present: number | string
  attendance_rate_pct: number | string
  last_raid_date: string
  first_raid_date: string
  possible_main: string
  has_possible_alt_in_logs: string | boolean
  race?: string
  note?: string
  source_refreshed_at?: string
}

export function Roster() {
  const { killColor, getAttendanceColor } = useColourBlind()
  const fullRoster = useGuildRoster()
  const liveRoster = useLiveRaidRoster()
  const raidTeam   = useRaidTeam()

  const [tab, setTab]           = useState<TabKey>('full')
  const [search, setSearch]     = useState('')
  const [sortKey, setSortKey]   = useState<SortKey>('attendance_rate_pct')
  const [sortDesc, setSortDesc] = useState(true)

  // ── Full roster ──
  const filteredFull = useMemo(() => {
    let rows = fullRoster.data
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r => matchesLooseSearch(q, r.name) || matchesLooseSearch(q, r.player_class))
    }
    return [...rows].sort((a, b) => {
      if (sortKey === 'name') {
        const r = a.name.localeCompare(b.name)
        return sortDesc ? -r : r
      }
      if (sortKey === 'rank') {
        const r = Number(a.rank) - Number(b.rank)
        return sortDesc ? -r : r
      }
      const av = Number(a[sortKey as 'attendance_rate_pct' | 'raids_present']) || 0
      const bv = Number(b[sortKey as 'attendance_rate_pct' | 'raids_present']) || 0
      return sortDesc ? bv - av : av - bv
    })
  }, [fullRoster.data, search, sortKey, sortDesc])

  // ── Raid team ──
  const teamRows = useMemo<TeamRow[]>(() => {
    if (liveRoster.data.length === 0) return raidTeam.data

    const guildByName = new Map(fullRoster.data.map(row => [row.name.toLowerCase(), row]))
    const raidByName = new Map(raidTeam.data.map(row => [row.name.toLowerCase(), row]))

    return liveRoster.data.map(row => {
      const key = row.name.toLowerCase()
      const guild = guildByName.get(key)
      const raid = raidByName.get(key)

      return {
        name: row.name,
        player_class: raid?.player_class || guild?.player_class || row.player_class || 'Unknown',
        realm: raid?.realm || guild?.realm || '',
        rank_label: row.roster_rank || raid?.rank_label || guild?.rank_label || 'Unknown',
        rank_category: row.roster_rank || raid?.rank_category || guild?.rank_category || 'Unknown',
        is_active: raid?.is_active ?? guild?.is_active ?? true,
        total_raids_tracked: raid?.total_raids_tracked ?? guild?.total_raids_tracked ?? 0,
        raids_present: raid?.raids_present ?? guild?.raids_present ?? 0,
        attendance_rate_pct: raid?.attendance_rate_pct ?? guild?.attendance_rate_pct ?? 0,
        last_raid_date: raid?.last_raid_date ?? guild?.last_raid_date ?? '',
        first_raid_date: raid?.first_raid_date ?? guild?.first_raid_date ?? '',
        possible_main: raid?.possible_main ?? '',
        has_possible_alt_in_logs: raid?.has_possible_alt_in_logs ?? false,
        race: row.race,
        note: row.note,
        source_refreshed_at: row.source_refreshed_at,
      }
    })
  }, [fullRoster.data, liveRoster.data, raidTeam.data])

  const filteredTeam = useMemo(() => {
    let rows = teamRows
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r => matchesLooseSearch(q, r.name) || matchesLooseSearch(q, r.player_class))
    }
    return [...rows].sort((a, b) => Number(b.attendance_rate_pct) - Number(a.attendance_rate_pct))
  }, [teamRows, search])

  const stats = useMemo(() => {
    const active   = fullRoster.data.filter(r => r.is_active === 'True' || r.is_active === true as unknown as string)
    const teamSize = teamRows.length
    const classes  = new Set(fullRoster.data.map(r => r.player_class)).size
    return { total: fullRoster.data.length, active: active.length, teamSize, classes }
  }, [fullRoster.data, teamRows])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-blue ml-1">{sortDesc ? '↓' : '↑'}</span>
  }

  const loading = tab === 'full' ? fullRoster.loading : (raidTeam.loading || liveRoster.loading)
  const error   = tab === 'full' ? fullRoster.error   : (raidTeam.error || liveRoster.error)
  const liveRosterActive = liveRoster.data.length > 0
  const liveRosterRefreshedAt = liveRoster.data[0]?.source_refreshed_at ?? ''

  return (
    <AppLayout title="Roster" subtitle="guild members">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Guild Size"   value={stats.total}    subValue="total members" icon="◎" />
        <StatCard label="Active"       value={stats.active}   subValue="seen in logs" icon="◉" accent="mauve" />
        <StatCard label="Raid Team"    value={stats.teamSize} subValue="ranked raiders" />
        <StatCard label="Classes"      value={stats.classes}  subValue="unique classes" />
      </div>

      {/* Tabs */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-1 bg-ctp-surface0 rounded-lg p-1 border border-ctp-surface1">
          <button
            onClick={() => setTab('full')}
            className={clsx(
              'px-3 py-2 rounded-md text-xs font-medium transition-all duration-150',
              tab === 'full' ? 'bg-ctp-blue/20 text-ctp-blue' : 'text-ctp-overlay1 hover:text-ctp-text'
            )}
          >
            Full Roster
          </button>
          <button
            onClick={() => setTab('team')}
            className={clsx(
              'px-3 py-2 rounded-md text-xs font-medium transition-all duration-150',
              tab === 'team' ? 'bg-ctp-blue/20 text-ctp-blue' : 'text-ctp-overlay1 hover:text-ctp-text'
            )}
          >
            Raid Team
          </button>
        </div>
        <span className="text-xs font-mono text-ctp-surface2">
          {tab === 'full' ? filteredFull.length : filteredTeam.length} members
        </span>
      </div>

      {/* Table */}
      {tab === 'full' ? (
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <CardTitle>Guild Roster</CardTitle>
              <input
                type="text"
                placeholder="Search name / class…"
                value={search}
                onChange={e => setSearch(e.target.value)}
                className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 w-52"
              />
            </div>
          </CardHeader>
          {loading ? (
            <CardBody><LoadingState rows={12} /></CardBody>
          ) : error ? (
            <CardBody><ErrorState message={error} /></CardBody>
          ) : (
            <Table>
              <THead>
                <tr>
                  <Th>
                    <button onClick={() => toggleSort('name')} className="hover:text-ctp-text">
                      Name <SortIcon k="name" />
                    </button>
                  </Th>
                  <Th>Class</Th>
                  <Th>Rank</Th>
                  <Th>Status</Th>
                  <Th right>
                    <button onClick={() => toggleSort('raids_present')} className="hover:text-ctp-text">
                      Raids <SortIcon k="raids_present" />
                    </button>
                  </Th>
                  <Th right>
                    <button onClick={() => toggleSort('attendance_rate_pct')} className="hover:text-ctp-text">
                      Attendance <SortIcon k="attendance_rate_pct" />
                    </button>
                  </Th>
                  <Th>Last Raid</Th>
                </tr>
              </THead>
              <TBody>
                {filteredFull.map(m => {
                  const isActive = m.is_active === 'True' || m.is_active === true as unknown as string
                  const isTeam   = m.is_raid_team === 'True' || m.is_raid_team === true as unknown as string
                  const rankColor = getRankColor(m.rank_category)
                  return (
                    <Tr key={`${m.name}-${m.realm}`}>
                      <Td>
                        <div className="flex items-center gap-2">
                          <ClassDot className={m.player_class} />
                          <span className="font-medium text-ctp-text">{m.name}</span>
                          {isTeam && (
                            <span className="text-[9px] font-mono text-ctp-mauve border border-ctp-mauve/30 rounded px-1">TEAM</span>
                          )}
                        </div>
                      </Td>
                      <Td><ClassLabel className={m.player_class} size="sm" /></Td>
                      <Td>
                        <span
                          className="text-xs font-mono font-medium"
                          style={{ color: rankColor }}
                        >
                          {m.rank_label}
                        </span>
                      </Td>
                      <Td>
                        <span className={clsx('inline-flex items-center gap-1 text-[10px] font-mono', !isActive && 'text-ctp-surface2')} style={{ color: isActive ? killColor : undefined }}>
                          <span className={clsx('w-1.5 h-1.5 rounded-full', !isActive && 'bg-ctp-surface2')} style={{ backgroundColor: isActive ? killColor : undefined }} />
                          {isActive ? 'Active' : 'Inactive'}
                        </span>
                      </Td>
                      <Td right mono className="text-ctp-overlay1">{m.raids_present || 0}</Td>
                      <Td right>
                        <div className="flex items-center justify-end gap-2">
                          {Number(m.total_raids_tracked) > 0 ? (
                            <>
                              <ProgressBar
                                value={Number(m.attendance_rate_pct)}
                                color={getAttendanceColor(Number(m.attendance_rate_pct))}
                                height="xs"
                                className="w-16"
                              />
                              <span className="text-xs font-mono w-10 text-right" style={{ color: getAttendanceColor(Number(m.attendance_rate_pct)) }}>
                                {Number(m.attendance_rate_pct).toFixed(0)}%
                              </span>
                            </>
                          ) : (
                            <span className="text-xs text-ctp-surface2">—</span>
                          )}
                        </div>
                      </Td>
                      <Td className="text-xs text-ctp-overlay0">{formatDate(m.last_raid_date)}</Td>
                    </Tr>
                  )
                })}
              </TBody>
            </Table>
          )}
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <CardTitle>{liveRosterActive ? 'Raid Team' : 'Raid Team'}</CardTitle>
                <p className="text-xs text-ctp-overlay0 mt-0.5">
                  {liveRosterActive
                    ? `Live membership from Google Sheets${liveRosterRefreshedAt ? `, refreshed ${liveRosterRefreshedAt}` : ''}. Attendance still comes from logs.`
                    : 'Rank categories: GM, Officer, Raider, Trial'}
                </p>
              </div>
              {liveRosterActive && (
                <span className="text-[10px] font-mono text-ctp-blue border border-ctp-blue/30 bg-ctp-blue/10 rounded px-2 py-1">
                  LIVE SHEET
                </span>
              )}
            </div>
            <input
              type="text"
              placeholder="Search name / class…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="mt-3 bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 w-52"
            />
          </CardHeader>
          {loading ? (
            <CardBody><LoadingState rows={12} /></CardBody>
          ) : error ? (
            <CardBody><ErrorState message={error} /></CardBody>
          ) : (
            <Table>
              <THead>
                <tr>
                  <Th>Name</Th>
                  <Th>Class</Th>
                  <Th>Rank</Th>
                  {liveRosterActive && <Th>Race</Th>}
                  <Th>Status</Th>
                  <Th right>Raids Present</Th>
                  <Th right>Attendance</Th>
                  <Th>Alt detected</Th>
                  {liveRosterActive && <Th>Note</Th>}
                  <Th>First Raid</Th>
                  <Th>Last Raid</Th>
                </tr>
              </THead>
              <TBody>
                {filteredTeam.map(m => {
                  const isActive = m.is_active === 'True' || m.is_active === true as unknown as string
                  const hasAlt   = m.has_possible_alt_in_logs === 'True' || m.has_possible_alt_in_logs === true as unknown as string
                  const rankColor = getRankColor(m.rank_category)
                  return (
                    <Tr key={`${m.name}-${m.realm}`}>
                      <Td>
                        <div className="flex items-center gap-2">
                          <ClassDot className={m.player_class} />
                          <span className="font-medium text-ctp-text">{m.name}</span>
                        </div>
                      </Td>
                      <Td><ClassLabel className={m.player_class} size="sm" /></Td>
                      <Td>
                        <span className="text-xs font-mono font-medium" style={{ color: rankColor }}>
                          {m.rank_label}
                        </span>
                      </Td>
                      {liveRosterActive && <Td className="text-xs text-ctp-overlay0">{m.race || '—'}</Td>}
                      <Td>
                        <span className={clsx('inline-flex items-center gap-1 text-[10px] font-mono', !isActive && 'text-ctp-surface2')} style={{ color: isActive ? killColor : undefined }}>
                          <span className={clsx('w-1.5 h-1.5 rounded-full', !isActive && 'bg-ctp-surface2')} style={{ backgroundColor: isActive ? killColor : undefined }} />
                          {isActive ? 'Active' : 'Inactive'}
                        </span>
                      </Td>
                      <Td right mono className="text-ctp-overlay1">{formatNumber(m.raids_present)}</Td>
                      <Td right>
                        {Number(m.total_raids_tracked) > 0 ? (
                          <div className="flex items-center justify-end gap-2">
                            <ProgressBar
                              value={Number(m.attendance_rate_pct)}
                              color={getAttendanceColor(Number(m.attendance_rate_pct))}
                              height="xs"
                              className="w-16"
                            />
                            <span className="text-xs font-mono w-10 text-right" style={{ color: getAttendanceColor(Number(m.attendance_rate_pct)) }}>
                              {Number(m.attendance_rate_pct).toFixed(0)}%
                            </span>
                          </div>
                        ) : (
                          <span className="text-xs text-ctp-surface2">—</span>
                        )}
                      </Td>
                      <Td>
                        {hasAlt ? (
                          <Badge size="sm" variant="yellow">Alt: {m.possible_main}</Badge>
                        ) : (
                          <span className="text-xs text-ctp-surface2">—</span>
                        )}
                      </Td>
                      {liveRosterActive && <Td className="text-xs text-ctp-overlay0">{m.note || '—'}</Td>}
                      <Td className="text-xs text-ctp-overlay0">{formatDate(m.first_raid_date)}</Td>
                      <Td className="text-xs text-ctp-overlay0">{formatDate(m.last_raid_date)}</Td>
                    </Tr>
                  )
                })}
              </TBody>
            </Table>
          )}
        </Card>
      )}
    </AppLayout>
  )
}
