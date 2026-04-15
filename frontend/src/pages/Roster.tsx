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
import { useGuildRoster, useRaidTeam } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { getRankColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

type TabKey = 'full' | 'team'
type SortKey = 'attendance_rate_pct' | 'raids_present' | 'name' | 'rank'

export function Roster() {
  const { killColor, getAttendanceColor } = useColourBlind()
  const fullRoster = useGuildRoster()
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
  const filteredTeam = useMemo(() => {
    let rows = raidTeam.data
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r => matchesLooseSearch(q, r.name) || matchesLooseSearch(q, r.player_class))
    }
    return [...rows].sort((a, b) => Number(b.attendance_rate_pct) - Number(a.attendance_rate_pct))
  }, [raidTeam.data, search])

  const stats = useMemo(() => {
    const active   = fullRoster.data.filter(r => r.is_active === 'True' || r.is_active === true as unknown as string)
    const teamSize = raidTeam.data.length
    const classes  = new Set(fullRoster.data.map(r => r.player_class)).size
    return { total: fullRoster.data.length, active: active.length, teamSize, classes }
  }, [fullRoster.data, raidTeam.data])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-mauve ml-1">{sortDesc ? '↓' : '↑'}</span>
  }

  const loading = tab === 'full' ? fullRoster.loading : raidTeam.loading
  const error   = tab === 'full' ? fullRoster.error   : raidTeam.error

  return (
    <AppLayout title="Roster" subtitle="guild members">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Guild Size"   value={stats.total}    subValue="total members" icon="◎" />
        <StatCard label="Active"       value={stats.active}   subValue="seen in logs" icon="◉" accent="mauve" />
        <StatCard label="Raid Team"    value={stats.teamSize} subValue="ranked raiders" />
        <StatCard label="Classes"      value={stats.classes}  subValue="unique classes" />
      </div>

      {/* Tabs + search */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-1 bg-ctp-surface0 rounded-lg p-1 border border-ctp-surface1">
          <button
            onClick={() => setTab('full')}
            className={clsx(
              'px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150',
              tab === 'full' ? 'bg-ctp-mauve/20 text-ctp-mauve' : 'text-ctp-overlay1 hover:text-ctp-text'
            )}
          >
            Full Roster
          </button>
          <button
            onClick={() => setTab('team')}
            className={clsx(
              'px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150',
              tab === 'team' ? 'bg-ctp-mauve/20 text-ctp-mauve' : 'text-ctp-overlay1 hover:text-ctp-text'
            )}
          >
            Raid Team
          </button>
        </div>
        <input
          type="text"
          placeholder="Search name / class…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 w-48"
        />
        <span className="text-xs font-mono text-ctp-surface2 ml-auto">
          {tab === 'full' ? filteredFull.length : filteredTeam.length} members
        </span>
      </div>

      {/* Table */}
      {tab === 'full' ? (
        <Card>
          <CardHeader>
            <CardTitle>Guild Roster</CardTitle>
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
            <CardTitle>Raid Team</CardTitle>
            <p className="text-xs text-ctp-overlay0 mt-0.5">Rank categories: GM, Officer, Raider, Trial</p>
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
                  <Th>Status</Th>
                  <Th right>Raids Present</Th>
                  <Th right>Attendance</Th>
                  <Th>Alt detected</Th>
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
