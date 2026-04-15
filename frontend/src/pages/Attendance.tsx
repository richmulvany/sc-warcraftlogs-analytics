import { useEffect, useMemo, useState } from 'react'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { usePlayerAttendance, useRaidSummary, useBossKillRoster } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { useColourBlind } from '../context/ColourBlindContext'

type SortKey = 'attendance_rate_pct' | 'raids_present' | 'total_raids_tracked'
type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'

interface ScopedAttendanceRow {
  player_name: string
  player_class: string
  raids_present: number
  total_raids_tracked: number
  raids_absent: number
  attendance_rate_pct: number
  first_raid_date: string
  last_raid_date: string
}

const DIFFICULTIES: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']

export function Attendance() {
  const { killColor, wipeColor, getAttendanceColor, topTierColor } = useColourBlind()
  const att = usePlayerAttendance()
  const raids = useRaidSummary()
  const killRoster = useBossKillRoster()

  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('attendance_rate_pct')
  const [sortDesc, setSortDesc] = useState(true)
  const [minRaids, setMinRaids] = useState(1)
  const [difficulty, setDifficulty] = useState<DifficultyFilter>('All')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')

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

  const tierOptions = useMemo(() =>
    [...new Set(
      [...validRaidRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
    )],
    [validRaidRows]
  )

  const currentTier = tierOptions[0] ?? ''

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const bossOptions = useMemo(() => {
    const bosses = [...new Set(
      killRoster.data
        .filter(row => row.zone_name === selectedTier)
        .map(row => row.boss_name)
        .filter(hasRealText)
    )].sort()
    return ['All', ...bosses]
  }, [killRoster.data, selectedTier])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const filteredSessions = useMemo(() => {
    let rows = validRaidRows
    if (selectedTier) rows = rows.filter(r => r.zone_name === selectedTier)
    if (difficulty !== 'All') rows = rows.filter(r => r.primary_difficulty === difficulty)
    if (selectedBoss !== 'All') {
      const matchingReports = new Set(
        killRoster.data
          .filter(row => row.zone_name === selectedTier)
          .filter(row => difficulty === 'All' || row.difficulty_label === difficulty)
          .filter(row => row.boss_name === selectedBoss)
          .map(row => row.report_code)
      )
      rows = rows.filter(r => matchingReports.has(r.report_code))
    }
    return rows
  }, [validRaidRows, selectedTier, difficulty, selectedBoss, killRoster.data])

  const scopedRows = useMemo(() => {
    const sessionCount = filteredSessions.length
    if (sessionCount === 0) return [] as ScopedAttendanceRow[]

    const attendanceClassMap = new Map(att.data.map(row => [row.player_name, row.player_class]))
    const grouped = new Map<string, { reports: Set<string>; first: string; last: string; player_class: string }>()

    for (const row of killRoster.data) {
      if (selectedTier && row.zone_name !== selectedTier) continue
      if (difficulty !== 'All' && row.difficulty_label !== difficulty) continue
      if (selectedBoss !== 'All' && row.boss_name !== selectedBoss) continue
      if (!filteredSessions.some(session => session.report_code === row.report_code)) continue

      const existing = grouped.get(row.player_name)
      if (!existing) {
        grouped.set(row.player_name, {
          reports: new Set([row.report_code]),
          first: row.raid_night_date,
          last: row.raid_night_date,
          player_class: attendanceClassMap.get(row.player_name) ?? row.player_class,
        })
        continue
      }

      existing.reports.add(row.report_code)
      if (row.raid_night_date.localeCompare(existing.first) < 0) existing.first = row.raid_night_date
      if (row.raid_night_date.localeCompare(existing.last) > 0) existing.last = row.raid_night_date
      if (!existing.player_class || existing.player_class === 'Unknown') {
        existing.player_class = attendanceClassMap.get(row.player_name) ?? row.player_class
      }
    }

    return [...grouped.entries()].map(([player_name, info]) => {
      const raids_present = info.reports.size
      const total_raids_tracked = sessionCount
      const raids_absent = Math.max(total_raids_tracked - raids_present, 0)
      const attendance_rate_pct = total_raids_tracked > 0 ? (raids_present / total_raids_tracked) * 100 : 0

      return {
        player_name,
        player_class: info.player_class || 'Unknown',
        raids_present,
        total_raids_tracked,
        raids_absent,
        attendance_rate_pct,
        first_raid_date: info.first,
        last_raid_date: info.last,
      }
    })
  }, [filteredSessions, killRoster.data, selectedTier, difficulty, selectedBoss, att.data])

  const sorted = useMemo(() => {
    let rows = scopedRows.filter(r => Number(r.raids_present) >= minRaids)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class)
      )
    }
    return [...rows].sort((a, b) => {
      const av = Number(a[sortKey]) || 0
      const bv = Number(b[sortKey]) || 0
      return sortDesc ? bv - av : av - bv
    })
  }, [scopedRows, search, sortKey, sortDesc, minRaids])

  const stats = useMemo(() => {
    const rows = scopedRows.filter(r => Number(r.total_raids_tracked) > 0)
    if (!rows.length) return null
    const avgAtt = rows.reduce((sum, row) => sum + row.attendance_rate_pct, 0) / rows.length
    const perfect = rows.filter(row => row.attendance_rate_pct === 100).length
    return { avgAtt, perfect, tracked: filteredSessions.length, total: rows.length }
  }, [scopedRows, filteredSessions.length])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-mauve ml-1">{sortDesc ? '↓' : '↑'}</span>
  }

  const loading = att.loading || raids.loading || killRoster.loading
  const error = att.error || raids.error || killRoster.error

  return (
    <AppLayout title="Attendance" subtitle="raid participation">
      {stats && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Players In Scope" value={stats.total} subValue="seen in matching raids" icon="◎" />
          <StatCard label="Avg Attendance" value={`${stats.avgAtt.toFixed(1)}%`} subValue="within current scope" icon="◷" valueColor={getAttendanceColor(stats.avgAtt)} accent="none" />
          <StatCard label="Perfect Attendance" value={stats.perfect} subValue="100% of scoped sessions" accent="green" />
          <StatCard label="Sessions In Scope" value={stats.tracked} subValue="matching raid reports" />
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {DIFFICULTIES.map(option => (
            <button
              key={option}
              onClick={() => setDifficulty(option)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-150',
                difficulty === option
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {option}
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
          placeholder="Search player or class…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 w-48"
        />
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono text-ctp-overlay0">Min raids:</span>
          <select
            value={minRaids}
            onChange={e => setMinRaids(Number(e.target.value))}
            className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-2 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40"
          >
            {[1, 2, 3, 5, 10].map(n => <option key={n} value={n}>{n}+</option>)}
          </select>
        </div>
        <span className="text-xs font-mono text-ctp-surface2 ml-auto">{sorted.length} players</span>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Attendance Breakdown</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Scoped from matching raid sessions and boss-roster presence. Missed = sessions in scope without a recorded appearance for players who showed up at least once.
          </p>
        </CardHeader>
        {loading ? (
          <CardBody><LoadingState rows={10} /></CardBody>
        ) : error ? (
          <CardBody><ErrorState message={error} /></CardBody>
        ) : sorted.length === 0 ? (
          <CardBody>
            <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No attendance rows match the current scope.</p>
          </CardBody>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>#</Th>
                <Th>Player</Th>
                <Th right>
                  <button onClick={() => toggleSort('attendance_rate_pct')} className="hover:text-ctp-text">
                    Rate <SortIcon k="attendance_rate_pct" />
                  </button>
                </Th>
                <Th className="w-40">Attendance</Th>
                <Th right>
                  <button onClick={() => toggleSort('raids_present')} className="hover:text-ctp-text">
                    Present <SortIcon k="raids_present" />
                  </button>
                </Th>
                <Th right>
                  <button onClick={() => toggleSort('total_raids_tracked')} className="hover:text-ctp-text">
                    Sessions <SortIcon k="total_raids_tracked" />
                  </button>
                </Th>
                <Th right>Missed</Th>
                <Th>First Seen</Th>
                <Th>Last Seen</Th>
              </tr>
            </THead>
            <TBody>
              {sorted.map((p, i) => {
                const pct = Number(p.attendance_rate_pct) || 0
                const color = getAttendanceColor(pct)
                return (
                  <Tr key={p.player_name}>
                    <Td mono className="text-ctp-surface2 text-xs">{i + 1}</Td>
                    <Td>
                      <div className="flex items-center gap-2">
                        <ClassDot className={p.player_class} />
                        <div>
                          <p className="text-sm font-medium text-ctp-text">{p.player_name}</p>
                          <ClassLabel className={p.player_class} size="sm" />
                        </div>
                      </div>
                    </Td>
                    <Td right>
                      <span className="text-sm font-mono font-semibold" style={{ color }}>
                        {pct.toFixed(1)}%
                      </span>
                    </Td>
                    <Td>
                      <ProgressBar value={pct} color={color} height="sm" showLabel={false} />
                    </Td>
                    <Td right mono style={{ color: killColor }}>{formatNumber(p.raids_present)}</Td>
                    <Td right mono className="text-ctp-overlay1">{formatNumber(p.total_raids_tracked)}</Td>
                    <Td right mono style={{ color: pct < 100 ? wipeColor : topTierColor, opacity: pct < 100 ? 1 : 0.7 }}>
                      {formatNumber(p.raids_absent)}
                    </Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(p.first_raid_date)}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(p.last_raid_date)}</Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
