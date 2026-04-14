import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { usePlayerAttendance } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { useColourBlind } from '../context/ColourBlindContext'

type SortKey = 'attendance_rate_pct' | 'raids_present' | 'total_raids_tracked'

export function Attendance() {
  const { killColor, wipeColor, getAttendanceColor, topTierColor } = useColourBlind()
  const att = usePlayerAttendance()
  const [search, setSearch]     = useState('')
  const [sortKey, setSortKey]   = useState<SortKey>('attendance_rate_pct')
  const [sortDesc, setSortDesc] = useState(true)
  const [minRaids, setMinRaids] = useState(1)

  const sorted = useMemo(() => {
    let rows = att.data.filter(r => Number(r.total_raids_tracked) >= minRaids)
    if (search.trim()) {
      const q = search.toLowerCase()
      rows = rows.filter(r => r.player_name.toLowerCase().includes(q))
    }
    return [...rows].sort((a, b) => {
      const av = Number(a[sortKey]) || 0
      const bv = Number(b[sortKey]) || 0
      return sortDesc ? bv - av : av - bv
    })
  }, [att.data, search, sortKey, sortDesc, minRaids])

  const stats = useMemo(() => {
    const rows = att.data.filter(r => Number(r.total_raids_tracked) > 0)
    if (!rows.length) return null
    const avgAtt = rows.reduce((s, r) => s + Number(r.attendance_rate_pct), 0) / rows.length
    const perfect = rows.filter(r => Number(r.attendance_rate_pct) === 100).length
    const tracked = Math.max(...rows.map(r => Number(r.total_raids_tracked)))
    return { avgAtt, perfect, tracked, total: rows.length }
  }, [att.data])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-mauve ml-1">{sortDesc ? '↓' : '↑'}</span>
  }

  return (
    <AppLayout title="Attendance" subtitle="raid participation">
      {/* Stats */}
      {stats && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Players Tracked" value={stats.total} subValue="unique raiders" icon="◎" />
          <StatCard label="Guild Avg Attendance" value={`${stats.avgAtt.toFixed(1)}%`} subValue="participation rate" icon="◷" valueColor={getAttendanceColor(stats.avgAtt)} accent="none" />
          <StatCard label="Perfect Attendance" value={stats.perfect} subValue="100% raid rate" accent="green" />
          <StatCard label="Raids Tracked" value={stats.tracked} subValue="maximum for any player" />
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <input
          type="text"
          placeholder="Search player…"
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

      {/* Attendance table */}
      <Card>
        <CardHeader>
          <CardTitle>Attendance Breakdown</CardTitle>
        </CardHeader>
        {att.loading ? (
          <CardBody><LoadingState rows={10} /></CardBody>
        ) : att.error ? (
          <CardBody><ErrorState message={att.error} /></CardBody>
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
                    Total <SortIcon k="total_raids_tracked" />
                  </button>
                </Th>
                <Th right>Benched</Th>
                <Th right>Absent</Th>
                <Th>First Raid</Th>
                <Th>Last Raid</Th>
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
                      <ProgressBar
                        value={pct}
                        color={color}
                        height="sm"
                        showLabel={false}
                      />
                    </Td>
                    <Td right mono style={{ color: killColor }}>{formatNumber(p.raids_present)}</Td>
                    <Td right mono className="text-ctp-overlay1">{formatNumber(p.total_raids_tracked)}</Td>
                    <Td right mono style={{ color: topTierColor, opacity: 0.7 }}>{p.raids_benched || 0}</Td>
                    <Td right mono style={{ color: wipeColor }}>{p.raids_absent || 0}</Td>
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
