import { useState, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { usePlayerPerformance } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatThroughput } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

type SortKey = 'avg_rank_percent' | 'best_rank_percent' | 'avg_throughput_per_second' | 'kills_tracked' | 'avg_item_level'
type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'

const ROLES: { key: RoleFilter; label: string }[] = [
  { key: 'all',    label: 'All'    },
  { key: 'dps',    label: 'DPS'    },
  { key: 'healer', label: 'Healer' },
  { key: 'tank',   label: 'Tank'   },
]

export function Players() {
  const { getParseColor } = useColourBlind()
  const perf     = usePlayerPerformance()
  const navigate = useNavigate()

  const [role,     setRole]     = useState<RoleFilter>('all')
  const [sortKey,  setSortKey]  = useState<SortKey>('avg_rank_percent')
  const [sortDesc, setSortDesc] = useState(true)
  const [search,   setSearch]   = useState('')

  const stats = useMemo(() => {
    const withData = perf.data.filter(p => p.avg_rank_percent > 0)
    if (!withData.length) return null
    const avg = withData.reduce((s, p) => s + p.avg_rank_percent, 0) / withData.length
    const top = Math.max(...withData.map(p => p.best_rank_percent))
    return { count: withData.length, avg, top, total: perf.data.length }
  }, [perf.data])

  const rows = useMemo(() => {
    let r = perf.data
    if (role !== 'all') r = r.filter(p => p.role === role)
    if (search.trim()) {
      const q = search.toLowerCase()
      r = r.filter(p =>
        p.player_name.toLowerCase().includes(q) ||
        p.player_class.toLowerCase().includes(q) ||
        p.primary_spec.toLowerCase().includes(q)
      )
    }
    return [...r].sort((a, b) => {
      const av = Number(a[sortKey]) || 0
      const bv = Number(b[sortKey]) || 0
      return sortDesc ? bv - av : av - bv
    })
  }, [perf.data, role, sortKey, sortDesc, search])

  function toggleSort(k: SortKey) {
    if (sortKey === k) setSortDesc(!sortDesc)
    else { setSortKey(k); setSortDesc(true) }
  }

  function SortBtn({ k, children }: { k: SortKey; children: React.ReactNode }) {
    return (
      <button
        onClick={() => toggleSort(k)}
        className="hover:text-ctp-subtext1 transition-colors flex items-center gap-1"
      >
        {children}
        {sortKey === k
          ? <span className="text-ctp-mauve">{sortDesc ? '↓' : '↑'}</span>
          : <span className="text-ctp-surface2">↕</span>}
      </button>
    )
  }

  return (
    <AppLayout title="Players" subtitle="performance rankings">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {perf.loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard label="Players Tracked" value={stats?.total ?? 0}            subValue="in logs"          icon="◉" accent="blue" />
            <StatCard label="With Parse Data"  value={stats?.count ?? 0}            subValue="min 1 kill"       icon="◈" accent="mauve" />
            <StatCard label="Guild Avg Parse"  value={`${stats?.avg?.toFixed(1) ?? '—'}%`} subValue="WCL rank %" icon="◷" accent="peach" />
            <StatCard label="Best Parse"       value={`${stats?.top?.toFixed(0) ?? '—'}%`} subValue="guild record"          accent="green" />
          </>
        )}
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3">
        {/* Role tabs */}
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {ROLES.map(r => (
            <button
              key={r.key}
              onClick={() => setRole(r.key)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-150',
                role === r.key
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {r.label}
            </button>
          ))}
        </div>
        <input
          type="text"
          placeholder="Search player, class, spec…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-52"
        />
        <span className="ml-auto text-xs font-mono text-ctp-overlay0">{rows.length} players</span>
      </div>

      {/* Table */}
      <Card>
        <CardHeader>
          <CardTitle>All Players</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">Click a row to see the full player profile</p>
        </CardHeader>
        {perf.loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : perf.error ? (
          <div className="p-5"><ErrorState message={perf.error} /></div>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th className="w-8">#</Th>
                <Th>Player</Th>
                <Th>Role</Th>
                <Th right><SortBtn k="avg_rank_percent">Avg Parse</SortBtn></Th>
                <Th right><SortBtn k="best_rank_percent">Best Parse</SortBtn></Th>
                <Th right><SortBtn k="avg_throughput_per_second">Avg DPS/HPS</SortBtn></Th>
                <Th right><SortBtn k="avg_item_level">Avg iLvl</SortBtn></Th>
                <Th right><SortBtn k="kills_tracked">Kills</SortBtn></Th>
                <Th>Last Seen</Th>
              </tr>
            </THead>
            <TBody>
              {rows.map((p, i) => (
                <Tr key={p.player_name} onClick={() => navigate(`/players/${encodeURIComponent(p.player_name)}`)}>
                  <Td mono className="text-ctp-overlay0 text-xs">{i + 1}</Td>
                  <Td>
                    <div className="flex items-center gap-2.5">
                      <ClassDot className={p.player_class} />
                      <div>
                        <p className="text-sm font-medium text-ctp-text">{p.player_name}</p>
                        <ClassLabel className={p.player_class} spec={p.primary_spec} size="xs" />
                      </div>
                    </div>
                  </Td>
                  <Td><RoleBadge role={p.role} /></Td>
                  <Td right>
                    {p.avg_rank_percent > 0 ? (
                      <div className="flex items-center justify-end gap-2">
                        <ProgressBar value={p.avg_rank_percent} color={getParseColor(p.avg_rank_percent)} height="xs" className="w-14" />
                        <span className="text-xs font-mono font-semibold w-10 text-right" style={{ color: getParseColor(p.avg_rank_percent) }}>
                          {p.avg_rank_percent.toFixed(1)}%
                        </span>
                      </div>
                    ) : <span className="text-ctp-overlay0 text-xs">—</span>}
                  </Td>
                  <Td right>
                    <span className="text-xs font-mono font-semibold" style={{ color: getParseColor(p.best_rank_percent) }}>
                      {p.best_rank_percent ? `${p.best_rank_percent.toFixed(0)}%` : '—'}
                    </span>
                  </Td>
                  <Td right mono className="text-ctp-subtext1">{formatThroughput(p.avg_throughput_per_second)}</Td>
                  <Td right mono className="text-ctp-overlay1">{p.avg_item_level ? p.avg_item_level.toFixed(0) : '—'}</Td>
                  <Td right mono className="text-ctp-overlay1">{formatNumber(p.kills_tracked)}</Td>
                  <Td className="text-xs text-ctp-overlay0">{formatDate(p.last_seen_date)}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
