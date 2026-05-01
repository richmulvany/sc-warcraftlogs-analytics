import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { SortableTh } from '../components/ui/SortableTh'
import { DataState } from '../components/ui/DataState'
import { FilterBar } from '../components/ui/FilterBar'
import { ProgressBar } from '../components/ui/ProgressBar'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { usePlayerPerformance, usePlayerSurvivability } from '../hooks/useGoldData'
import { formatNumber, formatDate, toFiniteNumber, meanIgnoringNulls } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { formatThroughput, getThroughputColor, normaliseRole } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

type SortKey = 'avg_rank_percent' | 'best_rank_percent' | 'avg_throughput_per_second' | 'kills_tracked' | 'avg_item_level'
type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'

export function Performance() {
  const { getParseColor, topTierColor, wipeColor, getDeathRateColor } = useColourBlind()
  const perf = usePlayerPerformance()
  const surv = usePlayerSurvivability()

  const [roleFilter, setRoleFilter] = useState<RoleFilter>('all')
  const [sortKey, setSortKey]       = useState<SortKey>('avg_rank_percent')
  const [sortDesc, setSortDesc]     = useState(true)
  const [search, setSearch]         = useState('')

  const sorted = useMemo(() => {
    let rows = perf.data
    if (roleFilter !== 'all') rows = rows.filter(r => normaliseRole(r.role) === roleFilter)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class)
      )
    }
    return [...rows].sort((a, b) => {
      // Treat null/undefined parse fields as -Infinity when sorting desc and
      // +Infinity when sorting asc, so missing data sinks to the bottom either
      // way rather than being silently coerced to 0 (which would mid-rank it).
      const av = toFiniteNumber(a[sortKey])
      const bv = toFiniteNumber(b[sortKey])
      const aSort = av ?? (sortDesc ? -Infinity : Infinity)
      const bSort = bv ?? (sortDesc ? -Infinity : Infinity)
      return sortDesc ? bSort - aSort : aSort - bSort
    })
  }, [perf.data, roleFilter, sortKey, sortDesc, search])

  const survivMap = useMemo(() => {
    const m: Record<string, typeof surv.data[0]> = {}
    surv.data.forEach(s => { m[s.player_name] = s })
    return m
  }, [surv.data])

  const statsAll = useMemo(() => {
    // Exclude players with null/missing parse data — do not coerce nulls to 0.
    const avgParses = perf.data
      .map(p => toFiniteNumber(p.avg_rank_percent))
      .filter((v): v is number => v !== null && v > 0)
    if (!avgParses.length) return null
    const bestParses = perf.data
      .map(p => toFiniteNumber(p.best_rank_percent))
      .filter((v): v is number => v !== null)
    const avgParse = meanIgnoringNulls(avgParses)
    const topParse = bestParses.length ? Math.max(...bestParses) : 0
    const avgIlvl  = meanIgnoringNulls(perf.data.map(p => toFiniteNumber(p.avg_item_level)))
    return { avgParse, topParse, avgIlvl, count: avgParses.length }
  }, [perf.data])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  const ROLES: { key: RoleFilter; label: string }[] = [
    { key: 'all',    label: 'All'    },
    { key: 'dps',    label: 'DPS'    },
    { key: 'healer', label: 'Healer' },
    { key: 'tank',   label: 'Tank'   },
  ]

  return (
    <AppLayout title="Performance" subtitle="player rankings">
      {/* Stats */}
      {statsAll && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Players Tracked" value={statsAll.count} subValue="with parse data" icon="◉" />
          <StatCard label="Guild Avg Parse"  value={`${statsAll.avgParse.toFixed(1)}%`} subValue="WCL rank %" icon="◈" valueColor={getParseColor(statsAll.avgParse)} accent="none" />
          <StatCard label="Best Parse"       value={`${statsAll.topParse.toFixed(0)}%`} subValue="guild record" valueColor={topTierColor} accent="none" />
          <StatCard label="Avg Item Level"   value={statsAll.avgIlvl.toFixed(0)} subValue="across all players" />
        </div>
      )}

      {/* Filter row */}
      <FilterBar>
        {/* Role tabs */}
        <div className="flex items-center gap-1 bg-ctp-surface0 rounded-lg p-1 border border-ctp-surface1">
          {ROLES.map(r => (
            <button
              key={r.key}
              onClick={() => setRoleFilter(r.key)}
              className={clsx(
                'px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150',
                roleFilter === r.key
                  ? 'bg-ctp-mauve/20 text-ctp-mauve'
                  : 'text-ctp-overlay1 hover:text-ctp-text'
              )}
            >
              {r.label}
            </button>
          ))}
        </div>

        {/* Search */}
        <input
          type="text"
          placeholder="Search player or class…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 w-56"
        />

        <span className="text-xs font-mono text-ctp-surface2 ml-auto">{sorted.length} players</span>
      </FilterBar>

      {/* Player rankings table */}
      <Card>
        <CardHeader>
          <CardTitle>Player Rankings</CardTitle>
        </CardHeader>
        <DataState loading={perf.loading} error={perf.error} data={sorted} loadingRows={10}>
          {(data) => (
          <Table>
            <THead>
              <tr>
                <Th className="w-8">#</Th>
                <Th>Player</Th>
                <Th>Role</Th>
                <SortableTh right sortKey="avg_rank_percent" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Avg Parse</SortableTh>
                <SortableTh right sortKey="best_rank_percent" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Best Parse</SortableTh>
                <SortableTh right sortKey="avg_throughput_per_second" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Avg DPS/HPS</SortableTh>
                <SortableTh right sortKey="avg_item_level" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Avg ilvl</SortableTh>
                <SortableTh right sortKey="kills_tracked" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Kills</SortableTh>
                <Th>Deaths</Th>
                <Th>Last Seen</Th>
              </tr>
            </THead>
            <TBody>
              {data.map((p, i) => {
                const sv = survivMap[p.player_name]
                const avgThroughput = toFiniteNumber(p.avg_throughput_per_second)
                return (
                  <Tr key={`${p.player_identity_key}:${p.role}`}>
                    <Td mono className="text-ctp-surface2 text-xs">{i + 1}</Td>
                    <Td>
                      <div className="flex items-center gap-2">
                        <ClassDot className={p.player_class} />
                        <div>
                          <p className="text-sm font-medium text-ctp-text">{p.player_name}</p>
                          <ClassLabel className={p.player_class} spec={p.primary_spec} size="sm" />
                        </div>
                      </div>
                    </Td>
                    <Td><RoleBadge role={p.role} /></Td>
                    <Td right>
                      <div className="flex items-center justify-end gap-2">
                        <ProgressBar
                          value={p.avg_rank_percent || 0}
                          color={getParseColor(p.avg_rank_percent)}
                          height="xs"
                          className="w-16"
                        />
                        <span
                          className="text-xs font-mono font-semibold w-10 text-right"
                          style={{ color: getParseColor(p.avg_rank_percent) }}
                        >
                          {p.avg_rank_percent ? `${p.avg_rank_percent.toFixed(1)}%` : '—'}
                        </span>
                      </div>
                    </Td>
                    <Td right>
                      <span
                        className="text-xs font-mono font-semibold"
                        style={{ color: getParseColor(p.best_rank_percent) }}
                      >
                        {p.best_rank_percent ? `${p.best_rank_percent.toFixed(0)}%` : '—'}
                      </span>
                    </Td>
                    <Td right mono style={{ color: getThroughputColor(p.role) }}>
                      {avgThroughput !== null ? formatThroughput(avgThroughput) : '—'}
                    </Td>
                    <Td right mono className="text-ctp-overlay1">
                      {p.avg_item_level ? p.avg_item_level.toFixed(0) : '—'}
                    </Td>
                    <Td right mono className="text-ctp-overlay1">
                      {formatNumber(p.kills_tracked)}
                    </Td>
                    <Td>
                      {sv ? (
                        <span className="text-xs font-mono" style={{ color: wipeColor }}>
                          {formatNumber(sv.total_deaths)} total
                        </span>
                      ) : (
                        <span className="text-xs text-ctp-surface2">—</span>
                      )}
                    </Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(p.last_seen_date)}</Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
          )}
        </DataState>
      </Card>

      {/* Survivability section */}
      <Card>
        <CardHeader>
          <CardTitle>Survivability</CardTitle>
          <p className="text-xs text-ctp-overlay0 mt-0.5">Death counts and most common killing blows</p>
        </CardHeader>
        <DataState loading={surv.loading} error={surv.error} data={surv.data} loadingRows={6}>
          {() => (
          <Table>
            <THead>
              <tr>
                <Th>Player</Th>
                <Th right>Total Deaths</Th>
                <Th right>Deaths / Kill</Th>
                <Th>Most Common Killing Blow</Th>
                <Th right>Count</Th>
              </tr>
            </THead>
            <TBody>
              {[...surv.data]
                .filter(s => s.total_deaths > 0)
                .sort((a, b) => b.deaths_per_kill - a.deaths_per_kill)
                .slice(0, 20)
                .map(s => (
                  <Tr key={s.player_name}>
                    <Td>
                      <div className="flex items-center gap-2">
                        <ClassDot className={s.player_class} />
                        <div>
                          <p className="text-sm text-ctp-text">{s.player_name}</p>
                          <ClassLabel className={s.player_class} size="sm" />
                        </div>
                      </div>
                    </Td>
                    <Td right mono style={{ color: wipeColor }}>{formatNumber(s.total_deaths)}</Td>
                    <Td right mono style={{ color: getDeathRateColor(toFiniteNumber(s.deaths_per_kill) ?? 0) }}>
                      {s.deaths_per_kill ? s.deaths_per_kill.toFixed(1) : '—'}
                    </Td>
                    <Td className="text-xs text-ctp-overlay1 max-w-[200px] truncate">
                      {s.most_common_killing_blow || '—'}
                    </Td>
                    <Td right mono className="text-ctp-overlay0">{s.most_common_killing_blow_count || '—'}</Td>
                  </Tr>
                ))}
            </TBody>
          </Table>
          )}
        </DataState>
      </Card>
    </AppLayout>
  )
}
