import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
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
} from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { useColourBlind } from '../context/ColourBlindContext'

type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'

const ROLES: { value: RoleFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'dps', label: 'DPS' },
  { value: 'healer', label: 'Healer' },
  { value: 'tank', label: 'Tank' },
]

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

function keyLevelColor(level: number): string {
  if (level >= 12) return '#cba6f7' // mauve — pushing
  if (level >= 10) return '#a6e3a1' // green — vault
  if (level >= 7)  return '#f9e2af' // yellow — middle
  if (level >= 1)  return '#fab387' // peach — low
  return '#45475a'                  // surface — none
}

const VAULT_TIERS = [
  { count: 1, label: '1 key' },
  { count: 4, label: '4 keys' },
  { count: 8, label: '8 keys' },
] as const

export function MythicPlus() {
  const { topTierColor, chartColors } = useColourBlind()
  const summary = usePlayerMplusSummary()
  const history = usePlayerMplusScoreHistory()
  const weekly = usePlayerMplusWeeklyActivity()
  const dungeons = usePlayerMplusDungeonBreakdown()
  const roster = useGuildRoster()

  const [role, setRole] = useState<RoleFilter>('all')
  const [search, setSearch] = useState('')
  const [minScore, setMinScore] = useState(1)

  const classByPlayer = useMemo(() => {
    const map = new Map<string, string>()
    for (const r of roster.data) {
      if (r.name) map.set(r.name, r.player_class || 'Unknown')
    }
    return map
  }, [roster.data])

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

  const filteredSummary = useMemo(() => {
    let rows = activeSummary.filter(r => safeNumber(r.score_all) >= minScore)
    if (role !== 'all') rows = rows.filter(r => r.role === role)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class)
      )
    }
    return [...rows].sort((a, b) => safeNumber(b.score_all) - safeNumber(a.score_all))
  }, [activeSummary, role, search, minScore])

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
    if (!activeSummary.length) return null
    const avgScore = activeSummary.reduce((s, r) => s + safeNumber(r.score_all), 0) / activeSummary.length
    const top = [...activeSummary].sort((a, b) => safeNumber(b.score_all) - safeNumber(a.score_all))[0]
    const keysThisWeek = [...currentWeekActivity.values()].reduce((s, v) => s + v, 0)
    return {
      active: activeSummary.length,
      avgScore,
      top,
      keysThisWeek,
    }
  }, [activeSummary, currentWeekActivity])

  // Vault progress this reset
  const vaultRows = useMemo(() => {
    if (!currentWeek) return []
    return activeSummary
      .map(r => {
        const runs = currentWeekActivity.get(r.player_name) ?? 0
        return { ...r, runs_this_week: runs }
      })
      .filter(r => role === 'all' || r.role === role)
      .filter(r => {
        if (!search.trim()) return true
        const q = normaliseSearchText(search)
        return matchesLooseSearch(q, r.player_name) || matchesLooseSearch(q, r.player_class)
      })
      .sort((a, b) => b.runs_this_week - a.runs_this_week)
  }, [activeSummary, currentWeekActivity, currentWeek, role, search])

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

  // Score trajectory: avg non-zero score per role per snapshot_date
  const trajectoryData = useMemo(() => {
    const playerRole = new Map(activeSummary.map(r => [r.player_name, r.role]))
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
  }, [history.data, activeSummary])

  // Push candidates: dungeon_breakdown rows where untimed key beats best timed
  const pushCandidates = useMemo(() => {
    const playerRole = new Map(activeSummary.map(r => [r.player_name, r.role]))
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
      .slice(0, 50)
  }, [dungeons.data, activeSummary, classByPlayer, role, search])

  const loading = summary.loading || history.loading || weekly.loading || dungeons.loading || roster.loading
  const error = summary.error || history.error || weekly.error || dungeons.error || roster.error

  return (
    <AppLayout title="Mythic+" subtitle="guild dashboard · score · vault · coverage · push">
      {summaryStats && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard
            label="Active Raiders"
            value={summaryStats.active}
            subValue="with non-zero current score"
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

      <Card>
        <CardHeader>
          <CardTitle>Roster Leaderboard</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Current-season Raider.IO score per player, role inferred from highest-scoring role bucket.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : error ? (
          <div className="p-5"><ErrorState message={error} /></div>
        ) : filteredSummary.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No M+ data matches the current filters.
          </div>
        ) : (
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
              {filteredSummary.map((p, i) => (
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
                  <Td right mono className="font-semibold" style={{ color: i === 0 ? topTierColor : undefined }}>
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
                  <Td right mono className="font-semibold" style={{ color: keyLevelColor(safeNumber(p.highest_timed_level)) }}>
                    +{safeNumber(p.highest_timed_level) || '—'}
                  </Td>
                  <Td className="text-xs text-ctp-overlay0">{formatDate(p.snapshot_at)}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        )}
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Vault Progress · This Reset</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Keys completed in the current week ({currentWeek ? formatDate(currentWeek) : 'no data'}).
            Vault tiers fill at 1 / 4 / 8 keys.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={6} /></div>
        ) : !currentWeek || vaultRows.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No weekly activity for the current reset.
          </div>
        ) : (
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
              {vaultRows.map(p => (
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
                            backgroundColor: filled ? '#a6e3a1' : 'transparent',
                            borderColor: filled ? '#a6e3a1' : '#45475a',
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
        )}
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Dungeon Coverage Matrix</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Best timed key per dungeon. Empty cells = never timed.
            Hover for untimed-best.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : dungeonMatrix.length === 0 || dungeonNames.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No dungeon coverage data.
          </div>
        ) : (
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
                          backgroundColor: cell.best_timed > 0 ? `${keyLevelColor(cell.best_timed)}22` : undefined,
                          color: cell.best_timed > 0 ? keyLevelColor(cell.best_timed) : '#45475a',
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
        )}
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Score Trajectory</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Average current-season score per role across active raiders, by snapshot date.
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
            <ResponsiveContainer width="100%" height={260}>
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
                <Line type="monotone" dataKey="dps"    name="DPS"    stroke="#89b4fa"               strokeWidth={1.5} dot={false} connectNulls />
                <Line type="monotone" dataKey="healer" name="Healer" stroke="#a6e3a1"               strokeWidth={1.5} dot={false} connectNulls />
                <Line type="monotone" dataKey="tank"   name="Tank"   stroke="#fab387"               strokeWidth={1.5} dot={false} connectNulls />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Push Candidates</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Players whose highest attempted key in a dungeon exceeds their best timed —
            close enough to push if reattempted with a tuned group.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={6} /></div>
        ) : pushCandidates.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No untimed runs that beat a player's best timed level.
          </div>
        ) : (
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
              {pushCandidates.map((p, i) => (
                <Tr key={`${p.player_name}-${p.dungeon}-${i}`}>
                  <Td>
                    <div className="flex items-center gap-2">
                      <ClassDot className={p.player_class} />
                      <span className="text-sm font-medium text-ctp-text">{p.player_name}</span>
                    </div>
                  </Td>
                  <Td><RoleBadge role={p.role} /></Td>
                  <Td className="text-xs text-ctp-subtext0">{p.dungeon}</Td>
                  <Td right mono style={{ color: keyLevelColor(p.highest_timed) }}>
                    {p.highest_timed > 0 ? `+${p.highest_timed}` : '—'}
                  </Td>
                  <Td right mono className="font-semibold" style={{ color: keyLevelColor(p.highest_untimed) }}>
                    +{p.highest_untimed}
                  </Td>
                  <Td right mono className="text-ctp-peach">+{p.gap}</Td>
                  <Td className="text-xs text-ctp-overlay0">{formatDate(p.latest)}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
