import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { FilterSelect } from '../components/ui/FilterSelect'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { SortableTh } from '../components/ui/SortableTh'
import { DataState } from '../components/ui/DataState'
import { FilterBar } from '../components/ui/FilterBar'
import { ProgressBar } from '../components/ui/ProgressBar'
import { SkeletonCard } from '../components/ui/LoadingState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { useBossKillRoster, useRaidSummary } from '../hooks/useGoldData'
import { formatNumber, formatDate, toFiniteNumber, hasRealText } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { isIncludedZoneName } from '../utils/zones'
import { formatThroughput, getThroughputColor, normaliseRole } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

type SortKey = 'avg_rank_percent' | 'best_rank_percent' | 'avg_throughput_per_second' | 'kills_tracked' | 'avg_item_level'
type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'
type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'

interface AggregatedPlayerRow {
  player_name: string
  player_class: string
  role: string
  primary_spec: string
  kills_tracked: number
  avg_throughput_per_second: number
  best_throughput_per_second: number
  avg_rank_percent: number
  best_rank_percent: number
  avg_item_level: number
  last_seen_date: string
}

const ROLES: { key: RoleFilter; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'dps', label: 'DPS' },
  { key: 'healer', label: 'Healer' },
  { key: 'tank', label: 'Tank' },
]

const DIFFICULTIES: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']

export function Players() {
  const { getParseColor, topTierColor } = useColourBlind()
  const raids = useRaidSummary()
  const killRoster = useBossKillRoster()
  const navigate = useNavigate()

  const [role, setRole] = useState<RoleFilter>('all')
  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [showTierHint, setShowTierHint] = useState(true)
  const [showBossHint, setShowBossHint] = useState(true)
  const [sortKey, setSortKey] = useState<SortKey>('avg_rank_percent')
  const [sortDesc, setSortDesc] = useState(true)
  const [search, setSearch] = useState('')

  const validRaidRows = useMemo(() =>
    raids.data.filter(r =>
      hasRealText(r.zone_name) &&
      isIncludedZoneName(r.zone_name) &&
      hasRealText(r.raid_night_date) &&
      hasRealText(r.primary_difficulty)
    ),
    [raids.data]
  )

  const tierOptions = useMemo(() =>
    ['All', ...new Set(
      [...validRaidRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
    )],
    [validRaidRows]
  )

  const currentTier = tierOptions[1] ?? ''

  useEffect(() => {
    if (!selectedTier && currentTier) {
      setSelectedTier(currentTier)
    }
  }, [selectedTier, currentTier])

  const tierBossRows = useMemo(() =>
    killRoster.data.filter(row =>
      isIncludedZoneName(row.zone_name) &&
      (selectedTier === 'All' || row.zone_name === selectedTier)
    ),
    [killRoster.data, selectedTier]
  )

  const bossOptions = useMemo(() => {
    const bosses = [...new Set(tierBossRows.map(row => row.boss_name).filter(hasRealText))].sort()
    return ['All', ...bosses]
  }, [tierBossRows])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) {
      setSelectedBoss('All')
    }
  }, [bossOptions, selectedBoss])

  const filteredRosterRows = useMemo(() =>
    tierBossRows.filter(row =>
      (difficulty === 'All' || row.difficulty_label === difficulty) &&
      (selectedBoss === 'All' || row.boss_name === selectedBoss)
    ),
    [tierBossRows, difficulty, selectedBoss]
  )

  const aggregatedRows = useMemo(() => {
    type Acc = AggregatedPlayerRow & {
      _rankSum: number
      _rankCount: number
      _throughputSum: number
      _throughputCount: number
      _ilvlSum: number
      _ilvlCount: number
    }
    const grouped = new Map<string, Acc>()

    for (const row of filteredRosterRows) {
      const playerName = row.player_name
      const rankPercent = toFiniteNumber(row.rank_percent)
      const throughput = toFiniteNumber(row.throughput_per_second)
      const itemLevel = toFiniteNumber(row.avg_item_level)
      const existing = grouped.get(playerName)

      if (!existing) {
        grouped.set(playerName, {
          player_name: playerName,
          player_class: row.player_class,
          role: row.role,
          primary_spec: row.spec,
          kills_tracked: 1,
          avg_throughput_per_second: throughput ?? 0,
          best_throughput_per_second: throughput ?? 0,
          avg_rank_percent: rankPercent ?? 0,
          best_rank_percent: rankPercent ?? 0,
          avg_item_level: itemLevel ?? 0,
          last_seen_date: row.raid_night_date,
          _rankSum: rankPercent ?? 0,
          _rankCount: rankPercent === null ? 0 : 1,
          _throughputSum: throughput ?? 0,
          _throughputCount: throughput === null ? 0 : 1,
          _ilvlSum: itemLevel ?? 0,
          _ilvlCount: itemLevel === null ? 0 : 1,
        })
        continue
      }

      existing.kills_tracked += 1
      if (throughput !== null) {
        existing._throughputSum += throughput
        existing._throughputCount += 1
        existing.best_throughput_per_second = Math.max(existing.best_throughput_per_second, throughput)
      }
      if (rankPercent !== null) {
        existing._rankSum += rankPercent
        existing._rankCount += 1
        existing.best_rank_percent = Math.max(existing.best_rank_percent, rankPercent)
      }
      if (itemLevel !== null) {
        existing._ilvlSum += itemLevel
        existing._ilvlCount += 1
      }
      if (String(row.raid_night_date).localeCompare(existing.last_seen_date) > 0) {
        existing.last_seen_date = row.raid_night_date
        existing.primary_spec = row.spec
        existing.player_class = row.player_class
        existing.role = row.role
      }
    }

    return [...grouped.values()].map(acc => ({
      ...acc,
      avg_throughput_per_second: acc._throughputCount ? acc._throughputSum / acc._throughputCount : 0,
      avg_rank_percent: acc._rankCount ? acc._rankSum / acc._rankCount : 0,
      avg_item_level: acc._ilvlCount ? acc._ilvlSum / acc._ilvlCount : 0,
    }))
  }, [filteredRosterRows])

  const stats = useMemo(() => {
    const withData = aggregatedRows.filter(p => p.avg_rank_percent > 0)
    const withItemLevel = aggregatedRows.filter(p => p.avg_item_level > 0)
    if (!withData.length && !withItemLevel.length && !aggregatedRows.length) return null
    const avg = withData.length
      ? withData.reduce((sum, p) => sum + p.avg_rank_percent, 0) / withData.length
      : 0
    const top = withData.length ? Math.max(...withData.map(p => p.best_rank_percent || 0)) : 0
    const avgItemLevel = withItemLevel.length
      ? withItemLevel.reduce((sum, p) => sum + p.avg_item_level, 0) / withItemLevel.length
      : 0
    const itemLevelStdDev = withItemLevel.length
      ? Math.sqrt(
          withItemLevel.reduce((sum, p) => sum + ((p.avg_item_level - avgItemLevel) ** 2), 0) / withItemLevel.length
        )
      : 0
    return { avg, top, avgItemLevel, itemLevelStdDev }
  }, [aggregatedRows])

  const rows = useMemo(() => {
    let r = aggregatedRows
    if (role !== 'all') r = r.filter(p => normaliseRole(p.role) === role)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      r = r.filter(p =>
        matchesLooseSearch(q, p.player_name) ||
        matchesLooseSearch(q, p.player_class)
      )
    }
    return [...r].sort((a, b) => {
      const NULL_LAST = sortDesc ? -Infinity : Infinity
      const av = toFiniteNumber(a[sortKey]) ?? NULL_LAST
      const bv = toFiniteNumber(b[sortKey]) ?? NULL_LAST
      return sortDesc ? bv - av : av - bv
    })
  }, [aggregatedRows, role, sortKey, sortDesc, search])

  const loading = raids.loading || killRoster.loading
  const error = raids.error || killRoster.error

  function toggleSort(k: SortKey) {
    if (sortKey === k) setSortDesc(!sortDesc)
    else {
      setSortKey(k)
      setSortDesc(true)
    }
  }

  return (
    <AppLayout title="Players" subtitle="performance rankings">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard label="Guild Item Level" value={stats ? stats.avgItemLevel.toFixed(1) : '—'} subValue="average in current scope" icon="◉" accent="blue" />
            <StatCard label="Item Level Spread" value={stats ? stats.itemLevelStdDev.toFixed(1) : '—'} subValue="standard deviation" icon="◈" accent="mauve" />
            <StatCard label="Guild Avg Parse" value={`${stats?.avg?.toFixed(1) ?? '—'}%`} subValue="WCL rank %" icon="◷" valueColor={stats ? getParseColor(stats.avg) : undefined} accent="none" />
            <StatCard label="Best Parse" value={`${stats?.top?.toFixed(0) ?? '—'}%`} subValue="filter-scope record" valueColor={stats ? topTierColor : undefined} accent="none" />
          </>
        )}
      </div>

      <FilterBar>
        <FilterTabs options={DIFFICULTIES} value={difficulty} onChange={setDifficulty} />
        <FilterSelect
          value={selectedTier}
          onChange={setSelectedTier}
          onFocus={() => setShowTierHint(false)}
          onMouseDown={() => setShowTierHint(false)}
          options={tierOptions}
          hint="tier"
          showHint={showTierHint}
          className="pl-3 pr-14 py-2 min-w-48 max-w-48"
        />
        <FilterSelect
          value={selectedBoss}
          onChange={setSelectedBoss}
          onFocus={() => setShowBossHint(false)}
          onMouseDown={() => setShowBossHint(false)}
          options={bossOptions}
          hint="boss"
          showHint={showBossHint}
          className="pl-3 pr-14 py-2 min-w-52 max-w-52"
        />
      </FilterBar>

      <Card>
        <CardHeader className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap items-center gap-3">
            <CardTitle>Rankings</CardTitle>
            <FilterTabs options={ROLES.map(r => ({ value: r.key, label: r.label }))} value={role} onChange={setRole} buttonClassName="min-w-[60px]" />
          </div>
          <div className="flex items-center gap-3 min-w-0">
            <input
              type="text"
              placeholder="Search player or class…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-36 sm:w-52"
            />
            <span className="inline-flex items-center self-center rounded-xl border border-ctp-mauve/30 bg-ctp-mauve/10 px-3 py-2 text-xs font-mono text-ctp-mauve">
              {rows.length} players
            </span>
          </div>
        </CardHeader>
        <DataState loading={loading} error={error} data={rows} empty="No players match the current filters." loadingRows={10}>
          {(data) => (
          <Table>
            <THead>
              <tr>
                <Th className="w-8">#</Th>
                <Th className="min-w-[160px]">Player</Th>
                <Th className="normal-case tracking-normal">Role</Th>
                <SortableTh right sortKey="avg_rank_percent" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Avg Parse</SortableTh>
                <SortableTh right sortKey="best_rank_percent" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Best Parse</SortableTh>
                <SortableTh right sortKey="avg_throughput_per_second" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Avg DPS/HPS</SortableTh>
                <SortableTh right sortKey="avg_item_level" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Item Level</SortableTh>
                <SortableTh right sortKey="kills_tracked" currentKey={sortKey} desc={sortDesc} onSort={toggleSort}>Kills</SortableTh>
                <Th className="normal-case tracking-normal">Last Seen</Th>
              </tr>
            </THead>
            <TBody>
              {data.map((p, i) => (
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
                      {p.best_rank_percent > 0 ? `${p.best_rank_percent.toFixed(0)}%` : '—'}
                    </span>
                  </Td>
                  <Td right mono style={{ color: getThroughputColor(p.role) }}>
                    {formatThroughput(p.avg_throughput_per_second)}
                  </Td>
                  <Td right mono className="text-ctp-overlay1">{p.avg_item_level > 0 ? p.avg_item_level.toFixed(0) : '—'}</Td>
                  <Td right mono className="text-ctp-overlay1">{formatNumber(p.kills_tracked)}</Td>
                  <Td className="text-xs text-ctp-overlay0">{formatDate(p.last_seen_date)}</Td>
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
