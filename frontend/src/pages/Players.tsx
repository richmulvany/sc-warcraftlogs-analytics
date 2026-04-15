import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { useBossKillRoster, useRaidSummary } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
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

  function hasRealText(value: unknown): value is string {
    return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
  }

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
    const grouped = new Map<string, AggregatedPlayerRow>()

    for (const row of filteredRosterRows) {
      const playerName = row.player_name
      const rankPercent = Number(row.rank_percent) || 0
      const throughput = Number(row.throughput_per_second) || 0
      const itemLevel = Number(row.avg_item_level) || 0
      const existing = grouped.get(playerName)

      if (!existing) {
        grouped.set(playerName, {
          player_name: playerName,
          player_class: row.player_class,
          role: row.role,
          primary_spec: row.spec,
          kills_tracked: 1,
          avg_throughput_per_second: throughput,
          best_throughput_per_second: throughput,
          avg_rank_percent: rankPercent,
          best_rank_percent: rankPercent,
          avg_item_level: itemLevel,
          last_seen_date: row.raid_night_date,
        })
        continue
      }

      const nextKills = existing.kills_tracked + 1
      existing.avg_throughput_per_second = ((existing.avg_throughput_per_second * existing.kills_tracked) + throughput) / nextKills
      existing.best_throughput_per_second = Math.max(existing.best_throughput_per_second, throughput)
      existing.avg_rank_percent = ((existing.avg_rank_percent * existing.kills_tracked) + rankPercent) / nextKills
      existing.best_rank_percent = Math.max(existing.best_rank_percent, rankPercent)
      existing.avg_item_level = ((existing.avg_item_level * existing.kills_tracked) + itemLevel) / nextKills
      existing.kills_tracked = nextKills
      if (String(row.raid_night_date).localeCompare(existing.last_seen_date) > 0) {
        existing.last_seen_date = row.raid_night_date
        existing.primary_spec = row.spec
        existing.player_class = row.player_class
        existing.role = row.role
      }
    }

    return [...grouped.values()]
  }, [filteredRosterRows])

  const stats = useMemo(() => {
    const withData = aggregatedRows.filter(p => p.avg_rank_percent > 0)
    if (!withData.length) return null
    const avg = withData.reduce((sum, p) => sum + p.avg_rank_percent, 0) / withData.length
    const top = Math.max(...withData.map(p => p.best_rank_percent || 0))
    return { count: withData.length, avg, top, total: aggregatedRows.length }
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
      const av = Number(a[sortKey]) || 0
      const bv = Number(b[sortKey]) || 0
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
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard label="Players Tracked" value={stats?.total ?? 0} subValue="in current filter scope" icon="◉" accent="blue" />
            <StatCard label="With Parse Data" value={stats?.count ?? 0} subValue="min 1 kill" icon="◈" accent="mauve" />
            <StatCard label="Guild Avg Parse" value={`${stats?.avg?.toFixed(1) ?? '—'}%`} subValue="WCL rank %" icon="◷" valueColor={stats ? getParseColor(stats.avg) : undefined} accent="none" />
            <StatCard label="Best Parse" value={`${stats?.top?.toFixed(0) ?? '—'}%`} subValue="filter-scope record" valueColor={stats ? topTierColor : undefined} accent="none" />
          </>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {ROLES.map(r => (
            <button
              key={r.key}
              onClick={() => setRole(r.key)}
              className={clsx(
                'px-3 py-2 rounded-lg text-xs font-medium transition-all duration-150',
                role === r.key
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {r.label}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {DIFFICULTIES.map(option => (
            <button
              key={option}
              onClick={() => setDifficulty(option)}
              className={clsx(
                'px-3 py-2 rounded-lg text-xs font-medium transition-all duration-150',
                difficulty === option
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {option}
            </button>
          ))}
        </div>

        <div className="relative">
          {showTierHint && (
            <span className="absolute right-7 top-1/2 -translate-y-1/2 text-xs font-mono text-ctp-overlay0 pointer-events-none">
              tier
            </span>
          )}
          <select
            value={selectedTier}
            onFocus={() => setShowTierHint(false)}
            onMouseDown={() => setShowTierHint(false)}
            onChange={e => setSelectedTier(e.target.value)}
            className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl pl-3 pr-14 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-48 max-w-48"
          >
            {tierOptions.map(tier => (
              <option key={tier} value={tier}>{tier}</option>
            ))}
          </select>
        </div>

        <div className="relative">
          {showBossHint && (
            <span className="absolute right-7 top-1/2 -translate-y-1/2 text-xs font-mono text-ctp-overlay0 pointer-events-none">
              boss
            </span>
          )}
          <select
            value={selectedBoss}
            onFocus={() => setShowBossHint(false)}
            onMouseDown={() => setShowBossHint(false)}
            onChange={e => setSelectedBoss(e.target.value)}
            className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl pl-3 pr-14 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-52 max-w-52"
          >
            {bossOptions.map(boss => (
              <option key={boss} value={boss}>{boss}</option>
            ))}
          </select>
        </div>

        <input
          type="text"
          placeholder="Search player or class…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-52"
        />
      </div>

      <Card>
        <CardHeader className="flex items-start justify-between gap-3">
          <div>
            <CardTitle>All Players</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              {selectedTier || currentTier || 'No tier'} · {difficulty} · {selectedBoss}
            </p>
          </div>
          <span className="inline-flex items-center rounded-lg border border-ctp-mauve/30 bg-ctp-mauve/12 px-2 py-1 text-xs font-mono text-ctp-mauve flex-shrink-0">
            {rows.length} players
          </span>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : error ? (
          <div className="p-5"><ErrorState message={error} /></div>
        ) : rows.length === 0 ? (
          <div className="p-5 text-xs font-mono text-ctp-overlay0">No players match the current filters.</div>
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
      </Card>
    </AppLayout>
  )
}
