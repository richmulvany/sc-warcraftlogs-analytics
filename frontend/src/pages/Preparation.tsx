import { useMemo, useState } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { usePlayerConsumables, usePlayerCombatStats } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { normaliseRole } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'
type SortKey = 'composite' | 'potion_use_rate' | 'healthstone_use_rate' | 'kills_tracked'

const ROLES: { value: RoleFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'dps', label: 'DPS' },
  { value: 'healer', label: 'Healer' },
  { value: 'tank', label: 'Tank' },
]

const STAT_KEYS = [
  { key: 'crit',    label: 'Crit',    latest: 'latest_crit_rating',        avg: 'avg_crit_rating' },
  { key: 'haste',   label: 'Haste',   latest: 'latest_haste_rating',       avg: 'avg_haste_rating' },
  { key: 'mastery', label: 'Mastery', latest: 'latest_mastery_rating',     avg: 'avg_mastery_rating' },
  { key: 'vers',    label: 'Vers',    latest: 'latest_versatility_rating', avg: 'avg_versatility_rating' },
] as const

function pctColor(pct: number): string {
  if (pct >= 90) return '#a6e3a1' // green
  if (pct >= 60) return '#f9e2af' // yellow
  if (pct >= 30) return '#fab387' // peach
  return '#f38ba8'                // red
}

function safeNumber(value: unknown): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

interface ConsumableRow {
  player_name: string
  player_class: string
  role: string
  kills_tracked: number
  potion_use_rate: number
  healthstone_use_rate: number
  composite: number
}

interface SpecOutlier {
  spec: string
  cohortSize: number
  mean: number
  stdev: number
}

export function Preparation() {
  const { topTierColor } = useColourBlind()
  const consumables = usePlayerConsumables()
  const stats = usePlayerCombatStats()

  const [role, setRole] = useState<RoleFilter>('all')
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('composite')
  const [sortDesc, setSortDesc] = useState(true)
  const [minKills, setMinKills] = useState(3)

  const consumableRows: ConsumableRow[] = useMemo(() => {
    return consumables.data
      .filter(r => r.player_name && safeNumber(r.kills_tracked) > 0)
      .map(r => {
        const potion = safeNumber(r.potion_use_rate) * 100
        const hs = safeNumber(r.healthstone_use_rate) * 100
        return {
          player_name: r.player_name,
          player_class: r.player_class || 'Unknown',
          role: normaliseRole(r.role),
          kills_tracked: safeNumber(r.kills_tracked),
          potion_use_rate: potion,
          healthstone_use_rate: hs,
          composite: (potion + hs) / 2,
        }
      })
  }, [consumables.data])

  const filteredConsumables = useMemo(() => {
    let rows = consumableRows.filter(r => r.kills_tracked >= minKills)
    if (role !== 'all') rows = rows.filter(r => r.role === role)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class)
      )
    }
    return [...rows].sort((a, b) => {
      const av = safeNumber(a[sortKey])
      const bv = safeNumber(b[sortKey])
      return sortDesc ? bv - av : av - bv
    })
  }, [consumableRows, role, search, sortKey, sortDesc, minKills])

  const consumableSummary = useMemo(() => {
    const rows = consumableRows.filter(r => r.kills_tracked >= minKills)
    if (!rows.length) return null
    const avgPotion = rows.reduce((s, r) => s + r.potion_use_rate, 0) / rows.length
    const avgHs = rows.reduce((s, r) => s + r.healthstone_use_rate, 0) / rows.length
    const detected = rows.filter(r => r.potion_use_rate > 0 || r.healthstone_use_rate > 0).length
    return { players: rows.length, avgPotion, avgHs, detected }
  }, [consumableRows, minKills])

  // Build per-spec outlier baselines for stat distribution.
  const specBaselines = useMemo(() => {
    const map = new Map<string, Record<string, SpecOutlier>>()
    const bySpec = new Map<string, typeof stats.data>()
    for (const r of stats.data) {
      const spec = r.spec || 'Unknown'
      if (!bySpec.has(spec)) bySpec.set(spec, [])
      bySpec.get(spec)!.push(r)
    }
    for (const [spec, rows] of bySpec) {
      const cohortSize = rows.length
      const baselines: Record<string, SpecOutlier> = {}
      for (const stat of STAT_KEYS) {
        const values = rows.map(r => safeNumber((r as any)[stat.latest])).filter(v => v > 0)
        if (!values.length) continue
        const mean = values.reduce((s, v) => s + v, 0) / values.length
        const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / values.length
        baselines[stat.key] = { spec, cohortSize, mean, stdev: Math.sqrt(variance) }
      }
      map.set(spec, baselines)
    }
    return map
  }, [stats.data])

  const filteredStats = useMemo(() => {
    let rows = stats.data.filter(r => r.player_name && safeNumber(r.kills_tracked) >= minKills)
    if (role !== 'all') rows = rows.filter(r => normaliseRole(r.role) === role)
    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(r =>
        matchesLooseSearch(q, r.player_name) ||
        matchesLooseSearch(q, r.player_class) ||
        matchesLooseSearch(q, r.spec)
      )
    }
    return [...rows].sort((a, b) =>
      safeNumber(b.latest_avg_item_level) - safeNumber(a.latest_avg_item_level)
    )
  }, [stats.data, role, search, minKills])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-blue ml-1">{sortDesc ? '↓' : '↑'}</span>
  }

  function statValueBadge(value: number, baseline: SpecOutlier | undefined) {
    if (!value) return <span className="text-ctp-overlay0">—</span>
    if (!baseline || baseline.cohortSize < 3 || baseline.stdev === 0) {
      return <span className="font-mono text-ctp-subtext1">{formatNumber(value)}</span>
    }
    const z = (value - baseline.mean) / baseline.stdev
    if (Math.abs(z) >= 1.5) {
      const color = z > 0 ? topTierColor : '#fab387'
      return (
        <span className="font-mono font-semibold" style={{ color }} title={`${z > 0 ? '+' : ''}${z.toFixed(1)}σ vs ${baseline.spec} mean (${formatNumber(Math.round(baseline.mean))})`}>
          {formatNumber(value)}
        </span>
      )
    }
    return <span className="font-mono text-ctp-subtext1">{formatNumber(value)}</span>
  }

  const loading = consumables.loading || stats.loading
  const error = consumables.error || stats.error

  return (
    <AppLayout title="Preparation" subtitle="raid readiness · consumables · stat distribution">
      {consumableSummary && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard
            label="Players Tracked"
            value={consumableSummary.players}
            subValue={`min ${minKills} kills tracked`}
            accent="none"
          />
          <StatCard
            label="Avg Potion Use"
            value={`${consumableSummary.avgPotion.toFixed(1)}%`}
            subValue="per kill, across players"
            valueColor={pctColor(consumableSummary.avgPotion)}
            accent="none"
          />
          <StatCard
            label="Avg Healthstone"
            value={`${consumableSummary.avgHs.toFixed(1)}%`}
            subValue="per kill, across players"
            valueColor={pctColor(consumableSummary.avgHs)}
            accent="none"
          />
          <StatCard
            label="With Any Detection"
            value={consumableSummary.detected}
            subValue={consumableSummary.detected === 0 ? 'no usage detected — see note below' : 'players with ≥1 use'}
            accent={consumableSummary.detected === 0 ? 'red' : 'green'}
          />
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <FilterTabs options={ROLES} value={role} onChange={setRole} />
        <input
          type="text"
          placeholder="Search player, class or spec…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-blue/40 w-56"
        />
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono text-ctp-overlay0">Min kills:</span>
          <select
            value={minKills}
            onChange={e => setMinKills(Number(e.target.value))}
            className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-2 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
          >
            {[1, 3, 5, 10, 20].map(n => <option key={n} value={n}>{n}+</option>)}
          </select>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Consumables Compliance</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Per-kill rate of potion and healthstone use, across all tracked boss kills.
            Composite is the average of the two.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : error ? (
          <div className="p-5"><ErrorState message={error} /></div>
        ) : filteredConsumables.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No consumables data matches the current filters.
          </div>
        ) : (
          <>
            {consumableSummary?.detected === 0 && (
              <div className="px-5 pt-3 text-[11px] font-mono text-ctp-peach">
                ⚠ No consumable usage detected in any tracked kill. The export currently
                reports zeros — likely a bronze/silver detection gap rather than guild behaviour.
                The page renders against whatever data is exported; values will populate once
                detection lands upstream.
              </div>
            )}
            <Table>
              <THead>
                <tr>
                  <Th>#</Th>
                  <Th>Player</Th>
                  <Th>Role</Th>
                  <Th right>
                    <button onClick={() => toggleSort('kills_tracked')} className="hover:text-ctp-text">
                      Kills <SortIcon k="kills_tracked" />
                    </button>
                  </Th>
                  <Th right className="w-32">
                    <button onClick={() => toggleSort('potion_use_rate')} className="hover:text-ctp-text">
                      Potion % <SortIcon k="potion_use_rate" />
                    </button>
                  </Th>
                  <Th right className="w-32">
                    <button onClick={() => toggleSort('healthstone_use_rate')} className="hover:text-ctp-text">
                      Healthstone % <SortIcon k="healthstone_use_rate" />
                    </button>
                  </Th>
                  <Th right className="w-40">
                    <button onClick={() => toggleSort('composite')} className="hover:text-ctp-text">
                      Composite <SortIcon k="composite" />
                    </button>
                  </Th>
                </tr>
              </THead>
              <TBody>
                {filteredConsumables.map((p, i) => (
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
                    <Td><RoleBadge role={p.role} /></Td>
                    <Td right mono className="text-ctp-overlay1">{formatNumber(p.kills_tracked)}</Td>
                    <Td right>
                      <span className="font-mono text-xs" style={{ color: pctColor(p.potion_use_rate) }}>
                        {p.potion_use_rate.toFixed(1)}%
                      </span>
                    </Td>
                    <Td right>
                      <span className="font-mono text-xs" style={{ color: pctColor(p.healthstone_use_rate) }}>
                        {p.healthstone_use_rate.toFixed(1)}%
                      </span>
                    </Td>
                    <Td>
                      <ProgressBar value={p.composite} color={pctColor(p.composite)} height="sm" />
                    </Td>
                  </Tr>
                ))}
              </TBody>
            </Table>
          </>
        )}
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Combat Stat Distribution</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Latest secondary stat ratings per player. Highlighted values are
            ≥1.5σ from the same-spec mean (gold = above, peach = below).
            Hover a highlighted value to see the cohort baseline.
          </p>
        </CardHeader>
        {loading ? (
          <div className="p-5"><LoadingState rows={10} /></div>
        ) : error ? (
          <div className="p-5"><ErrorState message={error} /></div>
        ) : filteredStats.length === 0 ? (
          <div className="p-5 text-xs text-ctp-overlay0 font-mono text-center py-8">
            No stat data matches the current filters.
          </div>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>#</Th>
                <Th>Player</Th>
                <Th>Spec</Th>
                <Th right>iLvl</Th>
                {STAT_KEYS.map(s => <Th key={s.key} right>{s.label}</Th>)}
                <Th right>Kills</Th>
                <Th>Last Kill</Th>
              </tr>
            </THead>
            <TBody>
              {filteredStats.map((p, i) => {
                const baselines = specBaselines.get(p.spec || 'Unknown') || {}
                return (
                  <Tr key={`${p.player_name}-${p.spec}`}>
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
                    <Td className="text-xs text-ctp-subtext0">{p.spec || '—'}</Td>
                    <Td right mono className="text-ctp-subtext1">{Math.round(safeNumber(p.latest_avg_item_level)) || '—'}</Td>
                    {STAT_KEYS.map(s => (
                      <Td key={s.key} right>
                        {statValueBadge(safeNumber((p as any)[s.latest]), baselines[s.key])}
                      </Td>
                    ))}
                    <Td right mono className="text-ctp-overlay1">{formatNumber(p.kills_tracked)}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(p.latest_kill_date)}</Td>
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
