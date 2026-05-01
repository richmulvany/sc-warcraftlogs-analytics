import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { AlertTriangle, BarChart3, ShieldCheck, Users } from 'lucide-react'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardBody, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { Badge, RoleBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot, ClassLabel } from '../components/ui/ClassLabel'
import { useLiveRaidRoster, usePreparationOverrides, usePreparationReadiness } from '../hooks/useGoldData'
import { formatDate, formatNumber, safeNumber } from '../utils/format'
import { matchesLooseSearch, normaliseSearchText } from '../utils/search'
import { useColourBlind } from '../context/ColourBlindContext'
import type { PreparationOverrideRow, PreparationReadiness } from '../types'

type RoleFilter = 'all' | 'dps' | 'healer' | 'tank'
type HealthFilter = 'all' | 'watch' | 'steady' | 'strong'
type SortKey = 'readiness' | 'attendance' | 'food' | 'flask' | 'weapon' | 'potion' | 'kills'

const ROLES: { value: RoleFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'dps', label: 'DPS' },
  { value: 'healer', label: 'Healer' },
  { value: 'tank', label: 'Tank' },
]

const HEALTH_STATES: { value: HealthFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'watch', label: 'Watchlist' },
  { value: 'steady', label: 'Steady' },
  { value: 'strong', label: 'Strong' },
]

interface JoinedReadinessRow {
  identity_key: string
  player_name: string
  player_class: string
  role: string
  rank_label: string
  is_active: boolean
  current_tier: string
  roster_source: string
  has_current_tier_data: boolean
  attendance_rate_pct: number
  raids_present: number
  total_raids_tracked: number
  kills_tracked: number
  food_rate: number
  flask_rate: number
  weapon_rate: number
  combat_potion_rate: number
  readiness_score: number
  readiness_label: HealthFilter
  readiness_notes: string[]
  spec: string
  latest_avg_item_level: number
  latest_kill_date: string
  weakest_signal_label: string
  recent_food_names: string
  recent_flask_names: string
  recent_weapon_names: string
  recent_combat_potion_names: string
  character_names: string[]
  override_label: string
}

interface PreparationOverride {
  id: string
  mode: 'replace' | 'pool'
  source_character?: string
  target_character?: string
  characters: string[]
  display_name?: string
  enabled: boolean
  notes?: string
  updated_by?: string
  updated_at?: string
  source: 'file' | 'local'
}

const EDITOR_UNLOCK_STORAGE_KEY = 'preparation_editor_unlocked_v1'
const OWNER_IDENTITY_HASH = 2213701259

function isTruthy(value: unknown): boolean {
  return value === true || value === 'True' || value === 'true'
}

function healthVariant(state: HealthFilter): 'red' | 'yellow' | 'green' | 'blue' {
  if (state === 'watch') return 'red'
  if (state === 'steady') return 'yellow'
  if (state === 'strong') return 'green'
  return 'blue'
}

function requiresCombatPotion(role: string): boolean {
  return role === 'dps'
}

function normalizeName(value: string): string {
  return value.trim().toLowerCase()
}

function splitCharacters(value?: string): string[] {
  return (value || '')
    .split('|')
    .map(part => part.trim())
    .filter(Boolean)
}

function isEnabledOverride(value: unknown): boolean {
  return value !== false && value !== 'false' && value !== 'False' && value !== 0
}

function slugifyName(value: string): string {
  return normalizeName(value).replace(/\s+/g, '-')
}

function quoteSqlString(value?: string | null): string {
  if (!value) return 'CAST(NULL AS STRING)'
  return `'${value.replace(/'/g, "''")}'`
}

function buildOverrideMergeSql(override: PreparationOverride): string {
  return [
    'MERGE INTO 00_governance.warcraftlogs_admin.preparation_identity_overrides AS target',
    'USING (',
    '  SELECT',
    `    ${quoteSqlString(override.id)} AS id,`,
    `    ${quoteSqlString(override.mode)} AS mode,`,
    `    ${quoteSqlString(override.mode === 'replace' ? override.source_character : null)} AS source_character,`,
    `    ${quoteSqlString(override.mode === 'replace' ? override.target_character : null)} AS target_character,`,
    `    ${quoteSqlString(override.mode === 'pool' ? override.characters.join('|') : null)} AS characters,`,
    `    ${quoteSqlString(override.display_name || null)} AS display_name,`,
    `    ${override.enabled ? 'TRUE' : 'FALSE'} AS enabled,`,
    `    ${quoteSqlString(override.notes || null)} AS notes,`,
    '    current_user() AS updated_by,',
    '    current_timestamp() AS updated_at',
    ') AS source',
    'ON target.id = source.id',
    'WHEN MATCHED THEN UPDATE SET *',
    'WHEN NOT MATCHED THEN INSERT *;',
  ].join('\n')
}

function formatFrontendIdentity(value?: string): string {
  if (!value) return ''
  if (hashIdentity(value) === OWNER_IDENTITY_HASH) return 'TV'
  return value
}

function hashIdentity(value: string): number {
  let hash = 5381
  for (const char of normalizeName(value)) {
    hash = ((hash << 5) + hash + char.charCodeAt(0)) >>> 0
  }
  return hash
}

function weakestSignalSummary(row: JoinedReadinessRow): string {
  if (!row.has_current_tier_data) return 'No current-tier logs'
  switch (row.weakest_signal_label) {
    case 'Attendance':
      return `Attendance lowest at ${row.attendance_rate_pct.toFixed(0)}%`
    case 'Food':
      return `Food coverage lowest at ${row.food_rate.toFixed(0)}%`
    case 'Flask':
      return `Flask coverage lowest at ${row.flask_rate.toFixed(0)}%`
    case 'Weapon':
      return `Weapon coverage lowest at ${row.weapon_rate.toFixed(0)}%`
    case 'Combat Potion':
      return `Combat potion usage lowest at ${row.combat_potion_rate.toFixed(0)}%`
    default:
      return row.readiness_notes[0] || 'Review recent current-tier data.'
  }
}

function strongestSignalSummary(row: JoinedReadinessRow): string {
  const candidates = [
    { label: 'Attendance', value: row.attendance_rate_pct },
    { label: 'Food', value: row.food_rate },
    { label: 'Flask', value: row.flask_rate },
    { label: 'Weapon', value: row.weapon_rate },
    ...(requiresCombatPotion(row.role) ? [{ label: 'Combat Potion', value: row.combat_potion_rate }] : []),
  ].sort((a, b) => b.value - a.value)

  const best = candidates[0]
  if (!best) return 'Strong current-tier readiness across tracked prep.'
  return `${best.label} leading at ${best.value.toFixed(0)}%`
}

function parseOverrideRow(row: PreparationOverrideRow, source: 'file' | 'local'): PreparationOverride | null {
  if (!row?.id || !row?.mode) return null
  const mode = row.mode === 'pool' ? 'pool' : row.mode === 'replace' ? 'replace' : null
  if (!mode) return null
  const sourceCharacter = row.source_character?.trim()
  const targetCharacter = row.target_character?.trim()
  const characters = mode === 'replace'
    ? [sourceCharacter, targetCharacter].filter((value): value is string => Boolean(value))
    : splitCharacters(row.characters)
  if (characters.length === 0) return null
  return {
    id: row.id.trim(),
    mode,
    source_character: sourceCharacter,
    target_character: targetCharacter,
    characters,
    display_name: row.display_name?.trim() || undefined,
    enabled: isEnabledOverride(row.enabled),
    notes: row.notes?.trim() || undefined,
    updated_by: row.updated_by?.trim() || undefined,
    updated_at: row.updated_at?.trim() || undefined,
    source,
  }
}

export function Preparation() {
  const { getAttendanceColor } = useColourBlind()
  const readiness = usePreparationReadiness()
  const liveRaidRoster = useLiveRaidRoster()
  const preparationOverrides = usePreparationOverrides()

  const [role, setRole] = useState<RoleFilter>('all')
  const [health, setHealth] = useState<HealthFilter>('all')
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('readiness')
  const [sortDesc, setSortDesc] = useState(false)
  const [showOverrides, setShowOverrides] = useState(false)
  const [draftOverrides, setDraftOverrides] = useState<PreparationOverride[]>([])
  const [editorCode, setEditorCode] = useState('')
  const [editorUnlocked, setEditorUnlocked] = useState(() => {
    try {
      return localStorage.getItem(EDITOR_UNLOCK_STORAGE_KEY) === 'true'
    } catch {
      return false
    }
  })
  const [replaceSource, setReplaceSource] = useState('')
  const [replaceTarget, setReplaceTarget] = useState('')
  const [replaceNotes, setReplaceNotes] = useState('')
  const [poolName, setPoolName] = useState('')
  const [poolCharacters, setPoolCharacters] = useState('')
  const [poolNotes, setPoolNotes] = useState('')

  const editorPassword = String(import.meta.env.VITE_PREPARATION_EDITOR_CODE || '')

  const liveRosterEntries = useMemo(
    () => liveRaidRoster.data.filter(row => row.name),
    [liveRaidRoster.data]
  )

  const fileOverrides = useMemo(
    () => preparationOverrides.data
      .map(row => parseOverrideRow(row, 'file'))
      .filter((row): row is PreparationOverride => row !== null && row.enabled),
    [preparationOverrides.data]
  )

  const activeOverrides = useMemo(() => {
    const merged = new Map<string, PreparationOverride>()
    for (const override of fileOverrides) merged.set(override.id, override)
    for (const override of draftOverrides.filter(entry => entry.enabled)) merged.set(override.id, override)
    return [...merged.values()]
  }, [draftOverrides, fileOverrides])

  const joinedRows = useMemo<JoinedReadinessRow[]>(() => {
    return readiness.data.map((row: PreparationReadiness) => {
      return {
        identity_key: row.identity_key,
        player_name: row.player_name,
        player_class: row.player_class || 'Unknown',
        role: row.role || 'unknown',
        rank_label: row.rank_label || 'Raider',
        is_active: isTruthy(row.is_active),
        current_tier: row.current_tier || '',
        roster_source: row.roster_source || '',
        has_current_tier_data: isTruthy(row.has_current_tier_data),
        attendance_rate_pct: safeNumber(row.attendance_rate_pct),
        raids_present: safeNumber(row.raids_present),
        total_raids_tracked: safeNumber(row.total_raids_tracked),
        kills_tracked: safeNumber(row.kills_tracked),
        food_rate: safeNumber(row.food_rate),
        flask_rate: safeNumber(row.flask_rate),
        weapon_rate: safeNumber(row.weapon_rate),
        combat_potion_rate: safeNumber(row.combat_potion_rate),
        readiness_score: safeNumber(row.readiness_score),
        readiness_label: row.readiness_label || 'steady',
        readiness_notes: row.readiness_notes ? row.readiness_notes.split('|').filter(Boolean) : [],
        spec: row.spec || '—',
        latest_avg_item_level: safeNumber(row.latest_avg_item_level),
        latest_kill_date: row.latest_kill_date || '',
        weakest_signal_label: row.weakest_signal_label || '—',
        recent_food_names: row.recent_food_names || '',
        recent_flask_names: row.recent_flask_names || '',
        recent_weapon_names: row.recent_weapon_names || '',
        recent_combat_potion_names: row.recent_combat_potion_names || '',
        character_names: row.character_names ? row.character_names.split('|').filter(Boolean) : [row.player_name],
        override_label: row.override_label || '',
      }
    })
  }, [readiness.data])

  const teamIdentityRows = joinedRows
  const currentTier = joinedRows[0]?.current_tier || 'Unknown'
  const currentTierRaidCount = joinedRows[0]?.total_raids_tracked ?? 0
  const rosterSource = joinedRows[0]?.roster_source || (liveRosterEntries.length > 0 ? 'live_raid_roster' : 'gold_raid_team')

  const filteredRows = useMemo(() => {
    let rows = [...joinedRows]
    if (role !== 'all') rows = rows.filter(row => row.role === role)
    if (health !== 'all') rows = rows.filter(row => row.readiness_label === health)

    if (search.trim()) {
      const q = normaliseSearchText(search)
      rows = rows.filter(row =>
        matchesLooseSearch(q, row.player_name) ||
        matchesLooseSearch(q, row.player_class) ||
        matchesLooseSearch(q, row.spec) ||
        matchesLooseSearch(q, row.rank_label)
      )
    }

    return rows.sort((a, b) => {
      const value = (row: JoinedReadinessRow) => {
        switch (sortKey) {
          case 'attendance': return row.attendance_rate_pct
          case 'food': return row.food_rate
          case 'flask': return row.flask_rate
          case 'weapon': return row.weapon_rate
          case 'potion': return row.combat_potion_rate
          case 'kills': return row.kills_tracked
          case 'readiness':
          default: return row.readiness_score
        }
      }
      const delta = value(a) - value(b)
      return sortDesc ? delta : -delta
    })
  }, [joinedRows, role, health, search, sortKey, sortDesc])

  const summary = useMemo(() => {
    if (!joinedRows.length) return null
    const avgReadiness = joinedRows.reduce((sum, row) => sum + row.readiness_score, 0) / joinedRows.length
    const trackedRows = joinedRows.filter(row => row.kills_tracked > 0)
    const avgCombatPotion = trackedRows.length
      ? trackedRows.reduce((sum, row) => sum + row.combat_potion_rate, 0) / trackedRows.length
      : 0
    const avgCoreCoverage = trackedRows.length
      ? trackedRows.reduce((sum, row) => sum + ((row.food_rate + row.flask_rate + row.weapon_rate) / 3), 0) / trackedRows.length
      : 0
    const watchlist = joinedRows.filter(row => row.readiness_label === 'watch').length
    const active = joinedRows.filter(row => row.is_active).length
    const currentTierCount = joinedRows.filter(row => row.has_current_tier_data).length
    return { avgReadiness, avgCombatPotion, avgCoreCoverage, watchlist, active, currentTierCount }
  }, [joinedRows])

  const watchlistRows = useMemo(
    () => [...joinedRows].filter(row => row.readiness_label === 'watch').sort((a, b) => a.readiness_score - b.readiness_score).slice(0, 3),
    [joinedRows]
  )

  const bestPreparedRows = useMemo(
    () => [...joinedRows].sort((a, b) => b.readiness_score - a.readiness_score).slice(0, 3),
    [joinedRows]
  )

  const zeroConsumableCoverage = useMemo(() => {
    const tracked = joinedRows.filter(row => row.kills_tracked > 0)
    return tracked.length > 0 && tracked.every(
      row => row.food_rate === 0 && row.flask_rate === 0 && row.weapon_rate === 0 && row.combat_potion_rate === 0
    )
  }, [joinedRows])

  function toggleSort(nextKey: SortKey) {
    if (sortKey === nextKey) setSortDesc(prev => !prev)
    else {
      setSortKey(nextKey)
      setSortDesc(false)
    }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <span className="text-ctp-overlay0 ml-1">↕</span>
    return <span className="text-ctp-blue ml-1">{sortDesc ? '↑' : '↓'}</span>
  }

  function StatPill({
    label,
    value,
    subValue,
    className,
    tone = 'muted',
  }: {
    label: string
    value: string
    subValue?: string
    className?: string
    tone?: 'muted' | 'positive' | 'negative'
  }) {
    return (
      <div
        className={clsx(
          'rounded-xl border px-3 py-2',
          tone === 'positive' && 'border-ctp-green/20 bg-ctp-green/5',
          tone === 'negative' && 'border-ctp-red/20 bg-ctp-red/5',
          tone === 'muted' && 'border-ctp-surface1 bg-ctp-surface0/70',
          className
        )}
      >
        <p className="text-[10px] font-mono uppercase tracking-[0.12em] text-ctp-overlay0">{label}</p>
        <p className="mt-1 text-sm font-medium text-ctp-text whitespace-normal break-words">{value}</p>
        {subValue && (
          <p className="mt-1 text-[11px] leading-4 text-ctp-overlay0 whitespace-normal break-words">{subValue}</p>
        )}
      </div>
    )
  }

  function ReadinessBadge({ row }: { row: JoinedReadinessRow }) {
    const variant = healthVariant(row.readiness_label)
    const label = row.readiness_label === 'watch' ? 'Watchlist' : row.readiness_label === 'strong' ? 'Strong' : 'Steady'
    return <Badge variant={variant} size="sm">{label}</Badge>
  }

  function unlockEditor() {
    if (!editorPassword || editorCode !== editorPassword) return
    setEditorUnlocked(true)
    try { localStorage.setItem(EDITOR_UNLOCK_STORAGE_KEY, 'true') } catch { /* ignore */ }
    setEditorCode('')
  }

  function lockEditor() {
    setEditorUnlocked(false)
    try { localStorage.removeItem(EDITOR_UNLOCK_STORAGE_KEY) } catch { /* ignore */ }
  }

  function addReplaceOverride() {
    const source = replaceSource.trim()
    const target = replaceTarget.trim()
    if (!source || !target) return
    const id = `${slugifyName(source)}-${slugifyName(target)}`
    setDraftOverrides(prev => [
      ...prev.filter(entry => entry.id !== id),
      {
        id,
        mode: 'replace',
        source_character: source,
        target_character: target,
        characters: [source, target],
        display_name: source,
        enabled: true,
        notes: replaceNotes.trim() || 'Draft replace override',
        source: 'local',
      },
    ])
    setReplaceSource('')
    setReplaceTarget('')
    setReplaceNotes('')
  }

  function addPoolOverride() {
    const displayName = poolName.trim()
    const characters = splitCharacters(poolCharacters)
    if (!displayName || characters.length < 2) return
    const id = `${slugifyName(displayName)}-pool`
    setDraftOverrides(prev => [
      ...prev.filter(entry => entry.id !== id),
      {
        id,
        mode: 'pool',
        characters,
        display_name: displayName,
        enabled: true,
        notes: poolNotes.trim() || 'Draft pool override',
        source: 'local',
      },
    ])
    setPoolName('')
    setPoolCharacters('')
    setPoolNotes('')
  }

  function removeDraftOverride(id: string) {
    setDraftOverrides(prev => prev.filter(entry => entry.id !== id))
  }

  const draftOverrideSql = useMemo(
    () => draftOverrides.map(buildOverrideMergeSql).join('\n\n'),
    [draftOverrides]
  )

  const loading = readiness.loading || liveRaidRoster.loading || preparationOverrides.loading
  const error = readiness.error || liveRaidRoster.error || preparationOverrides.error

  return (
    <AppLayout title="Preparation" subtitle="raid readiness · current tier · current raid team" wide>
      {summary && (
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
          <StatCard
            label="Current Raid Team"
            value={teamIdentityRows.length}
            subValue={rosterSource === 'live_raid_roster' ? `${summary.active} active · ${summary.currentTierCount} with current-tier logs` : `${summary.active} active · raid-team export fallback`}
            icon={<Users size={14} />}
            accent="none"
          />
          <StatCard
            label="Avg Readiness"
            value={`${summary.avgReadiness.toFixed(0)}%`}
            subValue="attendance plus current-tier consumable compliance"
            valueColor={getAttendanceColor(summary.avgReadiness)}
            icon={<ShieldCheck size={14} />}
            accent="none"
          />
          <StatCard
            label="Core Coverage"
            value={`${summary.avgCoreCoverage.toFixed(0)}%`}
            subValue="food, flask/phial, and weapon enhancement coverage"
            valueColor={getAttendanceColor(summary.avgCoreCoverage)}
            icon={<BarChart3 size={14} />}
            accent="none"
          />
          <StatCard
            label="Watchlist"
            value={summary.watchlist}
            subValue={`${summary.avgCombatPotion.toFixed(0)}% average combat potion usage`}
            icon={<AlertTriangle size={14} />}
            accent={summary.watchlist > 0 ? 'red' : 'green'}
          />
        </div>
      )}

      <Card className="overflow-hidden">
        <div className="relative">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(137,180,250,0.12),transparent_42%),radial-gradient(circle_at_bottom_right,rgba(203,166,247,0.14),transparent_38%)]" />
          <CardBody className="relative grid gap-5 lg:grid-cols-[1.5fr,1fr]">
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="blue" size="sm">Raid Readiness</Badge>
                <Badge variant="ghost" size="sm">
                  Source: <span className="font-mono">{rosterSource}</span> + current-tier Gold readiness rows only
                </Badge>
              </div>
              <h2 className="mt-4 text-2xl font-semibold tracking-tight text-ctp-text">
                Experimental current-tier preparedness rankings.
              </h2>
              <p className="mt-2 max-w-2xl text-sm leading-6 text-ctp-subtext0">
                Readiness combines current-tier attendance with tracked preparation metrics:<br></br>food coverage, flask coverage, weapon enhancement coverage, and combat potion usage.
              </p>
            </div>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatPill
                label="Tier"
                value={currentTier ?? 'Unknown'}
                tone="positive"
                className="bg-gradient-to-b from-ctp-green/10 via-ctp-green/5 to-transparent"
              />
              <StatPill
                label="Raid nights"
                value={String(currentTierRaidCount)}
                className="bg-gradient-to-b from-ctp-surface0/80 via-ctp-surface0/45 to-transparent"
              />
              <StatPill
                label="Roster"
                value={liveRaidRoster.data[0]?.source_refreshed_at ? `Live ${liveRaidRoster.data[0]?.source_refreshed_at}` : 'Export fallback'}
                className="bg-gradient-to-b from-ctp-surface0/80 via-ctp-surface0/45 to-transparent"
              />
            </div>
          </CardBody>
        </div>
      </Card>

      {zeroConsumableCoverage && (
        <Card className="border-ctp-peach/20 bg-ctp-peach/5">
          <CardBody className="flex items-start gap-3">
            <AlertTriangle className="mt-0.5 text-ctp-peach" size={16} />
            <div>
              <p className="text-sm font-medium text-ctp-text">Current-tier consumable detection looks empty.</p>
              <p className="mt-1 text-xs leading-5 text-ctp-subtext0">
                Current-tier kills are being scoped correctly, but the exported kill roster currently shows no food, flask, weapon, or combat potion coverage on those rows.
              </p>
            </div>
          </CardBody>
        </Card>
      )}

      {loading ? (
        <Card><CardBody><LoadingState rows={12} /></CardBody></Card>
      ) : error ? (
        <Card><CardBody><ErrorState message={error} /></CardBody></Card>
      ) : (
        <>
          <div className="grid gap-5 xl:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Watchlist</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">Lowest readiness on the current raid-team roster.</p>
              </CardHeader>
              <CardBody className="flex min-h-[14.5rem] flex-col justify-center gap-2 py-5">
                {watchlistRows.length === 0 ? (
                  <p className="text-xs font-mono text-ctp-overlay0">No rostered members currently flagged.</p>
                ) : watchlistRows.map(row => (
                  <Link
                    key={row.identity_key}
                    to={`/players/${encodeURIComponent(row.player_name)}`}
                    className="block rounded-xl border border-ctp-red/15 bg-ctp-surface0/70 px-3 py-2.5 transition-colors hover:border-ctp-red/30"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <ClassDot className={row.player_class} />
                          <span className="truncate text-sm font-medium text-ctp-text">{row.player_name}</span>
                          <ReadinessBadge row={row} />
                          {row.override_label && <Badge variant="ghost" size="sm">{row.override_label}</Badge>}
                        </div>
                        <p className="mt-1 text-[11px] leading-5 text-ctp-subtext0 whitespace-normal break-words">
                          {weakestSignalSummary(row)}
                        </p>
                      </div>
                      <div className="flex shrink-0 flex-col items-end gap-1">
                        <span className="text-base font-semibold" style={{ color: getAttendanceColor(row.readiness_score) }}>
                          {row.readiness_score.toFixed(0)}%
                        </span>
                        <span className="text-[10px] font-mono text-ctp-overlay0">
                          {row.kills_tracked} kills
                        </span>
                      </div>
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.12em] text-ctp-overlay0">
                      <span>Attend {row.attendance_rate_pct.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Food {row.food_rate.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Flask {row.flask_rate.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Weapon {row.weapon_rate.toFixed(0)}%</span>
                      {requiresCombatPotion(row.role) && (
                        <>
                          <span>•</span>
                          <span>Pot {row.combat_potion_rate.toFixed(0)}%</span>
                        </>
                      )}
                    </div>
                  </Link>
                ))}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Best Prepared</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">Highest readiness across current-tier rostered raiders.</p>
              </CardHeader>
              <CardBody className="flex min-h-[14.5rem] flex-col justify-center gap-2 py-5">
                {bestPreparedRows.map(row => (
                  <Link
                    key={row.identity_key}
                    to={`/players/${encodeURIComponent(row.player_name)}`}
                    className="block rounded-xl border border-ctp-green/15 bg-ctp-surface0/70 px-3 py-2.5 transition-colors hover:border-ctp-green/30"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <ClassDot className={row.player_class} />
                          <span className="truncate text-sm font-medium text-ctp-text">{row.player_name}</span>
                          <RoleBadge role={row.role} />
                          <Badge variant="ghost" size="sm">{row.spec}</Badge>
                        </div>
                        <p className="mt-1 text-[11px] leading-5 text-ctp-subtext0 whitespace-normal break-words">
                          {strongestSignalSummary(row)}
                        </p>
                      </div>
                      <div className="flex shrink-0 flex-col items-end gap-1">
                        <span className="text-base font-semibold" style={{ color: getAttendanceColor(row.readiness_score) }}>
                          {row.readiness_score.toFixed(0)}%
                        </span>
                        <span className="text-[10px] font-mono text-ctp-overlay0">
                          {row.kills_tracked} kills
                        </span>
                      </div>
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.12em] text-ctp-overlay0">
                      <span>Attend {row.attendance_rate_pct.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Food {row.food_rate.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Flask {row.flask_rate.toFixed(0)}%</span>
                      <span>•</span>
                      <span>Weapon {row.weapon_rate.toFixed(0)}%</span>
                      {requiresCombatPotion(row.role) && (
                        <>
                          <span>•</span>
                          <span>Pot {row.combat_potion_rate.toFixed(0)}%</span>
                        </>
                      )}
                    </div>
                  </Link>
                ))}
              </CardBody>
            </Card>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex flex-wrap items-center gap-3">
              <FilterTabs options={ROLES} value={role} onChange={setRole} />
              <FilterTabs options={HEALTH_STATES} value={health} onChange={setHealth} />
              <input
                type="text"
                placeholder="Search player, class, spec, rank…"
                value={search}
                onChange={event => setSearch(event.target.value)}
                className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-blue/40 w-64"
              />
              <span className="text-xs font-mono text-ctp-overlay0">
                {filteredRows.length} of {teamIdentityRows.length} raid identities visible
              </span>
            </div>
            <button
              type="button"
              onClick={() => setShowOverrides(prev => !prev)}
              className="rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-2 text-xs font-mono text-ctp-text transition-colors hover:border-ctp-blue/40"
            >
              {showOverrides ? 'Hide overrides' : 'Show overrides'}
            </button>
          </div>

          {showOverrides && (
            <Card>
              <CardHeader>
                <CardTitle>Raider Overrides</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Canonical overrides come from `00_governance.warcraftlogs_admin.preparation_identity_overrides` via export. The tool below only drafts SQL and previews the result in this browser until you apply it in Databricks and re-export.
                </p>
              </CardHeader>
              <CardBody className="space-y-4">
                <div className="flex flex-wrap items-center gap-2">
                  {activeOverrides.length === 0 ? (
                    <p className="text-xs font-mono text-ctp-overlay0">No active overrides.</p>
                  ) : activeOverrides.map(override => (
                    <div key={override.id} className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 px-3 py-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={override.mode === 'replace' ? 'blue' : 'green'} size="sm">
                          {override.mode === 'replace' ? 'Replace' : 'Pool'}
                        </Badge>
                        <Badge variant="ghost" size="sm">{override.source === 'file' ? 'Exported UC' : 'Draft Preview'}</Badge>
                        <span className="text-xs font-medium text-ctp-text">
                          {override.display_name || override.source_character || override.characters[0]}
                        </span>
                        <span className="text-xs text-ctp-overlay0">
                          {override.mode === 'replace'
                            ? `${override.source_character} -> ${override.target_character}`
                            : override.characters.join(' + ')}
                        </span>
                        {override.updated_at && (
                          <span className="text-xs text-ctp-overlay0">
                            Updated {formatDate(override.updated_at)}
                          </span>
                        )}
                        {override.updated_by && (
                          <span className="text-xs text-ctp-overlay0">
                            by {formatFrontendIdentity(override.updated_by)}
                          </span>
                        )}
                        {override.source === 'local' && (
                          <button
                            type="button"
                            onClick={() => removeDraftOverride(override.id)}
                            className="text-xs font-mono text-ctp-red hover:text-ctp-text"
                          >
                            Drop draft
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>

                {!editorUnlocked ? (
                  <div className="flex flex-wrap items-center gap-3">
                    <input
                      type="password"
                      placeholder={editorPassword ? 'Admin helper code' : 'Helper disabled until VITE_PREPARATION_EDITOR_CODE is set'}
                      value={editorCode}
                      onChange={event => setEditorCode(event.target.value)}
                      disabled={!editorPassword}
                      className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-blue/40 w-72 disabled:opacity-50"
                    />
                    <button
                      type="button"
                      onClick={unlockEditor}
                      disabled={!editorPassword || !editorCode}
                      className="rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-2 text-xs font-mono text-ctp-text disabled:opacity-50"
                    >
                      Unlock admin helper
                    </button>
                  </div>
                ) : (
                  <>
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-xs text-ctp-subtext0">
                        Drafts only preview here. Shared changes still require Databricks credentials to run the generated SQL against `00_governance.warcraftlogs_admin.preparation_identity_overrides`, then rerun [export_gold_tables.py](/Users/richardmulvany/vscode-projects/git-repos/sc-warcraftlogs-analytics/scripts/export_gold_tables.py).
                      </p>
                      <button
                        type="button"
                        onClick={lockEditor}
                        className="rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-2 text-xs font-mono text-ctp-text"
                      >
                        Lock
                      </button>
                    </div>

                    <div className="grid gap-4 xl:grid-cols-2">
                      <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface0/60 p-4">
                        <p className="text-sm font-medium text-ctp-text">Replace Character</p>
                        <p className="mt-1 text-xs text-ctp-overlay0">Example: Temitiri uses Yevie’s current-tier prep data.</p>
                        <div className="mt-3 grid gap-3">
                          <input
                            type="text"
                            placeholder="Roster character"
                            value={replaceSource}
                            onChange={event => setReplaceSource(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <input
                            type="text"
                            placeholder="Tracked character"
                            value={replaceTarget}
                            onChange={event => setReplaceTarget(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <input
                            type="text"
                            placeholder="Notes"
                            value={replaceNotes}
                            onChange={event => setReplaceNotes(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <button
                            type="button"
                            onClick={addReplaceOverride}
                            className="rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-2 text-xs font-mono text-ctp-text"
                          >
                            Draft replace override
                          </button>
                        </div>
                      </div>

                      <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface0/60 p-4">
                        <p className="text-sm font-medium text-ctp-text">Pool Characters</p>
                        <p className="mt-1 text-xs text-ctp-overlay0">Example: Budgetgoku and Mleko share attendance and prep coverage.</p>
                        <div className="mt-3 grid gap-3">
                          <input
                            type="text"
                            placeholder="Display name"
                            value={poolName}
                            onChange={event => setPoolName(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <input
                            type="text"
                            placeholder="Characters, pipe-separated"
                            value={poolCharacters}
                            onChange={event => setPoolCharacters(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <input
                            type="text"
                            placeholder="Notes"
                            value={poolNotes}
                            onChange={event => setPoolNotes(event.target.value)}
                            className="bg-ctp-base border border-ctp-surface1 rounded-xl px-3 py-2 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-blue/40"
                          />
                          <button
                            type="button"
                            onClick={addPoolOverride}
                            className="rounded-xl border border-ctp-surface1 bg-ctp-surface0 px-3 py-2 text-xs font-mono text-ctp-text"
                          >
                            Draft pool override
                          </button>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface0/60 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div>
                          <p className="text-sm font-medium text-ctp-text">SQL Preview</p>
                          <p className="mt-1 text-xs text-ctp-overlay0">
                            Run these `MERGE` statements in Databricks SQL, then rerun the export so the shared page reflects the override.
                          </p>
                        </div>
                        <span className="text-xs font-mono text-ctp-overlay0">
                          Setup SQL: `docs/admin/preparation_identity_overrides.sql`
                        </span>
                      </div>
                      <textarea
                        readOnly
                        value={draftOverrideSql || '-- Draft an override to generate SQL here.'}
                        className="mt-3 h-56 w-full resize-y rounded-xl border border-ctp-surface1 bg-ctp-base px-3 py-3 text-xs font-mono text-ctp-subtext1 focus:outline-none"
                      />
                    </div>
                  </>
                )}
              </CardBody>
            </Card>
          )}

          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <CardTitle>Roster Readiness Board</CardTitle>
                  <p className="text-xs text-ctp-overlay1 mt-0.5">
                    Every metric in this board is derived from current-tier raid-summary and current-tier kill-roster rows only.
                  </p>
                </div>
              </div>
            </CardHeader>
            {filteredRows.length === 0 ? (
              <CardBody>
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No current raid-team members match the current filters.
                </p>
              </CardBody>
            ) : (
              <>
                <div className="border-b border-ctp-surface1 px-5 py-3">
                  <div className="flex flex-wrap gap-2 text-[10px] font-mono uppercase tracking-[0.12em] text-ctp-overlay0">
                    <button onClick={() => toggleSort('readiness')} className="hover:text-ctp-text">
                      Readiness <SortIcon k="readiness" />
                    </button>
                    <button onClick={() => toggleSort('attendance')} className="hover:text-ctp-text">
                      Attendance <SortIcon k="attendance" />
                    </button>
                    <button onClick={() => toggleSort('food')} className="hover:text-ctp-text">
                      Food <SortIcon k="food" />
                    </button>
                    <button onClick={() => toggleSort('flask')} className="hover:text-ctp-text">
                      Flask <SortIcon k="flask" />
                    </button>
                    <button onClick={() => toggleSort('weapon')} className="hover:text-ctp-text">
                      Weapon <SortIcon k="weapon" />
                    </button>
                    <button onClick={() => toggleSort('potion')} className="hover:text-ctp-text">
                      Combat Potion <SortIcon k="potion" />
                    </button>
                    <button onClick={() => toggleSort('kills')} className="hover:text-ctp-text">
                      Kills <SortIcon k="kills" />
                    </button>
                  </div>
                </div>
                <CardBody className="space-y-4">
                  {filteredRows.map(row => (
                    <div key={row.identity_key} className="rounded-2xl border border-ctp-surface1 bg-ctp-surface0/60 p-4">
                      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <ClassDot className={row.player_class} />
                            <Link
                              to={`/players/${encodeURIComponent(row.player_name)}`}
                              className="text-sm font-medium text-ctp-text hover:text-ctp-blue"
                            >
                              {row.player_name}
                            </Link>
                            <ClassLabel className={row.player_class} size="sm" />
                            <RoleBadge role={row.role} />
                            <Badge variant="ghost" size="sm">{row.rank_label}</Badge>
                            <ReadinessBadge row={row} />
                            {row.override_label && <Badge variant="ghost" size="sm">{row.override_label}</Badge>}
                            {!row.is_active && <Badge variant="ghost" size="sm">Inactive</Badge>}
                            {!row.has_current_tier_data && <Badge variant="peach" size="sm">No current-tier logs</Badge>}
                          </div>
                          <p className="mt-2 text-xs leading-5 text-ctp-subtext0 whitespace-normal break-words">
                            {row.readiness_notes.join(' · ') || 'No major flags'}
                          </p>
                          {row.character_names.length > 1 && (
                            <p className="mt-1 text-[11px] leading-5 text-ctp-overlay0 whitespace-normal break-words">
                              Characters: {row.character_names.join(', ')}
                            </p>
                          )}
                          <p className="mt-1 text-[11px] leading-5 text-ctp-overlay0 whitespace-normal break-words">
                            Weakest signal: {row.weakest_signal_label} · Last seen {formatDate(row.latest_kill_date)}
                          </p>
                        </div>
                        <div className="w-full lg:w-64">
                          <div className="flex items-center justify-between gap-3">
                            <span className="text-[10px] font-mono uppercase tracking-[0.12em] text-ctp-overlay0">Readiness</span>
                            <span className="text-sm font-semibold" style={{ color: getAttendanceColor(row.readiness_score) }}>
                              {row.readiness_score.toFixed(0)}%
                            </span>
                          </div>
                          <div className="mt-2">
                            <ProgressBar value={row.readiness_score} color={getAttendanceColor(row.readiness_score)} height="sm" />
                          </div>
                        </div>
                      </div>
                      <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
                        <StatPill label="Attendance" value={`${row.attendance_rate_pct.toFixed(0)}%`} />
                        <StatPill
                          label="Food"
                          value={`${row.food_rate.toFixed(0)}%`}
                          subValue={row.recent_food_names || 'Missing'}
                        />
                        <StatPill
                          label="Flask"
                          value={`${row.flask_rate.toFixed(0)}%`}
                          subValue={row.recent_flask_names || 'Missing'}
                        />
                        <StatPill
                          label="Weapon"
                          value={`${row.weapon_rate.toFixed(0)}%`}
                          subValue={row.recent_weapon_names || 'Missing'}
                        />
                        <StatPill
                          label="Combat Potion"
                          value={`${row.combat_potion_rate.toFixed(0)}%`}
                          subValue={row.recent_combat_potion_names || 'Missing'}
                        />
                        <StatPill label="Kills" value={formatNumber(row.kills_tracked)} />
                      </div>
                    </div>
                  ))}
                </CardBody>
              </>
            )}
          </Card>
        </>
      )}
    </AppLayout>
  )
}
