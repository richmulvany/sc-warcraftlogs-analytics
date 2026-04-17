import { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { FilterSelect } from '../components/ui/FilterSelect'
import { StatCard } from '../components/ui/StatCard'
import { FilterTabs } from '../components/ui/FilterTabs'
import { RoleBadge } from '../components/ui/Badge'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassLabel } from '../components/ui/ClassLabel'
import { ProgressBar } from '../components/ui/ProgressBar'
import { DpsOverTimeChart } from '../components/charts/DpsOverTimeChart'
import { PerformanceHeatmap } from '../components/charts/PerformanceHeatmap'
import {
  usePlayerPerformance,
  usePlayerSurvivability,
  usePlayerAttendance,
  useBossKillRoster,
  usePlayerBossPerformance,
  useRaidSummary,
  usePlayerCharacterMedia,
  usePlayerCharacterEquipment,
  usePlayerRaidAchievements,
} from '../hooks/useGoldData'
import { formatThroughput, getClassColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import { formatDate, formatPct } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import type { PlayerBossPerformance, PlayerCharacterEquipment } from '../types'

type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'

const DIFFICULTIES: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']

const EQUIPMENT_SLOTS = [
  { type: 'HEAD', label: 'Head', side: 'left' },
  { type: 'NECK', label: 'Neck', side: 'left' },
  { type: 'SHOULDER', label: 'Shoulder', side: 'left' },
  { type: 'BACK', label: 'Back', side: 'left' },
  { type: 'CHEST', label: 'Chest', side: 'left' },
  { type: 'SHIRT', label: 'Shirt', side: 'left' },
  { type: 'TABARD', label: 'Tabard', side: 'left' },
  { type: 'WRIST', label: 'Wrist', side: 'left' },
  { type: 'HANDS', label: 'Hands', side: 'right' },
  { type: 'WAIST', label: 'Waist', side: 'right' },
  { type: 'LEGS', label: 'Legs', side: 'right' },
  { type: 'FEET', label: 'Feet', side: 'right' },
  { type: 'FINGER_1', label: 'Finger', side: 'right' },
  { type: 'FINGER_2', label: 'Finger', side: 'right' },
  { type: 'TRINKET_1', label: 'Trinket', side: 'right' },
  { type: 'TRINKET_2', label: 'Trinket', side: 'right' },
  { type: 'MAIN_HAND', label: 'Main Hand', side: 'bottom' },
  { type: 'OFF_HAND', label: 'Off Hand', side: 'bottom' },
] as const

const EQUIPMENT_SLOT_ORDER: string[] = EQUIPMENT_SLOTS.map(slot => slot.type)

const QUALITY_COLORS: Record<string, string> = {
  Poor: '#9ca3af',
  Common: '#d1d5db',
  Uncommon: '#a6e3a1',
  Rare: '#89b4fa',
  Epic: '#cba6f7',
  Legendary: '#fab387',
  Artifact: '#f9e2af',
  Heirloom: '#94e2d5',
}

interface GearEnhancement {
  display_string?: string
  item_name?: string
  source_item_name?: string
  socket_type?: string
  value?: number | string
  type?: string
  display?: string
  description?: string
  spell_name?: string
}

function parseGearJson(value: unknown): GearEnhancement[] {
  if (typeof value !== 'string' || !value.trim()) return []
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function ItemTooltip({ item, color }: { item: PlayerCharacterEquipment; color: string }) {
  const enchants = parseGearJson(item.enchantments_json)
  const sockets = parseGearJson(item.sockets_json)
  const stats = parseGearJson(item.stats_json)
  const spells = parseGearJson(item.spells_json)

  return (
    <div className="pointer-events-none absolute left-12 top-0 z-40 hidden w-72 rounded-xl border border-ctp-surface2 bg-ctp-crust/95 p-4 text-left shadow-2xl backdrop-blur group-hover:block">
      <p className="text-sm font-semibold" style={{ color }}>{item.item_name}</p>
      {item.transmog_name && (
        <p className="mt-1 text-xs text-ctp-pink">Transmog: {item.transmog_name}</p>
      )}
      <p className="mt-1 text-xs font-mono text-ctp-yellow">Item Level {item.item_level || '—'}</p>
      <p className="mt-2 text-xs text-ctp-subtext1">{item.binding || 'Binds when picked up'}</p>
      <p className="text-xs text-ctp-overlay1">{item.inventory_type || item.slot_name} {item.item_subclass ? `· ${item.item_subclass}` : ''}</p>

      {stats.length > 0 && (
        <div className="mt-3 space-y-0.5">
          {stats.slice(0, 8).map((stat, index) => (
            <p key={index} className="text-xs text-ctp-text">
              {stat.display || `${stat.value ? `+${stat.value} ` : ''}${stat.type ?? ''}`}
            </p>
          ))}
        </div>
      )}

      {enchants.length > 0 && (
        <div className="mt-3 space-y-0.5">
          {enchants.map((enchant, index) => (
            <p key={index} className="text-xs text-ctp-green">
              Enchanted: {enchant.display_string || enchant.source_item_name || 'Unknown enchant'}
            </p>
          ))}
        </div>
      )}

      {sockets.length > 0 && (
        <div className="mt-3 space-y-1">
          {sockets.map((socket, index) => (
            <p key={index} className="text-xs text-ctp-sapphire">
              {socket.item_name || socket.display_string || socket.socket_type || 'Empty socket'}
            </p>
          ))}
        </div>
      )}

      {spells.length > 0 && (
        <div className="mt-3 space-y-2">
          {spells.slice(0, 3).map((spell, index) => (
            <div key={index}>
              <p className="text-xs font-medium text-ctp-text">{spell.spell_name}</p>
              {spell.description && <p className="mt-0.5 text-xs leading-relaxed text-ctp-subtext1">{spell.description}</p>}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function GearSlot({ item, label, classColor }: { item?: PlayerCharacterEquipment; label: string; classColor: string }) {
  const qualityColor = item ? (QUALITY_COLORS[item.quality] ?? classColor) : '#45475a'
  const enchants = item ? parseGearJson(item.enchantments_json) : []
  const sockets = item ? parseGearJson(item.sockets_json) : []

  return (
    <div className="group relative flex min-h-[54px] items-center gap-2 rounded-xl border border-ctp-surface1 bg-ctp-surface0/55 px-2 py-1.5">
      <div
        className="relative flex h-10 w-10 flex-shrink-0 items-center justify-center overflow-hidden rounded-lg border bg-ctp-crust/80"
        style={{ borderColor: item ? qualityColor : '#313244' }}
      >
        {item?.icon_url ? (
          <img src={item.icon_url} alt="" className="h-full w-full object-cover" />
        ) : (
          <span className="text-[9px] font-mono text-ctp-surface2">{label.slice(0, 2)}</span>
        )}
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-2">
          <p className="truncate text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">{label}</p>
          <p className="text-[10px] font-mono text-ctp-yellow">{item?.item_level || '—'}</p>
        </div>
        <p className="mt-0.5 truncate text-xs font-medium" style={{ color: item ? qualityColor : '#6c7086' }}>
          {item?.item_name || 'Empty'}
        </p>
        {(enchants.length > 0 || sockets.length > 0) && (
          <div className="mt-0.5 flex items-center gap-1.5">
            {enchants.length > 0 && <span className="text-[9px] font-mono text-ctp-green">ench</span>}
            {sockets.slice(0, 3).map((socket, index) => (
              <span key={index} className="h-1.5 w-1.5 rounded-full bg-ctp-sapphire" title={socket.item_name || socket.socket_type} />
            ))}
          </div>
        )}
      </div>
      {item && <ItemTooltip item={item} color={qualityColor} />}
    </div>
  )
}

export function PlayerDetail() {
  const { getParseColor, wipeColor, getDeathRateColor, getAttendanceColor } = useColourBlind()
  const { playerName } = useParams<{ playerName: string }>()
  const navigate = useNavigate()

  const name = decodeURIComponent(playerName ?? '')

  const perf = usePlayerPerformance()
  const surv = usePlayerSurvivability()
  const att = usePlayerAttendance()
  const raids = useRaidSummary()
  const roster = useBossKillRoster()
  const bossPf = usePlayerBossPerformance()
  const characterMedia = usePlayerCharacterMedia()
  const characterEquipment = usePlayerCharacterEquipment()
  const raidAchievements = usePlayerRaidAchievements()

  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')

  function hasRealText(value: unknown): value is string {
    return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
  }

  const summary = useMemo(
    () => perf.data.find(p => p.player_name === name),
    [perf.data, name]
  )

  const survRow = useMemo(
    () => surv.data.find(s => s.player_name === name),
    [surv.data, name]
  )

  const attRow = useMemo(
    () => att.data.find(a => a.player_name === name),
    [att.data, name]
  )

  const playerRosterRows = useMemo(() =>
    roster.data
      .filter(r => r.player_name === name && Number(r.throughput_per_second) > 0)
      .filter(r => isIncludedZoneName(r.zone_name))
      .sort((a, b) => String(a.raid_night_date).localeCompare(String(b.raid_night_date))),
    [roster.data, name]
  )

  const tierOptions = useMemo(() =>
    ['All', ...new Set(
      [...playerRosterRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
        .filter(hasRealText)
    )],
    [playerRosterRows]
  )

  const currentTier = tierOptions[1] ?? ''

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const bossOptions = useMemo(() => {
    const bosses = [...new Set(
      playerRosterRows
        .filter(r => selectedTier === 'All' || !selectedTier || r.zone_name === selectedTier)
        .filter(r => difficulty === 'All' || r.difficulty_label === difficulty)
        .map(r => r.boss_name)
        .filter(hasRealText)
    )].sort()
    return ['All', ...bosses]
  }, [playerRosterRows, selectedTier, difficulty])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const scopedRosterRows = useMemo(() =>
    playerRosterRows.filter(r =>
      (selectedTier === 'All' || !selectedTier || r.zone_name === selectedTier) &&
      (difficulty === 'All' || r.difficulty_label === difficulty) &&
      (selectedBoss === 'All' || r.boss_name === selectedBoss)
    ),
    [playerRosterRows, selectedTier, difficulty, selectedBoss]
  )

  const dpsOverTime = useMemo(() =>
    scopedRosterRows.map(r => ({
      date: r.raid_night_date,
      throughput: Number(r.throughput_per_second),
      boss: r.boss_name,
      parse: Number(r.rank_percent) || 0,
    })),
    [scopedRosterRows]
  )

  const scopedBossPerformance = useMemo(
    () => bossPf.data.filter(b =>
      b.player_name === name &&
      (selectedTier === 'All' || !selectedTier || b.zone_name === selectedTier) &&
      (difficulty === 'All' || b.difficulty_label === difficulty) &&
      (selectedBoss === 'All' || b.boss_name === selectedBoss)
    ),
    [bossPf.data, name, selectedTier, difficulty, selectedBoss]
  )

  const fallbackBossPerformance = useMemo((): PlayerBossPerformance[] => {
    const grouped = new Map<string, PlayerBossPerformance>()

    for (const row of scopedRosterRows) {
      const key = `${row.encounter_id}-${row.difficulty}`
      const existing = grouped.get(key)
      if (!existing) {
        grouped.set(key, {
          player_name: row.player_name,
          player_class: row.player_class,
          role: row.role,
          primary_spec: row.spec,
          encounter_id: row.encounter_id,
          boss_name: row.boss_name,
          zone_name: row.zone_name,
          difficulty: row.difficulty,
          difficulty_label: row.difficulty_label,
          kills_on_boss: 1,
          avg_throughput_per_second: Number(row.throughput_per_second) || 0,
          best_throughput_per_second: Number(row.throughput_per_second) || 0,
          latest_throughput_per_second: Number(row.throughput_per_second) || 0,
          throughput_trend: 0,
          avg_rank_percent: Number(row.rank_percent) || 0,
          best_rank_percent: Number(row.rank_percent) || 0,
          avg_item_level: Number(row.avg_item_level) || 0,
          first_kill_date: row.raid_night_date,
          latest_kill_date: row.raid_night_date,
        })
        continue
      }

      const nextKills = existing.kills_on_boss + 1
      const throughput = Number(row.throughput_per_second) || 0
      const rank = Number(row.rank_percent) || 0
      existing.kills_on_boss = nextKills
      existing.avg_throughput_per_second = ((existing.avg_throughput_per_second * (nextKills - 1)) + throughput) / nextKills
      existing.best_throughput_per_second = Math.max(existing.best_throughput_per_second, throughput)
      existing.latest_throughput_per_second = throughput
      existing.avg_rank_percent = ((existing.avg_rank_percent * (nextKills - 1)) + rank) / nextKills
      existing.best_rank_percent = Math.max(existing.best_rank_percent, rank)
      existing.avg_item_level = ((existing.avg_item_level * (nextKills - 1)) + (Number(row.avg_item_level) || 0)) / nextKills
      existing.latest_kill_date = row.raid_night_date
    }

    return [...grouped.values()]
  }, [scopedRosterRows])

  const heatmapData = scopedBossPerformance.length > 0 ? scopedBossPerformance : fallbackBossPerformance

  const scopedSummary = useMemo(() => {
    if (scopedRosterRows.length === 0) return null

    const kills = scopedRosterRows.length
    const avgThroughput = scopedRosterRows.reduce((sum, row) => sum + (Number(row.throughput_per_second) || 0), 0) / kills
    const bestThroughput = Math.max(...scopedRosterRows.map(row => Number(row.throughput_per_second) || 0))
    const avgRank = scopedRosterRows.reduce((sum, row) => sum + (Number(row.rank_percent) || 0), 0) / kills
    const bestRank = Math.max(...scopedRosterRows.map(row => Number(row.rank_percent) || 0))

    return {
      avgThroughput,
      bestThroughput,
      avgRank,
      bestRank,
      kills,
      firstSeen: [...scopedRosterRows].sort((a, b) => a.raid_night_date.localeCompare(b.raid_night_date))[0]?.raid_night_date,
      lastSeen: [...scopedRosterRows].sort((a, b) => b.raid_night_date.localeCompare(a.raid_night_date))[0]?.raid_night_date,
      playerClass: scopedRosterRows[0].player_class,
      role: scopedRosterRows[0].role,
      spec: scopedRosterRows[0].spec,
    }
  }, [scopedRosterRows])

  const validRaidRows = useMemo(() =>
    raids.data.filter(r =>
      hasRealText(r.report_code) &&
      isIncludedZoneName(r.zone_name) &&
      hasRealText(r.raid_night_date) &&
      hasRealText(r.primary_difficulty)
    ),
    [raids.data]
  )

  const filteredSessions = useMemo(() => {
    let rows = validRaidRows
    if (selectedTier && selectedTier !== 'All') rows = rows.filter(r => r.zone_name === selectedTier)
    if (difficulty !== 'All') rows = rows.filter(r => r.primary_difficulty === difficulty)
    if (selectedBoss !== 'All') {
      const matchingReports = new Set(
        roster.data
          .filter(row => row.player_name === name)
          .filter(row => selectedTier === 'All' || !selectedTier || row.zone_name === selectedTier)
          .filter(row => difficulty === 'All' || row.difficulty_label === difficulty)
          .filter(row => row.boss_name === selectedBoss)
          .map(row => row.report_code)
      )
      rows = rows.filter(r => matchingReports.has(r.report_code))
    }
    return rows
  }, [validRaidRows, roster.data, name, selectedTier, difficulty, selectedBoss])

  const scopedAttendance = useMemo(() => {
    if (filteredSessions.length === 0) return null
    const presentReports = new Set(scopedRosterRows.map(r => r.report_code))
    const raidsPresent = presentReports.size
    const totalRaidsTracked = filteredSessions.length
    const attendanceRatePct = totalRaidsTracked > 0 ? (raidsPresent / totalRaidsTracked) * 100 : 0

    return {
      raidsPresent,
      totalRaidsTracked,
      attendanceRatePct,
    }
  }, [filteredSessions, scopedRosterRows])

  const dataCoverage = useMemo(() => {
    if (!summary) return []
    const gaps: string[] = []
    if (!attRow) gaps.push('attendance')
    if (!survRow) gaps.push('survivability')
    if (dpsOverTime.length === 0) gaps.push('fight timeline')
    if (heatmapData.length === 0) gaps.push('boss breakdown')
    return gaps
  }, [summary, attRow, survRow, dpsOverTime.length, heatmapData.length])

  const playerMedia = useMemo(
    () => characterMedia.data.find(row => row.player_name.toLowerCase() === name.toLowerCase()) ?? null,
    [characterMedia.data, name]
  )

  const playerEquipment = useMemo(() =>
    characterEquipment.data
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) =>
        (EQUIPMENT_SLOT_ORDER.indexOf(a.slot_type) === -1 ? 999 : EQUIPMENT_SLOT_ORDER.indexOf(a.slot_type)) -
        (EQUIPMENT_SLOT_ORDER.indexOf(b.slot_type) === -1 ? 999 : EQUIPMENT_SLOT_ORDER.indexOf(b.slot_type))
      ),
    [characterEquipment.data, name]
  )

  const equipmentBySlot = useMemo(() => {
    const map = new Map<string, PlayerCharacterEquipment>()
    playerEquipment.forEach(item => {
      if (item.slot_type) map.set(item.slot_type, item)
    })
    return map
  }, [playerEquipment])

  const playerRaidAchievements = useMemo(() =>
    [...raidAchievements.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => Number(b.completed_timestamp) - Number(a.completed_timestamp)),
    [raidAchievements.data, name]
  )

  function formatBlizzardTimestamp(value: unknown): string {
    const timestamp = Number(value)
    if (!Number.isFinite(timestamp) || timestamp <= 0) return '—'
    return formatDate(new Date(timestamp).toISOString())
  }

  const loading = perf.loading || surv.loading || att.loading || raids.loading || roster.loading || bossPf.loading
  const error = perf.error || surv.error || raids.error

  const classColor = getClassColor(scopedSummary?.playerClass ?? summary?.player_class ?? 'Unknown')

  if (!loading && !summary) {
    return (
      <AppLayout title={name} subtitle="player not found">
        <div className="py-16 text-center">
          <p className="text-ctp-overlay1 text-sm font-mono mb-4">Player "{name}" not found in data.</p>
          <button
            onClick={() => navigate('/players')}
            className="px-4 py-2 text-xs font-mono rounded-xl bg-ctp-surface0 border border-ctp-surface1 text-ctp-subtext1 hover:bg-ctp-surface1 transition-colors"
          >
            ← Back to Players
          </button>
        </div>
      </AppLayout>
    )
  }

  return (
    <AppLayout
      title={name}
      subtitle="player profile"
      actions={
        <button
          onClick={() => navigate('/players')}
          className="px-3 py-1.5 text-xs font-mono rounded-xl bg-ctp-surface0 border border-ctp-surface1 text-ctp-overlay1 hover:text-ctp-subtext1 hover:bg-ctp-surface1 transition-all"
        >
          ← All Players
        </button>
      }
    >
      <div
        className="rounded-2xl border p-6 shadow-card"
        style={{
          background: `linear-gradient(135deg, ${classColor}10 0%, #31324400 60%)`,
          borderColor: `${classColor}25`,
        }}
      >
        {loading ? (
          <div className="flex items-center gap-4">
            <div className="w-14 h-14 rounded-2xl bg-ctp-surface1 animate-pulse" />
            <div className="space-y-2">
              <div className="h-5 w-40 bg-ctp-surface1 rounded-lg animate-pulse" />
              <div className="h-3 w-24 bg-ctp-surface1/60 rounded-lg animate-pulse" />
            </div>
          </div>
        ) : summary ? (
          <div className="flex flex-wrap items-center gap-5">
            <div
              className="w-16 h-16 rounded-2xl flex items-center justify-center flex-shrink-0 text-xl font-bold border overflow-hidden"
              style={{ background: `${classColor}15`, borderColor: `${classColor}30`, color: classColor }}
            >
              {playerMedia?.inset_url || playerMedia?.avatar_url || playerMedia?.main_url ? (
                <img
                  src={playerMedia.inset_url || playerMedia.avatar_url || playerMedia.main_url}
                  alt={`${summary.player_name} character portrait`}
                  className="h-full w-full p-0.5 rounded-2xl  object-cover"
                />
              ) : (
                summary.player_class?.[0] ?? '?'
              )}
            </div>
            <div className="flex-1 min-w-0">
              <h2 className="text-xl font-semibold" style={{ color: classColor }}>
                {summary.player_name}
              </h2>
              <div className="flex items-center gap-2 mt-1">
                <ClassLabel className={summary.player_class} spec={summary.primary_spec} size="sm" />
                <span className="text-ctp-overlay0 text-xs">·</span>
                <RoleBadge role={summary.role} />
                <span className="text-ctp-overlay0 text-xs">·</span>
                <span className="text-xs text-ctp-overlay1 font-mono">{summary.realm}</span>
              </div>
            </div>

            <div className="flex items-center gap-6 flex-shrink-0">
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best Parse</p>
                <p className="text-lg font-semibold" style={{ color: getParseColor(scopedSummary?.bestRank ?? summary.best_rank_percent) }}>
                  {(scopedSummary?.bestRank ?? summary.best_rank_percent) ? `${(scopedSummary?.bestRank ?? summary.best_rank_percent).toFixed(0)}%` : '—'}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best DPS/HPS</p>
                <p className="text-lg font-semibold text-ctp-text">
                  {formatThroughput(scopedSummary?.bestThroughput ?? summary.best_throughput_per_second)}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Last Seen</p>
                <p className="text-sm font-mono text-ctp-subtext1">{formatDate(scopedSummary?.lastSeen ?? summary.last_seen_date)}</p>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <FilterTabs options={DIFFICULTIES} value={difficulty} onChange={setDifficulty} />
          <FilterSelect value={selectedTier} onChange={setSelectedTier} options={tierOptions} className="min-w-48" />
          <FilterSelect value={selectedBoss} onChange={setSelectedBoss} options={bossOptions} className="min-w-52" />
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard
              label="Avg DPS / HPS"
              value={formatThroughput(scopedSummary?.avgThroughput ?? 0)}
              subValue={`Best: ${formatThroughput(scopedSummary?.bestThroughput ?? 0)}`}
              icon="◉"
              accent="blue"
            />
            <StatCard
              label="Avg Percentile"
              value={scopedSummary ? `${scopedSummary.avgRank.toFixed(1)}%` : '—'}
              subValue={`Best: ${scopedSummary ? `${scopedSummary.bestRank.toFixed(0)}%` : '—'}`}
              icon="◈"
              valueColor={scopedSummary ? getParseColor(scopedSummary.avgRank) : undefined}
              accent="none"
            />
            <StatCard
              label="Deaths per Kill"
              value={survRow?.deaths_per_kill != null ? survRow.deaths_per_kill.toFixed(1) : '—'}
              subValue={`${survRow?.total_deaths ?? 0} total deaths · aggregate`}
              icon="☠"
              valueColor={survRow?.deaths_per_kill != null ? getDeathRateColor(survRow.deaths_per_kill) : undefined}
              accent="none"
            />
            <StatCard
              label="Attendance"
              value={scopedAttendance ? formatPct(scopedAttendance.attendanceRatePct) : '—'}
              subValue={scopedAttendance ? `${scopedAttendance.raidsPresent} / ${scopedAttendance.totalRaidsTracked} sessions` : 'No scoped sessions'}
              icon="◷"
              valueColor={scopedAttendance ? getAttendanceColor(scopedAttendance.attendanceRatePct) : undefined}
              accent="none"
            />
          </>
        )}
      </div>

      {(characterEquipment.data.length > 0 || raidAchievements.data.length > 0) && (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <Card className="xl:col-span-2">
            <CardHeader>
              <CardTitle>Equipped Gear</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                Blizzard profile snapshot
              </p>
            </CardHeader>
            <CardBody>
              {characterEquipment.loading ? (
                <LoadingState rows={6} />
              ) : playerEquipment.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No exposed equipment found for this character.
                </p>
              ) : (
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(300px,380px)_minmax(0,1fr)]">
                  <div className="space-y-2">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'left').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>

                  <div
                    className="relative order-first flex min-h-[520px] items-end justify-center overflow-hidden rounded-2xl border border-ctp-surface1 bg-ctp-crust/70 lg:order-none"
                    style={{
                      background: `radial-gradient(circle at 50% 22%, ${classColor}24 0%, rgba(24, 24, 37, 0.58) 38%, rgba(17, 17, 27, 0.92) 100%)`,
                    }}
                  >
                    {playerMedia?.main_raw_url || playerMedia?.main_url ? (
                      <img
                        src={playerMedia.main_raw_url || playerMedia.main_url}
                        alt={`${name} standing character render`}
                        className="absolute inset-x-[-32%] -bottom-9 z-10 h-[132%] w-[164%] max-w-none object-cover object-bottom drop-shadow-2xl"
                      />
                    ) : (
                      <div className="relative z-10 flex h-full w-full items-center justify-center text-5xl font-bold" style={{ color: classColor }}>
                        {name[0] ?? '?'}
                      </div>
                    )}
                    <div className="pointer-events-none absolute inset-0 z-20 bg-gradient-to-b from-ctp-crust/10 via-transparent to-ctp-crust/45" />
                  </div>

                  <div className="space-y-2">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'right').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>

                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:col-span-3">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'bottom').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>
                </div>
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Raid Feats</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                Exposed Cutting Edge / Famed Slayer style achievements
              </p>
            </CardHeader>
            <CardBody>
              {raidAchievements.loading ? (
                <LoadingState rows={4} />
              ) : playerRaidAchievements.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No exposed raid feats found for this character.
                </p>
              ) : (
                <div className="space-y-2.5">
                  {playerRaidAchievements.slice(0, 8).map(achievement => (
                    <div
                      key={`${achievement.achievement_id}-${achievement.completed_timestamp}`}
                      className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/60 px-3 py-2"
                    >
                      <p className="text-xs font-medium text-ctp-text">
                        {achievement.achievement_name}
                      </p>
                      <p className="mt-0.5 text-[10px] font-mono text-ctp-overlay0">
                        {formatBlizzardTimestamp(achievement.completed_timestamp)}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </CardBody>
          </Card>
        </div>
      )}

      {dataCoverage.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Profile Data Coverage</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Some profile modules are missing backing rows in the current static export: {dataCoverage.join(', ')}.
            </p>
          </CardHeader>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>DPS / HPS Over Time</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            {dpsOverTime.length} kill performances in scope
            {scopedSummary ? ` · avg ${formatThroughput(scopedSummary.avgThroughput)}` : ''}
          </p>
        </CardHeader>
        <CardBody>
          {roster.loading ? (
            <LoadingState rows={5} />
          ) : dpsOverTime.length === 0 && heatmapData.length > 0 ? (
            <div className="h-48 flex items-center justify-center text-center text-ctp-overlay0 text-sm font-mono px-6">
              Fight-by-fight timeline data is unavailable for this filtered scope. Boss-level aggregates are shown below instead.
            </div>
          ) : (
            <DpsOverTimeChart
              data={dpsOverTime}
              playerClass={scopedSummary?.playerClass ?? summary?.player_class ?? 'Unknown'}
              avgThroughput={scopedSummary?.avgThroughput}
            />
          )}
        </CardBody>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Parse Breakdown by Fight</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Individual WCL rank % per recorded fight in scope</p>
          </CardHeader>
          <CardBody className="space-y-2.5">
            {roster.loading ? (
              <LoadingState rows={6} />
            ) : dpsOverTime.length === 0 ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No fight data available in the current scope</p>
            ) : (
              [...dpsOverTime]
                .filter(d => d.parse > 0)
                .sort((a, b) => b.parse - a.parse)
                .slice(0, 12)
                .map((d, i) => (
                  <div key={i} className="flex items-center gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-ctp-subtext1 truncate">{d.boss}</span>
                        <span className="text-xs font-mono font-semibold flex-shrink-0 ml-2" style={{ color: getParseColor(d.parse) }}>
                          {d.parse.toFixed(0)}%
                        </span>
                      </div>
                      <ProgressBar value={d.parse} color={getParseColor(d.parse)} height="xs" />
                    </div>
                    <span className="text-[10px] font-mono text-ctp-overlay0 flex-shrink-0 w-16 text-right">
                      {formatDate(d.date)}
                    </span>
                  </div>
                ))
            )}
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Survivability</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Aggregate death analysis for this player</p>
          </CardHeader>
          <CardBody>
            {surv.loading ? (
              <LoadingState rows={4} />
            ) : !survRow ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No death data recorded</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Total Deaths</p>
                    <p className="text-xl font-semibold" style={{ color: wipeColor }}>{survRow.total_deaths}</p>
                  </div>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Deaths / Kill</p>
                    <p className="text-xl font-semibold" style={{ color: getDeathRateColor(Number(survRow.deaths_per_kill) || 0) }}>
                      {survRow.deaths_per_kill?.toFixed(1) ?? '—'}
                    </p>
                  </div>
                </div>

                <div>
                  <p className="section-label mb-2">Most Common Killing Blow</p>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="text-sm font-medium text-ctp-text">
                      {survRow.most_common_killing_blow || '—'}
                    </p>
                    <p className="text-xs text-ctp-overlay0 mt-0.5">
                      {survRow.most_common_killing_blow_count} occurrences
                    </p>
                  </div>
                </div>
              </div>
            )}
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Performance by Boss</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Avg WCL parse % per boss encounter in scope · colour = parse tier
          </p>
        </CardHeader>
        <CardBody>
          {bossPf.loading ? (
            <LoadingState rows={4} />
          ) : error ? (
            <ErrorState message={error} />
          ) : (
            <PerformanceHeatmap data={heatmapData} />
          )}
        </CardBody>
      </Card>
    </AppLayout>
  )
}
