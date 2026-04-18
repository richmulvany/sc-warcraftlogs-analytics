import { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
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
  usePlayerDeathEvents,
  usePlayerAttendance,
  useBossKillRoster,
  useBossProgression,
  useEncounterCatalog,
  usePlayerBossPerformance,
  useRaidSummary,
  usePlayerCharacterMedia,
  usePlayerCharacterEquipment,
  usePlayerRaidAchievements,
  usePlayerMplusSummary,
  usePlayerMplusScoreHistory,
  usePlayerMplusWeeklyActivity,
  usePlayerMplusDungeonBreakdown,
} from '../hooks/useGoldData'
import { formatThroughput, getClassColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import { formatDate, formatPct } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import type {
  PlayerBossPerformance,
  PlayerCharacterEquipment,
  PlayerMplusDungeonBreakdown,
  PlayerMplusScoreHistory,
  PlayerMplusWeeklyActivity,
} from '../types'

type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'
type BossParseMode = 'average' | 'best'

const DIFFICULTIES: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']
const BOSS_PARSE_MODES: readonly { value: BossParseMode; label: string }[] = [
  { value: 'average', label: 'Average' },
  { value: 'best', label: 'Best' },
]
const COMPLETION_DIFFICULTIES: Exclude<DifficultyFilter, 'All'>[] = ['Mythic', 'Heroic', 'Normal']
const COMPLETION_COLORS: Record<Exclude<DifficultyFilter, 'All'>, string> = {
  Mythic: '#cba6f7',
  Heroic: '#89b4fa',
  Normal: '#a6e3a1',
}
const WARCRAFTLOGS_LINK_TITLE = 'view on warcraftlogs - opens in a new tab'
const RAIDERIO_LINK_TITLE = 'view on raider.io - opens in a new tab'

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

const ENCHANTABLE_SLOTS = new Set([
  'CHEST',
  'LEGS',
  'FEET',
  'FINGER_1',
  'FINGER_2',
  'MAIN_HAND',
])

const SOCKET_EXPECTED_SLOTS = new Set([
  'NECK',
  'FINGER_1',
  'FINGER_2',
])

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

interface TierCompletionBoss {
  name: string
  killed: boolean
}

interface TierCompletionRow {
  difficulty: Exclude<DifficultyFilter, 'All'>
  completed: number
  total: number
  pct: number
  bosses: TierCompletionBoss[]
}

interface KillingBlowSummary {
  name: string
  count: number
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

function parseKillingBlowsJson(value: unknown): KillingBlowSummary[] {
  if (typeof value !== 'string' || !value.trim()) return []
  try {
    const parsed = JSON.parse(value)
    if (!Array.isArray(parsed)) return []
    return parsed
      .map(row => ({
        name: typeof row?.name === 'string' ? row.name : '',
        count: Number(row?.count) || 0,
      }))
      .filter(row => row.name && row.count > 0)
  } catch {
    return []
  }
}

function warcraftLogsReportUrl(reportCode: string, fightId?: string | number): string | null {
  if (!reportCode) return null
  const fight = fightId ? `#fight=${encodeURIComponent(String(fightId))}` : ''
  return `https://www.warcraftlogs.com/reports/${encodeURIComponent(reportCode)}${fight}`
}

function formatNumber(value: unknown, digits = 0): string {
  const number = Number(value)
  if (!Number.isFinite(number) || number <= 0) return '—'
  return number.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })
}

function formatKeyLevel(value: unknown): string {
  const number = Number(value)
  return Number.isFinite(number) && number > 0 ? `+${number}` : '—'
}

function formatRealmName(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) return ''
  return value
    .replace(/[-_]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim()
}

function formatRealmSlug(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) return ''
  return value
    .replace(/([a-z])([A-Z])/g, '$1-$2')
    .replace(/[\s_]+/g, '-')
    .replace(/-+/g, '-')
    .toLowerCase()
    .trim()
}

function externalCharacterLinks(playerName: string, realm: unknown, fallbackRealm: unknown) {
  const realmSlug = formatRealmSlug(realm) || formatRealmSlug(fallbackRealm)
  if (!playerName || !realmSlug) return null

  const characterSlug = playerName.toLowerCase()
  return {
    raiderIo: `https://raider.io/characters/eu/${encodeURIComponent(realmSlug)}/${encodeURIComponent(playerName)}`,
    armory: `https://worldofwarcraft.blizzard.com/en-gb/character/eu/${encodeURIComponent(realmSlug)}/${encodeURIComponent(characterSlug)}`,
  }
}

function stripBlizzardTextureTokens(value: string): string {
  return value.replace(/\|A:[^|]*\|a/g, '').replace(/\s+/g, ' ').trim()
}

function professionQualityTier(value: string): string | null {
  const match = value.match(/Tier(\d+)/i)
  return match ? `ench t${match[1]}` : null
}

function enchantLabel(enchant: GearEnhancement): string {
  const raw = enchant.display_string || enchant.source_item_name || 'Unknown enchant'
  const clean = stripBlizzardTextureTokens(raw)
    .replace(/^Enchanted:\s*/i, '')
    .replace(/^Enchanted\s+/i, '')
    .trim()
  return clean
}

function enchantTierLabel(enchants: GearEnhancement[]): string | null {
  for (const enchant of enchants) {
    const raw = enchant.display_string || enchant.source_item_name || ''
    const tier = professionQualityTier(raw)
    if (tier) return tier
  }
  return null
}

function socketLabel(socket: GearEnhancement): string {
  return stripBlizzardTextureTokens(socket.item_name || socket.display_string || socket.socket_type || 'Empty socket')
}

function parseDifficultyNames(value: unknown): Set<string> {
  if (Array.isArray(value)) {
    return new Set(value.map(String).filter(Boolean))
  }

  if (typeof value !== 'string' || !value.trim()) {
    return new Set()
  }

  const matches = value.match(/Normal|Heroic|Mythic/gi) ?? []
  return new Set(matches.map(match => match[0].toUpperCase() + match.slice(1).toLowerCase()))
}

function clampTooltipPosition(clientX: number, clientY: number) {
  const width = 288
  const height = 420
  const offset = 14
  const margin = 12
  return {
    left: Math.min(Math.max(clientX + offset, margin), window.innerWidth - width - margin),
    top: Math.min(Math.max(clientY + offset, margin), window.innerHeight - height - margin),
  }
}

function ItemTooltip({
  item,
  color,
  position,
}: {
  item: PlayerCharacterEquipment
  color: string
  position: { left: number; top: number }
}) {
  const enchants = parseGearJson(item.enchantments_json)
  const sockets = parseGearJson(item.sockets_json)
  const stats = parseGearJson(item.stats_json)
  const spells = parseGearJson(item.spells_json)

  return (
    <div
      className="pointer-events-none fixed z-50 max-h-[calc(100vh-24px)] w-72 overflow-y-auto rounded-xl border border-ctp-surface2 bg-ctp-crust/95 p-4 text-left shadow-2xl backdrop-blur"
      style={{ left: position.left, top: position.top }}
    >
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
              Enchanted: {enchantLabel(enchant)}
            </p>
          ))}
        </div>
      )}

      {sockets.length > 0 && (
        <div className="mt-3 space-y-1">
          {sockets.map((socket, index) => (
            <p key={index} className="text-xs text-ctp-sapphire">
              {socketLabel(socket)}
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
  const [tooltipPosition, setTooltipPosition] = useState<{ left: number; top: number } | null>(null)
  const qualityColor = item ? (QUALITY_COLORS[item.quality] ?? classColor) : '#45475a'
  const enchants = item ? parseGearJson(item.enchantments_json) : []
  const sockets = item ? parseGearJson(item.sockets_json) : []
  const missingEnchant = Boolean(item && ENCHANTABLE_SLOTS.has(item.slot_type) && enchants.length === 0)
  const missingGem = Boolean(item && SOCKET_EXPECTED_SLOTS.has(item.slot_type) && sockets.length === 0)
  const slotBorderColor = missingEnchant ? '#f38ba8' : item ? qualityColor : '#313244'

  return (
    <div
      className="group relative flex min-h-[54px] items-center gap-2 rounded-xl border bg-ctp-surface0/55 px-2 py-1.5"
      style={{ borderColor: missingEnchant ? '#f38ba8' : item ? `${qualityColor}88` : '#45475a' }}
      onMouseMove={(event) => {
        if (!item) return
        setTooltipPosition(clampTooltipPosition(event.clientX, event.clientY))
      }}
      onMouseLeave={() => setTooltipPosition(null)}
    >
      <div
        className="relative flex h-10 w-10 flex-shrink-0 items-center justify-center overflow-hidden rounded-lg border bg-ctp-crust/80"
        style={{ borderColor: slotBorderColor }}
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
        {(enchants.length > 0 || sockets.length > 0 || missingEnchant || missingGem) && (
          <div className="mt-0.5 flex items-center gap-1.5">
            {enchants.length > 0 && (
              <span className="text-[9px] font-mono text-ctp-green">
                {enchantTierLabel(enchants) ?? 'ench'}
              </span>
            )}
            {missingEnchant && <span className="text-[9px] font-mono text-ctp-red">missing enchant</span>}
            {sockets.slice(0, 3).map((socket, index) => (
              <span key={index} className="h-1.5 w-1.5 rounded-full bg-ctp-sapphire" title={socket.item_name || socket.socket_type} />
            ))}
            {missingGem && (
              <span className="inline-flex items-center gap-1 text-[9px] font-mono text-ctp-red">
                <span className="h-1.5 w-1.5 rounded-full bg-ctp-red" />
                missing gem
              </span>
            )}
          </div>
        )}
      </div>
      {item && tooltipPosition && <ItemTooltip item={item} color={qualityColor} position={tooltipPosition} />}
    </div>
  )
}

function CompletionTooltip({
  row,
  position,
}: {
  row: TierCompletionRow
  position: { left: number; top: number }
}) {
  const killed = row.bosses.filter(boss => boss.killed)
  const missing = row.bosses.filter(boss => !boss.killed)

  return (
    <div
      className="pointer-events-none fixed z-50 w-72 rounded-xl border border-ctp-surface2 bg-ctp-crust/95 p-4 text-left shadow-2xl backdrop-blur"
      style={{ left: position.left, top: position.top }}
    >
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-ctp-text">{row.difficulty}</p>
        <p className="text-xs font-mono text-ctp-subtext1">{row.completed}/{row.total || '—'}</p>
      </div>

      {row.total === 0 ? (
        <p className="mt-3 text-xs font-mono text-ctp-overlay0">
          No bosses are exported for this difficulty in the selected tier.
        </p>
      ) : (
        <div className="mt-3 grid grid-cols-1 gap-1.5">
          <div>
            {killed.length > 0 ? (
              <div className="space-y-1">
                {killed.map(boss => (
                  <p key={boss.name} className="truncate text-xs text-ctp-text">
                    <span className="mr-2 text-ctp-green">✓</span>{boss.name}
                  </p>
                ))}
              </div>
            ) : (
              <p className="text-xs font-mono text-ctp-overlay0">None yet</p>
            )}
          </div>

          <div>
            {missing.length > 0 ? (
              <div className="space-y-1">
                {missing.map(boss => (
                  <p key={boss.name} className="truncate text-xs text-ctp-subtext1">
                    <span className="mr-2 text-ctp-red">×</span>{boss.name}
                  </p>
                ))}
              </div>
            ) : (
              <p className="text-xs font-mono text-ctp-overlay0">Complete</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function CompletionRow({ row, color }: { row: TierCompletionRow; color: string }) {
  const [tooltipPosition, setTooltipPosition] = useState<{ left: number; top: number } | null>(null)

  return (
    <div
      className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/60 px-3 py-3 transition-colors hover:border-ctp-surface2"
      onMouseMove={(event) => setTooltipPosition(clampTooltipPosition(event.clientX, event.clientY))}
      onMouseLeave={() => setTooltipPosition(null)}
    >
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-sm font-semibold" style={{ color }}>
          {row.difficulty}
        </p>
        <p className="text-sm font-mono text-ctp-text">
          {row.completed}/{row.total || '—'}
        </p>
      </div>
      <ProgressBar value={row.pct} color={color} height="sm" />
      {tooltipPosition && <CompletionTooltip row={row} position={tooltipPosition} />}
    </div>
  )
}

function SectionDivider({ label, subtitle }: { label: string; subtitle?: string }) {
  return (
    <div className="flex items-center gap-4 py-2">
      <div className="h-px flex-1 bg-gradient-to-r from-transparent via-ctp-surface2 to-ctp-surface1" />
      <div className="text-center">
        <p className="text-[11px] font-mono uppercase tracking-[0.28em] text-ctp-mauve">
          {label}
        </p>
        {subtitle && (
          <p className="mt-1 text-[10px] font-mono text-ctp-overlay0">
            {subtitle}
          </p>
        )}
      </div>
      <div className="h-px flex-1 bg-gradient-to-l from-transparent via-ctp-surface2 to-ctp-surface1" />
    </div>
  )
}

function MplusScoreChart({ data }: { data: PlayerMplusScoreHistory[] }) {
  const chartData = data.map(row => ({
    date: row.snapshot_date || row.snapshot_at,
    score: Number(row.score_all) || 0,
  }))

  if (chartData.length < 2) {
    return (
      <div className="flex h-56 items-center justify-center rounded-xl border border-dashed border-ctp-surface2 bg-ctp-crust/35 px-6 text-center">
        <p className="text-xs font-mono text-ctp-overlay0">
          Score history starts from the first Raider.IO ingestion. Another snapshot is needed for a trend line.
        </p>
      </div>
    )
  }

  return (
    <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
          <CartesianGrid stroke="#313244" strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ background: '#11111b', border: '1px solid #45475a', borderRadius: 12 }}
            labelStyle={{ color: '#cdd6f4' }}
          />
          <Line type="monotone" dataKey="score" stroke="#cba6f7" strokeWidth={2.5} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function MplusWeeklyChart({ data }: { data: PlayerMplusWeeklyActivity[] }) {
  const chartData = data.map(row => ({
    week: row.week_start,
    timed: Number(row.timed_runs) || 0,
    untimed: Number(row.untimed_runs) || 0,
  }))

  if (chartData.length === 0) {
    return (
      <div className="flex h-56 items-center justify-center rounded-xl border border-dashed border-ctp-surface2 bg-ctp-crust/35 px-6 text-center">
        <p className="text-xs font-mono text-ctp-overlay0">No Mythic+ run activity has been exported for this character yet.</p>
      </div>
    )
  }

  return (
    <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
          <CartesianGrid stroke="#313244" strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="week" tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis allowDecimals={false} tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ background: '#11111b', border: '1px solid #45475a', borderRadius: 12 }}
            labelStyle={{ color: '#cdd6f4' }}
          />
          <Bar dataKey="timed" stackId="runs" fill="#a6e3a1" radius={[4, 4, 0, 0]} />
          <Bar dataKey="untimed" stackId="runs" fill="#f38ba8" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function DungeonBreakdownCard({ row }: { row: PlayerMplusDungeonBreakdown }) {
  const timedPct = Number(row.total_runs) > 0 ? (Number(row.timed_runs) / Number(row.total_runs)) * 100 : 0
  const href = row.best_run_url || undefined

  return (
    <a
      href={href}
      target={href ? '_blank' : undefined}
      rel={href ? 'noopener noreferrer' : undefined}
      title={href ? RAIDERIO_LINK_TITLE : undefined}
      className="group rounded-xl border border-ctp-surface1 bg-ctp-surface0/55 p-3 transition-all hover:border-ctp-mauve/50 hover:bg-ctp-surface0"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-sm font-semibold text-ctp-text transition-colors group-hover:text-ctp-mauve">{row.dungeon}</p>
          <p className="mt-0.5 text-[10px] font-mono text-ctp-overlay0">
            {Number(row.total_runs) || 0} runs · {Number(row.timed_runs) || 0} timed
          </p>
        </div>
        <div className="text-right">
          <p className="text-lg font-semibold text-ctp-mauve">{formatKeyLevel(row.best_key_level)}</p>
          <p className="text-[10px] font-mono text-ctp-overlay0">{formatNumber(row.best_score, 1)} score</p>
        </div>
      </div>
      <ProgressBar value={timedPct} color="#a6e3a1" height="xs" className="mt-3" />
      <p className="mt-2 text-[10px] font-mono text-ctp-overlay0">
        Latest: {row.latest_completed_at ? formatDate(row.latest_completed_at) : '—'}
      </p>
    </a>
  )
}

export function PlayerDetail() {
  const { getParseColor, wipeColor, getDeathRateColor, getAttendanceColor } = useColourBlind()
  const { playerName } = useParams<{ playerName: string }>()
  const navigate = useNavigate()

  const name = decodeURIComponent(playerName ?? '')

  const perf = usePlayerPerformance()
  const surv = usePlayerSurvivability()
  const deathEvents = usePlayerDeathEvents()
  const att = usePlayerAttendance()
  const raids = useRaidSummary()
  const roster = useBossKillRoster()
  const bossProgression = useBossProgression()
  const encounterCatalog = useEncounterCatalog()
  const bossPf = usePlayerBossPerformance()
  const characterMedia = usePlayerCharacterMedia()
  const characterEquipment = usePlayerCharacterEquipment()
  const raidAchievements = usePlayerRaidAchievements()
  const mplusSummary = usePlayerMplusSummary()
  const mplusScoreHistory = usePlayerMplusScoreHistory()
  const mplusWeeklyActivity = usePlayerMplusWeeklyActivity()
  const mplusDungeonBreakdown = usePlayerMplusDungeonBreakdown()

  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [bossParseMode, setBossParseMode] = useState<BossParseMode>('average')

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

  const currentRaidTier = useMemo(() => {
    const latestProgressionTier = [...bossProgression.data]
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row => hasRealText(row.zone_name))
      .filter(row => hasRealText(row.last_attempt_date) || hasRealText(row.first_kill_date))
      .sort((a, b) => {
        const aDate = a.last_attempt_date || a.first_kill_date || ''
        const bDate = b.last_attempt_date || b.first_kill_date || ''
        return String(bDate).localeCompare(String(aDate))
      })[0]?.zone_name

    return latestProgressionTier || currentTier || tierOptions.find(option => option !== 'All') || ''
  }, [bossProgression.data, currentTier, tierOptions])

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
      reportCode: r.report_code,
      fightId: r.fight_id,
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

  const scopedDeathRows = useMemo(() =>
    deathEvents.data
      .filter(row => row.player_name === name)
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row =>
        (selectedTier === 'All' || !selectedTier || row.zone_name === selectedTier) &&
        (difficulty === 'All' || row.difficulty_label === difficulty) &&
        (selectedBoss === 'All' || row.boss_name === selectedBoss)
      ),
    [deathEvents.data, name, selectedTier, difficulty, selectedBoss]
  )

  const reportHrefByBossKey = useMemo(() => {
    const map = new Map<string, string>()
    const latestRows = [...scopedRosterRows]
      .filter(row => row.report_code)
      .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))

    for (const row of latestRows) {
      const key = `${row.encounter_id}-${row.difficulty}`
      if (map.has(key)) continue
      const href = warcraftLogsReportUrl(row.report_code, row.fight_id)
      if (href) map.set(key, href)
    }

    return map
  }, [scopedRosterRows])

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

  const scopedSurvivability = useMemo(() => {
    if (deathEvents.data.length === 0) return null

    const killingBlowCounts = new Map<string, number>()
    for (const row of scopedDeathRows) {
      if (!hasRealText(row.killing_blow_name)) continue
      killingBlowCounts.set(row.killing_blow_name, (killingBlowCounts.get(row.killing_blow_name) ?? 0) + 1)
    }

    const killingBlows = [...killingBlowCounts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 3)
      .map(([name, count]) => ({ name, count }))

    const kills = scopedSummary?.kills ?? scopedRosterRows.length
    const totalDeaths = scopedDeathRows.length

    return {
      totalDeaths,
      deathsPerKill: totalDeaths / Math.max(kills, 1),
      killingBlows,
      isScoped: true,
    }
  }, [deathEvents.data.length, scopedDeathRows, scopedRosterRows.length, scopedSummary])

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

  const profileLinks = useMemo(
    () => externalCharacterLinks(name, playerMedia?.realm_slug, summary?.realm),
    [name, playerMedia?.realm_slug, summary?.realm]
  )

  const playerMplusSummary = useMemo(
    () => mplusSummary.data.find(row => row.player_name.toLowerCase() === name.toLowerCase()) ?? null,
    [mplusSummary.data, name]
  )

  const playerMplusScoreHistory = useMemo(
    () => [...mplusScoreHistory.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => String(a.snapshot_at).localeCompare(String(b.snapshot_at))),
    [mplusScoreHistory.data, name]
  )

  const playerMplusWeeklyActivity = useMemo(
    () => [...mplusWeeklyActivity.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => String(a.week_start).localeCompare(String(b.week_start))),
    [mplusWeeklyActivity.data, name]
  )

  const playerMplusDungeonBreakdown = useMemo(
    () => [...mplusDungeonBreakdown.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) =>
        (Number(b.best_score) || 0) - (Number(a.best_score) || 0) ||
        (Number(b.best_key_level) || 0) - (Number(a.best_key_level) || 0) ||
        a.dungeon.localeCompare(b.dungeon)
      ),
    [mplusDungeonBreakdown.data, name]
  )

  const hasMplusData = Boolean(
    playerMplusSummary ||
    playerMplusScoreHistory.length > 0 ||
    playerMplusWeeklyActivity.length > 0 ||
    playerMplusDungeonBreakdown.length > 0
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

  const playerItemLevel = useMemo(() => {
    const latestRosterRow = [...playerRosterRows]
      .filter(row => Number.isFinite(Number(row.avg_item_level)) && Number(row.avg_item_level) > 0)
      .sort((a, b) =>
        String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? ''))
      )[0]
    if (latestRosterRow) {
      return Number(latestRosterRow.avg_item_level)
    }

    const summaryLevel = Number(summary?.avg_item_level)
    return Number.isFinite(summaryLevel) && summaryLevel > 0 ? summaryLevel : null
  }, [playerRosterRows, summary])

  const survivabilityKillingBlows = useMemo(() => {
    if (scopedSurvivability) return scopedSurvivability.killingBlows
    if (!survRow) return []
    const parsed = parseKillingBlowsJson(survRow.top_killing_blows_json)
    if (parsed.length > 0) return parsed.slice(0, 3)
    if (survRow.most_common_killing_blow) {
      return [{
        name: survRow.most_common_killing_blow,
        count: Number(survRow.most_common_killing_blow_count) || 0,
      }]
    }
    return []
  }, [scopedSurvivability, survRow])

  const displayedTotalDeaths = scopedSurvivability?.totalDeaths ?? survRow?.total_deaths ?? 0
  const displayedDeathsPerKill = scopedSurvivability?.deathsPerKill ?? survRow?.deaths_per_kill
  const hasSurvivabilityData = Boolean(scopedSurvivability || survRow)

  const currentTierProgress = useMemo((): TierCompletionRow[] => {
    if (!currentRaidTier) return []

    return COMPLETION_DIFFICULTIES.map(diff => {
      const catalogBossRows = encounterCatalog.data
          .filter(row => isIncludedZoneName(row.zone_name))
          .filter(row => row.zone_name === currentRaidTier)
          .filter(row => hasRealText(row.encounter_name))
          .filter(row => {
            const difficultyNames = parseDifficultyNames(row.difficulty_names)
            return difficultyNames.size === 0 || difficultyNames.has(diff)
          })

      const totalBosses = new Map<string, { firstSeen: string; lastSeen: string }>()
      if (catalogBossRows.length > 0) {
        for (const row of catalogBossRows) {
          totalBosses.set(row.encounter_name, { firstSeen: '', lastSeen: '' })
        }
      } else {
        const tierBossRows = bossProgression.data
          .filter(row => isIncludedZoneName(row.zone_name))
          .filter(row => row.zone_name === currentRaidTier)
          .filter(row => hasRealText(row.boss_name))

        for (const row of tierBossRows) {
          const existing = totalBosses.get(row.boss_name)
          const firstSeen = String(row.first_kill_date || row.last_attempt_date || '')
          const lastSeen = String(row.last_attempt_date || row.first_kill_date || '')
          if (!existing) {
            totalBosses.set(row.boss_name, { firstSeen, lastSeen })
            continue
          }
          if (firstSeen && (!existing.firstSeen || firstSeen < existing.firstSeen)) existing.firstSeen = firstSeen
          if (lastSeen && (!existing.lastSeen || lastSeen > existing.lastSeen)) existing.lastSeen = lastSeen
        }
      }

      const playerBosses = new Set(
        playerRosterRows
          .filter(row => row.zone_name === currentRaidTier)
          .filter(row => row.difficulty_label === diff)
          .map(row => row.boss_name)
          .filter(hasRealText)
      )

      const bosses = [...totalBosses.entries()]
        .sort((a, b) => {
          const aSort = a[1].firstSeen || a[1].lastSeen || a[0]
          const bSort = b[1].firstSeen || b[1].lastSeen || b[0]
          const byDate = String(aSort).localeCompare(String(bSort))
          return byDate === 0 ? a[0].localeCompare(b[0]) : byDate
        })
        .map(([bossName]) => ({
          name: bossName,
          killed: playerBosses.has(bossName),
        }))

      return {
        difficulty: diff,
        completed: playerBosses.size,
        total: totalBosses.size,
        pct: totalBosses.size > 0 ? (playerBosses.size / totalBosses.size) * 100 : 0,
        bosses,
      }
    })
  }, [bossProgression.data, currentRaidTier, encounterCatalog.data, playerRosterRows])

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
                <span className="text-xs text-ctp-overlay1 font-mono">{formatRealmName(summary.realm)}</span>
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
          {profileLinks && (
            <div className="ml-auto flex items-center gap-2">
              <a
                href={profileLinks.raiderIo}
                target="_blank"
                rel="noopener noreferrer"
                title="view on raider.io - opens in a new tab"
                className="group inline-flex h-10 w-10 items-center justify-center rounded-lg transition-all hover:-translate-y-0.5 hover:drop-shadow-[0_0_12px_rgba(203,166,247,0.45)]"
              >
                <img
                  src="/rio-icon.png"
                  alt="Raider.IO"
                  className="h-8 w-8 object-contain opacity-80 transition-opacity group-hover:opacity-100"
                />
              </a>
              <a
                href={profileLinks.armory}
                target="_blank"
                rel="noopener noreferrer"
                title="view on world of warcraft - opens in a new tab"
                className="group inline-flex h-10 w-10 items-center justify-center rounded-lg transition-all hover:-translate-y-0.5 hover:drop-shadow-[0_0_12px_rgba(137,180,250,0.45)]"
              >
                <img
                  src="/wow-icon.png"
                  alt="World of Warcraft Armory"
                  className="h-8 w-8 object-contain opacity-80 transition-opacity group-hover:opacity-100"
                />
              </a>
            </div>
          )}
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
              value={displayedDeathsPerKill != null ? displayedDeathsPerKill.toFixed(1) : '—'}
              subValue={`${displayedTotalDeaths} total deaths${scopedSurvivability ? ' in scope' : ' · aggregate'}`}
              icon="☠"
              valueColor={displayedDeathsPerKill != null ? getDeathRateColor(displayedDeathsPerKill) : undefined}
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
              <div className="flex items-center justify-between gap-4">
                <div>
                  <CardTitle>Equipped Gear</CardTitle>
                  <p className="text-xs text-ctp-overlay1 mt-0.5">
                    Blizzard profile snapshot
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">Item Level</p>
                  <p className="mt-0.5 text-lg font-semibold text-ctp-text">
                    {playerItemLevel ? playerItemLevel.toFixed(1) : '—'}
                  </p>
                </div>
              </div>
            </CardHeader>
            <CardBody>
              {characterEquipment.loading ? (
                <LoadingState rows={6} />
              ) : playerEquipment.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No equipment found for this character.
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

          <div className="xl:col-span-2">
            <SectionDivider
              label="Raid Performance"
              subtitle="WarcraftLogs-derived raid progress, parses, deaths, and boss history"
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Current Tier Completion</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                {currentRaidTier || 'No tier'}
              </p>
            </CardHeader>
            <CardBody>
              {roster.loading || bossProgression.loading || encounterCatalog.loading ? (
                <LoadingState rows={3} />
              ) : currentTierProgress.every(row => row.total === 0) ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No current-tier boss completion data found for this character.
                </p>
              ) : (
                <div className="space-y-4">
                  {currentTierProgress.map(row => (
                    <CompletionRow key={row.difficulty} row={row} color={COMPLETION_COLORS[row.difficulty]} />
                  ))}
                </div>
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Raid Feats</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                Cutting Edge + Famed Slayer
              </p>
            </CardHeader>
            <CardBody>
              {raidAchievements.loading ? (
                <LoadingState rows={4} />
              ) : playerRaidAchievements.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No raid feats found for this character.
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

      <div className="grid grid-cols-1 items-stretch gap-6 lg:grid-cols-2">
        <Card className="h-full">
          <CardHeader>
            <CardTitle>Parse Breakdown by Fight</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Individual WCL rank % per recorded fight in scope</p>
          </CardHeader>
          <CardBody className="max-h-[318px] space-y-2.5 overflow-y-auto pr-3">
            {roster.loading ? (
              <LoadingState rows={6} />
            ) : dpsOverTime.length === 0 ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No fight data available in the current scope</p>
            ) : (
              [...dpsOverTime]
                .filter(d => d.parse > 0)
                .sort((a, b) => String(b.date).localeCompare(String(a.date)))
                .map((d, i) => (
                  <a
                    key={`${d.reportCode}-${d.fightId}-${i}`}
                    href={warcraftLogsReportUrl(d.reportCode, d.fightId) ?? undefined}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={WARCRAFTLOGS_LINK_TITLE}
                    className="group flex items-center gap-3 rounded-lg px-1.5 py-1 transition-colors hover:bg-ctp-surface0/70"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-ctp-subtext1 truncate transition-colors group-hover:text-ctp-mauve">{d.boss}</span>
                        <span className="text-xs font-mono font-semibold flex-shrink-0 ml-2" style={{ color: getParseColor(d.parse) }}>
                          {d.parse.toFixed(0)}%
                        </span>
                      </div>
                      <ProgressBar value={d.parse} color={getParseColor(d.parse)} height="xs" />
                    </div>
                    <div className="flex-shrink-0 text-right">
                      <span className="block text-[10px] font-mono text-ctp-overlay0 transition-colors group-hover:text-ctp-overlay1">
                        {formatDate(d.date)}
                      </span>
                      <span className="block text-[9px] font-mono text-ctp-overlay0 opacity-0 transition-opacity group-hover:opacity-100">
                        WCL ↗
                      </span>
                    </div>
                  </a>
                ))
            )}
          </CardBody>
        </Card>

        <Card className="h-full">
          <CardHeader>
            <CardTitle>Survivability</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              {scopedSurvivability ? 'Death analysis for the current page filters' : 'Aggregate death analysis for this player'}
            </p>
          </CardHeader>
          <CardBody>
            {surv.loading || deathEvents.loading ? (
              <LoadingState rows={4} />
            ) : !hasSurvivabilityData ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No death data recorded</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Total Deaths</p>
                    <p className="text-xl font-semibold" style={{ color: wipeColor }}>{displayedTotalDeaths}</p>
                  </div>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Deaths / Kill</p>
                    <p className="text-xl font-semibold" style={{ color: getDeathRateColor(Number(displayedDeathsPerKill) || 0) }}>
                      {displayedDeathsPerKill != null ? displayedDeathsPerKill.toFixed(1) : '—'}
                    </p>
                  </div>
                </div>

                <div>
                  <p className="section-label mb-2">Most Common Killing Blows</p>
                  <div className="space-y-2">
                    {survivabilityKillingBlows.length > 0 ? (
                      survivabilityKillingBlows.map((blow, index) => (
                        <div key={`${blow.name}-${index}`} className="flex items-center justify-between gap-3 rounded-xl bg-ctp-surface1/40 p-3">
                          <div className="min-w-0">
                            <p className="text-sm font-medium text-ctp-text truncate">
                              {blow.name}
                            </p>
                          </div>
                          <p className="flex-shrink-0 text-xs font-mono text-ctp-subtext1">
                            {blow.count} hit{blow.count !== 1 ? 's' : ''}
                          </p>
                        </div>
                      ))
                    ) : (
                      <div className="bg-ctp-surface1/40 rounded-xl p-3">
                        <p className="text-sm font-medium text-ctp-text">—</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>Performance by Boss</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                {bossParseMode === 'best' ? 'Best' : 'Average'} WCL parse % per boss encounter in scope · colour = parse tier
              </p>
            </div>
            <FilterTabs
              options={BOSS_PARSE_MODES}
              value={bossParseMode}
              onChange={setBossParseMode}
              className="flex-shrink-0"
              buttonClassName="px-3 py-1.5"
            />
          </div>
        </CardHeader>
        <CardBody>
          {bossPf.loading ? (
            <LoadingState rows={4} />
          ) : error ? (
            <ErrorState message={error} />
          ) : (
            <PerformanceHeatmap
              data={heatmapData}
              getHref={(row) => reportHrefByBossKey.get(`${row.encounter_id}-${row.difficulty}`) ?? null}
              externalLinkTitle={WARCRAFTLOGS_LINK_TITLE}
              parseMode={bossParseMode}
            />
          )}
        </CardBody>
      </Card>

      <SectionDivider
        label="Mythic+ Performance"
        subtitle="Raider.IO-backed dungeon score and key history"
      />

      {!hasMplusData && !mplusSummary.loading && !mplusScoreHistory.loading ? (
        <Card>
          <CardHeader>
            <CardTitle>Mythic+ Progression</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              No Raider.IO Mythic+ data has been exported for this character yet.
            </p>
          </CardHeader>
        </Card>
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
            <StatCard
              label="Raider.IO Score"
              value={formatNumber(playerMplusSummary?.score_all, 1)}
              subValue={playerMplusSummary?.world_rank ? `World #${formatNumber(playerMplusSummary.world_rank)}` : 'Current season'}
              icon="◆"
              valueColor="#cba6f7"
              accent="none"
            />
            <StatCard
              label="Best Timed Key"
              value={formatKeyLevel(playerMplusSummary?.highest_timed_level)}
              subValue={playerMplusSummary?.best_run_dungeon || 'No timed key exported'}
              icon="⏱"
              valueColor="#a6e3a1"
              accent="none"
            />
            <StatCard
              label="Timed / Untimed"
              value={`${Number(playerMplusSummary?.timed_runs) || 0} / ${Number(playerMplusSummary?.untimed_runs) || 0}`}
              subValue={`${Number(playerMplusSummary?.total_runs) || 0} exported runs`}
              icon="◒"
              accent="blue"
            />
            <StatCard
              label="Common Key Level"
              value={formatKeyLevel(playerMplusSummary?.most_common_key_level)}
              subValue={`${Number(playerMplusSummary?.most_common_key_count) || 0} runs at that level`}
              icon="◇"
              valueColor="#cba6f7"
              accent="none"
            />
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Score Over Time</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Nightly Raider.IO score snapshots from Databricks ingestion
                </p>
              </CardHeader>
              <CardBody>
                {mplusScoreHistory.loading ? (
                  <LoadingState rows={5} />
                ) : (
                  <MplusScoreChart data={playerMplusScoreHistory} />
                )}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Dungeon Activity</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  Weekly completed keys from recent/best Raider.IO runs
                </p>
              </CardHeader>
              <CardBody>
                {mplusWeeklyActivity.loading ? (
                  <LoadingState rows={5} />
                ) : (
                  <MplusWeeklyChart data={playerMplusWeeklyActivity} />
                )}
              </CardBody>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <div className="flex items-start justify-between gap-4">
                <div>
                  <CardTitle>Dungeon Breakdown</CardTitle>
                  <p className="text-xs text-ctp-overlay1 mt-0.5">
                    Best exported key per dungeon · cards link to Raider.IO where available
                  </p>
                </div>
                {playerMplusSummary?.profile_url && (
                  <a
                    href={playerMplusSummary.profile_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={RAIDERIO_LINK_TITLE}
                    className="text-xs font-mono text-ctp-mauve transition-colors hover:text-ctp-pink"
                  >
                    Raider.IO ↗
                  </a>
                )}
              </div>
            </CardHeader>
            <CardBody>
              {mplusDungeonBreakdown.loading ? (
                <LoadingState rows={6} />
              ) : playerMplusDungeonBreakdown.length === 0 ? (
                <p className="py-8 text-center text-xs font-mono text-ctp-overlay0">
                  No dungeon breakdown rows exported for this character yet.
                </p>
              ) : (
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                  {playerMplusDungeonBreakdown.map(row => (
                    <DungeonBreakdownCard key={`${row.dungeon}-${row.best_completed_at}`} row={row} />
                  ))}
                </div>
              )}
            </CardBody>
          </Card>
        </div>
      )}
    </AppLayout>
  )
}
