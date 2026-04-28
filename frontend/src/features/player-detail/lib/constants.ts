import type { DifficultyFilter, BossParseMode, MplusHeatmapMode } from './types'

export const PD_SECTIONS = [
  { id: 'profile', label: 'Profile' },
  { id: 'raid',    label: 'Raid' },
  { id: 'mplus',   label: 'M+' },
] as const

export const DIFFICULTIES: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']

export const BOSS_PARSE_MODES: readonly { value: BossParseMode; label: string }[] = [
  { value: 'average', label: 'Average' },
  { value: 'best', label: 'Best' },
]

export const MPLUS_HEATMAP_MODES: readonly { value: MplusHeatmapMode; label: string }[] = [
  { value: 'level', label: 'Key Level' },
  { value: 'quantity', label: 'Quantity' },
]

export const COMPLETION_DIFFICULTIES: Exclude<DifficultyFilter, 'All'>[] = ['Mythic', 'Heroic', 'Normal']

export const WARCRAFTLOGS_LINK_TITLE = 'view on warcraftlogs - opens in a new tab'
export const RAIDERIO_LINK_TITLE = 'view on raider.io - opens in a new tab'

export const HEATMAP_WEEKS = 53
export const HEATMAP_CELL = 15
export const HEATMAP_GAP = 4
export const HEATMAP_EMPTY_CELL = '#262735'

export const EQUIPMENT_SLOTS = [
  { type: 'HEAD',       label: 'Head',        side: 'left' as const },
  { type: 'NECK',       label: 'Neck',        side: 'left' as const },
  { type: 'SHOULDER',   label: 'Shoulder',    side: 'left' as const },
  { type: 'BACK',       label: 'Back',        side: 'left' as const },
  { type: 'CHEST',      label: 'Chest',       side: 'left' as const },
  { type: 'WRIST',      label: 'Wrist',       side: 'left' as const },
  { type: 'HANDS',      label: 'Hands',       side: 'left' as const },
  { type: 'WAIST',      label: 'Waist',       side: 'left' as const },
  { type: 'LEGS',       label: 'Legs',        side: 'left' as const },
  { type: 'FEET',       label: 'Feet',        side: 'left' as const },
  { type: 'FINGER_1',   label: 'Ring 1',      side: 'right' as const },
  { type: 'FINGER_2',   label: 'Ring 2',      side: 'right' as const },
  { type: 'TRINKET_1',  label: 'Trinket 1',   side: 'right' as const },
  { type: 'TRINKET_2',  label: 'Trinket 2',   side: 'right' as const },
  { type: 'MAIN_HAND',  label: 'Main Hand',   side: 'right' as const },
  { type: 'OFF_HAND',   label: 'Off Hand',    side: 'right' as const },
  { type: 'RANGED',     label: 'Ranged',      side: 'right' as const },
  { type: 'SHIRT',      label: 'Shirt',       side: 'bottom' as const },
  { type: 'TABARD',     label: 'Tabard',      side: 'bottom' as const },
]

export const EQUIPMENT_SLOT_ORDER: string[] = EQUIPMENT_SLOTS.map(slot => slot.type)

export const QUALITY_COLORS: Record<string, string> = {
  POOR:      '#9d9d9d',
  COMMON:    '#ffffff',
  UNCOMMON:  '#1eff00',
  RARE:      '#0070dd',
  EPIC:      '#a335ee',
  LEGENDARY: '#ff8000',
  ARTIFACT:  '#e6cc80',
  HEIRLOOM:  '#00ccff',
}

export const ENCHANTABLE_SLOTS = new Set([
  'BACK', 'CHEST', 'WRIST', 'LEGS', 'FEET', 'FINGER_1', 'FINGER_2', 'MAIN_HAND', 'OFF_HAND',
])

export const SOCKET_EXPECTED_SLOTS = new Set([
  'HEAD', 'NECK', 'WRIST', 'FINGER_1', 'FINGER_2', 'TRINKET_1', 'TRINKET_2',
])
