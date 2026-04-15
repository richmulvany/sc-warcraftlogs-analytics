// WoW class colour map — keys match the player_class values in gold tables.
// Note: the pipeline outputs "DeathKnight" (no space) and "DemonHunter".
// Near-Blizzard-official colours with minimal dark-mode adjustments:
//   Priest   #E8E8E8 → #DCDCDC (barely darker — keeps silver quality)
//   Monk     #00FF98 → #00EE8A (very slight neon reduction)
//   Rogue    #FFF468 → #F5E040 (very slight neon reduction)
//   Shaman   #2459FF → #4B7AFF (was too dark against dark bg, rest are original)
export const CLASS_COLORS: Record<string, string> = {
  // Space variants (WCL output)
  'Death Knight': '#C41E3A',
  'Demon Hunter': '#A330C9',
  'Druid':        '#FF7C0A',
  'Evoker':       '#33937F',
  'Hunter':       '#AAD372',
  'Mage':         '#3FC7EB',
  'Monk':         '#00EE8A',
  'Paladin':      '#F48CBA',
  'Priest':       '#DCDCDC',
  'Rogue':        '#FFF468',
  'Shaman':       '#4B7AFF',
  'Warlock':      '#8788EE',
  'Warrior':      '#C69B6D',
  // No-space variants (sometimes output by pipeline)
  'DeathKnight':  '#C41E3A',
  'DemonHunter':  '#A330C9',
  // Fallback
  'Unknown':      '#6B7280',
  'unknown':      '#6B7280',
}

export function getClassColor(cls: string): string {
  return CLASS_COLORS[cls] ?? '#6B7280'
}

export const ROLE_LABELS: Record<string, string> = {
  dps:    'DPS',
  healer: 'Healer',
  tank:   'Tank',
}

export function normaliseRole(role: string | null | undefined): string {
  const value = String(role ?? '').trim().toLowerCase()
  if (value.startsWith('heal')) return 'healer'
  if (value.startsWith('tank')) return 'tank'
  if (value.startsWith('dps')) return 'dps'
  return value
}

export const DIFFICULTY_ORDER: Record<string, number> = {
  'LFR':    0,
  'Normal': 1,
  'Heroic': 2,
  'Mythic': 3,
}

export const RANK_CATEGORY_COLORS: Record<string, string> = {
  'GM':      '#f9e2af',  // ctp-yellow
  'Officer': '#cba6f7',  // ctp-mauve
  'Raider':  '#89b4fa',  // ctp-blue
  'Social':  '#6c7086',  // ctp-overlay0
}

export function getRankColor(category: string): string {
  return RANK_CATEGORY_COLORS[category] ?? '#6c7086'
}

// Format seconds as "Xm Ys"
export function formatDuration(seconds: number): string {
  if (!seconds || isNaN(seconds)) return '—'
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

// Format big DPS/HPS numbers
export function formatThroughput(value: number): string {
  if (!value || isNaN(value)) return '—'
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`
  return String(Math.round(value))
}

export function getThroughputColor(role: string | null | undefined): string {
  return normaliseRole(role) === 'healer' ? '#a6e3a1' : '#f38ba8'
}
