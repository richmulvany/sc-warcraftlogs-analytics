// WoW class colour map — keys match the player_class values in gold tables.
// Note: the pipeline outputs "DeathKnight" (no space) and "DemonHunter".
// Colours are slightly adjusted from Blizzard-official for dark-mode readability:
//   Priest   #E8E8E8 → #B8B8C8 (less blindingly white on dark bg)
//   Monk     #00FF98 → #00CC7A (less neon)
//   Rogue    #FFF468 → #DDD24A (less neon)
//   Shaman   #2459FF → #4B7AFF (was too dark on dark bg)
export const CLASS_COLORS: Record<string, string> = {
  // Space variants (WCL output)
  'Death Knight': '#C41E3A',
  'Demon Hunter': '#A330C9',
  'Druid':        '#FF7C0A',
  'Evoker':       '#33937F',
  'Hunter':       '#AAD372',
  'Mage':         '#3FC7EB',
  'Monk':         '#00CC7A',
  'Paladin':      '#F48CBA',
  'Priest':       '#B8B8C8',
  'Rogue':        '#DDD24A',
  'Shaman':       '#4B7AFF',
  'Warlock':      '#8788EE',
  'Warrior':      '#C69B3A',
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

export const ROLE_COLORS: Record<string, string> = {
  dps:    '#F87171',
  healer: '#4ADE80',
  tank:   '#60A5FA',
}

export const ROLE_LABELS: Record<string, string> = {
  dps:    'DPS',
  healer: 'Healer',
  tank:   'Tank',
}

export function getRoleColor(role: string): string {
  return ROLE_COLORS[role?.toLowerCase()] ?? '#6B7280'
}

export const DIFFICULTY_COLORS: Record<string, string> = {
  'Normal':  '#4ADE80',
  'Heroic':  '#3FC7EB',
  'Mythic':  '#C084FC',
  'LFR':     '#6B7280',
  '3':       '#4ADE80',
  '4':       '#3FC7EB',
  '5':       '#C084FC',
}

export const DIFFICULTY_ORDER: Record<string, number> = {
  'LFR':    0,
  'Normal': 1,
  'Heroic': 2,
  'Mythic': 3,
}

export function getDifficultyColor(diff: string): string {
  return DIFFICULTY_COLORS[diff] ?? '#6B7280'
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

// Parse percent → colour band (WCL-inspired, tuned for Catppuccin Mocha)
export function getParseColor(pct: number): string {
  if (pct >= 99) return '#f5c2e7' // ctp-pink  (legendary)
  if (pct >= 95) return '#fab387' // ctp-peach (orange)
  if (pct >= 75) return '#cba6f7' // ctp-mauve (purple)
  if (pct >= 50) return '#89b4fa' // ctp-blue
  if (pct >= 25) return '#a6e3a1' // ctp-green
  return '#6c7086'                // ctp-overlay0 (grey)
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
