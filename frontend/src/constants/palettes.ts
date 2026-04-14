// Colourblind-safe parse tier colours
// Based on Okabe-Ito colourblind-safe palette principles
// Each mode has 6 tiers: legendary (≥99), epic (≥95), great (≥75), good (≥50), ok (≥25), grey

export type ColourBlindMode = 'normal' | 'deuteranopia' | 'protanopia' | 'tritanopia'

interface ParsePalette {
  legendary: string  // ≥99
  epic:      string  // ≥95
  great:     string  // ≥75
  good:      string  // ≥50
  ok:        string  // ≥25
  grey:      string  // <25
}

const PARSE_PALETTES: Record<ColourBlindMode, ParsePalette> = {
  // Normal: Catppuccin Mocha
  normal: {
    legendary: '#f5c2e7', // pink
    epic:      '#fab387', // peach
    great:     '#cba6f7', // mauve/purple
    good:      '#89b4fa', // blue
    ok:        '#a6e3a1', // green
    grey:      '#6c7086',
  },
  // Deuteranopia (red-green blind): use blue/orange/yellow — no green
  deuteranopia: {
    legendary: '#FFD700', // gold
    epic:      '#FF8C00', // dark orange
    great:     '#5B9BD5', // steel blue
    good:      '#87CEEB', // sky blue
    ok:        '#B0C4DE', // light steel blue
    grey:      '#6c7086',
  },
  // Protanopia (red blind, similar to deuteranopia but no red perception)
  protanopia: {
    legendary: '#FFD700', // gold
    epic:      '#E69F00', // orange (Okabe-Ito orange)
    great:     '#56B4E9', // sky blue (Okabe-Ito)
    good:      '#009E73', // teal-green (Okabe-Ito)
    ok:        '#F0E442', // yellow (Okabe-Ito)
    grey:      '#6c7086',
  },
  // Tritanopia (blue-yellow blind): no blue/yellow — use red/green/pink
  tritanopia: {
    legendary: '#FF69B4', // hot pink
    epic:      '#FF6347', // tomato red
    great:     '#9370DB', // medium purple
    good:      '#3CB371', // medium sea green
    ok:        '#90EE90', // light green
    grey:      '#6c7086',
  },
}

export function getParseColorForMode(pct: number, mode: ColourBlindMode): string {
  const p = PARSE_PALETTES[mode]
  if (pct >= 99) return p.legendary
  if (pct >= 95) return p.epic
  if (pct >= 75) return p.great
  if (pct >= 50) return p.good
  if (pct >= 25) return p.ok
  return p.grey
}

// Difficulty colours per mode
type DifficultyPalette = {
  [key: string]: string
  Normal: string
  Heroic: string
  Mythic: string
  LFR:    string
}

const DIFFICULTY_PALETTES: Record<ColourBlindMode, DifficultyPalette> = {
  normal:       { Normal: '#4ADE80', Heroic: '#3FC7EB', Mythic: '#C084FC', LFR: '#6B7280' },
  deuteranopia: { Normal: '#87CEEB', Heroic: '#5B9BD5', Mythic: '#FFD700', LFR: '#6B7280' },
  protanopia:   { Normal: '#009E73', Heroic: '#56B4E9', Mythic: '#E69F00', LFR: '#6B7280' },
  tritanopia:   { Normal: '#3CB371', Heroic: '#9370DB', Mythic: '#FF6347', LFR: '#6B7280' },
}

export function getDifficultyColorForMode(diff: string, mode: ColourBlindMode): string {
  const p = DIFFICULTY_PALETTES[mode]
  return p[diff] ?? '#6B7280'
}

export const MODE_LABELS: Record<ColourBlindMode, string> = {
  normal:       'Normal',
  deuteranopia: 'Deuteranopia',
  protanopia:   'Protanopia',
  tritanopia:   'Tritanopia',
}
