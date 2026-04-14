/**
 * Colourblind-aware palette system.
 *
 * Each mode provides a complete set of semantic colours. Palette choices
 * are based on Okabe-Ito colourblind-safe principles:
 *   – Deuteranopia / Protanopia (red-green blind): replace red/green with
 *     orange/blue. Both modes see orange and sky-blue as clearly different
 *     and distinct from the dark dashboard background.
 *   – Tritanopia (blue-yellow blind): blue looks teal/green, yellow looks
 *     pink. Use red/green/purple instead of blue/yellow.
 */

export type ColourBlindMode = 'normal' | 'deuteranopia' | 'protanopia' | 'tritanopia'

interface ColourPalette {
  // Parse tiers (WCL rank %)
  parseLegendary: string  // ≥99
  parseEpic:      string  // ≥95
  parseGreat:     string  // ≥75
  parseGood:      string  // ≥50
  parseOk:        string  // ≥25
  parseGrey:      string  // <25

  // Boss difficulty
  diffMythic:  string
  diffHeroic:  string
  diffNormal:  string
  diffLFR:     string

  // Player roles
  roleDps:     string
  roleHealer:  string
  roleTank:    string

  // Kill vs wipe (the most critical red/green distinction)
  kill:        string   // positive — boss killed, attendance present
  wipe:        string   // negative — wipe, death, absence

  // Chart series (primary = kills/positive, secondary = wipes/negative)
  chartA:      string
  chartB:      string
}

const PALETTES: Record<ColourBlindMode, ColourPalette> = {
  // ── Normal ─────────────────────────────────────────────────────────────────
  normal: {
    parseLegendary: '#f5c2e7',  // ctp-pink   (legendary)
    parseEpic:      '#fab387',  // ctp-peach  (epic)
    parseGreat:     '#cba6f7',  // ctp-mauve  (great)
    parseGood:      '#89b4fa',  // ctp-blue
    parseOk:        '#a6e3a1',  // ctp-green
    parseGrey:      '#6c7086',  // ctp-overlay0

    diffMythic:  '#cba6f7',  // purple
    diffHeroic:  '#89b4fa',  // blue
    diffNormal:  '#a6e3a1',  // green
    diffLFR:     '#6c7086',

    roleDps:     '#f38ba8',  // ctp-red
    roleHealer:  '#94e2d5',  // ctp-teal
    roleTank:    '#89b4fa',  // ctp-blue

    kill:    '#a6e3a1',  // ctp-green
    wipe:    '#f38ba8',  // ctp-red

    chartA:  '#89b4fa',  // blue  (kills line)
    chartB:  '#f38ba8',  // red   (wipes line)
  },

  // ── Deuteranopia (red-green, most common ~8% of males) ────────────────────
  // Red and green are confused — replaced with orange and sky-blue.
  deuteranopia: {
    parseLegendary: '#FFD700',  // gold — unmistakeable top tier
    parseEpic:      '#E69F00',  // Okabe orange
    parseGreat:     '#CC79A7',  // Okabe pink/rose — visible to deuteranopes
    parseGood:      '#56B4E9',  // Okabe sky blue
    parseOk:        '#B0C4DE',  // lighter steel blue — lower tier
    parseGrey:      '#6c7086',

    diffMythic:  '#CC79A7',  // rose/pink — premium tier
    diffHeroic:  '#E69F00',  // orange
    diffNormal:  '#56B4E9',  // sky blue (normal green replaced)
    diffLFR:     '#6c7086',

    roleDps:     '#E69F00',  // orange (replaces red)
    roleHealer:  '#56B4E9',  // sky blue (replaces teal-green)
    roleTank:    '#0072B2',  // dark blue — clearly different from sky blue

    kill:    '#56B4E9',  // sky blue (replaces green)
    wipe:    '#E69F00',  // orange   (replaces red)

    chartA:  '#56B4E9',  // sky blue (kills)
    chartB:  '#E69F00',  // orange   (wipes)
  },

  // ── Protanopia (red blind, ~1% of males) ─────────────────────────────────
  // Red appears very dark/black. Green is still somewhat perceivable.
  // Similar fixes to deuteranopia — orange/blue remain safe.
  protanopia: {
    parseLegendary: '#FFD700',  // gold
    parseEpic:      '#E69F00',  // Okabe orange
    parseGreat:     '#CC79A7',  // Okabe pink
    parseGood:      '#56B4E9',  // Okabe sky blue
    parseOk:        '#009E73',  // Okabe teal — visible to protanopes (not red)
    parseGrey:      '#6c7086',

    diffMythic:  '#CC79A7',  // pink
    diffHeroic:  '#E69F00',  // orange
    diffNormal:  '#56B4E9',  // sky blue
    diffLFR:     '#6c7086',

    roleDps:     '#E69F00',  // orange
    roleHealer:  '#009E73',  // teal (visible, not confused with orange)
    roleTank:    '#0072B2',  // dark blue

    kill:    '#009E73',  // teal
    wipe:    '#E69F00',  // orange

    chartA:  '#009E73',  // teal (kills)
    chartB:  '#E69F00',  // orange (wipes)
  },

  // ── Tritanopia (blue-yellow blind, rare ~0.003%) ──────────────────────────
  // Blue appears green/teal, yellow appears violet/pink.
  // Red and green remain distinguishable — safe to use them.
  tritanopia: {
    parseLegendary: '#FF1493',  // deep pink — unambiguous top tier
    parseEpic:      '#FF6347',  // tomato red
    parseGreat:     '#9370DB',  // medium purple — violet visible in tritanopia
    parseGood:      '#3CB371',  // sea green
    parseOk:        '#90EE90',  // light green — lighter than above
    parseGrey:      '#6c7086',

    diffMythic:  '#FF6347',  // red — premium tier
    diffHeroic:  '#9370DB',  // purple
    diffNormal:  '#3CB371',  // sea green
    diffLFR:     '#6c7086',

    roleDps:     '#FF6347',  // tomato red
    roleHealer:  '#3CB371',  // sea green
    roleTank:    '#9370DB',  // purple (replacing blue which looks green)

    kill:    '#3CB371',  // sea green
    wipe:    '#FF6347',  // tomato red

    chartA:  '#3CB371',  // sea green (kills)
    chartB:  '#FF6347',  // red       (wipes)
  },
}

// ── Derived getter functions ─────────────────────────────────────────────────

export function getPaletteForMode(mode: ColourBlindMode): ColourPalette {
  return PALETTES[mode]
}

export function getParseColorForMode(pct: number, mode: ColourBlindMode): string {
  const p = PALETTES[mode]
  if (pct >= 99) return p.parseLegendary
  if (pct >= 95) return p.parseEpic
  if (pct >= 75) return p.parseGreat
  if (pct >= 50) return p.parseGood
  if (pct >= 25) return p.parseOk
  return p.parseGrey
}

export function getDifficultyColorForMode(diff: string, mode: ColourBlindMode): string {
  const p = PALETTES[mode]
  const map: Record<string, string> = {
    'Mythic': p.diffMythic, 'Heroic': p.diffHeroic,
    'Normal': p.diffNormal, 'LFR':    p.diffLFR,
    '5': p.diffMythic, '4': p.diffHeroic, '3': p.diffNormal,
  }
  return map[diff] ?? '#6c7086'
}

export function getRoleColorForMode(role: string, mode: ColourBlindMode): string {
  const p = PALETTES[mode]
  const r = role?.toLowerCase()
  if (r === 'dps')    return p.roleDps
  if (r === 'healer') return p.roleHealer
  if (r === 'tank')   return p.roleTank
  return '#6c7086'
}

export const MODE_LABELS: Record<ColourBlindMode, string> = {
  normal:       'Normal',
  deuteranopia: 'Deuteranopia',
  protanopia:   'Protanopia',
  tritanopia:   'Tritanopia',
}
