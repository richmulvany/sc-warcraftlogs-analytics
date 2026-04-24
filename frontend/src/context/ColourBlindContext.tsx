import { createContext, useContext, useState, useCallback, useMemo, type ReactNode } from 'react'
import {
  type ColourBlindMode,
  getPaletteForMode,
  getParseColorForMode,
  getDifficultyColorForMode,
  getRoleColorForMode,
  getPhaseColorsForMode,
  getAttendanceColorForMode,
  getDeathRateColorForMode,
  getTopTierColorForMode,
} from '../constants/palettes'

interface ChartColors {
  primary:   string   // kills / positive series
  secondary: string   // wipes / negative series
}

export interface ColourBlindContextValue {
  mode:               ColourBlindMode
  setMode:            (mode: ColourBlindMode) => void
  // Per-value getters
  getParseColor:      (pct: number) => string
  getDifficultyColor: (diff: string) => string
  getRoleColor:       (role: string) => string
  getAttendanceColor: (pct: number) => string
  getDeathRateColor:  (deathsPerKill: number) => string
  // Semantic colours (access directly where needed)
  killColor:    string
  wipeColor:    string
  /** Top-tier colour — legendary parse / best-ever values (gold/pink/trophy) */
  topTierColor: string
  chartColors:  ChartColors
  /** Four distinct colours safe for each mode — for phase/category/bucket breakdowns */
  phaseColors:  [string, string, string, string]
}

const ColourBlindContext = createContext<ColourBlindContextValue | null>(null)

const STORAGE_KEY = 'sc-wcl-colour-mode'

function loadMode(): ColourBlindMode {
  try {
    const s = localStorage.getItem(STORAGE_KEY)
    if (s === 'normal' || s === 'deuteranopia' || s === 'protanopia' || s === 'tritanopia') return s
  } catch { /* ignore */ }
  return 'normal'
}

export function ColourBlindProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ColourBlindMode>(loadMode)

  const setMode = useCallback((m: ColourBlindMode) => {
    setModeState(m)
    try { localStorage.setItem(STORAGE_KEY, m) } catch { /* ignore */ }
  }, [])

  const value = useMemo((): ColourBlindContextValue => {
    const palette = getPaletteForMode(mode)
    return {
      mode,
      setMode,
      getParseColor:      (pct)  => getParseColorForMode(pct, mode),
      getDifficultyColor: (diff) => getDifficultyColorForMode(diff, mode),
      getRoleColor:       (role) => getRoleColorForMode(role, mode),
      getAttendanceColor: (pct)  => getAttendanceColorForMode(pct, mode),
      getDeathRateColor:  (dpk)  => getDeathRateColorForMode(dpk, mode),
      killColor:    palette.kill,
      wipeColor:    palette.wipe,
      topTierColor: getTopTierColorForMode(mode),
      chartColors:  { primary: palette.chartA, secondary: palette.chartB },
      phaseColors:  getPhaseColorsForMode(mode),
    }
  }, [mode, setMode])

  return (
    <ColourBlindContext.Provider value={value}>
      {children}
    </ColourBlindContext.Provider>
  )
}

export function useColourBlind(): ColourBlindContextValue {
  const ctx = useContext(ColourBlindContext)
  if (!ctx) throw new Error('useColourBlind must be used within ColourBlindProvider')
  return ctx
}
