import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'
import {
  type ColourBlindMode,
  getParseColorForMode,
  getDifficultyColorForMode,
} from '../constants/palettes'

interface ColourBlindContextValue {
  mode:               ColourBlindMode
  setMode:            (mode: ColourBlindMode) => void
  getParseColor:      (pct: number) => string
  getDifficultyColor: (diff: string) => string
}

const ColourBlindContext = createContext<ColourBlindContextValue | null>(null)

const STORAGE_KEY = 'sc-wcl-colour-mode'

function loadMode(): ColourBlindMode {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored === 'normal' || stored === 'deuteranopia' || stored === 'protanopia' || stored === 'tritanopia') {
      return stored
    }
  } catch { /* ignore */ }
  return 'normal'
}

export function ColourBlindProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ColourBlindMode>(loadMode)

  const setMode = useCallback((m: ColourBlindMode) => {
    setModeState(m)
    try { localStorage.setItem(STORAGE_KEY, m) } catch { /* ignore */ }
  }, [])

  const getParseColor      = useCallback((pct: number) => getParseColorForMode(pct, mode), [mode])
  const getDifficultyColor = useCallback((diff: string) => getDifficultyColorForMode(diff, mode), [mode])

  return (
    <ColourBlindContext.Provider value={{ mode, setMode, getParseColor, getDifficultyColor }}>
      {children}
    </ColourBlindContext.Provider>
  )
}

export function useColourBlind(): ColourBlindContextValue {
  const ctx = useContext(ColourBlindContext)
  if (!ctx) throw new Error('useColourBlind must be used within ColourBlindProvider')
  return ctx
}
