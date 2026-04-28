export type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'
export type BossParseMode = 'average' | 'best'
export type MplusHeatmapMode = 'level' | 'quantity'

export interface GearEnhancement {
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

export interface TierCompletionBoss {
  name: string
  killed: boolean
}

export interface TierCompletionRow {
  difficulty: Exclude<DifficultyFilter, 'All'>
  completed: number
  total: number
  pct: number
  bosses: TierCompletionBoss[]
}

export interface KillingBlowSummary {
  name: string
  count: number
}

export interface ScopedSummary {
  avgThroughput: number | null
  bestThroughput: number
  avgRank: number | null
  bestRank: number
  kills: number
  firstSeen: string | undefined
  lastSeen: string | undefined
  playerClass: string
  role: string
  spec: string
}

export interface ScopedSurvivability {
  totalDeaths: number
  deathsPerKill: number
  killingBlows: KillingBlowSummary[]
  isScoped: boolean
}

export interface ScopedAttendance {
  raidsPresent: number
  totalRaidsTracked: number
  attendanceRatePct: number
}

export interface TeamDeathRank {
  rank: number
  total: number
  deaths: number
  kills: number
  deathsPerKill: number
}

export interface ProfileLinks {
  raiderIo: string
  armory: string
}

export interface DpsDataPoint {
  date: string
  throughput: number
  boss: string
  parse: number | null
  reportCode: string
  fightId: string | number | undefined
}
