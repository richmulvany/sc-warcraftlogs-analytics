import { useCSV } from './useCSV'
import type {
  RaidSummary,
  PlayerPerformanceSummary,
  BossProgression,
  BossKillRosterRow,
  PlayerAttendance,
  GuildRoster,
  WeeklyActivity,
  BossWipeAnalysis,
  BossProgressHistoryRow,
  PlayerSurvivability,
  ProgressionTimeline,
  RaidTeamMember,
  LiveRaidRosterEntry,
  BestKill,
  BossMechanics,
  PlayerBossPerformance,
} from '../types'

export const useRaidSummary        = () => useCSV<RaidSummary>('gold_raid_summary.csv')
export const usePlayerPerformance  = () => useCSV<PlayerPerformanceSummary>('gold_player_performance_summary.csv')
export const useBossProgression    = () => useCSV<BossProgression>('gold_boss_progression.csv')
export const useBossKillRoster     = () => useCSV<BossKillRosterRow>('gold_boss_kill_roster.csv')
export const usePlayerAttendance   = () => useCSV<PlayerAttendance>('gold_player_attendance.csv')
export const useGuildRoster        = () => useCSV<GuildRoster>('gold_guild_roster.csv')
export const useWeeklyActivity     = () => useCSV<WeeklyActivity>('gold_weekly_activity.csv')
export const useBossWipeAnalysis   = () => useCSV<BossWipeAnalysis>('gold_boss_wipe_analysis.csv')
export const useBossProgressHistory = () => useCSV<BossProgressHistoryRow>('gold_boss_progress_history.csv', { optional: true })
export const usePlayerSurvivability = () => useCSV<PlayerSurvivability>('gold_player_survivability.csv')
export const useProgressionTimeline = () => useCSV<ProgressionTimeline>('gold_progression_timeline.csv')
export const useRaidTeam           = () => useCSV<RaidTeamMember>('gold_raid_team.csv')
export const useLiveRaidRoster     = () => useCSV<LiveRaidRosterEntry>('live_raid_roster.csv', { optional: true })
export const useBestKills          = () => useCSV<BestKill>('gold_best_kills.csv')
export const useBossMechanics      = () => useCSV<BossMechanics>('gold_boss_mechanics.csv')
export const usePlayerBossPerformance = () => useCSV<PlayerBossPerformance>('gold_player_boss_performance.csv')
