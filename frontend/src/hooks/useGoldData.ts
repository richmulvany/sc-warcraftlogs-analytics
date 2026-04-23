import { useCSV } from './useCSV'
import type {
  RaidSummary,
  PlayerPerformanceSummary,
  BossProgression,
  EncounterCatalog,
  BossKillRosterRow,
  PlayerAttendance,
  GuildRoster,
  WeeklyActivity,
  GuildZoneRank,
  BossWipeAnalysis,
  BossProgressHistoryRow,
  BossPullHistoryRow,
  PlayerSurvivability,
  PlayerDeathEvent,
  PlayerUtilityByPull,
  WipeSurvivalEvent,
  WipeCooldownUtilization,
  ProgressionTimeline,
  RaidTeamMember,
  LiveRaidRosterEntry,
  PlayerCharacterMedia,
  PlayerCharacterEquipment,
  PlayerRaidAchievement,
  BestKill,
  BossMechanics,
  PlayerBossPerformance,
  PlayerMplusSummary,
  PlayerMplusScoreHistory,
  PlayerMplusRunHistory,
  PlayerMplusWeeklyActivity,
  PlayerMplusDungeonBreakdown,
  PlayerConsumables,
  PlayerCombatStats,
} from '../types'

export const useRaidSummary        = () => useCSV<RaidSummary>('gold_raid_summary.csv')
export const usePlayerPerformance  = () => useCSV<PlayerPerformanceSummary>('gold_player_performance_summary.csv')
export const useBossProgression    = () => useCSV<BossProgression>('gold_boss_progression.csv')
export const useEncounterCatalog   = () => useCSV<EncounterCatalog>('gold_encounter_catalog.csv', { optional: true })
export const useBossKillRoster     = () => useCSV<BossKillRosterRow>('gold_boss_kill_roster.csv')
export const usePlayerAttendance   = () => useCSV<PlayerAttendance>('gold_player_attendance.csv')
export const usePlayerUtilityByPull = () => useCSV<PlayerUtilityByPull>('gold_player_utility_by_pull.csv', { optional: true })
export const useWipeSurvivalEvents = () => useCSV<WipeSurvivalEvent>('gold_wipe_survival_events.csv', { optional: true })
export const useWipeCooldownUtilization = () => useCSV<WipeCooldownUtilization>('gold_wipe_cooldown_utilization.csv', { optional: true })
export const useGuildRoster        = () => useCSV<GuildRoster>('gold_guild_roster.csv')
export const useWeeklyActivity     = () => useCSV<WeeklyActivity>('gold_weekly_activity.csv')
export const useGuildZoneRanks     = () => useCSV<GuildZoneRank>('guild_zone_ranks.csv', { optional: true })
export const useBossWipeAnalysis   = () => useCSV<BossWipeAnalysis>('gold_boss_wipe_analysis.csv')
export const useBossProgressHistory = () => useCSV<BossProgressHistoryRow>('gold_boss_progress_history.csv', { optional: true })
export const useBossPullHistory    = () => useCSV<BossPullHistoryRow>('gold_boss_pull_history.csv', { optional: true })
export const usePlayerSurvivability = () => useCSV<PlayerSurvivability>('gold_player_survivability.csv')
export const usePlayerDeathEvents  = () => useCSV<PlayerDeathEvent>('gold_player_death_events.csv', { optional: true })
export const useProgressionTimeline = () => useCSV<ProgressionTimeline>('gold_progression_timeline.csv')
export const useRaidTeam           = () => useCSV<RaidTeamMember>('gold_raid_team.csv')
export const useLiveRaidRoster     = () => useCSV<LiveRaidRosterEntry>('live_raid_roster.csv', { optional: true })
export const usePlayerCharacterMedia = () => useCSV<PlayerCharacterMedia>('player_character_media.csv', { optional: true })
export const usePlayerCharacterEquipment = () => useCSV<PlayerCharacterEquipment>('player_character_equipment.csv', { optional: true })
export const usePlayerRaidAchievements = () => useCSV<PlayerRaidAchievement>('player_raid_achievements.csv', { optional: true })
export const useBestKills          = () => useCSV<BestKill>('gold_best_kills.csv')
export const useBossMechanics      = () => useCSV<BossMechanics>('gold_boss_mechanics.csv')
export const usePlayerBossPerformance = () => useCSV<PlayerBossPerformance>('gold_player_boss_performance.csv')
export const usePlayerMplusSummary = () => useCSV<PlayerMplusSummary>('gold_player_mplus_summary.csv', { optional: true })
export const usePlayerMplusScoreHistory = () => useCSV<PlayerMplusScoreHistory>('gold_player_mplus_score_history.csv', { optional: true })
export const usePlayerMplusRunHistory = () => useCSV<PlayerMplusRunHistory>('gold_player_mplus_run_history.csv', { optional: true })
export const usePlayerMplusWeeklyActivity = () => useCSV<PlayerMplusWeeklyActivity>('gold_player_mplus_weekly_activity.csv', { optional: true })
export const usePlayerMplusDungeonBreakdown = () => useCSV<PlayerMplusDungeonBreakdown>('gold_player_mplus_dungeon_breakdown.csv', { optional: true })
export const usePlayerConsumables  = () => useCSV<PlayerConsumables>('gold_player_consumables.csv', { optional: true })
export const usePlayerCombatStats  = () => useCSV<PlayerCombatStats>('gold_player_combat_stats.csv', { optional: true })
