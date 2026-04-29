import { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { SectionNav, useActiveSection } from '../../components/ui/SectionNav'
import { AppLayout } from '../../components/layout/AppLayout'
import {
  usePlayerPerformance,
  usePlayerSurvivability,
  usePlayerDeathEvents,
  usePlayerAttendance,
  useBossKillRoster,
  useBossProgression,
  useEncounterCatalog,
  usePlayerBossPerformance,
  useRaidSummary,
  usePlayerCharacterMedia,
  usePlayerCharacterEquipment,
  usePlayerRaidAchievements,
  usePlayerMplusSummary,
  usePlayerMplusScoreHistory,
  usePlayerMplusRunHistory,
  usePlayerMplusDungeonBreakdown,
  useRaidTeam,
} from '../../hooks/useGoldData'
import { getClassColor } from '../../constants/wow'
import { toFiniteNumber, meanIgnoringNulls, hasRealText } from '../../utils/format'
import { isIncludedZoneName } from '../../utils/zones'
import type { PlayerBossPerformance } from '../../types'
import { PD_SECTIONS, COMPLETION_DIFFICULTIES, EQUIPMENT_SLOT_ORDER } from './lib/constants'
import { parseKillingBlowsJson, parseDifficultyNames } from './lib/gear'
import { externalCharacterLinks, warcraftLogsReportUrl } from './lib/utils'
import type { DifficultyFilter, BossParseMode, MplusHeatmapMode, ScopedSummary, ScopedSurvivability, ScopedAttendance, TeamDeathRank } from './lib/types'
import { ProfileSection } from './sections/ProfileSection'
import { RaidSection } from './sections/RaidSection'
import { MplusSection } from './sections/MplusSection'

export function PlayerDetail() {
  const { playerName } = useParams<{ playerName: string }>()
  const navigate = useNavigate()

  const name = decodeURIComponent(playerName ?? '')

  const perf = usePlayerPerformance()
  const surv = usePlayerSurvivability()
  const deathEvents = usePlayerDeathEvents()
  const att = usePlayerAttendance()
  const raids = useRaidSummary()
  const roster = useBossKillRoster()
  const bossProgression = useBossProgression()
  const encounterCatalog = useEncounterCatalog()
  const bossPf = usePlayerBossPerformance()
  const characterMedia = usePlayerCharacterMedia()
  const characterEquipment = usePlayerCharacterEquipment()
  const raidAchievements = usePlayerRaidAchievements()
  const raidTeam = useRaidTeam()
  const mplusSummary = usePlayerMplusSummary()
  const mplusScoreHistory = usePlayerMplusScoreHistory()
  const mplusRunHistory = usePlayerMplusRunHistory()
  const mplusDungeonBreakdown = usePlayerMplusDungeonBreakdown()

  const activeSectionId = useActiveSection(PD_SECTIONS)

  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [bossParseMode, setBossParseMode] = useState<BossParseMode>('average')
  const [mplusHeatmapMode, setMplusHeatmapMode] = useState<MplusHeatmapMode>('quantity')

  const summary = useMemo(
    () => perf.data.find(p => p.player_name === name),
    [perf.data, name]
  )

  const survRow = useMemo(
    () => surv.data.find(s => s.player_name === name),
    [surv.data, name]
  )

  const attRow = useMemo(
    () => att.data.find(a => a.player_name === name),
    [att.data, name]
  )

  const playerRosterRows = useMemo(() =>
    roster.data
      .filter(r => r.player_name === name && Number(r.throughput_per_second) > 0)
      .filter(r => isIncludedZoneName(r.zone_name))
      .sort((a, b) => String(a.raid_night_date).localeCompare(String(b.raid_night_date))),
    [roster.data, name]
  )

  const tierOptions = useMemo(() =>
    ['All', ...new Set(
      [...playerRosterRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
        .filter(hasRealText)
    )],
    [playerRosterRows]
  )

  const currentTier = tierOptions[1] ?? ''

  const currentRaidTier = useMemo(() => {
    const latestProgressionTier = [...bossProgression.data]
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row => hasRealText(row.zone_name))
      .filter(row => hasRealText(row.last_attempt_date) || hasRealText(row.first_kill_date))
      .sort((a, b) => {
        const aDate = a.last_attempt_date || a.first_kill_date || ''
        const bDate = b.last_attempt_date || b.first_kill_date || ''
        return String(bDate).localeCompare(String(aDate))
      })[0]?.zone_name

    return latestProgressionTier || currentTier || tierOptions.find(option => option !== 'All') || ''
  }, [bossProgression.data, currentTier, tierOptions])

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const bossOptions = useMemo(() => {
    const bosses = [...new Set(
      playerRosterRows
        .filter(r => selectedTier === 'All' || !selectedTier || r.zone_name === selectedTier)
        .filter(r => difficulty === 'All' || r.difficulty_label === difficulty)
        .map(r => r.boss_name)
        .filter(hasRealText)
    )].sort()
    return ['All', ...bosses]
  }, [playerRosterRows, selectedTier, difficulty])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const scopedRosterRows = useMemo(() =>
    playerRosterRows.filter(r =>
      (selectedTier === 'All' || !selectedTier || r.zone_name === selectedTier) &&
      (difficulty === 'All' || r.difficulty_label === difficulty) &&
      (selectedBoss === 'All' || r.boss_name === selectedBoss)
    ),
    [playerRosterRows, selectedTier, difficulty, selectedBoss]
  )

  const dpsOverTime = useMemo(() =>
    scopedRosterRows.map(r => ({
      date: String(r.raid_night_date ?? ''),
      throughput: Number(r.throughput_per_second),
      boss: String(r.boss_name ?? ''),
      parse: toFiniteNumber(r.rank_percent),
      reportCode: r.report_code,
      fightId: r.fight_id as string | number | undefined,
    })),
    [scopedRosterRows]
  )

  const scopedBossPerformance = useMemo(
    () => bossPf.data.filter(b =>
      b.player_name === name &&
      (selectedTier === 'All' || !selectedTier || b.zone_name === selectedTier) &&
      (difficulty === 'All' || b.difficulty_label === difficulty) &&
      (selectedBoss === 'All' || b.boss_name === selectedBoss)
    ),
    [bossPf.data, name, selectedTier, difficulty, selectedBoss]
  )

  const fallbackBossPerformance = useMemo((): PlayerBossPerformance[] => {
    type Acc = PlayerBossPerformance & {
      _rankSum: number; _rankCount: number
      _throughputSum: number; _throughputCount: number
      _ilvlSum: number; _ilvlCount: number
    }
    const grouped = new Map<string, Acc>()

    for (const row of scopedRosterRows) {
      const key = `${row.encounter_id}-${row.difficulty}`
      const throughput = toFiniteNumber(row.throughput_per_second)
      const rank = toFiniteNumber(row.rank_percent)
      const ilvl = toFiniteNumber(row.avg_item_level)
      const existing = grouped.get(key)
      if (!existing) {
        grouped.set(key, {
          player_name: row.player_name,
          player_class: row.player_class,
          role: row.role,
          primary_spec: row.spec,
          encounter_id: row.encounter_id,
          boss_name: row.boss_name,
          zone_name: row.zone_name,
          difficulty: row.difficulty,
          difficulty_label: row.difficulty_label,
          kills_on_boss: 1,
          avg_throughput_per_second: throughput ?? 0,
          best_throughput_per_second: throughput ?? 0,
          latest_throughput_per_second: throughput ?? 0,
          throughput_trend: 0,
          avg_rank_percent: rank ?? 0,
          best_rank_percent: rank ?? 0,
          avg_item_level: ilvl ?? 0,
          first_kill_date: '',
          latest_kill_date: '',
          _rankSum: rank ?? 0,
          _rankCount: rank != null ? 1 : 0,
          _throughputSum: throughput ?? 0,
          _throughputCount: throughput != null ? 1 : 0,
          _ilvlSum: ilvl ?? 0,
          _ilvlCount: ilvl != null ? 1 : 0,
        })
      } else {
        existing.kills_on_boss++
        if (throughput != null) {
          existing._throughputSum += throughput
          existing._throughputCount++
          existing.avg_throughput_per_second = existing._throughputSum / existing._throughputCount
          if (throughput > existing.best_throughput_per_second) existing.best_throughput_per_second = throughput
          existing.latest_throughput_per_second = throughput
        }
        if (rank != null) {
          existing._rankSum += rank
          existing._rankCount++
          existing.avg_rank_percent = existing._rankSum / existing._rankCount
          if (rank > existing.best_rank_percent) existing.best_rank_percent = rank
        }
        if (ilvl != null) {
          existing._ilvlSum += ilvl
          existing._ilvlCount++
          existing.avg_item_level = existing._ilvlSum / existing._ilvlCount
        }
      }
    }

    return [...grouped.values()].map((row) => {
      const result: Partial<Acc> = { ...row }
      delete result._rankSum
      delete result._rankCount
      delete result._throughputSum
      delete result._throughputCount
      delete result._ilvlSum
      delete result._ilvlCount
      return result as PlayerBossPerformance
    })
  }, [scopedRosterRows])

  const heatmapData = scopedBossPerformance.length > 0 ? scopedBossPerformance : fallbackBossPerformance

  const scopedDeathRows = useMemo(() =>
    deathEvents.data
      .filter(row => row.player_name === name)
      .filter(row => isIncludedZoneName(row.zone_name))
      .filter(row =>
        (selectedTier === 'All' || !selectedTier || row.zone_name === selectedTier) &&
        (difficulty === 'All' || row.difficulty_label === difficulty) &&
        (selectedBoss === 'All' || row.boss_name === selectedBoss)
      ),
    [deathEvents.data, name, selectedTier, difficulty, selectedBoss]
  )

  const teamDeathRank = useMemo((): TeamDeathRank | null => {
    const raidTeamNames = new Set(
      raidTeam.data
        .map(row => row.name)
        .filter(hasRealText)
        .map(player => player.toLowerCase())
    )

    const scopedParticipants = new Set(
      roster.data
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row =>
          (selectedTier === 'All' || !selectedTier || row.zone_name === selectedTier) &&
          (difficulty === 'All' || row.difficulty_label === difficulty) &&
          (selectedBoss === 'All' || row.boss_name === selectedBoss)
        )
        .map(row => row.player_name)
        .filter(hasRealText)
        .filter(player => raidTeamNames.size === 0 || raidTeamNames.has(player.toLowerCase()))
    )

    const targetName = name.toLowerCase()
    if (scopedParticipants.size === 0 || !scopedParticipants.has(name)) return null

    const playerStats = new Map<string, { deaths: number; kills: number }>()
    scopedParticipants.forEach(player => playerStats.set(player.toLowerCase(), { deaths: 0, kills: 0 }))

    for (const row of roster.data) {
      if (!hasRealText(row.player_name)) continue
      const playerKey = row.player_name.toLowerCase()
      const stats = playerStats.get(playerKey)
      if (!stats) continue
      if (!isIncludedZoneName(row.zone_name)) continue
      if (selectedTier !== 'All' && selectedTier && row.zone_name !== selectedTier) continue
      if (difficulty !== 'All' && row.difficulty_label !== difficulty) continue
      if (selectedBoss !== 'All' && row.boss_name !== selectedBoss) continue
      stats.kills += 1
    }

    for (const row of deathEvents.data) {
      if (!hasRealText(row.player_name)) continue
      const playerKey = row.player_name.toLowerCase()
      const stats = playerStats.get(playerKey)
      if (!stats) continue
      if (!isIncludedZoneName(row.zone_name)) continue
      if (selectedTier !== 'All' && selectedTier && row.zone_name !== selectedTier) continue
      if (difficulty !== 'All' && row.difficulty_label !== difficulty) continue
      if (selectedBoss !== 'All' && row.boss_name !== selectedBoss) continue
      stats.deaths += 1
    }

    const rankedStats = [...playerStats.values()]
      .filter(stats => stats.kills > 0)
      .map(stats => ({ ...stats, deathsPerKill: stats.deaths / stats.kills }))
      .sort((a, b) => a.deathsPerKill - b.deathsPerKill)

    const playerStatsForRank = playerStats.get(targetName)
    if (!playerStatsForRank || playerStatsForRank.kills === 0) return null

    const playerDeathsPerKill = playerStatsForRank.deaths / playerStatsForRank.kills
    return {
      rank: rankedStats.findIndex(stats => stats.deathsPerKill === playerDeathsPerKill) + 1,
      total: rankedStats.length,
      deaths: playerStatsForRank.deaths,
      kills: playerStatsForRank.kills,
      deathsPerKill: playerDeathsPerKill,
    }
  }, [deathEvents.data, difficulty, name, raidTeam.data, roster.data, selectedBoss, selectedTier])

  const reportHrefByBossKey = useMemo(() => {
    const map = new Map<string, string>()
    const latestRows = [...scopedRosterRows]
      .filter(row => row.report_code)
      .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))

    for (const row of latestRows) {
      const key = `${row.encounter_id}-${row.difficulty}`
      if (map.has(key)) continue
      const href = warcraftLogsReportUrl(row.report_code, row.fight_id)
      if (href) map.set(key, href)
    }

    return map
  }, [scopedRosterRows])

  const scopedSummary = useMemo((): ScopedSummary | null => {
    if (scopedRosterRows.length === 0) return null

    const kills = scopedRosterRows.length
    const throughputs = scopedRosterRows.map(row => toFiniteNumber(row.throughput_per_second))
    const ranks = scopedRosterRows.map(row => toFiniteNumber(row.rank_percent))
    const avgThroughput = meanIgnoringNulls(throughputs)
    const bestThroughput = Math.max(0, ...throughputs.filter((v): v is number => v !== null))
    const avgRank = meanIgnoringNulls(ranks)
    const bestRank = Math.max(0, ...ranks.filter((v): v is number => v !== null))

    return {
      avgThroughput,
      bestThroughput,
      avgRank,
      bestRank,
      kills,
      firstSeen: [...scopedRosterRows].sort((a, b) => a.raid_night_date.localeCompare(b.raid_night_date))[0]?.raid_night_date,
      lastSeen: [...scopedRosterRows].sort((a, b) => b.raid_night_date.localeCompare(a.raid_night_date))[0]?.raid_night_date,
      playerClass: scopedRosterRows[0].player_class,
      role: scopedRosterRows[0].role,
      spec: scopedRosterRows[0].spec,
    }
  }, [scopedRosterRows])

  const scopedSurvivability = useMemo((): ScopedSurvivability | null => {
    if (deathEvents.data.length === 0) return null

    const killingBlowCounts = new Map<string, number>()
    for (const row of scopedDeathRows) {
      if (!hasRealText(row.killing_blow_name)) continue
      killingBlowCounts.set(row.killing_blow_name, (killingBlowCounts.get(row.killing_blow_name) ?? 0) + 1)
    }

    const killingBlows = [...killingBlowCounts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 3)
      .map(([bname, count]) => ({ name: bname, count }))

    const kills = scopedSummary?.kills ?? scopedRosterRows.length
    const totalDeaths = scopedDeathRows.length

    return {
      totalDeaths,
      deathsPerKill: totalDeaths / Math.max(kills, 1),
      killingBlows,
      isScoped: true,
    }
  }, [deathEvents.data.length, scopedDeathRows, scopedRosterRows.length, scopedSummary])

  const validRaidRows = useMemo(() =>
    raids.data.filter(r =>
      hasRealText(r.report_code) &&
      isIncludedZoneName(r.zone_name) &&
      hasRealText(r.raid_night_date) &&
      hasRealText(r.primary_difficulty)
    ),
    [raids.data]
  )

  const filteredSessions = useMemo(() => {
    let rows = validRaidRows
    if (selectedTier && selectedTier !== 'All') rows = rows.filter(r => r.zone_name === selectedTier)
    if (difficulty !== 'All') rows = rows.filter(r => r.primary_difficulty === difficulty)
    if (selectedBoss !== 'All') {
      const matchingReports = new Set(
        roster.data
          .filter(row => row.player_name === name)
          .filter(row => selectedTier === 'All' || !selectedTier || row.zone_name === selectedTier)
          .filter(row => difficulty === 'All' || row.difficulty_label === difficulty)
          .filter(row => row.boss_name === selectedBoss)
          .map(row => row.report_code)
      )
      rows = rows.filter(r => matchingReports.has(r.report_code))
    }
    return rows
  }, [validRaidRows, roster.data, name, selectedTier, difficulty, selectedBoss])

  const scopedAttendance = useMemo((): ScopedAttendance | null => {
    if (filteredSessions.length === 0) return null
    const presentReports = new Set(scopedRosterRows.map(r => r.report_code))
    const raidsPresent = presentReports.size
    const totalRaidsTracked = filteredSessions.length
    const attendanceRatePct = totalRaidsTracked > 0 ? (raidsPresent / totalRaidsTracked) * 100 : 0
    return { raidsPresent, totalRaidsTracked, attendanceRatePct }
  }, [filteredSessions, scopedRosterRows])

  const dataCoverage = useMemo(() => {
    if (!summary) return []
    const gaps: string[] = []
    if (!attRow) gaps.push('attendance')
    if (!survRow) gaps.push('survivability')
    if (dpsOverTime.length === 0) gaps.push('fight timeline')
    if (heatmapData.length === 0) gaps.push('boss breakdown')
    return gaps
  }, [summary, attRow, survRow, dpsOverTime.length, heatmapData.length])

  const playerMedia = useMemo(
    () => characterMedia.data.find(row => row.player_name.toLowerCase() === name.toLowerCase()) ?? null,
    [characterMedia.data, name]
  )

  const profileLinks = useMemo(
    () => externalCharacterLinks(name, playerMedia?.realm_slug, summary?.realm),
    [name, playerMedia?.realm_slug, summary?.realm]
  )

  const playerMplusSummary = useMemo(
    () => mplusSummary.data.find(row => row.player_name.toLowerCase() === name.toLowerCase()) ?? null,
    [mplusSummary.data, name]
  )

  const guildMplusRank = useMemo(() => {
    const scored = mplusSummary.data
      .filter(row => Number(row.score_all) > 0)
      .sort((a, b) => Number(b.score_all) - Number(a.score_all))
    const idx = scored.findIndex(row => row.player_name.toLowerCase() === name.toLowerCase())
    if (idx === -1 || !playerMplusSummary || !Number(playerMplusSummary.score_all)) return null
    return { rank: idx + 1, total: scored.length }
  }, [mplusSummary.data, name, playerMplusSummary])

  const playerMplusScoreHistory = useMemo(
    () => [...mplusScoreHistory.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => String(a.snapshot_at).localeCompare(String(b.snapshot_at))),
    [mplusScoreHistory.data, name]
  )

  const playerMplusRunHistory = useMemo(
    () => [...mplusRunHistory.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => String(a.completed_at).localeCompare(String(b.completed_at))),
    [mplusRunHistory.data, name]
  )

  const recentMplusRuns = useMemo(
    () => [...playerMplusRunHistory]
      .sort((a, b) => String(b.completed_at).localeCompare(String(a.completed_at)))
      .slice(0, 8),
    [playerMplusRunHistory]
  )

  const playerMplusDungeonBreakdown = useMemo(
    () => [...mplusDungeonBreakdown.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) =>
        (toFiniteNumber(b.best_score) ?? -1) - (toFiniteNumber(a.best_score) ?? -1) ||
        (toFiniteNumber(b.best_key_level) ?? -1) - (toFiniteNumber(a.best_key_level) ?? -1) ||
        a.dungeon.localeCompare(b.dungeon)
      ),
    [mplusDungeonBreakdown.data, name]
  )

  const bestMplusRunKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const row of playerMplusDungeonBreakdown) {
      if (row.best_run_url) keys.add(`url:${row.best_run_url}`)
      keys.add([
        row.dungeon,
        row.best_completed_at,
        toFiniteNumber(row.best_key_level) ?? 0,
      ].join('|').toLowerCase())
    }
    return keys
  }, [playerMplusDungeonBreakdown])

  const bestTimedDungeon = useMemo(
    () => [...playerMplusDungeonBreakdown]
      .filter(d => Number(d.highest_timed_level) > 0)
      .sort((a, b) => Number(b.highest_timed_level) - Number(a.highest_timed_level))[0] ?? null,
    [playerMplusDungeonBreakdown]
  )

  const mplusRunsThisYear = useMemo(() => {
    const year = new Date().getFullYear()
    return playerMplusRunHistory.filter(run => {
      const date = typeof run.completed_date === 'string' && run.completed_date
        ? run.completed_date
        : typeof run.completed_at === 'string'
          ? run.completed_at.slice(0, 10)
          : ''
      return date.startsWith(`${year}-`)
    }).length
  }, [playerMplusRunHistory])

  const hasMplusData = Boolean(
    playerMplusSummary ||
    playerMplusScoreHistory.length > 0 ||
    playerMplusRunHistory.length > 0 ||
    playerMplusDungeonBreakdown.length > 0
  )

  const playerEquipment = useMemo(() =>
    characterEquipment.data
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) =>
        (EQUIPMENT_SLOT_ORDER.indexOf(a.slot_type) === -1 ? 999 : EQUIPMENT_SLOT_ORDER.indexOf(a.slot_type)) -
        (EQUIPMENT_SLOT_ORDER.indexOf(b.slot_type) === -1 ? 999 : EQUIPMENT_SLOT_ORDER.indexOf(b.slot_type))
      ),
    [characterEquipment.data, name]
  )

  const equipmentBySlot = useMemo(() => {
    const map = new Map<string, typeof playerEquipment[0]>()
    playerEquipment.forEach(item => {
      if (item.slot_type) map.set(item.slot_type, item)
    })
    return map
  }, [playerEquipment])

  const playerRaidAchievements = useMemo(() =>
    [...raidAchievements.data]
      .filter(row => row.player_name.toLowerCase() === name.toLowerCase())
      .sort((a, b) => Number(b.completed_timestamp) - Number(a.completed_timestamp)),
    [raidAchievements.data, name]
  )

  const playerItemLevel = useMemo(() => {
    // Prefer Blizzard armoury equipment over raid-log avg_item_level: WCL playerDetails
    // reports the level worn during a kill, which can lag behind upgrades and undercount
    // legendaries. Armoury reflects current equipped state. Exclude cosmetic slots.
    const armouryLevels = playerEquipment
      .filter(item => {
        const slot = String(item.slot_type ?? '').toUpperCase()
        return slot !== 'SHIRT' && slot !== 'TABARD'
      })
      .map(item => toFiniteNumber(item.item_level))
      .filter((v): v is number => v !== null && v > 0)
    if (armouryLevels.length > 0) {
      return armouryLevels.reduce((s, v) => s + v, 0) / armouryLevels.length
    }

    const latestRosterRow = [...playerRosterRows]
      .filter(row => Number.isFinite(Number(row.avg_item_level)) && Number(row.avg_item_level) > 0)
      .sort((a, b) => String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? '')))[0]
    if (latestRosterRow) return Number(latestRosterRow.avg_item_level)

    const summaryLevel = Number(summary?.avg_item_level)
    return Number.isFinite(summaryLevel) && summaryLevel > 0 ? summaryLevel : null
  }, [playerEquipment, playerRosterRows, summary])

  const survivabilityKillingBlows = useMemo(() => {
    if (scopedSurvivability) return scopedSurvivability.killingBlows
    if (!survRow) return []
    const parsed = parseKillingBlowsJson(survRow.top_killing_blows_json)
    if (parsed.length > 0) return parsed.slice(0, 3)
    if (survRow.most_common_killing_blow) {
      return [{
        name: survRow.most_common_killing_blow,
        count: toFiniteNumber(survRow.most_common_killing_blow_count) ?? 0,
      }]
    }
    return []
  }, [scopedSurvivability, survRow])

  const displayedTotalDeaths = scopedSurvivability?.totalDeaths ?? survRow?.total_deaths ?? 0
  const displayedDeathsPerKill = scopedSurvivability?.deathsPerKill ?? survRow?.deaths_per_kill
  const hasSurvivabilityData = Boolean(scopedSurvivability || survRow)

  const currentTierProgress = useMemo(() => {
    if (!currentRaidTier) return []

    return COMPLETION_DIFFICULTIES.map(diff => {
      const catalogBossRows = encounterCatalog.data
        .filter(row => isIncludedZoneName(row.zone_name))
        .filter(row => row.zone_name === currentRaidTier)
        .filter(row => hasRealText(row.encounter_name))
        .filter(row => {
          const difficultyNames = parseDifficultyNames(row.difficulty_names)
          return difficultyNames.size === 0 || difficultyNames.has(diff)
        })

      const totalBosses = new Map<string, { firstSeen: string; lastSeen: string }>()
      if (catalogBossRows.length > 0) {
        for (const row of catalogBossRows) {
          totalBosses.set(row.encounter_name, { firstSeen: '', lastSeen: '' })
        }
      } else {
        const tierBossRows = bossProgression.data
          .filter(row => isIncludedZoneName(row.zone_name))
          .filter(row => row.zone_name === currentRaidTier)
          .filter(row => hasRealText(row.boss_name))

        for (const row of tierBossRows) {
          const existing = totalBosses.get(row.boss_name)
          const firstSeen = String(row.first_kill_date || row.last_attempt_date || '')
          const lastSeen = String(row.last_attempt_date || row.first_kill_date || '')
          if (!existing) {
            totalBosses.set(row.boss_name, { firstSeen, lastSeen })
            continue
          }
          if (firstSeen && (!existing.firstSeen || firstSeen < existing.firstSeen)) existing.firstSeen = firstSeen
          if (lastSeen && (!existing.lastSeen || lastSeen > existing.lastSeen)) existing.lastSeen = lastSeen
        }
      }

      const playerBosses = new Set(
        playerRosterRows
          .filter(row => row.zone_name === currentRaidTier)
          .filter(row => row.difficulty_label === diff)
          .map(row => row.boss_name)
          .filter(hasRealText)
      )

      const bosses = [...totalBosses.entries()]
        .sort((a, b) => {
          const aSort = a[1].firstSeen || a[1].lastSeen || a[0]
          const bSort = b[1].firstSeen || b[1].lastSeen || b[0]
          const byDate = String(aSort).localeCompare(String(bSort))
          return byDate === 0 ? a[0].localeCompare(b[0]) : byDate
        })
        .map(([bossName]) => ({ name: bossName, killed: playerBosses.has(bossName) }))

      return {
        difficulty: diff,
        completed: playerBosses.size,
        total: totalBosses.size,
        pct: totalBosses.size > 0 ? (playerBosses.size / totalBosses.size) * 100 : 0,
        bosses,
      }
    })
  }, [bossProgression.data, currentRaidTier, encounterCatalog.data, playerRosterRows])

  const loading = perf.loading || surv.loading || att.loading || raids.loading || roster.loading || bossPf.loading
  const error = perf.error || surv.error || raids.error
  const classColor = getClassColor(scopedSummary?.playerClass ?? summary?.player_class ?? 'Unknown')

  if (!loading && !summary) {
    return (
      <AppLayout title={name} subtitle="player not found" wide>
        <div className="py-16 text-center">
          <p className="text-ctp-overlay1 text-sm font-mono mb-4">Player "{name}" not found in data.</p>
          <button
            onClick={() => navigate('/players')}
            className="px-4 py-2 text-xs font-mono rounded-xl bg-ctp-surface0 border border-ctp-surface1 text-ctp-subtext1 hover:bg-ctp-surface1 transition-colors"
          >
            ← Back to Players
          </button>
        </div>
      </AppLayout>
    )
  }

  return (
    <AppLayout
      title={name}
      subtitle="player profile"
      actions={
        <button
          onClick={() => navigate('/players')}
          className="px-3 py-1.5 text-xs font-mono rounded-xl bg-ctp-surface0 border border-ctp-surface1 text-ctp-overlay1 hover:text-ctp-subtext1 hover:bg-ctp-surface1 transition-all"
        >
          ← All Players
        </button>
      }
      nav={<SectionNav sections={PD_SECTIONS} activeId={activeSectionId} />}
      wide
    >
      <ProfileSection
        classColor={classColor}
        loading={loading}
        summary={summary}
        playerMedia={playerMedia}
        profileLinks={profileLinks}
        scopedSummary={scopedSummary}
        displayedTotalDeaths={displayedTotalDeaths}
        displayedDeathsPerKill={displayedDeathsPerKill}
        scopedSurvivability={scopedSurvivability}
        scopedAttendance={scopedAttendance}
        difficulty={difficulty}
        setDifficulty={setDifficulty}
        tierOptions={tierOptions}
        selectedTier={selectedTier}
        setSelectedTier={setSelectedTier}
        bossOptions={bossOptions}
        selectedBoss={selectedBoss}
        setSelectedBoss={setSelectedBoss}
      />

      <RaidSection
        name={name}
        classColor={classColor}
        playerMedia={playerMedia}
        hasEquipmentOrAchievements={characterEquipment.data.length > 0 || raidAchievements.data.length > 0}
        playerItemLevel={playerItemLevel}
        equipmentLoading={characterEquipment.loading}
        playerEquipment={playerEquipment}
        equipmentBySlot={equipmentBySlot}
        achievementsLoading={raidAchievements.loading}
        playerRaidAchievements={playerRaidAchievements}
        rosterLoading={roster.loading}
        bossProgressionLoading={bossProgression.loading}
        encounterCatalogLoading={encounterCatalog.loading}
        currentTierProgress={currentTierProgress}
        currentRaidTier={currentRaidTier}
        dataCoverage={dataCoverage}
        dpsOverTime={dpsOverTime}
        scopedSummary={scopedSummary}
        heatmapData={heatmapData}
        survLoading={surv.loading}
        deathEventsLoading={deathEvents.loading}
        hasSurvivabilityData={hasSurvivabilityData}
        displayedTotalDeaths={displayedTotalDeaths}
        displayedDeathsPerKill={displayedDeathsPerKill}
        teamDeathRank={teamDeathRank}
        survivabilityKillingBlows={survivabilityKillingBlows}
        bossPfLoading={bossPf.loading}
        error={error}
        reportHrefByBossKey={reportHrefByBossKey}
        bossParseMode={bossParseMode}
        setBossParseMode={setBossParseMode}
        raidRosterLoading={roster.loading}
      />

      <MplusSection
        playerMplusSummary={playerMplusSummary}
        guildMplusRank={guildMplusRank}
        playerMplusScoreHistory={playerMplusScoreHistory}
        playerMplusRunHistory={playerMplusRunHistory}
        recentMplusRuns={recentMplusRuns}
        playerMplusDungeonBreakdown={playerMplusDungeonBreakdown}
        bestMplusRunKeys={bestMplusRunKeys}
        bestTimedDungeon={bestTimedDungeon}
        mplusRunsThisYear={mplusRunsThisYear}
        hasMplusData={hasMplusData}
        mplusHeatmapMode={mplusHeatmapMode}
        setMplusHeatmapMode={setMplusHeatmapMode}
        mplusSummaryLoading={mplusSummary.loading}
        mplusRunHistoryLoading={mplusRunHistory.loading}
        mplusScoreHistoryLoading={mplusScoreHistory.loading}
        mplusDungeonBreakdownLoading={mplusDungeonBreakdown.loading}
      />
    </AppLayout>
  )
}

export default PlayerDetail
