import { Card, CardHeader, CardTitle, CardBody } from '../../../components/ui/Card'
import { FilterTabs } from '../../../components/ui/FilterTabs'
import { LoadingState } from '../../../components/ui/LoadingState'
import { ErrorState } from '../../../components/ui/ErrorState'
import { ProgressBar } from '../../../components/ui/ProgressBar'
import { DpsOverTimeChart } from '../../../components/charts/DpsOverTimeChart'
import { PerformanceHeatmap } from '../../../components/charts/PerformanceHeatmap'
import { useColourBlind } from '../../../context/ColourBlindContext'
import { formatDate } from '../../../utils/format'
import { formatThroughput } from '../../../constants/wow'
import type { PlayerCharacterMedia, PlayerCharacterEquipment, PlayerRaidAchievement, PlayerBossPerformance } from '../../../types'
import { EQUIPMENT_SLOTS, BOSS_PARSE_MODES, WARCRAFTLOGS_LINK_TITLE } from '../lib/constants'
import { formatBlizzardTimestamp, getSurvivabilityRankColor, warcraftLogsReportUrl } from '../lib/utils'
import type { TierCompletionRow, BossParseMode, KillingBlowSummary, TeamDeathRank, ScopedSummary, DpsDataPoint } from '../lib/types'
import { GearSlot } from '../components/GearSlot'
import { CompletionRow } from '../components/CompletionRow'
import { SectionDivider } from '../components/SectionDivider'

interface RaidSectionProps {
  name: string
  classColor: string
  playerMedia: PlayerCharacterMedia | null
  // Equipment
  hasEquipmentOrAchievements: boolean
  playerItemLevel: number | null
  equipmentLoading: boolean
  playerEquipment: PlayerCharacterEquipment[]
  equipmentBySlot: Map<string, PlayerCharacterEquipment>
  // Achievements
  achievementsLoading: boolean
  playerRaidAchievements: PlayerRaidAchievement[]
  // Completion
  rosterLoading: boolean
  bossProgressionLoading: boolean
  encounterCatalogLoading: boolean
  currentTierProgress: TierCompletionRow[]
  currentRaidTier: string
  // Data coverage notice
  dataCoverage: string[]
  // DPS timeline
  dpsOverTime: DpsDataPoint[]
  scopedSummary: ScopedSummary | null
  heatmapData: PlayerBossPerformance[]
  // Survivability
  survLoading: boolean
  deathEventsLoading: boolean
  hasSurvivabilityData: boolean
  displayedTotalDeaths: number
  displayedDeathsPerKill: number | undefined
  teamDeathRank: TeamDeathRank | null
  survivabilityKillingBlows: KillingBlowSummary[]
  // Boss heatmap
  bossPfLoading: boolean
  error: string | null
  reportHrefByBossKey: Map<string, string>
  bossParseMode: BossParseMode
  setBossParseMode: (v: BossParseMode) => void
  // Raid loading (for DPS chart)
  raidRosterLoading: boolean
}

export function RaidSection({
  name,
  classColor,
  playerMedia,
  hasEquipmentOrAchievements,
  playerItemLevel,
  equipmentLoading,
  playerEquipment,
  equipmentBySlot,
  achievementsLoading,
  playerRaidAchievements,
  rosterLoading,
  bossProgressionLoading,
  encounterCatalogLoading,
  currentTierProgress,
  currentRaidTier,
  dataCoverage,
  dpsOverTime,
  scopedSummary,
  heatmapData,
  survLoading,
  deathEventsLoading,
  hasSurvivabilityData,
  displayedTotalDeaths,
  displayedDeathsPerKill,
  teamDeathRank,
  survivabilityKillingBlows,
  bossPfLoading,
  error,
  reportHrefByBossKey,
  bossParseMode,
  setBossParseMode,
  raidRosterLoading,
}: RaidSectionProps) {
  const { getParseColor, wipeColor, getDeathRateColor, getDifficultyColor } = useColourBlind()

  return (
    <section id="raid" className="space-y-7 scroll-mt-20">
      {hasEquipmentOrAchievements && (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <Card className="xl:col-span-2">
            <CardHeader>
              <div className="flex items-center justify-between gap-4">
                <div>
                  <CardTitle>Equipped Gear</CardTitle>
                  <p className="text-xs text-ctp-overlay1 mt-0.5">
                    Blizzard profile snapshot
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">Item Level</p>
                  <p className="mt-0.5 text-lg font-semibold text-ctp-text">
                    {playerItemLevel ? playerItemLevel.toFixed(1) : '—'}
                  </p>
                </div>
              </div>
            </CardHeader>
            <CardBody>
              {equipmentLoading ? (
                <LoadingState rows={6} />
              ) : playerEquipment.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No equipment found for this character.
                </p>
              ) : (
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(300px,380px)_minmax(0,1fr)]">
                  <div className="space-y-2">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'left').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>

                  <div
                    className="relative order-first flex min-h-[520px] items-end justify-center overflow-hidden rounded-2xl border border-ctp-surface1 bg-ctp-crust/70 lg:order-none"
                    style={{
                      background: `radial-gradient(circle at 50% 22%, ${classColor}24 0%, rgba(24, 24, 37, 0.58) 38%, rgba(17, 17, 27, 0.92) 100%)`,
                    }}
                  >
                    {playerMedia?.main_raw_url || playerMedia?.main_url ? (
                      <img
                        src={playerMedia.main_raw_url || playerMedia.main_url}
                        alt={`${name} standing character render`}
                        className="absolute inset-x-[-32%] -bottom-9 z-10 h-[132%] w-[164%] max-w-none object-cover object-bottom drop-shadow-2xl"
                      />
                    ) : (
                      <div className="relative z-10 flex h-full w-full items-center justify-center text-5xl font-bold" style={{ color: classColor }}>
                        {name[0] ?? '?'}
                      </div>
                    )}
                    <div className="pointer-events-none absolute inset-0 z-20 bg-gradient-to-b from-ctp-crust/10 via-transparent to-ctp-crust/40" />
                  </div>

                  <div className="space-y-2">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'right').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>

                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:col-span-3">
                    {EQUIPMENT_SLOTS.filter(slot => slot.side === 'bottom').map(slot => (
                      <GearSlot
                        key={slot.type}
                        label={slot.label}
                        item={equipmentBySlot.get(slot.type)}
                        classColor={classColor}
                      />
                    ))}
                  </div>
                </div>
              )}
            </CardBody>
          </Card>

          <div className="xl:col-span-2">
            <SectionDivider
              label="Raid Performance"
              subtitle="WarcraftLogs-derived raid progress, parses, deaths, and boss history"
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Current Tier Completion</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                {currentRaidTier || 'No tier'}
              </p>
            </CardHeader>
            <CardBody>
              {rosterLoading || bossProgressionLoading || encounterCatalogLoading ? (
                <LoadingState rows={3} />
              ) : currentTierProgress.every(row => row.total === 0) ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No current-tier boss completion data found for this character.
                </p>
              ) : (
                <div className="space-y-4">
                  {currentTierProgress.map(row => (
                    <CompletionRow key={row.difficulty} row={row} color={getDifficultyColor(row.difficulty)} />
                  ))}
                </div>
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Raid Feats</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                Cutting Edge + Famed Slayer
              </p>
            </CardHeader>
            <CardBody>
              {achievementsLoading ? (
                <LoadingState rows={4} />
              ) : playerRaidAchievements.length === 0 ? (
                <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">
                  No raid feats found for this character.
                </p>
              ) : (
                <div className="space-y-2.5">
                  {playerRaidAchievements.slice(0, 8).map(achievement => (
                    <div
                      key={`${achievement.achievement_id}-${achievement.completed_timestamp}`}
                      className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/60 px-3 py-2"
                    >
                      <p className="text-xs font-medium text-ctp-text">
                        {achievement.achievement_name}
                      </p>
                      <p className="mt-0.5 text-[10px] font-mono text-ctp-overlay0">
                        {formatBlizzardTimestamp(achievement.completed_timestamp)}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </CardBody>
          </Card>
        </div>
      )}

      {dataCoverage.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Profile Data Coverage</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Some profile modules are missing backing rows in the current static export: {dataCoverage.join(', ')}.
            </p>
          </CardHeader>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>DPS / HPS Over Time</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            {dpsOverTime.length} kill performances in scope
            {scopedSummary ? ` · avg ${formatThroughput(scopedSummary.avgThroughput ?? 0)}` : ''}
          </p>
        </CardHeader>
        <CardBody>
          {raidRosterLoading ? (
            <LoadingState rows={5} />
          ) : dpsOverTime.length === 0 && heatmapData.length > 0 ? (
            <div className="h-48 flex items-center justify-center text-center text-ctp-overlay0 text-sm font-mono px-6">
              Fight-by-fight timeline data is unavailable for this filtered scope. Boss-level aggregates are shown below instead.
            </div>
          ) : (
            <DpsOverTimeChart
              data={dpsOverTime}
              playerClass={scopedSummary?.playerClass ?? 'Unknown'}
              avgThroughput={scopedSummary?.avgThroughput ?? undefined}
            />
          )}
        </CardBody>
      </Card>

      <div className="grid grid-cols-1 items-stretch gap-6 lg:grid-cols-2">
        <Card className="h-full">
          <CardHeader>
            <CardTitle>Parse Breakdown by Fight</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Individual WCL rank % per recorded fight in scope</p>
          </CardHeader>
          <CardBody className="max-h-[318px] space-y-2.5 overflow-y-auto pr-3">
            {raidRosterLoading ? (
              <LoadingState rows={6} />
            ) : dpsOverTime.length === 0 ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No fight data available in the current scope</p>
            ) : (
              [...dpsOverTime]
                .filter((d): d is typeof d & { parse: number } => d.parse !== null && d.parse > 0)
                .sort((a, b) => String(b.date).localeCompare(String(a.date)))
                .map((d, i) => (
                  <a
                    key={`${d.reportCode}-${d.fightId}-${i}`}
                    href={warcraftLogsReportUrl(d.reportCode, d.fightId) ?? undefined}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={WARCRAFTLOGS_LINK_TITLE}
                    className="group flex items-center gap-3 rounded-lg px-1.5 py-1 transition-colors hover:bg-ctp-surface0/70"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-ctp-subtext1 truncate transition-colors group-hover:text-ctp-mauve">{String(d.boss)}</span>
                        <span className="text-xs font-mono font-semibold flex-shrink-0 ml-2" style={{ color: getParseColor(d.parse) }}>
                          {d.parse.toFixed(0)}%
                        </span>
                      </div>
                      <ProgressBar value={d.parse} color={getParseColor(d.parse)} height="xs" />
                    </div>
                    <div className="flex-shrink-0 text-right">
                      <span className="block text-[10px] font-mono text-ctp-overlay0 transition-colors group-hover:text-ctp-overlay1">
                        {formatDate(d.date)}
                      </span>
                      <span className="block text-[9px] font-mono text-ctp-overlay0 opacity-0 transition-opacity group-hover:opacity-100">
                        WCL ↗
                      </span>
                    </div>
                  </a>
                ))
            )}
          </CardBody>
        </Card>

        <Card className="h-full">
          <CardHeader>
            <CardTitle>Survivability</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              {hasSurvivabilityData ? 'Death analysis for the current page filters' : 'Aggregate death analysis for this player'}
            </p>
          </CardHeader>
          <CardBody>
            {survLoading || deathEventsLoading ? (
              <LoadingState rows={4} />
            ) : !hasSurvivabilityData ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No death data recorded</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Total Deaths</p>
                    <p className="text-xl font-semibold" style={{ color: wipeColor }}>{displayedTotalDeaths}</p>
                  </div>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Deaths / Kill</p>
                    <p className="text-xl font-semibold" style={{ color: getDeathRateColor(displayedDeathsPerKill ?? 0) }}>
                      {displayedDeathsPerKill != null ? displayedDeathsPerKill.toFixed(1) : '—'}
                    </p>
                    <p className="mt-0.5 text-[10px] font-mono text-ctp-overlay0">
                      {displayedDeathsPerKill != null ? 'lower = better' : ''}
                    </p>
                  </div>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Team Rank</p>
                    <p
                      className="text-xl font-semibold"
                      style={{
                        color: teamDeathRank
                          ? getSurvivabilityRankColor(teamDeathRank.rank, teamDeathRank.total, getParseColor, wipeColor)
                          : undefined,
                      }}
                    >
                      {teamDeathRank ? `#${teamDeathRank.rank}` : '—'}
                    </p>
                    <p className="mt-0.5 text-[10px] font-mono text-ctp-overlay0">
                      {teamDeathRank
                        ? `out of ${teamDeathRank.total} raiders`
                        : 'No scoped team data'}
                    </p>
                  </div>
                </div>

                <div>
                  <p className="section-label mb-2">Most Common Killing Blows</p>
                  <div className="space-y-2">
                    {survivabilityKillingBlows.length > 0 ? (
                      survivabilityKillingBlows.map((blow, index) => (
                        <div key={`${blow.name}-${index}`} className="flex items-center justify-between gap-3 rounded-xl bg-ctp-surface1/40 p-3">
                          <div className="min-w-0">
                            <p className="text-sm font-medium text-ctp-text truncate">
                              {blow.name}
                            </p>
                          </div>
                          <p className="flex-shrink-0 text-xs font-mono text-ctp-subtext1">
                            {blow.count} hit{blow.count !== 1 ? 's' : ''}
                          </p>
                        </div>
                      ))
                    ) : (
                      <div className="bg-ctp-surface1/40 rounded-xl p-3">
                        <p className="text-sm font-medium text-ctp-text">—</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>Performance by Boss</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                {bossParseMode === 'best' ? 'Best' : 'Average'} WCL parse % per boss encounter in scope · colour = parse tier
              </p>
            </div>
            <FilterTabs
              options={BOSS_PARSE_MODES}
              value={bossParseMode}
              onChange={setBossParseMode}
              className="flex-shrink-0"
              buttonClassName="px-3 py-1.5"
            />
          </div>
        </CardHeader>
        <CardBody>
          {bossPfLoading ? (
            <LoadingState rows={4} />
          ) : error ? (
            <ErrorState message={error} />
          ) : (
            <PerformanceHeatmap
              data={heatmapData}
              getHref={(row) => reportHrefByBossKey.get(`${row.encounter_id}-${row.difficulty}`) ?? null}
              externalLinkTitle={WARCRAFTLOGS_LINK_TITLE}
              parseMode={bossParseMode}
            />
          )}
        </CardBody>
      </Card>
    </section>
  )
}
