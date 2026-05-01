import { Card, CardHeader, CardTitle, CardBody } from '../../../components/ui/Card'
import { FilterTabs } from '../../../components/ui/FilterTabs'
import { StatCard } from '../../../components/ui/StatCard'
import { LoadingState } from '../../../components/ui/LoadingState'
import { useColourBlind } from '../../../context/ColourBlindContext'
import { toFiniteNumber } from '../../../utils/format'
import type { PlayerMplusSummary, PlayerMplusScoreHistory, PlayerMplusRunHistory, PlayerMplusDungeonBreakdown } from '../../../types'
import { MPLUS_HEATMAP_MODES, RAIDERIO_LINK_TITLE } from '../lib/constants'
import { formatKeyLevel } from '../lib/mplus'
import { formatNumber } from '../lib/utils'
import type { MplusHeatmapMode } from '../lib/types'
import { MplusScoreChart } from '../components/MplusScoreChart'
import { MplusActivityHeatmap } from '../components/MplusActivityHeatmap'
import { DungeonBreakdownCard, RecentDungeonRunCard } from '../components/DungeonCards'
import { SectionDivider } from '../components/SectionDivider'

interface MplusSectionProps {
  playerMplusSummary: PlayerMplusSummary | null
  guildMplusRank: { rank: number; total: number; percentile: number } | null
  playerMplusScoreHistory: PlayerMplusScoreHistory[]
  playerMplusRunHistory: PlayerMplusRunHistory[]
  recentMplusRuns: PlayerMplusRunHistory[]
  playerMplusDungeonBreakdown: PlayerMplusDungeonBreakdown[]
  bestMplusRunKeys: Set<string>
  bestTimedDungeon: PlayerMplusDungeonBreakdown | null
  mplusRunsThisYear: number
  hasMplusData: boolean
  mplusHeatmapMode: MplusHeatmapMode
  setMplusHeatmapMode: (v: MplusHeatmapMode) => void
  mplusSummaryLoading: boolean
  mplusRunHistoryLoading: boolean
  mplusScoreHistoryLoading: boolean
  mplusDungeonBreakdownLoading: boolean
}

export function MplusSection({
  playerMplusSummary,
  guildMplusRank,
  playerMplusScoreHistory,
  playerMplusRunHistory,
  recentMplusRuns,
  playerMplusDungeonBreakdown,
  bestMplusRunKeys,
  bestTimedDungeon,
  mplusRunsThisYear,
  hasMplusData,
  mplusHeatmapMode,
  setMplusHeatmapMode,
  mplusSummaryLoading,
  mplusRunHistoryLoading,
  mplusScoreHistoryLoading,
  mplusDungeonBreakdownLoading,
}: MplusSectionProps) {
  const { getParseColor, topTierColor, killColor, wipeColor, getRoleColor } = useColourBlind()

  return (
    <section id="mplus" className="space-y-7 scroll-mt-20">
      <SectionDivider
        label="Mythic+ Performance"
        subtitle="Raider.IO-backed dungeon score and key history"
      />

      {!hasMplusData && !mplusSummaryLoading && !mplusRunHistoryLoading ? (
        <Card>
          <CardHeader>
            <CardTitle>Mythic+ Progression</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              No Raider.IO Mythic+ data has been exported for this character yet.
            </p>
          </CardHeader>
        </Card>
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
            <StatCard
              label="Raider.IO Score"
              value={formatNumber(playerMplusSummary?.score_all, 1)}
              subValue={
                guildMplusRank
                  ? `Guild #${guildMplusRank.rank} of ${guildMplusRank.total}`
                  : 'Current season'
              }
              subValueColor={
                guildMplusRank
                  ? getParseColor(guildMplusRank.percentile)
                  : undefined
              }
              icon="◆"
              valueColor={topTierColor}
              accent="none"
            />
            <StatCard
              label="Best Timed Key"
              value={formatKeyLevel(playerMplusSummary?.highest_timed_level)}
              subValue={bestTimedDungeon?.dungeon || (playerMplusSummary ? 'No timed keys' : '—')}
              icon="⏱"
              valueColor={getRoleColor('tank')}
              accent="none"
            />
            <StatCard
              label="Timed / Untimed"
              value={(
                <>
                  <span style={{ color: killColor }}>{toFiniteNumber(playerMplusSummary?.timed_runs) ?? 0}</span>
                  <span className="mx-1 text-ctp-overlay0">/</span>
                  <span style={{ color: wipeColor }}>{toFiniteNumber(playerMplusSummary?.untimed_runs) ?? 0}</span>
                </>
              )}
              subValue={`${toFiniteNumber(playerMplusSummary?.total_runs) ?? 0} exported runs`}
              icon="◒"
              accent="blue"
            />
            <StatCard
              label="Common Key Level"
              value={formatKeyLevel(playerMplusSummary?.most_common_key_level)}
              subValue={`${toFiniteNumber(playerMplusSummary?.most_common_key_count) ?? 0} runs at that level`}
              icon="◇"
              valueColor={topTierColor}
              accent="none"
            />
          </div>

          <Card>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle>Key Activity</CardTitle>
                  <p className="text-xs text-ctp-overlay1 mt-0.5">
                    <span className="font-semibold text-ctp-mauve">{mplusRunsThisYear}</span> keys completed this year
                  </p>
                </div>
                <FilterTabs
                  options={MPLUS_HEATMAP_MODES}
                  value={mplusHeatmapMode}
                  onChange={setMplusHeatmapMode}
                  className="flex-shrink-0"
                  buttonClassName="px-3 py-1.5"
                />
              </div>
            </CardHeader>
            <CardBody>
              {mplusRunHistoryLoading ? (
                <LoadingState rows={5} />
              ) : (
                <MplusActivityHeatmap data={playerMplusRunHistory} mode={mplusHeatmapMode} />
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Score Over Time</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">
                Nightly Raider.IO score snapshots from Databricks ingestion
              </p>
            </CardHeader>
            <CardBody>
              {mplusScoreHistoryLoading ? (
                <LoadingState rows={5} />
              ) : (
                <MplusScoreChart data={playerMplusScoreHistory} lineColor={topTierColor} />
              )}
            </CardBody>
          </Card>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Recent Dungeon Runs</CardTitle>
              </CardHeader>
              <CardBody>
                {mplusRunHistoryLoading ? (
                  <LoadingState rows={6} />
                ) : recentMplusRuns.length === 0 ? (
                  <p className="py-8 text-center text-xs font-mono text-ctp-overlay0">
                    No recent dungeon runs exported for this character yet.
                  </p>
                ) : (
                  <div className="max-h-[36rem] space-y-3 overflow-y-auto pr-2">
                    {recentMplusRuns.map(row => {
                      const isNewBest = Boolean(row.url && bestMplusRunKeys.has(`url:${row.url}`)) ||
                        bestMplusRunKeys.has([
                          row.dungeon,
                          row.completed_at,
                          toFiniteNumber(row.mythic_level) ?? 0,
                        ].join('|').toLowerCase())

                      return (
                        <RecentDungeonRunCard
                          key={`${row.dungeon}-${row.completed_at}-${row.mythic_level}`}
                          row={row}
                          isNewBest={isNewBest}
                        />
                      )
                    })}
                  </div>
                )}
              </CardBody>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex items-start justify-between gap-4">
                  <CardTitle>Best by Dungeon</CardTitle>
                  {playerMplusSummary?.profile_url && (
                    <a
                      href={playerMplusSummary.profile_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      title={RAIDERIO_LINK_TITLE}
                      className="text-xs font-mono text-ctp-mauve transition-colors hover:text-ctp-pink"
                    >
                      Raider.IO ↗
                    </a>
                  )}
                </div>
              </CardHeader>
              <CardBody>
                {mplusDungeonBreakdownLoading ? (
                  <LoadingState rows={6} />
                ) : playerMplusDungeonBreakdown.length === 0 ? (
                  <p className="py-8 text-center text-xs font-mono text-ctp-overlay0">
                    No dungeon breakdown rows exported for this character yet.
                  </p>
                ) : (
                  <div className="max-h-[36rem] space-y-3 overflow-y-auto pr-2">
                    {playerMplusDungeonBreakdown.map(row => (
                      <DungeonBreakdownCard key={`${row.dungeon}-${row.best_completed_at}`} row={row} />
                    ))}
                  </div>
                )}
              </CardBody>
            </Card>
          </div>
        </div>
      )}
    </section>
  )
}
