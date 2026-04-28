import { FilterTabs } from '../../../components/ui/FilterTabs'
import { FilterSelect } from '../../../components/ui/FilterSelect'
import { StatCard } from '../../../components/ui/StatCard'
import { SkeletonCard } from '../../../components/ui/LoadingState'
import { ClassLabel } from '../../../components/ui/ClassLabel'
import { RoleBadge } from '../../../components/ui/Badge'
import { useColourBlind } from '../../../context/ColourBlindContext'
import { formatDate, formatPct } from '../../../utils/format'
import { formatThroughput } from '../../../constants/wow'
import type { PlayerPerformanceSummary, PlayerCharacterMedia } from '../../../types'
import { DIFFICULTIES } from '../lib/constants'
import { formatRealmName } from '../lib/utils'
import type { DifficultyFilter, ScopedSummary, ScopedSurvivability, ScopedAttendance, ProfileLinks } from '../lib/types'

interface ProfileSectionProps {
  classColor: string
  loading: boolean
  summary: PlayerPerformanceSummary | undefined
  playerMedia: PlayerCharacterMedia | null
  profileLinks: ProfileLinks | null
  scopedSummary: ScopedSummary | null
  displayedTotalDeaths: number
  displayedDeathsPerKill: number | undefined
  scopedSurvivability: ScopedSurvivability | null
  scopedAttendance: ScopedAttendance | null
  difficulty: DifficultyFilter
  setDifficulty: (v: DifficultyFilter) => void
  tierOptions: string[]
  selectedTier: string
  setSelectedTier: (v: string) => void
  bossOptions: string[]
  selectedBoss: string
  setSelectedBoss: (v: string) => void
}

export function ProfileSection({
  classColor,
  loading,
  summary,
  playerMedia,
  profileLinks,
  scopedSummary,
  displayedTotalDeaths,
  displayedDeathsPerKill,
  scopedAttendance,
  difficulty,
  setDifficulty,
  tierOptions,
  selectedTier,
  setSelectedTier,
  bossOptions,
  selectedBoss,
  setSelectedBoss,
}: ProfileSectionProps) {
  const { getParseColor, getDeathRateColor, getAttendanceColor } = useColourBlind()

  return (
    <section id="profile" className="space-y-7 scroll-mt-20">
      <div
        className="rounded-2xl border p-6 shadow-card"
        style={{
          background: `linear-gradient(135deg, ${classColor}10 0%, #31324400 60%)`,
          borderColor: `${classColor}25`,
        }}
      >
        {loading ? (
          <div className="flex items-center gap-4">
            <div className="w-14 h-14 rounded-2xl bg-ctp-surface1 animate-pulse" />
            <div className="space-y-2">
              <div className="h-5 w-40 bg-ctp-surface1 rounded-lg animate-pulse" />
              <div className="h-3 w-24 bg-ctp-surface1/60 rounded-lg animate-pulse" />
            </div>
          </div>
        ) : summary ? (
          <div className="flex flex-wrap items-center gap-5">
            <div
              className="w-16 h-16 rounded-2xl flex items-center justify-center flex-shrink-0 text-xl font-bold border overflow-hidden"
              style={{ background: `${classColor}15`, borderColor: `${classColor}30`, color: classColor }}
            >
              {playerMedia?.inset_url || playerMedia?.avatar_url || playerMedia?.main_url ? (
                <img
                  src={playerMedia.inset_url || playerMedia.avatar_url || playerMedia.main_url}
                  alt={`${summary.player_name} character portrait`}
                  className="h-full w-full p-0.5 rounded-2xl object-cover"
                />
              ) : (
                summary.player_class?.[0] ?? '?'
              )}
            </div>
            <div className="flex-1 min-w-0">
              <h2 className="text-xl font-semibold" style={{ color: classColor }}>
                {summary.player_name}
              </h2>
              <div className="flex items-center gap-2 mt-1">
                <ClassLabel className={summary.player_class} spec={summary.primary_spec} size="sm" />
                <span className="text-ctp-overlay0 text-xs">·</span>
                <RoleBadge role={summary.role} />
                <span className="text-ctp-overlay0 text-xs">·</span>
                <span className="text-xs text-ctp-overlay1 font-mono">{formatRealmName(summary.realm)}</span>
              </div>
            </div>

            <div className="flex items-center gap-6 flex-shrink-0">
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best Parse</p>
                <p className="text-lg font-semibold" style={{ color: getParseColor(scopedSummary?.bestRank ?? summary.best_rank_percent) }}>
                  {(scopedSummary?.bestRank ?? summary.best_rank_percent) ? `${(scopedSummary?.bestRank ?? summary.best_rank_percent).toFixed(0)}%` : '—'}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best DPS/HPS</p>
                <p className="text-lg font-semibold text-ctp-text">
                  {formatThroughput(scopedSummary?.bestThroughput ?? summary.best_throughput_per_second)}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Last Seen</p>
                <p className="text-sm font-mono text-ctp-subtext1">{formatDate(scopedSummary?.lastSeen ?? summary.last_seen_date)}</p>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <FilterTabs options={DIFFICULTIES} value={difficulty} onChange={setDifficulty} />
          <FilterSelect value={selectedTier} onChange={setSelectedTier} options={tierOptions} className="min-w-48" />
          <FilterSelect value={selectedBoss} onChange={setSelectedBoss} options={bossOptions} className="min-w-52" />
          {profileLinks && (
            <div className="ml-auto flex items-center gap-2">
              <a
                href={profileLinks.raiderIo}
                target="_blank"
                rel="noopener noreferrer"
                title="view on raider.io - opens in a new tab"
                className="group inline-flex h-10 w-10 items-center justify-center rounded-lg transition-all hover:-translate-y-0.5 hover:drop-shadow-[0_0_12px_rgba(203,166,247,0.45)]"
              >
                <img
                  src="/rio-icon.png"
                  alt="Raider.IO"
                  className="h-8 w-8 object-contain opacity-80 transition-opacity group-hover:opacity-100"
                />
              </a>
              <a
                href={profileLinks.armory}
                target="_blank"
                rel="noopener noreferrer"
                title="view on world of warcraft - opens in a new tab"
                className="group inline-flex h-10 w-10 items-center justify-center rounded-lg transition-all hover:-translate-y-0.5 hover:drop-shadow-[0_0_12px_rgba(137,180,250,0.45)]"
              >
                <img
                  src="/wow-icon.png"
                  alt="World of Warcraft Armory"
                  className="h-8 w-8 object-contain opacity-80 transition-opacity group-hover:opacity-100"
                />
              </a>
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard
              label="Avg DPS / HPS"
              value={formatThroughput(scopedSummary?.avgThroughput ?? 0)}
              subValue={`Best: ${formatThroughput(scopedSummary?.bestThroughput ?? 0)}`}
              icon="◉"
              accent="blue"
            />
            <StatCard
              label="Avg Percentile"
              value={scopedSummary ? `${scopedSummary.avgRank?.toFixed(1) ?? '—'}%` : '—'}
              subValue={`Best: ${scopedSummary ? `${scopedSummary.bestRank.toFixed(0)}%` : '—'}`}
              icon="◈"
              valueColor={scopedSummary?.avgRank != null ? getParseColor(scopedSummary.avgRank) : undefined}
              accent="none"
            />
            <StatCard
              label="Deaths per Kill"
              value={displayedDeathsPerKill != null ? displayedDeathsPerKill.toFixed(1) : '—'}
              subValue={`${displayedTotalDeaths} total deaths`}
              icon="☠"
              valueColor={displayedDeathsPerKill != null ? getDeathRateColor(displayedDeathsPerKill) : undefined}
              accent="none"
            />
            <StatCard
              label="Attendance"
              value={scopedAttendance ? formatPct(scopedAttendance.attendanceRatePct) : '—'}
              subValue={scopedAttendance ? `${scopedAttendance.raidsPresent} / ${scopedAttendance.totalRaidsTracked} sessions` : 'No scoped sessions'}
              icon="◷"
              valueColor={scopedAttendance ? getAttendanceColor(scopedAttendance.attendanceRatePct) : undefined}
              accent="none"
            />
          </>
        )}
      </div>
    </section>
  )
}
