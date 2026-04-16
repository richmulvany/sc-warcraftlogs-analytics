import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ChevronLeft, ChevronRight, Cross, Shield, Swords } from 'lucide-react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { FilterTabs } from '../components/ui/FilterTabs'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { WeeklyActivityChart } from '../components/charts/WeeklyActivityChart'
import { ParseHistogramChart, getParseDistributionSummary } from '../components/charts/ParseHistogramChart'
import {
  useRaidSummary,
  useBossProgression,
  useBossKillRoster,
  useBossWipeAnalysis,
} from '../hooks/useGoldData'
import { formatNumber, formatDateShort } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import { formatDuration, formatThroughput, getClassColor, getThroughputColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

type DifficultyFilter = 'All' | 'Mythic' | 'Heroic' | 'Normal'

const DIFFICULTY_FILTERS: DifficultyFilter[] = ['All', 'Mythic', 'Heroic', 'Normal']

interface LeaderboardRow {
  player_name: string
  player_class: string
  primary_spec: string
  role: string
  avg_rank_percent: number
  avg_throughput_per_second: number
  kills_tracked: number
}

export function Overview() {
  const { getParseColor, killColor, wipeColor, chartColors, topTierColor } = useColourBlind()
  const raids = useRaidSummary()
  const bosses = useBossProgression()
  const bossWipes = useBossWipeAnalysis()
  const killRoster = useBossKillRoster()
  const navigate = useNavigate()
  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')
  const [activityMode, setActivityMode] = useState<'tiers' | 'compare'>('tiers')
  const [parseView, setParseView] = useState<'No Curve' | 'Curve'>('No Curve')

  function num(value: unknown): number {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : 0
  }

  function hasRealText(value: unknown): value is string {
    return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
  }

  function normaliseDifficulty(value: unknown): string {
    return String(value ?? '').trim().toLowerCase()
  }

  function matchesDifficulty(value: unknown): boolean {
    return difficulty === 'All' || normaliseDifficulty(value) === difficulty.toLowerCase()
  }

  const validRaidRows = useMemo(() =>
    raids.data.filter(r =>
      hasRealText(r.report_code) &&
      isIncludedZoneName(r.zone_name) &&
      hasRealText(r.raid_night_date) &&
      hasRealText(r.primary_difficulty)
    ),
    [raids.data]
  )

  const currentTier = useMemo(() =>
    [...validRaidRows]
      .sort((a, b) => String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? '')))[0]?.zone_name ?? null,
    [validRaidRows]
  )

  const scopedRaids = useMemo(() =>
    validRaidRows.filter(r => r.zone_name === currentTier && matchesDifficulty(r.primary_difficulty)),
    [validRaidRows, currentTier, difficulty]
  )

  const scopedBossRows = useMemo(() =>
    killRoster.data.filter(row =>
      row.zone_name === currentTier &&
      isIncludedZoneName(row.zone_name) &&
      matchesDifficulty(row.difficulty_label || row.difficulty)
    ),
    [killRoster.data, currentTier, difficulty]
  )

  const stats = useMemo(() => {
    if (!scopedRaids.length) return null
    const totalKills = scopedRaids.reduce((s, r) => s + num(r.boss_kills), 0)
    const totalRaids = scopedRaids.length
    return { totalKills, totalRaids }
  }, [scopedRaids])

  const activePlayers = useMemo(() =>
    new Set(scopedBossRows.map(row => row.player_name).filter(hasRealText)).size,
    [scopedBossRows]
  )

  const killedBosses = useMemo(() =>
    bosses.data.filter(b =>
      b.zone_name === currentTier &&
      isIncludedZoneName(b.zone_name) &&
      matchesDifficulty(b.difficulty_label || b.difficulty) &&
      (b.is_killed === 'True' || b.is_killed === (true as unknown as string))
    ),
    [bosses.data, currentTier, difficulty]
  )

  const currentTierBossRows = useMemo(() =>
    bosses.data.filter(b =>
      b.zone_name === currentTier &&
      isIncludedZoneName(b.zone_name)
    ),
    [bosses.data, currentTier]
  )

  const highestAvailableDifficulty = useMemo(() => {
    for (const option of ['Mythic', 'Heroic', 'Normal'] as const) {
      if (currentTierBossRows.some(row => row.difficulty_label === option)) return option
    }
    return null
  }, [currentTierBossRows])

  const highestDifficultyBossRows = useMemo(() =>
    highestAvailableDifficulty
      ? currentTierBossRows.filter(row => row.difficulty_label === highestAvailableDifficulty)
      : [],
    [currentTierBossRows, highestAvailableDifficulty]
  )

  const bestHpByBoss = useMemo(() => {
    const map = new Map<string, number>()
    bossWipes.data.forEach(row => {
      if (!isIncludedZoneName(row.zone_name)) return
      map.set(`${row.encounter_id}-${row.difficulty}`, Number(row.best_wipe_pct) || 100)
    })
    return map
  }, [bossWipes.data])

  const currentProgBoss = useMemo(() =>
    [...highestDifficultyBossRows]
      .filter(b => !(b.is_killed === 'True' || b.is_killed === (true as unknown as string)))
      .sort((a, b) => String(b.last_attempt_date ?? '').localeCompare(String(a.last_attempt_date ?? '')))[0] ?? null,
    [highestDifficultyBossRows]
  )

  const lastDownedBoss = useMemo(() =>
    [...highestDifficultyBossRows]
      .filter(b => b.is_killed === 'True' || b.is_killed === (true as unknown as string))
      .sort((a, b) => String(b.first_kill_date ?? '').localeCompare(String(a.first_kill_date ?? '')))[0] ?? null,
    [highestDifficultyBossRows]
  )

  const topPerformers = useMemo(() => {
    const grouped = new Map<string, LeaderboardRow>()

    for (const row of scopedBossRows) {
      const playerName = row.player_name
      const rankPercent = num(row.rank_percent)
      const throughput = num(row.throughput_per_second)
      const existing = grouped.get(playerName)

      if (!existing) {
        grouped.set(playerName, {
          player_name: playerName,
          player_class: row.player_class,
          primary_spec: row.spec,
          role: row.role,
          avg_rank_percent: rankPercent,
          avg_throughput_per_second: throughput,
          kills_tracked: 1,
        })
        continue
      }

      const nextKills = existing.kills_tracked + 1
      existing.avg_rank_percent = ((existing.avg_rank_percent * existing.kills_tracked) + rankPercent) / nextKills
      existing.avg_throughput_per_second = ((existing.avg_throughput_per_second * existing.kills_tracked) + throughput) / nextKills
      existing.kills_tracked = nextKills
    }

    return [...grouped.values()]
      .filter(player => player.kills_tracked >= 2)
      .sort((a, b) => b.avg_rank_percent - a.avg_rank_percent)
      .slice(0, 9)
  }, [scopedBossRows])

  const recentRaids = useMemo(() =>
    [...scopedRaids]
      .sort((a, b) => String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? '')))
      .slice(0, 5),
    [scopedRaids]
  )

  const parseDistributionSummary = useMemo(() =>
    getParseDistributionSummary(scopedBossRows),
    [scopedBossRows]
  )

  function bossHref(encounterId: string, difficultyValue: string) {
    return `/bosses/${encounterId}/${difficultyValue}`
  }

  function roleKind(role: unknown): 'healer' | 'tank' | 'dps' {
    const value = String(role ?? '').trim().toLowerCase()
    if (value.includes('heal')) return 'healer'
    if (value.includes('tank')) return 'tank'
    return 'dps'
  }

  function RoleIcon({ role, playerClass }: { role: string; playerClass: string }) {
    const kind = roleKind(role)
    const Icon = kind === 'healer' ? Cross : kind === 'tank' ? Shield : Swords
    return (
      <Icon
        className="h-3.5 w-3.5 flex-shrink-0"
        style={{ color: getClassColor(playerClass), opacity: 0.9 }}
        aria-hidden="true"
      />
    )
  }

  const loading = raids.loading || bosses.loading || killRoster.loading
  const error = raids.error || bosses.error || killRoster.error

  return (
    <AppLayout title="Dashboard" subtitle="Student Council · Twisting Nether EU" hideHeader>
      <div className="space-y-3">
        <div
          className="relative overflow-hidden rounded-2xl border border-ctp-surface1/80 bg-ctp-surface0"
          style={{
            backgroundImage: "linear-gradient(90deg, rgba(24, 24, 37, 0.82) 0%, rgba(24, 24, 37, 0.5) 48%, rgba(17, 17, 27, 0.78) 100%), url('/raid-banner.png')",
            backgroundSize: 'cover',
            backgroundPosition: 'center',
          }}
        >
          <div className="absolute right-3 top-1/2 z-10 hidden -translate-y-1/2 sm:block">
            <FilterTabs options={DIFFICULTY_FILTERS} value={difficulty} onChange={setDifficulty} />
          </div>
          <div className="px-5 py-4 sm:pr-[17rem]">
            <p className="text-[10px] font-mono uppercase tracking-[0.18em] text-ctp-subtext0/90">
              Current Tier
            </p>
            <p className="text-xl font-semibold text-ctp-text mt-1">
              {currentTier ?? 'No current tier data'}
            </p>
            <p className="text-[11px] font-mono text-ctp-overlay0 mt-1">
              {difficulty === 'All' ? 'all difficulties' : difficulty}
            </p>
          </div>
        </div>
        <div className="flex justify-center sm:hidden">
          <FilterTabs options={DIFFICULTY_FILTERS} value={difficulty} onChange={setDifficulty} />
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4"><ErrorState message={error} /></div>
        ) : (
          <>
            <StatCard
              label="Total Boss Kills"
              value={formatNumber(stats?.totalKills ?? 0)}
              subValue={difficulty === 'All' ? 'current tier · all difficulties' : `${difficulty} · current tier`}
              icon="⚔"
              accent="mauve"
            />
            <StatCard
              label="Raid Nights"
              value={formatNumber(stats?.totalRaids ?? 0)}
              subValue="current tier sessions"
              icon="◷"
              accent="blue"
            />
            <StatCard
              label="Active Players"
              value={formatNumber(activePlayers)}
              subValue="seen in scoped kills"
              icon="◉"
              accent="teal"
            />
            <StatCard
              label="Unique Bosses Killed"
              value={killedBosses.length}
              subValue={currentTier ?? 'current tier'}
              icon="◈"
              accent="peach"
            />
          </>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-stretch">
        <Card className="xl:col-span-2">
          <CardHeader>
            <div className="flex items-start justify-between gap-3">
              <div>
                <CardTitle>Weekly Activity</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">
                  {activityMode === 'tiers'
                    ? 'Current raid tier vs previous raid tier by raid weeks split by tier'
                    : 'Current tier vs previous tier overlaid and split by kill/wipe outcome'}
                </p>
              </div>
              <div className="flex items-center gap-1 rounded-xl border border-ctp-surface1 bg-ctp-surface0/70 p-1">
                <button
                  onClick={() => setActivityMode(mode => mode === 'tiers' ? 'compare' : 'tiers')}
                  className="rounded-lg p-1.5 text-ctp-overlay1 hover:bg-ctp-surface1/60 hover:text-ctp-text transition-colors"
                  aria-label="Previous weekly activity view"
                >
                  <ChevronLeft className="w-3.5 h-3.5" />
                </button>
                <button
                  onClick={() => setActivityMode(mode => mode === 'tiers' ? 'compare' : 'tiers')}
                  className="rounded-lg p-1.5 text-ctp-overlay1 hover:bg-ctp-surface1/60 hover:text-ctp-text transition-colors"
                  aria-label="Next weekly activity view"
                >
                  <ChevronRight className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>
          </CardHeader>
          <CardBody>
            {raids.loading ? <LoadingState rows={4} /> :
             raids.error ? <ErrorState message={raids.error} /> :
             <WeeklyActivityChart raids={validRaidRows} mode={activityMode} difficulty={difficulty} />}
            <div className="flex items-center gap-5 mt-3">
              {activityMode === 'compare' ? (
                <>
                  <div className="flex items-center gap-1.5">
                    <div className="w-4 h-0.5 rounded" style={{ backgroundColor: '#cba6f7' }} />
                    <span className="text-[11px] font-mono text-ctp-overlay0">Current Tier</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <div className="w-4 h-0.5 rounded" style={{ backgroundColor: '#89b4fa' }} />
                    <span className="text-[11px] font-mono text-ctp-overlay0">Previous Tier</span>
                  </div>
                </>
              ) : (
                <>
                  <div className="flex items-center gap-1.5">
                    <div className="w-4 h-0.5 rounded" style={{ backgroundColor: chartColors.primary }} />
                    <span className="text-[11px] font-mono text-ctp-overlay0">Boss Kills</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <div className="w-4 h-0.5 rounded" style={{ backgroundColor: chartColors.secondary }} />
                    <span className="text-[11px] font-mono text-ctp-overlay0">Wipes</span>
                  </div>
                </>
              )}
            </div>
          </CardBody>
        </Card>

        <div className="flex flex-col gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Recent Raids</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">{currentTier ?? 'No tier'} · {difficulty}</p>
            </CardHeader>
            <div className="divide-y divide-ctp-surface1">
              {raids.loading ? (
                <div className="p-5"><LoadingState rows={5} /></div>
              ) : recentRaids.length === 0 ? (
                <div className="p-5 text-xs font-mono text-ctp-overlay0">No raids match this difficulty in the current tier.</div>
              ) : recentRaids.map(r => (
                <button
                  key={r.report_code}
                  onClick={() => navigate(`/raids/${r.report_code}`)}
                  className="w-full text-left px-4 py-3 hover:bg-ctp-surface1/40 transition-colors"
                >
                  <div className="flex items-center justify-between gap-2 mb-1">
                    <span className="text-xs text-ctp-text font-medium truncate flex-1">{r.zone_name || r.report_title || 'Unknown raid'}</span>
                    <DiffBadge label={r.primary_difficulty || 'Unknown'} />
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-mono text-ctp-overlay0">{r.raid_night_date ? formatDateShort(r.raid_night_date) : '—'}</span>
                    <span className="text-[11px] font-mono flex items-center gap-0.5">
                      <Swords className="w-2.5 h-2.5 flex-shrink-0" style={{ color: killColor }} />
                      <span style={{ color: killColor }}>{num(r.boss_kills)}</span>
                      <span className="text-ctp-overlay0 mx-0.5">/</span>
                      <span style={{ color: wipeColor }}>{num(r.total_wipes)}✗</span>
                    </span>
                  </div>
                </button>
              ))}
            </div>
          </Card>

          <div className="flex flex-col gap-3">
            {currentProgBoss ? (
              <button
                onClick={() => navigate(bossHref(currentProgBoss.encounter_id, currentProgBoss.difficulty))}
                className="text-left rounded-2xl border border-ctp-surface1/60 bg-ctp-surface0/60 px-3.5 py-3 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-card-hover"
              >
                <div className="flex items-start justify-between gap-2 mb-1.5">
                  <div className="flex items-center gap-1.5">
                    <span className="w-1.5 h-1.5 rounded-full flex-shrink-0 mt-0.5 bg-ctp-overlay0" />
                    <p className="text-xs font-semibold text-ctp-text leading-tight">{currentProgBoss.boss_name}</p>
                  </div>
                  <DiffBadge label={currentProgBoss.difficulty_label} />
                </div>
                <p className="text-[10px] font-mono mb-1.5 whitespace-nowrap" style={{ color: chartColors.secondary }}>Currently Progging</p>
                <div className="flex items-center justify-between text-[11px] font-mono mb-1.5">
                  <span style={{ color: wipeColor }}>{currentProgBoss.total_wipes}✗</span>
                  <span className="text-ctp-overlay0">{currentProgBoss.total_pulls} pulls</span>
                </div>
                <ProgressBar
                  value={bestHpByBoss.get(`${currentProgBoss.encounter_id}-${currentProgBoss.difficulty}`) ?? 100}
                  color={wipeColor}
                  height="xs"
                />
                <p className="text-[10px] font-mono text-ctp-overlay0 mt-1.5">
                  Best HP: {(bestHpByBoss.get(`${currentProgBoss.encounter_id}-${currentProgBoss.difficulty}`) ?? 100).toFixed(1)}%
                </p>
              </button>
            ) : (
              <div className="rounded-2xl border border-ctp-surface1/60 bg-ctp-surface0/60 px-3.5 py-3 flex items-center justify-center text-xs font-mono text-ctp-overlay0">
                No boss currently being progressed
              </div>
            )}

            {lastDownedBoss ? (
              <button
                onClick={() => navigate(bossHref(lastDownedBoss.encounter_id, lastDownedBoss.difficulty))}
                className="text-left rounded-2xl border border-ctp-surface1 bg-ctp-surface0 px-3.5 py-3 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-card-hover"
              >
                <div className="flex items-start justify-between gap-2 mb-1.5">
                  <div className="flex items-center gap-1.5">
                    <span className="w-1.5 h-1.5 rounded-full flex-shrink-0 mt-0.5" style={{ backgroundColor: killColor }} />
                    <p className="text-xs font-semibold text-ctp-text leading-tight">{lastDownedBoss.boss_name}</p>
                  </div>
                  <DiffBadge label={lastDownedBoss.difficulty_label} />
                </div>
                <p className="text-[10px] font-mono mb-1.5 whitespace-nowrap" style={{ color: killColor }}>Last Downed</p>
                <div className="flex items-center justify-between text-[11px] font-mono mb-1.5">
                  <span className="inline-flex items-center gap-1.5">
                    <span className="inline-flex items-center gap-1" style={{ color: killColor }}>
                      <Swords className="w-3 h-3" />
                      {lastDownedBoss.total_kills}
                    </span>
                    <span style={{ color: wipeColor }}>{lastDownedBoss.total_wipes}✗</span>
                  </span>
                  <span style={{ color: topTierColor }}>{formatDuration(Number(lastDownedBoss.best_kill_seconds))}</span>
                </div>
                <p className="text-[10px] font-mono text-ctp-overlay0 mt-1.5 whitespace-nowrap">
                  First Kill: {lastDownedBoss.first_kill_date ? formatDateShort(lastDownedBoss.first_kill_date) : '—'}
                </p>
              </button>
            ) : (
              <div className="rounded-2xl border border-ctp-surface1 bg-ctp-surface0 px-3.5 py-3 flex items-center justify-center text-xs font-mono text-ctp-overlay0">
                No boss killed yet at this difficulty
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2 flex flex-col">
          <CardHeader>
            <div className="flex items-start justify-between gap-3">
              <div>
                <CardTitle>Parse Distribution</CardTitle>
                <p className="text-xs text-ctp-overlay1 mt-0.5">{currentTier ?? 'No tier'} · {difficulty} · fight-level parse spread</p>
              </div>
              <div className="flex items-center gap-3">
                {parseView === 'Curve' && parseDistributionSummary && (
                  <div className="text-right font-mono whitespace-nowrap">
                    <p className="text-[10px] leading-tight">
                      <span className="text-ctp-mauve">{parseDistributionSummary.type} Distribution</span>{' '}
                      <span style={{ color: getParseColor(parseDistributionSummary.probability) }}>
                        {parseDistributionSummary.probability.toFixed(2)}%
                      </span>
                    </p>
                    <p className="mt-0.5 text-[9px] leading-tight text-ctp-red">
                      σ {parseDistributionSummary.standardDeviation.toFixed(2)} · γ₁ {parseDistributionSummary.skewness.toFixed(2)} · κ {parseDistributionSummary.kurtosis.toFixed(2)}
                    </p>
                  </div>
                )}
                <FilterTabs options={['No Curve', 'Curve']} value={parseView} onChange={setParseView} />
              </div>
            </div>
          </CardHeader>
          <CardBody className="flex-1 flex">
            {killRoster.loading ? (
              <div className="w-full"><LoadingState rows={4} /></div>
            ) : (
              <ParseHistogramChart data={scopedBossRows} showCurve={parseView === 'Curve'} />
            )}
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Leaderboard</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">{currentTier ?? 'No tier'} · {difficulty} · avg parse % · min 2 kills</p>
          </CardHeader>
          <div className="divide-y divide-ctp-surface1">
            {loading ? (
              <div className="p-5"><LoadingState rows={6} /></div>
            ) : topPerformers.length === 0 ? (
              <div className="p-5 text-xs font-mono text-ctp-overlay0">No player kill data matches this dashboard scope.</div>
            ) : topPerformers.map((p, i) => (
              <div
                key={p.player_name}
                className="relative h-[58px] px-3.5 py-2.5 hover:bg-ctp-surface1/30 transition-colors cursor-pointer"
                onClick={() => navigate(`/players/${encodeURIComponent(p.player_name)}`)}
              >
                <span className="absolute left-1.5 top-1 text-[9px] font-mono leading-none text-ctp-overlay0">{i + 1}</span>
                <div className="grid h-full grid-cols-[0.75rem_minmax(0,1fr)] items-center gap-x-2">
                  <div className="flex h-4 items-center justify-center">
                    <RoleIcon role={p.role} playerClass={p.player_class} />
                  </div>
                  <div className="min-w-0 flex flex-col justify-center gap-1">
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate text-xs font-medium text-ctp-text">{p.player_name}</span>
                      <span
                        className="text-xs font-mono font-semibold flex-shrink-0"
                        style={{ color: getParseColor(num(p.avg_rank_percent)) }}
                      >
                        {num(p.avg_rank_percent).toFixed(0)}%
                      </span>
                    </div>
                    <ProgressBar
                      value={num(p.avg_rank_percent)}
                      color={getParseColor(num(p.avg_rank_percent))}
                      height="xs"
                    />
                    <div className="-mt-0.5 flex items-center justify-between gap-3">
                      <span className="min-w-0 truncate pl-0.5 text-[10px] font-mono text-ctp-overlay0">
                        {p.primary_spec} {p.player_class}
                      </span>
                      <span className="flex-shrink-0 text-[10px] font-mono text-right" style={{ color: getThroughputColor(p.role) }}>
                        {formatThroughput(p.avg_throughput_per_second)}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </AppLayout>
  )
}
