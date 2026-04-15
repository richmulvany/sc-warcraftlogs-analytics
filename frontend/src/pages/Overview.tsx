import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Swords } from 'lucide-react'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { WeeklyActivityChart } from '../components/charts/WeeklyActivityChart'
import { ParseHistogramChart } from '../components/charts/ParseHistogramChart'
import { ClassDot } from '../components/ui/ClassLabel'
import {
  useRaidSummary,
  useBossProgression,
  useBossKillRoster,
} from '../hooks/useGoldData'
import { formatNumber, formatDateShort } from '../utils/format'
import { isIncludedZoneName } from '../utils/zones'
import { formatThroughput, getThroughputColor } from '../constants/wow'
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
  const { getParseColor, killColor, wipeColor, chartColors } = useColourBlind()
  const raids = useRaidSummary()
  const bosses = useBossProgression()
  const killRoster = useBossKillRoster()
  const navigate = useNavigate()
  const [difficulty, setDifficulty] = useState<DifficultyFilter>('Mythic')

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
      .slice(0, 8)
  }, [scopedBossRows])

  const recentRaids = useMemo(() =>
    [...scopedRaids]
      .sort((a, b) => String(b.raid_night_date ?? '').localeCompare(String(a.raid_night_date ?? '')))
      .slice(0, 6),
    [scopedRaids]
  )

  const loading = raids.loading || bosses.loading || killRoster.loading
  const error = raids.error || bosses.error || killRoster.error

  return (
    <AppLayout title="Dashboard" subtitle="Student Council · Twisting Nether EU">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <p className="text-[10px] font-mono text-ctp-overlay0">Dashboard Scope</p>
          <p className="text-sm font-semibold text-ctp-text mt-0.5">
            {currentTier ?? 'No current tier data'}
          </p>
        </div>
        <div className="ml-auto flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {DIFFICULTY_FILTERS.map(option => (
            <button
              key={option}
              onClick={() => setDifficulty(option)}
              className={clsx(
                'px-3 py-2 rounded-lg text-xs font-medium transition-all duration-150',
                difficulty === option
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {option}
            </button>
          ))}
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

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Weekly Activity</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Current raid tier vs previous raid tier across all available Wednesday-start raid weeks</p>
          </CardHeader>
          <CardBody>
            {raids.loading ? <LoadingState rows={4} /> :
             raids.error ? <ErrorState message={raids.error} /> :
             <WeeklyActivityChart raids={validRaidRows} />}
            <div className="flex items-center gap-5 mt-3">
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 rounded" style={{ backgroundColor: chartColors.primary }} />
                <span className="text-[11px] font-mono text-ctp-overlay0">Boss Kills</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 rounded" style={{ backgroundColor: chartColors.secondary }} />
                <span className="text-[11px] font-mono text-ctp-overlay0">Wipes</span>
              </div>
            </div>
          </CardBody>
        </Card>

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
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Parse Distribution</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">{currentTier ?? 'No tier'} · {difficulty} · fight-level parse spread</p>
          </CardHeader>
          <CardBody>
            {killRoster.loading ? <LoadingState rows={4} /> : <ParseHistogramChart data={scopedBossRows} />}
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
                className="px-4 py-2.5 hover:bg-ctp-surface1/30 transition-colors cursor-pointer"
                onClick={() => navigate(`/players/${encodeURIComponent(p.player_name)}`)}
              >
                <div className="flex items-center gap-2.5">
                  <span className="text-[10px] font-mono text-ctp-overlay0 w-4 flex-shrink-0">{i + 1}</span>
                  <ClassDot className={p.player_class} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-ctp-text font-medium truncate">{p.player_name}</span>
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
                      className="mt-1"
                    />
                  </div>
                </div>
                <div className="mt-0.5 ml-10 flex items-center gap-3">
                  <span className="text-[10px] font-mono text-ctp-overlay0">
                    {p.primary_spec} {p.player_class}
                  </span>
                  <span className="text-[10px] font-mono" style={{ color: getThroughputColor(p.role) }}>
                    {formatThroughput(p.avg_throughput_per_second)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </AppLayout>
  )
}
