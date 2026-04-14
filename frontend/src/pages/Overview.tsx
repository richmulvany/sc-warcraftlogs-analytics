import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { WeeklyActivityChart } from '../components/charts/WeeklyActivityChart'
import { ParseDistributionChart } from '../components/charts/ParseDistributionChart'
import { ClassDot } from '../components/ui/ClassLabel'
import {
  useRaidSummary,
  usePlayerPerformance,
  useBossProgression,
  useWeeklyActivity,
  useGuildRoster,
} from '../hooks/useGoldData'
import { formatNumber, formatDateShort } from '../utils/format'
import { formatThroughput } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

export function Overview() {
  const { getParseColor, killColor, wipeColor } = useColourBlind()
  const raids   = useRaidSummary()
  const players = usePlayerPerformance()
  const bosses  = useBossProgression()
  const weekly  = useWeeklyActivity()
  const roster  = useGuildRoster()
  const navigate = useNavigate()

  const stats = useMemo(() => {
    if (!raids.data.length) return null
    const totalKills = raids.data.reduce((s, r) => s + Number(r.boss_kills), 0)
    const totalWipes = raids.data.reduce((s, r) => s + Number(r.total_wipes), 0)
    const zones = [...new Set(raids.data.map(r => r.zone_name))]
    return { totalKills, totalWipes, totalRaids: raids.data.length, zones }
  }, [raids.data])

  const activePlayers = useMemo(() =>
    roster.data.filter(r => r.is_active === 'True' || r.is_active === (true as unknown as string)).length,
    [roster.data]
  )

  const killedBosses = useMemo(() =>
    bosses.data.filter(b => b.is_killed === 'True' || b.is_killed === (true as unknown as string)),
    [bosses.data]
  )

  const topPerformers = useMemo(() =>
    [...players.data]
      .filter(p => p.avg_rank_percent != null && p.kills_tracked >= 2)
      .sort((a, b) => b.avg_rank_percent - a.avg_rank_percent)
      .slice(0, 8),
    [players.data]
  )

  const recentRaids = useMemo(() =>
    [...raids.data].sort((a, b) => b.raid_night_date.localeCompare(a.raid_night_date)).slice(0, 6),
    [raids.data]
  )

  const loading = raids.loading || players.loading || bosses.loading || weekly.loading
  const error   = raids.error || players.error || bosses.error

  return (
    <AppLayout title="Dashboard" subtitle="Student Council · Twisting Nether EU">
      {/* KPI strip */}
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
              subValue="all difficulties"
              icon="⚔"
              accent="mauve"
            />
            <StatCard
              label="Raid Nights"
              value={formatNumber(stats?.totalRaids ?? 0)}
              subValue="tracked sessions"
              icon="◷"
              accent="blue"
            />
            <StatCard
              label="Active Players"
              value={formatNumber(activePlayers)}
              subValue="raid team members"
              icon="◉"
              accent="teal"
            />
            <StatCard
              label="Unique Bosses Killed"
              value={killedBosses.length}
              subValue={`across ${stats?.zones.length ?? 0} zones`}
              icon="◈"
              accent="peach"
            />
          </>
        )}
      </div>

      {/* Middle row: activity chart + recent raids */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Weekly Activity</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Boss kills vs wipes per raid week</p>
          </CardHeader>
          <CardBody>
            {weekly.loading ? <LoadingState rows={4} /> :
             weekly.error   ? <ErrorState message={weekly.error} /> :
             <WeeklyActivityChart data={weekly.data} />}
            <div className="flex items-center gap-5 mt-3">
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 rounded bg-ctp-blue" />
                <span className="text-[11px] font-mono text-ctp-overlay0">Boss Kills</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 rounded bg-ctp-red" />
                <span className="text-[11px] font-mono text-ctp-overlay0">Wipes</span>
              </div>
            </div>
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent Raids</CardTitle>
          </CardHeader>
          <div className="divide-y divide-ctp-surface1">
            {raids.loading ? (
              <div className="p-5"><LoadingState rows={5} /></div>
            ) : recentRaids.map(r => (
              <div key={r.report_code} className="px-4 py-3 hover:bg-ctp-surface1/30 transition-colors cursor-default">
                <div className="flex items-center justify-between gap-2 mb-1">
                  <span className="text-xs text-ctp-text font-medium truncate flex-1">{r.zone_name}</span>
                  <DiffBadge label={r.primary_difficulty} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-[11px] font-mono text-ctp-overlay0">{formatDateShort(r.raid_night_date)}</span>
                  <span className="text-[11px] font-mono">
                    <span style={{ color: killColor }}>{r.boss_kills}↓</span>
                    <span className="text-ctp-overlay0 mx-0.5">/</span>
                    <span style={{ color: wipeColor }}>{r.total_wipes}✗</span>
                  </span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Bottom row: parse chart + leaderboard */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Parse Distribution</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Average WCL rank % per player (top 15)</p>
          </CardHeader>
          <CardBody>
            {players.loading ? <LoadingState rows={4} /> : <ParseDistributionChart data={players.data} />}
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Leaderboard</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Avg parse % · min 2 kills</p>
          </CardHeader>
          <div className="divide-y divide-ctp-surface1">
            {players.loading ? (
              <div className="p-5"><LoadingState rows={6} /></div>
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
                        style={{ color: getParseColor(p.avg_rank_percent) }}
                      >
                        {p.avg_rank_percent?.toFixed(0)}%
                      </span>
                    </div>
                    <ProgressBar
                      value={p.avg_rank_percent}
                      color={getParseColor(p.avg_rank_percent)}
                      height="xs"
                      className="mt-1"
                    />
                  </div>
                </div>
                <div className="mt-0.5 ml-10 flex items-center gap-3">
                  <span className="text-[10px] font-mono text-ctp-overlay0">
                    {p.primary_spec} {p.player_class}
                  </span>
                  <span className="text-[10px] font-mono text-ctp-overlay0">
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
