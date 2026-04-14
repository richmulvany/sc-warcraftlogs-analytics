import { useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { RoleBadge } from '../components/ui/Badge'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassLabel } from '../components/ui/ClassLabel'
import { ProgressBar } from '../components/ui/ProgressBar'
import { DpsOverTimeChart } from '../components/charts/DpsOverTimeChart'
import { PerformanceHeatmap } from '../components/charts/PerformanceHeatmap'
import {
  usePlayerPerformance,
  usePlayerSurvivability,
  usePlayerAttendance,
  useBossKillRoster,
  usePlayerBossPerformance,
} from '../hooks/useGoldData'
import { formatThroughput, getClassColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import { formatDate, formatPct } from '../utils/format'

export function PlayerDetail() {
  const { getParseColor, wipeColor } = useColourBlind()
  const { playerName } = useParams<{ playerName: string }>()
  const navigate       = useNavigate()

  const name = decodeURIComponent(playerName ?? '')

  const perf   = usePlayerPerformance()
  const surv   = usePlayerSurvivability()
  const att    = usePlayerAttendance()
  const roster = useBossKillRoster()
  const bossPf = usePlayerBossPerformance()

  // Summary row for this player
  const summary = useMemo(
    () => perf.data.find(p => p.player_name === name),
    [perf.data, name]
  )

  // Survivability row
  const survRow = useMemo(
    () => surv.data.find(s => s.player_name === name),
    [surv.data, name]
  )

  // Attendance row
  const attRow = useMemo(
    () => att.data.find(a => a.player_name === name),
    [att.data, name]
  )

  // Boss kill roster: DPS over time for this player
  const dpsOverTime = useMemo(() => {
    return roster.data
      .filter(r => r.player_name === name && r.throughput_per_second > 0)
      .map(r => ({
        date:       r.raid_night_date,
        throughput: Number(r.throughput_per_second),
        boss:       r.boss_name,
        parse:      Number(r.rank_percent) || 0,
      }))
      .sort((a, b) => a.date.localeCompare(b.date))
  }, [roster.data, name])

  // Boss performance for heatmap
  const bossPerformance = useMemo(
    () => bossPf.data.filter(b => b.player_name === name),
    [bossPf.data, name]
  )

  const loading = perf.loading || surv.loading || att.loading || roster.loading || bossPf.loading
  const error   = perf.error || surv.error

  const classColor = summary ? getClassColor(summary.player_class) : '#cdd6f4'

  if (!loading && !summary) {
    return (
      <AppLayout title={name} subtitle="player not found">
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
    >
      {/* Player header card */}
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
            {/* Class icon circle */}
            <div
              className="w-14 h-14 rounded-2xl flex items-center justify-center flex-shrink-0 text-xl font-bold border"
              style={{ background: `${classColor}15`, borderColor: `${classColor}30`, color: classColor }}
            >
              {summary.player_class[0]}
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
                <span className="text-xs text-ctp-overlay1 font-mono">{summary.realm}</span>
              </div>
            </div>

            {/* Quick stats inline */}
            <div className="flex items-center gap-6 flex-shrink-0">
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best Parse</p>
                <p
                  className="text-lg font-semibold"
                  style={{ color: getParseColor(summary.best_rank_percent) }}
                >
                  {summary.best_rank_percent ? `${summary.best_rank_percent.toFixed(0)}%` : '—'}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Best DPS/HPS</p>
                <p className="text-lg font-semibold text-ctp-text">
                  {formatThroughput(summary.best_throughput_per_second)}
                </p>
              </div>
              <div className="text-center">
                <p className="text-xs font-mono text-ctp-overlay0 mb-0.5">Last Seen</p>
                <p className="text-sm font-mono text-ctp-subtext1">{formatDate(summary.last_seen_date)}</p>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      {/* 4 KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard
              label="Avg DPS / HPS"
              value={formatThroughput(summary?.avg_throughput_per_second ?? 0)}
              subValue={`Best: ${formatThroughput(summary?.best_throughput_per_second ?? 0)}`}
              icon="◉"
              accent="blue"
            />
            <StatCard
              label="Avg Percentile"
              value={summary?.avg_rank_percent ? `${summary.avg_rank_percent.toFixed(1)}%` : '—'}
              subValue={`Best: ${summary?.best_rank_percent ? `${summary.best_rank_percent.toFixed(0)}%` : '—'}`}
              icon="◈"
              accent="mauve"
            />
            <StatCard
              label="Deaths per Kill"
              value={survRow?.deaths_per_kill != null ? survRow.deaths_per_kill.toFixed(1) : '—'}
              subValue={`${survRow?.total_deaths ?? 0} total deaths`}
              icon="☠"
              accent="red"
            />
            <StatCard
              label="Attendance"
              value={attRow?.attendance_rate_pct != null ? formatPct(attRow.attendance_rate_pct) : '—'}
              subValue={attRow ? `${attRow.raids_present} / ${attRow.total_raids_tracked} raids` : 'No data'}
              icon="◷"
              accent="green"
            />
          </>
        )}
      </div>

      {/* DPS over time */}
      <Card>
        <CardHeader>
          <CardTitle>DPS / HPS Over Time</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            {dpsOverTime.length} kill performances recorded
            {summary?.avg_throughput_per_second
              ? ` · avg ${formatThroughput(summary.avg_throughput_per_second)}`
              : ''}
          </p>
        </CardHeader>
        <CardBody>
          {roster.loading ? (
            <LoadingState rows={5} />
          ) : (
            <DpsOverTimeChart
              data={dpsOverTime}
              playerClass={summary?.player_class ?? 'Unknown'}
              avgThroughput={summary?.avg_throughput_per_second}
            />
          )}
        </CardBody>
      </Card>

      {/* Parse & survivability split */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Parse by boss breakdown (from dpsOverTime data) */}
        <Card>
          <CardHeader>
            <CardTitle>Parse Breakdown by Fight</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Individual WCL rank % per recorded fight</p>
          </CardHeader>
          <CardBody className="space-y-2.5">
            {roster.loading ? (
              <LoadingState rows={6} />
            ) : dpsOverTime.length === 0 ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No fight data available</p>
            ) : (
              [...dpsOverTime]
                .filter(d => d.parse > 0)
                .sort((a, b) => b.parse - a.parse)
                .slice(0, 12)
                .map((d, i) => (
                  <div key={i} className="flex items-center gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs text-ctp-subtext1 truncate">{d.boss}</span>
                        <span
                          className="text-xs font-mono font-semibold flex-shrink-0 ml-2"
                          style={{ color: getParseColor(d.parse) }}
                        >
                          {d.parse.toFixed(0)}%
                        </span>
                      </div>
                      <ProgressBar
                        value={d.parse}
                        color={getParseColor(d.parse)}
                        height="xs"
                      />
                    </div>
                    <span className="text-[10px] font-mono text-ctp-overlay0 flex-shrink-0 w-16 text-right">
                      {formatDate(d.date)}
                    </span>
                  </div>
                ))
            )}
          </CardBody>
        </Card>

        {/* Survivability */}
        <Card>
          <CardHeader>
            <CardTitle>Survivability</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Death analysis</p>
          </CardHeader>
          <CardBody>
            {surv.loading ? (
              <LoadingState rows={4} />
            ) : !survRow ? (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No death data recorded</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Total Deaths</p>
                    <p className="text-xl font-semibold" style={{ color: wipeColor }}>{survRow.total_deaths}</p>
                  </div>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="section-label mb-1">Deaths / Kill</p>
                    <p className="text-xl font-semibold text-ctp-peach">
                      {survRow.deaths_per_kill?.toFixed(1) ?? '—'}
                    </p>
                  </div>
                </div>

                <div>
                  <p className="section-label mb-2">Most Common Killing Blow</p>
                  <div className="bg-ctp-surface1/40 rounded-xl p-3">
                    <p className="text-sm font-medium text-ctp-text">
                      {survRow.most_common_killing_blow || '—'}
                    </p>
                    <p className="text-xs text-ctp-overlay0 mt-0.5">
                      {survRow.most_common_killing_blow_count} occurrences
                    </p>
                  </div>
                </div>
              </div>
            )}
          </CardBody>
        </Card>
      </div>

      {/* Boss performance heatmap */}
      <Card>
        <CardHeader>
          <CardTitle>Performance by Boss</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Avg WCL parse % per boss encounter · colour = parse tier
          </p>
        </CardHeader>
        <CardBody>
          {bossPf.loading ? (
            <LoadingState rows={4} />
          ) : error ? (
            <ErrorState message={error} />
          ) : (
            <PerformanceHeatmap data={bossPerformance} />
          )}
        </CardBody>
      </Card>
    </AppLayout>
  )
}
