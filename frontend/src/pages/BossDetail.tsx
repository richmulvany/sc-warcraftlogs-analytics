import { useMemo } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, ExternalLink } from 'lucide-react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardBody, CardHeader, CardTitle } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { Table, TBody, Td, Th, THead, Tr } from '../components/ui/Table'
import { ErrorState } from '../components/ui/ErrorState'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { BossProgressHistoryChart } from '../components/charts/BossProgressHistoryChart'
import { useBestKills, useBossProgressHistory, useBossProgression, useBossWipeAnalysis } from '../hooks/useGoldData'
import { formatDate, formatNumber } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

function isKilled(value: string | boolean) {
  return value === true || value === 'true' || value === 'True'
}

export function BossDetail() {
  const { encounterId, difficulty } = useParams<{ encounterId: string; difficulty: string }>()
  const navigate = useNavigate()
  const { getDifficultyColor, killColor, wipeColor, topTierColor } = useColourBlind()

  const progression = useBossProgression()
  const history = useBossProgressHistory()
  const bestKills = useBestKills()
  const wipes = useBossWipeAnalysis()

  const boss = useMemo(() =>
    progression.data.find(row => row.encounter_id === encounterId && row.difficulty === difficulty),
    [progression.data, encounterId, difficulty]
  )

  const bossHistory = useMemo(() =>
    [...history.data]
      .filter(row => row.encounter_id === encounterId && row.difficulty === difficulty)
      .sort((a, b) => {
        const byDate = String(a.raid_night_date).localeCompare(String(b.raid_night_date))
        if (byDate !== 0) return byDate
        return String(a.start_time_utc ?? '').localeCompare(String(b.start_time_utc ?? ''))
      }),
    [history.data, encounterId, difficulty]
  )

  const wipe = useMemo(() =>
    wipes.data.find(row => row.encounter_id === encounterId && row.difficulty === difficulty),
    [wipes.data, encounterId, difficulty]
  )

  const bestKill = useMemo(() =>
    bestKills.data.find(row => row.encounter_id === encounterId && row.difficulty === difficulty),
    [bestKills.data, encounterId, difficulty]
  )

  const loading = progression.loading || history.loading || bestKills.loading || wipes.loading
  const error = progression.error || history.error || bestKills.error || wipes.error
  const killed = boss ? isKilled(boss.is_killed) : false
  const currentBestHp = killed ? 0 : Number(wipe?.best_wipe_pct ?? 100)

  return (
    <AppLayout
      title={boss?.boss_name ?? 'Boss Detail'}
      subtitle={boss ? `${boss.zone_name} · ${boss.difficulty_label}` : `${encounterId} · ${difficulty}`}
    >
      <button
        onClick={() => navigate(-1)}
        className="flex items-center gap-1.5 text-xs font-mono text-ctp-overlay1 hover:text-ctp-subtext1 transition-colors mb-2"
      >
        <ArrowLeft className="w-3.5 h-3.5" />
        back
      </button>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4"><ErrorState message={error} /></div>
        ) : boss ? (
          <>
            <StatCard
              label="Best HP"
              value={killed ? '0.0%' : `${currentBestHp.toFixed(1)}%`}
              subValue={killed ? 'boss killed' : 'closest wipe'}
              valueColor={getDifficultyColor(boss.difficulty_label)}
              accent="none"
            />
            <StatCard
              label="Total Pulls"
              value={formatNumber(boss.total_pulls)}
              subValue="all tracked pulls"
            />
            <StatCard
              label="Kills / Wipes"
              value={`${formatNumber(boss.total_kills)} / ${formatNumber(boss.total_wipes)}`}
              subValue="overall record"
              valueColor={killed ? killColor : wipeColor}
              accent="none"
            />
            <StatCard
              label="Best Kill"
              value={bestKill?.best_kill_mm_ss ?? (killed ? formatDuration(Number(boss.best_kill_seconds)) : '—')}
              subValue={killed ? `first kill ${formatDate(boss.first_kill_date)}` : 'not killed yet'}
              valueColor={killed ? topTierColor : undefined}
              accent="none"
            />
          </>
        ) : (
          <div className="col-span-4"><ErrorState message="Boss not found in exported progression data." /></div>
        )}
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <CardTitle>Progress Curve</CardTitle>
              <p className="text-xs text-ctp-overlay1 mt-0.5">Best boss HP remaining by raid night</p>
            </div>
            {boss && <DiffBadge label={boss.difficulty_label} />}
          </div>
        </CardHeader>
        <CardBody>
          <BossProgressHistoryChart data={bossHistory} />
        </CardBody>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Progression Log</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">One row per report with direct links to Warcraft Logs</p>
        </CardHeader>
        {loading ? (
          <CardBody><LoadingState rows={8} /></CardBody>
        ) : error ? (
          <CardBody><ErrorState message={error} /></CardBody>
        ) : bossHistory.length === 0 ? (
          <CardBody>
            <div className="h-40 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
              No boss progress history exported yet
            </div>
          </CardBody>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>Date</Th>
                <Th>Report</Th>
                <Th right>Pulls</Th>
                <Th right>Wipes</Th>
                <Th right>Best HP</Th>
                <Th right>Kill Time</Th>
                <Th>Result</Th>
              </tr>
            </THead>
            <TBody>
              {[...bossHistory].reverse().map(row => {
                const rowKilled = isKilled(row.is_kill_on_night)
                return (
                  <Tr key={`${row.report_code}-${row.raid_night_date}`}>
                    <Td className="text-xs text-ctp-overlay1">{formatDate(row.raid_night_date)}</Td>
                    <Td>
                      <a
                        href={`https://www.warcraftlogs.com/reports/${row.report_code}`}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1.5 text-xs font-medium text-ctp-text hover:text-ctp-mauve transition-colors"
                      >
                        <span className="truncate max-w-[18rem]">{row.report_title || row.report_code}</span>
                        <ExternalLink className="w-3 h-3 flex-shrink-0" />
                      </a>
                    </Td>
                    <Td right mono>{formatNumber(row.pulls_on_night)}</Td>
                    <Td right mono style={{ color: wipeColor }}>{formatNumber(row.wipes_on_night)}</Td>
                    <Td right mono style={{ color: getDifficultyColor(String(row.difficulty_label)) }}>
                      {rowKilled ? '0.0%' : `${Number(row.best_boss_hp_remaining).toFixed(1)}%`}
                    </Td>
                    <Td right mono style={{ color: rowKilled ? topTierColor : undefined }}>
                      {rowKilled && Number(row.kill_duration_seconds) > 0 ? formatDuration(Number(row.kill_duration_seconds)) : '—'}
                    </Td>
                    <Td>
                      <span className="text-xs font-mono" style={{ color: rowKilled ? killColor : wipeColor }}>
                        {rowKilled ? 'Kill' : 'Wipe'}
                      </span>
                    </Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
        )}
      </Card>

      {boss && (
        <div className="flex justify-end">
          <Link
            to="/bosses"
            className="text-xs font-mono text-ctp-overlay1 hover:text-ctp-subtext1 transition-colors"
          >
            back to boss progression
          </Link>
        </div>
      )}
    </AppLayout>
  )
}
