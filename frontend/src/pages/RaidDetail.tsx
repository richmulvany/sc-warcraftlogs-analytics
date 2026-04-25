import { useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Swords } from 'lucide-react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge, RoleBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot } from '../components/ui/ClassLabel'
import { ProgressBar } from '../components/ui/ProgressBar'
import { useRaidSummary, useBossKillRoster } from '../hooks/useGoldData'
import { formatNumber, formatDate, toFiniteNumber, meanIgnoringNulls } from '../utils/format'
import { formatDuration, formatThroughput, getThroughputColor } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'

export function RaidDetail() {
  const { reportCode } = useParams<{ reportCode: string }>()
  const navigate = useNavigate()
  const { getParseColor, killColor, wipeColor } = useColourBlind()

  const raidSummary = useRaidSummary()
  const killRoster  = useBossKillRoster()

  const raid = useMemo(
    () => raidSummary.data.find(r => r.report_code === reportCode),
    [raidSummary.data, reportCode]
  )

  // All kill-roster rows for this report, grouped by encounter
  const bossRows = useMemo(
    () => killRoster.data.filter(r =>
      r.report_code === reportCode &&
      (!raid?.zone_name || r.zone_name === raid.zone_name)
    ),
    [killRoster.data, reportCode, raid?.zone_name]
  )

  const bosses = useMemo(() => {
    const map = new Map<string, { bossName: string; encounterKey: string; difficulty: string; difficultyLabel: string; duration: number; players: typeof bossRows }>()
    for (const row of bossRows) {
      const key = `${row.encounter_id}-${row.fight_id}`
      if (!map.has(key)) {
        map.set(key, {
          bossName:       row.boss_name,
          encounterKey:   key,
          difficulty:     row.difficulty,
          difficultyLabel: row.difficulty_label,
          duration:       Number(row.duration_seconds),
          players:        [],
        })
      }
      map.get(key)!.players.push(row)
    }
    return [...map.values()].sort((a, b) => a.bossName.localeCompare(b.bossName))
  }, [bossRows])

  const topParses = useMemo(() =>
    [...bossRows]
      .filter(r => {
        const v = toFiniteNumber(r.rank_percent)
        return v !== null && v > 0
      })
      .sort((a, b) => (toFiniteNumber(b.rank_percent) ?? 0) - (toFiniteNumber(a.rank_percent) ?? 0))
      .slice(0, 10),
    [bossRows]
  )

  const loading = raidSummary.loading || killRoster.loading
  const error   = raidSummary.error || killRoster.error

  return (
    <AppLayout
      title={raid?.zone_name ?? 'Raid Detail'}
      subtitle={raid ? `${formatDate(raid.raid_night_date)} · ${raid.primary_difficulty}` : reportCode}
    >
      {/* Back nav */}
      <button
        onClick={() => navigate(-1)}
        className="flex items-center gap-1.5 text-xs font-mono text-ctp-overlay1 hover:text-ctp-subtext1 transition-colors mb-2"
      >
        <ArrowLeft className="w-3.5 h-3.5" />
        back
      </button>

      {/* KPI strip */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4"><ErrorState message={error} /></div>
        ) : (
          <>
            <StatCard
              label="Boss Kills"
              value={formatNumber(raid?.boss_kills ?? 0)}
              subValue={raid?.zone_name}
              icon={<Swords className="w-3.5 h-3.5" />}
              valueColor={killColor}
              accent="none"
            />
            <StatCard
              label="Total Wipes"
              value={formatNumber(raid?.total_wipes ?? 0)}
              subValue="before kills"
              icon="✗"
              valueColor={wipeColor}
              accent="none"
            />
            <StatCard
              label="Total Pulls"
              value={formatNumber(raid?.total_pulls ?? 0)}
              subValue="including kills"
            />
            <StatCard
              label="Raid Duration"
              value={formatDuration(Number(raid?.total_fight_seconds ?? 0))}
              subValue="active fight time"
            />
          </>
        )}
      </div>

      {/* Bosses killed */}
      {!loading && bosses.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Bosses Killed</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">{bosses.length} encounters</p>
          </CardHeader>
          <Table>
            <THead>
              <tr>
                <Th>Boss</Th>
                <Th>Difficulty</Th>
                <Th right>Duration</Th>
                <Th right>Players</Th>
                <Th right>Avg Parse</Th>
              </tr>
            </THead>
            <TBody>
              {bosses.map(b => {
                const parses = b.players.map(p => toFiniteNumber(p.rank_percent))
                const haveParses = parses.some(v => v !== null)
                const avgParse = meanIgnoringNulls(parses)
                return (
                  <Tr key={b.encounterKey}>
                    <Td className="font-medium text-ctp-text">{b.bossName}</Td>
                    <Td><DiffBadge label={b.difficultyLabel} /></Td>
                    <Td right mono className="text-ctp-subtext1">{formatDuration(b.duration)}</Td>
                    <Td right mono className="text-ctp-overlay1">{b.players.length}</Td>
                    <Td right mono>
                      {haveParses
                        ? <span style={{ color: getParseColor(avgParse) }}>{avgParse.toFixed(0)}%</span>
                        : <span className="text-ctp-overlay0">—</span>}
                    </Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
        </Card>
      )}

      {/* Top parses this raid */}
      {!loading && topParses.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Top Parses This Raid</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Highest individual WCL rank %</p>
          </CardHeader>
          <div className="divide-y divide-ctp-surface1">
            {topParses.map((p, i) => (
              <div key={`${p.fight_id}-${p.player_name}`} className="px-4 py-2.5">
                <div className="flex items-center gap-2.5">
                  <span className="text-[10px] font-mono text-ctp-overlay0 w-4 flex-shrink-0">{i + 1}</span>
                  <ClassDot className={p.player_class} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-ctp-text font-medium truncate">{p.player_name}</span>
                      {(() => {
                        const v = toFiniteNumber(p.rank_percent) ?? 0
                        return (
                          <span
                            className="text-xs font-mono font-semibold flex-shrink-0"
                            style={{ color: getParseColor(v) }}
                          >
                            {v.toFixed(0)}%
                          </span>
                        )
                      })()}
                    </div>
                    {(() => {
                      const v = toFiniteNumber(p.rank_percent) ?? 0
                      return (
                        <ProgressBar
                          value={v}
                          color={getParseColor(v)}
                          height="xs"
                          className="mt-1"
                        />
                      )
                    })()}
                  </div>
                </div>
                <div className="mt-0.5 ml-10 flex items-center gap-3">
                  <span className="text-[10px] font-mono text-ctp-overlay0">{p.boss_name}</span>
                  <RoleBadge role={p.role} />
                  <span className="text-[10px] font-mono" style={{ color: getThroughputColor(p.role) }}>
                    {formatThroughput(Number(p.throughput_per_second))}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Full roster table */}
      {!loading && bossRows.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Full Performance Log</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">All tracked players across all kills</p>
          </CardHeader>
          <Table>
            <THead>
              <tr>
                <Th>Player</Th>
                <Th>Boss</Th>
                <Th>Role</Th>
                <Th>Spec</Th>
                <Th right>iLvl</Th>
                <Th right>DPS/HPS</Th>
                <Th right>Parse %</Th>
              </tr>
            </THead>
            <TBody>
              {[...bossRows]
                .sort((a, b) => (toFiniteNumber(b.rank_percent) ?? -1) - (toFiniteNumber(a.rank_percent) ?? -1))
                .map((row, i) => (
                  <Tr
                    key={`${row.fight_id}-${row.player_name}-${i}`}
                    onClick={() => navigate(`/players/${encodeURIComponent(row.player_name)}`)}
                  >
                    <Td>
                      <div className="flex items-center gap-2">
                        <ClassDot className={row.player_class} />
                        <span className="font-medium text-ctp-text">{row.player_name}</span>
                      </div>
                    </Td>
                    <Td className="text-ctp-overlay1 text-xs">{row.boss_name}</Td>
                    <Td><RoleBadge role={row.role} /></Td>
                    <Td className="text-ctp-overlay1 text-xs">{row.spec}</Td>
                    <Td right mono className="text-ctp-overlay1">{Number(row.avg_item_level).toFixed(0)}</Td>
                    <Td right mono style={{ color: getThroughputColor(row.role) }}>
                      {formatThroughput(Number(row.throughput_per_second))}
                    </Td>
                    <Td right mono>
                      {(() => {
                        const v = toFiniteNumber(row.rank_percent)
                        return v === null
                          ? <span className="text-ctp-overlay0">—</span>
                          : <span style={{ color: getParseColor(v) }}>{v.toFixed(0)}%</span>
                      })()}
                    </Td>
                  </Tr>
                ))}
            </TBody>
          </Table>
        </Card>
      )}

      {!loading && !error && bossRows.length === 0 && (
        <Card>
          <CardBody>
            <p className="text-xs font-mono text-ctp-overlay0 text-center py-8">
              No kill data found for this report code.
            </p>
          </CardBody>
        </Card>
      )}
    </AppLayout>
  )
}
