import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useBossProgression, useBossWipeAnalysis, useBestKills } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal']

export function Progression() {
  const { getDifficultyColor, killColor, wipeColor } = useColourBlind()
  const prog  = useBossProgression()
  const wipes = useBossWipeAnalysis()
  const best  = useBestKills()

  const [diffFilter, setDiffFilter] = useState('All')
  const [zoneFilter, setZoneFilter] = useState('All')

  const zones = useMemo(() => {
    const zs = [...new Set(prog.data.map(b => b.zone_name))].sort()
    return ['All', ...zs]
  }, [prog.data])

  const filtered = useMemo(() =>
    prog.data
      .filter(b => diffFilter === 'All' || b.difficulty_label === diffFilter)
      .filter(b => zoneFilter === 'All' || b.zone_name === zoneFilter)
      .sort((a, b) => {
        // Sort: zone asc, then by total_kills desc
        if (a.zone_name !== b.zone_name) return a.zone_name.localeCompare(b.zone_name)
        return Number(b.total_kills) - Number(a.total_kills)
      }),
    [prog.data, diffFilter, zoneFilter]
  )

  const summaryStats = useMemo(() => {
    const killed = prog.data.filter(b => b.is_killed === 'True' || b.is_killed === true as unknown as string)
    const totalPulls = prog.data.reduce((s, b) => s + Number(b.total_pulls), 0)
    const totalKills = prog.data.reduce((s, b) => s + Number(b.total_kills), 0)
    const mythicKills = prog.data.filter(b => b.difficulty_label === 'Mythic' && (b.is_killed === 'True' || b.is_killed === true as unknown as string))
    return { killed: killed.length, totalPulls, totalKills, mythicKills: mythicKills.length }
  }, [prog.data])

  // Best wipes info per boss
  const wipeMap = useMemo(() => {
    const m: Record<string, typeof wipes.data[0]> = {}
    wipes.data.forEach(w => { m[`${w.encounter_id}-${w.difficulty}`] = w })
    return m
  }, [wipes.data])

  const loading = prog.loading
  const error   = prog.error

  return (
    <AppLayout title="Progression" subtitle="boss kill history">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Bosses Killed" value={summaryStats.killed} subValue="unique bosses" icon="⚔" accent="mauve" />
        <StatCard label="Total Kills"   value={formatNumber(summaryStats.totalKills)} subValue="all difficulties" />
        <StatCard label="Mythic Kills"  value={summaryStats.mythicKills} subValue="mythic difficulty" />
        <StatCard label="Total Pulls"   value={formatNumber(summaryStats.totalPulls)} subValue="all attempts" />
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-3">
        {/* Difficulty tabs */}
        <div className="flex items-center gap-1 bg-ctp-surface0 rounded-lg p-1 border border-ctp-surface1">
          {DIFFS.map(d => (
            <button
              key={d}
              onClick={() => setDiffFilter(d)}
              className={clsx(
                'px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150',
                diffFilter === d
                  ? 'bg-ctp-mauve/20 text-ctp-mauve'
                  : 'text-ctp-overlay1 hover:text-ctp-text'
              )}
            >
              {d}
            </button>
          ))}
        </div>

        {/* Zone select */}
        <select
          value={zoneFilter}
          onChange={e => setZoneFilter(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-lg px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40"
        >
          {zones.map(z => <option key={z} value={z}>{z}</option>)}
        </select>

        <span className="text-xs font-mono text-ctp-surface2 ml-auto">
          {filtered.length} boss{filtered.length !== 1 ? 'es' : ''}
        </span>
      </div>

      {/* Boss progression table */}
      <Card>
        <CardHeader>
          <CardTitle>Boss Progression</CardTitle>
        </CardHeader>
        {loading ? (
          <CardBody><LoadingState rows={8} /></CardBody>
        ) : error ? (
          <CardBody><ErrorState message={error} /></CardBody>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>Boss</Th>
                <Th>Zone</Th>
                <Th>Diff</Th>
                <Th right>Pulls</Th>
                <Th right>Kills</Th>
                <Th right>Wipes</Th>
                <Th right>Best Kill</Th>
                <Th>First Kill</Th>
                <Th>Progress</Th>
              </tr>
            </THead>
            <TBody>
              {filtered.map(b => {
                const killed = b.is_killed === 'True' || b.is_killed === true as unknown as string
                const key = `${b.encounter_id}-${b.difficulty}`
                const wipeInfo = wipeMap[key]
                const killRate = Number(b.total_pulls) > 0
                  ? (Number(b.total_kills) / Number(b.total_pulls)) * 100
                  : 0

                return (
                  <Tr key={key}>
                    <Td>
                      <div className="flex items-center gap-2">
                        <span
                          className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                          style={{ backgroundColor: killed ? killColor : '#6B7280' }}
                        />
                        <span className="font-medium text-ctp-text">{b.boss_name}</span>
                      </div>
                    </Td>
                    <Td className="text-ctp-overlay1 text-xs max-w-[140px] truncate">{b.zone_name}</Td>
                    <Td><DiffBadge label={b.difficulty_label} /></Td>
                    <Td right mono>{formatNumber(b.total_pulls)}</Td>
                    <Td right mono style={{ color: killed ? killColor : undefined }}>
                      {formatNumber(b.total_kills)}
                    </Td>
                    <Td right mono style={{ color: wipeColor }}>{formatNumber(b.total_wipes)}</Td>
                    <Td right mono className="text-ctp-subtext1">
                      {killed ? formatDuration(Number(b.best_kill_seconds)) : (
                        wipeInfo
                          ? <span className="text-ctp-overlay0">{wipeInfo.avg_wipe_pct_rounded?.toFixed(0)}% avg</span>
                          : '—'
                      )}
                    </Td>
                    <Td className="text-xs text-ctp-overlay0">
                      {killed ? formatDate(b.first_kill_date) : (
                        <span className="text-ctp-surface2 italic">In progress</span>
                      )}
                    </Td>
                    <Td className="w-28">
                      <ProgressBar
                        value={killRate}
                        color={getDifficultyColor(b.difficulty_label)}
                        height="xs"
                      />
                    </Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
        )}
      </Card>

      {/* Best kill times */}
      <Card>
        <CardHeader>
          <CardTitle>Best Kill Times</CardTitle>
          <p className="text-xs text-ctp-overlay0 mt-0.5">Fastest recorded kills per boss</p>
        </CardHeader>
        {best.loading ? (
          <CardBody><LoadingState rows={5} /></CardBody>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>Boss</Th>
                <Th>Zone</Th>
                <Th>Diff</Th>
                <Th right>Best Time</Th>
                <Th right>Avg Kill</Th>
                <Th right>Total Kills</Th>
                <Th>First Kill</Th>
                <Th>Latest Kill</Th>
              </tr>
            </THead>
            <TBody>
              {[...best.data]
                .sort((a, b) => Number(a.best_kill_seconds) - Number(b.best_kill_seconds))
                .slice(0, 20)
                .map(b => (
                  <Tr key={`${b.encounter_id}-${b.difficulty}`}>
                    <Td className="font-medium text-ctp-text">{b.boss_name}</Td>
                    <Td className="text-xs text-ctp-overlay0">{b.zone_name}</Td>
                    <Td><DiffBadge label={b.difficulty_label} /></Td>
                    <Td right mono className="text-ctp-yellow font-semibold">{b.best_kill_mm_ss || formatDuration(Number(b.best_kill_seconds))}</Td>
                    <Td right mono className="text-ctp-overlay1">{formatDuration(Number(b.avg_kill_seconds))}</Td>
                    <Td right mono>{b.total_kills}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(b.first_kill_date)}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(b.latest_kill_date)}</Td>
                  </Tr>
                ))}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
