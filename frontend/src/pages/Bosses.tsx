import { useState, useMemo } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useBossProgression, useBestKills } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal']

export function Bosses() {
  const { getDifficultyColor, killColor, wipeColor } = useColourBlind()
  const prog = useBossProgression()
  const best = useBestKills()

  const [diff,   setDiff]   = useState('All')
  const [zone,   setZone]   = useState('All')
  const [search, setSearch] = useState('')

  const zones = useMemo(() => {
    const zs = [...new Set(prog.data.map(b => b.zone_name))].sort()
    return ['All', ...zs]
  }, [prog.data])

  const filtered = useMemo(() =>
    prog.data
      .filter(b => diff === 'All' || b.difficulty_label === diff)
      .filter(b => zone === 'All' || b.zone_name === zone)
      .filter(b => !search.trim() || b.boss_name.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => {
        if (a.zone_name !== b.zone_name) return a.zone_name.localeCompare(b.zone_name)
        return Number(b.total_kills) - Number(a.total_kills)
      }),
    [prog.data, diff, zone, search]
  )

  const stats = useMemo(() => {
    const killed  = prog.data.filter(b => b.is_killed === 'True' || b.is_killed === (true as unknown as string))
    const mythic  = killed.filter(b => b.difficulty_label === 'Mythic')
    const heroic  = killed.filter(b => b.difficulty_label === 'Heroic')
    const pulls   = prog.data.reduce((s, b) => s + Number(b.total_pulls), 0)
    return { killed: killed.length, mythic: mythic.length, heroic: heroic.length, pulls }
  }, [prog.data])

  const wipeMap = useMemo(() => {
    const m: Record<string, typeof best.data[0]> = {}
    best.data.forEach(b => { m[`${b.encounter_id}-${b.difficulty}`] = b })
    return m
  }, [best.data])

  return (
    <AppLayout title="Bosses" subtitle="progression tracker">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {prog.loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard label="Bosses Killed"   value={stats.killed}           subValue="all difficulties"  icon="⚔" accent="mauve" />
            <StatCard label="Mythic Kills"     value={stats.mythic}           subValue="mythic difficulty" icon="◈" accent="peach" />
            <StatCard label="Heroic Kills"     value={stats.heroic}           subValue="heroic difficulty" icon="◉" accent="blue" />
            <StatCard label="Total Pulls"      value={formatNumber(stats.pulls)} subValue="all attempts"  icon="◷" />
          </>
        )}
      </div>

      {/* Filter row */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {DIFFS.map(d => (
            <button
              key={d}
              onClick={() => setDiff(d)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-150',
                diff === d
                  ? 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow'
                  : 'text-ctp-overlay1 hover:text-ctp-subtext1'
              )}
            >
              {d}
            </button>
          ))}
        </div>
        <select
          value={zone}
          onChange={e => setZone(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors"
        >
          {zones.map(z => <option key={z} value={z}>{z}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search boss…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-44"
        />
        <span className="ml-auto text-xs font-mono text-ctp-overlay0">{filtered.length} bosses</span>
      </div>

      {/* Progress grid — visual cards for each boss */}
      {!prog.loading && !prog.error && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          {filtered.map(b => {
            const killed    = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
            const killRate  = Number(b.total_pulls) > 0 ? (Number(b.total_kills) / Number(b.total_pulls)) * 100 : 0
            const diffColor = getDifficultyColor(b.difficulty_label)
            const bk        = wipeMap[`${b.encounter_id}-${b.difficulty}`]

            return (
              <div
                key={`${b.encounter_id}-${b.difficulty}`}
                className={clsx(
                  'rounded-2xl border p-4 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-card-hover',
                  killed
                    ? 'bg-ctp-surface0 border-ctp-surface1'
                    : 'bg-ctp-surface0/60 border-ctp-surface1/60'
                )}
              >
                {/* Boss name + killed indicator */}
                <div className="flex items-start justify-between gap-2 mb-2">
                  <div className="flex items-center gap-1.5">
                    <span
                      className="w-1.5 h-1.5 rounded-full flex-shrink-0 mt-0.5"
                      style={{ backgroundColor: killed ? killColor : '#6c7086' }}
                    />
                    <p className="text-xs font-semibold text-ctp-text leading-tight">{b.boss_name}</p>
                  </div>
                  <DiffBadge label={b.difficulty_label} />
                </div>

                {/* Zone */}
                <p className="text-[10px] font-mono text-ctp-overlay0 mb-3 truncate">
                  {b.zone_name}
                </p>

                {/* Stats row */}
                <div className="flex items-center justify-between text-[11px] font-mono mb-2.5">
                  <span style={{ color: killColor }}>{b.total_kills}↓</span>
                  <span style={{ color: wipeColor }}>{b.total_wipes}✗</span>
                  {killed && bk ? (
                    <span className="text-ctp-yellow">{bk.best_kill_mm_ss || formatDuration(Number(b.best_kill_seconds))}</span>
                  ) : (
                    <span className="text-ctp-overlay0">—</span>
                  )}
                </div>

                {/* Progress bar */}
                <ProgressBar
                  value={killRate}
                  color={diffColor}
                  height="xs"
                />

                {/* Kill date */}
                {killed && b.first_kill_date && (
                  <p className="text-[10px] font-mono text-ctp-overlay0 mt-2">
                    First: {formatDate(b.first_kill_date)}
                  </p>
                )}
              </div>
            )
          })}
        </div>
      )}

      {prog.loading && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          {Array(12).fill(null).map((_, i) => <SkeletonCard key={i} />)}
        </div>
      )}
      {prog.error && <ErrorState message={prog.error} />}

      {/* Detailed table */}
      <Card>
        <CardHeader>
          <CardTitle>Detailed Progression</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">Full stats per boss encounter</p>
        </CardHeader>
        {prog.loading ? (
          <CardBody><LoadingState rows={8} /></CardBody>
        ) : prog.error ? (
          <CardBody><ErrorState message={prog.error} /></CardBody>
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
                <Th right>Avg Kill</Th>
                <Th>First Kill</Th>
                <Th>Kill Rate</Th>
              </tr>
            </THead>
            <TBody>
              {filtered.map(b => {
                const killed   = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
                const killRate = Number(b.total_pulls) > 0 ? (Number(b.total_kills) / Number(b.total_pulls)) * 100 : 0
                const bk       = wipeMap[`${b.encounter_id}-${b.difficulty}`]

                return (
                  <Tr key={`${b.encounter_id}-${b.difficulty}`}>
                    <Td>
                      <div className="flex items-center gap-2">
                        <span
                          className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                          style={{ backgroundColor: killed ? killColor : '#6c7086' }}
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
                    <Td right mono className="text-ctp-yellow font-semibold">
                      {bk?.best_kill_mm_ss || (killed ? formatDuration(Number(b.best_kill_seconds)) : '—')}
                    </Td>
                    <Td right mono className="text-ctp-overlay1">
                      {killed ? formatDuration(Number(b.avg_pull_duration_seconds)) : '—'}
                    </Td>
                    <Td className="text-xs text-ctp-overlay1">
                      {killed ? formatDate(b.first_kill_date) : (
                        <span className="italic text-ctp-overlay0">In progress</span>
                      )}
                    </Td>
                    <Td className="w-28">
                      <ProgressBar value={killRate} color={getDifficultyColor(b.difficulty_label)} height="xs" />
                    </Td>
                  </Tr>
                )
              })}
            </TBody>
          </Table>
        )}
      </Card>
    </AppLayout>
  )
}
