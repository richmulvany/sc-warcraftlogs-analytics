import { useEffect, useMemo, useState } from 'react'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useBossProgression, useBestKills, useRaidSummary } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal']

export function Bosses() {
  const { getDifficultyColor, killColor, wipeColor, topTierColor } = useColourBlind()
  const prog = useBossProgression()
  const best = useBestKills()
  const raids = useRaidSummary()

  const [diff, setDiff] = useState('All')
  const [selectedTier, setSelectedTier] = useState('')
  const [selectedBoss, setSelectedBoss] = useState('All')
  const [search, setSearch] = useState('')

  function hasRealText(value: unknown): value is string {
    return typeof value === 'string' && value.trim() !== '' && value.trim().toLowerCase() !== 'null'
  }

  const validRaidRows = useMemo(() =>
    raids.data.filter(r => hasRealText(r.zone_name) && hasRealText(r.raid_night_date)),
    [raids.data]
  )

  const tierOptions = useMemo(() =>
    [...new Set(
      [...validRaidRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
    )],
    [validRaidRows]
  )

  const currentTier = tierOptions[0] ?? ''

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const tierBosses = useMemo(() =>
    prog.data.filter(b => b.zone_name === selectedTier),
    [prog.data, selectedTier]
  )

  const bossOptions = useMemo(() => {
    const values = [...new Set(tierBosses.map(b => b.boss_name).filter(hasRealText))].sort()
    return ['All', ...values]
  }, [tierBosses])

  useEffect(() => {
    if (!bossOptions.includes(selectedBoss)) setSelectedBoss('All')
  }, [bossOptions, selectedBoss])

  const filtered = useMemo(() =>
    tierBosses
      .filter(b => diff === 'All' || b.difficulty_label === diff)
      .filter(b => selectedBoss === 'All' || b.boss_name === selectedBoss)
      .filter(b => !search.trim() || b.boss_name.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => Number(b.total_kills) - Number(a.total_kills)),
    [tierBosses, diff, selectedBoss, search]
  )

  const stats = useMemo(() => {
    const killed = filtered.filter(b => b.is_killed === 'True' || b.is_killed === (true as unknown as string))
    const mythic = filtered.filter(b => b.difficulty_label === 'Mythic' && (b.is_killed === 'True' || b.is_killed === (true as unknown as string)))
    const heroic = filtered.filter(b => b.difficulty_label === 'Heroic' && (b.is_killed === 'True' || b.is_killed === (true as unknown as string)))
    const pulls = filtered.reduce((s, b) => s + Number(b.total_pulls), 0)
    return { killed: killed.length, mythic: mythic.length, heroic: heroic.length, pulls }
  }, [filtered])

  const wipeMap = useMemo(() => {
    const m: Record<string, typeof best.data[0]> = {}
    best.data.forEach(b => { m[`${b.encounter_id}-${b.difficulty}`] = b })
    return m
  }, [best.data])

  return (
    <AppLayout title="Boss Progression" subtitle="progression tracker">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {prog.loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard label="Bosses Killed" value={stats.killed} subValue={selectedTier || 'selected tier'} icon="⚔" accent="mauve" />
            <StatCard label="Mythic Kills" value={stats.mythic} subValue="within current filter" icon="◈" accent="peach" />
            <StatCard label="Heroic Kills" value={stats.heroic} subValue="within current filter" icon="◉" accent="blue" />
            <StatCard label="Total Pulls" value={formatNumber(stats.pulls)} subValue="filtered attempts" icon="◷" />
          </>
        )}
      </div>

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
          value={selectedTier}
          onChange={e => setSelectedTier(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-48"
        >
          {tierOptions.map(tier => <option key={tier} value={tier}>{tier}</option>)}
        </select>
        <select
          value={selectedBoss}
          onChange={e => setSelectedBoss(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors min-w-52"
        >
          {bossOptions.map(boss => <option key={boss} value={boss}>{boss}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search boss…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-44"
        />
        <span className="ml-auto text-xs font-mono text-ctp-overlay0">{filtered.length} boss rows</span>
      </div>

      {!prog.loading && !prog.error && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          {filtered.map(b => {
            const killed = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
            const killRate = Number(b.total_pulls) > 0 ? (Number(b.total_kills) / Number(b.total_pulls)) * 100 : 0
            const diffColor = getDifficultyColor(b.difficulty_label)
            const bk = wipeMap[`${b.encounter_id}-${b.difficulty}`]

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
                <div className="flex items-start justify-between gap-2 mb-2">
                  <div className="flex items-center gap-1.5">
                    <span className="w-1.5 h-1.5 rounded-full flex-shrink-0 mt-0.5" style={{ backgroundColor: killed ? killColor : '#6c7086' }} />
                    <p className="text-xs font-semibold text-ctp-text leading-tight">{b.boss_name}</p>
                  </div>
                  <DiffBadge label={b.difficulty_label} />
                </div>
                <p className="text-[10px] font-mono text-ctp-overlay0 mb-3 truncate">{b.zone_name}</p>
                <div className="flex items-center justify-between text-[11px] font-mono mb-2.5">
                  <span style={{ color: killColor }}>{b.total_kills}↓</span>
                  <span style={{ color: wipeColor }}>{b.total_wipes}✗</span>
                  {killed && bk ? (
                    <span style={{ color: topTierColor }}>{bk.best_kill_mm_ss || formatDuration(Number(b.best_kill_seconds))}</span>
                  ) : (
                    <span className="text-ctp-overlay0">—</span>
                  )}
                </div>
                <ProgressBar value={killRate} color={diffColor} height="xs" />
                {killed && b.first_kill_date && (
                  <p className="text-[10px] font-mono text-ctp-overlay0 mt-2">First: {formatDate(b.first_kill_date)}</p>
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

      <Card>
        <CardHeader>
          <CardTitle>Detailed Progression</CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">Full stats per boss encounter in the selected scope</p>
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
                const killed = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
                const killRate = Number(b.total_pulls) > 0 ? (Number(b.total_kills) / Number(b.total_pulls)) * 100 : 0
                const bk = wipeMap[`${b.encounter_id}-${b.difficulty}`]

                return (
                  <Tr key={`${b.encounter_id}-${b.difficulty}`}>
                    <Td>
                      <div className="flex items-center gap-2">
                        <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: killed ? killColor : '#6c7086' }} />
                        <span className="font-medium text-ctp-text">{b.boss_name}</span>
                      </div>
                    </Td>
                    <Td className="text-ctp-overlay1 text-xs max-w-[140px] truncate">{b.zone_name}</Td>
                    <Td><DiffBadge label={b.difficulty_label} /></Td>
                    <Td right mono>{formatNumber(b.total_pulls)}</Td>
                    <Td right mono style={{ color: killed ? killColor : undefined }}>{formatNumber(b.total_kills)}</Td>
                    <Td right mono style={{ color: wipeColor }}>{formatNumber(b.total_wipes)}</Td>
                    <Td right mono className="font-semibold" style={{ color: topTierColor }}>
                      {bk?.best_kill_mm_ss || (killed ? formatDuration(Number(b.best_kill_seconds)) : '—')}
                    </Td>
                    <Td right mono className="text-ctp-overlay1">
                      {killed ? formatDuration(Number(b.avg_pull_duration_seconds)) : '—'}
                    </Td>
                    <Td className="text-xs text-ctp-overlay1">
                      {killed ? formatDate(b.first_kill_date) : <span className="italic text-ctp-overlay0">In progress</span>}
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
