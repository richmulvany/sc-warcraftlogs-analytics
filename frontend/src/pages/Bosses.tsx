import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { ProgressBar } from '../components/ui/ProgressBar'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { BossProgressHistoryChart } from '../components/charts/BossProgressHistoryChart'
import { useBossProgression, useBestKills, useRaidSummary, useBossWipeAnalysis, useBossProgressHistory } from '../hooks/useGoldData'
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
  const wipeAnalysis = useBossWipeAnalysis()
  const history = useBossProgressHistory()

  const [diff, setDiff] = useState('Mythic')
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
    ['All', ...new Set(
      [...validRaidRows]
        .sort((a, b) => String(b.raid_night_date).localeCompare(String(a.raid_night_date)))
        .map(r => r.zone_name)
    )],
    [validRaidRows]
  )

  const currentTier = tierOptions[1] ?? ''

  useEffect(() => {
    if (!selectedTier && currentTier) setSelectedTier(currentTier)
  }, [selectedTier, currentTier])

  const tierBosses = useMemo(() =>
    prog.data.filter(b => selectedTier === 'All' || b.zone_name === selectedTier),
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

  const bestHpMap = useMemo(() => {
    const m = new Map<string, number>()
    wipeAnalysis.data.forEach(row => {
      m.set(`${row.encounter_id}-${row.difficulty}`, Number(row.best_wipe_pct) || 100)
    })
    return m
  }, [wipeAnalysis.data])

  const focusBoss = useMemo(() => {
    const inProgress = [...filtered]
      .filter(b => !(b.is_killed === 'True' || b.is_killed === (true as unknown as string)))
      .sort((a, b) => String(b.last_attempt_date).localeCompare(String(a.last_attempt_date)))
    if (inProgress.length > 0) return inProgress[0]

    const latestFirstKill = [...filtered]
      .filter(b => b.is_killed === 'True' || b.is_killed === (true as unknown as string))
      .sort((a, b) => String(b.first_kill_date).localeCompare(String(a.first_kill_date)))
    return latestFirstKill[0] ?? null
  }, [filtered])

  const focusHistory = useMemo(() => {
    if (!focusBoss) return []
    return [...history.data]
      .filter(row => row.encounter_id === focusBoss.encounter_id && row.difficulty === focusBoss.difficulty)
      .sort((a, b) => {
        const byDate = String(a.raid_night_date).localeCompare(String(b.raid_night_date))
        if (byDate !== 0) return byDate
        return String(a.start_time_utc ?? '').localeCompare(String(b.start_time_utc ?? ''))
      })
  }, [focusBoss, history.data])

  function bossHref(encounterId: string, difficulty: string) {
    return `/bosses/${encounterId}/${difficulty}`
  }

  return (
    <AppLayout title="Boss Progression" subtitle="progression tracker">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {prog.loading || wipeAnalysis.loading ? (
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
                'px-3 py-2 rounded-lg text-xs font-medium transition-all duration-150',
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

      <Card>
        <CardHeader>
          <CardTitle>
            {focusBoss
              ? `${focusBoss.boss_name} Progress Curve`
              : 'Boss Progress Curve'}
          </CardTitle>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            {focusBoss
              ? (focusBoss.is_killed === 'True' || focusBoss.is_killed === (true as unknown as string))
                ? 'Most recently first-killed boss in the current scope, showing best boss HP remaining by raid night'
                : 'Most recently progressed unkilled boss in the current scope, showing best boss HP remaining by raid night'
              : 'No boss selected in the current scope'}
          </p>
        </CardHeader>
        <CardBody>
          {focusBoss ? (
            <>
              <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
                <div>
                  <Link
                    to={bossHref(focusBoss.encounter_id, focusBoss.difficulty)}
                    className="text-sm font-semibold text-ctp-text hover:text-ctp-mauve transition-colors"
                  >
                    {focusBoss.boss_name}
                  </Link>
                  <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5">
                    {focusBoss.zone_name} · {focusBoss.difficulty_label}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-[10px] font-mono text-ctp-overlay0">Current best</p>
                  <p className="text-sm font-semibold" style={{ color: getDifficultyColor(focusBoss.difficulty_label) }}>
                    {(focusBoss.is_killed === 'True' || focusBoss.is_killed === (true as unknown as string))
                      ? '0.0%'
                      : `${(bestHpMap.get(`${focusBoss.encounter_id}-${focusBoss.difficulty}`) ?? 100).toFixed(1)}%`}
                  </p>
                </div>
              </div>
              <BossProgressHistoryChart data={focusHistory} />
            </>
          ) : (
            <div className="h-64 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
              No boss progression data in the current scope
            </div>
          )}
        </CardBody>
      </Card>

      {!prog.loading && !prog.error && !wipeAnalysis.loading && !wipeAnalysis.error && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3 max-h-[36rem] overflow-y-auto pr-2">
          {filtered.map(b => {
            const killed = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
            const bestHpRemaining = killed ? 0 : (bestHpMap.get(`${b.encounter_id}-${b.difficulty}`) ?? 100)
            const diffColor = getDifficultyColor(b.difficulty_label)
            const bk = wipeMap[`${b.encounter_id}-${b.difficulty}`]

            return (
              <Link
                key={`${b.encounter_id}-${b.difficulty}`}
                to={bossHref(b.encounter_id, b.difficulty)}
                className={clsx(
                  'block rounded-2xl border p-4 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-card-hover',
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
                <ProgressBar value={bestHpRemaining} color={diffColor} height="xs" />
                <p className="text-[10px] font-mono text-ctp-overlay0 mt-2">
                  {killed ? 'Killed' : `Best HP: ${bestHpRemaining.toFixed(1)}%`}
                </p>
                {killed && b.first_kill_date && (
                  <p className="text-[10px] font-mono text-ctp-overlay0 mt-1">First: {formatDate(b.first_kill_date)}</p>
                )}
              </Link>
            )
          })}
        </div>
      )}

      {prog.loading && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          {Array(12).fill(null).map((_, i) => <SkeletonCard key={i} />)}
        </div>
      )}
      {(prog.error || wipeAnalysis.error) && <ErrorState message={prog.error || wipeAnalysis.error || 'Unknown error'} />}

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
          <div className="max-h-[34rem] overflow-auto">
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
                <Th>Best HP</Th>
              </tr>
            </THead>
            <TBody>
              {filtered.map(b => {
                const killed = b.is_killed === 'True' || b.is_killed === (true as unknown as string)
                const bestHpRemaining = killed ? 0 : (bestHpMap.get(`${b.encounter_id}-${b.difficulty}`) ?? 100)
                const bk = wipeMap[`${b.encounter_id}-${b.difficulty}`]

                return (
                  <Tr key={`${b.encounter_id}-${b.difficulty}`}>
                    <Td>
                      <Link to={bossHref(b.encounter_id, b.difficulty)} className="flex items-center gap-2 hover:text-ctp-mauve transition-colors">
                        <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: killed ? killColor : '#6c7086' }} />
                        <span className="font-medium text-ctp-text">{b.boss_name}</span>
                      </Link>
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
                      <div className="space-y-1">
                        <ProgressBar value={bestHpRemaining} color={getDifficultyColor(b.difficulty_label)} height="xs" />
                        <p className="text-[10px] font-mono text-ctp-overlay0 text-right">
                          {killed ? '0.0%' : `${bestHpRemaining.toFixed(1)}%`}
                        </p>
                      </div>
                    </Td>
                  </Tr>
                )
              })}
            </TBody>
            </Table>
          </div>
        )}
      </Card>
    </AppLayout>
  )
}
