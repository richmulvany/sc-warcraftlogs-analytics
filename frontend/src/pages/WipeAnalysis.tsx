/**
 * Wipe Analysis — focuses on WHY wipes happen.
 * Data sources: gold_boss_mechanics (pull duration distribution, phase progression,
 * improvement trend) and gold_player_survivability (death counts, killing blows).
 */
import { useState, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  Legend,
} from 'recharts'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { ClassDot } from '../components/ui/ClassLabel'
import { useBossMechanics, usePlayerSurvivability, useBossWipeAnalysis } from '../hooks/useGoldData'
import { formatNumber } from '../utils/format'
import { useColourBlind } from '../context/ColourBlindContext'
import clsx from 'clsx'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CtpTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-ctp-surface0 border border-ctp-surface2 rounded-xl px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="text-ctp-overlay1 mb-2">{label}</p>
      {payload.map((p: { name: string; value: number; color: string }, i: number) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}: <span className="font-semibold">{typeof p.value === 'number' ? p.value.toFixed(1) : p.value}</span>
        </p>
      ))}
    </div>
  )
}

export function WipeAnalysis() {
  const { wipeColor, phaseColors, chartColors } = useColourBlind()
  const mechs      = useBossMechanics()
  const survival   = usePlayerSurvivability()
  const wipeData   = useBossWipeAnalysis()

  const [bossFilter, setBossFilter] = useState('All')
  const [search,     setSearch]     = useState('')

  // Boss options for filter
  const bossOptions = useMemo(() => {
    const names = [...new Set(mechs.data.map(m => m.boss_name))].sort()
    return ['All', ...names]
  }, [mechs.data])

  // Filtered mechanics rows
  const filteredMechs = useMemo(() =>
    mechs.data
      .filter(m => bossFilter === 'All' || m.boss_name === bossFilter)
      .filter(m => !search.trim() || m.boss_name.toLowerCase().includes(search.toLowerCase())),
    [mechs.data, bossFilter, search]
  )

  // Summary stats
  const stats = useMemo(() => {
    const totalDeaths     = survival.data.reduce((s, p) => s + Number(p.total_deaths), 0)
    const avgDeathsPerKill = survival.data.reduce((s, p) => s + Number(p.deaths_per_kill), 0) / (survival.data.length || 1)
    const fastWipes       = mechs.data.reduce((s, m) => s + Number(m.wipes_lt_1min), 0)
    const totalWipes      = wipeData.data.reduce((s, b) => s + Number(b.total_wipes), 0)
    return { totalDeaths, avgDeathsPerKill, fastWipes, totalWipes }
  }, [survival.data, mechs.data, wipeData.data])

  // Killing blow aggregation — top causes of death across all players
  const killingBlows = useMemo(() => {
    const map = new Map<string, number>()
    for (const p of survival.data) {
      if (!p.most_common_killing_blow) continue
      const count = Number(p.most_common_killing_blow_count) || 1
      map.set(p.most_common_killing_blow, (map.get(p.most_common_killing_blow) ?? 0) + count)
    }
    return [...map.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([name, count]) => ({ name: name.length > 22 ? name.slice(0, 21) + '…' : name, count, fullName: name }))
  }, [survival.data])

  // Pull duration distribution — aggregated across filtered bosses
  const durationBuckets = useMemo(() => {
    const totals = { lt1: 0, one3: 0, three5: 0, gt5: 0 }
    for (const m of filteredMechs) {
      totals.lt1   += Number(m.wipes_lt_1min)
      totals.one3  += Number(m.wipes_1_3min)
      totals.three5 += Number(m.wipes_3_5min)
      totals.gt5   += Number(m.wipes_5plus_min)
    }
    return [
      { label: '<1 min',   wipes: totals.lt1,    fill: phaseColors[0] },
      { label: '1–3 min',  wipes: totals.one3,   fill: phaseColors[1] },
      { label: '3–5 min',  wipes: totals.three5, fill: phaseColors[2] },
      { label: '5+ min',   wipes: totals.gt5,    fill: '#cba6f7' },
    ]
  }, [filteredMechs, phaseColors])

  // Boss progress trend — avg boss % over time (last few bosses, sorted by latest attempt)
  const progressTrend = useMemo(() =>
    [...mechs.data]
      .filter(m => m.progress_trend != null)
      .sort((a, b) => a.boss_name.localeCompare(b.boss_name))
      .map(m => ({
        boss:      m.boss_name.length > 12 ? m.boss_name.slice(0, 11) + '…' : m.boss_name,
        avgPct:    Number(m.avg_boss_pct),
        lastWeek:  Number(m.last_week_avg_boss_pct),
        trend:     Number(m.progress_trend),
      })),
    [mechs.data]
  )

  const loading = mechs.loading || survival.loading || wipeData.loading
  const error   = mechs.error   || survival.error   || wipeData.error

  return (
    <AppLayout title="Wipe Analysis" subtitle="understanding why wipes happen">
      {/* KPI strip */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : error ? (
          <div className="col-span-4"><ErrorState message={error} /></div>
        ) : (
          <>
            <StatCard
              label="Total Wipes"
              value={formatNumber(stats.totalWipes)}
              subValue="all bosses"
              icon="✗"
              valueColor={wipeColor}
              accent="none"
            />
            <StatCard
              label="Total Deaths"
              value={formatNumber(stats.totalDeaths)}
              subValue="across all kills"
              icon="💀"
              accent="red"
            />
            <StatCard
              label="Avg Deaths / Kill"
              value={stats.avgDeathsPerKill.toFixed(1)}
              subValue="per boss kill"
              accent="peach"
            />
            <StatCard
              label="Early Wipes (<1 min)"
              value={formatNumber(stats.fastWipes)}
              subValue="immediate failures"
              valueColor={wipeColor}
              accent="none"
            />
          </>
        )}
      </div>

      {/* Boss filter */}
      <div className="flex flex-wrap items-center gap-3">
        <select
          value={bossFilter}
          onChange={e => setBossFilter(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors"
        >
          {bossOptions.map(b => <option key={b} value={b}>{b}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search boss…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-44"
        />
        {(bossFilter !== 'All' || search) && (
          <button
            onClick={() => { setBossFilter('All'); setSearch('') }}
            className="text-xs font-mono text-ctp-mauve hover:text-ctp-mauve/70 transition-colors"
          >
            clear
          </button>
        )}
      </div>

      {/* Top row: pull duration + killing blows */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Pull duration distribution */}
        <Card>
          <CardHeader>
            <CardTitle>Wipe Duration Breakdown</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              How long pulls last before wiping — early wipes suggest coordination or tank issues
            </p>
          </CardHeader>
          <CardBody>
            {mechs.loading ? <LoadingState rows={4} /> : (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={durationBuckets} margin={{ top: 4, right: 4, left: -20, bottom: 0 }}>
                  <XAxis
                    dataKey="label"
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                  />
                  <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                  <Bar dataKey="wipes" name="Wipes" radius={[4, 4, 0, 0]}>
                    {durationBuckets.map((b, i) => (
                      <Cell key={i} fill={b.fill} fillOpacity={0.85} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardBody>
        </Card>

        {/* Most common killing blows */}
        <Card>
          <CardHeader>
            <CardTitle>Most Common Killing Blows</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Mechanics killing players most frequently — the mechanics to learn
            </p>
          </CardHeader>
          <CardBody>
            {survival.loading ? <LoadingState rows={6} /> : killingBlows.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <BarChart
                  data={killingBlows}
                  layout="vertical"
                  margin={{ top: 4, right: 16, left: 0, bottom: 0 }}
                >
                  <XAxis
                    type="number"
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                  />
                  <YAxis
                    dataKey="name"
                    type="category"
                    width={130}
                    tick={{ fontSize: 10, fill: '#a6adc8', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                  />
                  <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                  <Bar dataKey="count" name="Deaths" radius={[0, 4, 4, 0]} fill={wipeColor} fillOpacity={0.8} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No survivability data</p>
            )}
          </CardBody>
        </Card>
      </div>

      {/* Progress trend — boss % improvement over time */}
      {progressTrend.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Boss HP % on Wipe — This Week vs Last</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Lower % = closer to kill. Downward trend is good.
            </p>
          </CardHeader>
          <CardBody>
            {mechs.loading ? <LoadingState rows={4} /> : (
              <>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={progressTrend} margin={{ top: 4, right: 4, left: -20, bottom: 36 }}>
                    <XAxis
                      dataKey="boss"
                      tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                      axisLine={false} tickLine={false}
                      angle={-35} textAnchor="end" interval={0}
                    />
                    <YAxis
                      tickFormatter={v => `${v}%`}
                      tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                      axisLine={false} tickLine={false}
                    />
                    <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                    <Legend
                      iconType="circle"
                      iconSize={7}
                      formatter={(value: string, entry: { color?: string }) => (
                        <span style={{ fontSize: 11, color: entry.color ?? '#a6adc8', fontFamily: 'IBM Plex Mono' }}>
                          {value}
                        </span>
                      )}
                    />
                    <Bar dataKey="lastWeek" name="Last Week Avg %" radius={[4, 4, 0, 0]} fill={chartColors.secondary} fillOpacity={0.45} />
                    <Bar dataKey="avgPct"   name="This Week Avg %" radius={[4, 4, 0, 0]} fill={wipeColor} fillOpacity={0.85} />
                  </BarChart>
                </ResponsiveContainer>
                <p className="text-[10px] font-mono text-ctp-overlay0 mt-2">
                  * Boss HP % remaining on wipe. Lower = closer to kill. Missing last-week bar means first week on boss.
                </p>
              </>
            )}
          </CardBody>
        </Card>
      )}

      {/* Per-player death analysis */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Deaths per player */}
        <Card>
          <CardHeader>
            <CardTitle>Deaths per Kill by Player</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">High deaths/kill = consistent mechanic failure</p>
          </CardHeader>
          {survival.loading ? (
            <CardBody><LoadingState rows={6} /></CardBody>
          ) : (
            <Table>
              <THead>
                <tr>
                  <Th>Player</Th>
                  <Th right>Total Deaths</Th>
                  <Th right>Deaths/Kill</Th>
                  <Th>Most Killed By</Th>
                </tr>
              </THead>
              <TBody>
                {[...survival.data]
                  .sort((a, b) => Number(b.deaths_per_kill) - Number(a.deaths_per_kill))
                  .slice(0, 15)
                  .map(p => (
                    <Tr key={p.player_name}>
                      <Td>
                        <div className="flex items-center gap-2">
                          <ClassDot className={p.player_class} />
                          <span className="text-ctp-text font-medium text-xs">{p.player_name}</span>
                        </div>
                      </Td>
                      <Td right mono style={{ color: wipeColor }}>{formatNumber(p.total_deaths)}</Td>
                      <Td right mono className={clsx(
                        Number(p.deaths_per_kill) >= 2
                          ? 'text-ctp-red'
                          : Number(p.deaths_per_kill) >= 1
                          ? 'text-ctp-peach'
                          : 'text-ctp-overlay1'
                      )}>
                        {Number(p.deaths_per_kill).toFixed(2)}
                      </Td>
                      <Td className="text-[10px] text-ctp-overlay0 font-mono truncate max-w-[140px]">
                        {p.most_common_killing_blow ?? '—'}
                      </Td>
                    </Tr>
                  ))}
              </TBody>
            </Table>
          )}
        </Card>

        {/* Boss mechanics detail */}
        <Card>
          <CardHeader>
            <CardTitle>Per-Boss Mechanics</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Phase distribution and pull duration data</p>
          </CardHeader>
          {mechs.loading ? (
            <CardBody><LoadingState rows={6} /></CardBody>
          ) : (
            <Table>
              <THead>
                <tr>
                  <Th>Boss</Th>
                  <Th right>Wipes</Th>
                  <Th right>Avg %</Th>
                  <Th right>P1%</Th>
                  <Th right>P2%</Th>
                  <Th right>P3+%</Th>
                </tr>
              </THead>
              <TBody>
                {filteredMechs
                  .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes))
                  .map(m => (
                    <Tr key={`${m.encounter_id}-${m.difficulty}`}>
                      <Td className="font-medium text-ctp-text text-xs">{m.boss_name}</Td>
                      <Td right mono style={{ color: wipeColor }}>{formatNumber(m.total_wipes)}</Td>
                      <Td right mono className="text-ctp-overlay1">{Number(m.avg_boss_pct).toFixed(1)}%</Td>
                      <Td right mono style={{ color: phaseColors[0] }}>{Number(m.pct_wipes_phase_1).toFixed(0)}%</Td>
                      <Td right mono style={{ color: phaseColors[1] }}>{Number(m.pct_wipes_phase_2).toFixed(0)}%</Td>
                      <Td right mono style={{ color: phaseColors[2] }}>{Number(m.pct_wipes_phase_3_plus).toFixed(0)}%</Td>
                    </Tr>
                  ))}
              </TBody>
            </Table>
          )}
        </Card>
      </div>
    </AppLayout>
  )
}
