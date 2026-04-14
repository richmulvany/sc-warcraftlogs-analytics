import { useState, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  Cell, PieChart, Pie, Legend,
} from 'recharts'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardHeader, CardTitle, CardBody } from '../components/ui/Card'
import { StatCard } from '../components/ui/StatCard'
import { DiffBadge } from '../components/ui/Badge'
import { Table, THead, TBody, Th, Td, Tr } from '../components/ui/Table'
import { LoadingState, SkeletonCard } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { useBossWipeAnalysis, useBossMechanics } from '../hooks/useGoldData'
import { formatNumber, formatDate } from '../utils/format'
import { formatDuration } from '../constants/wow'
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
          {p.name}: <span className="font-semibold">{p.value}</span>
        </p>
      ))}
    </div>
  )
}

export function BossWipes() {
  const { getDifficultyColor, wipeColor, phaseColors, getParseColor } = useColourBlind()
  const wipes   = useBossWipeAnalysis()
  const mechs   = useBossMechanics()
  const [diff,        setDiff]        = useState('All')
  const [zone,        setZone]        = useState('All')
  const [search,      setSearch]      = useState('')
  const [activeBoss,  setActiveBoss]  = useState<string | null>(null)

  const zones = useMemo(() => {
    const zs = [...new Set(wipes.data.map(b => b.zone_name))].sort()
    return ['All', ...zs]
  }, [wipes.data])

  const filtered = useMemo(() =>
    wipes.data
      .filter(b => diff === 'All' || b.difficulty_label === diff)
      .filter(b => zone === 'All' || b.zone_name === zone)
      .filter(b => !search.trim() || b.boss_name.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes)),
    [wipes.data, diff, zone, search]
  )

  const stats = useMemo(() => {
    const total    = wipes.data.reduce((s, b) => s + Number(b.total_wipes), 0)
    const avgBoss  = wipes.data.reduce((s, b) => s + Number(b.avg_wipe_pct), 0) / (wipes.data.length || 1)
    const hardest  = [...wipes.data].sort((a, b) => Number(b.total_wipes) - Number(a.total_wipes))[0]
    const closest  = [...wipes.data].filter(b => Number(b.best_wipe_pct) > 0)
                       .sort((a, b) => Number(a.best_wipe_pct) - Number(b.best_wipe_pct))[0]
    return { total, avgBoss, hardest, closest }
  }, [wipes.data])

  // Phase breakdown data for pie chart (aggregated) — uses colourblind-aware phase colours
  const phaseData = useMemo(() => {
    if (!mechs.data.length) return []
    const p1 = mechs.data.reduce((s, m) => s + Number(m.pct_wipes_phase_1), 0) / mechs.data.length
    const p2 = mechs.data.reduce((s, m) => s + Number(m.pct_wipes_phase_2), 0) / mechs.data.length
    const p3 = mechs.data.reduce((s, m) => s + Number(m.pct_wipes_phase_3_plus), 0) / mechs.data.length
    return [
      { name: 'Phase 1',  value: Math.round(p1), fill: phaseColors[0] },
      { name: 'Phase 2',  value: Math.round(p2), fill: phaseColors[1] },
      { name: 'Phase 3+', value: Math.round(p3), fill: phaseColors[2] },
    ]
  }, [mechs.data, phaseColors])

  // Top wipe-heavy bosses bar chart — clicking a bar filters the table to that boss
  const topWipes = useMemo(() =>
    [...filtered].slice(0, 12).map(b => ({
      name:      b.boss_name.length > 14 ? b.boss_name.slice(0, 13) + '…' : b.boss_name,
      fullName:  b.boss_name,
      wipes:     Number(b.total_wipes),
      diff:      b.difficulty_label,
    })),
    [filtered]
  )

  // Table rows: if a boss is active (selected from chart), show only that boss
  const tableRows = useMemo(() =>
    activeBoss ? filtered.filter(b => b.boss_name === activeBoss) : filtered,
    [filtered, activeBoss]
  )

  const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal']

  return (
    <AppLayout title="Boss Wipes" subtitle="wipe counts, phase distribution, progression proximity">
      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {wipes.loading ? (
          Array(4).fill(null).map((_, i) => <SkeletonCard key={i} />)
        ) : (
          <>
            <StatCard
              label="Total Wipes"
              value={formatNumber(stats.total)}
              subValue="all bosses"
              icon="✗"
              valueColor={wipeColor}
              accent="none"
            />
            <StatCard
              label="Avg Boss % on Wipe"
              value={`${stats.avgBoss.toFixed(1)}%`}
              subValue="how far we got"
              icon="◉"
              accent="peach"
            />
            <StatCard
              label="Most Wiped Boss"
              value={stats.hardest?.boss_name ?? '—'}
              subValue={stats.hardest ? `${stats.hardest.total_wipes} wipes` : ''}
              accent="mauve"
            />
            <StatCard
              label="Closest Wipe"
              value={stats.closest?.boss_name ?? '—'}
              subValue={stats.closest ? `${stats.closest.best_wipe_pct?.toFixed(1)}% boss HP` : ''}
              accent="peach"
            />
          </>
        )}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Wipes per boss bar — click a bar to filter table */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Wipes per Boss</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">
              Top 12 most wiped bosses · click a bar to filter the table
              {activeBoss && (
                <button
                  onClick={() => setActiveBoss(null)}
                  className="ml-2 text-ctp-mauve hover:text-ctp-mauve/70 transition-colors"
                >
                  (clear filter)
                </button>
              )}
            </p>
          </CardHeader>
          <CardBody>
            {wipes.loading ? <LoadingState rows={4} /> : (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={topWipes} margin={{ top: 4, right: 4, left: -20, bottom: 36 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                    angle={-35} textAnchor="end" interval={0}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: '#6c7086', fontFamily: 'IBM Plex Mono, monospace' }}
                    axisLine={false} tickLine={false}
                  />
                  <Tooltip content={<CtpTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                  <Bar
                    dataKey="wipes"
                    name="Wipes"
                    radius={[4, 4, 0, 0]}
                    cursor="pointer"
                    onClick={(data) => {
                      const full = data.fullName as string
                      setActiveBoss(prev => prev === full ? null : full)
                    }}
                  >
                    {topWipes.map((entry, i) => (
                      <Cell
                        key={i}
                        fill={getDifficultyColor(entry.diff)}
                        fillOpacity={activeBoss && activeBoss !== entry.fullName ? 0.3 : 0.85}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardBody>
        </Card>

        {/* Phase breakdown donut — uses phaseColors from context */}
        <Card>
          <CardHeader>
            <CardTitle>Wipes by Phase</CardTitle>
            <p className="text-xs text-ctp-overlay1 mt-0.5">Avg across all bosses</p>
          </CardHeader>
          <CardBody>
            {mechs.loading ? <LoadingState rows={3} /> : phaseData.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <PieChart>
                  <Pie
                    data={phaseData}
                    cx="50%"
                    cy="45%"
                    innerRadius={52}
                    outerRadius={76}
                    paddingAngle={3}
                    dataKey="value"
                    stroke="none"
                  >
                    {phaseData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} fillOpacity={0.85} />
                    ))}
                  </Pie>
                  <Legend
                    iconType="circle"
                    iconSize={7}
                    formatter={(value: string, entry: { color?: string }) => (
                      <span style={{ fontSize: 11, color: entry.color ?? '#a6adc8', fontFamily: 'IBM Plex Mono' }}>
                        {value}
                      </span>
                    )}
                  />
                  <Tooltip
                    formatter={(v: number) => [`${v.toFixed(1)}%`, 'Avg wipes']}
                    contentStyle={{
                      background: '#313244',
                      border: '1px solid #45475a',
                      borderRadius: 12,
                      fontSize: 11,
                      fontFamily: 'IBM Plex Mono',
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-xs text-ctp-overlay0 font-mono text-center py-8">No mechanics data</p>
            )}
          </CardBody>
        </Card>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 bg-ctp-surface0 rounded-xl p-1 border border-ctp-surface1">
          {DIFFS.map(d => (
            <button
              key={d}
              onClick={() => { setDiff(d); setActiveBoss(null) }}
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
          onChange={e => { setZone(e.target.value); setActiveBoss(null) }}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors"
        >
          {zones.map(z => <option key={z} value={z}>{z}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search boss…"
          value={search}
          onChange={e => { setSearch(e.target.value); setActiveBoss(null) }}
          className="bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs text-ctp-subtext1 placeholder-ctp-overlay0 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors w-44"
        />
        <span className="ml-auto text-xs font-mono text-ctp-overlay0">
          {activeBoss ? (
            <span>
              showing <span className="text-ctp-mauve">{activeBoss}</span>
            </span>
          ) : (
            `${filtered.length} bosses`
          )}
        </span>
      </div>

      {/* Detailed wipe table */}
      <Card>
        <CardHeader>
          <CardTitle>Wipe Breakdown</CardTitle>
        </CardHeader>
        {wipes.loading ? (
          <CardBody><LoadingState rows={8} /></CardBody>
        ) : wipes.error ? (
          <CardBody><ErrorState message={wipes.error} /></CardBody>
        ) : (
          <Table>
            <THead>
              <tr>
                <Th>Boss</Th>
                <Th>Zone</Th>
                <Th>Diff</Th>
                <Th right>Total Wipes</Th>
                <Th right>Best Boss %</Th>
                <Th right>Avg Boss %</Th>
                <Th right>Avg Duration</Th>
                <Th right>Longest</Th>
                <Th right>Raid Nights</Th>
                <Th>First Wipe</Th>
                <Th>Latest Wipe</Th>
              </tr>
            </THead>
            <TBody>
              {tableRows.map(b => {
                // Invert: lower boss HP % remaining = closer to kill
                // Use getParseColor on the "closeness" score (100 - boss %)
                const closeness = 100 - Number(b.best_wipe_pct)
                const closeColor = getParseColor(closeness)
                return (
                  <Tr key={`${b.encounter_id}-${b.difficulty}`}>
                    <Td className="font-medium text-ctp-text">{b.boss_name}</Td>
                    <Td className="text-ctp-overlay1 text-xs truncate max-w-[130px]">{b.zone_name}</Td>
                    <Td><DiffBadge label={b.difficulty_label} /></Td>
                    <Td right mono style={{ color: wipeColor }}>{formatNumber(b.total_wipes)}</Td>
                    <Td right mono>
                      <span style={{ color: closeColor }}>{b.best_wipe_pct?.toFixed(1)}%</span>
                    </Td>
                    <Td right mono className="text-ctp-overlay1">{b.avg_wipe_pct_rounded?.toFixed(1)}%</Td>
                    <Td right mono className="text-ctp-overlay1">{formatDuration(Number(b.avg_wipe_duration_seconds))}</Td>
                    <Td right mono className="text-ctp-overlay1">{formatDuration(Number(b.longest_wipe_seconds))}</Td>
                    <Td right mono className="text-ctp-overlay1">{b.raid_nights_attempted}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(b.first_wipe_date)}</Td>
                    <Td className="text-xs text-ctp-overlay0">{formatDate(b.latest_wipe_date)}</Td>
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
