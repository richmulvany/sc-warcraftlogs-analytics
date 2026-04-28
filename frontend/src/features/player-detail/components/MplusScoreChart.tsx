import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { toFiniteNumber, getRelativeScoreDomain } from '../../../utils/format'
import type { PlayerMplusScoreHistory } from '../../../types'

export function MplusScoreChart({ data, lineColor }: { data: PlayerMplusScoreHistory[]; lineColor: string }) {
  const chartData = data.map(row => ({
    date: row.snapshot_date || row.snapshot_at,
    score: toFiniteNumber(row.score_all),
  }))
  const yDomain = getRelativeScoreDomain(chartData.map(row => row.score))

  if (chartData.length < 2) {
    return (
      <div className="flex h-56 items-center justify-center rounded-xl border border-dashed border-ctp-surface2 bg-ctp-crust/[0.35] px-6 text-center">
        <p className="text-xs font-mono text-ctp-overlay0">
          Score history starts from the first Raider.IO ingestion. Another snapshot is needed for a trend line.
        </p>
      </div>
    )
  }

  return (
    <div className="h-56">
      {yDomain && (
        <p className="mb-2 text-[10px] font-mono text-ctp-overlay0">
          Y-axis is scaled to this player&apos;s captured score range.
        </p>
      )}
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
          <CartesianGrid stroke="#313244" strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis domain={yDomain ?? undefined} tick={{ fill: '#6c7086', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ background: '#11111b', border: '1px solid #45475a', borderRadius: 12 }}
            labelStyle={{ color: '#cdd6f4' }}
          />
          <Line type="monotone" dataKey="score" stroke={lineColor} strokeWidth={2.5} dot={{ r: 3 }} activeDot={{ r: 5 }} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
