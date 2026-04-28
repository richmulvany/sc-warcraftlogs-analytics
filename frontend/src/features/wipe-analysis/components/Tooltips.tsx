import { formatNumber, formatPct } from '../../../utils/format'
import type { ChartTooltipProps } from '../../../components/charts/types'
import type { WipeWallRow, RecurringKillerRow } from '../types'
import { StatusPill } from './primitives'

export function CtpTooltip({ active, payload, label }: ChartTooltipProps) {
  if (!active || !payload?.length) return null
  return (
    <div className="min-w-[170px] rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      {label ? <p className="mb-2 text-ctp-overlay1">{label}</p> : null}
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color }}>
          {p.name}:{' '}
          <span className="font-semibold">
            {typeof p.value === 'number' ? formatNumber(p.value) : p.value}
          </span>
        </p>
      ))}
    </div>
  )
}

export function WipeWallTooltip({ active, payload }: ChartTooltipProps<WipeWallRow>) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  if (!row) return null
  const bestPct = Number(row.bestPct)
  const avgPct = Number(row.avgPct)
  return (
    <div className="min-w-[230px] space-y-1 rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="mb-1 font-semibold text-ctp-text">{row.fullName}</p>
      <div className="flex items-center gap-2">
        <p className="text-ctp-overlay1">{row.diff}</p>
        <StatusPill label={row.isCleared ? 'Cleared' : 'Active'} active={!row.isCleared} />
      </div>
      <p style={{ color: payload[0].color }}>
        Wipes: <span className="font-semibold">{formatNumber(row.wipes)}</span>
      </p>
      <p className="text-ctp-subtext1">
        Best pull: <span className="font-semibold">{bestPct > 0 ? formatPct(bestPct) : '—'}</span>
      </p>
      <p className="text-ctp-subtext1">
        Avg wipe: <span className="font-semibold">{avgPct > 0 ? formatPct(avgPct) : '—'}</span>
      </p>
      <p className="text-ctp-overlay1">
        Nights attempted: <span className="font-semibold">{formatNumber(row.nights)}</span>
      </p>
    </div>
  )
}

export function RecurringKillerTooltip({ active, payload }: ChartTooltipProps<RecurringKillerRow>) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  if (!row) return null
  return (
    <div className="min-w-[220px] space-y-1 rounded-xl border border-ctp-surface2 bg-ctp-surface0 px-3 py-2.5 text-xs font-mono shadow-xl">
      <p className="mb-1 font-semibold text-ctp-text">{row.fullName}</p>
      <p style={{ color: payload[0].color }}>
        Unique players killed: <span className="font-semibold">{row.uniquePlayers}</span>
      </p>
      <p className="text-ctp-subtext1">
        Wipe deaths: <span className="font-semibold">{row.deaths}</span>
      </p>
      <p className="text-ctp-overlay1">
        Bosses affected: <span className="font-semibold">{row.uniqueBosses}</span>
      </p>
    </div>
  )
}
