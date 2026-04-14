import type { PlayerBossPerformance } from '../../types'
import { formatThroughput } from '../../constants/wow'
import { useColourBlind } from '../../context/ColourBlindContext'
import { DiffBadge } from '../ui/Badge'
import clsx from 'clsx'

interface Props {
  data: PlayerBossPerformance[]
}

export function PerformanceHeatmap({ data }: Props) {
  const { getParseColor } = useColourBlind()

  if (!data.length) {
    return (
      <div className="h-32 flex items-center justify-center text-ctp-overlay0 text-sm font-mono">
        No boss performance data
      </div>
    )
  }

  const sorted = [...data].sort((a, b) => {
    // Sort by zone then boss name
    if (a.zone_name !== b.zone_name) return a.zone_name.localeCompare(b.zone_name)
    return a.boss_name.localeCompare(b.boss_name)
  })

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2.5">
      {sorted.map((b, i) => {
        const parse = b.avg_rank_percent
        const hasData = parse != null && parse > 0
        const color = hasData ? getParseColor(parse) : '#45475a'

        return (
          <div
            key={i}
            className={clsx(
              'relative rounded-xl border p-3 transition-all duration-150 hover:border-ctp-surface2',
              hasData ? 'border-ctp-surface1' : 'border-ctp-surface1 opacity-50'
            )}
            style={hasData ? {
              background: `linear-gradient(135deg, ${color}0d 0%, transparent 100%)`,
              borderColor: `${color}30`,
            } : undefined}
          >
            {/* Parse % badge top-right */}
            {hasData && (
              <div
                className="absolute top-2 right-2 text-[10px] font-mono font-bold"
                style={{ color }}
              >
                {parse.toFixed(0)}%
              </div>
            )}

            <p className="text-[11px] font-medium text-ctp-text leading-tight mb-1.5 pr-8 truncate">
              {b.boss_name}
            </p>

            <div className="flex items-center gap-1.5 mb-1.5">
              <DiffBadge label={b.difficulty_label} />
              <span className="text-[9px] font-mono text-ctp-overlay0 truncate">
                {b.zone_name.split(',')[0]}
              </span>
            </div>

            {hasData ? (
              <div>
                <p className="text-[11px] font-mono text-ctp-subtext0">
                  {formatThroughput(b.avg_throughput_per_second)}
                </p>
                <p className="text-[9px] text-ctp-overlay0 mt-0.5">
                  {b.kills_on_boss} kill{b.kills_on_boss !== 1 ? 's' : ''}
                </p>
              </div>
            ) : (
              <p className="text-[10px] font-mono text-ctp-overlay0">no data</p>
            )}
          </div>
        )
      })}
    </div>
  )
}
