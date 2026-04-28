import { useState } from 'react'
import { ProgressBar } from '../../../components/ui/ProgressBar'
import { useColourBlind } from '../../../context/ColourBlindContext'
import type { TierCompletionRow } from '../lib/types'
import { clampTooltipPosition } from '../lib/utils'

function CompletionTooltip({
  row,
  position,
}: {
  row: TierCompletionRow
  position: { left: number; top: number }
}) {
  const { killColor, wipeColor } = useColourBlind()
  const killed = row.bosses.filter(boss => boss.killed)
  const missing = row.bosses.filter(boss => !boss.killed)

  return (
    <div
      className="pointer-events-none fixed z-50 w-72 rounded-xl border border-ctp-surface2 bg-ctp-crust/95 p-4 text-left shadow-2xl backdrop-blur"
      style={{ left: position.left, top: position.top }}
    >
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-ctp-text">{row.difficulty}</p>
        <p className="text-xs font-mono text-ctp-subtext1">{row.completed}/{row.total || '—'}</p>
      </div>

      {row.total === 0 ? (
        <p className="mt-3 text-xs font-mono text-ctp-overlay0">
          No bosses are exported for this difficulty in the selected tier.
        </p>
      ) : (
        <div className="mt-3 grid grid-cols-1 gap-1.5">
          <div>
            {killed.length > 0 ? (
              <div className="space-y-1">
                {killed.map(boss => (
                  <p key={boss.name} className="truncate text-xs text-ctp-text">
                    <span className="mr-2" style={{ color: killColor }}>✓</span>{boss.name}
                  </p>
                ))}
              </div>
            ) : (
              <p className="text-xs font-mono text-ctp-overlay0">None yet</p>
            )}
          </div>
          <div>
            {missing.length > 0 ? (
              <div className="space-y-1">
                {missing.map(boss => (
                  <p key={boss.name} className="truncate text-xs text-ctp-subtext1">
                    <span className="mr-2" style={{ color: wipeColor }}>×</span>{boss.name}
                  </p>
                ))}
              </div>
            ) : (
              <p className="text-xs font-mono text-ctp-overlay0">Complete</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export function CompletionRow({ row, color }: { row: TierCompletionRow; color: string }) {
  const [tooltipPosition, setTooltipPosition] = useState<{ left: number; top: number } | null>(null)

  return (
    <div
      className="rounded-xl border border-ctp-surface1 bg-ctp-surface0/60 px-3 py-3 transition-colors hover:border-ctp-surface2"
      onMouseMove={(event) => setTooltipPosition(clampTooltipPosition(event.clientX, event.clientY))}
      onMouseLeave={() => setTooltipPosition(null)}
    >
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-sm font-semibold" style={{ color }}>
          {row.difficulty}
        </p>
        <p className="text-sm font-mono text-ctp-text">
          {row.completed}/{row.total || '—'}
        </p>
      </div>
      <ProgressBar value={row.pct} color={color} height="sm" />
      {tooltipPosition && <CompletionTooltip row={row} position={tooltipPosition} />}
    </div>
  )
}
