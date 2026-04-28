import { useColourBlind } from '../../../context/ColourBlindContext'
import { timerUsedPct, formatRunTime } from '../lib/mplus'

export function DungeonTimerBar({
  clearTimeMs,
  parTimeMs,
  timed,
  theme = 'timed',
}: {
  clearTimeMs: unknown
  parTimeMs: unknown
  timed: boolean
  theme?: 'best' | 'timed' | 'overtime'
}) {
  const { topTierColor, killColor, wipeColor } = useColourBlind()
  const pct = timerUsedPct(clearTimeMs, parTimeMs)
  const visualMax = pct == null ? 100 : Math.min(Math.max(pct, 100), 140)
  const timerMarkerPct = (100 / visualMax) * 100
  const clearPct = pct == null ? 0 : Math.min((pct / visualMax) * 100, 100)
  const inTimePct = Math.min(clearPct, timerMarkerPct)
  const overtimePct = Math.max(0, clearPct - timerMarkerPct)
  const inTimeColor = timed
    ? theme === 'best' ? topTierColor : killColor
    : '#45475a'
  const textColor = timed
    ? theme === 'best' ? topTierColor : killColor
    : wipeColor

  return (
    <div className="mt-3">
      <div className="relative h-2.5 rounded-full bg-ctp-crust/80">
        <div
          className="absolute left-0 top-0 h-full rounded-l-full transition-all"
          style={{ width: `${inTimePct}%`, backgroundColor: inTimeColor }}
        />
        {overtimePct > 0 && (
          <div
            className="absolute top-0 h-full rounded-r-full"
            style={{ left: `${timerMarkerPct}%`, width: `${overtimePct}%`, backgroundColor: wipeColor, opacity: 0.75 }}
          />
        )}
        <div
          className="absolute inset-y-[-3px] border-r border-dashed border-ctp-overlay1/80"
          style={{ left: `${timerMarkerPct}%` }}
        />
      </div>
      <div className="mt-2 flex min-w-0 items-center justify-between gap-2 text-[10px] font-mono">
        <span className="min-w-0 truncate" style={pct == null ? undefined : { color: textColor }}>
          {pct == null ? 'Timer unavailable' : `${Math.round(pct)}% of timer`}
        </span>
        <span className="shrink-0 whitespace-nowrap" style={pct == null ? undefined : { color: textColor }}>
          {formatRunTime(clearTimeMs)} / {formatRunTime(parTimeMs)}
        </span>
      </div>
    </div>
  )
}
