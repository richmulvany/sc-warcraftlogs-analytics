import { formatDate, toFiniteNumber } from '../../../utils/format'
import type { PlayerMplusDungeonBreakdown, PlayerMplusRunHistory } from '../../../types'
import { RAIDERIO_LINK_TITLE } from '../lib/constants'
import { formatKeyLevel, isTimed } from '../lib/mplus'
import { formatNumber } from '../lib/utils'
import { DungeonTimerBar } from './DungeonTimerBar'

export function DungeonBreakdownCard({ row }: { row: PlayerMplusDungeonBreakdown }) {
  const href = row.best_run_url || undefined
  const timed = isTimed(row.best_timed)

  return (
    <a
      href={href}
      target={href ? '_blank' : undefined}
      rel={href ? 'noopener noreferrer' : undefined}
      title={href ? RAIDERIO_LINK_TITLE : undefined}
      className="group block min-w-0 overflow-hidden rounded-xl border border-ctp-surface1 bg-ctp-surface0/55 p-3 transition-all hover:border-ctp-mauve/50 hover:bg-ctp-surface0"
    >
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="truncate text-sm font-semibold text-ctp-mauve transition-colors group-hover:text-ctp-pink">{row.dungeon}</p>
          <p className="mt-0.5 truncate text-[10px] font-mono text-ctp-overlay0">
            {toFiniteNumber(row.total_runs) ?? 0} runs · {toFiniteNumber(row.timed_runs) ?? 0} timed
          </p>
        </div>
        <div className="shrink-0 text-right">
          <p className="text-lg font-semibold text-ctp-pink">
            {formatKeyLevel(row.best_key_level)}
          </p>
          <p className="whitespace-nowrap text-[10px] font-mono text-ctp-pink">{formatNumber(row.best_score, 1)} score</p>
        </div>
      </div>
      <DungeonTimerBar clearTimeMs={row.best_clear_time_ms} parTimeMs={row.best_par_time_ms} timed={timed} theme="best" />
      <p className="mt-2 truncate text-[10px] font-mono text-ctp-overlay0">
        Latest: {row.latest_completed_at ? formatDate(row.latest_completed_at) : '—'}
      </p>
    </a>
  )
}

export function RecentDungeonRunCard({ row, isNewBest }: { row: PlayerMplusRunHistory; isNewBest: boolean }) {
  const href = row.url || undefined
  const timed = isTimed(row.timed)
  const theme = isNewBest ? 'best' : timed ? 'timed' : 'overtime'
  const titleClass = isNewBest
    ? 'text-ctp-mauve group-hover:text-ctp-pink'
    : timed
      ? 'text-ctp-green group-hover:text-ctp-teal'
      : 'text-ctp-overlay1 group-hover:text-ctp-red'
  const metaClass = isNewBest ? 'text-ctp-mauve/75' : timed ? 'text-ctp-green/75' : 'text-ctp-overlay0'
  const keyClass = isNewBest ? 'text-ctp-pink' : timed ? 'text-ctp-green' : 'text-ctp-red'
  const hoverClass = isNewBest ? 'hover:border-ctp-mauve/50' : timed ? 'hover:border-ctp-green/40' : 'hover:border-ctp-red/40'

  return (
    <a
      href={href}
      target={href ? '_blank' : undefined}
      rel={href ? 'noopener noreferrer' : undefined}
      title={href ? RAIDERIO_LINK_TITLE : undefined}
      className={`group block min-w-0 overflow-hidden rounded-xl border border-ctp-surface1 bg-ctp-surface0/55 p-3 transition-all hover:bg-ctp-surface0 ${hoverClass}`}
    >
      <div className="flex min-w-0 items-start justify-between gap-3">
        <div className="min-w-0">
          <p className={`truncate text-sm font-semibold transition-colors ${titleClass}`}>{row.dungeon}</p>
          <p className={`mt-0.5 truncate text-[10px] font-mono ${metaClass}`}>
            {row.completed_at ? formatDate(row.completed_at) : '—'} · {isNewBest ? 'new best' : timed ? 'timed' : 'over timer'}
          </p>
        </div>
        <div className="shrink-0 text-right">
          <p className={`text-lg font-semibold ${keyClass}`}>
            {formatKeyLevel(row.mythic_level)}
          </p>
          <p className={`whitespace-nowrap text-[10px] font-mono ${isNewBest ? 'text-ctp-pink' : timed ? 'text-ctp-green/80' : 'text-ctp-overlay0'}`}>
            {formatNumber(row.score, 1)} score
          </p>
        </div>
      </div>
      <DungeonTimerBar clearTimeMs={row.clear_time_ms} parTimeMs={row.par_time_ms} timed={timed} theme={theme} />
    </a>
  )
}
