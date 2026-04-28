import { useMemo } from 'react'
import { toFiniteNumber } from '../../../utils/format'
import type { PlayerMplusRunHistory } from '../../../types'
import type { MplusHeatmapMode } from '../lib/types'
import { HEATMAP_WEEKS, HEATMAP_CELL, HEATMAP_GAP } from '../lib/constants'
import { getMplusCellColor, getMplusQuantityCellColor, dateKey, dateFromKey, addDays } from '../lib/mplus'

export function MplusActivityHeatmap({
  data,
  mode,
}: {
  data: PlayerMplusRunHistory[]
  mode: MplusHeatmapMode
}) {
  const byDate = useMemo(() => {
    const map = new Map<string, { runs: number; highestLevel: number; timed: number }>()
    for (const run of data) {
      const date =
        typeof run.completed_date === 'string' && run.completed_date
          ? run.completed_date
          : typeof run.completed_at === 'string'
            ? run.completed_at.slice(0, 10)
            : null
      if (!date) continue
      const entry = map.get(date) ?? { runs: 0, highestLevel: 0, timed: 0 }
      entry.runs++
      const level = toFiniteNumber(run.mythic_level) ?? 0
      if (level > entry.highestLevel) entry.highestLevel = level
      if (String(run.timed) === 'true') entry.timed++
      map.set(date, entry)
    }
    return map
  }, [data])

  const { weeks, monthLabels } = useMemo(() => {
    const todayDate = new Date()
    const currentSunday = addDays(todayDate, -todayDate.getDay())
    const start = addDays(currentSunday, -(HEATMAP_WEEKS - 1) * 7)

    const weekList: string[][] = Array.from({ length: HEATMAP_WEEKS }, (_, weekIndex) =>
      Array.from({ length: 7 }, (_, dayIndex) => dateKey(addDays(start, weekIndex * 7 + dayIndex)))
    )

    const months: { label: string; weekIndex: number }[] = []
    const seenMonths = new Set<string>()
    weekList.forEach((week, weekIndex) => {
      const firstOfMonth = week
        .map(dateFromKey)
        .find(date => date.getDate() === 1)
      if (!firstOfMonth) return

      const monthKey = `${firstOfMonth.getFullYear()}-${firstOfMonth.getMonth()}`
      if (seenMonths.has(monthKey)) return
      seenMonths.add(monthKey)
      months.push({
        label: firstOfMonth.toLocaleString('en', { month: 'short' }),
        weekIndex,
      })
    })

    return { weeks: weekList, monthLabels: months }
  }, [data])

  if (data.length === 0) {
    return (
      <div className="flex h-36 items-center justify-center rounded-xl border border-dashed border-ctp-mauve/20">
        <p className="text-xs font-mono text-ctp-overlay0">No run history exported yet.</p>
      </div>
    )
  }

  const todayStr = dateKey(new Date())
  const DAY_LABEL_W = 24
  const weekWidth = HEATMAP_CELL + HEATMAP_GAP
  const gridWidth = DAY_LABEL_W + HEATMAP_GAP + (weeks.length * weekWidth)
  const legendValues = mode === 'level' ? [0, 7, 12, 17, 20] : [0, 1, 2, 3, 4]

  return (
    <div className="flex justify-center">
      <div className="max-w-full overflow-x-auto pb-1">
        <div className="mx-auto shrink-0" style={{ width: gridWidth, minWidth: gridWidth }}>
          <div className="relative mb-1 h-4" style={{ paddingLeft: DAY_LABEL_W + HEATMAP_GAP }}>
            {monthLabels.map(({ label, weekIndex }) => (
              <span
                key={label + weekIndex}
                className="absolute text-[10px] font-mono text-ctp-overlay0"
                style={{ left: DAY_LABEL_W + HEATMAP_GAP + weekIndex * weekWidth }}
              >
                {label}
              </span>
            ))}
          </div>
          <div className="flex items-start" style={{ gap: HEATMAP_GAP, width: gridWidth }}>
            <div className="flex flex-col text-right shrink-0" style={{ width: DAY_LABEL_W, gap: HEATMAP_GAP }}>
              {['', 'Mon', '', 'Wed', '', 'Fri', ''].map((label, i) => (
                <div
                  key={i}
                  className="text-[10px] font-mono text-ctp-overlay0"
                  style={{ height: HEATMAP_CELL, lineHeight: `${HEATMAP_CELL}px` }}
                >
                  {label}
                </div>
              ))}
            </div>
            <div className="flex shrink-0" style={{ gap: HEATMAP_GAP }}>
              {weeks.map((week, wi) => (
                <div key={wi} className="flex flex-col shrink-0" style={{ gap: HEATMAP_GAP }}>
                  {week.map((dateStr, di) => {
                    const entry = byDate.get(dateStr)
                    const level = entry?.highestLevel ?? 0
                    const count = entry?.runs ?? 0
                    const isFuture = dateStr > todayStr
                    const tooltip = entry
                      ? `${dateStr}: ${entry.runs} run${entry.runs !== 1 ? 's' : ''}, best +${level}${entry.timed > 0 ? `, ${entry.timed} timed` : ''}`
                      : dateStr
                    const backgroundColor = mode === 'level'
                      ? getMplusCellColor(level)
                      : getMplusQuantityCellColor(count)
                    return (
                      <div
                        key={di}
                        title={tooltip}
                        style={{
                          width: HEATMAP_CELL,
                          height: HEATMAP_CELL,
                          borderRadius: 3,
                          backgroundColor: isFuture ? 'transparent' : backgroundColor,
                        }}
                      />
                    )
                  })}
                </div>
              ))}
            </div>
          </div>
          <div className="mt-2 flex items-center justify-end gap-1.5">
            <span className="text-[10px] font-mono text-ctp-overlay0">
              {mode === 'level' ? 'Low' : '0'}
            </span>
            {legendValues.map(value => (
              <div
                key={value}
                title={mode === 'level' ? (value === 0 ? 'No runs' : `+${value}`) : `${value}${value === 4 ? '+' : ''} runs`}
                style={{
                  width: HEATMAP_CELL,
                  height: HEATMAP_CELL,
                  borderRadius: 3,
                  backgroundColor: mode === 'level' ? getMplusCellColor(value) : getMplusQuantityCellColor(value),
                }}
              />
            ))}
            <span className="text-[10px] font-mono text-ctp-overlay0">
              {mode === 'level' ? '+20' : '4+'}
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}
