import { formatNumber } from '../../../utils/format'
import type { ProgressSnapshotDatum } from '../types'
import { clampPct } from '../utils'

export function ProgressSnapshotCandles({
  data,
  improvedColor,
  worseColor,
  neutralColor,
}: {
  data: ProgressSnapshotDatum[]
  improvedColor: string
  worseColor: string
  neutralColor: string
}) {
  const width = 720
  const chartHeight = 220
  const chartTop = 12
  const chartBottom = 46
  const plotHeight = chartHeight - chartTop - chartBottom
  const leftPad = 36
  const rightPad = 18
  const slotWidth = (width - leftPad - rightPad) / Math.max(data.length, 1)
  const candleWidth = Math.max(3, Math.min(18, slotWidth * 0.46))
  const wickWidth = data.length > 24 ? 1 : 1.5
  const labelEvery = data.length <= 10 ? 1 : Math.ceil(data.length / 10)
  const yForPct = (value: number) => chartTop + ((100 - clampPct(value)) / 100) * plotHeight
  const ticks = [100, 75, 50, 25, 0]

  return (
    <div className="pb-1">
      <svg
        viewBox={`0 0 ${width} ${chartHeight}`}
        className="h-[250px] w-full"
        role="img"
        aria-label="Progress snapshot candle chart"
        preserveAspectRatio="none"
      >
        {ticks.map(tick => {
          const y = yForPct(tick)
          return (
            <g key={tick}>
              <line
                x1={leftPad}
                x2={width - rightPad}
                y1={y}
                y2={y}
                stroke="#45475a"
                strokeDasharray={tick === 0 ? undefined : '4 6'}
                strokeOpacity={tick === 0 ? 0.55 : 0.42}
              />
              <text
                x={leftPad - 10}
                y={y + 3}
                textAnchor="end"
                className="fill-ctp-overlay0 font-mono text-[10px]"
              >
                {tick}%
              </text>
            </g>
          )
        })}

        {data.map((row, index) => {
          const x = leftPad + index * slotWidth + slotWidth / 2
          const open = clampPct(Number(row.open))
          const close = clampPct(Number(row.close))
          const high = clampPct(Number(row.high))
          const low = clampPct(Number(row.low))
          const highY = yForPct(high)
          const lowY = yForPct(low)
          const openY = yForPct(open)
          const closeY = yForPct(close)
          const bodyTop = Math.min(openY, closeY)
          const bodyHeight = Math.max(7, Math.abs(openY - closeY))
          const improved = close < open
          const regressed = close > open
          const bodyColor = improved ? improvedColor : regressed ? worseColor : neutralColor
          const showLabel = index % labelEvery === 0 || index === data.length - 1
          const tooltip = [
            `${row.boss_name}${row.difficulty_label ? ` (${row.difficulty_label})` : ''}`,
            row.currentNight,
            row.previousNight ? `Open (${row.previousNight} final pull): ${open.toFixed(1)}%` : `Open: ${open.toFixed(1)}%`,
            `Close (${row.currentNight} final pull): ${close.toFixed(1)}%`,
            `High: ${high.toFixed(1)}%`,
            `Low: ${low.toFixed(1)}%`,
            `Pulls: ${formatNumber(row.pullCount)}`,
          ]
            .filter(Boolean)
            .join('\n')

          return (
            <g key={`${row.boss_name}-${row.difficulty_label ?? 'unknown'}-${row.currentNight}-${index}`}>
              <title>{tooltip}</title>
              <line
                x1={x}
                x2={x}
                y1={highY}
                y2={lowY}
                stroke={bodyColor}
                strokeWidth={wickWidth}
                strokeLinecap="round"
                opacity={0.95}
              />
              <rect
                x={x - candleWidth / 2}
                y={bodyTop}
                width={candleWidth}
                height={bodyHeight}
                rx={1.5}
                fill={bodyColor}
                fillOpacity={improved ? 0.04 : regressed ? 0.78 : 0.18}
                stroke={bodyColor}
                strokeWidth={1.5}
              />
              {showLabel ? (
                <>
                  <text
                    x={x}
                    y={chartHeight - 27}
                    textAnchor="middle"
                    className="fill-ctp-subtext1 font-mono text-[10px]"
                  >
                    {row.label}
                  </text>
                  {data.length <= 14 ? (
                    <text
                      x={x}
                      y={chartHeight - 12}
                      textAnchor="middle"
                      className="fill-ctp-overlay0 font-mono text-[9px]"
                    >
                      {row.subLabel}
                    </text>
                  ) : null}
                </>
              ) : null}
            </g>
          )
        })}
      </svg>
    </div>
  )
}
