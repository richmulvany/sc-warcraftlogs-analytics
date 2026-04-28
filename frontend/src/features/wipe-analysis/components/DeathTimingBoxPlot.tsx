import { formatNumber } from '../../../utils/format'
import type { DeathTimingSummary } from '../types'
import { buildTimingTicks, formatTimingLabel, formatAxisTick } from '../utils'

export function DeathTimingBoxPlot({
  summary,
  boxColor,
  lineColor,
  axisColor,
  labelColor,
}: {
  summary: DeathTimingSummary
  boxColor: string
  lineColor: string
  axisColor: string
  labelColor: string
}) {
  const domainMax = Math.max(summary.upperWhisker * 1.1, 60_000)
  const ticks = buildTimingTicks(domainMax)

  const pct = (value: number) => Math.max(0, Math.min(100, (value / domainMax) * 100))

  const lower = pct(summary.lowerWhisker)
  const q1Pos = pct(summary.q1)
  const medPos = pct(summary.median)
  const q3Pos = pct(summary.q3)
  const upper = pct(summary.upperWhisker)
  const boxW = Math.max(q3Pos - q1Pos, 1.5)
  const boxLeft = q1Pos
  const boxRight = q1Pos + boxW

  return (
    <div className="w-full">
      <svg
        viewBox="0 0 100 52"
        className="h-52 w-full overflow-visible"
        aria-label="Death timing box plot"
      >
        <line
          x1="0"
          y1="40"
          x2="100"
          y2="40"
          stroke={axisColor}
          strokeOpacity="0.2"
          strokeWidth="0.35"
        />

        <line
          x1={lower}
          y1="22"
          x2={boxLeft}
          y2="22"
          stroke={lineColor}
          strokeOpacity="0.48"
          strokeWidth="0.55"
        />
        <line
          x1={boxRight}
          y1="22"
          x2={upper}
          y2="22"
          stroke={lineColor}
          strokeOpacity="0.48"
          strokeWidth="0.55"
        />
        <line
          x1={lower}
          y1="18"
          x2={lower}
          y2="26"
          stroke={lineColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />
        <line
          x1={upper}
          y1="18"
          x2={upper}
          y2="26"
          stroke={lineColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />

        <rect
          x={q1Pos}
          y="16"
          width={boxW}
          height="12"
          rx="1.2"
          fill={boxColor}
          fillOpacity="0.1"
          stroke={boxColor}
          strokeOpacity="0.58"
          strokeWidth="0.55"
        />

        <line
          x1={medPos}
          y1="13"
          x2={medPos}
          y2="31"
          stroke={boxColor}
          strokeWidth="1.1"
          strokeLinecap="round"
        />

        {summary.outliers.slice(0, 40).map((value, index) => (
          <circle
            key={`${value}-${index}`}
            cx={pct(value)}
            cy="22"
            r="1"
            fill={boxColor}
            fillOpacity="0.7"
          />
        ))}

        {ticks.map((tick, index) => {
          const x = pct(tick)
          const anchor =
            index === 0 ? 'start' : index === ticks.length - 1 ? 'end' : 'middle'

          return (
            <g key={index}>
              <line
                x1={x}
                y1="40"
                x2={x}
                y2="43.5"
                stroke={axisColor}
                strokeOpacity="0.35"
                strokeWidth="0.35"
              />
              <text
                x={x}
                y="50"
                textAnchor={anchor}
                fontSize="4.5"
                fill={labelColor}
                style={{ fontFamily: 'IBM Plex Mono, monospace' }}
              >
                {formatAxisTick(tick)}
              </text>
            </g>
          )
        })}
      </svg>

      <div className="mt-2 grid grid-cols-2 sm:grid-cols-4 gap-2">
        {[
          { label: 'Q1', value: formatTimingLabel(summary.q1) },
          { label: 'Median', value: formatTimingLabel(summary.median) },
          { label: 'Q3', value: formatTimingLabel(summary.q3) },
          { label: 'Samples', value: formatNumber(summary.count) },
        ].map(({ label, value }) => (
          <div
            key={label}
            className="rounded-lg border border-ctp-surface1 bg-ctp-surface1/20 px-2.5 py-2.5 text-center"
          >
            <p className="font-mono text-[9px] uppercase tracking-widest text-ctp-overlay0">
              {label}
            </p>
            <p className="mt-1 font-mono text-sm font-semibold text-ctp-text">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
