import { ReactNode } from 'react'
import clsx from 'clsx'

type Accent = 'mauve' | 'blue' | 'green' | 'peach' | 'red' | 'teal' | 'none'

const ACCENT_STYLES: Record<Accent, { border: string; text: string; iconBg: string }> = {
  mauve: { border: 'border-ctp-mauve/20',  text: 'text-ctp-mauve',  iconBg: 'bg-ctp-mauve/10' },
  blue:  { border: 'border-ctp-blue/20',   text: 'text-ctp-blue',   iconBg: 'bg-ctp-blue/10' },
  green: { border: 'border-ctp-green/20',  text: 'text-ctp-green',  iconBg: 'bg-ctp-green/10' },
  peach: { border: 'border-ctp-peach/20',  text: 'text-ctp-peach',  iconBg: 'bg-ctp-peach/10' },
  red:   { border: 'border-ctp-red/20',    text: 'text-ctp-red',    iconBg: 'bg-ctp-red/10' },
  teal:  { border: 'border-ctp-teal/20',   text: 'text-ctp-teal',   iconBg: 'bg-ctp-teal/10' },
  none:  { border: 'border-ctp-surface1',  text: 'text-ctp-text',   iconBg: 'bg-ctp-surface1' },
}

interface StatCardProps {
  label: string
  value: ReactNode
  subValue?: string
  trend?: 'up' | 'down' | 'neutral'
  icon?: ReactNode
  accent?: Accent
  /** Override the value colour with a colourblind-aware inline colour */
  valueColor?: string
  /** Override the subValue colour with a colourblind-aware inline colour */
  subValueColor?: string
  className?: string
}

export function StatCard({ label, value, subValue, trend, icon, accent = 'none', valueColor, subValueColor, className }: StatCardProps) {
  const style = ACCENT_STYLES[accent]

  return (
    <div
      className={clsx(
        'bg-ctp-surface0 rounded-2xl p-4 md:p-5 border shadow-card min-h-[112px] md:min-h-[132px] h-full flex flex-col',
        style.border,
        className
      )}
    >
      {/* Label row */}
      <div className="flex items-start justify-between gap-3 min-h-[28px]">
        <span className="section-label">{label}</span>
        {icon && (
          <div className={clsx('w-7 h-7 rounded-lg flex items-center justify-center text-sm flex-shrink-0', style.iconBg)}>
            <span className={style.text}>{icon}</span>
          </div>
        )}
      </div>

      {/* Value */}
      <div className="flex items-end gap-2 mt-0.5">

        <span
          className={clsx('text-2xl md:text-3xl font-semibold tracking-tight', !valueColor && style.text)}
          style={valueColor ? { color: valueColor } : undefined}
        >
          {value}
        </span>
        {trend && (
          <span
            className={clsx('mb-0.5 text-sm font-medium', {
              'text-ctp-green':   trend === 'up',
              'text-ctp-red':     trend === 'down',
              'text-ctp-overlay1':trend === 'neutral',
            })}
          >
            {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '—'}
          </span>
        )}
      </div>

      <div className="mt-auto pt-3.5 min-h-[28px]">
        {subValue && (
          <p
            className={clsx('text-xs 2xl:text-[13px]', !subValueColor && 'text-ctp-overlay1')}
            style={subValueColor ? { color: subValueColor } : undefined}
          >
            {subValue}
          </p>
        )}
      </div>
    </div>
  )
}
