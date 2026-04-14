import clsx from 'clsx'

interface ProgressBarProps {
  value: number
  max?: number
  color?: string
  height?: 'xs' | 'sm' | 'md'
  showLabel?: boolean
  className?: string
}

export function ProgressBar({ value, max = 100, color, height = 'sm', showLabel = false, className }: ProgressBarProps) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100))
  const heightClass = height === 'xs' ? 'h-1' : height === 'sm' ? 'h-1.5' : 'h-2'

  return (
    <div className={clsx('flex items-center gap-2', className)}>
      <div className={clsx('flex-1 bg-ctp-surface1 rounded-full overflow-hidden', heightClass)}>
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${pct}%`, backgroundColor: color ?? '#89b4fa' }}
        />
      </div>
      {showLabel && (
        <span className="text-xs font-mono text-ctp-overlay1 w-8 text-right">{Math.round(pct)}%</span>
      )}
    </div>
  )
}
