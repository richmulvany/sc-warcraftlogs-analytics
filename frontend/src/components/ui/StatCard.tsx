import { ReactNode } from 'react'
import clsx from 'clsx'

interface StatCardProps {
  label: string
  value: string | number
  subValue?: string
  trend?: 'up' | 'down' | 'neutral'
  icon?: ReactNode
  className?: string
}

export function StatCard({ label, value, subValue, trend, icon, className }: StatCardProps) {
  return (
    <div className={clsx('stat-card flex flex-col gap-2', className)}>
      <div className="flex items-start justify-between">
        <span className="text-xs font-mono uppercase tracking-widest text-slate-500">{label}</span>
        {icon && <span className="text-slate-600">{icon}</span>}
      </div>
      <div className="flex items-end gap-2">
        <span className="text-3xl font-light tracking-tight text-slate-100">{value}</span>
        {trend && (
          <span
            className={clsx('mb-1 text-sm font-medium', {
              'text-accent-teal': trend === 'up',
              'text-accent-coral': trend === 'down',
              'text-slate-500': trend === 'neutral',
            })}
          >
            {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '—'}
          </span>
        )}
      </div>
      {subValue && <span className="text-xs text-slate-600">{subValue}</span>}
    </div>
  )
}
