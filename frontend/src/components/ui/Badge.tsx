import clsx from 'clsx'

type BadgeVariant = 'default' | 'success' | 'warning' | 'danger' | 'info'

interface BadgeProps {
  children: React.ReactNode
  variant?: BadgeVariant
}

const variantClasses: Record<BadgeVariant, string> = {
  default: 'bg-surface-3 text-slate-400',
  success: 'bg-accent-teal/10 text-accent-teal',
  warning: 'bg-accent-gold/10 text-accent-gold',
  danger:  'bg-accent-coral/10 text-accent-coral',
  info:    'bg-brand-400/10 text-brand-400',
}

export function Badge({ children, variant = 'default' }: BadgeProps) {
  return (
    <span className={clsx('inline-flex items-center px-2 py-0.5 rounded text-xs font-mono', variantClasses[variant])}>
      {children}
    </span>
  )
}
