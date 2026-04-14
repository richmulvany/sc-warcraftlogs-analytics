import clsx from 'clsx'

export type BadgeVariant = 'default' | 'mauve' | 'blue' | 'green' | 'yellow' | 'peach' | 'red' | 'teal' | 'ghost'

const VARIANTS: Record<BadgeVariant, string> = {
  default: 'bg-ctp-surface1 text-ctp-subtext1 border-ctp-surface2',
  mauve:   'bg-ctp-mauve/10 text-ctp-mauve border-ctp-mauve/20',
  blue:    'bg-ctp-blue/10 text-ctp-blue border-ctp-blue/20',
  green:   'bg-ctp-green/10 text-ctp-green border-ctp-green/20',
  yellow:  'bg-ctp-yellow/10 text-ctp-yellow border-ctp-yellow/20',
  peach:   'bg-ctp-peach/10 text-ctp-peach border-ctp-peach/20',
  red:     'bg-ctp-red/10 text-ctp-red border-ctp-red/20',
  teal:    'bg-ctp-teal/10 text-ctp-teal border-ctp-teal/20',
  ghost:   'bg-transparent text-ctp-overlay1 border-transparent',
}

interface BadgeProps {
  children: React.ReactNode
  variant?: BadgeVariant
  size?: 'xs' | 'sm' | 'md'
  className?: string
  style?: React.CSSProperties
}

export function Badge({ children, variant = 'default', size = 'sm', className, style }: BadgeProps) {
  return (
    <span
      style={style}
      className={clsx(
        'inline-flex items-center font-mono font-medium border rounded-lg',
        size === 'xs' ? 'text-[9px] px-1 py-0.5' :
        size === 'sm' ? 'text-[10px] px-1.5 py-0.5' :
                        'text-xs px-2 py-1',
        VARIANTS[variant],
        className
      )}
    >
      {children}
    </span>
  )
}

export function DiffBadge({ label }: { label: string }) {
  const variant: BadgeVariant =
    label === 'Mythic' ? 'mauve' :
    label === 'Heroic' ? 'blue' :
    label === 'Normal' ? 'green' : 'ghost'
  return <Badge variant={variant} size="sm">{label}</Badge>
}

export function RoleBadge({ role }: { role: string }) {
  const label   = role === 'dps' ? 'DPS' : role === 'healer' ? 'Healer' : role === 'tank' ? 'Tank' : role
  const variant: BadgeVariant = role === 'healer' ? 'teal' : role === 'tank' ? 'blue' : 'red'
  return <Badge variant={variant} size="sm">{label}</Badge>
}
