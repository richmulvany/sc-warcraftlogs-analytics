import clsx from 'clsx'
import { useColourBlind } from '../../context/ColourBlindContext'
import { normaliseRole } from '../../constants/wow'

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

// Inline-styled badge driven by a hex colour from context.
// Generates background at 10% opacity and border at 20% opacity automatically.
function ColourBadge({ label, color, size = 'sm' }: { label: string; color: string; size?: 'xs' | 'sm' | 'md' }) {
  return (
    <span
      className={clsx(
        'inline-flex items-center font-mono font-medium border rounded-lg',
        size === 'xs' ? 'text-[9px] px-1 py-0.5' :
        size === 'sm' ? 'text-[10px] px-1.5 py-0.5' :
                        'text-xs px-2 py-1',
      )}
      style={{
        color,
        backgroundColor: `${color}1a`,  // ~10% opacity
        borderColor:     `${color}33`,  // ~20% opacity
      }}
    >
      {label}
    </span>
  )
}

export function DiffBadge({ label }: { label: string }) {
  const { getDifficultyColor } = useColourBlind()
  // Ghost for unknown difficulties
  if (!label || label === 'LFR' || label === 'Unknown') {
    return <Badge variant="ghost" size="sm">{label || '—'}</Badge>
  }
  return <ColourBadge label={label} color={getDifficultyColor(label)} />
}

export function RoleBadge({ role }: { role: string }) {
  const { getRoleColor } = useColourBlind()
  const value = normaliseRole(role)
  const label = value === 'dps' ? 'DPS' : value === 'healer' ? 'Healer' : value === 'tank' ? 'Tank' : role
  return <ColourBadge label={label} color={getRoleColor(value)} />
}
