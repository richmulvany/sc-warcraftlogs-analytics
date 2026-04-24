import { getClassColor } from '../../constants/wow'

interface ClassLabelProps {
  className: string
  spec?: string
  size?: 'xs' | 'sm' | 'md'
}

export function ClassLabel({ className: cls, spec, size = 'sm' }: ClassLabelProps) {
  const color = getClassColor(cls)
  const sizeClass = size === 'xs' ? 'text-[10px]' : size === 'sm' ? 'text-xs' : 'text-sm'
  return (
    <span className={`${sizeClass} font-medium`} style={{ color }}>
      {spec ? `${spec} ${cls}` : cls}
    </span>
  )
}

export function ClassDot({ className: cls, size = 'sm' }: { className: string; size?: 'xs' | 'sm' | 'md' }) {
  const s = size === 'xs' ? 'w-1.5 h-1.5' : size === 'sm' ? 'w-2 h-2' : 'w-2.5 h-2.5'
  return (
    <span
      className={`inline-block ${s} rounded-full flex-shrink-0`}
      style={{ backgroundColor: getClassColor(cls) }}
    />
  )
}
