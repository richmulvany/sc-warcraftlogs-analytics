import { Th } from './Table'

interface SortableThProps<K extends string> {
  sortKey: K
  currentKey: K
  desc: boolean
  onSort: (key: K) => void
  right?: boolean
  children: React.ReactNode
  className?: string
  activeClass?: string
}

export function SortableTh<K extends string>({
  sortKey,
  currentKey,
  desc,
  onSort,
  right,
  children,
  className,
  activeClass = 'text-ctp-mauve',
}: SortableThProps<K>) {
  const isActive = sortKey === currentKey
  return (
    <Th
      right={right}
      className={className}
      aria-sort={isActive ? (desc ? 'descending' : 'ascending') : 'none'}
    >
      <button
        onClick={() => onSort(sortKey)}
        className="hover:text-ctp-subtext1 transition-colors flex items-center gap-1"
      >
        {children}
        {isActive
          ? <span className={activeClass}>{desc ? '↓' : '↑'}</span>
          : <span className="text-ctp-overlay0">↕</span>}
      </button>
    </Th>
  )
}
