import clsx from 'clsx'

export function Table({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={clsx('overflow-x-auto table-row-hover', className)}>
      <table className="w-full text-sm border-collapse">{children}</table>
    </div>
  )
}

export function THead({ children }: { children: React.ReactNode }) {
  return <thead className="border-b border-ctp-surface1">{children}</thead>
}

export function TBody({ children }: { children: React.ReactNode }) {
  return <tbody className="divide-y divide-ctp-surface0">{children}</tbody>
}

export function Th({ children, className, right }: { children: React.ReactNode; className?: string; right?: boolean }) {
  return (
    <th
      className={clsx(
        'px-4 py-3 text-[10px] font-mono font-semibold uppercase tracking-[0.1em] text-ctp-overlay0 whitespace-nowrap',
        right ? 'text-right' : 'text-left',
        className
      )}
    >
      {children}
    </th>
  )
}

export function Td({
  children,
  className,
  right,
  mono,
}: {
  children: React.ReactNode
  className?: string
  right?: boolean
  mono?: boolean
}) {
  return (
    <td
      className={clsx(
        'px-4 py-3 text-sm text-ctp-subtext1 whitespace-nowrap',
        right && 'text-right',
        mono && 'font-mono',
        className
      )}
    >
      {children}
    </td>
  )
}

export function Tr({
  children,
  className,
  onClick,
}: {
  children: React.ReactNode
  className?: string
  onClick?: () => void
}) {
  return (
    <tr
      onClick={onClick}
      className={clsx(
        'transition-colors duration-100',
        onClick && 'cursor-pointer',
        className
      )}
    >
      {children}
    </tr>
  )
}
