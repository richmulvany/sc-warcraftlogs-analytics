import clsx from 'clsx'

export function FilterBar({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={clsx('flex flex-wrap items-center gap-3', className)}>
      {children}
    </div>
  )
}
