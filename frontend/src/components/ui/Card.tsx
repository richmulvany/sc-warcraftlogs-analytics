import clsx from 'clsx'

interface CardProps {
  children: React.ReactNode
  className?: string
  hover?: boolean
}

export function Card({ children, className, hover = false }: CardProps) {
  return (
    <div
      className={clsx(
        'ctp-card',
        hover && 'cursor-pointer transition-all duration-200 hover:border-ctp-surface2 hover:shadow-card-hover hover:-translate-y-0.5',
        className
      )}
    >
      {children}
    </div>
  )
}

export function CardHeader({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={clsx('px-4 py-3 md:px-5 md:py-4 border-b border-ctp-surface1', className)}>
      {children}
    </div>
  )
}

export function CardTitle({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <h3 className={clsx('text-sm 2xl:text-[15px] font-semibold text-ctp-text', className)}>
      {children}
    </h3>
  )
}

export function CardSubtitle({ children }: { children: React.ReactNode }) {
  return <p className="text-xs 2xl:text-sm text-ctp-overlay1 mt-0.5">{children}</p>
}

export function CardBody({ children, className }: { children: React.ReactNode; className?: string }) {
  return <div className={clsx('p-4 md:p-5', className)}>{children}</div>
}
