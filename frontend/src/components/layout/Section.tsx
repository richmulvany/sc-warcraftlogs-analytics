import { ReactNode } from 'react'

interface SectionProps {
  title: string
  children: ReactNode
  action?: ReactNode
}

export function Section({ title, children, action }: SectionProps) {
  return (
    <section className="mb-10">
      <div className="flex items-center justify-between mb-4">
        <h2 className="section-heading">{title}</h2>
        {action}
      </div>
      {children}
    </section>
  )
}
