import { ReactNode } from 'react'

interface PageLayoutProps {
  title: string
  subtitle?: string
  children: ReactNode
}

export function PageLayout({ children }: PageLayoutProps) {
  return (
    <div className="min-h-screen bg-ctp-base">
      <main className="max-w-7xl mx-auto px-6 py-8">
        {children}
      </main>
    </div>
  )
}
