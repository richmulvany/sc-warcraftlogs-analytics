import { ReactNode } from 'react'
import { Header } from './Header'

interface PageLayoutProps {
  title: string
  subtitle?: string
  children: ReactNode
}

export function PageLayout({ title, subtitle, children }: PageLayoutProps) {
  return (
    <div className="min-h-screen bg-surface-0">
      {/* Subtle grid texture */}
      <div
        className="fixed inset-0 pointer-events-none opacity-[0.02]"
        style={{
          backgroundImage: 'linear-gradient(rgba(255,255,255,.3) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,.3) 1px, transparent 1px)',
          backgroundSize: '40px 40px',
        }}
      />

      <Header title={title} subtitle={subtitle} />

      <main className="max-w-7xl mx-auto px-6 py-8">
        {children}
      </main>
    </div>
  )
}
