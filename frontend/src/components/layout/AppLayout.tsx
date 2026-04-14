import { Sidebar } from './Sidebar'

interface Props {
  children: React.ReactNode
  title: string
  subtitle?: string
  actions?: React.ReactNode
}

export function AppLayout({ children, title, subtitle, actions }: Props) {
  return (
    <div className="h-screen overflow-hidden bg-ctp-base flex">
      <Sidebar />

      <div className="flex-1 flex flex-col overflow-hidden min-w-0">
        {/* Sticky top bar */}
        <header className="flex-shrink-0 z-30 bg-ctp-base/80 backdrop-blur-md border-b border-ctp-surface0 px-8 py-4 flex items-center gap-4">
          <div className="flex-1 min-w-0">
            <h1 className="text-base font-semibold text-ctp-text leading-tight">{title}</h1>
            {subtitle && (
              <p className="text-xs font-mono text-ctp-overlay0 mt-0.5">{subtitle}</p>
            )}
          </div>
          {actions && (
            <div className="flex items-center gap-3 flex-shrink-0">
              {actions}
            </div>
          )}
        </header>

        {/* Scrollable page body */}
        <main className="flex-1 overflow-y-auto px-8 py-7 space-y-7 animate-fade-in">
          {children}
        </main>
      </div>
    </div>
  )
}
