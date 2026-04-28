import { useState } from 'react'
import { Sidebar } from './Sidebar'
import { MobileNav } from './MobileNav'

interface Props {
  children: React.ReactNode
  title: string
  subtitle?: string
  actions?: React.ReactNode
  nav?: React.ReactNode
  hideHeader?: boolean
  wide?: boolean
}

export function AppLayout({ children, title, subtitle, actions, nav, hideHeader = false, wide = false }: Props) {
  const maxW = wide
    ? 'xl:max-w-[1600px] 2xl:max-w-[1920px]'
    : 'xl:max-w-[1400px] 2xl:max-w-[1600px]'
  const [drawerOpen, setDrawerOpen] = useState(false)

  return (
    <div className="h-screen overflow-hidden bg-ctp-base flex flex-col md:flex-row">

      {/* Mobile top bar + drawer (hidden md+) */}
      <MobileNav
        title={title}
        isOpen={drawerOpen}
        onOpen={() => setDrawerOpen(true)}
        onClose={() => setDrawerOpen(false)}
      />

      {/* Tablet rail sidebar (hidden below md, hidden lg+) */}
      <div className="hidden md:flex lg:hidden flex-shrink-0">
        <Sidebar variant="rail" />
      </div>

      {/* Desktop full sidebar (hidden below lg) */}
      <div className="hidden lg:flex flex-shrink-0">
        <Sidebar variant="full" />
      </div>

      {/* Content column */}
      <div className="flex-1 flex flex-col overflow-hidden min-w-0">
        {!hideHeader && (
          <header className="hidden md:block flex-shrink-0 z-30 bg-ctp-base/80 backdrop-blur-md border-b border-ctp-surface0">
            <div className={`mx-auto w-full max-w-none ${maxW} px-4 py-4 md:px-6 md:py-4 lg:px-8 flex items-center gap-4`}>
              <div className="flex-1 min-w-0">
                <h1 className="text-base 2xl:text-lg font-semibold text-ctp-text leading-tight">{title}</h1>
                {subtitle && (
                  <p className="text-xs 2xl:text-[13px] font-mono text-ctp-overlay1 mt-0.5">{subtitle}</p>
                )}
              </div>
              {actions && (
                <div className="flex items-center gap-3 flex-shrink-0">
                  {actions}
                </div>
              )}
            </div>
            {nav}
          </header>
        )}

        {/* Scrollable page body */}
        <main className="flex-1 overflow-y-auto animate-fade-in">
          <div className={`mx-auto w-full max-w-none ${maxW} px-4 py-4 md:px-6 md:py-6 lg:px-8 lg:py-7 space-y-6 md:space-y-7`}>
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
