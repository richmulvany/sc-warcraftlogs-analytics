import { Menu } from 'lucide-react'
import { Sidebar } from './Sidebar'

interface Props {
  title: string
  isOpen: boolean
  onOpen: () => void
  onClose: () => void
}

export function MobileNav({ title, isOpen, onOpen, onClose }: Props) {
  return (
    <>
      {/* Top bar — visible only below md */}
      <div className="flex md:hidden flex-shrink-0 items-center gap-3 px-4 h-14 bg-ctp-mantle border-b border-ctp-surface0 z-40">
        <button
          onClick={onOpen}
          aria-label="Open navigation"
          className="p-1.5 rounded-lg text-ctp-subtext0 hover:text-ctp-text hover:bg-ctp-surface0 transition-colors"
        >
          <Menu className="w-5 h-5" />
        </button>
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-6 h-6 rounded-lg bg-ctp-mauve/20 border border-ctp-mauve/30 flex items-center justify-center flex-shrink-0 overflow-hidden">
            <img src="/logo.jpg" alt="" className="w-5 h-5 rounded-md object-cover" />
          </div>
          <span className="text-sm font-semibold text-ctp-text truncate">{title}</span>
        </div>
      </div>

      {/* Drawer — only rendered when open on mobile */}
      {isOpen && (
        <div className="fixed inset-0 z-50 flex md:hidden">
          {/* Scrim */}
          <div
            className="absolute inset-0 bg-black/50"
            onClick={onClose}
            aria-hidden="true"
          />
          {/* Panel — z-10 lifts it above the absolute scrim so scrim clicks reach its onClick */}
          <div className="relative z-10 flex flex-col w-64 max-w-[80vw]">
            <Sidebar variant="drawer" onNavClick={onClose} />
          </div>
        </div>
      )}
    </>
  )
}
