import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Users, Skull, Swords, CalendarDays, Shield, Eye, ChevronDown, AlertTriangle, Beaker, KeyRound } from 'lucide-react'
import clsx from 'clsx'
import { useColourBlind } from '../../context/ColourBlindContext'
import { type ColourBlindMode, MODE_LABELS } from '../../constants/palettes'
import { useManifest } from '../../hooks/useManifest'

const NAV = [
  { to: '/',               label: 'Dashboard',        Icon: LayoutDashboard },
  { to: '/players',        label: 'Players',          Icon: Users },
  { to: '/bosses',         label: 'Boss Progression', Icon: Skull },
  { to: '/wipe-analysis',  label: 'Wipe Analysis',    Icon: AlertTriangle },
  { to: '/raids',          label: 'Raids',            Icon: Swords },
  { to: '/mythic-plus',    label: 'Mythic+',          Icon: KeyRound },
]

const SECONDARY = [
  { to: '/attendance',  label: 'Attendance',  Icon: CalendarDays },
  { to: '/roster',      label: 'Roster',      Icon: Shield },
  { to: '/preparation', label: 'Preparation', Icon: Beaker },
]

const CB_MODES: ColourBlindMode[] = ['normal', 'deuteranopia', 'protanopia', 'tritanopia']

function formatManifestDate(value?: string): string {
  if (!value) return 'local'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

type Variant = 'full' | 'rail' | 'drawer'

interface Props {
  variant?: Variant
  onNavClick?: () => void
}

export function Sidebar({ variant = 'full', onNavClick }: Props) {
  const { mode, setMode } = useColourBlind()
  const { manifest } = useManifest()
  const [cbOpen, setCbOpen] = useState(false)

  const isRail = variant === 'rail'

  /* ── Rail variant: icon-only strip ── */
  if (isRail) {
    const allNav = [...NAV, ...SECONDARY]
    return (
      <aside className="flex-shrink-0 w-16 flex flex-col bg-ctp-mantle border-r border-ctp-surface0 overflow-hidden">
        <div className="px-2 pt-4 pb-3 border-b border-ctp-surface0 flex justify-center flex-shrink-0">
          <div className="w-8 h-8 rounded-xl bg-ctp-mauve/20 border border-ctp-mauve/30 flex items-center justify-center overflow-hidden">
            <img src="/logo.jpg" alt="Student Council logo" className="w-7 h-7 rounded-[10px] object-cover" />
          </div>
        </div>
        <nav className="flex-1 flex flex-col items-center px-2 py-3 gap-1 overflow-y-auto">
          {allNav.map(({ to, label, Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              title={label}
              className={({ isActive }) =>
                clsx(
                  'w-10 h-10 flex items-center justify-center rounded-xl transition-all duration-150',
                  isActive
                    ? 'bg-ctp-surface0 text-ctp-mauve'
                    : 'text-ctp-subtext0 hover:text-ctp-text hover:bg-ctp-surface0/70'
                )
              }
            >
              <Icon className="w-4 h-4" />
            </NavLink>
          ))}
        </nav>
      </aside>
    )
  }

  /* ── Full / drawer variant: labelled sidebar ── */
  const asideClass = variant === 'drawer'
    ? 'flex flex-col w-full h-full bg-ctp-mantle overflow-hidden'
    : 'flex-shrink-0 w-56 flex flex-col bg-ctp-mantle border-r border-ctp-surface0 overflow-hidden'

  return (
    <aside className={asideClass}>

      {/* Guild branding */}
      <div className="px-5 pt-6 pb-5 border-b border-ctp-surface0 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-xl bg-ctp-mauve/20 border border-ctp-mauve/30 flex items-center justify-center flex-shrink-0 overflow-hidden">
            <img src="/logo.jpg" alt="Student Council logo" className="w-7 h-7 rounded-[10px] object-cover" />
          </div>
          <div className="min-w-0">
            <p className="text-sm 2xl:text-[15px] font-semibold text-ctp-text truncate">Student Council</p>
            <p className="text-[10px] 2xl:text-[11px] font-mono text-ctp-overlay1 mt-0.5">Twisting Nether · EU</p>
          </div>
        </div>
      </div>

      {/* Primary nav */}
      <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
        <p className="section-label px-2 mb-3">Analytics</p>
        {NAV.map(({ to, label, Icon }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            onClick={onNavClick}
            className={({ isActive }) =>
              clsx(
                'flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm 2xl:text-[15px] font-medium transition-all duration-150',
                isActive
                  ? 'bg-ctp-surface0 text-ctp-text'
                  : 'text-ctp-subtext0 hover:text-ctp-text hover:bg-ctp-surface0/70'
              )
            }
          >
            {({ isActive }) => (
              <>
                <Icon className={clsx(
                  'w-4 h-4 flex-shrink-0 transition-opacity',
                  isActive ? 'text-ctp-mauve opacity-100' : 'opacity-60'
                )} />
                <span>{label}</span>
              </>
            )}
          </NavLink>
        ))}

        <div className="pt-4">
          <p className="section-label px-2 mb-3">Guild</p>
          {SECONDARY.map(({ to, label, Icon }) => (
            <NavLink
              key={to}
              to={to}
              onClick={onNavClick}
              className={({ isActive }) =>
                clsx(
                  'flex items-center gap-3 px-3 py-2 rounded-xl text-sm 2xl:text-[15px] font-medium transition-all duration-150',
                  isActive
                    ? 'bg-ctp-surface0 text-ctp-text'
                    : 'text-ctp-overlay1 hover:text-ctp-subtext1 hover:bg-ctp-surface0/50'
                )
              }
            >
              {({ isActive }) => (
                <>
                  <Icon className={clsx(
                    'w-4 h-4 flex-shrink-0 transition-opacity',
                    isActive ? 'text-ctp-blue opacity-100' : 'opacity-60'
                  )} />
                  <span>{label}</span>
                </>
              )}
            </NavLink>
          ))}
        </div>
      </nav>

      {/* Colour blind mode toggle (collapsible) */}
      <div className="px-3 pb-3 flex-shrink-0">
        <div className="bg-ctp-surface0/50 rounded-xl border border-ctp-surface1/50 overflow-hidden">
          <button
            onClick={() => setCbOpen(o => !o)}
            className="w-full flex items-center gap-2 px-3 py-2.5 hover:bg-ctp-surface1/30 transition-colors"
          >
            <Eye className="w-3 h-3 text-ctp-overlay1 flex-shrink-0" aria-hidden="true" />
            <p className="text-[10px] font-mono text-ctp-overlay1 uppercase tracking-wide flex-1 text-left">
              Colour mode
            </p>
            {mode !== 'normal' && (
              <span className="text-[9px] font-mono text-ctp-mauve truncate max-w-[54px]">
                {MODE_LABELS[mode]}
              </span>
            )}
            <ChevronDown className={clsx(
              'w-3 h-3 text-ctp-overlay0 transition-transform flex-shrink-0',
              cbOpen && 'rotate-180'
            )} aria-hidden="true" />
          </button>

          {cbOpen && (
            <div className="px-2 pb-2 grid grid-cols-2 gap-1">
              {CB_MODES.map(m => (
                <button
                  key={m}
                  onClick={() => setMode(m)}
                  className={clsx(
                    'px-1.5 py-1 rounded-lg text-[10px] font-mono transition-all duration-150 text-left truncate',
                    mode === m
                      ? 'bg-ctp-mauve/20 text-ctp-mauve'
                      : 'text-ctp-overlay0 hover:text-ctp-subtext0 hover:bg-ctp-surface1/50'
                  )}
                >
                  {MODE_LABELS[m]}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="px-5 py-3 border-t border-ctp-surface0 flex-shrink-0">
        <p className="text-[10px] font-mono text-ctp-overlay0 leading-relaxed truncate">
          Contracts: {manifest?.contract_set_version ?? 'local'}
        </p>
        <p className="text-[10px] font-mono text-ctp-overlay0 leading-relaxed truncate">
          Data: {formatManifestDate(manifest?.generated_at)}
        </p>
      </div>
    </aside>
  )
}
