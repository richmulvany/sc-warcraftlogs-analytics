import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Users, Skull, Swords, CalendarDays, Shield, Eye, ChevronDown, AlertTriangle } from 'lucide-react'
import clsx from 'clsx'
import { useColourBlind } from '../../context/ColourBlindContext'
import { type ColourBlindMode, MODE_LABELS } from '../../constants/palettes'

const NAV = [
  { to: '/',               label: 'Dashboard',        Icon: LayoutDashboard },
  { to: '/players',        label: 'Players',          Icon: Users },
  { to: '/bosses',         label: 'Boss Progression', Icon: Skull },
  { to: '/wipe-analysis',  label: 'Wipe Analysis',    Icon: AlertTriangle },
  { to: '/raids',          label: 'Raids',            Icon: Swords },
]

const SECONDARY = [
  { to: '/attendance', label: 'Attendance', Icon: CalendarDays },
  { to: '/roster',     label: 'Roster',     Icon: Shield },
]

const CB_MODES: ColourBlindMode[] = ['normal', 'deuteranopia', 'protanopia', 'tritanopia']

export function Sidebar() {
  const { mode, setMode } = useColourBlind()
  const [cbOpen, setCbOpen] = useState(false)

  return (
    <aside className="flex-shrink-0 w-56 flex flex-col bg-ctp-mantle border-r border-ctp-surface0 overflow-hidden">

      {/* Guild branding */}
      <div className="px-5 pt-6 pb-5 border-b border-ctp-surface0 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-xl bg-ctp-mauve/20 border border-ctp-mauve/30 flex items-center justify-center flex-shrink-0">
            <Swords className="w-4 h-4 text-ctp-mauve" />
          </div>
          <div className="min-w-0">
            <p className="text-sm font-semibold text-ctp-text truncate">Student Council</p>
            <p className="text-[10px] font-mono text-ctp-overlay0 mt-0.5">Twisting Nether · EU</p>
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
            className={({ isActive }) =>
              clsx(
                'flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-150',
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
              className={({ isActive }) =>
                clsx(
                  'flex items-center gap-3 px-3 py-2 rounded-xl text-sm font-medium transition-all duration-150',
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
          {/* Header — always visible, click to expand */}
          <button
            onClick={() => setCbOpen(o => !o)}
            className="w-full flex items-center gap-2 px-3 py-2.5 hover:bg-ctp-surface1/30 transition-colors"
          >
            <Eye className="w-3 h-3 text-ctp-overlay1 flex-shrink-0" />
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
            )} />
          </button>

          {/* Expandable button grid */}
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
        <p className="text-[10px] font-mono text-ctp-overlay0 leading-relaxed">
          WarcraftLogs Analytics
          <br />
          <span className="text-ctp-surface2">debug export · v0.2</span>
        </p>
      </div>
    </aside>
  )
}
