import type { ReactNode } from 'react'

export function MiniNote({ children }: { children: ReactNode }) {
  return <p className="text-[10px] font-mono text-ctp-overlay0">{children}</p>
}

export function StatusPill({
  label,
  active = false,
  compact = false,
}: {
  label: string
  active?: boolean
  compact?: boolean
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full border font-mono uppercase ${
        compact ? 'px-1.5 py-0.5 text-[9px] tracking-[0.1em]' : 'px-2.5 py-1 text-[10px] tracking-[0.18em]'
      } ${
        active
          ? 'border-ctp-mauve/30 bg-ctp-mauve/10 text-ctp-mauve'
          : 'border-ctp-surface2 bg-ctp-surface1/50 text-ctp-overlay0'
      }`}
    >
      {label}
    </span>
  )
}

export function SignalTile({
  label,
  value,
  detail,
  accentClass = 'text-ctp-text',
}: {
  label: string
  value: ReactNode
  detail: ReactNode
  accentClass?: string
}) {
  return (
    <div className="rounded-2xl border border-ctp-surface1/60 bg-ctp-surface1/30 p-3.5">
      <p className="mb-2 text-[10px] font-mono uppercase tracking-[0.18em] text-ctp-overlay0">
        {label}
      </p>
      <div className={`text-sm font-semibold leading-tight ${accentClass}`}>{value}</div>
      <p className="mt-1 text-[10px] font-mono leading-relaxed text-ctp-overlay0">{detail}</p>
    </div>
  )
}
