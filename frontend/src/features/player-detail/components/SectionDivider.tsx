export function SectionDivider({ label, subtitle }: { label: string; subtitle?: string }) {
  return (
    <div className="flex items-center gap-4 py-2">
      <div className="h-px flex-1 bg-gradient-to-r from-transparent via-ctp-surface2 to-ctp-surface1" />
      <div className="text-center">
        <p className="text-[11px] font-mono uppercase tracking-[0.28em] text-ctp-mauve">
          {label}
        </p>
        {subtitle && (
          <p className="mt-1 text-[10px] font-mono text-ctp-overlay0">
            {subtitle}
          </p>
        )}
      </div>
      <div className="h-px flex-1 bg-gradient-to-l from-transparent via-ctp-surface2 to-ctp-surface1" />
    </div>
  )
}
