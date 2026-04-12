import { useManifest } from '../../hooks/useManifest'
import { formatRelativeTime } from '../../utils/format'

interface HeaderProps {
  title: string
  subtitle?: string
}

export function Header({ title, subtitle }: HeaderProps) {
  const { manifest } = useManifest()

  return (
    <header className="border-b border-white/5 bg-surface-1/80 backdrop-blur-sm sticky top-0 z-10">
      <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-2 h-2 rounded-full bg-accent-teal shadow-[0_0_8px_rgba(34,201,160,0.6)]" />
          <h1 className="text-sm font-mono font-medium text-slate-200 tracking-wide">{title}</h1>
          {subtitle && (
            <span className="hidden sm:block text-xs text-slate-600 font-mono">/ {subtitle}</span>
          )}
        </div>

        <div className="flex items-center gap-4">
          {manifest && (
            <span className="text-xs font-mono text-slate-600">
              updated {formatRelativeTime(manifest.exported_at)}
            </span>
          )}
          <a
            href="https://github.com/richmulvany/databricks-pipeline-template"
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs font-mono text-slate-600 hover:text-slate-400 transition-colors"
          >
            github ↗
          </a>
        </div>
      </div>
    </header>
  )
}
