interface ErrorStateProps {
  message: string
  onRetry?: () => void
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="rounded-xl border border-accent-coral/20 bg-accent-coral/5 p-6 text-center">
      <p className="text-sm font-mono text-accent-coral mb-1">Failed to load data</p>
      <p className="text-xs text-slate-500 mb-4">{message}</p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="px-4 py-1.5 text-xs font-mono rounded-lg bg-surface-3 hover:bg-surface-4 text-slate-300 transition-colors"
        >
          retry
        </button>
      )}
    </div>
  )
}
