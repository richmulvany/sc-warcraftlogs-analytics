interface ErrorStateProps {
  message: string
  onRetry?: () => void
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="rounded-2xl border border-ctp-red/20 bg-ctp-red/5 p-6 text-center">
      <p className="text-sm font-mono text-ctp-red mb-1">Failed to load data</p>
      <p className="text-xs text-ctp-overlay1 mb-4">{message}</p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="px-4 py-1.5 text-xs font-mono rounded-xl bg-ctp-surface1 hover:bg-ctp-surface2 text-ctp-subtext1 transition-colors"
        >
          retry
        </button>
      )}
    </div>
  )
}
