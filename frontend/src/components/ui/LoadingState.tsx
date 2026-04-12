export function LoadingState({ rows = 3 }: { rows?: number }) {
  return (
    <div className="space-y-3">
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="h-20 rounded-xl bg-surface-2 animate-pulse"
          style={{ opacity: 1 - i * 0.2 }}
        />
      ))}
    </div>
  )
}
