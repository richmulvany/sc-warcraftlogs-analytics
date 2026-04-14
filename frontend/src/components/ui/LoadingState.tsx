export function LoadingState({ rows = 3 }: { rows?: number }) {
  return (
    <div className="space-y-2.5 animate-pulse">
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="h-10 rounded-xl bg-ctp-surface1/50"
          style={{ opacity: 1 - i * 0.18 }}
        />
      ))}
    </div>
  )
}

export function SkeletonCard() {
  return (
    <div className="bg-ctp-surface0 rounded-2xl p-5 border border-ctp-surface1 animate-pulse">
      <div className="h-3 w-20 bg-ctp-surface1 rounded-lg mb-4" />
      <div className="h-7 w-28 bg-ctp-surface1/80 rounded-lg mb-2.5" />
      <div className="h-3 w-24 bg-ctp-surface1/60 rounded-lg" />
    </div>
  )
}
