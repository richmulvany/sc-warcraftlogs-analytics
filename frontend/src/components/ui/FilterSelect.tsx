import clsx from 'clsx'

interface FilterSelectProps<T extends string> {
  value: T
  onChange: (value: T) => void
  options: readonly T[]
  className?: string
  hint?: string
  showHint?: boolean
  onFocus?: () => void
  onMouseDown?: () => void
}

export function FilterSelect<T extends string>({
  value,
  onChange,
  options,
  className,
  hint,
  showHint = false,
  onFocus,
  onMouseDown,
}: FilterSelectProps<T>) {
  return (
    <div className="relative">
      {hint && showHint ? (
        <span className="absolute right-7 top-1/2 -translate-y-1/2 text-xs font-mono text-ctp-overlay0 pointer-events-none">
          {hint}
        </span>
      ) : null}
      <select
        value={value}
        onFocus={onFocus}
        onMouseDown={onMouseDown}
        onChange={e => onChange(e.target.value as T)}
        className={clsx(
          'bg-ctp-surface0 border border-ctp-surface1 rounded-xl px-3 py-1.5 text-xs 2xl:text-[13px] text-ctp-subtext1 font-mono focus:outline-none focus:border-ctp-mauve/40 transition-colors',
          className
        )}
      >
        {options.map(option => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </div>
  )
}
