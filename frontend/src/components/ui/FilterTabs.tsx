import clsx from 'clsx'

type FilterTabOption<T extends string> = T | { value: T; label: string }

interface FilterTabsProps<T extends string> {
  options: readonly FilterTabOption<T>[]
  value: T
  onChange: (value: T) => void
  className?: string
  activeClassName?: string
  inactiveClassName?: string
  buttonClassName?: string
}

export function FilterTabs<T extends string>({
  options,
  value,
  onChange,
  className,
  activeClassName = 'bg-ctp-mauve/20 text-ctp-mauve shadow-mauve-glow',
  inactiveClassName = 'text-ctp-overlay1 hover:text-ctp-subtext1',
  buttonClassName,
}: FilterTabsProps<T>) {
  return (
    <div className={clsx('flex items-center gap-0.5 bg-ctp-surface0 rounded-lg p-1.5 border border-ctp-surface1', className)}>
      {options.map(option => {
        const optionValue = typeof option === 'string' ? option : option.value
        const optionLabel = typeof option === 'string' ? option : option.label

        return (
        <button
          key={optionValue}
          onClick={() => onChange(optionValue)}
          className={clsx(
            'px-3 py-2 rounded-md text-xs font-medium transition-all duration-150',
            buttonClassName,
            value === optionValue ? activeClassName : inactiveClassName
          )}
        >
          {optionLabel}
        </button>
      )})}
    </div>
  )
}
