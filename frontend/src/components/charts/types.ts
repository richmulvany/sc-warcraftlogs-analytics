export interface ChartTooltipProps<T = Record<string, unknown>> {
  active?: boolean
  payload?: Array<{
    dataKey?: string
    name?: string
    value?: number | string
    color?: string
    payload?: T
  }>
  label?: string | number
}
