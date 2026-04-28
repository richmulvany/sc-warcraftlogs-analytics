import type { ReactNode } from 'react'
import { CardBody } from './Card'
import { LoadingState } from './LoadingState'
import { ErrorState } from './ErrorState'

interface DataStateProps<T> {
  loading: boolean
  error: string | null
  data: T[]
  children: (data: T[]) => ReactNode
  empty?: ReactNode
  loadingRows?: number
}

export function DataState<T>({
  loading,
  error,
  data,
  children,
  empty,
  loadingRows = 8,
}: DataStateProps<T>): ReactNode {
  if (loading) return <CardBody><LoadingState rows={loadingRows} /></CardBody>
  if (error) return <CardBody><ErrorState message={error} /></CardBody>
  if (!data.length) {
    return (
      <CardBody>
        {typeof empty === 'string' || empty === undefined
          ? <p className="text-xs font-mono text-ctp-overlay0 text-center py-8">{empty ?? 'No data available.'}</p>
          : empty}
      </CardBody>
    )
  }
  return <>{children(data)}</>
}
