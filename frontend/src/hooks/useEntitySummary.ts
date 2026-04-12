import { useState, useEffect } from 'react'
import { api, EntitySummaryRow, ExportEnvelope } from '../api'

interface UseEntitySummaryResult {
  data: ExportEnvelope<EntitySummaryRow> | null
  loading: boolean
  error: string | null
}

export function useEntitySummary(): UseEntitySummaryResult {
  const [data, setData] = useState<ExportEnvelope<EntitySummaryRow> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api
      .fetchEntitySummary()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  return { data, loading, error }
}
