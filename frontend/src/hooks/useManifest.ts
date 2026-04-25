import { useState, useEffect } from 'react'
import {
  DashboardManifest,
  fetchManifest,
  isRemoteDashboardDataEnabled,
} from '../lib/dashboardDataClient'

export function useManifest() {
  const [manifest, setManifest] = useState<DashboardManifest | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!isRemoteDashboardDataEnabled()) {
      setManifest(null)
      setError('Remote dashboard data is not configured (VITE_DASHBOARD_DATA_BASE_URL is unset).')
      setLoading(false)
      return
    }

    fetchManifest()
      .then(m => {
        setManifest(m)
        setError(null)
      })
      .catch(err => {
        console.error('[dashboard] failed to load manifest.json', err)
        setManifest(null)
        setError(String(err))
      })
      .finally(() => setLoading(false))
  }, [])

  return { manifest, loading, error }
}
