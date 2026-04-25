import { useState, useEffect } from 'react'
import {
  DashboardManifest,
  fetchManifest,
  isRemoteDashboardDataEnabled,
} from '../lib/dashboardDataClient'

export function useManifest() {
  const [manifest, setManifest] = useState<DashboardManifest | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!isRemoteDashboardDataEnabled()) {
      setManifest(null)
      setLoading(false)
      return
    }

    fetchManifest()
      .then(setManifest)
      .catch(() => setManifest(null))
      .finally(() => setLoading(false))
  }, [])

  return { manifest, loading }
}
