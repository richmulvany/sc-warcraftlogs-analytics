import { useState, useEffect } from 'react'
import { api, ExportManifest } from '../api'

export function useManifest() {
  const [manifest, setManifest] = useState<ExportManifest | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api
      .fetchManifest()
      .then(setManifest)
      .catch(() => setManifest(null))
      .finally(() => setLoading(false))
  }, [])

  return { manifest, loading }
}
