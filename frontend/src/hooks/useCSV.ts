import { useState, useEffect } from 'react'
import Papa from 'papaparse'

export interface CSVResult<T> {
  data: T[]
  loading: boolean
  error: string | null
}

const BASE = import.meta.env.VITE_DATA_BASE_URL ?? '/data'

export function useCSV<T extends object>(filename: string): CSVResult<T> {
  const [data, setData] = useState<T[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`${BASE}/${filename}`)
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status} loading ${filename}`)
        return res.text()
      })
      .then(text => {
        const result = Papa.parse<T>(text, {
          header: true,
          skipEmptyLines: true,
          dynamicTyping: true,
          transformHeader: h => h.trim(),
        })
        if (!cancelled) {
          setData(result.data)
          setLoading(false)
        }
      })
      .catch(err => {
        if (!cancelled) {
          setError(String(err))
          setLoading(false)
        }
      })

    return () => { cancelled = true }
  }, [filename])

  return { data, loading, error }
}
