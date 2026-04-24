import { useState, useEffect } from 'react'
import Papa from 'papaparse'

export interface CSVResult<T> {
  data: T[]
  loading: boolean
  error: string | null
}

interface UseCSVOptions {
  optional?: boolean
}

const BASE = import.meta.env.VITE_DATA_BASE_URL ?? '/data'

function normaliseCSVValue(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString()
  if (typeof value !== 'string') return value

  const trimmed = value.trim()
  if (trimmed === '') return ''
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed)
  if (trimmed === 'true') return true
  if (trimmed === 'false') return false
  return trimmed
}

export function useCSV<T extends object>(filename: string, options: UseCSVOptions = {}): CSVResult<T> {
  const [data, setData] = useState<T[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { optional = false } = options

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`${BASE}/${filename}`)
      .then(res => {
        if (!res.ok) {
          if (optional && res.status === 404) return ''
          throw new Error(`HTTP ${res.status} loading ${filename}`)
        }
        return res.text()
      })
      .then(text => {
        if (optional && text.trim() === '') {
          setData([])
          setLoading(false)
          return
        }

        const result = Papa.parse<T>(text, {
          header: true,
          skipEmptyLines: true,
          dynamicTyping: true,
          transformHeader: h => h.trim(),
          transform: value => typeof value === 'string' ? value.trim() : value,
        })
        if (!cancelled) {
          setData(
            result.data.map(row =>
              Object.fromEntries(
                Object.entries(row).map(([key, value]) => [key, normaliseCSVValue(value)])
              ) as T
            )
          )
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
  }, [filename, optional])

  return { data, loading, error }
}
