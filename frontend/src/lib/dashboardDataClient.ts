export type DashboardManifest = {
  generated_at: string
  snapshot_id: string
  format_version: number
  contract_set_version?: string | null
  datasets: Record<
    string,
    {
      path: string
      row_count: number
      byte_size?: number
      source_table?: string
      contract_id?: string
      contract_version?: string
      source_contract_id?: string
      source_contract_version?: string
    }
  >
}

const remoteBaseUrl = import.meta.env.VITE_DASHBOARD_DATA_BASE_URL?.replace(/\/$/, '') ?? ''

const manifestCache = new Map<string, Promise<DashboardManifest>>()
const datasetCache = new Map<string, Promise<unknown[]>>()

export function getDashboardDataBaseUrl(): string | null {
  return remoteBaseUrl || null
}

export function isRemoteDashboardDataEnabled(): boolean {
  return Boolean(getDashboardDataBaseUrl())
}

export async function fetchManifest(): Promise<DashboardManifest> {
  const baseUrl = getDashboardDataBaseUrl()
  if (!baseUrl) {
    throw new Error('Remote dashboard data is not configured. Set VITE_DASHBOARD_DATA_BASE_URL.')
  }

  const cached = manifestCache.get(baseUrl)
  if (cached) return cached

  const promise = fetch(`${baseUrl}/manifest.json`)
    .then(async res => {
      if (!res.ok) {
        throw new Error(`Failed to fetch manifest.json: HTTP ${res.status}`)
      }
      return res.json() as Promise<DashboardManifest>
    })
    .then(manifest => {
      if (!manifest || typeof manifest !== 'object' || !manifest.datasets) {
        throw new Error('manifest.json is malformed')
      }
      return manifest
    })

  manifestCache.set(baseUrl, promise)
  return promise
}

export async function fetchDataset<T = unknown>(datasetKey: string): Promise<T[]> {
  const baseUrl = getDashboardDataBaseUrl()
  if (!baseUrl) {
    throw new Error('Remote dashboard data is not configured. Set VITE_DASHBOARD_DATA_BASE_URL.')
  }

  const cacheKey = `${baseUrl}::${datasetKey}`
  const cached = datasetCache.get(cacheKey)
  if (cached) return cached as Promise<T[]>

  const promise = fetchManifest()
    .then(async manifest => {
      const dataset = manifest.datasets[datasetKey]
      if (!dataset) {
        throw new Error(`Dataset "${datasetKey}" is missing from manifest.json`)
      }

      const response = await fetch(`${baseUrl}/${dataset.path.replace(/^\//, '')}`)
      if (!response.ok) {
        throw new Error(`Failed to fetch dataset "${datasetKey}": HTTP ${response.status}`)
      }

      const payload = await response.json()
      if (!Array.isArray(payload)) {
        throw new Error(`Dataset "${datasetKey}" is malformed; expected an array`)
      }
      return payload as T[]
    })

  datasetCache.set(cacheKey, promise as Promise<unknown[]>)
  return promise
}
