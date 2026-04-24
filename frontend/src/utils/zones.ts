const EXCLUDED_ZONE_NAMES = new Set(['Blackrock Depths'])

export function isIncludedZoneName(value: unknown): value is string {
  if (typeof value !== 'string') return false
  const trimmed = value.trim()
  return trimmed !== '' && trimmed.toLowerCase() !== 'null' && !EXCLUDED_ZONE_NAMES.has(trimmed)
}
