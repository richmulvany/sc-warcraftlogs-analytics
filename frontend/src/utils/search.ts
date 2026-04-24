export function normaliseSearchText(value: unknown): string {
  return String(value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
}

function isSubsequence(query: string, value: string): boolean {
  let index = 0
  for (const char of value) {
    if (char === query[index]) index += 1
    if (index === query.length) return true
  }
  return false
}

export function matchesLooseSearch(query: string, rawValue: unknown): boolean {
  const value = normaliseSearchText(rawValue)
  if (!query) return true
  if (!value) return false
  if (value.includes(query)) return true

  if (query.length >= 5 && value.startsWith(query.slice(0, 3)) && isSubsequence(query, value)) {
    return true
  }

  return false
}
