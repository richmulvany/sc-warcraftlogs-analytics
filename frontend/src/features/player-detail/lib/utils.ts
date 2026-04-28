import { formatDate } from '../../../utils/format'

export function formatNumber(value: unknown, digits = 0): string {
  const number = Number(value)
  if (!Number.isFinite(number) || number <= 0) return '—'
  return number.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })
}

export function rankToParseScale(rank: number, total: number): number {
  if (total <= 1) return 100
  return Math.round(((total - rank) / (total - 1)) * 100)
}

export function getSurvivabilityRankColor(
  rank: number,
  total: number,
  getParseColor: (rank: number) => string,
  worstColor: string,
): string {
  const scaledRank = rankToParseScale(rank, total)
  return scaledRank < 25 ? worstColor : getParseColor(scaledRank)
}

export function formatRealmName(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) return ''
  return value
    .replace(/[-_]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim()
}

export function formatRealmSlug(value: unknown): string {
  if (typeof value !== 'string' || !value.trim()) return ''
  return value
    .replace(/([a-z])([A-Z])/g, '$1-$2')
    .replace(/[\s_]+/g, '-')
    .replace(/-+/g, '-')
    .toLowerCase()
    .trim()
}

export function externalCharacterLinks(playerName: string, realm: unknown, fallbackRealm: unknown) {
  const realmSlug = formatRealmSlug(realm) || formatRealmSlug(fallbackRealm)
  if (!playerName || !realmSlug) return null
  const characterSlug = playerName.toLowerCase()
  return {
    raiderIo: `https://raider.io/characters/eu/${encodeURIComponent(realmSlug)}/${encodeURIComponent(playerName)}`,
    armory: `https://worldofwarcraft.blizzard.com/en-gb/character/eu/${encodeURIComponent(realmSlug)}/${encodeURIComponent(characterSlug)}`,
  }
}

export function clampTooltipPosition(clientX: number, clientY: number) {
  const width = 288
  const height = 420
  const offset = 14
  const margin = 12
  return {
    left: Math.min(Math.max(clientX + offset, margin), window.innerWidth - width - margin),
    top: Math.min(Math.max(clientY + offset, margin), window.innerHeight - height - margin),
  }
}

export function warcraftLogsReportUrl(reportCode: string, fightId?: string | number): string | null {
  if (!reportCode) return null
  const fight = fightId ? `#fight=${encodeURIComponent(String(fightId))}` : ''
  return `https://www.warcraftlogs.com/reports/${encodeURIComponent(reportCode)}${fight}`
}

export function formatBlizzardTimestamp(value: unknown): string {
  const timestamp = Number(value)
  if (!Number.isFinite(timestamp) || timestamp <= 0) return '—'
  return formatDate(new Date(timestamp).toISOString())
}
