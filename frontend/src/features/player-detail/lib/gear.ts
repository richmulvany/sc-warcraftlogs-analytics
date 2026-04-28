import { toFiniteNumber } from '../../../utils/format'
import type { GearEnhancement, KillingBlowSummary } from './types'

export function parseGearJson(value: unknown): GearEnhancement[] {
  if (typeof value !== 'string' || !value.trim()) return []
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

export function parseKillingBlowsJson(value: unknown): KillingBlowSummary[] {
  if (typeof value !== 'string' || !value.trim()) return []
  try {
    const parsed = JSON.parse(value)
    if (!Array.isArray(parsed)) return []
    return parsed
      .map(row => ({
        name: typeof row?.name === 'string' ? row.name : '',
        count: toFiniteNumber(row?.count) ?? 0,
      }))
      .filter(row => row.name && row.count > 0)
  } catch {
    return []
  }
}

export function parseDifficultyNames(value: unknown): Set<string> {
  if (Array.isArray(value)) {
    return new Set(value.map(String).filter(Boolean))
  }
  if (typeof value !== 'string' || !value.trim()) {
    return new Set()
  }
  const matches = value.match(/Normal|Heroic|Mythic/gi) ?? []
  return new Set(matches.map(match => match[0].toUpperCase() + match.slice(1).toLowerCase()))
}

export function stripBlizzardTextureTokens(value: string): string {
  return value.replace(/\|A:[^|]*\|a/g, '').replace(/\s+/g, ' ').trim()
}

function professionQualityTier(value: string): string | null {
  const match = value.match(/Tier(\d+)/i)
  return match ? `ench t${match[1]}` : null
}

export function enchantLabel(enchant: GearEnhancement): string {
  const raw = enchant.display_string || enchant.source_item_name || 'Unknown enchant'
  return stripBlizzardTextureTokens(raw)
    .replace(/^Enchanted:\s*/i, '')
    .replace(/^Enchanted\s+/i, '')
    .trim()
}

export function enchantTierLabel(enchants: GearEnhancement[]): string | null {
  for (const enchant of enchants) {
    const raw = enchant.display_string || enchant.source_item_name || ''
    const tier = professionQualityTier(raw)
    if (tier) return tier
  }
  return null
}

export function socketLabel(socket: GearEnhancement): string {
  return stripBlizzardTextureTokens(socket.item_name || socket.display_string || socket.socket_type || 'Empty socket')
}
