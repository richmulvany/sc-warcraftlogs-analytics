import { normaliseRole } from '../../constants/wow'

export const DIFFS = ['All', 'Mythic', 'Heroic', 'Normal'] as const

export const WA_SECTIONS = [
  { id: 'overview',  label: 'Overview' },
  { id: 'wipes',     label: 'Wipe Walls' },
  { id: 'deaths',    label: 'Deaths' },
  { id: 'survival',  label: 'Survival' },
] as const

// Maps the lowercase output of normaliseRole to the Title-case keys used in the roles Record below.
const WIPE_ROLE_KEY: Record<string, 'Tank' | 'Healer' | 'DPS'> = {
  tank: 'Tank', healer: 'Healer', dps: 'DPS',
}

export function toWipeRoleKey(raw: string | null | undefined): 'Tank' | 'Healer' | 'DPS' | 'Unknown' {
  return WIPE_ROLE_KEY[normaliseRole(raw)] ?? 'Unknown'
}

// Fallback for players not in the kill roster (pure-DPS classes only; hybrids → Unknown)
export const CLASS_ROLE_FALLBACK: Record<string, 'DPS'> = {
  Hunter: 'DPS',
  Mage: 'DPS',
  Rogue: 'DPS',
  Warlock: 'DPS',
}
