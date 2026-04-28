import { useState } from 'react'
import { useColourBlind } from '../../../context/ColourBlindContext'
import type { PlayerCharacterEquipment } from '../../../types'
import { QUALITY_COLORS, ENCHANTABLE_SLOTS, SOCKET_EXPECTED_SLOTS } from '../lib/constants'
import { parseGearJson, enchantLabel, enchantTierLabel, socketLabel } from '../lib/gear'
import { clampTooltipPosition } from '../lib/utils'

function ItemTooltip({
  item,
  color,
  position,
}: {
  item: PlayerCharacterEquipment
  color: string
  position: { left: number; top: number }
}) {
  const { killColor } = useColourBlind()
  const enchants = parseGearJson(item.enchantments_json)
  const sockets = parseGearJson(item.sockets_json)
  const stats = parseGearJson(item.stats_json)
  const spells = parseGearJson(item.spells_json)

  return (
    <div
      className="pointer-events-none fixed z-50 max-h-[calc(100vh-24px)] w-72 overflow-y-auto rounded-xl border border-ctp-surface2 bg-ctp-crust/95 p-4 text-left shadow-2xl backdrop-blur"
      style={{ left: position.left, top: position.top }}
    >
      <p className="text-sm font-semibold" style={{ color }}>{item.item_name}</p>
      {item.transmog_name && (
        <p className="mt-1 text-xs text-ctp-pink">Transmog: {item.transmog_name}</p>
      )}
      <p className="mt-1 text-xs font-mono text-ctp-yellow">Item Level {item.item_level || '—'}</p>
      <p className="mt-2 text-xs text-ctp-subtext1">{item.binding || 'Binds when picked up'}</p>
      <p className="text-xs text-ctp-overlay1">{item.inventory_type || item.slot_name} {item.item_subclass ? `· ${item.item_subclass}` : ''}</p>

      {stats.length > 0 && (
        <div className="mt-3 space-y-0.5">
          {stats.slice(0, 8).map((stat, index) => (
            <p key={index} className="text-xs text-ctp-text">
              {stat.display || `${stat.value ? `+${stat.value} ` : ''}${stat.type ?? ''}`}
            </p>
          ))}
        </div>
      )}

      {enchants.length > 0 && (
        <div className="mt-3 space-y-0.5">
          {enchants.map((enchant, index) => (
            <p key={index} className="text-xs" style={{ color: killColor }}>
              Enchanted: {enchantLabel(enchant)}
            </p>
          ))}
        </div>
      )}

      {sockets.length > 0 && (
        <div className="mt-3 space-y-1">
          {sockets.map((socket, index) => (
            <p key={index} className="text-xs text-ctp-sapphire">
              {socketLabel(socket)}
            </p>
          ))}
        </div>
      )}

      {spells.length > 0 && (
        <div className="mt-3 space-y-2">
          {spells.slice(0, 3).map((spell, index) => (
            <div key={index}>
              <p className="text-xs font-medium text-ctp-text">{spell.spell_name}</p>
              {spell.description && <p className="mt-0.5 text-xs leading-relaxed text-ctp-subtext1">{spell.description}</p>}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export function GearSlot({ item, label, classColor }: { item?: PlayerCharacterEquipment; label: string; classColor: string }) {
  const { killColor, wipeColor } = useColourBlind()
  const [tooltipPosition, setTooltipPosition] = useState<{ left: number; top: number } | null>(null)
  const qualityColor = item ? (QUALITY_COLORS[item.quality] ?? classColor) : '#45475a'
  const enchants = item ? parseGearJson(item.enchantments_json) : []
  const sockets = item ? parseGearJson(item.sockets_json) : []
  const missingEnchant = Boolean(item && ENCHANTABLE_SLOTS.has(item.slot_type) && enchants.length === 0)
  const missingGem = Boolean(item && SOCKET_EXPECTED_SLOTS.has(item.slot_type) && sockets.length === 0)

  return (
    <div
      className="group relative flex min-h-[54px] items-center gap-2 rounded-xl border bg-ctp-surface0/55 px-2 py-1.5"
      style={{ borderColor: missingEnchant ? wipeColor : item ? `${qualityColor}88` : '#45475a' }}
      onMouseMove={(event) => {
        if (!item) return
        setTooltipPosition(clampTooltipPosition(event.clientX, event.clientY))
      }}
      onMouseLeave={() => setTooltipPosition(null)}
    >
      <div
        className="relative flex h-10 w-10 flex-shrink-0 items-center justify-center overflow-hidden rounded-lg border bg-ctp-crust/80"
        style={{ borderColor: missingEnchant ? wipeColor : item ? qualityColor : '#313244' }}
      >
        {item?.icon_url ? (
          <img src={item.icon_url} alt="" className="h-full w-full object-cover" />
        ) : (
          <span className="text-[9px] font-mono text-ctp-surface2">{label.slice(0, 2)}</span>
        )}
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-2">
          <p className="truncate text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">{label}</p>
          <p className="text-[10px] font-mono text-ctp-yellow">{item?.item_level || '—'}</p>
        </div>
        <p className="mt-0.5 truncate text-xs font-medium" style={{ color: item ? qualityColor : '#6c7086' }}>
          {item?.item_name || 'Empty'}
        </p>
        {(enchants.length > 0 || sockets.length > 0 || missingEnchant || missingGem) && (
          <div className="mt-0.5 flex items-center gap-1.5">
            {enchants.length > 0 && (
              <span className="text-[9px] font-mono" style={{ color: killColor }}>
                {enchantTierLabel(enchants) ?? 'ench'}
              </span>
            )}
            {missingEnchant && <span className="text-[9px] font-mono" style={{ color: wipeColor }}>missing enchant</span>}
            {sockets.slice(0, 3).map((socket, index) => (
              <span key={index} className="h-1.5 w-1.5 rounded-full bg-ctp-sapphire" title={socket.item_name || socket.socket_type} />
            ))}
            {missingGem && (
              <span className="inline-flex items-center gap-1 text-[9px] font-mono" style={{ color: wipeColor }}>
                <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: wipeColor }} />
                missing gem
              </span>
            )}
          </div>
        )}
      </div>
      {item && tooltipPosition && <ItemTooltip item={item} color={qualityColor} position={tooltipPosition} />}
    </div>
  )
}
