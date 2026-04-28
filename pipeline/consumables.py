"""Shared consumable classification helpers for raid-preparation products."""

from __future__ import annotations

import re
from collections.abc import Iterable

MIDNIGHT_FLASK_OR_PHIAL_NAMES = {
    "flask of thalassian resistance",
    "flask of the blood knights",
    "flask of the magisters",
    "flask of the shattered sun",
    "vicious thalassian flask of honor",
    "haranir phial of perception",
    "haranir phial of ingenuity",
    "haranir phial of finesse",
}
MIDNIGHT_FLASK_OR_PHIAL_IDS: tuple[int, ...] = ()

MIDNIGHT_AUGMENT_RUNE_NAMES = {
    "void-touched augment rune",
}

MIDNIGHT_WEAPON_ENHANCEMENT_NAMES = {
    "thalassian phoenix oil",
    "smugglers enchanted edge",
    "oil of dawn",
    "refulgent weightstone",
    "refulgent whetstone",
    "refulgent razorstone",
    "laced zoomshots",
    "weighted boomshots",
    "smugglers lynxeye",
    "farstriders hawkeye",
    "flametongue weapon",
    "windfury weapon",
    "earthliving weapon",
}
MIDNIGHT_WEAPON_ENHANCEMENT_IDS: tuple[int, ...] = ()

MIDNIGHT_COMBAT_POTION_NAMES = {
    "lights potential",
    "potion of recklessness",
    "potion of zealotry",
    "draught of rampant abandon",
}
MIDNIGHT_COMBAT_POTION_IDS = (
    1236616,  # Light's Potential buff
    1236994,  # Potion of Recklessness buff
    1238443,  # Potion of Zealotry buff
    1237154,  # Draught of Rampant Abandon buff
)

MIDNIGHT_FOOD_NAMES = {
    "silvermoon parade",
    "harandar celebration",
    "queldorei medley",
    "blooming feast",
    "royal roast",
    "impossibly royal roast",
    "flora frenzy",
    "champions bento",
    "warped wise wings",
    "void-kissed fish rolls",
    "sun-seared lumifin",
    "null and void plate",
    "glitter skewers",
    "fel-kissed filet",
    "buttered root crab",
    "arcano cutlets",
    "tasty smoked tetra",
    "crimson calamari",
    "braised blood hunter",
    "sunwell delight",
    "hearthflame supper",
    "fried bloomtail",
    "felberry figs",
    "eversong pudding",
    "bloodthistle-wrapped cutlets",
    "wise tails",
    "twilight anglers medley",
    "spellfire filet",
    "spiced biscuits",
    "silvermoon standard",
    "quick sandwich",
    "portable snack",
    "mana-infused stew",
    "forager's medley",
    "farstrider rations",
    "bloom skewers",
}
MIDNIGHT_FOOD_IDS: tuple[int, ...] = ()

_SPACE_RE = re.compile(r"\s+")
_APOSTROPHE_RE = re.compile(r"[']")


def _normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    normalized = _APOSTROPHE_RE.sub("", value.strip().lower())
    return _SPACE_RE.sub(" ", normalized)


def _unique_preserve(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _match_food(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    if normalized in MIDNIGHT_FOOD_NAMES:
        return True
    return "well fed" in normalized or "feast" in normalized


def _match_flask_or_phial(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    if normalized in MIDNIGHT_FLASK_OR_PHIAL_NAMES:
        return True
    return "flask" in normalized or "phial" in normalized


def _match_augment_rune(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    return normalized in MIDNIGHT_AUGMENT_RUNE_NAMES or "augment rune" in normalized


def _match_weapon_enhancement(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    if normalized in MIDNIGHT_WEAPON_ENHANCEMENT_NAMES:
        return True
    weapon_keywords = (
        " oil",
        "oil ",
        "whetstone",
        "weightstone",
        "razorstone",
        "shots",
        "enchanted edge",
        "lynxeye",
        "hawkeye",
    )
    return any(keyword in normalized for keyword in weapon_keywords)


def _match_combat_potion(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    return normalized in MIDNIGHT_COMBAT_POTION_NAMES


def _classify(names: Iterable[str] | None, matcher) -> list[str]:
    if not names:
        return []
    matched = []
    for name in names:
        trimmed = (name or "").strip()
        if trimmed and matcher(trimmed):
            matched.append(trimmed)
    return _unique_preserve(matched)


def classify_food_names(names: Iterable[str] | None) -> list[str]:
    return _classify(names, _match_food)


def classify_flask_or_phial_names(names: Iterable[str] | None) -> list[str]:
    return _classify(names, _match_flask_or_phial)


def classify_augment_rune_names(names: Iterable[str] | None) -> list[str]:
    return _classify(names, _match_augment_rune)


def classify_weapon_enhancement_names(names: Iterable[str] | None) -> list[str]:
    return _classify(names, _match_weapon_enhancement)


def classify_combat_potion_names(names: Iterable[str] | None) -> list[str]:
    return _classify(names, _match_combat_potion)


def join_consumable_names(names: Iterable[str] | None) -> str | None:
    cleaned = _unique_preserve(
        (name or "").strip()
        for name in (names or [])
        if (name or "").strip()
    )
    return " | ".join(cleaned) if cleaned else None


def merge_consumable_name_strings(*parts: str | None) -> str | None:
    names: list[str] = []
    for part in parts:
        if not part:
            continue
        names.extend(piece.strip() for piece in part.split("|"))
    return join_consumable_names(names)
