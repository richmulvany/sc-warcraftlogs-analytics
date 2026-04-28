"""WarcraftLogs rankings completeness helpers."""

from __future__ import annotations

import json

RANKINGS_BACKFILL_MAX_AGE_DAYS = 14

# A fight is incomplete if more than this fraction of its characters have a
# null rankPercent. Tolerates a small number of genuinely unrankable rows
# (exotic off-spec, very recent spec) without thrashing the refetch loop.
RANKINGS_INCOMPLETE_NULL_FRACTION = 0.10


def rankings_completeness(path: str) -> tuple[int, int, int, int]:
    """Return (incomplete_fights, total_fights, null_chars, total_chars).

    A fight is considered incomplete if the fraction of characters with null
    `rankPercent` exceeds RANKINGS_INCOMPLETE_NULL_FRACTION across the
    role-appropriate payload — DPS payload for tanks/dps buckets, HPS payload
    for the healers bucket. This catches both the "rankings still computing"
    state (all-null) and the partial-null state where most players ranked but
    a subset are still pending.

    A landing file that pre-dates the dual-metric fetch (no `rankings_hps_json`
    field) is treated as incomplete so the ingestion job re-fetches and
    populates the HPS payload.

    Returns (0, 0, 0, 0) if the file cannot be parsed.
    """
    try:
        with open(path) as fh:
            record = json.loads(fh.readline())
        dps_payload = json.loads(record.get("rankings_json") or "null")
        hps_raw = record.get("rankings_hps_json")
    except (OSError, ValueError):
        return (0, 0, 0, 0)

    if hps_raw is None:
        fights = (dps_payload or {}).get("data") or []
        return (len(fights), len(fights), 0, 0)

    try:
        hps_payload = json.loads(hps_raw)
    except ValueError:
        return (0, 0, 0, 0)

    dps_fights = (dps_payload or {}).get("data") or []
    hps_fights = (hps_payload or {}).get("data") or []
    hps_by_id = {fight.get("fightID"): fight for fight in hps_fights if isinstance(fight, dict)}

    incomplete = 0
    null_chars_total = 0
    chars_total = 0
    for fight in dps_fights:
        dps_roles = (fight or {}).get("roles") or {}
        hps_roles = (hps_by_id.get((fight or {}).get("fightID")) or {}).get("roles") or {}
        fight_chars = 0
        fight_nulls = 0
        for role_key in ("tanks", "dps"):
            for character in (dps_roles.get(role_key) or {}).get("characters") or []:
                fight_chars += 1
                if character.get("rankPercent") is None:
                    fight_nulls += 1
        for character in (hps_roles.get("healers") or {}).get("characters") or []:
            fight_chars += 1
            if character.get("rankPercent") is None:
                fight_nulls += 1
        chars_total += fight_chars
        null_chars_total += fight_nulls
        if fight_chars > 0 and (fight_nulls / fight_chars) > RANKINGS_INCOMPLETE_NULL_FRACTION:
            incomplete += 1
    return (incomplete, len(dps_fights), null_chars_total, chars_total)
