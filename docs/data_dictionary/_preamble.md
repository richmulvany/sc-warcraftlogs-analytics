## Conventions and freshness

All gold tables are produced by the DLT pipeline and are the source of truth
for the frontend dashboard. They are refreshed nightly after ingestion.

### Player identity

Player-keyed gold and dashboard products use `player_identity_key` as the
relational character key. The key is deterministic and lowercase:

- `player_name:player_class:realm` when class is available (most WCL-derived products)
- `player_name:unknown:realm_slug` for Raider.IO and Blizzard profile products that do not carry class
- legacy realmless rows fall back to the explicit `unknown` sentinel

`dim_player.player_name` is sufficient for joining facts that only carry a
character name; use `player_identity_key` for joins that need to disambiguate
across realms or class changes.

### Mythic+ caveat

Raider.IO data is current-season only. Score history (`gold_player_mplus_score_history`)
begins at the first successful Raider.IO ingestion — it is **not** true
season-start history unless ingestion began at season start. Trend charts
should be clear that they show captured snapshots only.

### Throughput

`throughput_per_second` on `fact_player_fight_performance` and the `*_throughput_per_second`
fields on player summary products come from WCL `rankings.amount`. The value is
already DPS for dps/tank rows or HPS for healer rows — do not recompute from
`playerDetails`. The field is null when WCL has no ranking entry for the
fight/player; do not bucket null as zero.
