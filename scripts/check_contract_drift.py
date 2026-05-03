"""Detect schema drift in gold contracts.

For each contract, compute a stable hash of the model name plus the sorted list
of ``(field_name, type, nullable)`` triples. The hashes are stored in
``pipeline/contracts/.hashes.json``. Running with ``--check`` exits non-zero if
any contract's hash differs from the stored value (i.e. someone changed a
schema without updating the recorded hash and presumably without bumping
``info.version``). ``--update`` rewrites the hash file.

CI usage::

    python scripts/check_contract_drift.py --check

Local usage when intentionally changing a schema::

    python scripts/check_contract_drift.py --update
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from scripts.dashboard_asset_contracts import (
    GoldProductContract,
    load_gold_product_contracts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
HASHES_PATH = REPO_ROOT / "pipeline" / "contracts" / ".hashes.json"


def contract_hash(contract: GoldProductContract) -> str:
    payload = {
        "model": contract.model_name,
        "version": contract.version,
        "fields": sorted((name, f.type, f.nullable) for name, f in contract.fields.items()),
        "primaryKey": list(contract.unique_key),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def compute_all_hashes() -> dict[str, str]:
    return {name: contract_hash(c) for name, c in load_gold_product_contracts().items()}


def load_stored_hashes() -> dict[str, str]:
    if not HASHES_PATH.exists():
        return {}
    return json.loads(HASHES_PATH.read_text(encoding="utf-8"))


def write_hashes(hashes: dict[str, str]) -> None:
    HASHES_PATH.write_text(json.dumps(hashes, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="Compare current to stored.")
    group.add_argument("--update", action="store_true", help="Rewrite the hash file.")
    args = parser.parse_args(argv)

    current = compute_all_hashes()

    if args.update:
        write_hashes(current)
        print(f"Wrote {len(current)} hashes to {HASHES_PATH.relative_to(REPO_ROOT)}")
        return 0

    stored = load_stored_hashes()
    added = sorted(set(current) - set(stored))
    removed = sorted(set(stored) - set(current))
    changed = sorted(name for name in set(current) & set(stored) if current[name] != stored[name])
    if not (added or removed or changed):
        print(f"No drift across {len(current)} contracts.")
        return 0
    if added:
        print("Added contracts (run --update to record):")
        for name in added:
            print(f"  + {name}  hash={current[name]}")
    if removed:
        print("Removed contracts (run --update to record):")
        for name in removed:
            print(f"  - {name}  was hash={stored[name]}")
    if changed:
        print("Changed contracts — bump info.version and run --update:")
        for name in changed:
            print(f"  ~ {name}  was {stored[name]} now {current[name]}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
