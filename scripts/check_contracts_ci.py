"""CI bundle for contract governance.

Runs three checks in sequence and reports each. Exits non-zero if any fail.

  1. ``check_contract_drift.py --check`` — schema hashes match the recorded
     fingerprints in ``pipeline/contracts/.hashes.json``. Prevents silent
     schema changes without a version bump.
  2. ``dashboard_asset_contracts.py --validate-descriptions`` — every
     ``chatbotTier: primary`` contract has full semantic metadata
     (description, AI summary, example questions, per-field descriptions).
  3. ``generate_data_dictionary.py --check`` — the human-readable dictionary
     under ``docs/data_dictionary/README.md`` matches what the contracts would
     produce. Run the script without ``--check`` to update.

Designed to run in CI as a single step:

    python scripts/check_contracts_ci.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

CHECKS: list[tuple[str, list[str]]] = [
    ("schema drift", ["python", "scripts/check_contract_drift.py", "--check"]),
    (
        "primary-tier semantic metadata",
        ["python", "scripts/dashboard_asset_contracts.py", "--validate-descriptions"],
    ),
    (
        "data dictionary up to date",
        ["python", "scripts/generate_data_dictionary.py", "--check"],
    ),
]


def main() -> int:
    failures: list[str] = []
    for label, command in CHECKS:
        print(f"\n=== {label} ===", flush=True)
        sys.stdout.flush()
        result = subprocess.run(command, cwd=REPO_ROOT, stdout=sys.stdout, stderr=sys.stderr)
        if result.returncode != 0:
            failures.append(label)

    print()
    if failures:
        print(f"FAILED: {', '.join(failures)}")
        return 1
    print(f"OK: {len(CHECKS)} contract checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
