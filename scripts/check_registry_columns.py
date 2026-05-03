"""Compare each registry table's declared columns to the columns Databricks
actually exposes. Reports any phantom columns (in the registry but not in the
table) and any unclaimed columns (in the table but not in the registry).
Phantom columns are dangerous — the LLM may emit them and Databricks will
reject the SQL.

Requires DATABRICKS_HOST + DATABRICKS_TOKEN. Skips silently if Databricks
credentials are missing so the check can be safely included in offline CI.
Run with ``--strict`` to fail when credentials are absent.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from backend.app.config import get_settings  # noqa: E402
from backend.app.db import execute_select  # noqa: E402
from backend.app.semantic_registry import load_registry  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if Databricks credentials are missing (otherwise skip).",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    if not (settings.databricks_host and settings.databricks_token):
        msg = "Databricks credentials missing — skipping column integrity check."
        if args.strict:
            print(msg, file=sys.stderr)
            return 2
        print(msg)
        return 0

    load_registry.cache_clear()
    registry = load_registry()
    bad = 0
    for info in registry.tables.values():
        try:
            r = execute_select(f"DESCRIBE TABLE {info.table}")
        except Exception as exc:
            print(f"!! {info.table}: cannot describe ({exc})")
            bad += 1
            continue
        actual = {row[0] for row in r.rows if row[0] and not row[0].startswith("#")}
        declared = {c["name"] for c in info.columns}
        phantom = sorted(declared - actual)
        unclaimed = sorted(actual - declared)
        flag = "" if not phantom else "  <-- PHANTOM"
        if phantom or unclaimed:
            print(f"\n[{info.chatbot_tier}] {info.table}{flag}")
            if phantom:
                print(f"  phantom (in contract, not in table): {phantom}")
            if unclaimed:
                print(f"  unclaimed (in table, not in contract): {unclaimed}")
        if phantom:
            bad += 1
    if bad:
        print(f"\n{bad} tables had phantom columns.")
        return 1
    print("\nAll registry columns exist in Databricks.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
