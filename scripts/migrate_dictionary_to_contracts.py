"""One-shot migrator: port column descriptions from the markdown data dictionary
into the gold contract YAMLs.

Reads ``docs/data_dictionary/README.md``, finds each ``### <table>`` section,
parses the column table (``| `col` | TYPE | description |``), then walks every
``pipeline/contracts/gold/*.yml`` file and inserts a ``description:`` key under
each field block that does not already have one.

This is line-based, not a YAML round-trip, so existing formatting and comments
are preserved. Run once after dictionary updates; the dictionary generator
(``scripts/generate_data_dictionary.py``) is the reverse direction once
contracts are the source of truth.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DICTIONARY_PATH = REPO_ROOT / "docs" / "data_dictionary" / "README.md"
GOLD_CONTRACT_DIR = REPO_ROOT / "pipeline" / "contracts" / "gold"

SECTION_RE = re.compile(r"^###\s+([a-z][a-z0-9_]+)\s*$")
COLUMN_RE = re.compile(r"^\|\s*`([a-zA-Z0-9_]+)`\s*\|\s*[^|]*\|\s*(.*?)\s*\|\s*$")
FIELD_RE = re.compile(r"^      ([a-zA-Z0-9_]+):\s*$")


def parse_dictionary(path: Path = DICTIONARY_PATH) -> dict[str, dict[str, str]]:
    """Return {table_name: {column_name: description}} parsed from the markdown."""

    out: dict[str, dict[str, str]] = {}
    current: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = SECTION_RE.match(line)
        if m:
            current = m.group(1)
            out.setdefault(current, {})
            continue
        if current is None:
            continue
        cm = COLUMN_RE.match(line)
        if cm:
            col, desc = cm.group(1), cm.group(2).strip()
            if desc:
                out[current][col] = desc
    return out


def _yaml_quote(text: str) -> str:
    """Return a double-quoted YAML scalar safe for inline use."""

    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def patch_contract(yaml_path: Path, descriptions: dict[str, str]) -> tuple[int, int]:
    """Insert ``description:`` keys for fields in ``yaml_path``.

    Returns ``(inserted, skipped)`` counts.
    """

    if not descriptions:
        return (0, 0)

    lines = yaml_path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    inserted = 0
    skipped = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        m = FIELD_RE.match(line)
        if not m:
            i += 1
            continue
        field_name = m.group(1)
        # Look ahead within the field block (8-space-indented lines) to see
        # whether description: is already set.
        j = i + 1
        already_has = False
        while j < len(lines):
            nxt = lines[j]
            if nxt.startswith("        "):
                if nxt.lstrip().startswith("description:"):
                    already_has = True
                j += 1
                continue
            break
        if not already_has and field_name in descriptions:
            out.append(f"        description: {_yaml_quote(descriptions[field_name])}")
            inserted += 1
        else:
            skipped += 1
        i += 1

    new_text = "\n".join(out) + "\n"
    if new_text != yaml_path.read_text(encoding="utf-8"):
        yaml_path.write_text(new_text, encoding="utf-8")
    return (inserted, skipped)


def main(argv: list[str] | None = None) -> int:
    dictionary = parse_dictionary()
    print(f"Parsed {len(dictionary)} table sections from {DICTIONARY_PATH.name}")

    total_inserted = 0
    total_skipped = 0
    contracts_touched = 0
    for yaml_path in sorted(GOLD_CONTRACT_DIR.glob("*.yml")):
        # Contract filename matches the table name.
        table = yaml_path.stem
        descs = dictionary.get(table, {})
        if not descs:
            print(f"  - {table}: no dictionary entry")
            continue
        inserted, skipped = patch_contract(yaml_path, descs)
        total_inserted += inserted
        total_skipped += skipped
        if inserted:
            contracts_touched += 1
            print(f"  + {table}: inserted {inserted}, already had {skipped}")
        else:
            print(
                f"  = {table}: nothing to insert ({skipped} fields already documented or unmatched)"
            )
    print(
        f"\nDone. Contracts modified: {contracts_touched}. "
        f"Field descriptions inserted: {total_inserted}. "
        f"Fields untouched: {total_skipped}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
