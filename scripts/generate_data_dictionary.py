"""Regenerate the human-readable data dictionary from gold contracts.

After ``migrate_dictionary_to_contracts.py`` ports descriptions into the YAML
contracts, the contracts are the source of truth and this script re-emits
``docs/data_dictionary/README.md`` from them. Tables with ``chatbotTier:
excluded`` are still emitted (the dictionary is for humans, not just the bot).

Output groups tables by tier (primary first, then secondary, then excluded) and
within each tier alphabetises table names. Each section shows table identity,
grain, AI summary, primary key, and a column table.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.dashboard_asset_contracts import (
    GoldProductContract,
    load_gold_product_contracts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "data_dictionary" / "README.md"
PREAMBLE_PATH = REPO_ROOT / "docs" / "data_dictionary" / "_preamble.md"
APPENDIX_PATH = REPO_ROOT / "docs" / "data_dictionary" / "_appendix.md"


def _relative(path: Path) -> Path | str:
    try:
        return path.relative_to(REPO_ROOT)
    except ValueError:
        return path


TIER_ORDER = ("primary", "secondary", "(unset)", "excluded")
TIER_HEADINGS = {
    "primary": "Primary tier (chatbot-facing)",
    "secondary": "Secondary tier",
    "(unset)": "Unclassified",
    "excluded": "Excluded from chatbot",
}


def _format_field_row(field) -> str:
    desc = field.description or ""
    if field.enum:
        desc = (
            desc + (" " if desc else "") + f"Allowed: {', '.join(map(str, field.enum))}."
        ).strip()
    if field.unit:
        desc = (desc + (" " if desc else "") + f"Unit: `{field.unit}`.").strip()
    if field.classification and field.classification != "public":
        desc = (desc + (" " if desc else "") + f"Sensitivity: {field.classification}.").strip()
    if field.nullable:
        desc = (desc + (" " if desc else "") + "(nullable)").strip()
    return f"| `{field.name}` | {field.type} | {desc} |"


def _format_table_section(contract: GoldProductContract) -> str:
    lines: list[str] = []
    lines.append(f"### {contract.model_name}")
    lines.append("")
    if contract.info_description or contract.model_description:
        lines.append(contract.info_description or contract.model_description)
        lines.append("")
    if contract.grain:
        lines.append(f"**Grain**: {contract.grain}")
        lines.append("")
    if contract.unique_key:
        lines.append(f"**Primary key**: `{', '.join(contract.unique_key)}`")
        lines.append("")
    if contract.ai_summary:
        lines.append(f"**Summary**: {contract.ai_summary}")
        lines.append("")
    lines.append("| Column | Type | Description |")
    lines.append("|--------|------|-------------|")
    for field in contract.fields.values():
        lines.append(_format_field_row(field))
    if contract.metrics:
        lines.append("")
        lines.append("**Metrics**:")
        for m in contract.metrics:
            name = m.get("name", "?")
            definition = m.get("definition") or m.get("formula") or ""
            lines.append(f"- `{name}` — {definition}")
    if contract.example_questions:
        lines.append("")
        lines.append("**Example questions**:")
        for q in contract.example_questions:
            lines.append(f"- {q}")
    if contract.not_recommended_for:
        lines.append("")
        lines.append("**Avoid using for**:")
        for item in contract.not_recommended_for:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def render_dictionary() -> str:
    contracts = load_gold_product_contracts()
    by_tier: dict[str, list[GoldProductContract]] = {tier: [] for tier in TIER_ORDER}
    for name in sorted(contracts):
        contract = contracts[name]
        tier = contract.chatbot_tier or "(unset)"
        by_tier.setdefault(tier, []).append(contract)

    out: list[str] = []
    out.append("# Data Dictionary — Gold Layer")
    out.append("")
    out.append(
        "Generated from `pipeline/contracts/gold/*.yml` by "
        "`scripts/generate_data_dictionary.py`. Do not edit by hand — update the "
        "contracts and re-run the generator."
    )
    out.append("")
    out.append("**Unity Catalog path**: `03_gold.sc_analytics.<table_name>`")
    out.append("")
    out.append("---")
    out.append("")

    if PREAMBLE_PATH.exists():
        out.append(PREAMBLE_PATH.read_text(encoding="utf-8").rstrip())
        out.append("")
        out.append("---")
        out.append("")

    for tier in TIER_ORDER:
        contracts_in_tier = by_tier.get(tier) or []
        if not contracts_in_tier:
            continue
        out.append(f"## {TIER_HEADINGS[tier]}")
        out.append("")
        for contract in contracts_in_tier:
            out.append(_format_table_section(contract))

    if APPENDIX_PATH.exists():
        out.append(APPENDIX_PATH.read_text(encoding="utf-8").rstrip())
        out.append("")

    return "\n".join(out).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the file on disk would change.",
    )
    args = parser.parse_args(argv)

    rendered = render_dictionary()
    out_path = Path(args.output)
    if args.check:
        existing = out_path.read_text(encoding="utf-8") if out_path.exists() else ""
        if existing != rendered:
            print(f"{_relative(out_path)} is out of date — re-run without --check.")
            return 1
        print(f"{_relative(out_path)} is up to date.")
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(rendered, encoding="utf-8")
    print(f"Wrote {_relative(out_path)} ({len(rendered.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
