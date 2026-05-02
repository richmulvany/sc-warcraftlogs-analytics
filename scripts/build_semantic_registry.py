"""Build the chatbot's semantic registry from gold contracts.

Reads every ``pipeline/contracts/gold/*.yml`` and emits a single JSON file at
``backend/app/semantic_registry.json`` containing only the metadata the
LangChain prompt needs at runtime: table identity, grain, primary key,
descriptions, AI summary, columns, metrics, example questions, join keys, and
``chatbotTier``. Tables marked ``excluded`` are dropped entirely.

Run after editing contracts, or in CI to keep the registry in sync.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from scripts.dashboard_asset_contracts import (
    GoldProductContract,
    load_gold_product_contracts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "backend" / "app" / "semantic_registry.json"


def _serialize_contract(contract: GoldProductContract) -> dict:
    return {
        "table": contract.source_table,
        "model": contract.model_name,
        "version": contract.version,
        "chatbot_tier": contract.chatbot_tier or "secondary",
        "grain": contract.grain,
        "primary_key": list(contract.unique_key),
        "description": contract.info_description or contract.model_description or "",
        "ai_summary": contract.ai_summary,
        "example_questions": list(contract.example_questions),
        "metrics": [dict(m) for m in contract.metrics],
        "join_keys": [dict(j) for j in contract.join_keys],
        "not_recommended_for": list(contract.not_recommended_for),
        "columns": [
            {
                "name": f.name,
                "type": f.type,
                "nullable": f.nullable,
                "description": f.description,
                "enum": list(f.enum) if f.enum else None,
                "unit": f.unit or None,
                "classification": f.classification or None,
            }
            for f in contract.fields.values()
        ],
    }


def build_registry() -> dict:
    contracts = load_gold_product_contracts()
    tables = []
    for table_name in sorted(contracts):
        contract = contracts[table_name]
        if contract.chatbot_tier == "excluded":
            continue
        tables.append(_serialize_contract(contract))
    return {
        "version": 1,
        "catalog": "03_gold",
        "schema": "sc_analytics",
        "tables": tables,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSON path.")
    args = parser.parse_args(argv)

    registry = build_registry()
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")

    primary = sum(1 for t in registry["tables"] if t["chatbot_tier"] == "primary")
    secondary = sum(1 for t in registry["tables"] if t["chatbot_tier"] == "secondary")
    print(
        f"Wrote {out_path.relative_to(REPO_ROOT)} — "
        f"{len(registry['tables'])} tables ({primary} primary, {secondary} secondary)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
