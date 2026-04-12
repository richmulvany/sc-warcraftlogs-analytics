"""
Health check script — verifies the project is correctly configured.
Run with: make verify  or  python scripts/verify_setup.py
"""

import subprocess
import sys
from pathlib import Path


def check(label: str, passed: bool, hint: str = "") -> bool:
    icon = "[OK]  " if passed else "[FAIL]"
    print(f"  {icon}  {label}")
    if not passed and hint:
        print(f"          Hint: {hint}")
    return passed


def main() -> None:
    print("\nRunning setup verification...\n")
    results = []

    results.append(
        check(
            ".env file exists",
            Path(".env").exists(),
            "Run: cp .env.example .env  then fill in your values",
        )
    )

    cli_ok = subprocess.run(["databricks", "auth", "test"], capture_output=True).returncode == 0
    results.append(
        check(
            "Databricks CLI connected",
            cli_ok,
            "Run: databricks configure --token",
        )
    )

    results.append(
        check(
            "Sample data present",
            Path("data/samples").exists() and any(Path("data/samples").iterdir()),
            "Add mock API response JSON files to data/samples/",
        )
    )

    results.append(
        check(
            "Frontend dependencies installed",
            Path("frontend/node_modules").exists(),
            "Run: cd frontend && npm install",
        )
    )

    try:
        import httpx  # noqa: F401
        import pydantic  # noqa: F401
        import tenacity  # noqa: F401

        py_ok = True
    except ImportError:
        py_ok = False
    results.append(
        check(
            "Python dependencies installed",
            py_ok,
            "Run: pip install -e '.[dev]'",
        )
    )

    print()
    passed = sum(results)
    print(f"  {passed}/{len(results)} checks passed.\n")
    if passed < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
