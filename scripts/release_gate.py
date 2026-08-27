from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
SCAN_ROOTS = (ROOT / "src", ROOT / "migrations", ROOT / "deploy")
SCAN_FILES = (
    ROOT / "compose.prod.yml",
    ROOT / "compose.test.yml",
    ROOT / ".env.dev.example",
    ROOT / ".env.test.example",
    ROOT / ".env.prod.example",
)
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "OpenAI-style token": re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    "unredacted bearer token": re.compile(r"(?i)bearer\s+(?!\[REDACTED\])[a-z0-9._~+/=-]{16,}"),
}
REQUIRED = (
    ROOT / "migrations/0007_m6_observability_reliability.sql",
    ROOT / "migrations/0007_m6_observability_reliability.down.sql",
    ROOT / "docs/development/stage-gates.md",
    ROOT / "docs/operations/observability-and-canary.md",
    ROOT / "compose.prod.yml",
    ROOT / "compose.test.yml",
    ROOT / "docs/operations/production-deployment.md",
)


def main() -> int:
    failures: list[str] = []
    for path in REQUIRED:
        if not path.is_file():
            failures.append(f"missing release artifact: {path.relative_to(ROOT)}")
    for scan_root in SCAN_ROOTS:
        for path in scan_root.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sql"}:
                continue
            content = path.read_text(errors="replace")
            for name, pattern in SECRET_PATTERNS.items():
                if pattern.search(content):
                    failures.append(f"{name} found in {path.relative_to(ROOT)}")
    for path in SCAN_FILES:
        content = path.read_text(errors="replace")
        for name, pattern in SECRET_PATTERNS.items():
            if pattern.search(content):
                failures.append(f"{name} found in {path.relative_to(ROOT)}")
    contracts_and_domain = tuple(
        (ROOT / "src/auraclaw" / name).rglob("*.py")
        for name in ("contracts", "domain")
    )
    for group in contracts_and_domain:
        for path in group:
            content = path.read_text()
            if "from fastapi" in content or "import fastapi" in content or "asyncpg" in content:
                failures.append(f"architecture boundary violation: {path.relative_to(ROOT)}")
    if failures:
        print("release gate failed")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("release gate passed: artifacts, architecture boundaries, and secret scan")
    return 0


if __name__ == "__main__":
    sys.exit(main())
