from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATE = ROOT / "scripts/prod_like_gate.py"


def _write_report(path: Path, *, tests: int, skipped: int = 0) -> None:
    path.write_text(
        f'<testsuites><testsuite tests="{tests}" failures="0" '
        f'errors="0" skipped="{skipped}" /></testsuites>'
    )


def test_prod_like_report_requires_minimum_without_skips(tmp_path: Path) -> None:
    report = tmp_path / "junit.xml"
    _write_report(report, tests=8)
    accepted = subprocess.run(
        [
            sys.executable,
            str(GATE),
            "verify-junit",
            "--report",
            str(report),
            "--minimum-tests",
            "8",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr

    _write_report(report, tests=8, skipped=1)
    skipped = subprocess.run(
        [
            sys.executable,
            str(GATE),
            "verify-junit",
            "--report",
            str(report),
            "--minimum-tests",
            "8",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert skipped.returncode == 1
    assert "skipped=1" in skipped.stdout


def test_prod_like_report_rejects_missing_coverage(tmp_path: Path) -> None:
    report = tmp_path / "junit.xml"
    _write_report(report, tests=7)
    result = subprocess.run(
        [
            sys.executable,
            str(GATE),
            "verify-junit",
            "--report",
            str(report),
            "--minimum-tests",
            "8",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 1
    assert "expected at least 8" in result.stdout
