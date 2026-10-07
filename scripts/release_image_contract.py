from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]
OCI_DIGEST_REFERENCE = re.compile(
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/"
    r"[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$"
)


def is_immutable_image_reference(value: str) -> bool:
    """Return whether value is a canonical, non-placeholder OCI digest reference."""
    if not OCI_DIGEST_REFERENCE.fullmatch(value):
        return False
    return value.rsplit("sha256:", 1)[1] != "0" * 64


def project_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as source:
        return str(tomllib.load(source)["project"]["version"])


def validate_release_tag(tag: str) -> str | None:
    expected = f"v{project_version()}"
    if tag != expected:
        return f"release tag {tag!r} must equal project version tag {expected!r}"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="validate AuraClaw release image identity")
    parser.add_argument("--image", help="fully qualified image@sha256 digest reference")
    parser.add_argument("--tag", help="release tag, which must match pyproject.toml")
    args = parser.parse_args()
    failures: list[str] = []
    if not args.image and not args.tag:
        parser.error("at least one of --image or --tag is required")
    if args.image and not is_immutable_image_reference(args.image):
        failures.append("image must be a fully qualified, non-placeholder OCI sha256 digest")
    if args.tag and (failure := validate_release_tag(args.tag)):
        failures.append(failure)
    if failures:
        for failure in failures:
            print(f"release identity failed: {failure}")
        return 1
    print("release image identity passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
