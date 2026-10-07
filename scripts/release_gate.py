from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml
from dotenv import dotenv_values

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
    ROOT / "uv.lock",
    ROOT / "migrations/0007_m6_observability_reliability.sql",
    ROOT / "migrations/0007_m6_observability_reliability.down.sql",
    ROOT / "docs/development/stage-gates.md",
    ROOT / "docs/operations/observability-and-canary.md",
    ROOT / "compose.prod.yml",
    ROOT / "compose.test.yml",
    ROOT / "docs/operations/production-deployment.md",
)
CURRENT_RELEASE_DOCS = (
    ROOT / "docs/operations/production-deployment.md",
    ROOT / "docs/operations/release.md",
    ROOT / "docs/operations/dev-service-deployment.md",
)
DATABASE_SECRET_BY_SERVICE = {
    "task-api": "task_api_database_url",
    "session": "session_database_url",
    "projection-worker": "projection_database_url",
    "orchestrator": "orchestrator_database_url",
    "model-gateway": "model_gateway_database_url",
    "action-hands": "action_hands_database_url",
    "policy": "policy_database_url",
    "credential-proxy": "credential_proxy_database_url",
    "artifact-service": "artifact_database_url",
    "streaming-gateway": "streaming_database_url",
    "delivery-worker": "delivery_database_url",
}


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    seen: set[Any] = set()
    for key_node, _ in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            continue
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise ValueError(f"duplicate YAML key {key!r} at line {key_node.start_mark.line + 1}")
        seen.add(key)
    loader.flatten_mapping(node)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _latest_migration() -> str:
    versions = sorted(
        path.name.split("_", 1)[0]
        for path in (ROOT / "migrations").glob("[0-9][0-9][0-9][0-9]_*.sql")
        if not path.name.endswith(".down.sql")
    )
    if not versions:
        raise ValueError("no schema migrations found")
    return versions[-1]


def _check_release_truth(failures: list[str]) -> None:
    latest = _latest_migration()
    for path in CURRENT_RELEASE_DOCS:
        content = path.read_text()
        if f"当前目标为 `{latest}`" not in content and f"当前目标 `{latest}`" not in content:
            if f"当前迁移基线为 {latest}" not in content:
                failures.append(
                    f"current migration {latest} is missing from {path.relative_to(ROOT)}"
                )
    for path in (ROOT / "compose.prod.yml", ROOT / ".env.prod.example"):
        if latest not in path.read_text():
            failures.append(
                f"current migration {latest} is missing from {path.relative_to(ROOT)}"
            )

    canonical_text = "\n".join(
        path.read_text()
        for path in (
            ROOT / "README.md",
            ROOT / "docs/architecture/system/00 Managed Agent 系统架构总览.md",
            ROOT / "docs/architecture/system/04 Read Model Store.md",
            ROOT / "docs/architecture/decisions/ADR-001-production-service-boundaries.md",
        )
    )
    if re.search(r"\bMVP\b|first vertical slice", canonical_text, re.IGNORECASE):
        failures.append("canonical product documentation still contains MVP terminology")


def _check_production_compose(failures: list[str]) -> None:
    try:
        compose = yaml.load((ROOT / "compose.prod.yml").read_text(), Loader=_UniqueKeyLoader)
    except (yaml.YAMLError, ValueError) as exc:
        failures.append(f"invalid production Compose YAML: {exc}")
        return
    services = compose.get("services", {})
    secrets = compose.get("secrets", {})
    for service_name, secret_name in DATABASE_SECRET_BY_SERVICE.items():
        service = services.get(service_name, {})
        environment = service.get("environment", {})
        mounted = service.get("secrets", [])
        if environment.get("AURACLAW_DATABASE_URL_FILE") != f"/run/secrets/{secret_name}":
            failures.append(f"{service_name} does not use its role-scoped database secret")
        if secret_name not in mounted or secret_name not in secrets:
            failures.append(f"{service_name} database secret {secret_name} is not mounted")
        role = service.get("labels", {}).get("auraclaw.database-role")
        if not role:
            failures.append(f"{service_name} is missing auraclaw.database-role")
    if "database_url" in secrets:
        failures.append("production Compose still defines a shared database_url secret")
    image_template = (ROOT / ".env.prod.example").read_text()
    placeholder_digest = "sha256:" + "0" * 64
    if f"AURACLAW_IMAGE=ghcr.io/sushaofei/auraclaw@{placeholder_digest}" not in image_template:
        failures.append("production env template does not require an OCI digest reference")
    if f"@{placeholder_digest}" not in (ROOT / "compose.prod.yml").read_text():
        failures.append("production Compose image default is not a fail-closed digest placeholder")
    runtime_environment = services.get("agent-runtime", {}).get("environment", {})
    if runtime_environment.get("AURACLAW_RUNTIME_EVENT_BACKEND") == "memory":
        failures.append("agent-runtime production runtime events cannot use memory")

    production_env = dotenv_values(ROOT / ".env.prod.example")
    database_variables = [
        f"AURACLAW_{secret_name.upper()}" for secret_name in DATABASE_SECRET_BY_SERVICE.values()
    ]
    database_urls = [production_env.get(name) for name in database_variables]
    if not all(database_urls):
        failures.append("production env template is missing role-scoped database URLs")
    elif len(database_urls) != len(set(database_urls)):
        failures.append("production env template reuses a database URL across services")


def _check_supply_chain(failures: list[str]) -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    required_fragments = (
        "COPY pyproject.toml uv.lock README.md ./",
        "uv sync --locked --no-dev --no-editable",
        "python:3.13-slim@sha256:",
        "ghcr.io/astral-sh/uv:0.11.3@sha256:",
        "/usr/local/lib/python3.13/ensurepip",
        "/usr/local/lib/python3.13/site-packages/pip-*.dist-info",
        "USER auraclaw",
    )
    for fragment in required_fragments:
        if fragment not in dockerfile:
            failures.append(f"Dockerfile is missing reproducible build contract: {fragment}")
    if "pip install" in dockerfile:
        failures.append("Dockerfile bypasses uv.lock with pip install")

    workflow = (ROOT / ".github/workflows/release-gate.yml").read_text()
    for required in (
        "cyclonedx1.5",
        "pip-audit==",
        "trivy-all.json",
        "trivy-gate.sarif",
        "upload-artifact@",
    ):
        if required not in workflow:
            failures.append(f"release workflow is missing supply-chain gate: {required}")
    for required in (
        "prod-like-integration:",
        "postgres:17.6-alpine@sha256:",
        "apache/kafka:4.0.0@sha256:",
        "chrislusf/seaweedfs:3.85@sha256:",
        "scripts/prod_like_gate.py verify-junit",
        "--minimum-tests 8",
    ):
        if required not in workflow:
            failures.append(f"release workflow is missing prod-like gate: {required}")
    release_image_workflow = ROOT / ".github/workflows/release-image.yml"
    if not release_image_workflow.is_file():
        failures.append("release image publication workflow is missing")
    else:
        release_image = release_image_workflow.read_text()
        for required in (
            "scripts/release_image_contract.py --tag",
            "docker/login-action@",
            "actions/attest@",
            "subject-digest:",
            "sbom-path: artifacts/auraclaw.cdx.json",
            "gh attestation verify",
            "packages: write",
            "id-token: write",
            "attestations: write",
        ):
            if required not in release_image:
                failures.append(f"release image workflow is missing contract: {required}")
    for workflow_path in (ROOT / ".github/workflows").glob("*.yml"):
        workflow_content = workflow_path.read_text()
        try:
            yaml.load(workflow_content, Loader=_UniqueKeyLoader)
        except (yaml.YAMLError, ValueError) as exc:
            failures.append(f"invalid workflow YAML {workflow_path.name}: {exc}")
        mutable_action = re.search(
            r"uses:\s+[^\s]+@(v?\d+(?:\.\d+){0,2})\s*(?:#.*)?$",
            workflow_content,
            re.M,
        )
        if mutable_action:
            failures.append(
                f"{workflow_path.name} uses mutable action tag: {mutable_action.group(0)}"
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
    _check_release_truth(failures)
    _check_production_compose(failures)
    _check_supply_chain(failures)
    if failures:
        print("release gate failed")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("release gate passed: artifacts, architecture boundaries, and secret scan")
    return 0


if __name__ == "__main__":
    sys.exit(main())
