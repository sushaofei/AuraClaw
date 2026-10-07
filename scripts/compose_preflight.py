from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).parents[1]
WORKLOAD_TOKENS = (
    "AURACLAW_TASK_API_WORKLOAD_TOKEN",
    "AURACLAW_PROJECTION_WORKLOAD_TOKEN",
    "AURACLAW_ORCHESTRATOR_WORKLOAD_TOKEN",
    "AURACLAW_RUNTIME_WORKLOAD_TOKEN",
    "AURACLAW_MODEL_GATEWAY_WORKLOAD_TOKEN",
    "AURACLAW_ACTION_HANDS_WORKLOAD_TOKEN",
    "AURACLAW_CREDENTIAL_PROXY_WORKLOAD_TOKEN",
    "AURACLAW_ARTIFACT_SERVICE_WORKLOAD_TOKEN",
    "AURACLAW_POLICY_WORKLOAD_TOKEN",
    "AURACLAW_DELIVERY_WORKLOAD_TOKEN",
    "AURACLAW_STREAMING_GATEWAY_WORKLOAD_TOKEN",
)
SERVICE_DATABASE_URLS = (
    "AURACLAW_TASK_API_DATABASE_URL",
    "AURACLAW_SESSION_DATABASE_URL",
    "AURACLAW_PROJECTION_DATABASE_URL",
    "AURACLAW_ORCHESTRATOR_DATABASE_URL",
    "AURACLAW_MODEL_GATEWAY_DATABASE_URL",
    "AURACLAW_ACTION_HANDS_DATABASE_URL",
    "AURACLAW_POLICY_DATABASE_URL",
    "AURACLAW_CREDENTIAL_PROXY_DATABASE_URL",
    "AURACLAW_ARTIFACT_DATABASE_URL",
    "AURACLAW_STREAMING_DATABASE_URL",
    "AURACLAW_DELIVERY_DATABASE_URL",
)
BASE_REQUIRED = (
    "AURACLAW_IMAGE",
    "AURACLAW_MIGRATION_DATABASE_URL",
    *WORKLOAD_TOKENS,
    "AURACLAW_LEASE_SIGNING_KEY",
    "AURACLAW_MODEL_API_KEY",
    "AURACLAW_MODEL_BASE_URL",
    "AURACLAW_MODEL_NAME",
    "AURACLAW_CREDENTIAL_VAULT_ADDR",
    "AURACLAW_UPSTREAM_WORKLOAD_TOKEN",
    "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON",
    "AURACLAW_ARTIFACT_SCANNER_BASE_URL",
)
SEAWEEDFS_REQUIRED = (
    "SEAWEEDFS_HOST",
    "SEAWEEDFS_ACCESS_KEY",
    "SEAWEEDFS_SECRET_KEY",
)
OBS_REQUIRED = (
    "OBS_ENDPOINT",
    "OBS_BUCKET",
    "OBS_AK",
    "OBS_SK",
    "OBS_REGION",
)
OCI_DIGEST_REFERENCE = re.compile(
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/"
    r"[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$"
)


def is_immutable_image_reference(value: str) -> bool:
    if not OCI_DIGEST_REFERENCE.fullmatch(value):
        return False
    return value.rsplit("sha256:", 1)[1] != "0" * 64


def _compose_file_for_env(env_path: Path) -> Path:
    name = env_path.name
    if name in {".env.test", ".env.test.example"} or name.endswith(".test"):
        return ROOT / "compose.test.yml"
    return ROOT / "compose.prod.yml"


def _resolved_artifact_backend(values: dict[str, str]) -> str:
    backend = values.get("AURACLAW_ARTIFACT_BACKEND", "auto")
    if backend == "local":
        return "local"
    if backend == "obs":
        return "obs"
    if backend == "seaweedfs":
        return "seaweedfs"
    if values.get("OBS_ENDPOINT"):
        return "obs"
    if values.get("SEAWEEDFS_HOST"):
        return "seaweedfs"
    return "obs"


def required_variables(
    values: dict[str, str], *, role_scoped_database: bool | None = None
) -> tuple[str, ...]:
    backend = _resolved_artifact_backend(values)
    if role_scoped_database is None:
        role_scoped_database = any(values.get(name) for name in SERVICE_DATABASE_URLS)
    database_variables = (
        SERVICE_DATABASE_URLS if role_scoped_database else ("AURACLAW_DATABASE_URL",)
    )
    if backend == "obs":
        return (*BASE_REQUIRED, *database_variables, *OBS_REQUIRED)
    if backend == "local":
        return (*BASE_REQUIRED, *database_variables)
    return (*BASE_REQUIRED, *database_variables, *SEAWEEDFS_REQUIRED)


def main() -> int:
    parser = argparse.ArgumentParser(description="validate AuraClaw Compose inputs")
    parser.add_argument("--env-file", default=".env.prod")
    parser.add_argument(
        "--compose-file",
        default=None,
        help="Compose file (default: compose.test.yml for .env.test, else compose.prod.yml)",
    )
    args = parser.parse_args()
    env_path = Path(args.env_file)
    compose_path = (
        Path(args.compose_file) if args.compose_file else _compose_file_for_env(env_path)
    )
    if not env_path.is_file():
        print(f"preflight failed: env file not found: {env_path}")
        return 1
    if not compose_path.is_file():
        print(f"preflight failed: compose file not found: {compose_path}")
        return 1
    file_values = dotenv_values(env_path)
    backend_inputs = {
        name: os.environ.get(name) or file_values.get(name) or ""
        for name in (
            *BASE_REQUIRED,
            *SEAWEEDFS_REQUIRED,
            *OBS_REQUIRED,
            *SERVICE_DATABASE_URLS,
            "AURACLAW_DATABASE_URL",
            "AURACLAW_ARTIFACT_BACKEND",
            "AURACLAW_CREDENTIAL_VAULT_TOKEN",
            "AURACLAW_CREDENTIAL_VAULT_APPROLE_ROLE_ID",
            "AURACLAW_CREDENTIAL_VAULT_APPROLE_SECRET_ID",
        )
    }
    role_scoped_database = compose_path.name == "compose.prod.yml"
    required = required_variables(
        backend_inputs, role_scoped_database=role_scoped_database
    )
    values = {
        name: os.environ.get(name) or file_values.get(name) or "" for name in required
    }
    failures = [f"missing {name}" for name in required if not values[name]]
    vault_token = backend_inputs["AURACLAW_CREDENTIAL_VAULT_TOKEN"]
    vault_role_id = backend_inputs["AURACLAW_CREDENTIAL_VAULT_APPROLE_ROLE_ID"]
    vault_secret_id = backend_inputs["AURACLAW_CREDENTIAL_VAULT_APPROLE_SECRET_ID"]
    if not vault_token and not (vault_role_id and vault_secret_id):
        failures.append(
            "Vault requires AURACLAW_CREDENTIAL_VAULT_TOKEN or a complete AppRole pair"
        )
    if bool(vault_role_id) != bool(vault_secret_id):
        failures.append("Vault AppRole requires both role_id and secret_id")

    image = values["AURACLAW_IMAGE"]
    if image and role_scoped_database and not is_immutable_image_reference(image):
        failures.append(
            "production AURACLAW_IMAGE must use a fully qualified, non-placeholder "
            "image@sha256 digest"
        )
    elif image and not role_scoped_database and (
        image.endswith(":latest") or ":" not in image.split("/")[-1]
    ):
        failures.append("AURACLAW_IMAGE must use a version or SHA tag")

    token_values = [values[name] for name in WORKLOAD_TOKENS if values[name]]
    if any(len(value) < 32 for value in token_values):
        failures.append("workload tokens must contain at least 32 characters")
    if len(token_values) != len(set(token_values)):
        failures.append("workload tokens must be unique per service identity")
    database_values = [values[name] for name in SERVICE_DATABASE_URLS if values.get(name)]
    if role_scoped_database and len(database_values) != len(set(database_values)):
        failures.append("database URLs must use unique least-privilege service accounts")
    lease_key = values["AURACLAW_LEASE_SIGNING_KEY"]
    if lease_key and len(lease_key) < 32:
        failures.append("AURACLAW_LEASE_SIGNING_KEY must contain at least 32 characters")
    upstream_token = values["AURACLAW_UPSTREAM_WORKLOAD_TOKEN"]
    if upstream_token and len(upstream_token) < 32:
        failures.append("AURACLAW_UPSTREAM_WORKLOAD_TOKEN must contain at least 32 characters")
    if upstream_token and upstream_token in token_values:
        failures.append(
            "AURACLAW_UPSTREAM_WORKLOAD_TOKEN must differ from internal service tokens"
        )
    signing_keys = values["AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON"]
    if signing_keys:
        try:
            payload = json.loads(signing_keys)
        except json.JSONDecodeError:
            payload = None
        if not isinstance(payload, dict) or not payload:
            failures.append(
                "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON must be a JSON object of kid to HMAC key"
            )
        elif any(len(str(value).encode()) < 32 for value in payload.values()):
            failures.append("agent context signing keys must contain at least 32 bytes")

    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_path),
            "-f",
            str(compose_path),
            "--profile",
            "migrate",
            "config",
            "--quiet",
        ],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode:
        failures.append("docker compose config validation failed")
    if failures:
        print("Compose preflight failed")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Compose preflight passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
