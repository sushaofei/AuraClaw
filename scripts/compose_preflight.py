from __future__ import annotations

import argparse
import json
import os
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
BASE_REQUIRED = (
    "AURACLAW_IMAGE",
    "AURACLAW_DATABASE_URL",
    "AURACLAW_MIGRATION_DATABASE_URL",
    *WORKLOAD_TOKENS,
    "AURACLAW_LEASE_SIGNING_KEY",
    "AURACLAW_MODEL_API_KEY",
    "AURACLAW_MODEL_BASE_URL",
    "AURACLAW_MODEL_NAME",
    "AURACLAW_CREDENTIAL_VAULT_ADDR",
    "AURACLAW_CREDENTIAL_VAULT_TOKEN",
    "AURACLAW_CHAINTOWER_WORKLOAD_TOKEN",
    "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON",
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


def required_variables(values: dict[str, str]) -> tuple[str, ...]:
    backend = _resolved_artifact_backend(values)
    if backend == "obs":
        return (*BASE_REQUIRED, *OBS_REQUIRED)
    if backend == "local":
        return BASE_REQUIRED
    return (*BASE_REQUIRED, *SEAWEEDFS_REQUIRED)


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
            "AURACLAW_ARTIFACT_BACKEND",
        )
    }
    required = required_variables(backend_inputs)
    values = {
        name: os.environ.get(name) or file_values.get(name) or "" for name in required
    }
    failures = [f"missing {name}" for name in required if not values[name]]

    image = values["AURACLAW_IMAGE"]
    if image and (
        image.endswith(":latest")
        or "replace-with-immutable-sha" in image
        or ":" not in image.split("/")[-1]
    ):
        failures.append("AURACLAW_IMAGE must use an immutable digest or version/SHA tag")

    token_values = [values[name] for name in WORKLOAD_TOKENS if values[name]]
    if any(len(value) < 32 for value in token_values):
        failures.append("workload tokens must contain at least 32 characters")
    if len(token_values) != len(set(token_values)):
        failures.append("workload tokens must be unique per service identity")
    lease_key = values["AURACLAW_LEASE_SIGNING_KEY"]
    if lease_key and len(lease_key) < 32:
        failures.append("AURACLAW_LEASE_SIGNING_KEY must contain at least 32 characters")
    chaintower_token = values["AURACLAW_CHAINTOWER_WORKLOAD_TOKEN"]
    if chaintower_token and len(chaintower_token) < 32:
        failures.append("AURACLAW_CHAINTOWER_WORKLOAD_TOKEN must contain at least 32 characters")
    if chaintower_token and chaintower_token in token_values:
        failures.append(
            "AURACLAW_CHAINTOWER_WORKLOAD_TOKEN must differ from internal service tokens"
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
