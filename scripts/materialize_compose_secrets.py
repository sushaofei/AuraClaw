from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import dotenv_values

SECRET_VARIABLES = {
    "database_url": "AURACLAW_DATABASE_URL",
    "task_api_database_url": "AURACLAW_TASK_API_DATABASE_URL",
    "session_database_url": "AURACLAW_SESSION_DATABASE_URL",
    "projection_database_url": "AURACLAW_PROJECTION_DATABASE_URL",
    "orchestrator_database_url": "AURACLAW_ORCHESTRATOR_DATABASE_URL",
    "model_gateway_database_url": "AURACLAW_MODEL_GATEWAY_DATABASE_URL",
    "action_hands_database_url": "AURACLAW_ACTION_HANDS_DATABASE_URL",
    "policy_database_url": "AURACLAW_POLICY_DATABASE_URL",
    "credential_proxy_database_url": "AURACLAW_CREDENTIAL_PROXY_DATABASE_URL",
    "artifact_database_url": "AURACLAW_ARTIFACT_DATABASE_URL",
    "streaming_database_url": "AURACLAW_STREAMING_DATABASE_URL",
    "delivery_database_url": "AURACLAW_DELIVERY_DATABASE_URL",
    "migration_database_url": "AURACLAW_MIGRATION_DATABASE_URL",
    "task_api_workload_token": "AURACLAW_TASK_API_WORKLOAD_TOKEN",
    "projection_workload_token": "AURACLAW_PROJECTION_WORKLOAD_TOKEN",
    "orchestrator_workload_token": "AURACLAW_ORCHESTRATOR_WORKLOAD_TOKEN",
    "runtime_workload_token": "AURACLAW_RUNTIME_WORKLOAD_TOKEN",
    "model_gateway_workload_token": "AURACLAW_MODEL_GATEWAY_WORKLOAD_TOKEN",
    "action_hands_workload_token": "AURACLAW_ACTION_HANDS_WORKLOAD_TOKEN",
    "policy_workload_token": "AURACLAW_POLICY_WORKLOAD_TOKEN",
    "credential_proxy_workload_token": "AURACLAW_CREDENTIAL_PROXY_WORKLOAD_TOKEN",
    "artifact_service_workload_token": "AURACLAW_ARTIFACT_SERVICE_WORKLOAD_TOKEN",
    "delivery_workload_token": "AURACLAW_DELIVERY_WORKLOAD_TOKEN",
    "streaming_gateway_workload_token": "AURACLAW_STREAMING_GATEWAY_WORKLOAD_TOKEN",
    "lease_signing_key": "AURACLAW_LEASE_SIGNING_KEY",
    "model_api_key": "AURACLAW_MODEL_API_KEY",
    "vault_token": "AURACLAW_CREDENTIAL_VAULT_TOKEN",
    "vault_approle_secret_id": "AURACLAW_CREDENTIAL_VAULT_APPROLE_SECRET_ID",
    "obs_ak": "OBS_AK",
    "obs_sk": "OBS_SK",
    "upstream_workload_token": "AURACLAW_UPSTREAM_WORKLOAD_TOKEN",
    "agent_context_signing_keys_json": "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON",
}
OPTIONAL_VARIABLES = {
    "AURACLAW_CREDENTIAL_VAULT_TOKEN",
    "AURACLAW_CREDENTIAL_VAULT_APPROLE_SECRET_ID",
}
ROLE_SCOPED_DATABASE_VARIABLES = {
    variable
    for filename, variable in SECRET_VARIABLES.items()
    if filename.endswith("_database_url") and filename != "migration_database_url"
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="materialize ignored 0600 files for Docker Compose secrets"
    )
    parser.add_argument("--env-file", default=".env.prod")
    parser.add_argument("--output-dir", default=".runtime/compose-secrets")
    args = parser.parse_args()
    env_file = Path(args.env_file)
    if not env_file.is_file():
        print(f"secret materialization failed: env file not found: {env_file}")
        return 1
    configured = dotenv_values(env_file)
    values = {
        variable: os.environ.get(variable) or configured.get(variable) or ""
        for variable in SECRET_VARIABLES.values()
    }
    test_profile = env_file.name in {".env.test", ".env.test.example"} or env_file.name.endswith(
        ".test"
    )
    profile_optional = (
        ROLE_SCOPED_DATABASE_VARIABLES if test_profile else {"AURACLAW_DATABASE_URL"}
    )
    missing = [
        variable
        for variable in SECRET_VARIABLES.values()
        if variable not in OPTIONAL_VARIABLES
        and variable not in profile_optional
        and not values[variable]
    ]
    role_id = os.environ.get("AURACLAW_CREDENTIAL_VAULT_APPROLE_ROLE_ID") or configured.get(
        "AURACLAW_CREDENTIAL_VAULT_APPROLE_ROLE_ID"
    )
    token = values["AURACLAW_CREDENTIAL_VAULT_TOKEN"]
    secret_id = values["AURACLAW_CREDENTIAL_VAULT_APPROLE_SECRET_ID"]
    if not token and not (role_id and secret_id):
        missing.append(
            "AURACLAW_CREDENTIAL_VAULT_TOKEN or complete Vault AppRole credentials"
        )
    if bool(role_id) != bool(secret_id):
        missing.append("complete Vault AppRole role_id + secret_id pair")
    if missing:
        print("secret materialization failed")
        for variable in missing:
            print(f"- missing {variable}")
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    for filename, variable in SECRET_VARIABLES.items():
        target = output_dir / filename
        temporary = output_dir / f".{filename}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            payload = values[variable]
            os.write(descriptor, payload.encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        temporary.replace(target)
        target.chmod(0o600)
    print(f"materialized {len(SECRET_VARIABLES)} Compose secret files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
