"""Create private, isolated development evaluation configuration; never print credentials."""

import argparse
import io
import json
import secrets
import tarfile
from pathlib import Path

from dotenv import dotenv_values

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--model-env", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
parser.add_argument("--image", required=True)
parser.add_argument("--nginx-image", required=True)
args = parser.parse_args()
if not args.image.strip() or "@sha256:" not in args.nginx_image:
    parser.error("Require an application image and a digest-pinned Nginx image")
root = Path(__file__).resolve().parents[2]
out = args.output_dir.resolve()
if out.is_relative_to(root) and not out.is_relative_to(root / ".runtime"):
    parser.error("Use an ignored .runtime directory or a private path outside the checkout")
out.mkdir(parents=True, mode=0o700, exist_ok=True)
out.chmod(0o700)
state = out / "deployment-credentials.json"
if state.exists():
    d = json.loads(state.read_text())
else:
    d = {
        "postgres_password": secrets.token_urlsafe(36),
        "s3_access": secrets.token_urlsafe(24),
        "s3_secret": secrets.token_urlsafe(36),
    }
    d["tokens"] = {
        k: secrets.token_urlsafe(36)
        for k in [
            "task_api",
            "projection",
            "orchestrator",
            "runtime",
            "model_gateway",
            "action_hands",
            "policy",
            "credential_proxy",
            "artifact_service",
            "delivery",
            "streaming_gateway",
            "lease",
            "upstream",
        ]
    }
    d["signing_keys"] = {"e2": secrets.token_urlsafe(48)}
    state.write_text(json.dumps(d))
    state.chmod(0o600)
# Preserve existing deployment secrets when changing to one shared application login.
if "app_password" not in d:
    d["app_password"] = secrets.token_urlsafe(36)
    state.write_text(json.dumps(d))
    state.chmod(0o600)
p = dotenv_values(args.model_env)
env = {
    "AURACLAW_IMAGE": args.image,
    "AURACLAW_EVAL_NGINX_IMAGE": args.nginx_image,
    "AURACLAW_DEPLOYMENT_PROFILE": "development",
    "AURACLAW_STORAGE_BACKEND": "postgres",
    "AURACLAW_SECRET_DIR": ".runtime/compose-secrets",
    "AURACLAW_MIGRATE_TARGET": "0071",
    "AURACLAW_INTERNAL_NETWORK": "auraclaw-e2-internal",
    "AURACLAW_EDGE_NETWORK": "auraclaw-e2-edge",
    "AURACLAW_PLATFORM_NETWORK": "auraclaw-e2-platform",
    "AURACLAW_OBSERVABILITY_OTLP_HTTP_ENDPOINT": "http://unused.invalid",
    "AURACLAW_ALERT_RECEIVER_URL": "http://unused.invalid",
    "AURACLAW_ARTIFACT_SCANNER_BASE_URL": "http://unused.invalid",
    "AURACLAW_ARTIFACT_BACKEND": "seaweedfs",
    "KAFKA_HOST": "kafka",
    "KAFKA_PORT": "9092",
    "AURACLAW_CREDENTIAL_VAULT_ADDR": "",
    "AURACLAW_CREDENTIAL_VAULT_TOKEN": secrets.token_urlsafe(32),
    "OBS_AK": d["s3_access"],
    "OBS_SK": d["s3_secret"],
    "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON": json.dumps(
        d["signing_keys"], separators=(",", ":")
    ),
    "AURACLAW_DATABASE_URL": f"postgresql+asyncpg://auraclaw_app:{d['app_password']}@postgres:5432/auraclaw",
}
env["AURACLAW_MIGRATION_DATABASE_URL"] = (
    f"postgresql+asyncpg://auraclaw_migration:{d['postgres_password']}@postgres:5432/auraclaw"
)
for k in [
    "AURACLAW_MODEL_API_KEY",
    "AURACLAW_MODEL_BASE_URL",
    "AURACLAW_MODEL_NAME",
    "AURACLAW_MODEL_PROVIDER",
]:
    assert p.get(k), k
    env[k] = p[k]
for k, v in d["tokens"].items():
    name = "AURACLAW_" + (
        {"lease": "LEASE_SIGNING_KEY", "upstream": "UPSTREAM_WORKLOAD_TOKEN"}.get(
            k, k.upper() + "_WORKLOAD_TOKEN"
        )
    )
    env[name] = v
# Existing Compose mounts the same database_url secret into every SQL service.
# No role-specific DSNs or Compose overlay are needed for this deployment.
s3 = {
    "identities": [
        {
            "name": "auraclaw-eval",
            "credentials": [{"accessKey": d["s3_access"], "secretKey": d["s3_secret"]}],
            "actions": ["Admin", "Read", "Write", "List", "Tagging"],
        }
    ]
}
sql = "ALTER ROLE auraclaw_app PASSWORD '" + d["app_password"] + "';\n"
files = {
    ".env.test": "\n".join(k + "=" + v for k, v in env.items()) + "\n",
    ".runtime/compose-secrets/eval_postgres_password": d["postgres_password"],
    ".runtime/compose-secrets/eval_s3_config": json.dumps(s3),
    ".runtime/app-password.sql": sql,
}
with tarfile.open(out / "deployment-config.tar", "w") as tar:
    for name, data in files.items():
        raw = data.encode()
        info = tarfile.TarInfo(name)
        info.size = len(raw)
        info.mode = 0o600
        info.uid = 10001 if "compose-secrets/" in name else 0
        info.gid = 10001 if info.uid else 0
        tar.addfile(info, io.BytesIO(raw))
(out / "deployment-config.tar").chmod(0o600)
print("Prepared private deployment configuration; no credentials printed.")
