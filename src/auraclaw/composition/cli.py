import argparse
import asyncio
import base64
import binascii
import hashlib
import json
import multiprocessing
import os
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from auraclaw.action.skill_packages import (
    DefaultSkillPackageContentScanner,
    Ed25519SkillSignatureVerifier,
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    SkillSignatureVerifier,
    skill_package_archive,
    skill_package_digest,
    skill_signing_payload,
    validate_skill_test_vectors,
)
from auraclaw.composition.local_ingress import (
    create_local_ingress_app,
    loopback_connect_host,
)
from auraclaw.composition.services import SERVICE_BY_COMMAND, create_service_app, service_spec
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.errors import SkillContentRejectedError
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.skills import SkillManifest
from auraclaw.infrastructure.clients.admin import RemoteAdminClient
from auraclaw.infrastructure.persistence.migration_runner import (
    create_migration_runner,
    default_migrations_directory,
)
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.projection.maintenance import ProjectionMaintenanceService
from auraclaw.projection.relay import OutboxRelay


async def _run_projection_command(
    action: str, tenant_id: str | None, *, watch: bool = False, interval: float = 1.0
) -> None:
    settings = get_settings()
    token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
    if token:
        client = RemoteAdminClient(settings.projection_base_url, bearer_token=token)
        try:
            response = await client.execute(
                ServiceIdentity.PROJECTION_WORKER,
                action,
                {"tenant_id": tenant_id} if tenant_id else {},
                tenant_id=tenant_id or "system",
            )
            print(f"projection {action} status={response.status} result={response.result}")
        finally:
            await client.aclose()
        return
    if not settings.sql_storage_enabled:
        raise SystemExit("projection maintenance requires SQL storage configuration")
    event_store = PostgresEventStore(settings.resolved_database_url)
    projector = PostgresTaskProjection(settings.resolved_database_url)
    try:
        if action == "rebuild":
            count = await ProjectionMaintenanceService(event_store, projector).rebuild_tasks(
                tenant_id
            )
        else:
            relay = OutboxRelay(event_store, projector)
            count = 0
            while True:
                processed = await relay.relay_once(limit=100)
                count += processed
                if not watch:
                    break
                await asyncio.sleep(interval if processed == 0 else 0)
        print(f"projection {action} processed {count} event(s)")
    finally:
        await event_store.close()
        await projector.close()


async def _run_operations_command(
    action: str,
    tenant_id: str | None,
    *,
    queue: str | None = None,
    item_id: str | None = None,
) -> None:
    settings = get_settings()
    token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
    if not token:
        raise SystemExit("operations admin requires Task API workload identity")
    projection = RemoteAdminClient(settings.projection_base_url, bearer_token=token)
    delivery = RemoteAdminClient(settings.delivery_base_url, bearer_token=token)
    artifact = RemoteAdminClient(settings.artifact_base_url, bearer_token=token)
    try:
        if action == "status":
            projection_status = await projection.execute(
                ServiceIdentity.PROJECTION_WORKER,
                "status",
                {"tenant_id": tenant_id} if tenant_id else {},
                tenant_id=tenant_id or "system",
            )
            print(f"operations status projection={projection_status.result}")
        elif action == "retention":
            response = await artifact.execute(
                ServiceIdentity.ARTIFACT_SERVICE,
                "retention",
                {},
                tenant_id=tenant_id or "system",
            )
            print(f"operations retention status={response.status} result={response.result}")
        else:
            if tenant_id is None or queue is None or item_id is None:
                raise SystemExit("redrive requires --tenant, --queue and --item-id")
            if queue == "projection":
                response = await projection.execute(
                    ServiceIdentity.PROJECTION_WORKER,
                    "redrive",
                    {"tenant_id": tenant_id, "event_id": item_id},
                    tenant_id=tenant_id,
                )
            else:
                response = await delivery.execute(
                    ServiceIdentity.DELIVERY_WORKER,
                    "redrive",
                    {"tenant_id": tenant_id, "delivery_id": item_id},
                    tenant_id=tenant_id,
                )
            print(
                f"operations redrive queue={queue} item_id={item_id} "
                f"status={response.status} result={response.result}"
            )
    finally:
        await projection.aclose()
        await delivery.aclose()
        await artifact.aclose()


async def _run_migration_command(
    action: str,
    *,
    target: str | None,
    directory: str | None,
    confirm_existing_schema: bool = False,
) -> None:
    settings = get_settings()
    migration_dir = Path(directory) if directory else default_migrations_directory(
        settings.resolved_db_dialect if settings.sql_storage_enabled else settings.db_dialect
    )
    runner = create_migration_runner(
        settings.resolved_migration_database_url,
        migration_dir,
    )
    if action == "status":
        for item in await runner.status():
            print(f"{item.version} {item.state} {item.name} sha256={item.checksum[:12]}")
        return
    if action == "baseline":
        if target is None or not confirm_existing_schema:
            raise SystemExit(
                "baseline requires --target and --confirm-existing-schema"
            )
        baselined = await runner.baseline(target)
        print(f"migrations baselined={len(baselined)} target={target}")
        return
    applied = await runner.apply(target)
    print(f"migrations applied={len(applied)}")
    for name in applied:
        print(name)


class _ValidationArtifactWriter:
    async def put(self, **kwargs: object) -> Any:
        del kwargs
        raise RuntimeError("validation must not write Artifacts")


def _read_skill_directory(directory: str) -> tuple[Path, dict[str, bytes]]:
    root = Path(directory).resolve()
    if not root.is_dir():
        raise SystemExit(f"Skill directory does not exist: {directory}")
    files: dict[str, bytes] = {}
    total_size = 0
    for item in sorted(root.rglob("*")):
        if item.is_symlink():
            raise SystemExit(f"Skill directory contains a symlink: {item.relative_to(root)}")
        if not item.is_file():
            continue
        relative = item.relative_to(root).as_posix()
        files[relative] = item.read_bytes()
        total_size += len(files[relative])
        if len(files) > 512 or total_size > 16 * 1024 * 1024:
            raise SystemExit("Skill package exceeds local validation limits")
    return root, files


def _load_skill_directory(directory: str) -> SkillPackage:
    _root, files = _read_skill_directory(directory)
    return SkillPackage.from_files(files)


def _validate_local_skill(
    package: SkillPackage,
    settings: Settings,
    *,
    external_public_key: bytes | None = None,
) -> SkillPackage:
    verifier: SkillSignatureVerifier
    if package.manifest.publisher == "platform":
        signing_key = (
            settings.skill_signing_key.get_secret_value().encode()
            if settings.skill_signing_key is not None
            else b"auraclaw-development-platform-skill-key"
        )
        verifier = HmacSkillSignatureVerifier(
            {package.manifest.publisher: signing_key}
        )
    else:
        key_id = package.manifest.signature_key_id
        if external_public_key is None or key_id is None:
            raise SystemExit(
                "External Skill validation requires signature_key_id and public key"
            )
        verifier = Ed25519SkillSignatureVerifier(
            {(package.manifest.publisher, key_id): external_public_key}
        )
    registry = SkillPackageRegistry(
        artifacts=_ValidationArtifactWriter(),
        signature_verifier=verifier,
    )
    validated = registry.validate(package)
    findings = DefaultSkillPackageContentScanner().scan(validated)
    if findings:
        raise SkillContentRejectedError(findings[0])
    return validated


def _decode_ed25519_key(value: str, *, kind: str) -> bytes:
    if not value or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in value
    ):
        raise SystemExit(f"Ed25519 {kind} key is not valid base64url")
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error) as exc:
        raise SystemExit(f"Ed25519 {kind} key is not valid base64url") from exc
    if len(decoded) != 32:
        raise SystemExit(f"Ed25519 {kind} key must contain exactly 32 bytes")
    return decoded


def _key_from_environment(variable: str, *, kind: str) -> bytes:
    value = os.environ.get(variable)
    if not value:
        raise SystemExit(f"Ed25519 {kind} key is required in {variable}")
    return _decode_ed25519_key(value, kind=kind)


def _sign_external_skill_directory(
    directory: str,
    *,
    publisher: str,
    key_id: str,
    private_key: bytes,
) -> tuple[SkillPackage, str]:
    if publisher == "platform":
        raise SystemExit("External signing cannot claim the platform publisher")
    root, files = _read_skill_directory(directory)
    manifest_content = files.get("manifest.json")
    if manifest_content is None:
        raise SystemExit("Skill package is missing manifest.json")
    try:
        raw_manifest = json.loads(manifest_content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit("Skill manifest is invalid JSON") from exc
    if not isinstance(raw_manifest, dict):
        raise SystemExit("Skill manifest must be a JSON object")
    if raw_manifest.get("publisher") != publisher:
        raise SystemExit("--publisher must match manifest publisher")
    raw_manifest["signature_key_id"] = key_id
    raw_manifest["signature"] = "ed25519:unsigned"
    try:
        unsigned_manifest = SkillManifest.model_validate(raw_manifest)
        signing_key = Ed25519PrivateKey.from_private_bytes(private_key)
    except ValueError as exc:
        raise SystemExit("Skill manifest or Ed25519 private key is invalid") from exc
    unsigned = SkillPackage(manifest=unsigned_manifest, files=files)
    signature = signing_key.sign(skill_signing_payload(unsigned))
    encoded_signature = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    signed_manifest = unsigned_manifest.model_copy(
        update={"signature": f"ed25519:{encoded_signature}"}
    )
    signed_files = {
        **files,
        "manifest.json": signed_manifest.model_dump_json().encode(),
    }
    package = SkillPackage.from_files(signed_files)
    public_key = signing_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    _validate_local_skill(
        package,
        Settings(_env_file=None),
        external_public_key=public_key,
    )
    target = root / "manifest.json"
    temporary = root / f".manifest.json.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(signed_files["manifest.json"])
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    encoded_public_key = base64.urlsafe_b64encode(public_key).rstrip(b"=").decode()
    return package, encoded_public_key


def _identity_headers(
    *, tenant_id: str, actor_id: str, token: str | None, command_id: str | None = None
) -> dict[str, str]:
    headers = {
        "X-Tenant-ID": tenant_id,
        "X-Actor-ID": actor_id,
        "X-Correlation-ID": command_id or f"skill-cli-{uuid.uuid4().hex}",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if command_id:
        headers["Idempotency-Key"] = command_id
    return headers


def _skill_subcommand_id(command_id: str, operation: str) -> str:
    digest = hashlib.sha256(f"{command_id}:{operation}".encode()).hexdigest()
    return f"skill-cli-{operation}-{digest}"


async def _publish_skill_archive(
    *,
    client: httpx.AsyncClient,
    package: SkillPackage,
    tenant_id: str,
    actor_id: str,
    publisher: str,
    source_id: str,
    activate: bool,
    expected_revision: int,
    command_id: str,
    token: str | None,
) -> dict[str, Any]:
    if publisher != package.manifest.publisher:
        raise SystemExit("--publisher must match manifest publisher")
    archive = skill_package_archive(package)
    checksum = hashlib.sha256(archive).hexdigest()
    headers = _identity_headers(
        tenant_id=tenant_id,
        actor_id=actor_id,
        token=token,
        command_id=command_id,
    )
    create = await client.post(
        "/v1/admin/skill-package-uploads",
        headers={
            **headers,
            "Idempotency-Key": _skill_subcommand_id(command_id, "upload"),
        },
        json={
            "name": (
                f"{package.manifest.publisher}.{package.manifest.name}-"
                f"{package.manifest.version}.skill.json"
            ),
            "expected_size": len(archive),
            "expected_checksum": checksum,
        },
    )
    _require_cli_success(create, "create staged upload")
    upload = create.json()
    parts: list[dict[str, object]] = []
    if upload.get("upload_mode") == "multipart":
        part_size = int(upload["part_size"])
        for number, url in enumerate(upload.get("part_urls", ()), start=1):
            offset = (number - 1) * part_size
            response = await client.put(
                str(url),
                content=archive[offset : offset + part_size],
                headers={"Content-Type": "application/vnd.auraclaw.skill-package+json"},
            )
            _require_cli_success(response, f"upload part {number}")
            etag = response.headers.get("ETag")
            if not etag:
                raise SystemExit(f"upload part {number} did not return ETag")
            parts.append({"part_number": number, "etag": etag})
    else:
        response = await client.put(
            str(upload["upload_url"]),
            content=archive,
            headers={"Content-Type": "application/vnd.auraclaw.skill-package+json"},
        )
        _require_cli_success(response, "upload Skill package")
    finalized = await client.post(
        f"/v1/admin/skill-package-uploads/{upload['artifact_id']}:finalize",
        headers={
            **headers,
            "Idempotency-Key": _skill_subcommand_id(command_id, "finalize"),
        },
        json={
            "upload_id": upload["upload_id"],
            "version": upload["version"],
            "size": len(archive),
            "checksum": checksum,
            "parts": parts,
        },
    )
    _require_cli_success(finalized, "finalize staged upload")
    artifact_ref = finalized.json()["artifact_ref"]
    published = await client.post(
        "/v1/admin/skill-publications",
        headers={**headers, "X-Expected-Revision": str(expected_revision)},
        json={
            "source_id": source_id,
            "activate": activate,
            "artifact_ref": artifact_ref,
            "expected_digest": skill_package_digest(package),
        },
    )
    _require_cli_success(published, "publish Skill package")
    return dict(published.json())


def _require_cli_success(response: httpx.Response, operation: str) -> None:
    if response.is_error:
        raise SystemExit(f"Unable to {operation}: HTTP {response.status_code}")


async def _run_skills_command(args: argparse.Namespace) -> None:
    settings = get_settings()
    if args.action == "sign":
        private_key = _key_from_environment(args.private_key_env, kind="private")
        package, public_key = _sign_external_skill_directory(
            args.directory,
            publisher=args.publisher,
            key_id=args.key_id,
            private_key=private_key,
        )
        print(
            json.dumps(
                {
                    "publisher": package.manifest.publisher,
                    "name": package.manifest.name,
                    "version": package.manifest.version,
                    "signature_key_id": package.manifest.signature_key_id,
                    "public_key": public_key,
                    "package_digest": skill_package_digest(package),
                },
                sort_keys=True,
            )
        )
        return
    package = _load_skill_directory(args.directory)
    external_public_key = None
    if package.manifest.publisher != "platform":
        external_public_key = _key_from_environment(
            args.public_key_env, kind="public"
        )
    package = _validate_local_skill(
        package,
        settings,
        external_public_key=external_public_key,
    )
    if args.action == "validate":
        print(
            json.dumps(
                {
                    "publisher": package.manifest.publisher,
                    "name": package.manifest.name,
                    "version": package.manifest.version,
                    "package_digest": skill_package_digest(package),
                },
                sort_keys=True,
            )
        )
        return
    if args.action == "test":
        print(json.dumps({"declarative_test_vectors": validate_skill_test_vectors(package)}))
        return
    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(f"Skill publication requires bearer token in {args.token_env}")
    api_url = args.api_url or f"http://127.0.0.1:{settings.task_api_port}"
    command_id = args.command_id or f"skill-publish-{uuid.uuid4().hex}"
    async with httpx.AsyncClient(base_url=api_url, timeout=120.0) as client:
        result = await _publish_skill_archive(
            client=client,
            package=package,
            tenant_id=args.tenant,
            actor_id=args.actor,
            publisher=args.publisher,
            source_id=args.source,
            activate=not args.staged,
            expected_revision=args.expected_revision,
            command_id=command_id,
            token=token,
        )
    print(json.dumps(result, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="auraclaw")
    subcommands = parser.add_subparsers(dest="command")
    serve = subcommands.add_parser(
        "serve",
        help=(
            "run the production-isomorphic 12-process topology plus a local ingress "
            "that splits /v1/streams/ to Streaming Gateway"
        ),
    )
    serve.add_argument("--host")
    projection = subcommands.add_parser("projection")
    projection.add_argument("action", choices=("relay", "rebuild"))
    projection.add_argument("--tenant")
    projection.add_argument("--watch", action="store_true")
    projection.add_argument("--interval", type=float, default=None)
    projection.add_argument("--host")
    projection.add_argument("--port", type=int)
    operations = subcommands.add_parser("operations")
    operations.add_argument("action", choices=("status", "retention", "redrive"))
    operations.add_argument("--tenant")
    operations.add_argument("--queue", choices=("projection", "delivery"))
    operations.add_argument("--item-id")
    migrate = subcommands.add_parser("migrate")
    migrate.add_argument("action", choices=("status", "up", "baseline"))
    migrate.add_argument("--target")
    migrate.add_argument(
        "--directory",
        default=None,
        help="Migration directory (default: migrations/ or migrations/mysql/ by dialect)",
    )
    migrate.add_argument("--confirm-existing-schema", action="store_true")
    skills = subcommands.add_parser("skills")
    skill_commands = skills.add_subparsers(dest="action", required=True)
    for action in ("validate", "test"):
        skill_command_parser = skill_commands.add_parser(action)
        skill_command_parser.add_argument("directory")
        skill_command_parser.add_argument(
            "--public-key-env", default="AURACLAW_SKILL_PUBLIC_KEY"
        )
    sign = skill_commands.add_parser("sign")
    sign.add_argument("directory")
    sign.add_argument("--publisher", required=True)
    sign.add_argument("--key-id", required=True)
    sign.add_argument(
        "--private-key-env", default="AURACLAW_SKILL_SIGNING_KEY"
    )
    publish = skill_commands.add_parser("publish")
    publish.add_argument("directory")
    publish.add_argument("--tenant", required=True)
    publish.add_argument("--publisher", required=True)
    publish.add_argument("--actor", default="skill-cli")
    publish.add_argument("--source", default="sks_admin_upload")
    publish.add_argument("--api-url")
    publish.add_argument("--token-env", default="AURACLAW_API_TOKEN")
    publish.add_argument(
        "--public-key-env", default="AURACLAW_SKILL_PUBLIC_KEY"
    )
    publish.add_argument("--command-id")
    publish.add_argument("--expected-revision", type=int, default=0)
    publish.add_argument("--staged", action="store_true")
    for command in SERVICE_BY_COMMAND:
        if command == "projection":
            continue
        service = subcommands.add_parser(
            command,
            help="production process entrypoint (compose / auraclaw serve)",
        )
        service.add_argument("action", choices=("run",))
        service.add_argument("--host")
        service.add_argument("--port", type=int)
    return parser


def _run_service_process(
    command: str,
    host: str,
    port: int,
    log_level: str,
    worker_interval: float | None,
) -> None:
    settings = get_settings()
    app = (
        create_service_app(command, settings, worker_interval=worker_interval)
        if command == "projection" and worker_interval is not None
        else create_service_app(command, settings)
    )
    uvicorn.run(app, host=host, port=port, log_level=log_level)


def _run_ingress_process(
    host: str,
    port: int,
    task_api_base_url: str,
    streaming_base_url: str,
    log_level: str,
) -> None:
    app = create_local_ingress_app(
        task_api_base_url=task_api_base_url,
        streaming_base_url=streaming_base_url,
    )
    uvicorn.run(app, host=host, port=port, log_level=log_level)


def _serve_topology(settings: Settings, *, host: str) -> None:
    if not settings.sql_storage_enabled:
        raise ValueError(
            "auraclaw serve requires SQL storage (postgres, mysql, or kingbase) so "
            "MCP registry, session facts and projections survive restarts. Configure "
            "AURACLAW_STORAGE_BACKEND and DB_* credentials in .env.dev."
        )
    if not settings.kafka_enabled:
        raise ValueError(
            "auraclaw serve requires Kafka for cross-process runtime event streaming. "
            "Configure KAFKA_HOST in .env.dev."
        )
    processes: list[multiprocessing.Process] = []
    for command in SERVICE_BY_COMMAND:
        spec = service_spec(command, settings)
        worker_interval = None
        if command == "projection":
            worker_interval = (
                settings.worker_idle_interval
                if settings.worker_wake_enabled
                else settings.projection_worker_interval
            )
        process = multiprocessing.Process(
            target=_run_service_process,
            args=(
                command,
                host,
                spec.port,
                settings.log_level.lower(),
                worker_interval,
            ),
            name=spec.name,
        )
        process.start()
        processes.append(process)
    if settings.ingress_enabled:
        connect_host = loopback_connect_host(host)
        ingress = multiprocessing.Process(
            target=_run_ingress_process,
            args=(
                host,
                settings.ingress_port,
                f"http://{connect_host}:{settings.task_api_port}",
                f"http://{connect_host}:{settings.streaming_port}",
                settings.log_level.lower(),
            ),
            name="local-ingress",
        )
        ingress.start()
        processes.append(ingress)
    try:
        for process in processes:
            process.join()
            if process.exitcode not in {0, None}:
                raise SystemExit(process.exitcode or 1)
    except KeyboardInterrupt:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join(timeout=5)


def main(
    argv: Sequence[str] | None = None,
    *,
    uvicorn_runner: Callable[..., Any] = uvicorn.run,
    serve_runner: Callable[..., Any] | None = None,
) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.error("specify a command, for example: auraclaw serve")
    if args.command == "serve":
        settings = get_settings()
        runner = serve_runner or _serve_topology
        runner(settings, host=args.host or settings.host)
        return
    if args.command == "projection":
        if args.action == "relay" and args.watch:
            settings = get_settings()
            if settings.deployment_profile != "production":
                raise SystemExit(
                    "Projection worker watch mode is reserved for production "
                    "compose. Use `auraclaw serve` for local development."
                )
            spec = service_spec("projection", settings)
            interval = (
                args.interval
                if args.interval is not None
                else (
                    settings.worker_idle_interval
                    if settings.worker_wake_enabled
                    else settings.projection_worker_interval
                )
            )
            uvicorn_runner(
                create_service_app(
                    "projection", settings, worker_interval=interval
                ),
                host=args.host or settings.host,
                port=args.port or spec.port,
                log_level=settings.log_level.lower(),
            )
            return
        settings = get_settings()
        interval = (
            args.interval
            if args.interval is not None
            else (
                settings.worker_idle_interval
                if settings.worker_wake_enabled
                else settings.projection_worker_interval
            )
        )
        asyncio.run(
            _run_projection_command(
                args.action, args.tenant, watch=args.watch, interval=interval
            )
        )
        return
    if args.command == "operations":
        asyncio.run(
            _run_operations_command(
                args.action,
                args.tenant,
                queue=args.queue,
                item_id=args.item_id,
            )
        )
        return
    if args.command == "migrate":
        asyncio.run(
            _run_migration_command(
                args.action,
                target=args.target,
                directory=args.directory,
                confirm_existing_schema=args.confirm_existing_schema,
            )
        )
        return
    if args.command == "skills":
        asyncio.run(_run_skills_command(args))
        return
    if args.command in SERVICE_BY_COMMAND and args.command != "projection":
        settings = get_settings()
        if settings.deployment_profile != "production":
            raise SystemExit(
                "Single-process service entrypoints are reserved for production "
                "compose. Use `auraclaw serve` for local development."
            )
        spec = service_spec(args.command, settings)
        uvicorn_runner(
            create_service_app(args.command, settings),
            host=args.host or settings.host,
            port=args.port or spec.port,
            log_level=settings.log_level.lower(),
        )
        return
    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
