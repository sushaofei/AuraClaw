import argparse
import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import uvicorn

from auraclaw.composition.services import SERVICE_BY_COMMAND, create_service_app, service_spec
from auraclaw.config import get_settings
from auraclaw.contracts.internal import ServiceIdentity
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
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
        if not token:
            raise SystemExit("projection admin requires Task API workload identity")
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
    if (
        settings.deployment_profile == "production"
        and settings.migration_database_url is None
    ):
        raise SystemExit(
            "production migration requires AURACLAW_MIGRATION_DATABASE_URL"
        )
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="auraclaw")
    subcommands = parser.add_subparsers(dest="command")
    serve = subcommands.add_parser("serve", help="development combined profile")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
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
    for command in SERVICE_BY_COMMAND:
        if command == "projection":
            continue
        service = subcommands.add_parser(command)
        service.add_argument("action", choices=("run",))
        service.add_argument("--host")
        service.add_argument("--port", type=int)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    uvicorn_runner: Callable[..., Any] = uvicorn.run,
) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "projection":
        if args.action == "relay" and args.watch:
            settings = get_settings()
            spec = service_spec("projection", settings)
            interval = (
                args.interval
                if args.interval is not None
                else settings.projection_worker_interval
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
            else settings.projection_worker_interval
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
    if args.command in SERVICE_BY_COMMAND and args.command != "projection":
        settings = get_settings()
        spec = service_spec(args.command, settings)
        uvicorn_runner(
            create_service_app(args.command, settings),
            host=args.host or settings.host,
            port=args.port or spec.port,
            log_level=settings.log_level.lower(),
        )
        return
    settings = get_settings()
    uvicorn_runner(
        "auraclaw.main:app",
        host=getattr(args, "host", None) or settings.host,
        port=getattr(args, "port", None) or settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
