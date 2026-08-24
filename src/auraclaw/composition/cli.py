import argparse
import asyncio
import multiprocessing
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import uvicorn

from auraclaw.composition.local_ingress import (
    create_local_ingress_app,
    loopback_connect_host,
)
from auraclaw.composition.services import SERVICE_BY_COMMAND, create_service_app, service_spec
from auraclaw.config import Settings, get_settings
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
    if not settings.sql_storage_enabled and not settings.kafka_enabled:
        raise ValueError(
            "auraclaw serve requires shared SQL storage or Kafka for cross-process "
            "runtime event streaming"
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
