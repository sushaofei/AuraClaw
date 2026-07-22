import argparse
import asyncio
from collections.abc import Callable, Sequence
from typing import Any

import uvicorn

from auraclaw.composition.services import SERVICE_BY_COMMAND, create_service_app, service_spec
from auraclaw.config import get_settings
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.persistence.postgres_operations_store import PostgresOperationsStore
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.projection.maintenance import ProjectionMaintenanceService
from auraclaw.projection.relay import OutboxRelay


async def _run_projection_command(
    action: str, tenant_id: str | None, *, watch: bool = False, interval: float = 1.0
) -> None:
    settings = get_settings()
    if not settings.postgres_enabled:
        raise SystemExit("projection maintenance requires PostgreSQL storage configuration")
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
    if not settings.postgres_enabled:
        raise SystemExit("operations maintenance requires PostgreSQL storage configuration")
    store = PostgresOperationsStore(settings.resolved_database_url)
    try:
        if action == "status":
            summary = await store.failure_queue_summary(tenant_id)
            print(
                "operations status "
                f"projection_outbox_pending={summary.projection_outbox_pending} "
                f"projection_poison={summary.projection_poison} "
                f"delivery_dlq={summary.delivery_dlq}"
            )
        elif action == "retention":
            deleted = await store.apply_retention()
            retention_summary = " ".join(
                f"{key}={value}" for key, value in deleted.items()
            )
            print(f"operations retention {retention_summary}")
        else:
            if tenant_id is None or queue is None or item_id is None:
                raise SystemExit("redrive requires --tenant, --queue and --item-id")
            if queue == "projection":
                changed = await store.redrive_projection_poison(tenant_id, item_id)
            else:
                changed = await store.redrive_delivery(tenant_id, item_id)
            print(f"operations redrive queue={queue} item_id={item_id} changed={changed}")
    finally:
        await store.close()


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
    projection.add_argument("--interval", type=float, default=1.0)
    projection.add_argument("--host")
    projection.add_argument("--port", type=int)
    operations = subcommands.add_parser("operations")
    operations.add_argument("action", choices=("status", "retention", "redrive"))
    operations.add_argument("--tenant")
    operations.add_argument("--queue", choices=("projection", "delivery"))
    operations.add_argument("--item-id")
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
            uvicorn_runner(
                create_service_app(
                    "projection", settings, worker_interval=args.interval
                ),
                host=args.host or settings.host,
                port=args.port or spec.port,
                log_level=settings.log_level.lower(),
            )
            return
        asyncio.run(
            _run_projection_command(
                args.action, args.tenant, watch=args.watch, interval=args.interval
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
