import argparse
import asyncio

import uvicorn

from auraclaw.application.maintenance import ProjectionMaintenanceService
from auraclaw.config import get_settings
from auraclaw.infrastructure.operations import PostgresOperationsStore
from auraclaw.infrastructure.postgres import PostgresEventStore, PostgresTaskProjection
from auraclaw.projections.relay import OutboxRelay


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


def main() -> None:
    parser = argparse.ArgumentParser(prog="auraclaw")
    subcommands = parser.add_subparsers(dest="command")
    subcommands.add_parser("serve")
    projection = subcommands.add_parser("projection")
    projection.add_argument("action", choices=("relay", "rebuild"))
    projection.add_argument("--tenant")
    projection.add_argument("--watch", action="store_true")
    projection.add_argument("--interval", type=float, default=1.0)
    operations = subcommands.add_parser("operations")
    operations.add_argument("action", choices=("status", "retention", "redrive"))
    operations.add_argument("--tenant")
    operations.add_argument("--queue", choices=("projection", "delivery"))
    operations.add_argument("--item-id")
    args = parser.parse_args()
    if args.command == "projection":
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
    settings = get_settings()
    uvicorn.run(
        "auraclaw.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
