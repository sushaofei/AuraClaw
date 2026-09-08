from auraclaw.projection.ports import ProjectionRebuilder
from auraclaw.session.ports import EventStore


class ProjectionMaintenanceService:
    def __init__(self, event_store: EventStore, projector: ProjectionRebuilder) -> None:
        self._event_store = event_store
        self._projector = projector

    async def rebuild_tasks(self, tenant_id: str | None = None) -> int:
        events = await self._event_store.load_all(tenant_id)
        return await self._projector.rebuild(events, tenant_id)
