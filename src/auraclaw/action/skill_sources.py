from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Protocol

from auraclaw.action.skill_lifecycle import (
    SkillLifecycleStore,
    SkillSourceConfigCommit,
)
from auraclaw.contracts.errors import NotFoundError, PolicyDeniedError
from auraclaw.contracts.skills import (
    ConfigureSkillSourceCommand,
    RetireSkillSourceCommand,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)


class SkillSourceSynchronizer(Protocol):
    async def reconcile_source(
        self, tenant_id: str, source_id: str
    ) -> object: ...


class SkillSourceProjector(Protocol):
    async def rebuild_tenant(self, tenant_id: str) -> object: ...


class SkillSourceService:
    def __init__(
        self,
        lifecycle: SkillLifecycleStore,
        *,
        synchronizer: SkillSourceSynchronizer | None = None,
        projector: SkillSourceProjector | None = None,
    ) -> None:
        self._lifecycle = lifecycle
        self._synchronizer = synchronizer
        self._projector = projector

    async def list_sources(self, tenant_id: str) -> tuple[SkillSourceRecord, ...]:
        return await self._lifecycle.list_sources(tenant_id)

    async def get_source(self, tenant_id: str, source_id: str) -> SkillSourceRecord:
        source = await self._lifecycle.get_source(tenant_id, source_id)
        if source is None:
            raise NotFoundError("Skill Source not found")
        return source

    async def get_source_sync_state(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceSyncState | None:
        await self.get_source(tenant_id, source_id)
        return await self._lifecycle.get_sync_state(tenant_id, source_id)

    async def configure(
        self, command: ConfigureSkillSourceCommand
    ) -> SkillSourceRecord:
        current = await self._lifecycle.get_source(
            command.tenant_id, command.source_id
        )
        if current is not None and current.kind is not command.kind:
            raise PolicyDeniedError("Skill Source kind is immutable")
        now = datetime.now(UTC)
        record = SkillSourceRecord(
            source_id=command.source_id,
            tenant_id=command.tenant_id,
            kind=command.kind,
            desired_state=command.desired_state,
            publisher_allowlist=command.publisher_allowlist,
            credential_ref=command.credential_ref,
            config_metadata=command.config_metadata,
            priority=command.priority,
            revision=1 if current is None else current.revision + 1,
            created_by=command.actor_id if current is None else current.created_by,
            updated_by=command.actor_id,
            created_at=now if current is None else current.created_at,
            updated_at=now,
        )
        result = await self._lifecycle.commit_source_config(
            SkillSourceConfigCommit(
                command_id=command.command_id,
                request_digest=_command_digest(command.model_dump(mode="json")),
                operation="configure",
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                reason_code=None,
                expected_revision=command.expected_revision,
                source=record,
                occurred_at=now,
            )
        )
        if self._projector is not None:
            await self._projector.rebuild_tenant(command.tenant_id)
        return result

    async def configure_source(
        self, command: ConfigureSkillSourceCommand
    ) -> SkillSourceRecord:
        return await self.configure(command)

    async def retire(self, command: RetireSkillSourceCommand) -> SkillSourceRecord:
        current = await self.get_source(command.tenant_id, command.source_id)
        now = datetime.now(UTC)
        record = current.model_copy(
            update={
                "desired_state": SkillSourceDesiredState.RETIRED,
                "revision": current.revision + 1,
                "updated_by": command.actor_id,
                "updated_at": now,
            }
        )
        result = await self._lifecycle.commit_source_config(
            SkillSourceConfigCommit(
                command_id=command.command_id,
                request_digest=_command_digest(command.model_dump(mode="json")),
                operation="retire",
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                reason_code=command.reason_code,
                expected_revision=command.expected_revision,
                source=record,
                occurred_at=now,
            )
        )
        if self._projector is not None:
            await self._projector.rebuild_tenant(command.tenant_id)
        return result

    async def retire_source(
        self, command: RetireSkillSourceCommand
    ) -> SkillSourceRecord:
        return await self.retire(command)

    async def sync(self, tenant_id: str, source_id: str) -> object:
        source = await self.get_source(tenant_id, source_id)
        if source.desired_state is not SkillSourceDesiredState.ENABLED:
            raise PolicyDeniedError("Skill Source is not enabled")
        if source.kind is not SkillSourceKind.MCP or self._synchronizer is None:
            raise PolicyDeniedError("Skill Source synchronization is not available")
        return await self._synchronizer.reconcile_source(tenant_id, source_id)

    async def sync_source(self, tenant_id: str, source_id: str) -> dict[str, object]:
        result = await self.sync(tenant_id, source_id)
        if isinstance(result, dict):
            return {str(key): value for key, value in result.items()}
        state = getattr(result, "__dict__", None)
        return dict(state) if isinstance(state, dict) else {"status": "completed"}


def _command_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
