from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from auraclaw.contracts.capabilities import McpAuthStrategy, McpNetworkMode
from auraclaw.contracts.errors import (
    AuraClawError,
    AuthorizationError,
    InvalidTransitionError,
    NotFoundError,
    VersionConflictError,
)
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpRegistryOperationKind,
    McpRegistryOperationStatus,
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerOperationRecord,
    McpServerRecord,
    McpServerRevisionRecord,
    McpServerRuntimeRecord,
    McpServerWriteCommand,
)

PLATFORM_TENANT = "platform"


class McpServerRegistryStore(Protocol):
    async def get_server(self, server_id: str) -> McpServerRecord | None: ...

    async def list_servers(self, tenant_id: str) -> tuple[McpServerRecord, ...]: ...

    async def get_revision(
        self, server_id: str, revision: int
    ) -> McpServerRevisionRecord | None: ...

    async def get_operation(
        self, operation_id: str
    ) -> McpServerOperationRecord | None: ...

    async def get_operation_by_command(
        self, command_id: str, tenant_id: str
    ) -> McpServerOperationRecord | None: ...

    async def insert_candidate(
        self,
        *,
        record: McpServerRecord,
        revision: McpServerRevisionRecord,
        operation: McpServerOperationRecord,
        create: bool,
    ) -> None: ...

    async def set_desired_state(
        self,
        *,
        server_id: str,
        expected_revision: int,
        desired_state: McpDesiredState,
        active_revision: int | None,
        operation: McpServerOperationRecord,
    ) -> McpServerRecord: ...

    async def complete_operation(
        self, operation: McpServerOperationRecord
    ) -> McpServerOperationRecord: ...

    async def update_runtime(self, runtime: McpServerRuntimeRecord) -> None: ...

    async def list_active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]: ...


class McpRuntimeController(Protocol):
    async def test(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def apply(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def revoke(self, server_id: str) -> None: ...


@dataclass
class InMemoryMcpServerRegistryStore:
    _servers: dict[str, McpServerRecord] = field(default_factory=dict)
    _revisions: dict[tuple[str, int], McpServerRevisionRecord] = field(
        default_factory=dict
    )
    _operations: dict[str, McpServerOperationRecord] = field(default_factory=dict)
    _operations_by_command: dict[tuple[str, str], str] = field(default_factory=dict)
    _runtime: dict[str, McpServerRuntimeRecord] = field(default_factory=dict)

    async def get_server(self, server_id: str) -> McpServerRecord | None:
        record = self._servers.get(server_id)
        if record is None:
            return None
        return self._hydrate(record)

    async def list_servers(self, tenant_id: str) -> tuple[McpServerRecord, ...]:
        records = [
            self._hydrate(record)
            for record in self._servers.values()
            if record.tenant_id in {tenant_id, None}
            and record.desired_state is not McpDesiredState.RETIRED
        ]
        return tuple(sorted(records, key=lambda item: item.server_id))

    async def get_revision(
        self, server_id: str, revision: int
    ) -> McpServerRevisionRecord | None:
        return self._revisions.get((server_id, revision))

    async def get_operation(
        self, operation_id: str
    ) -> McpServerOperationRecord | None:
        return self._operations.get(operation_id)

    async def get_operation_by_command(
        self, command_id: str, tenant_id: str
    ) -> McpServerOperationRecord | None:
        operation_id = self._operations_by_command.get((tenant_id, command_id))
        if operation_id is None:
            return None
        return self._operations.get(operation_id)

    async def insert_candidate(
        self,
        *,
        record: McpServerRecord,
        revision: McpServerRevisionRecord,
        operation: McpServerOperationRecord,
        create: bool,
    ) -> None:
        existing = self._servers.get(record.server_id)
        if create and existing is not None:
            raise VersionConflictError("MCP server already exists")
        if not create and existing is None:
            raise NotFoundError("MCP server was not found")
        if not create and existing is not None:
            if existing.latest_revision != record.latest_revision - 1:
                raise VersionConflictError("MCP server revision conflict")
        self._servers[record.server_id] = record
        self._revisions[(revision.server_id, revision.revision)] = revision
        self._save_operation(operation)
        if record.server_id not in self._runtime:
            self._runtime[record.server_id] = McpServerRuntimeRecord(
                server_id=record.server_id,
                observed_state=McpObservedState.PENDING,
                updated_at=record.updated_at,
            )

    async def set_desired_state(
        self,
        *,
        server_id: str,
        expected_revision: int,
        desired_state: McpDesiredState,
        active_revision: int | None,
        operation: McpServerOperationRecord,
    ) -> McpServerRecord:
        existing = self._servers.get(server_id)
        if existing is None:
            raise NotFoundError("MCP server was not found")
        if existing.latest_revision != expected_revision:
            raise VersionConflictError("MCP server revision conflict")
        now = datetime.now(UTC)
        updated = existing.model_copy(
            update={
                "desired_state": desired_state,
                "active_revision": active_revision,
                "updated_at": now,
            }
        )
        self._servers[server_id] = updated
        self._save_operation(operation)
        return self._hydrate(updated)

    async def complete_operation(
        self, operation: McpServerOperationRecord
    ) -> McpServerOperationRecord:
        self._save_operation(operation)
        return operation

    async def update_runtime(self, runtime: McpServerRuntimeRecord) -> None:
        self._runtime[runtime.server_id] = runtime

    async def list_active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]:
        entries: list[McpActiveSnapshotEntry] = []
        for record in self._servers.values():
            if (
                record.desired_state is not McpDesiredState.ENABLED
                or record.active_revision is None
            ):
                continue
            revision = self._revisions[(record.server_id, record.active_revision)]
            runtime = self._runtime.get(record.server_id)
            entries.append(
                McpActiveSnapshotEntry(
                    server_id=record.server_id,
                    tenant_id=record.tenant_id,
                    revision=record.active_revision,
                    config=revision.config,
                    desired_state=record.desired_state,
                    observed_state=(
                        runtime.observed_state
                        if runtime is not None
                        else McpObservedState.PENDING
                    ),
                )
            )
        return tuple(sorted(entries, key=lambda item: item.server_id))

    def _save_operation(self, operation: McpServerOperationRecord) -> None:
        self._operations[operation.operation_id] = operation
        self._operations_by_command[
            (operation.tenant_id or PLATFORM_TENANT, operation.command_id)
        ] = operation.operation_id

    def _hydrate(self, record: McpServerRecord) -> McpServerRecord:
        latest = self._revisions.get((record.server_id, record.latest_revision))
        active = (
            self._revisions.get((record.server_id, record.active_revision))
            if record.active_revision is not None
            else None
        )
        return record.model_copy(
            update={
                "latest_config": None if latest is None else latest.config,
                "active_config": None if active is None else active.config,
                "runtime": self._runtime.get(record.server_id),
            }
        )


class McpServerRegistryService:
    """Unique writer for MCP server configuration, revisions, and desired state."""

    def __init__(
        self,
        store: McpServerRegistryStore,
        *,
        runtime: McpRuntimeController | None = None,
        allow_private_auth_none: bool = False,
    ) -> None:
        self._store = store
        self._runtime = runtime
        self._allow_private_auth_none = allow_private_auth_none

    def bind_runtime(self, runtime: McpRuntimeController) -> None:
        self._runtime = runtime

    async def get_server(
        self, *, tenant_id: str, server_id: str, actor_id: str
    ) -> McpServerRecord:
        del actor_id
        record = await self._store.get_server(server_id)
        if record is None or record.desired_state is McpDesiredState.RETIRED:
            raise NotFoundError("MCP server was not found")
        self._assert_visible(record, tenant_id)
        return record

    async def list_servers(self, *, tenant_id: str) -> tuple[McpServerRecord, ...]:
        return await self._store.list_servers(tenant_id)

    async def get_operation(
        self, *, tenant_id: str, operation_id: str
    ) -> McpServerOperationRecord:
        record = await self._store.get_operation(operation_id)
        if record is None:
            raise NotFoundError("MCP operation was not found")
        if record.tenant_id not in {tenant_id, None} and tenant_id != PLATFORM_TENANT:
            raise AuthorizationError("MCP operation is outside tenant scope")
        return record

    async def create(self, command: McpServerWriteCommand) -> McpServerOperationRecord:
        self._assert_writable(command.config, command.tenant_id)
        self._assert_auth_policy(command.config)
        if command.expected_revision != 0:
            raise VersionConflictError("create requires expected revision 0")
        existing = await self._idempotent(command.command_id, command.tenant_id)
        if existing is not None:
            return existing
        now = datetime.now(UTC)
        revision = McpServerRevisionRecord(
            server_id=command.config.server_id,
            revision=1,
            config=command.config,
            config_digest=command.config.config_digest(),
            created_by=command.actor_id,
            created_at=now,
        )
        record = McpServerRecord(
            server_id=command.config.server_id,
            tenant_id=command.config.tenant_id,
            desired_state=McpDesiredState.DISABLED,
            latest_revision=1,
            active_revision=None,
            created_by=command.actor_id,
            created_at=now,
            updated_at=now,
        )
        operation = _new_operation(
            command,
            server_id=command.config.server_id,
            kind=McpRegistryOperationKind.CREATE,
            target_revision=1,
            now=now,
        )
        await self._store.insert_candidate(
            record=record, revision=revision, operation=operation, create=True
        )
        return await self._store.complete_operation(
            operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.SUCCEEDED,
                    "completed_at": datetime.now(UTC),
                    "result": {"latest_revision": 1, "desired_state": "disabled"},
                }
            )
        )

    async def update(self, command: McpServerWriteCommand) -> McpServerOperationRecord:
        self._assert_writable(command.config, command.tenant_id)
        self._assert_auth_policy(command.config)
        existing = await self._idempotent(command.command_id, command.tenant_id)
        if existing is not None:
            return existing
        current = await self._require_server(command.config.server_id, command.tenant_id)
        if current.desired_state is McpDesiredState.RETIRED:
            raise InvalidTransitionError("retired MCP server cannot be updated")
        if current.latest_revision != command.expected_revision:
            raise VersionConflictError("MCP server revision conflict")
        now = datetime.now(UTC)
        next_revision = current.latest_revision + 1
        revision = McpServerRevisionRecord(
            server_id=command.config.server_id,
            revision=next_revision,
            config=command.config,
            config_digest=command.config.config_digest(),
            created_by=command.actor_id,
            created_at=now,
        )
        record = current.model_copy(
            update={
                "latest_revision": next_revision,
                "updated_at": now,
                "tenant_id": command.config.tenant_id,
            }
        )
        operation = _new_operation(
            command,
            server_id=command.config.server_id,
            kind=McpRegistryOperationKind.UPDATE,
            target_revision=next_revision,
            now=now,
        )
        await self._store.insert_candidate(
            record=record, revision=revision, operation=operation, create=False
        )
        return await self._store.complete_operation(
            operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.SUCCEEDED,
                    "completed_at": datetime.now(UTC),
                    "result": {"latest_revision": next_revision},
                }
            )
        )

    async def test(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(
            server_id, command, McpRegistryOperationKind.TEST
        )

    async def enable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(
            server_id, command, McpRegistryOperationKind.ENABLE
        )

    async def disable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(
            server_id, command, McpRegistryOperationKind.DISABLE
        )

    async def reconcile(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(
            server_id, command, McpRegistryOperationKind.RECONCILE
        )

    async def retire(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(
            server_id, command, McpRegistryOperationKind.RETIRE
        )

    async def active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]:
        return await self._store.list_active_snapshot()

    async def record_runtime(self, runtime: McpServerRuntimeRecord) -> None:
        await self._store.update_runtime(runtime)

    async def _lifecycle(
        self,
        server_id: str,
        command: McpServerLifecycleCommand,
        kind: McpRegistryOperationKind,
    ) -> McpServerOperationRecord:
        existing = await self._idempotent(command.command_id, command.tenant_id)
        if existing is not None:
            return existing
        current = await self._require_server(server_id, command.tenant_id)
        if current.latest_revision != command.expected_revision:
            raise VersionConflictError("MCP server revision conflict")
        if current.desired_state is McpDesiredState.RETIRED:
            raise InvalidTransitionError("retired MCP server cannot change state")
        target_revision = command.target_revision or current.latest_revision
        revision = await self._store.get_revision(server_id, target_revision)
        if revision is None:
            raise NotFoundError("MCP server revision was not found")
        now = datetime.now(UTC)
        operation = _new_operation(
            command,
            server_id=server_id,
            kind=kind,
            target_revision=target_revision,
            now=now,
        )
        await self._store.complete_operation(operation)
        try:
            result = await self._apply_lifecycle(current, revision, kind)
        except Exception as exc:
            failed = operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.FAILED,
                    "safe_error_code": _safe_error(exc),
                    "completed_at": datetime.now(UTC),
                }
            )
            return await self._store.complete_operation(failed)
        desired, active = result
        updated = await self._store.set_desired_state(
            server_id=server_id,
            expected_revision=command.expected_revision,
            desired_state=desired,
            active_revision=active,
            operation=operation,
        )
        del updated
        return await self._store.complete_operation(
            operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.SUCCEEDED,
                    "completed_at": datetime.now(UTC),
                    "result": {
                        "desired_state": desired.value,
                        "active_revision": active,
                    },
                }
            )
        )

    async def _apply_lifecycle(
        self,
        current: McpServerRecord,
        revision: McpServerRevisionRecord,
        kind: McpRegistryOperationKind,
    ) -> tuple[McpDesiredState, int | None]:
        entry = McpActiveSnapshotEntry(
            server_id=current.server_id,
            tenant_id=current.tenant_id,
            revision=revision.revision,
            config=revision.config,
            desired_state=McpDesiredState.ENABLED,
            observed_state=McpObservedState.LOADING,
        )
        if kind is McpRegistryOperationKind.TEST:
            if self._runtime is not None:
                await self._runtime.test(entry)
            return current.desired_state, current.active_revision
        if kind is McpRegistryOperationKind.ENABLE:
            if self._runtime is not None:
                await self._runtime.test(entry)
                await self._runtime.apply(entry)
            return McpDesiredState.ENABLED, revision.revision
        if kind is McpRegistryOperationKind.DISABLE:
            if self._runtime is not None:
                await self._runtime.revoke(current.server_id)
            return McpDesiredState.DISABLED, current.active_revision
        if kind is McpRegistryOperationKind.RECONCILE:
            if (
                self._runtime is not None
                and current.desired_state is McpDesiredState.ENABLED
                and current.active_revision is not None
            ):
                active = await self._store.get_revision(
                    current.server_id, current.active_revision
                )
                if active is not None:
                    await self._runtime.apply(
                        entry.model_copy(
                            update={
                                "revision": active.revision,
                                "config": active.config,
                            }
                        )
                    )
            return current.desired_state, current.active_revision
        if self._runtime is not None:
            await self._runtime.revoke(current.server_id)
        return McpDesiredState.RETIRED, current.active_revision

    async def _require_server(
        self, server_id: str, tenant_id: str
    ) -> McpServerRecord:
        record = await self._store.get_server(server_id)
        if record is None:
            raise NotFoundError("MCP server was not found")
        self._assert_visible(record, tenant_id)
        return record

    async def _idempotent(
        self, command_id: str, tenant_id: str
    ) -> McpServerOperationRecord | None:
        return await self._store.get_operation_by_command(command_id, tenant_id)

    def _assert_visible(self, record: McpServerRecord, tenant_id: str) -> None:
        if record.tenant_id is None:
            return
        if record.tenant_id != tenant_id:
            raise AuthorizationError("MCP server is outside tenant scope")

    def _assert_writable(self, config: McpServerConfig, tenant_id: str) -> None:
        if config.tenant_id is None and tenant_id != PLATFORM_TENANT:
            raise AuthorizationError("platform MCP servers require platform admin")
        if config.tenant_id is not None and config.tenant_id != tenant_id:
            raise AuthorizationError("MCP server tenant does not match caller")

    def _assert_auth_policy(self, config: McpServerConfig) -> None:
        if config.auth_strategy is not McpAuthStrategy.NONE:
            return
        if config.network_mode is McpNetworkMode.PRIVATE and not self._allow_private_auth_none:
            raise AuthorizationError("private MCP servers may not use auth_strategy none")


def _new_operation(
    command: McpServerWriteCommand | McpServerLifecycleCommand,
    *,
    server_id: str,
    kind: McpRegistryOperationKind,
    target_revision: int | None,
    now: datetime,
) -> McpServerOperationRecord:
    return McpServerOperationRecord(
        operation_id=str(uuid4()),
        server_id=server_id,
        tenant_id=command.tenant_id,
        target_revision=target_revision,
        command_id=command.command_id,
        actor_id=command.actor_id,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id,
        operation=kind,
        status=McpRegistryOperationStatus.ACCEPTED,
        created_at=now,
    )


def _safe_error(exc: BaseException) -> str:
    if isinstance(exc, AuraClawError):
        return exc.code
    text = str(exc)
    lowered = text.lower()
    if "dns" in lowered or "address" in lowered or "loopback" in lowered:
        return "mcp_network_denied"
    if "credential" in lowered or "oauth" in lowered:
        return "mcp_auth_denied"
    return "mcp_connection_test_failed"
