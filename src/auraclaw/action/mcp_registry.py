from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
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


@dataclass(frozen=True)
class McpOperationClaim:
    acquired: bool
    operation: McpServerOperationRecord
    claim_token: str | None = None


class McpServerRegistryStore(Protocol):
    async def get_server(self, server_id: str) -> McpServerRecord | None: ...

    async def list_servers(self, tenant_id: str) -> tuple[McpServerRecord, ...]: ...

    async def get_revision(
        self, server_id: str, revision: int
    ) -> McpServerRevisionRecord | None: ...

    async def get_operation(self, operation_id: str) -> McpServerOperationRecord | None: ...

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
        claim_token: str | None = None,
    ) -> McpServerRecord: ...

    async def claim_operation(
        self,
        operation: McpServerOperationRecord,
        *,
        request_digest: str,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> McpOperationClaim: ...

    async def renew_operation(
        self,
        operation_id: str,
        *,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool: ...

    async def complete_operation(
        self,
        operation: McpServerOperationRecord,
        *,
        claim_token: str | None = None,
    ) -> McpServerOperationRecord: ...

    async def update_runtime(self, runtime: McpServerRuntimeRecord) -> None: ...

    async def list_active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]: ...

    async def delete_server(
        self,
        server_id: str,
        *,
        expected_revision: int | None = None,
        expected_created_at: datetime | None = None,
    ) -> None: ...

    async def list_pending_deletes(
        self, *, limit: int = 100
    ) -> tuple[McpServerOperationRecord, ...]: ...


class McpRuntimeController(Protocol):
    async def test(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def apply(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def revoke(self, server_id: str) -> None: ...


@dataclass
class InMemoryMcpServerRegistryStore:
    _servers: dict[str, McpServerRecord] = field(default_factory=dict)
    _revisions: dict[tuple[str, int], McpServerRevisionRecord] = field(default_factory=dict)
    _operations: dict[str, McpServerOperationRecord] = field(default_factory=dict)
    _operations_by_command: dict[tuple[str, str], str] = field(default_factory=dict)
    _runtime: dict[tuple[str, str], McpServerRuntimeRecord] = field(default_factory=dict)
    _operation_claims: dict[str, tuple[str, str, datetime]] = field(default_factory=dict)
    _operation_digests: dict[str, str] = field(default_factory=dict)

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
        ]
        return tuple(sorted(records, key=lambda item: item.server_id))

    async def get_revision(self, server_id: str, revision: int) -> McpServerRevisionRecord | None:
        return self._revisions.get((server_id, revision))

    async def get_operation(self, operation_id: str) -> McpServerOperationRecord | None:
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

    async def set_desired_state(
        self,
        *,
        server_id: str,
        expected_revision: int,
        desired_state: McpDesiredState,
        active_revision: int | None,
        operation: McpServerOperationRecord,
        claim_token: str | None = None,
    ) -> McpServerRecord:
        if claim_token is not None and not self._claim_is_active(
            operation.operation_id, claim_token
        ):
            raise VersionConflictError("MCP operation claim was lost")
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

    async def claim_operation(
        self,
        operation: McpServerOperationRecord,
        *,
        request_digest: str,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> McpOperationClaim:
        existing = await self.get_operation_by_command(
            operation.command_id, operation.tenant_id or PLATFORM_TENANT
        )
        if existing is not None:
            if self._operation_digests.get(existing.operation_id) != request_digest:
                raise VersionConflictError(
                    "MCP command id was already used for a different request"
                )
            claim = self._operation_claims.get(existing.operation_id)
            if (
                existing.status is McpRegistryOperationStatus.RUNNING
                and claim is not None
                and claim[2] <= datetime.now(UTC)
            ):
                unknown = existing.model_copy(
                    update={
                        "status": McpRegistryOperationStatus.UNKNOWN_SIDE_EFFECT,
                        "safe_error_code": "mcp_operation_recovery_required",
                        "completed_at": datetime.now(UTC),
                    }
                )
                self._save_operation(unknown)
                return McpOperationClaim(False, unknown)
            return McpOperationClaim(False, existing)
        running = operation.model_copy(update={"status": McpRegistryOperationStatus.RUNNING})
        self._save_operation(running)
        self._operation_digests[running.operation_id] = request_digest
        self._operation_claims[running.operation_id] = (
            claimed_by,
            claim_token,
            datetime.now(UTC) + claim_ttl,
        )
        return McpOperationClaim(True, running, claim_token)

    async def renew_operation(
        self,
        operation_id: str,
        *,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool:
        claim = self._operation_claims.get(operation_id)
        if (
            claim is None
            or claim[0] != claimed_by
            or claim[1] != claim_token
            or claim[2] <= datetime.now(UTC)
        ):
            return False
        self._operation_claims[operation_id] = (
            claimed_by,
            claim_token,
            datetime.now(UTC) + claim_ttl,
        )
        return True

    async def complete_operation(
        self,
        operation: McpServerOperationRecord,
        *,
        claim_token: str | None = None,
    ) -> McpServerOperationRecord:
        if claim_token is not None and not self._claim_is_active(
            operation.operation_id, claim_token
        ):
            current = self._operations.get(operation.operation_id)
            return current if current is not None else operation
        self._save_operation(operation)
        if claim_token is not None:
            self._operation_claims.pop(operation.operation_id, None)
        return operation

    async def update_runtime(self, runtime: McpServerRuntimeRecord) -> None:
        self._runtime[(runtime.server_id, runtime.instance_id)] = runtime

    async def list_active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]:
        entries: list[McpActiveSnapshotEntry] = []
        for record in self._servers.values():
            if (
                record.desired_state is not McpDesiredState.ENABLED
                or record.active_revision is None
            ):
                continue
            revision = self._revisions[(record.server_id, record.active_revision)]
            runtimes = tuple(
                value for key, value in self._runtime.items() if key[0] == record.server_id
            )
            runtime = _aggregate_runtime(runtimes)
            entries.append(
                McpActiveSnapshotEntry(
                    server_id=record.server_id,
                    tenant_id=record.tenant_id,
                    revision=record.active_revision,
                    config=revision.config,
                    desired_state=record.desired_state,
                    observed_state=(
                        runtime.observed_state if runtime is not None else McpObservedState.PENDING
                    ),
                )
            )
        return tuple(sorted(entries, key=lambda item: item.server_id))

    async def list_pending_deletes(
        self, *, limit: int = 100
    ) -> tuple[McpServerOperationRecord, ...]:
        return tuple(
            item
            for item in self._operations.values()
            if item.operation is McpRegistryOperationKind.DELETE
            and item.status is McpRegistryOperationStatus.RECONCILING
        )[:limit]

    async def delete_server(
        self,
        server_id: str,
        *,
        expected_revision: int | None = None,
        expected_created_at: datetime | None = None,
    ) -> None:
        if server_id not in self._servers:
            raise NotFoundError("MCP server was not found")
        current = self._servers[server_id]
        if expected_revision is not None and (
            current.latest_revision != expected_revision
            or current.desired_state is not McpDesiredState.RETIRED
            or (expected_created_at is not None and current.created_at != expected_created_at)
        ):
            raise VersionConflictError("MCP deletion was superseded")
        del self._servers[server_id]
        for runtime_key in [key for key in self._runtime if key[0] == server_id]:
            self._runtime.pop(runtime_key, None)
        for revision_key in [
            revision_key for revision_key in self._revisions if revision_key[0] == server_id
        ]:
            del self._revisions[revision_key]

    def _save_operation(self, operation: McpServerOperationRecord) -> None:
        self._operations[operation.operation_id] = operation
        self._operations_by_command[
            (operation.tenant_id or PLATFORM_TENANT, operation.command_id)
        ] = operation.operation_id

    def _claim_is_active(self, operation_id: str, claim_token: str) -> bool:
        claim = self._operation_claims.get(operation_id)
        return claim is not None and claim[1] == claim_token and claim[2] > datetime.now(UTC)

    def _hydrate(self, record: McpServerRecord) -> McpServerRecord:
        latest = self._revisions.get((record.server_id, record.latest_revision))
        active = (
            self._revisions.get((record.server_id, record.active_revision))
            if record.active_revision is not None
            else None
        )
        runtimes = tuple(
            sorted(
                (value for key, value in self._runtime.items() if key[0] == record.server_id),
                key=lambda item: item.instance_id,
            )
        )
        return record.model_copy(
            update={
                "latest_config": None if latest is None else latest.config,
                "active_config": None if active is None else active.config,
                "runtime": _aggregate_runtime(runtimes),
                "runtimes": runtimes,
            }
        )


def _aggregate_runtime(
    runtimes: tuple[McpServerRuntimeRecord, ...],
) -> McpServerRuntimeRecord | None:
    if not runtimes:
        return None
    rank = {
        McpObservedState.ACTIVE: 0,
        McpObservedState.DEGRADED: 1,
        McpObservedState.LOADING: 2,
        McpObservedState.PENDING: 3,
        McpObservedState.UNAVAILABLE: 4,
        McpObservedState.QUARANTINED: 5,
        McpObservedState.DISABLED: 6,
    }
    representative = min(
        runtimes,
        key=lambda item: (rank[item.observed_state], -item.updated_at.timestamp()),
    )
    return representative.model_copy(update={"instance_id": "aggregate"})


class McpServerRegistryService:
    """Unique writer for MCP server configuration, revisions, and desired state."""

    def __init__(
        self,
        store: McpServerRegistryStore,
        *,
        runtime: McpRuntimeController | None = None,
        allow_private_auth_none: bool = False,
        instance_id: str | None = None,
        operation_claim_ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        if operation_claim_ttl <= timedelta(0):
            raise ValueError("operation_claim_ttl must be positive")
        self._store = store
        self._runtime = runtime
        self._allow_private_auth_none = allow_private_auth_none
        self._instance_id = instance_id or f"mcp-registry-{secrets.token_hex(8)}"
        self._operation_claim_ttl = operation_claim_ttl

    def bind_runtime(self, runtime: McpRuntimeController) -> None:
        self._runtime = runtime

    async def get_server(self, *, tenant_id: str, server_id: str, actor_id: str) -> McpServerRecord:
        del actor_id
        record = await self._store.get_server(server_id)
        if record is None:
            raise NotFoundError("MCP server was not found")
        self._assert_visible(record, tenant_id)
        return record

    async def list_servers(self, *, tenant_id: str) -> tuple[McpServerRecord, ...]:
        return await self._store.list_servers(tenant_id)

    async def get_operation(self, *, tenant_id: str, operation_id: str) -> McpServerOperationRecord:
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
        current = await self._store.get_server(command.config.server_id)
        if current is not None:
            if current.desired_state is McpDesiredState.RETIRED:
                return await self._append_revision(
                    command,
                    current,
                    kind=McpRegistryOperationKind.CREATE,
                    desired_state=McpDesiredState.DISABLED,
                )
            raise VersionConflictError("MCP server already exists")
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
        return await self._append_revision(
            command,
            current,
            kind=McpRegistryOperationKind.UPDATE,
        )

    async def _append_revision(
        self,
        command: McpServerWriteCommand,
        current: McpServerRecord,
        *,
        kind: McpRegistryOperationKind,
        desired_state: McpDesiredState | None = None,
    ) -> McpServerOperationRecord:
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
                **({"desired_state": desired_state} if desired_state is not None else {}),
            }
        )
        operation = _new_operation(
            command,
            server_id=command.config.server_id,
            kind=kind,
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
                    "result": {
                        "latest_revision": next_revision,
                        "desired_state": record.desired_state.value,
                    },
                }
            )
        )

    async def test(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(server_id, command, McpRegistryOperationKind.TEST)

    async def enable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(server_id, command, McpRegistryOperationKind.ENABLE)

    async def disable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(server_id, command, McpRegistryOperationKind.DISABLE)

    async def reconcile(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(server_id, command, McpRegistryOperationKind.RECONCILE)

    async def retire(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle(server_id, command, McpRegistryOperationKind.RETIRE)

    async def delete(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        existing = await self._idempotent(command.command_id, command.tenant_id)
        if existing is not None:
            return existing
        current = await self._require_server(server_id, command.tenant_id)
        if current.latest_revision != command.expected_revision:
            raise VersionConflictError("MCP server revision conflict")
        now = datetime.now(UTC)
        operation = _new_operation(
            command,
            server_id=server_id,
            kind=McpRegistryOperationKind.DELETE,
            target_revision=current.latest_revision,
            now=now,
        )
        await self._store.set_desired_state(
            server_id=server_id,
            expected_revision=command.expected_revision,
            desired_state=McpDesiredState.RETIRED,
            active_revision=current.active_revision,
            operation=operation.model_copy(
                update={"status": McpRegistryOperationStatus.RECONCILING}
            ),
        )
        try:
            if self._runtime is not None:
                purge = getattr(self._runtime, "purge", None)
                if callable(purge):
                    await purge(server_id)
                else:
                    await self._runtime.revoke(server_id)
            await self._store.delete_server(
                server_id,
                expected_revision=command.expected_revision,
                expected_created_at=current.created_at,
            )
        except Exception as exc:
            failed = operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.RECONCILING,
                    "safe_error_code": _safe_error(exc),
                    "result": _failure_result(exc),
                    "completed_at": datetime.now(UTC),
                }
            )
            return await self._store.complete_operation(failed)
        return await self._store.complete_operation(
            operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.SUCCEEDED,
                    "completed_at": datetime.now(UTC),
                    "result": {"deleted": True, "server_id": server_id},
                }
            )
        )

    async def active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]:
        return await self._store.list_active_snapshot()

    async def record_runtime(self, runtime: McpServerRuntimeRecord) -> None:
        if await self._store.get_server(runtime.server_id) is not None:
            await self._store.update_runtime(runtime)

    async def reconcile_pending_deletes(self, *, limit: int = 16) -> int:
        completed = 0
        for operation in await self._store.list_pending_deletes(limit=limit):
            current = await self._store.get_server(operation.server_id)
            if current is not None and (
                current.created_at > operation.created_at
                or current.desired_state is not McpDesiredState.RETIRED
                or current.latest_revision != operation.target_revision
            ):
                await self._store.complete_operation(
                    operation.model_copy(
                        update={
                            "status": McpRegistryOperationStatus.FAILED,
                            "safe_error_code": "mcp_delete_superseded",
                            "completed_at": datetime.now(UTC),
                        }
                    )
                )
                continue
            try:
                if self._runtime is not None:
                    purge = getattr(self._runtime, "purge", self._runtime.revoke)
                    await asyncio.wait_for(purge(operation.server_id), timeout=30)
                if await self._store.get_server(operation.server_id) is not None:
                    await self._store.delete_server(
                        operation.server_id,
                        expected_revision=operation.target_revision,
                        expected_created_at=None if current is None else current.created_at,
                    )
                await self._store.complete_operation(
                    operation.model_copy(
                        update={
                            "status": McpRegistryOperationStatus.SUCCEEDED,
                            "completed_at": datetime.now(UTC),
                            "safe_error_code": None,
                            "result": {"deleted": True, "server_id": operation.server_id},
                        }
                    )
                )
                completed += 1
            except Exception:
                # Desired RETIRED and the original operation remain durable.
                continue
        return completed

    async def _lifecycle(
        self,
        server_id: str,
        command: McpServerLifecycleCommand,
        kind: McpRegistryOperationKind,
    ) -> McpServerOperationRecord:
        current = await self._require_server(server_id, command.tenant_id)
        target_revision = command.target_revision or current.latest_revision
        now = datetime.now(UTC)
        operation = _new_operation(
            command,
            server_id=server_id,
            kind=kind,
            target_revision=target_revision,
            now=now,
        )
        request_digest = _lifecycle_request_digest(server_id, command, kind)
        claim_token = secrets.token_urlsafe(24)
        claim = await self._store.claim_operation(
            operation,
            request_digest=request_digest,
            claimed_by=self._instance_id,
            claim_token=claim_token,
            claim_ttl=self._operation_claim_ttl,
        )
        if not claim.acquired or claim.claim_token is None:
            return claim.operation
        operation = claim.operation
        parent = asyncio.current_task()
        heartbeat = asyncio.create_task(
            self._heartbeat_operation(operation.operation_id, claim_token, parent)
        )
        desired, active = current.desired_state, current.active_revision
        desired_committed = False
        try:
            if current.latest_revision != command.expected_revision:
                raise VersionConflictError("MCP server revision conflict")
            if current.desired_state is McpDesiredState.RETIRED:
                raise InvalidTransitionError("retired MCP server cannot change state")
            revision = await self._store.get_revision(server_id, target_revision)
            if revision is None:
                raise NotFoundError("MCP server revision was not found")
            desired, active = self._lifecycle_target(current, revision, kind)
            if kind is not McpRegistryOperationKind.TEST:
                current = await self._store.set_desired_state(
                    server_id=server_id,
                    expected_revision=command.expected_revision,
                    desired_state=desired,
                    active_revision=active,
                    operation=operation,
                    claim_token=claim_token,
                )
                desired_committed = True
            await self._apply_lifecycle(current, revision, kind)
        except asyncio.CancelledError:
            unknown = operation.model_copy(
                update={
                    "status": McpRegistryOperationStatus.UNKNOWN_SIDE_EFFECT,
                    "safe_error_code": "mcp_operation_claim_lost",
                    "result": {"manual_recovery_required": True},
                    "completed_at": datetime.now(UTC),
                }
            )
            return await self._store.complete_operation(unknown, claim_token=claim_token)
        except Exception as exc:
            failed = operation.model_copy(
                update={
                    "status": (
                        McpRegistryOperationStatus.RECONCILING
                        if desired_committed
                        else McpRegistryOperationStatus.FAILED
                    ),
                    "safe_error_code": _safe_error(exc),
                    "result": {
                        **_failure_result(exc),
                        "desired_state_committed": desired_committed,
                    },
                    "completed_at": datetime.now(UTC),
                }
            )
            return await self._store.complete_operation(failed, claim_token=claim_token)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
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
            ),
            claim_token=claim_token,
        )

    @staticmethod
    def _lifecycle_target(
        current: McpServerRecord,
        revision: McpServerRevisionRecord,
        kind: McpRegistryOperationKind,
    ) -> tuple[McpDesiredState, int | None]:
        if kind is McpRegistryOperationKind.ENABLE:
            return McpDesiredState.ENABLED, revision.revision
        if kind is McpRegistryOperationKind.DISABLE:
            return McpDesiredState.DISABLED, current.active_revision
        if kind is McpRegistryOperationKind.RETIRE:
            return McpDesiredState.RETIRED, current.active_revision
        return current.desired_state, current.active_revision

    async def _heartbeat_operation(
        self,
        operation_id: str,
        claim_token: str,
        parent: asyncio.Task[object] | None,
    ) -> None:
        interval = max(self._operation_claim_ttl.total_seconds() / 3, 0.01)
        try:
            while True:
                await asyncio.sleep(interval)
                renewed = await self._store.renew_operation(
                    operation_id,
                    claimed_by=self._instance_id,
                    claim_token=claim_token,
                    claim_ttl=self._operation_claim_ttl,
                )
                if not renewed:
                    if parent is not None and not parent.done():
                        parent.cancel()
                    return
        except Exception:
            if parent is not None and not parent.done():
                parent.cancel()

    async def _apply_lifecycle(
        self,
        current: McpServerRecord,
        revision: McpServerRevisionRecord,
        kind: McpRegistryOperationKind,
    ) -> None:
        entry = McpActiveSnapshotEntry(
            server_id=current.server_id,
            tenant_id=current.tenant_id,
            revision=revision.revision,
            config=revision.config,
            desired_state=(
                McpDesiredState.DISABLED
                if kind is McpRegistryOperationKind.TEST
                else McpDesiredState.ENABLED
            ),
            observed_state=McpObservedState.LOADING,
        )
        if kind is McpRegistryOperationKind.TEST:
            if self._runtime is not None:
                await self._runtime.test(entry)
            return
        if kind is McpRegistryOperationKind.ENABLE:
            if self._runtime is not None:
                await self._runtime.apply(entry)
            return
        if kind is McpRegistryOperationKind.DISABLE:
            if self._runtime is not None:
                await self._runtime.revoke(current.server_id)
            return
        if kind is McpRegistryOperationKind.RECONCILE:
            if (
                self._runtime is not None
                and current.desired_state is McpDesiredState.ENABLED
                and current.active_revision is not None
            ):
                active = await self._store.get_revision(current.server_id, current.active_revision)
                if active is not None:
                    await self._runtime.apply(
                        entry.model_copy(
                            update={
                                "revision": active.revision,
                                "config": active.config,
                            }
                        )
                    )
            return
        if self._runtime is not None:
            await self._runtime.revoke(current.server_id)
        return

    async def _require_server(self, server_id: str, tenant_id: str) -> McpServerRecord:
        record = await self._store.get_server(server_id)
        if record is None:
            raise NotFoundError("MCP server was not found")
        self._assert_visible(record, tenant_id)
        return record

    async def _idempotent(self, command_id: str, tenant_id: str) -> McpServerOperationRecord | None:
        return await self._store.get_operation_by_command(command_id, tenant_id)

    def _assert_visible(self, record: McpServerRecord, tenant_id: str) -> None:
        if record.tenant_id is None:
            return
        if record.tenant_id != tenant_id:
            raise AuthorizationError("MCP server is outside tenant scope")

    def _assert_writable(self, config: McpServerConfig, tenant_id: str) -> None:
        # Management authorization belongs to the authenticated upstream gateway.
        # A shared server has no tenant owner; the caller tenant is audit context,
        # not an administrator role. Tenant-owned records still retain their scope.
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


def _lifecycle_request_digest(
    server_id: str,
    command: McpServerLifecycleCommand,
    kind: McpRegistryOperationKind,
) -> str:
    payload = json.dumps(
        {
            "tenant_id": command.tenant_id,
            "server_id": server_id,
            "operation": kind.value,
            "expected_revision": command.expected_revision,
            "target_revision": command.target_revision,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _failure_result(exc: BaseException) -> dict[str, str]:
    payload = {"error_type": type(exc).__name__}
    message = getattr(exc, "message", None)
    if not isinstance(message, str) or not message:
        message = str(exc)
    if message:
        payload["error_message"] = message[:200]
    if isinstance(exc, AuraClawError):
        payload["error_code"] = exc.code
        if exc.detail:
            payload["error_detail"] = exc.detail[:200]
    return payload


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
