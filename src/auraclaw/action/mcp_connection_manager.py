from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable, MutableMapping
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from auraclaw.action.bounded import bounded_partition_map
from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.catalog_reconciler import (
    CapabilityCatalogReconciler,
    McpReconcileResult,
)
from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.action.ports import CapabilityConnector
from auraclaw.action.tool_gateway import JsonSchemaValidator
from auraclaw.contracts.capabilities import (
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.errors import (
    AuraClawError,
    AuthorizationError,
    InvalidTransitionError,
    NotFoundError,
    SchemaValidationError,
)
from auraclaw.contracts.hands import HandsTrustedContext
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpServerRuntimeRecord,
)

McpConnectorFactory = Callable[[McpServerDefinition], CapabilityConnector]
logger = logging.getLogger(__name__)


class McpEgressLoader(Protocol):
    async def apply(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def revoke(self, server_id: str, *, expected_revision: int | None = None) -> None: ...


class McpConnectionManager:
    """Loads active MCP revisions into Action Hands without process restart."""

    def __init__(
        self,
        *,
        registry: McpServerRegistryService,
        connectors: MutableMapping[str, CapabilityConnector],
        factory: McpConnectorFactory,
        catalog: CapabilityCatalog | None = None,
        reconciler: CapabilityCatalogReconciler | None = None,
        egress: McpEgressLoader | None = None,
        drain_seconds: float = 5.0,
        instance_id: str = "legacy",
        max_concurrent: int = 8,
        max_concurrent_per_tenant: int | None = None,
        max_concurrent_per_host: int | None = None,
        server_timeout_seconds: float = 60.0,
    ) -> None:
        tenant_limit = (
            max_concurrent if max_concurrent_per_tenant is None else max_concurrent_per_tenant
        )
        host_limit = max_concurrent if max_concurrent_per_host is None else max_concurrent_per_host
        if (
            max_concurrent < 1
            or not 1 <= tenant_limit <= max_concurrent
            or not 1 <= host_limit <= max_concurrent
            or server_timeout_seconds <= 0
        ):
            raise ValueError("MCP connection capacity and timeout must be positive")
        self._registry = registry
        self._connectors = connectors
        self._factory = factory
        self._catalog = catalog
        self._reconciler = reconciler
        self._egress = egress
        self._drain_seconds = drain_seconds
        self._instance_id = instance_id
        self._generations: dict[str, int] = {}
        self._pending_revokes: dict[str, int | None] = {}
        self._closing: dict[str, CapabilityConnector] = {}
        self._draining: list[CapabilityConnector] = []
        self._drain_locks: dict[int, asyncio.Lock] = {}
        self._max_concurrent = max_concurrent
        self._max_concurrent_per_tenant = tenant_limit
        self._max_concurrent_per_host = host_limit
        self._server_timeout_seconds = server_timeout_seconds
        self._server_locks: dict[str, asyncio.Lock] = {}

    async def restore(self) -> int:
        snapshot = await self._registry.active_snapshot()
        results = await bounded_partition_map(
            snapshot,
            lambda entry: self._apply_isolated(entry, restore=True),
            max_concurrent=self._max_concurrent,
            partitions=(
                (
                    lambda entry: entry.tenant_id or "platform",
                    self._max_concurrent_per_tenant,
                ),
                (
                    lambda entry: urlsplit(entry.config.endpoint).hostname or entry.server_id,
                    self._max_concurrent_per_host,
                ),
            ),
        )
        return sum(results)

    async def test(self, entry: McpActiveSnapshotEntry, *, persist_egress: bool = False) -> None:
        keep_egress = persist_egress or self._generations.get(entry.server_id) == entry.revision
        if self._egress is not None:
            await self._egress.apply(entry)
        connector = self._factory(self._definition(entry, enabled=True))
        try:
            await connector.snapshot(_probe_context(entry))
        except BaseException:
            if self._egress is not None and not keep_egress:
                await self._egress.revoke(entry.server_id, expected_revision=entry.revision)
            raise
        finally:
            await connector.aclose()
        if self._egress is not None and not keep_egress:
            await self._egress.revoke(entry.server_id, expected_revision=entry.revision)
        await self._record_tested(entry)

    async def test_capability(
        self,
        *,
        tenant_id: str,
        actor_id: str,
        dept_id: str | None,
        server_id: str,
        capability_id: str,
        input_payload: dict[str, Any],
        expected_output: Any = None,
    ) -> dict[str, Any]:
        """Run an explicit, read-only capability probe through the managed connector."""
        if self._catalog is None:
            raise NotFoundError("MCP capability catalog is unavailable")
        capability = await self._catalog.get(tenant_id=tenant_id, capability_id=capability_id)
        if capability is None or capability.server_id != server_id:
            raise NotFoundError("MCP capability was not found")
        connector = self._connectors.get(server_id)
        if connector is None:
            raise InvalidTransitionError("MCP server is not active on this runtime")

        source = capability.metadata.get("source")
        source = source if isinstance(source, dict) else {}
        trusted = HandsTrustedContext(
            tenant_id=tenant_id,
            root_session_id="mcp-capability-test",
            session_id="mcp-capability-test",
            run_id=f"mcp-capability-test-{uuid4()}",
            runtime_id=self._instance_id,
            lease_id="mcp-capability-test",
            fencing_token=1,
            user_id=actor_id,
            dept_id=dept_id,
        )
        started = monotonic()
        schema_valid: bool | None = None
        error: str | None = None

        if capability.kind is CapabilityKind.TOOL:
            if capability.permission != "read-only":
                raise AuthorizationError(
                    "Only read-only MCP tools can be invoked from capability tests"
                )
            input_schema = source.get("inputSchema")
            if isinstance(input_schema, dict):
                JsonSchemaValidator.validate(input_payload, input_schema)
            result = await connector.call_tool(
                trusted,
                name=capability.canonical_name,
                arguments=input_payload,
                invocation_id=f"mcp-test-{uuid4()}",
            )
            output: Any = result.content
            if output is None:
                output = result.as_dict()
            if result.status != "success":
                error = result.summary or result.error_code or "MCP Tool returned an error"
            output_schema = source.get("outputSchema")
            if isinstance(output_schema, dict) and output_schema:
                try:
                    JsonSchemaValidator.validate(output, output_schema)
                    schema_valid = True
                except SchemaValidationError as exc:
                    schema_valid = False
                    error = exc.message
        elif capability.kind in {
            CapabilityKind.RESOURCE,
            CapabilityKind.RESOURCE_TEMPLATE,
        }:
            configured_uri = source.get("uri")
            uri = input_payload.get("uri", configured_uri)
            if not isinstance(uri, str) or not uri:
                raise SchemaValidationError(
                    "Resource template tests require input.uri with a concrete URI"
                )
            if capability.kind is CapabilityKind.RESOURCE:
                allowed = uri == configured_uri
            else:
                template = source.get("uri_template") or source.get("uriTemplate")
                allowed = isinstance(template, str) and _matches_test_uri(template, uri)
            if not allowed:
                raise AuthorizationError("Resource test URI must match the selected capability")
            contents = await connector.read_resource(trusted, uri)
            output = [item.model_dump(mode="json") for item in contents]
        elif capability.kind is CapabilityKind.PROMPT:
            if any(not isinstance(value, str) for value in input_payload.values()):
                raise SchemaValidationError("Prompt test arguments must be strings")
            prompt_result = await connector.get_prompt(
                trusted,
                capability.canonical_name,
                arguments={key: str(value) for key, value in input_payload.items()},
            )
            output = prompt_result.model_dump(mode="json")
        else:
            raise InvalidTransitionError("This capability kind cannot be tested")

        expectation_matched = (
            None if expected_output is None else _contains_expected(output, expected_output)
        )
        passed = error is None and expectation_matched is not False
        return {
            "status": "passed" if passed else "failed",
            "kind": capability.kind.value,
            "output": output,
            "schema_valid": schema_valid,
            "expectation_matched": expectation_matched,
            "duration_ms": max(0, round((monotonic() - started) * 1000)),
            "error": error,
        }

    async def apply(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
        tested: bool = False,
    ) -> None:
        async with self._server_locks.setdefault(entry.server_id, asyncio.Lock()):
            if self._generations.get(entry.server_id, 0) > entry.revision:
                return
            await self._apply(entry, restore=restore, tested=tested)
            desired = {item.server_id: item for item in await self._registry.active_snapshot()}
            current = desired.get(entry.server_id)
            if current is None or current.revision != entry.revision:
                if self._reconciler is not None:
                    self._reconciler.block_server(entry.server_id)
                self._pending_revokes[entry.server_id] = entry.revision
                await self._registry.record_runtime(
                    McpServerRuntimeRecord(
                        server_id=entry.server_id,
                        instance_id=self._instance_id,
                        loaded_revision=entry.revision,
                        observed_state=McpObservedState.DEGRADED,
                        safe_error_code="mcp_apply_superseded",
                        updated_at=datetime.now(UTC),
                    )
                )
                raise InvalidTransitionError("MCP apply was superseded by a desired-state change")
            self._pending_revokes.pop(entry.server_id, None)
            previous = self._closing.pop(entry.server_id, None)
            if previous is not None:
                self._draining.append(previous)
                asyncio.create_task(self._drain_safely(previous))

    async def _apply(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
        tested: bool = False,
    ) -> None:
        if not restore and not tested:
            await self.test(entry, persist_egress=True)
        elif self._egress is not None:
            await self._egress.apply(entry)
        previous = self._connectors.get(entry.server_id)
        definition = self._definition(entry, enabled=True)
        connector = self._factory(definition)
        if self._reconciler is not None:
            setter = getattr(connector, "set_notification_handler", None)
            if setter is not None:
                setter(self._reconciler.handle_notification)
        self._connectors[entry.server_id] = connector
        self._generations[entry.server_id] = entry.revision
        if self._catalog is not None:
            await self._catalog.register_server(definition)
        now = datetime.now(UTC)
        tested_at = None if restore else now
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=entry.server_id,
                instance_id=self._instance_id,
                loaded_revision=entry.revision,
                observed_state=(McpObservedState.LOADING),
                last_test_at=tested_at,
                updated_at=now,
            )
        )
        if previous is not None and previous is not connector:
            self._draining.append(previous)
            asyncio.create_task(self._drain_safely(previous))
        if self._reconciler is not None:
            result = await self._reconciler.reconcile_server(definition)
            observed = {
                CapabilityStatus.ACTIVE: McpObservedState.ACTIVE,
                CapabilityStatus.DEGRADED: McpObservedState.DEGRADED,
                CapabilityStatus.QUARANTINED: McpObservedState.QUARANTINED,
            }.get(result.status, McpObservedState.QUARANTINED)
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=entry.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=entry.revision,
                    observed_state=observed,
                    last_test_at=tested_at,
                    last_sync_at=datetime.now(UTC),
                    consecutive_failures=result.consecutive_failures,
                    safe_error_code=(None if result.error is None else "mcp_catalog_degraded"),
                    updated_at=datetime.now(UTC),
                )
            )

    async def revoke(self, server_id: str) -> None:
        async with self._server_locks.setdefault(server_id, asyncio.Lock()):
            desired = {entry.server_id: entry for entry in await self._registry.active_snapshot()}
            if server_id in desired:
                return
            revision = self._pending_revokes.setdefault(server_id, self._generations.get(server_id))
            if self._reconciler is not None:
                self._reconciler.block_server(server_id)
            previous = self._connectors.pop(server_id, None)
            if previous is not None:
                self._closing[server_id] = previous
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=server_id,
                    instance_id=self._instance_id,
                    loaded_revision=revision,
                    observed_state=McpObservedState.DEGRADED,
                    safe_error_code="mcp_revocation_pending",
                    updated_at=datetime.now(UTC),
                )
            )
            # The generation/pending entry is retained across every failing await.
            if self._reconciler is not None:
                await self._reconciler.drop_server(server_id)
            if self._egress is not None:
                await self._egress.revoke(server_id, expected_revision=revision)
            previous = self._closing.get(server_id)
            if previous is not None:
                await self._drain(previous)
                self._closing.pop(server_id, None)
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=server_id,
                    instance_id=self._instance_id,
                    loaded_revision=None,
                    observed_state=McpObservedState.DISABLED,
                    updated_at=datetime.now(UTC),
                )
            )
            self._generations.pop(server_id, None)
            self._pending_revokes.pop(server_id, None)

    async def purge(self, server_id: str) -> None:
        await self.revoke(server_id)
        if any(item.server_id == server_id for item in await self._registry.active_snapshot()):
            raise InvalidTransitionError("MCP purge was superseded by an enabled configuration")
        if self._catalog is not None:
            await self._catalog.remove_server(server_id)

    async def reconcile_loaded(self) -> int:
        for connector in tuple(self._draining):
            await self._drain_safely(connector)
        snapshot = {entry.server_id: entry for entry in await self._registry.active_snapshot()}
        operations: list[tuple[str, McpActiveSnapshotEntry | str]] = []
        scheduled: set[str] = set()
        local_ids = set(self._generations) | set(self._connectors) | set(self._pending_revokes)
        for server_id in local_ids:
            revision = self._generations.get(server_id)
            desired = snapshot.get(server_id)
            if desired is None:
                operations.append(("revoke", server_id))
                scheduled.add(server_id)
                continue
            if desired.revision != revision or server_id in self._pending_revokes:
                operations.append(("apply", desired))
                scheduled.add(server_id)
        for server_id, entry in snapshot.items():
            if server_id not in scheduled and self._generations.get(server_id) != entry.revision:
                operations.append(("apply", entry))

        async def execute(operation: tuple[str, McpActiveSnapshotEntry | str]) -> bool:
            action, target = operation
            if action == "revoke":
                assert isinstance(target, str)
                return await self._revoke_isolated(target)
            assert not isinstance(target, str)
            return await self._apply_isolated(target)

        results = await bounded_partition_map(
            operations,
            execute,
            max_concurrent=self._max_concurrent,
            partitions=(
                (
                    lambda operation: (
                        operation[1].tenant_id or "platform"
                        if isinstance(operation[1], McpActiveSnapshotEntry)
                        else operation[1]
                    ),
                    self._max_concurrent_per_tenant,
                ),
                (
                    lambda operation: (
                        urlsplit(operation[1].config.endpoint).hostname or operation[1].server_id
                        if isinstance(operation[1], McpActiveSnapshotEntry)
                        else operation[1]
                    ),
                    self._max_concurrent_per_host,
                ),
            ),
        )
        return sum(results)

    async def record_reconcile_results(self, results: tuple[McpReconcileResult, ...]) -> None:
        """Heartbeat this instance's catalog load state without global last-writer state."""
        now = datetime.now(UTC)
        for result in results:
            revision = self._generations.get(result.server_id)
            if revision is None:
                continue
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=result.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=revision,
                    observed_state=(
                        McpObservedState.ACTIVE
                        if result.status is CapabilityStatus.ACTIVE
                        else McpObservedState.QUARANTINED
                        if result.status is CapabilityStatus.QUARANTINED
                        else McpObservedState.DEGRADED
                    ),
                    last_sync_at=now,
                    consecutive_failures=result.consecutive_failures,
                    safe_error_code=(None if result.error is None else "mcp_catalog_degraded"),
                    updated_at=now,
                )
            )

    async def _apply_isolated(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
    ) -> bool:
        try:
            await asyncio.wait_for(
                self.apply(entry, restore=restore),
                timeout=self._server_timeout_seconds,
            )
            return True
        except Exception as exc:
            logger.warning(
                "MCP server %s is unreachable; continuing with other servers (%s)",
                entry.server_id,
                type(exc).__name__,
            )
            await self._record_unavailable(entry, exc)
            return False

    async def _revoke_isolated(self, server_id: str) -> bool:
        try:
            await asyncio.wait_for(self.revoke(server_id), timeout=self._server_timeout_seconds)
            return True
        except Exception as exc:
            logger.warning(
                "MCP server %s revoke failed; continuing with other servers (%s)",
                server_id,
                type(exc).__name__,
            )
            return False

    async def _record_unavailable(self, entry: McpActiveSnapshotEntry, exc: BaseException) -> None:
        safe_error_code = (
            exc.code
            if isinstance(exc, AuraClawError) and isinstance(exc.code, str)
            else "mcp_connection_test_failed"
        )
        try:
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=entry.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=self._generations.get(entry.server_id),
                    observed_state=McpObservedState.UNAVAILABLE,
                    consecutive_failures=1,
                    safe_error_code=safe_error_code,
                    updated_at=datetime.now(UTC),
                )
            )
        except Exception:
            logger.warning(
                "failed to persist unavailable state for MCP server %s",
                entry.server_id,
            )

    async def _record_tested(self, entry: McpActiveSnapshotEntry) -> None:
        now = datetime.now(UTC)
        record = await self._registry.get_server(
            tenant_id=entry.tenant_id or "platform",
            server_id=entry.server_id,
            actor_id="mcp-admin",
        )
        previous = record.runtime
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=entry.server_id,
                instance_id=self._instance_id,
                loaded_revision=(previous.loaded_revision if previous is not None else None),
                observed_state=(
                    previous.observed_state if previous is not None else McpObservedState.PENDING
                ),
                last_test_at=now,
                last_sync_at=previous.last_sync_at if previous is not None else None,
                consecutive_failures=0,
                safe_error_code=None,
                updated_at=now,
            )
        )

    def _definition(self, entry: McpActiveSnapshotEntry, *, enabled: bool) -> McpServerDefinition:
        return entry.config.materialize(
            revision=entry.revision,
            desired_state=(McpDesiredState.ENABLED if enabled else McpDesiredState.DISABLED),
            observed_state=entry.observed_state,
        ).model_copy(update={"enabled": enabled, "status": CapabilityStatus.ACTIVE})

    async def _drain_safely(self, connector: Any) -> None:
        try:
            await self._drain(connector)
        except Exception:
            logger.warning("MCP connector close pending; retry on next reconciliation")

    async def _drain(self, connector: Any) -> None:
        async with self._drain_locks.setdefault(id(connector), asyncio.Lock()):
            await self._close_connector(connector)

    async def _close_connector(self, connector: Any) -> None:
        await asyncio.sleep(self._drain_seconds)
        close = getattr(connector, "aclose", None)
        if close is not None:
            await close()
        if connector in self._draining:
            self._draining.remove(connector)


def _probe_context(entry: McpActiveSnapshotEntry) -> Any:
    from auraclaw.contracts.hands import HandsTrustedContext

    return HandsTrustedContext(
        tenant_id=entry.tenant_id or "platform",
        root_session_id="mcp-test",
        session_id="mcp-test",
        run_id="mcp-test",
        runtime_id="mcp-test",
        lease_id="mcp-test",
        fencing_token=1,
        deadline=None,
        user_id="mcp-admin",
    )


def _contains_expected(actual: Any, expected: Any) -> bool:
    """Return whether actual contains the expected JSON fragment recursively."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _contains_expected(actual[key], value)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) >= len(expected)
            and all(
                _contains_expected(actual[index], value) for index, value in enumerate(expected)
            )
        )
    # JSON booleans must not compare equal to numbers (True == 1 in Python).
    if isinstance(actual, bool) != isinstance(expected, bool):
        return False
    return bool(actual == expected)


def _matches_test_uri(template: str, uri: str) -> bool:
    """Support simple path variables; reject other URI-template operators."""
    escaped = re.escape(template)
    pattern = re.sub(r"\\\{[A-Za-z0-9_.-]+\\\}", r"[^/?#]+", escaped)
    return re.fullmatch(pattern, uri) is not None
