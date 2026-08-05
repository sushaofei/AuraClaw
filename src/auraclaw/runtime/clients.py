from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.control.ports import ControlStateStore, RuntimeAssignment
from auraclaw.runtime.ports import RuntimeEvent, ToolCall
from auraclaw.session.ports import EventStore


def assignment_resource_id(assignment: RuntimeAssignment) -> str:
    return f"session:{assignment.tenant_id}:{assignment.session_id}"


class FencedSessionClient:
    """Runtime-facing Session port; it never mutates aggregate state directly."""

    def __init__(self, event_store: EventStore, control_store: ControlStateStore) -> None:
        self._event_store = event_store
        self._control_store = control_store

    async def load(self, assignment: RuntimeAssignment) -> list[CanonicalEvent]:
        await self._control_store.assert_fencing(
            assignment_resource_id(assignment), assignment.fencing_token
        )
        return await self._event_store.load(assignment.tenant_id, assignment.session_id)

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: Sequence[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        await self._control_store.assert_fencing(
            assignment_resource_id(assignment), assignment.fencing_token
        )
        version = expected_version
        if version is None:
            current = await self._event_store.load(
                assignment.tenant_id, assignment.session_id
            )
            version = len(current)
        try:
            return await self._append_at(
                assignment,
                events,
                command_id=command_id,
                operation=operation,
                expected_version=version,
            )
        except VersionConflictError:
            if expected_version is None:
                raise
            current = await self._event_store.load(
                assignment.tenant_id, assignment.session_id
            )
            return await self._append_at(
                assignment,
                events,
                command_id=command_id,
                operation=operation,
                expected_version=len(current),
            )

    async def _append_at(
        self,
        assignment: RuntimeAssignment,
        events: Sequence[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int,
    ) -> list[CanonicalEvent]:
        result = await self._event_store.append(
            root_session_id=assignment.root_session_id,
            session_id=assignment.session_id,
            run_id=assignment.run_id,
            context=CommandContext(
                command_id=command_id,
                tenant_id=assignment.tenant_id,
                actor=Actor(type="runtime", id=assignment.runtime_id),
                correlation_id=assignment.run_id,
                expected_version=expected_version,
                operation=operation,
            ),
            events=events,
            command_result={"session_id": assignment.session_id, "run_id": assignment.run_id},
        )
        return result.events


class InMemoryRuntimeEventBus:
    def __init__(self, *, fail_publish: bool = False) -> None:
        self._events: list[RuntimeEvent] = []
        self._lock = asyncio.Lock()
        self._fail_publish = fail_publish

    async def publish(self, event: RuntimeEvent) -> None:
        if self._fail_publish:
            raise RuntimeError("runtime event bus unavailable")
        async with self._lock:
            self._events.append(event)

    async def events(self) -> list[RuntimeEvent]:
        async with self._lock:
            return list(self._events)


class IdempotentToolClient:
    """M2 boundary adapter; real side-effect execution is introduced in M3."""

    def __init__(self, handlers: dict[str, Any] | None = None) -> None:
        self._handlers = handlers or {}
        self._results: dict[str, dict[str, Any]] = {}
        self.calls = 0

    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> dict[str, Any]:
        del assignment
        previous = self._results.get(call.tool_invocation_id)
        if previous is not None:
            return dict(previous)
        self.calls += 1
        handler = self._handlers.get(call.name)
        if handler is None:
            result = {"ok": True, "tool": call.name, "arguments": call.arguments}
        else:
            value = handler(call.arguments)
            result = await value if hasattr(value, "__await__") else value
        normalized = dict(result)
        self._results[call.tool_invocation_id] = normalized
        return dict(normalized)


class FencedToolClient:
    """Execution boundary rejecting calls from a Runtime that lost ownership."""

    def __init__(self, delegate: IdempotentToolClient, control_store: ControlStateStore) -> None:
        self._delegate = delegate
        self._control_store = control_store

    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> dict[str, Any]:
        await self._control_store.assert_fencing(
            assignment_resource_id(assignment), assignment.fencing_token
        )
        return await self._delegate.execute(assignment, call)
