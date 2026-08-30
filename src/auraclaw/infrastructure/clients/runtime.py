from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx

from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import CanonicalEvent, NewEvent
from auraclaw.contracts.internal import (
    AssignmentAbandonRequest,
    AssignmentAbandonResponse,
    AssignmentClaimRequest,
    AssignmentClaimResponse,
    AssignmentDispositionRequest,
    AssignmentDispositionResponse,
    AssignmentRenewRequest,
    AssignmentRenewResponse,
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    CheckpointState,
    CollaborationCommandRequest,
    CollaborationCommandResponse,
    EventInput,
    InternalRequestContext,
    LoadCheckpointRequest,
    RuntimeHeartbeatRequest,
    RuntimeHeartbeatResponse,
    RuntimeRegistrationRequest,
    SaveCheckpointRequest,
    ServiceIdentity,
    SessionAppendRequest,
    SessionAppendResponse,
    SessionFeedRequest,
    SessionFeedResponse,
    ValidateLeaseRequest,
    ValidateLeaseResponse,
)
from auraclaw.control.ports import (
    DEFAULT_RUNTIME_MAX_STEPS,
    RuntimeAssignment,
    RuntimeBudget,
    RuntimeCheckpoint,
)
from auraclaw.infrastructure.clients.session import canonical_event_from_dict
from auraclaw.internal.http import HttpContractClient


def _context(tenant_id: str, request_id: str, correlation_id: str) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id=tenant_id,
        service_identity=ServiceIdentity.AGENT_RUNTIME,
        request_id=request_id,
        correlation_id=correlation_id,
        causation_id=request_id,
    )


def _assertion_expires_at(assignment: RuntimeAssignment) -> datetime | None:
    assertion = assignment.lease_assertion
    if assertion is None:
        return None
    expires_at = assertion.expires_at
    if expires_at.tzinfo is None:
        return expires_at.replace(tzinfo=UTC)
    return expires_at.astimezone(UTC)


def select_assignment_for_fence(
    assignments: dict[tuple[str, str, str], tuple[str, RuntimeAssignment]],
    resource_id: str,
    fencing_token: int,
    *,
    now: datetime | None = None,
) -> RuntimeAssignment | None:
    """Pick a still-valid assignment for a Session fence.

    The in-process cache keeps prior Runs of the same Session. Those entries often
    share fencing_token=1 after the lease row is re-inserted, so the first match
    can be an expired assertion and block follow-up Runs in `_guard`.
    """
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    live: list[RuntimeAssignment] = []
    for _task_id, assignment in assignments.values():
        if f"session:{assignment.tenant_id}:{assignment.session_id}" != resource_id:
            continue
        if assignment.fencing_token != fencing_token:
            continue
        expires_at = _assertion_expires_at(assignment)
        if expires_at is None or expires_at <= current:
            continue
        live.append(assignment)
    if not live:
        return None
    return max(live, key=lambda item: _assertion_expires_at(item) or current)


class RemoteRuntimeSessionClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def load(self, assignment: RuntimeAssignment) -> list[CanonicalEvent]:
        response = await self._contract.call(
            "/internal/v1/session/feed",
            SessionFeedRequest(
                context=_context(
                    assignment.tenant_id,
                    f"feed:{assignment.run_id}",
                    assignment.run_id,
                ),
                session_id=assignment.session_id,
                limit=1000,
            ),
            SessionFeedResponse,
        )
        return [canonical_event_from_dict(event) for event in response.events]
    async def append(
        self,
        assignment: RuntimeAssignment,
        events: Sequence[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        if assignment.lease_assertion is None:
            raise RuntimeError("Runtime assignment has no signed lease assertion")
        version = expected_version
        if version is None:
            current = await self.load(assignment)
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
            current = await self.load(assignment)
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
        assert assignment.lease_assertion is not None
        response = await self._contract.call(
            "/internal/v1/session/append",
            SessionAppendRequest(
                context=_context(
                    assignment.tenant_id, command_id, assignment.run_id
                ),
                root_session_id=assignment.root_session_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                command_id=command_id,
                expected_version=expected_version,
                operation=operation,
                actor_type="runtime",
                actor_id=assignment.runtime_id,
                events=tuple(
                    EventInput(
                        type=event.type,
                        payload=dict(event.payload),
                        visibility=event.visibility.value,
                    )
                    for event in events
                ),
                command_result={
                    "session_id": assignment.session_id,
                    "run_id": assignment.run_id,
                },
                lease_assertion=assignment.lease_assertion,
            ),
            SessionAppendResponse,
        )
        return [canonical_event_from_dict(event) for event in response.events]


class RemoteCollaborationClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def execute(
        self,
        assignment: RuntimeAssignment,
        *,
        operation: str,
        arguments: dict[str, Any],
        command_id: str,
    ) -> dict[str, Any]:
        if assignment.lease_assertion is None:
            raise RuntimeError("Runtime assignment has no signed lease assertion")
        response = await self._contract.call(
            "/internal/v1/collaboration/command",
            CollaborationCommandRequest(
                context=_context(
                    assignment.tenant_id, command_id, assignment.run_id
                ),
                lease_assertion=assignment.lease_assertion,
                root_session_id=assignment.root_session_id,
                session_id=assignment.session_id,
                command_id=command_id,
                operation=operation,
                arguments=dict(arguments),
            ),
            CollaborationCommandResponse,
        )
        return dict(response.result)


class RemoteOrchestratorSessionClient:
    """Orchestrator-only lifecycle writer with no Session database access."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _context(tenant_id: str, request_id: str, correlation_id: str) -> InternalRequestContext:
        return InternalRequestContext(
            tenant_id=tenant_id,
            service_identity=ServiceIdentity.ORCHESTRATOR,
            request_id=request_id,
            correlation_id=correlation_id,
            causation_id=request_id,
        )

    async def load(self, assignment: RuntimeAssignment) -> list[CanonicalEvent]:
        response = await self._contract.call(
            "/internal/v1/session/feed",
            SessionFeedRequest(
                context=self._context(
                    assignment.tenant_id,
                    f"orchestrator-feed:{assignment.run_id}",
                    assignment.run_id,
                ),
                session_id=assignment.session_id,
                limit=1000,
            ),
            SessionFeedResponse,
        )
        return [canonical_event_from_dict(event) for event in response.events]

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: Sequence[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        version = expected_version
        if version is None:
            current = await self.load(assignment)
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
            current = await self.load(assignment)
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
        response = await self._contract.call(
            "/internal/v1/session/append",
            SessionAppendRequest(
                context=self._context(
                    assignment.tenant_id, command_id, assignment.run_id
                ),
                root_session_id=assignment.root_session_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                command_id=command_id,
                expected_version=expected_version,
                operation=operation,
                actor_type="orchestrator",
                actor_id="orchestrator",
                events=tuple(
                    EventInput(
                        type=event.type,
                        payload=dict(event.payload),
                        visibility=event.visibility.value,
                    )
                    for event in events
                ),
                command_result={
                    "session_id": assignment.session_id,
                    "run_id": assignment.run_id,
                },
            ),
            SessionAppendResponse,
        )
        return [canonical_event_from_dict(event) for event in response.events]

class RemoteRuntimeControlClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        runtime_id: str,
        role: str,
        node_id: str,
        capacity: int,
        registration_id: str | None = None,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.runtime_id = runtime_id
        self.role = role
        self.node_id = node_id
        self.capacity = capacity
        self.registration_id = registration_id or uuid4().hex
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._assignments: dict[tuple[str, str, str], tuple[str, RuntimeAssignment]] = {}

    async def aclose(self) -> None:
        await self._client.aclose()

    async def register(self) -> None:
        await self._contract.call(
            "/internal/v1/control/runtimes/register",
            RuntimeRegistrationRequest(
                context=_context("system", f"register:{self.runtime_id}", self.runtime_id),
                runtime_id=self.runtime_id,
                runtime_type="agent",
                role=self.role,
                node_id=self.node_id,
                capacity=self.capacity,
                registration_id=self.registration_id,
            ),
            RuntimeHeartbeatResponse,
        )

    async def heartbeat(self) -> None:
        await self._contract.call(
            "/internal/v1/control/runtimes/heartbeat",
            RuntimeHeartbeatRequest(
                context=_context("system", f"heartbeat:{self.runtime_id}", self.runtime_id),
                runtime_id=self.runtime_id,
                capacity_available=self.capacity,
                registration_id=self.registration_id,
            ),
            RuntimeHeartbeatResponse,
        )

    async def claim(self, *, limit: int = 1) -> list[RuntimeAssignment]:
        response = await self._contract.call(
            "/internal/v1/control/assignments/claim",
            AssignmentClaimRequest(
                context=_context("system", f"claim:{self.runtime_id}", self.runtime_id),
                runtime_id=self.runtime_id,
                role=self.role,
                registration_id=self.registration_id,
                limit=limit,
            ),
            AssignmentClaimResponse,
        )
        assignments: list[RuntimeAssignment] = []
        for record in response.assignments:
            budget = dict(record.budget)
            assignment = RuntimeAssignment(
                tenant_id=record.tenant_id,
                root_session_id=record.root_session_id,
                session_id=record.session_id,
                run_id=record.run_id,
                runtime_id=record.runtime_id,
                lease_id=record.lease_assertion.lease_id,
                fencing_token=record.lease_assertion.fencing_token,
                role=record.role,
                resource_profile=dict(record.resource_profile),
                deadline=record.deadline,
                budget=RuntimeBudget(
                    max_steps=int(budget.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
                    max_output_tokens=int(budget.get("max_output_tokens", 8192)),
                    max_cost=(
                        float(budget["max_cost"])
                        if budget.get("max_cost") is not None
                        else None
                    ),
                ),
                lease_expires_at=record.lease_assertion.expires_at,
                lease_assertion=record.lease_assertion,
                user_id=record.lease_assertion.user_id,
                dept_id=record.lease_assertion.dept_id,
                execution_claim_token=record.execution_claim_token,
                execution_claim_expires_at=record.execution_claim_expires_at,
            )
            self._assignments[
                (assignment.tenant_id, assignment.session_id, assignment.run_id)
            ] = (record.task_id, assignment)
            assignments.append(assignment)
        return assignments

    async def renew_assignment(self, assignment: RuntimeAssignment) -> None:
        task_id, _ = self._assignment(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        )
        if assignment.execution_claim_token is None:
            raise RuntimeError("Runtime assignment has no execution claim")
        response = await self._contract.call(
            "/internal/v1/control/assignments/renew",
            AssignmentRenewRequest(
                context=_context(
                    assignment.tenant_id,
                    f"renew:{task_id}",
                    assignment.run_id,
                ),
                task_id=task_id,
                runtime_id=assignment.runtime_id,
                registration_id=self.registration_id,
                execution_claim_token=assignment.execution_claim_token,
                lease_id=assignment.lease_id,
                fencing_token=assignment.fencing_token,
            ),
            AssignmentRenewResponse,
        )
        assignment.lease_assertion = response.lease_assertion
        assignment.lease_expires_at = response.lease_assertion.expires_at
        assignment.execution_claim_expires_at = response.execution_claim_expires_at

    def _assignment(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> tuple[str, RuntimeAssignment]:
        try:
            return self._assignments[(tenant_id, session_id, run_id)]
        except KeyError as exc:
            raise RuntimeError("Runtime does not own this assignment") from exc

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
        match = select_assignment_for_fence(
            self._assignments, resource_id, fencing_token
        )
        if match is None or match.lease_assertion is None:
            raise RuntimeError("Runtime does not own this fencing token")
        await self._contract.call(
            "/internal/v1/control/leases/validate",
            ValidateLeaseRequest(
                context=_context(match.tenant_id, f"validate:{match.lease_id}", match.run_id),
                assertion=match.lease_assertion,
            ),
            ValidateLeaseResponse,
        )

    async def is_cancelled(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> bool:
        response = await self._contract.call(
            "/internal/v1/control/cancellation/status",
            CancellationRequest(
                context=_context(tenant_id, f"cancel-status:{run_id}", run_id),
                session_id=session_id,
                run_id=run_id,
            ),
            CancellationResponse,
        )
        return response.cancelled

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        _, assignment = self._assignment(
            checkpoint.tenant_id, checkpoint.session_id, checkpoint.run_id
        )
        if assignment.lease_assertion is None:
            raise RuntimeError("Runtime assignment has no signed lease assertion")
        await self._contract.call(
            "/internal/v1/control/checkpoints/save",
            SaveCheckpointRequest(
                context=_context(
                    checkpoint.tenant_id,
                    f"checkpoint:{checkpoint.run_id}:{checkpoint.phase}",
                    checkpoint.run_id,
                ),
                session_id=checkpoint.session_id,
                run_id=checkpoint.run_id,
                lease_assertion=assignment.lease_assertion,
                state=CheckpointState(
                    phase=checkpoint.phase,
                    harness_state=dict(checkpoint.state),
                ),
            ),
            CheckpointResponse,
        )

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None:
        response = await self._contract.call(
            "/internal/v1/control/checkpoints/load",
            LoadCheckpointRequest(
                context=_context(tenant_id, f"checkpoint-load:{run_id}", run_id),
                session_id=session_id,
                run_id=run_id,
            ),
            CheckpointResponse,
        )
        if not response.found or response.state is None:
            return None
        return RuntimeCheckpoint(
            tenant_id=tenant_id,
            session_id=session_id,
            run_id=run_id,
            fencing_token=response.fencing_token or 0,
            phase=response.state.phase,
            state=dict(response.state.harness_state),
            updated_at=response.updated_at or datetime.now(UTC),
        )

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        entry = next(
            (
                assignment
                for known_task_id, assignment in self._assignments.values()
                if known_task_id == task_id
            ),
            None,
        )
        if entry is None:
            raise RuntimeError("Runtime does not own this task")
        disposition = (
            "finish"
            if outcome == "completed"
            else "fail" if outcome == "failed" else "ack"
        )
        await self._contract.call(
            "/internal/v1/control/assignments/disposition",
            AssignmentDispositionRequest(
                context=_context(entry.tenant_id, f"finish:{task_id}", entry.run_id),
                task_id=task_id,
                runtime_id=entry.runtime_id,
                lease_id=entry.lease_id,
                fencing_token=entry.fencing_token,
                disposition=disposition,
                outcome=outcome,
                execution_claim_token=entry.execution_claim_token or "",
            ),
            AssignmentDispositionResponse,
        )
        for key, (known_task_id, _) in list(self._assignments.items()):
            if known_task_id == task_id:
                self._assignments.pop(key, None)

    async def abandon_assignment(
        self,
        task_id: str,
        *,
        runtime_id: str,
        lease_id: str,
        fencing_token: int,
    ) -> bool:
        entry = next(
            (
                assignment
                for known_task_id, assignment in self._assignments.values()
                if known_task_id == task_id
            ),
            None,
        )
        if entry is None:
            raise RuntimeError("Runtime does not own this task")
        response = await self._contract.call(
            "/internal/v1/control/assignments/abandon",
            AssignmentAbandonRequest(
                context=_context(entry.tenant_id, f"abandon:{task_id}", entry.run_id),
                task_id=task_id,
                runtime_id=runtime_id,
                lease_id=lease_id,
                fencing_token=fencing_token,
                execution_claim_token=entry.execution_claim_token,
            ),
            AssignmentAbandonResponse,
        )
        if response.accepted:
            for key, (known_task_id, _) in list(self._assignments.items()):
                if known_task_id == task_id:
                    self._assignments.pop(key, None)
        return response.accepted

    async def suspend_assignment(self, task_id: str, reason: str) -> None:
        entry = next(
            (
                assignment
                for known_task_id, assignment in self._assignments.values()
                if known_task_id == task_id
            ),
            None,
        )
        if entry is None:
            raise RuntimeError("Runtime does not own this task")
        await self._contract.call(
            "/internal/v1/control/assignments/disposition",
            AssignmentDispositionRequest(
                context=_context(entry.tenant_id, f"suspend:{task_id}", entry.run_id),
                task_id=task_id,
                runtime_id=entry.runtime_id,
                lease_id=entry.lease_id,
                fencing_token=entry.fencing_token,
                disposition="suspend",
                outcome=reason,
                execution_claim_token=entry.execution_claim_token or "",
            ),
            AssignmentDispositionResponse,
        )
        for key, (known_task_id, _) in list(self._assignments.items()):
            if known_task_id == task_id:
                self._assignments.pop(key, None)
