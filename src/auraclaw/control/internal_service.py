from __future__ import annotations

from datetime import UTC, datetime

from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.internal import (
    AssignmentClaimRequest,
    AssignmentClaimResponse,
    AssignmentDispositionRequest,
    AssignmentDispositionResponse,
    AssignmentRecord,
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    CheckpointState,
    LeaseAssertion,
    LoadCheckpointRequest,
    RuntimeHeartbeatRequest,
    RuntimeHeartbeatResponse,
    RuntimeRegistrationRequest,
    SaveCheckpointRequest,
    ServiceIdentity,
    ValidateLeaseRequest,
    ValidateLeaseResponse,
)
from auraclaw.control.ports import ControlStateStore, RuntimeCheckpoint, RuntimeInstance
from auraclaw.internal.security import LeaseAssertionSigner, LeaseAssertionVerifier


class ControlInternalService:
    def __init__(
        self,
        store: ControlStateStore,
        *,
        lease_verifier: LeaseAssertionVerifier,
        lease_signer: LeaseAssertionSigner | None = None,
    ) -> None:
        self._store = store
        self._lease_verifier = lease_verifier
        self._lease_signer = lease_signer

    @staticmethod
    def _require_runtime(identity: ServiceIdentity) -> None:
        if identity is not ServiceIdentity.AGENT_RUNTIME:
            raise AuthorizationError("operation is restricted to agent-runtime")

    async def save_checkpoint(self, request: SaveCheckpointRequest) -> CheckpointResponse:
        self._require_runtime(request.context.service_identity)
        assertion = request.lease_assertion
        await self._lease_verifier.verify(
            assertion,
            tenant_id=request.context.tenant_id,
            session_id=request.session_id,
            run_id=request.run_id,
        )
        updated_at = datetime.now(UTC)
        checkpoint = RuntimeCheckpoint(
            tenant_id=request.context.tenant_id,
            session_id=request.session_id,
            run_id=request.run_id,
            fencing_token=assertion.fencing_token,
            phase=request.state.phase,
            state={
                "resume_cursor": request.state.resume_cursor,
                "artifact_refs": list(request.state.artifact_refs),
                "harness_state": dict(request.state.harness_state),
            },
            updated_at=updated_at,
        )
        await self._store.save_checkpoint(checkpoint)
        return CheckpointResponse(
            found=True,
            fencing_token=checkpoint.fencing_token,
            state=request.state,
            updated_at=updated_at,
        )

    async def load_checkpoint(self, request: LoadCheckpointRequest) -> CheckpointResponse:
        self._require_runtime(request.context.service_identity)
        checkpoint = await self._store.load_checkpoint(
            request.context.tenant_id, request.session_id, request.run_id
        )
        if checkpoint is None:
            return CheckpointResponse(found=False)
        return CheckpointResponse(
            found=True,
            fencing_token=checkpoint.fencing_token,
            state=CheckpointState(
                phase=checkpoint.phase,
                resume_cursor=checkpoint.state.get("resume_cursor"),
                artifact_refs=tuple(checkpoint.state.get("artifact_refs", ())),
                harness_state=dict(checkpoint.state.get("harness_state", {})),
            ),
            updated_at=checkpoint.updated_at,
        )

    async def request_cancel(self, request: CancellationRequest) -> CancellationResponse:
        if request.context.service_identity not in {
            ServiceIdentity.TASK_API,
            ServiceIdentity.ORCHESTRATOR,
        }:
            raise AuthorizationError("service identity cannot request cancellation")
        await self._store.request_cancel(
            request.context.tenant_id, request.session_id, request.run_id
        )
        return CancellationResponse(cancelled=True)

    async def is_cancelled(self, request: CancellationRequest) -> CancellationResponse:
        self._require_runtime(request.context.service_identity)
        cancelled = await self._store.is_cancelled(
            request.context.tenant_id, request.session_id, request.run_id
        )
        return CancellationResponse(cancelled=cancelled)

    async def validate_lease(self, request: ValidateLeaseRequest) -> ValidateLeaseResponse:
        self._require_runtime(request.context.service_identity)
        assertion = request.assertion
        await self._lease_verifier.verify(
            assertion,
            tenant_id=request.context.tenant_id,
            session_id=assertion.session_id,
            run_id=assertion.run_id,
        )
        await self._store.assert_fencing(
            f"session:{assertion.tenant_id}:{assertion.session_id}",
            assertion.fencing_token,
        )
        return ValidateLeaseResponse(
            valid=True,
            fencing_token=assertion.fencing_token,
            expires_at=assertion.expires_at,
        )

    async def register_runtime(
        self, request: RuntimeRegistrationRequest
    ) -> RuntimeHeartbeatResponse:
        self._require_runtime(request.context.service_identity)
        await self._store.register_runtime(
            RuntimeInstance(
                runtime_id=request.runtime_id,
                runtime_type=request.runtime_type,
                role=request.role,
                node_id=request.node_id,
                capabilities=dict(request.capabilities),
                capacity=request.capacity,
            )
        )
        return RuntimeHeartbeatResponse(accepted=True, observed_at=datetime.now(UTC))

    async def heartbeat(
        self, request: RuntimeHeartbeatRequest
    ) -> RuntimeHeartbeatResponse:
        self._require_runtime(request.context.service_identity)
        await self._store.heartbeat(request.runtime_id)
        return RuntimeHeartbeatResponse(accepted=True, observed_at=datetime.now(UTC))

    async def claim_assignments(
        self, request: AssignmentClaimRequest
    ) -> AssignmentClaimResponse:
        self._require_runtime(request.context.service_identity)
        if self._lease_signer is None:
            raise AuthorizationError("lease assertion signer is unavailable")
        claimed = await self._store.claim_assignments(
            request.runtime_id, request.role, limit=request.limit
        )
        records: list[AssignmentRecord] = []
        for item in claimed:
            assignment = item.assignment
            if assignment.lease_expires_at is None:
                raise AuthorizationError("assignment lease expiry is unavailable")
            assertion = self._lease_signer.sign(
                LeaseAssertion(
                    key_id="pending",
                    audience="runtime",
                    tenant_id=assignment.tenant_id,
                    root_session_id=assignment.root_session_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    runtime_id=assignment.runtime_id,
                    user_id=assignment.user_id,
                    dept_id=assignment.dept_id,
                    lease_id=assignment.lease_id,
                    fencing_token=assignment.fencing_token,
                    expires_at=assignment.lease_expires_at,
                    signature="",
                )
            )
            records.append(
                AssignmentRecord(
                    task_id=item.task_id,
                    tenant_id=assignment.tenant_id,
                    root_session_id=assignment.root_session_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    runtime_id=assignment.runtime_id,
                    lease_assertion=assertion,
                    role=assignment.role,
                    resource_profile=dict(assignment.resource_profile),
                    budget={
                        "max_steps": assignment.budget.max_steps,
                        "max_output_tokens": assignment.budget.max_output_tokens,
                        "max_cost": assignment.budget.max_cost,
                    },
                    deadline=assignment.deadline,
                )
            )
        return AssignmentClaimResponse(assignments=tuple(records))

    async def disposition_assignment(
        self, request: AssignmentDispositionRequest
    ) -> AssignmentDispositionResponse:
        self._require_runtime(request.context.service_identity)
        assignment = await self._store.get_assignment(request.task_id)
        if (
            assignment is None
            or assignment.runtime_id != request.runtime_id
            or assignment.lease_id != request.lease_id
            or assignment.fencing_token != request.fencing_token
        ):
            raise AuthorizationError("assignment ownership does not match Runtime")
        await self._store.assert_fencing(
            f"session:{assignment.tenant_id}:{assignment.session_id}",
            request.fencing_token,
        )
        if request.disposition != "ack":
            await self._store.finish_assignment(
                request.task_id,
                "completed" if request.disposition == "finish" else "failed",
            )
        return AssignmentDispositionResponse(accepted=True)
