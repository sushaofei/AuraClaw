from __future__ import annotations

from datetime import UTC, datetime

from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.internal import (
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    CheckpointState,
    LoadCheckpointRequest,
    SaveCheckpointRequest,
    ServiceIdentity,
    ValidateLeaseRequest,
    ValidateLeaseResponse,
)
from auraclaw.control.ports import ControlStateStore, RuntimeCheckpoint
from auraclaw.internal.security import LeaseAssertionVerifier


class ControlInternalService:
    def __init__(
        self,
        store: ControlStateStore,
        *,
        lease_verifier: LeaseAssertionVerifier,
    ) -> None:
        self._store = store
        self._lease_verifier = lease_verifier

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
