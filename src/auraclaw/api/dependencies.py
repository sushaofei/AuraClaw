from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from fastapi import Header, Request

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.contracts.identity import (
    IdentityErrorReason,
    IdentityVerificationRequest,
    VerifiedIdentityEnvelope,
    assertion_jti_digest,
    identity_error,
)
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.observability.service import ObservabilityService
from auraclaw.projection.ports import CollaborationReader, TaskReader


@dataclass(frozen=True)
class RequestIdentity:
    tenant_id: str
    actor: Actor
    correlation_id: str
    session_id: str | None = None
    caller_subject: str | None = None
    key_id: str | None = None
    jti_digest: str | None = None


def _declared_identity_fields(payload: Any) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None
    tenant = payload.get("tenant_id")
    user = payload.get("user_id")
    if user is None:
        user = payload.get("actor_id")
    return (
        str(tenant) if tenant is not None else None,
        str(user) if user is not None else None,
    )


async def request_identity(
    request: Request,
    authorization: str | None = Header(default=None),
    agent_context: str | None = Header(default=None, alias="X-CT-Agent-Context"),
    tenant_id: str | None = Header(default=None, alias="X-Tenant-ID"),
    actor_id: str | None = Header(default=None, alias="X-Actor-ID"),
    correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> RequestIdentity:
    verifier = getattr(request.app.state, "identity_verifier", None)
    if verifier is None:
        raise identity_error(
            "identity verifier is not configured",
            reason=IdentityErrorReason.VERIFIER_UNAVAILABLE,
        )
    query_tenant = request.query_params.get("tenant_id")
    query_user = request.query_params.get("user_id") or request.query_params.get(
        "actor_id"
    )
    body_tenant: str | None = None
    body_user: str | None = None
    if request.method in {"POST", "PUT", "PATCH"}:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            try:
                body_tenant, body_user = _declared_identity_fields(await request.json())
            except Exception:
                body_tenant, body_user = None, None
    declared_tenants = {
        value
        for value in (tenant_id, query_tenant, body_tenant)
        if value is not None
    }
    declared_users = {
        value for value in (actor_id, query_user, body_user) if value is not None
    }
    if len(declared_tenants) > 1 or len(declared_users) > 1:
        raise identity_error(
            "declared tenant or user is inconsistent",
            reason=IdentityErrorReason.TENANT_SESSION_MISMATCH,
        )
    declared_tenant = next(iter(declared_tenants), None)
    declared_user = next(iter(declared_users), None)
    write = request.method in {"POST", "PUT", "PATCH", "DELETE"}
    envelope: VerifiedIdentityEnvelope = await verifier.verify(
        IdentityVerificationRequest(
            workload_credential=authorization,
            assertion=agent_context,
            declared_tenant_id=declared_tenant,
            declared_user_id=declared_user,
            bound_session_id=request.path_params.get("session_id"),
            command_id=idempotency_key if write else None,
            correlation_id=correlation_id,
            operation="write" if write else "read",
        )
    )
    identity = RequestIdentity(
        tenant_id=envelope.user.tenant_id,
        actor=Actor(type="user", id=envelope.user.user_id),
        correlation_id=correlation_id or f"corr_{uuid4().hex}",
        session_id=envelope.user.session_id,
        caller_subject=envelope.caller.subject,
        key_id=None if envelope.assertion is None else envelope.assertion.key_id,
        jti_digest=(
            None
            if envelope.assertion is None
            else assertion_jti_digest(envelope.assertion.jti)
        ),
    )
    request.state.tenant_id = identity.tenant_id
    request.state.user_id = identity.actor.id
    request.state.identity_kid = identity.key_id
    request.state.identity_jti = identity.jti_digest
    return identity


def command_context(
    *,
    identity: RequestIdentity,
    command_id: str,
    expected_version: int,
    operation: str,
) -> CommandContext:
    return CommandContext(
        command_id=command_id,
        tenant_id=identity.tenant_id,
        actor=identity.actor,
        correlation_id=identity.correlation_id,
        expected_version=expected_version,
        operation=operation,
    )


def get_task_command_gateway() -> TaskCommandGateway:
    raise RuntimeError("TaskCommandGateway dependency was not configured by composition")


def get_task_projection() -> TaskReader:
    raise RuntimeError("TaskReader dependency was not configured by composition")


def get_task_query_service() -> TaskQueryService:
    raise RuntimeError("TaskQueryService dependency was not configured by composition")


def get_collaboration_projection() -> CollaborationReader:
    raise RuntimeError("CollaborationReader dependency was not configured by composition")


def get_streaming_gateway() -> StreamingGateway:
    raise RuntimeError("StreamingGateway dependency was not configured by composition")


def get_observability_service() -> ObservabilityService:
    raise RuntimeError("ObservabilityService dependency was not configured by composition")
