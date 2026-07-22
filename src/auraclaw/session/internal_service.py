from __future__ import annotations

from collections.abc import Mapping

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.internal import (
    ServiceIdentity,
    SessionAppendRequest,
    SessionAppendResponse,
    SessionFeedRequest,
    SessionFeedResponse,
)
from auraclaw.contracts.state import Visibility
from auraclaw.internal.security import LeaseAssertionVerifier
from auraclaw.session.ports import EventStore

DEFAULT_EVENT_ALLOWLIST: Mapping[ServiceIdentity, tuple[str, ...]] = {
    ServiceIdentity.TASK_API: (
        "session.",
        "task.",
        "run.cancel",
        "human.",
        "approval.",
    ),
    ServiceIdentity.ORCHESTRATOR: ("run.scheduled", "runtime.", "run.requeued"),
    ServiceIdentity.AGENT_RUNTIME: (
        "run.",
        "model.",
        "tool.",
        "child.",
        "runtime.",
    ),
    ServiceIdentity.DELIVERY_WORKER: ("delivery.",),
    ServiceIdentity.POLICY: ("approval.", "policy."),
}

DEFAULT_ACTOR_ALLOWLIST: Mapping[ServiceIdentity, tuple[str, ...]] = {
    ServiceIdentity.TASK_API: ("user", "service"),
    ServiceIdentity.ORCHESTRATOR: ("orchestrator", "service"),
    ServiceIdentity.AGENT_RUNTIME: ("runtime",),
    ServiceIdentity.DELIVERY_WORKER: ("delivery", "service"),
    ServiceIdentity.POLICY: ("policy", "service"),
}


def _allowed(value: str, patterns: tuple[str, ...]) -> bool:
    return any(
        value == pattern or (pattern.endswith(".") and value.startswith(pattern))
        for pattern in patterns
    )


class SessionInternalService:
    def __init__(
        self,
        event_store: EventStore,
        *,
        lease_verifier: LeaseAssertionVerifier,
        event_allowlist: Mapping[ServiceIdentity, tuple[str, ...]] = DEFAULT_EVENT_ALLOWLIST,
        actor_allowlist: Mapping[ServiceIdentity, tuple[str, ...]] = DEFAULT_ACTOR_ALLOWLIST,
    ) -> None:
        self._event_store = event_store
        self._lease_verifier = lease_verifier
        self._event_allowlist = event_allowlist
        self._actor_allowlist = actor_allowlist

    async def append(self, request: SessionAppendRequest) -> SessionAppendResponse:
        identity = request.context.service_identity
        allowed_events = self._event_allowlist.get(identity, ())
        if not request.events or any(
            not _allowed(event.type, allowed_events) for event in request.events
        ):
            raise AuthorizationError("service identity is not allowed to append this event type")
        if request.actor_type not in self._actor_allowlist.get(identity, ()):
            raise AuthorizationError("service identity cannot use the supplied actor type")
        if (
            request.lease_assertion is not None
            and request.context.tenant_id != request.lease_assertion.tenant_id
        ):
            raise AuthorizationError("request and lease tenant mismatch")
        if identity is ServiceIdentity.AGENT_RUNTIME:
            assertion = request.lease_assertion
            if assertion is None or request.run_id is None:
                raise AuthorizationError("Runtime append requires a lease assertion")
            await self._lease_verifier.verify(
                assertion,
                tenant_id=request.context.tenant_id,
                session_id=request.session_id,
                run_id=request.run_id,
            )
        events = tuple(
            NewEvent(
                type=event.type,
                payload=dict(event.payload),
                visibility=Visibility(event.visibility),
            )
            for event in request.events
        )
        result = await self._event_store.append(
            root_session_id=request.root_session_id,
            session_id=request.session_id,
            run_id=request.run_id,
            context=CommandContext(
                command_id=request.command_id,
                tenant_id=request.context.tenant_id,
                actor=Actor(type=request.actor_type, id=request.actor_id),
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
                expected_version=request.expected_version,
                operation=request.operation,
            ),
            events=events,
            command_result=dict(request.command_result),
        )
        return SessionAppendResponse(
            events=tuple(event.as_dict() for event in result.events),
            command_result=result.command_result,
            deduplicated=result.deduplicated,
        )

    async def feed(self, request: SessionFeedRequest) -> SessionFeedResponse:
        events = await self._event_store.load(
            request.context.tenant_id,
            request.session_id,
            from_version=request.from_version,
        )
        page = events[: request.limit]
        next_version = None
        if len(events) > len(page) and page:
            next_version = page[-1].aggregate_version + 1
        return SessionFeedResponse(
            events=tuple(event.as_dict() for event in page),
            next_version=next_version,
        )
