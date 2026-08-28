from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ServiceIdentity,
    SkillBindingReferenceRequest,
)
from auraclaw.contracts.state import Visibility
from auraclaw.session.internal_service import SessionInternalService

_DIGEST = f"sha256:{'a' * 64}"


class _Events:
    def __init__(self, events: list[CanonicalEvent]) -> None:
        self.events = events

    async def load_all(self, tenant_id: str | None = None) -> list[CanonicalEvent]:
        assert tenant_id == "tenant-m14f"
        return self.events

    async def has_skill_package_reference(
        self, tenant_id: str, package_digest: str
    ) -> bool:
        assert tenant_id == "tenant-m14f"
        for event in self.events:
            direct = event.payload.get("package_digest")
            activation = event.payload.get("activation")
            binding = activation.get("binding") if isinstance(activation, dict) else None
            nested = binding.get("package_digest") if isinstance(binding, dict) else None
            if direct == package_digest or nested == package_digest:
                return True
        return False


def _event(payload: dict[str, object]) -> CanonicalEvent:
    return CanonicalEvent(
        event_id="evt_m14f",
        tenant_id="tenant-m14f",
        root_session_id="root-m14f",
        session_id="session-m14f",
        run_id="run-m14f",
        aggregate_version=1,
        type="skill.activated",
        occurred_at=datetime.now(UTC),
        actor=Actor(type="runtime", id="runtime-m14f"),
        correlation_id="corr-m14f",
        causation_id="cause-m14f",
        visibility=Visibility.INTERNAL,
        schema_version=1,
        payload=payload,
    )


def _request(identity: ServiceIdentity) -> SkillBindingReferenceRequest:
    return SkillBindingReferenceRequest(
        context=InternalRequestContext(
            tenant_id="tenant-m14f",
            service_identity=identity,
            request_id="request-m14f",
            correlation_id="corr-m14f",
            causation_id="cause-m14f",
        ),
        package_digest=_DIGEST,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"package_digest": _DIGEST},
        {"activation": {"binding": {"package_digest": _DIGEST}}},
    ],
)
async def test_session_binding_reference_reads_canonical_event_shapes(
    payload: dict[str, object],
) -> None:
    service = SessionInternalService(  # type: ignore[arg-type]
        _Events([_event(payload)]),
        lease_verifier=AsyncMock(),
    )
    response = await service.skill_binding_reference(
        _request(ServiceIdentity.ACTION_HANDS)
    )
    assert response.referenced


@pytest.mark.asyncio
async def test_session_binding_reference_rejects_other_workloads() -> None:
    service = SessionInternalService(  # type: ignore[arg-type]
        _Events([]),
        lease_verifier=AsyncMock(),
    )
    with pytest.raises(AuthorizationError):
        await service.skill_binding_reference(_request(ServiceIdentity.TASK_API))
