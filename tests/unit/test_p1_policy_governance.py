from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.config import Settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.events import Actor
from auraclaw.contracts.internal import (
    ArtifactShareRequest,
    InternalRequestContext,
    ModelGenerateRequest,
    PolicyEvaluateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.policy.governance import ProductionPolicy
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.ports import ModelResponse, ModelStreamChunk
from auraclaw.session.task_service import TaskService


def _context(identity: ServiceIdentity) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id="tenant-1",
        service_identity=identity,
        request_id="request-1",
        correlation_id="correlation-1",
        causation_id="causation-1",
    )


def _governance() -> ProductionPolicy:
    return ProductionPolicy(
        runtime_budget={
            "max_steps": 20,
            "max_output_tokens": 2048,
            "max_cost": 1.5,
            "tree_max_steps": 100,
            "tree_max_output_tokens": 10000,
            "tree_max_cost": 5.0,
            "policy_version": "2",
        },
        model_provider="managed-provider",
        model_name="managed-model",
        model_data_region="cn-east",
        allowed_data_regions=("cn-east",),
        artifact_share_max_ttl_seconds=300,
        artifact_share_classifications=("public", "internal"),
    )


def test_policy_configuration_rejects_unapproved_model_region() -> None:
    with pytest.raises(ValueError, match="model data region"):
        Settings(
            _env_file=None,
            model_data_region="eu-west",
            policy_allowed_data_regions="cn-east",
        )


class _BudgetAdmission:
    async def admit(self, *, goal: str, context: CommandContext) -> None:
        del goal, context

    async def govern_budget(
        self,
        *,
        goal: str,
        context: CommandContext,
        runtime_budget: dict[str, object],
    ) -> dict[str, object]:
        del goal, context, runtime_budget
        return {"max_steps": 7, "max_output_tokens": 700, "policy_version": "2"}


@pytest.mark.asyncio
async def test_task_persists_policy_governed_budget_in_canonical_facts() -> None:
    store = InMemoryEventStore()
    projection = InMemoryTaskProjection()
    service = TaskService(
        event_store=store,
        relay=OutboxRelay(store, projection),
        reader=projection,
        admission=_BudgetAdmission(),
        runtime_budget={"max_steps": 99, "max_output_tokens": 9999},
    )
    await service.create_task(
        goal="govern this task",
        context=CommandContext(
            command_id="command-1",
            tenant_id="tenant-1",
            actor=Actor(type="user", id="user-1"),
            correlation_id="correlation-1",
            expected_version=0,
            operation="create_task",
        ),
    )
    events = await store.load_all("tenant-1")
    budgets = [event.payload["budget"] for event in events if "budget" in event.payload]
    assert budgets
    assert all(budget["max_steps"] == 7 for budget in budgets)
    assert all(budget["max_output_tokens"] == 700 for budget in budgets)


@pytest.mark.asyncio
async def test_policy_owns_runtime_budget_and_model_selection() -> None:
    policy = PolicyInternalService(governance=_governance())
    task = await policy.evaluate(
        PolicyEvaluateRequest(
            context=_context(ServiceIdentity.TASK_API),
            subject="user-1",
            action="task.create",
            resource="task",
            input_digest="digest",
            attributes={"permission": "write-autonomous", "risk_level": "medium"},
        )
    )
    assert task.decision == "allow_with_constraints"
    assert task.constraints["runtime_budget"]["max_steps"] == 20

    model = await policy.evaluate(
        PolicyEvaluateRequest(
            context=_context(ServiceIdentity.MODEL_GATEWAY),
            subject="agent-runtime",
            action="model.generate",
            resource="general",
            input_digest="digest",
            attributes={
                "permission": "read-only",
                "risk_level": "medium",
                "required_data_region": "cn-east",
                "max_output_tokens": 8192,
            },
        )
    )
    assert model.decision == "allow_with_constraints"
    assert model.constraints["preferred_model"] == "managed-model"
    assert model.constraints["allowed_providers"] == ["managed-provider"]
    assert model.constraints["max_output_tokens"] == 2048
    assert model.constraints["run_max_cost"] == 1.5


@pytest.mark.asyncio
async def test_policy_denies_wrong_region_and_confidential_share() -> None:
    policy = PolicyInternalService(governance=_governance())
    for action, attributes in (
        (
            "model.generate",
            {
                "permission": "read-only",
                "risk_level": "medium",
                "required_data_region": "eu-west",
            },
        ),
        (
            "artifact.share",
            {
                "permission": "read-only",
                "risk_level": "medium",
                "classification": "confidential",
                "audience": "customer-1",
                "ttl_seconds": 60,
            },
        ),
    ):
        response = await policy.evaluate(
            PolicyEvaluateRequest(
                context=_context(ServiceIdentity.MODEL_GATEWAY),
                subject="subject",
                action=action,
                resource="resource",
                input_digest="digest",
                attributes=attributes,
            )
        )
        assert response.decision == "deny"
        assert response.constraints == {}


class _GovernedPolicy:
    async def evaluate_action(self, **_kwargs: Any) -> PolicyEvaluation:
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            decision_id="decision-1",
            policy_version="test",
            constraints={
                "preferred_model": "managed-model",
                "allowed_providers": ["managed-provider"],
                "data_region": "cn-east",
                "max_output_tokens": 32,
            },
        )


class _RecordingModel:
    def __init__(self) -> None:
        self.request: Any = None

    async def generate_stream(self, request: Any):
        self.request = request
        yield ModelStreamChunk(
            kind="completed",
            response=ModelResponse(
                model_call_id=request.model_call_id,
                provider="managed-provider",
                model="managed-model",
                completed_output="ok",
            ),
        )


@pytest.mark.asyncio
async def test_model_gateway_applies_policy_constraints_before_dispatch() -> None:
    model = _RecordingModel()
    gateway = ModelGatewayInternalService(
        model,
        policy=_GovernedPolicy(),
        configured_provider="managed-provider",
        configured_model="managed-model",
        data_region="cn-east",
    )
    await gateway.generate(
        ModelGenerateRequest(
            context=_context(ServiceIdentity.AGENT_RUNTIME),
            model_call_id="call-1",
            run_id="run-1",
            messages=({"role": "user", "content": "hello"},),
            max_output_tokens=100,
        )
    )
    assert model.request.policy.preferred_model == "managed-model"
    assert model.request.policy.allowed_providers == ("managed-provider",)
    assert model.request.max_output_tokens == 32


class _Presigner:
    def presign(self, method: str, object_key: str, *, ttl: timedelta):
        return f"https://objects.invalid/{object_key}?ttl={int(ttl.total_seconds())}", (
            datetime.now(UTC) + ttl
        )


class _SharePolicy:
    async def validate_decision(self, **_kwargs: Any) -> bool:
        return True

    async def evaluate_action(self, **kwargs: Any) -> PolicyEvaluation:
        if kwargs["attributes"]["classification"] == "confidential":
            decision = PolicyDecision.DENY
            constraints = {}
        else:
            decision = PolicyDecision.ALLOW_WITH_CONSTRAINTS
            constraints = {"audience": kwargs["attributes"]["audience"], "ttl_seconds": 60}
        return PolicyEvaluation(
            decision=decision,
            decision_id="share-decision",
            policy_version="test",
            constraints=constraints,
        )


def _ready(classification: str) -> PendingUpload:
    return PendingUpload(
        tenant_id="tenant-1",
        artifact_id="artifact-1",
        upload_id="upload-1",
        object_key="tenant-1/artifact-1",
        root_session_id="session-1",
        session_id="session-1",
        name="result.txt",
        media_type="text/plain",
        expected_size=2,
        expected_checksum="checksum",
        classification=classification,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        lifecycle_status="ready",
        scan_status="clean",
    )


@pytest.mark.asyncio
async def test_artifact_share_uses_trusted_classification_and_policy_ttl() -> None:
    service = ArtifactInternalService(_Presigner(), policy=_SharePolicy())
    service._ready[("tenant-1", "artifact-1", 1)] = _ready("internal")
    response = await service.share(
        ArtifactShareRequest(
            context=_context(ServiceIdentity.TASK_API),
            artifact_id="artifact-1",
            version=1,
            actor_id="user-1",
            audience="customer-1",
            ttl_seconds=300,
        )
    )
    assert "ttl=60" in response.share_url
    assert response.policy_decision_id == "share-decision"

    service._ready[("tenant-1", "artifact-1", 1)] = _ready("confidential")
    with pytest.raises(ArtifactAccessError):
        await service.share(
            ArtifactShareRequest(
                context=_context(ServiceIdentity.TASK_API),
                artifact_id="artifact-1",
                version=1,
                actor_id="user-1",
                audience="customer-1",
                ttl_seconds=300,
            )
        )
