import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from auraclaw.action.tool_gateway import PolicyEngine, ToolGateway, ToolRegistry
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import (
    ApprovalValidationError,
    ArtifactAccessError,
    CredentialAccessError,
    NotFoundError,
    PolicyDeniedError,
    SandboxViolationError,
    SchemaValidationError,
)
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.contracts.tools import (
    ApprovalStatus,
    CredentialReference,
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)
from auraclaw.control.orchestrator import LocalRuntimeProvisioner, ManagedOrchestrator
from auraclaw.domain.approval import ApprovalAggregate
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.hands.local import LocalHandsService
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.tool_client import GatewayToolClient
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.clients import (
    FencedSessionClient,
    InMemoryRuntimeEventBus,
)
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall
from auraclaw.session.task_service import TaskService


class RecordingHands(LocalHandsService):
    def __init__(self, result: Any) -> None:
        self.calls = 0
        self.result = result
        super().__init__(workspace_root=Path.cwd(), handlers={"managed": self._handle})

    def _handle(self, arguments: dict[str, Any]) -> Any:
        self.calls += 1
        del arguments
        return self.result


def _capability(
    permission: ToolPermission = ToolPermission.WRITE_WITH_APPROVAL,
    *,
    runtime_location: str = "hands",
) -> ToolCapability:
    return ToolCapability(
        name="managed",
        version="1",
        description="managed test tool",
        input_schema={
            "type": "object",
            "properties": {"target": {"type": "string", "minLength": 1}},
            "required": ["target"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=permission,
        risk_level=RiskLevel.HIGH,
        runtime_location=runtime_location,
        allowed_credential_operations=("write",),
    )


def _invocation(
    *,
    target: str = "resource-a",
    key: str = "stable-key",
    approval_id: str | None = None,
    credential_ref: str | None = None,
) -> ToolInvocation:
    return ToolInvocation(
        tool_invocation_id=f"tool-{key}",
        tenant_id="tenant-m3",
        root_session_id="session-m3",
        session_id="session-m3",
        run_id="run-m3",
        tool_name="managed",
        tool_version="1",
        arguments={"target": target},
        expected_side_effect="write",
        idempotency_key=key,
        deadline=datetime.now(UTC) + timedelta(minutes=1),
        fencing_token=1,
        actor_id="runtime-m3",
        approval_id=approval_id,
        credential_ref=credential_ref,
    )


def _event(event_type: str, payload: dict[str, Any], version: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"event-{version}",
        tenant_id="tenant-m3",
        root_session_id="session-m3",
        session_id="session-m3",
        run_id="run-m3",
        aggregate_version=version,
        type=event_type,
        occurred_at=datetime.now(UTC),
        actor=Actor(type="runtime", id="runtime-m3"),
        correlation_id="run-m3",
        causation_id=f"cause-{version}",
        visibility=Visibility.INTERNAL,
        schema_version=1,
        payload=payload,
    )


def _gateway(
    hands: RecordingHands,
    approvals: InMemoryApprovalProjection,
    *,
    permission: ToolPermission = ToolPermission.WRITE_WITH_APPROVAL,
    max_inline_bytes: int = 64 * 1024,
) -> tuple[ToolGateway, ArtifactStore]:
    artifacts = ArtifactStore(InMemoryObjectStorage(), signing_key=b"m3-test-signing-key")
    return (
        ToolGateway(
            registry=ToolRegistry((_capability(permission),)),
            policy=PolicyEngine(),
            approvals=approvals,
            hands=hands,
            artifacts=artifacts,
            max_inline_bytes=max_inline_bytes,
        ),
        artifacts,
    )


def test_tool_gateway_surfaces_controlled_boundary_reason() -> None:
    class DenyingHands:
        async def execute(self, invocation: ToolInvocation, capability: ToolCapability) -> object:
            del invocation, capability
            raise PolicyDeniedError("chaintower MCP call is missing trusted user context")

    async def scenario() -> None:
        artifacts = ArtifactStore(
            InMemoryObjectStorage(), signing_key=b"m3-test-signing-key"
        )
        gateway = ToolGateway(
            registry=ToolRegistry((_capability(ToolPermission.READ_ONLY),)),
            policy=PolicyEngine(),
            approvals=InMemoryApprovalProjection(),
            hands=DenyingHands(),
            artifacts=artifacts,
        )
        result = await gateway.execute(_invocation())
        assert result.status.value == "denied"
        assert result.error_code == "policy_denied"
        assert result.summary == "chaintower MCP call is missing trusted user context"

    asyncio.run(scenario())


def test_schema_validation_happens_before_hands_execution() -> None:
    async def scenario() -> None:
        hands = RecordingHands({"ok": True})
        gateway, _ = _gateway(hands, InMemoryApprovalProjection())
        invalid = _invocation()
        invalid = ToolInvocation(**{**invalid.__dict__, "arguments": {"unexpected": True}})
        with pytest.raises(SchemaValidationError):
            await gateway.execute(invalid)
        assert hands.calls == 0

    asyncio.run(scenario())


def test_write_requires_approval_and_argument_change_invalidates_it() -> None:
    async def scenario() -> None:
        approvals = InMemoryApprovalProjection()
        hands = RecordingHands({"ok": True})
        gateway, _ = _gateway(hands, approvals)
        invocation = _invocation()
        denied = await gateway.execute(invocation)
        assert denied.error_code == "approval_required"
        assert hands.calls == 0

        payload = dict(denied.metadata["approval_request"])
        await approvals.project([_event("approval.requested", payload, 1)])
        record = await approvals.get("tenant-m3", str(payload["approval_id"]))
        assert record is not None
        approved = ApprovalAggregate.respond(
            record, actor_id="human", decision="approved", feedback=None
        )
        await approvals.project(
            [
                _event(
                    "approval.approved",
                    {
                        "approval_id": approved.approval_id,
                        "decision": ApprovalStatus.APPROVED.value,
                    },
                    2,
                )
            ]
        )

        success = await gateway.execute(
            _invocation(approval_id=approved.approval_id)
        )
        assert success.status.value == "success"
        assert hands.calls == 1

        with pytest.raises(ApprovalValidationError, match="digest"):
            await gateway.execute(
                _invocation(
                    target="resource-b",
                    key="changed-action",
                    approval_id=approved.approval_id,
                )
            )
        assert hands.calls == 1

    asyncio.run(scenario())


def test_idempotency_key_prevents_duplicate_side_effect() -> None:
    async def scenario() -> None:
        hands = RecordingHands({"resource_id": "external-1"})
        gateway, _ = _gateway(
            hands,
            InMemoryApprovalProjection(),
            permission=ToolPermission.WRITE_AUTONOMOUS,
        )
        first, second = await asyncio.gather(
            gateway.execute(_invocation()), gateway.execute(_invocation())
        )
        assert first.as_dict() == second.as_dict()
        assert hands.calls == 1
        conflict = await gateway.execute(_invocation(target="different"))
        assert conflict.error_code == "idempotency_conflict"
        assert hands.calls == 1

    asyncio.run(scenario())


def test_large_output_becomes_tenant_scoped_artifact_ref() -> None:
    async def scenario() -> None:
        hands = RecordingHands({"payload": "x" * 1_000})
        gateway, artifacts = _gateway(
            hands,
            InMemoryApprovalProjection(),
            permission=ToolPermission.READ_ONLY,
            max_inline_bytes=100,
        )
        result = await gateway.execute(_invocation())
        serialized = json.dumps(result.as_dict())
        assert "x" * 100 not in serialized
        assert isinstance(result.as_dict()["content"], dict)
        artifact_id = result.as_dict()["content"]["artifact_ref"]["artifact_id"]
        token = await artifacts.issue_download_token(
            tenant_id="tenant-m3", artifact_id=artifact_id, actor_id="human"
        )
        content = await artifacts.download(
            token=token, tenant_id="tenant-m3", actor_id="human"
        )
        assert json.loads(content)["payload"] == "x" * 1_000
        with pytest.raises(ArtifactAccessError):
            await artifacts.download(token=token, tenant_id="other", actor_id="human")
        derived = await artifacts.derive_version(
            tenant_id="tenant-m3",
            source_artifact_id=artifact_id,
            content=b'{"payload":"revised"}',
            producer="reviewer",
        )
        derived_metadata = await artifacts.metadata("tenant-m3", derived.artifact_id)
        assert derived.version == 2
        assert derived_metadata.lineage_refs == (artifact_id,)

    asyncio.run(scenario())


def test_credential_proxy_redacts_secret_and_hands_environment_has_none() -> None:
    async def scenario() -> None:
        secret = "real-super-secret-token"
        vault = InMemoryVault({"cred-1": secret})
        proxy = CredentialProxy(vault)
        proxy.register_reference(
            "tenant-m3",
            CredentialReference(
                credential_ref="cred-1",
                provider="example",
                account_scope="account-1",
                allowed_operations=("write",),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        )

        def external(arguments: dict[str, Any], credential: str) -> dict[str, Any]:
            return {
                "target": arguments["target"],
                "authorization": f"Bearer {credential}",
                "echo": credential,
            }

        hands = RecordingHands({"unused": True})
        gateway = ToolGateway(
            registry=ToolRegistry(
                (_capability(ToolPermission.WRITE_AUTONOMOUS, runtime_location="credential_proxy"),)
            ),
            policy=PolicyEngine(),
            approvals=InMemoryApprovalProjection(),
            hands=hands,
            artifacts=ArtifactStore(
                InMemoryObjectStorage(), signing_key=b"credential-artifact-key"
            ),
            credential_proxy=proxy,
            credential_adapters={"managed": external},
        )
        result = await gateway.execute(_invocation(credential_ref="cred-1"))
        assert secret not in json.dumps(result.as_dict())
        assert hands.calls == 0
        assert proxy.usage_audit()[0]["credential_ref"] == "cred-1"

        with pytest.raises(CredentialAccessError):
            await proxy.invoke(
                tenant_id="other-tenant",
                session_id="session-m3",
                tool_name="managed",
                credential_ref="cred-1",
                operation="write",
                request={},
                adapter=external,
            )
        with pytest.raises(CredentialAccessError):
            await proxy.invoke(
                tenant_id="tenant-m3",
                session_id="session-m3",
                tool_name="managed",
                credential_ref="cred-1",
                operation="admin",
                request={},
                adapter=external,
            )
        proxy.register_reference(
            "tenant-m3",
            CredentialReference(
                credential_ref="cred-expired",
                provider="example",
                account_scope="account-1",
                allowed_operations=("write",),
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            ),
        )
        with pytest.raises(CredentialAccessError):
            await proxy.invoke(
                tenant_id="tenant-m3",
                session_id="session-m3",
                tool_name="managed",
                credential_ref="cred-expired",
                operation="write",
                request={},
                adapter=external,
            )
        await vault.revoke("cred-1")
        revoked = await gateway.execute(
            _invocation(key="after-revoke", credential_ref="cred-1")
        )
        assert revoked.error_code == "credential_access_denied"

        env_hands = LocalHandsService(
            workspace_root=Path.cwd(), allowed_executables=(Path("/usr/bin/env"),)
        )
        environment = await env_hands.run_process(
            Path("/usr/bin/env"), (), timeout_seconds=2
        )
        assert secret not in environment["stdout"]

    asyncio.run(scenario())


def test_sandbox_rejects_file_escape() -> None:
    async def scenario() -> None:
        hands = LocalHandsService(workspace_root=Path.cwd())
        with pytest.raises(SandboxViolationError):
            await hands.read_file("../../etc/passwd")

    asyncio.run(scenario())


def test_gateway_cancels_long_running_hands_call() -> None:
    async def scenario() -> None:
        started = asyncio.Event()

        async def slow_handler(arguments: dict[str, Any]) -> dict[str, Any]:
            del arguments
            started.set()
            await asyncio.sleep(60)
            return {"ok": True}

        hands = LocalHandsService(
            workspace_root=Path.cwd(), handlers={"managed": slow_handler}
        )
        gateway = ToolGateway(
            registry=ToolRegistry((_capability(ToolPermission.READ_ONLY),)),
            policy=PolicyEngine(),
            approvals=InMemoryApprovalProjection(),
            hands=hands,
            artifacts=ArtifactStore(
                InMemoryObjectStorage(), signing_key=b"cancellation-artifact-key"
            ),
        )
        running = asyncio.create_task(gateway.execute(_invocation()))
        await started.wait()
        assert await gateway.cancel("tool-stable-key")
        result = await running
        assert result.status.value == "cancelled"
        assert gateway.get_status("tool-stable-key") == "cancelled"

    asyncio.run(scenario())


def test_runtime_waits_for_approval_then_resumes_same_tool_call() -> None:
    class WriteProvider:
        async def generate(self, request: ModelRequest) -> ModelResponse:
            return ModelResponse(
                model_call_id=request.model_call_id,
                provider="test",
                model="test-model",
                completed_output="controlled write",
                tool_calls=(
                    ToolCall(
                        tool_invocation_id="tool-runtime-approval",
                        name="managed",
                        arguments={"target": "release"},
                        expected_side_effect="write",
                    ),
                ),
            )

    async def scenario() -> None:
        tenant_id = "tenant-runtime-m3"
        event_store = InMemoryEventStore()
        task_projection = InMemoryTaskProjection()
        service = TaskService(
            event_store=event_store,
            relay=OutboxRelay(event_store, task_projection),
            reader=task_projection,
            admission=AllowAllAdmissionController(),
        )
        created = await service.create_task(
            goal="write after approval",
            context=CommandContext(
                command_id="create-runtime-m3",
                tenant_id=tenant_id,
                actor=Actor(type="user", id="human"),
                correlation_id="corr-runtime-m3",
                expected_version=0,
                operation="create_task",
            ),
        )
        task_view = await task_projection.get_task(tenant_id, str(created["session_id"]))
        assert task_view is not None
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        orchestrator = ManagedOrchestrator(
            orchestrator_id="orchestrator-m3",
            control_store=control,
            session=session,
            provisioner=LocalRuntimeProvisioner(),
        )
        assert await orchestrator.watch([task_view]) == 1
        assignment = await orchestrator.schedule_once()
        assert assignment is not None

        approvals = InMemoryApprovalProjection()
        hands = RecordingHands({"resource_id": "release-1"})
        gateway, _ = _gateway(hands, approvals)
        harness = AgentHarness(
            control_store=control,
            session=session,
            model=WriteProvider(),
            tools=GatewayToolClient(gateway),
            runtime_events=InMemoryRuntimeEventBus(),
        )
        await harness.execute(assignment)
        waiting_events = await event_store.load(tenant_id, assignment.session_id)
        assert any(event.type == "approval.requested" for event in waiting_events)
        assert any(event.type == "tool.call.denied" for event in waiting_events)
        assert not any(event.type == "run.completed" for event in waiting_events)
        approval_event = next(
            event for event in waiting_events if event.type == "approval.requested"
        )
        await approvals.project([approval_event])
        record = await approvals.get(tenant_id, str(approval_event.payload["approval_id"]))
        assert record is not None
        decided = ApprovalAggregate.respond(
            record, actor_id="human", decision="approved", feedback=None
        )
        approved_event = CanonicalEvent(
            **{
                **approval_event.__dict__,
                "event_id": "runtime-approved-event",
                "aggregate_version": approval_event.aggregate_version + 1,
                "type": "approval.approved",
                "payload": {
                    "approval_id": decided.approval_id,
                    "decision": "approved",
                },
            }
        )
        await approvals.project([approved_event])

        task_id = f"{tenant_id}:{assignment.session_id}:{assignment.run_id}"
        assert await control.wake_assignment(task_id)
        resumed_assignment = await orchestrator.schedule_once()
        assert resumed_assignment is not None
        assert resumed_assignment.run_id == assignment.run_id
        assert resumed_assignment.fencing_token > assignment.fencing_token
        await harness.execute(resumed_assignment)
        completed_events = await event_store.load(tenant_id, assignment.session_id)
        assert [event.type for event in completed_events].count("tool.call.completed") == 1
        assert [event.type for event in completed_events].count("run.completed") == 1
        assert hands.calls == 1

    asyncio.run(scenario())


def test_approval_response_rebuilds_from_canonical_events_without_projection() -> None:
    async def scenario() -> None:
        tenant_id = "tenant-hitl-events"
        event_store = InMemoryEventStore()
        task_projection = InMemoryTaskProjection()
        service = TaskService(
            event_store=event_store,
            relay=OutboxRelay(event_store, task_projection),
            reader=task_projection,
            admission=AllowAllAdmissionController(),
        )
        created = await service.create_task(
            goal="approve from events",
            context=CommandContext(
                command_id="create-hitl-events",
                tenant_id=tenant_id,
                actor=Actor(type="user", id="human"),
                correlation_id="corr-hitl-events",
                expected_version=0,
                operation="create_task",
            ),
        )
        session_id = str(created["session_id"])
        run_id = str(created["run_id"])
        approval_id = "apr_unprojected"
        await event_store.append(
            root_session_id=session_id,
            session_id=session_id,
            run_id=run_id,
            context=CommandContext(
                command_id="runtime-approval-request",
                tenant_id=tenant_id,
                actor=Actor(type="runtime", id="runtime-1"),
                correlation_id=run_id,
                expected_version=2,
                operation="runtime.approval.requested",
            ),
            events=[
                NewEvent(
                    type="approval.requested",
                    payload={
                        "approval_id": approval_id,
                        "run_id": run_id,
                        "action_digest": "digest-hitl-events",
                        "tool_name": "auramcp.about.auraclaw",
                        "redacted_arguments": {},
                        "risk": "high",
                        "reason": "write-with-approval action requires human approval",
                        "expected_effect": "write",
                        "allowed_decisions": ["approved", "rejected"],
                        "assigned_approvers": [],
                        "policy_version": "m3-v1",
                        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                        "status": "waiting",
                    },
                )
            ],
            command_result={"approval_id": approval_id},
        )
        responded = await service.record_approval_response(
            session_id=session_id,
            approval_id=approval_id,
            decision="approved",
            feedback=None,
            context=CommandContext(
                command_id="human-approve",
                tenant_id=tenant_id,
                actor=Actor(type="user", id="human"),
                correlation_id=run_id,
                expected_version=3,
                operation="record_approval_response",
            ),
        )
        assert responded["decision"] == "approved"
        events = await event_store.load(tenant_id, session_id)
        assert any(event.type == "approval.approved" for event in events)
        with pytest.raises(NotFoundError, match="apr_missing"):
            await service.record_approval_response(
                session_id=session_id,
                approval_id="apr_missing",
                decision="approved",
                feedback=None,
                context=CommandContext(
                    command_id="human-approve-missing",
                    tenant_id=tenant_id,
                    actor=Actor(type="user", id="human"),
                    correlation_id=run_id,
                    expected_version=5,
                    operation="record_approval_response",
                ),
            )

    asyncio.run(scenario())
