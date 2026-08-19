from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from auraclaw.action.hands import HandsGateway
from auraclaw.action.hands_http import StaticHandsAuthenticator, create_hands_http_app
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.errors import AuraClawError, AuthorizationError, FencingTokenError
from auraclaw.contracts.hands import HANDS_TOOLS_LIST, HandsTrustedContext
from auraclaw.contracts.internal import (
    INTERNAL_API_VERSION,
    AdminOperationRequest,
    AdminOperationResponse,
    ArtifactCreateUploadRequest,
    ArtifactDownloadRequest,
    ArtifactDownloadResponse,
    ArtifactUploadResponse,
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    CheckpointState,
    CredentialInvokeRequest,
    CredentialInvokeResponse,
    EventInput,
    InternalRequestContext,
    LeaseAssertion,
    LoadCheckpointRequest,
    ModelGenerateRequest,
    ModelGenerateResponse,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    RuntimeServiceConfig,
    SaveCheckpointRequest,
    ServiceIdentity,
    SessionAppendRequest,
    SessionAppendResponse,
)
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.internal.http import (
    HttpContractClient,
    InProcessContractClient,
    contract_route,
    create_contract_app,
)
from auraclaw.internal.routes import control_routes, session_routes
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.ports import ToolCall
from auraclaw.session.internal_service import SessionInternalService

SIGNING_KEY = b"s1-lease-assertion-signing-key-0001"


def _context(
    identity: ServiceIdentity,
    *,
    tenant_id: str = "tenant-s1",
    request_id: str = "request-s1",
) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id=tenant_id,
        service_identity=identity,
        request_id=request_id,
        correlation_id="correlation-s1",
        causation_id="causation-s1",
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def _assertion(
    *,
    token: int = 1,
    audience: str = "session",
    session_id: str = "session-s1",
    run_id: str = "run-s1",
) -> LeaseAssertion:
    unsigned = LeaseAssertion(
        key_id="pending",
        audience=audience,
        tenant_id="tenant-s1",
        session_id=session_id,
        run_id=run_id,
        lease_id=f"lease-{token}",
        fencing_token=token,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
        signature="",
    )
    return LeaseAssertionSigner(key_id="key-s1", signing_key=SIGNING_KEY).sign(unsigned)


def _verifier(*, audience: str = "session") -> LeaseAssertionVerifier:
    return LeaseAssertionVerifier(
        {"key-s1": SIGNING_KEY},
        ledger=InMemoryFencingTokenLedger(),
        audience=audience,
    )


def _append_request(assertion: LeaseAssertion) -> SessionAppendRequest:
    return SessionAppendRequest(
        context=_context(ServiceIdentity.AGENT_RUNTIME),
        root_session_id="session-s1",
        session_id="session-s1",
        run_id="run-s1",
        command_id="command-s1",
        expected_version=0,
        operation="runtime.run.started",
        actor_type="runtime",
        actor_id="runtime-s1",
        events=(EventInput(type="run.started", payload={"run_id": "run-s1"}),),
        command_result={"accepted": True},
        lease_assertion=assertion,
    )


def test_session_in_process_and_http_adapters_share_the_contract() -> None:
    async def scenario() -> None:
        for transport in ("in-process", "http"):
            store = InMemoryEventStore()
            service = SessionInternalService(store, lease_verifier=_verifier())
            routes = session_routes(service)
            request = _append_request(_assertion())
            if transport == "in-process":
                response = await InProcessContractClient(routes).call(
                    "/internal/v1/session/append", request, SessionAppendResponse
                )
            else:
                app = create_contract_app(
                    "session",
                    routes,
                    workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://session"
                ) as raw:
                    response = await HttpContractClient(
                        raw, bearer_token="runtime-token"
                    ).call(
                        "/internal/v1/session/append", request, SessionAppendResponse
                    )
            assert response.api_version == INTERNAL_API_VERSION
            assert response.events[0]["causation_id"] == "causation-s1"
            assert response.events[0]["actor"] == {"type": "runtime", "id": "runtime-s1"}

    asyncio.run(scenario())


def test_task_api_can_atomically_create_a_session_and_request_its_first_run() -> None:
    async def scenario() -> None:
        service = SessionInternalService(
            InMemoryEventStore(),
            lease_verifier=_verifier(),
        )
        response = await service.append(
            SessionAppendRequest(
                context=_context(ServiceIdentity.TASK_API),
                root_session_id="session-s1",
                session_id="session-s1",
                run_id="run-s1",
                command_id="command-create-s1",
                expected_version=0,
                operation="task.create",
                actor_type="user",
                actor_id="user-s1",
                events=(
                    EventInput(type="session.created", payload={"goal": "test"}),
                    EventInput(type="run.requested", payload={"run_id": "run-s1"}),
                ),
                command_result={"accepted": True},
            )
        )

        assert [event["type"] for event in response.events] == [
            "session.created",
            "run.requested",
        ]

    asyncio.run(scenario())


def test_task_api_can_append_user_message_and_cancel_run() -> None:
    async def scenario() -> None:
        service = SessionInternalService(
            InMemoryEventStore(),
            lease_verifier=_verifier(),
        )
        await service.append(
            SessionAppendRequest(
                context=_context(ServiceIdentity.TASK_API),
                root_session_id="session-s1-followup",
                session_id="session-s1-followup",
                run_id="run-s1-followup",
                command_id="command-create-followup",
                expected_version=0,
                operation="task.create",
                actor_type="user",
                actor_id="user-s1",
                events=(
                    EventInput(type="session.created", payload={"goal": "test"}),
                    EventInput(type="run.requested", payload={"run_id": "run-s1-followup"}),
                ),
                command_result={"accepted": True},
            )
        )
        message = await service.append(
            SessionAppendRequest(
                context=_context(ServiceIdentity.TASK_API),
                root_session_id="session-s1-followup",
                session_id="session-s1-followup",
                run_id="run-s1-followup",
                command_id="command-append-followup",
                expected_version=2,
                operation="append_message",
                actor_type="user",
                actor_id="user-s1",
                events=(
                    EventInput(
                        type="user.message.appended",
                        payload={"message": "你好"},
                    ),
                ),
                command_result={"accepted": True},
            )
        )
        cancel = await service.append(
            SessionAppendRequest(
                context=_context(ServiceIdentity.TASK_API),
                root_session_id="session-s1-followup",
                session_id="session-s1-followup",
                run_id="run-s1-followup",
                command_id="command-cancel-followup",
                expected_version=3,
                operation="cancel",
                actor_type="user",
                actor_id="user-s1",
                events=(
                    EventInput(
                        type="run.cancelled",
                        payload={"run_id": "run-s1-followup", "reason": "stop"},
                    ),
                ),
                command_result={"accepted": True},
            )
        )

        assert [event["type"] for event in message.events] == ["user.message.appended"]
        assert [event["type"] for event in cancel.events] == ["run.cancelled"]

    asyncio.run(scenario())


def test_session_rejects_event_spoofing_bad_signatures_and_stale_fencing() -> None:
    async def scenario() -> None:
        service = SessionInternalService(
            InMemoryEventStore(),
            lease_verifier=_verifier(),
        )
        request = _append_request(_assertion(token=2))
        await service.append(request)

        stale = _append_request(_assertion(token=1)).model_copy(
            update={"command_id": "stale", "expected_version": 1}
        )
        with pytest.raises(FencingTokenError):
            await service.append(stale)

        spoofed = request.model_copy(
            update={
                "command_id": "spoofed",
                "expected_version": 1,
                "events": (EventInput(type="delivery.completed"),),
            }
        )
        with pytest.raises(AuthorizationError):
            await service.append(spoofed)

        valid = _assertion(token=3)
        tampered = valid.model_copy(update={"session_id": "other-session"})
        with pytest.raises(AuthorizationError):
            await service.append(
                request.model_copy(
                    update={
                        "command_id": "tampered",
                        "expected_version": 1,
                        "lease_assertion": tampered,
                    }
                )
            )

    asyncio.run(scenario())


def test_internal_http_requires_contract_version_and_matching_workload_identity() -> None:
    async def scenario() -> None:
        service = SessionInternalService(
            InMemoryEventStore(),
            lease_verifier=_verifier(),
        )
        app = create_contract_app(
            "session",
            session_routes(service),
            workload_identities={"task-token": ServiceIdentity.TASK_API},
        )
        request = _append_request(_assertion())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://session"
        ) as raw:
            missing_version = await raw.post(
                "/internal/v1/session/append",
                json=request.model_dump(mode="json"),
            )
            assert missing_version.status_code == 426
            with pytest.raises(AuraClawError, match="workload identity"):
                await HttpContractClient(raw, bearer_token="task-token").call(
                    "/internal/v1/session/append",
                    request,
                    SessionAppendResponse,
                )

    asyncio.run(scenario())


def test_control_checkpoint_cancellation_and_lease_contracts() -> None:
    async def scenario() -> None:
        store = InMemoryControlStateStore()
        lease = await store.acquire_lease(
            "session:tenant-s1:session-s1", "runtime-s1", ttl=timedelta(minutes=1)
        )
        assert lease is not None
        unsigned = LeaseAssertion(
            key_id="pending",
            audience="control",
            tenant_id="tenant-s1",
            session_id="session-s1",
            run_id="run-s1",
            lease_id=lease.lease_id,
            fencing_token=lease.fencing_token,
            expires_at=lease.expires_at,
            signature="",
        )
        assertion = LeaseAssertionSigner(key_id="key-s1", signing_key=SIGNING_KEY).sign(
            unsigned
        )
        service = ControlInternalService(store, lease_verifier=_verifier(audience="control"))
        routes = control_routes(service)
        client = InProcessContractClient(routes)
        saved = await client.call(
            "/internal/v1/control/checkpoints/save",
            SaveCheckpointRequest(
                context=_context(ServiceIdentity.AGENT_RUNTIME),
                session_id="session-s1",
                run_id="run-s1",
                lease_assertion=assertion,
                state=CheckpointState(
                    phase="tool",
                    resume_cursor="cursor-1",
                    artifact_refs=("artifact-1",),
                    harness_state={"step": 2},
                ),
            ),
            CheckpointResponse,
        )
        assert saved.found is True
        assert saved.state is not None and saved.state.resume_cursor == "cursor-1"

        loaded = await service.load_checkpoint(
            LoadCheckpointRequest(
                context=_context(ServiceIdentity.AGENT_RUNTIME),
                session_id="session-s1",
                run_id="run-s1",
            )
        )
        assert loaded.state is not None and loaded.state.artifact_refs == ("artifact-1",)

        await service.request_cancel(
            CancellationRequest(
                context=_context(ServiceIdentity.TASK_API),
                session_id="session-s1",
                run_id="run-s1",
            )
        )
        status = await service.is_cancelled(
            CancellationRequest(
                context=_context(ServiceIdentity.AGENT_RUNTIME),
                session_id="session-s1",
                run_id="run-s1",
            )
        )
        assert status == CancellationResponse(cancelled=True)

    asyncio.run(scenario())
class _ApprovalReader:
    async def get(self, tenant_id: str, approval_id: str) -> None:
        del tenant_id, approval_id
        return None

    async def find_approved(
        self, tenant_id: str, session_id: str, digest: str, policy_version: str
    ) -> None:
        del tenant_id, session_id, digest, policy_version
        return None


class _RecordingHands:
    def __init__(self) -> None:
        self.invocations: list[Any] = []

    async def execute(self, invocation: Any, capability: ToolCapability) -> dict[str, Any]:
        del capability
        self.invocations.append(invocation)
        return {"ok": True, "tenant": invocation.tenant_id}


def _hands_fixture() -> tuple[HandsGateway, _RecordingHands]:
    capability = ToolCapability(
        name="lookup",
        version="1",
        description="lookup managed data",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
    )
    registry = ToolRegistry((capability,))
    hands = _RecordingHands()
    gateway = ToolGateway(
        registry=registry,
        policy=PolicyEngine(version="s1-v1"),
        approvals=_ApprovalReader(),
        hands=hands,
        artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=b"s1-artifact-key-0001"),
    )
    return HandsGateway(registry=registry, gateway=gateway), hands


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-s1",
        root_session_id="session-s1",
        session_id="session-s1",
        run_id="run-s1",
        runtime_id="runtime-s1",
        lease_id="lease-s1",
        fencing_token=7,
        role="worker",
        resource_profile={},
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def test_hands_in_process_lists_and_calls_with_trusted_context() -> None:
    async def scenario() -> None:
        gateway, hands = _hands_fixture()
        client = HandsRuntimeAdapter(InProcessHandsClient(gateway))
        assignment = _assignment()
        assert [tool["name"] for tool in await client.list_tools(assignment)] == ["lookup"]

        result = await client.execute(
            assignment,
            ToolCall(
                tool_invocation_id="tool-stable-1",
                name="lookup",
                arguments={"tenant_id": "attacker-controlled"},
            ),
        )
        assert result["status"] == "success"
        assert hands.invocations[0].tenant_id == "tenant-s1"
        assert hands.invocations[0].arguments["tenant_id"] == "attacker-controlled"
        assert hands.invocations[0].tool_invocation_id == "tool-stable-1"

        repeated = await client.execute(
            assignment,
            ToolCall(
                tool_invocation_id="tool-stable-1",
                name="lookup",
                arguments={"tenant_id": "attacker-controlled"},
            ),
        )
        assert repeated == result
        assert len(hands.invocations) == 1

    asyncio.run(scenario())


def test_hands_http_auth_and_version_failure() -> None:
    async def scenario() -> None:
        gateway, hands = _hands_fixture()
        assignment = _assignment()
        trusted = HandsTrustedContext(
            tenant_id=assignment.tenant_id,
            root_session_id=assignment.root_session_id,
            session_id=assignment.session_id,
            run_id=assignment.run_id,
            runtime_id=assignment.runtime_id,
            lease_id=assignment.lease_id,
            fencing_token=assignment.fencing_token,
            deadline=assignment.deadline,
        )
        app = create_hands_http_app(
            gateway,
            authenticator=StaticHandsAuthenticator({"runtime-token": trusted}),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://hands"
        ) as raw:
            unauthenticated = await raw.post(HANDS_TOOLS_LIST, json={})
            assert unauthenticated.status_code == 401

            unsupported = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={
                    "Authorization": "Bearer runtime-token",
                    "X-AuraClaw-Contract-Version": "2024-11-05",
                },
            )
            assert unsupported.status_code == 426

            client = HandsRuntimeAdapter(
                HttpHandsClient(raw, bearer_tokens={"runtime-s1": "runtime-token"})
            )
            result = await client.execute(
                assignment,
                ToolCall(
                    tool_invocation_id="tool-http-1",
                    name="lookup",
                    arguments={"query": "state"},
                ),
            )
            assert result["status"] == "success"
            assert len(hands.invocations) == 1

    asyncio.run(scenario())


def test_policy_credential_artifact_model_and_admin_http_contracts() -> None:
    async def scenario() -> None:
        expires_at = datetime.now(UTC) + timedelta(minutes=5)
        cases = [
            (
                "/internal/v1/policy/evaluate",
                PolicyEvaluateRequest(
                    context=_context(ServiceIdentity.ACTION_HANDS),
                    subject="runtime-s1",
                    action="tool.call",
                    resource="tool:lookup",
                    input_digest="sha256:digest",
                ),
                PolicyEvaluateResponse(
                    decision_id="decision-s1",
                    decision="allow",
                    policy_version="s1-v1",
                    expires_at=expires_at,
                ),
            ),
            (
                "/internal/v1/credentials/invoke",
                CredentialInvokeRequest(
                    context=_context(ServiceIdentity.ACTION_HANDS),
                    session_id="session-s1",
                    credential_ref="credential-ref-s1",
                    operation="read",
                    target="https://example.invalid",
                    method="GET",
                    policy_decision_id="decision-s1",
                ),
                CredentialInvokeResponse(
                    usage_id="usage-s1",
                    status="completed",
                    response={"ok": True},
                ),
            ),
            (
                "/internal/v1/artifacts/uploads/create",
                ArtifactCreateUploadRequest(
                    context=_context(ServiceIdentity.AGENT_RUNTIME),
                    root_session_id="session-s1",
                    session_id="session-s1",
                    name="result.json",
                    media_type="application/json",
                    expected_size=2,
                    expected_checksum="sha256:digest",
                ),
                ArtifactUploadResponse(
                    artifact_id="artifact-s1",
                    version=1,
                    upload_id="upload-s1",
                    upload_url="https://storage.invalid/upload",
                    expires_at=expires_at,
                ),
            ),
            (
                "/internal/v1/artifacts/download",
                ArtifactDownloadRequest(
                    context=_context(ServiceIdentity.DELIVERY_WORKER),
                    artifact_id="artifact-s1",
                    version=1,
                    actor_id="delivery-s1",
                    policy_decision_id="decision-s1",
                ),
                ArtifactDownloadResponse(
                    download_url="https://storage.invalid/download",
                    expires_at=expires_at,
                ),
            ),
            (
                "/internal/v1/model/generate",
                ModelGenerateRequest(
                    context=_context(ServiceIdentity.AGENT_RUNTIME),
                    model_call_id="model-s1",
                    run_id="run-s1",
                    messages=({"role": "user", "content": "hello"},),
                ),
                ModelGenerateResponse(
                    model_call_id="model-s1",
                    provider="provider",
                    model="model",
                    completed_output="hello",
                ),
            ),
            (
                "/internal/v1/admin/operations",
                AdminOperationRequest(
                    context=_context(ServiceIdentity.TASK_API),
                    operation_id="operation-s1",
                    owner_service=ServiceIdentity.PROJECTION_WORKER,
                    operation="projection.rebuild",
                ),
                AdminOperationResponse(
                    operation_id="operation-s1",
                    status="accepted",
                ),
            ),
        ]
        for path, request, expected in cases:
            async def handler(_request: Any, expected: Any = expected) -> Any:
                return expected

            route = contract_route(type(request), type(expected), handler)
            app = create_contract_app("contract-test", {path: route})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://internal"
            ) as raw:
                response = await HttpContractClient(raw).call(path, request, type(expected))
            assert response == expected

    asyncio.run(scenario())


def test_runtime_config_and_wire_models_cannot_accept_provider_secrets() -> None:
    config = RuntimeServiceConfig(
        runtime_id="runtime-s1",
        control_base_url="http://control",
        session_base_url="http://session",
        model_gateway_base_url="http://model-gateway",
        hands_url="http://hands",
        artifact_base_url="http://artifact",
        workload_token_file="/var/run/secrets/auraclaw/token",
    )
    serialized = config.model_dump()
    assert not {"api_key", "model_api_key", "credential", "secret"}.intersection(serialized)
    with pytest.raises(ValidationError):
        RuntimeServiceConfig.model_validate(
            {**serialized, "model_api_key": "must-not-enter-runtime"}
        )
