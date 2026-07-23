from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from auraclaw.artifact.internal_service import ArtifactInternalService
from auraclaw.composition.services import create_service_app
from auraclaw.config import Settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.internal import (
    AssignmentClaimRequest,
    AssignmentClaimResponse,
    AssignmentDispositionRequest,
    AssignmentDispositionResponse,
    InternalRequestContext,
    LeaseAssertion,
    RuntimeHeartbeatResponse,
    RuntimeRegistrationRequest,
    ServiceIdentity,
)
from auraclaw.contracts.mcp import MCP_PROTOCOL_VERSION
from auraclaw.contracts.tools import (
    ApprovalRecord,
    CredentialReference,
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.ports import RunnableItem, RuntimeAssignment
from auraclaw.credential_proxy.internal_service import CredentialProxyInternalService
from auraclaw.infrastructure.artifacts.seaweedfs import SeaweedFSS3Presigner
from auraclaw.infrastructure.clients.artifact import RemoteArtifactWriter
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.clients.runtime import (
    RemoteRuntimeControlClient,
    RemoteRuntimeSessionClient,
)
from auraclaw.infrastructure.clients.session import RemoteSessionEventStore
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.http import HttpContractClient, create_contract_app
from auraclaw.internal.routes import (
    artifact_routes,
    control_routes,
    credential_routes,
    model_routes,
    policy_routes,
    session_routes,
)
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.runtime.ports import ModelRequest, ModelResponse
from auraclaw.session.internal_service import SessionInternalService

ROOT = Path(__file__).resolve().parents[2]


def _settings(**values: object) -> Settings:
    return Settings(_env_file=None, **values)


class _DeterministicModel:
    async def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="test-provider",
            model="test-model",
            completed_output="remote result",
            deltas=("remote ", "result"),
            usage={"output_tokens": 2},
        )


@pytest.mark.asyncio
async def test_task_api_appends_and_loads_only_through_authenticated_session_http() -> None:
    store = InMemoryEventStore()
    verifier = LeaseAssertionVerifier(
        {"test": b"test-session-lease-signing-key-0001"},
        ledger=InMemoryFencingTokenLedger(),
    )
    service = SessionInternalService(store, lease_verifier=verifier)
    app = create_contract_app(
        "session",
        session_routes(service),
        workload_identities={
            "task-token": ServiceIdentity.TASK_API,
            "projection-token": ServiceIdentity.PROJECTION_WORKER,
        },
    )
    remote = RemoteSessionEventStore(
        "http://session.test",
        service_identity=ServiceIdentity.TASK_API,
        bearer_token="task-token",
        transport=httpx.ASGITransport(app=app),
    )
    context = CommandContext(
        command_id="cmd-create",
        tenant_id="tenant-a",
        actor=Actor(type="user", id="user-a"),
        correlation_id="corr-create",
        expected_version=0,
        operation="create_task",
    )
    result = await remote.append(
        root_session_id="session-a",
        session_id="session-a",
        run_id="run-a",
        context=context,
        events=(NewEvent(type="session.created", payload={"goal": "test"}),),
        command_result={"session_id": "session-a"},
    )
    loaded = await remote.load("tenant-a", "session-a")
    assert result.events[0].type == "session.created"
    assert [event.event_id for event in loaded] == [result.events[0].event_id]
    assert await remote.get_snapshot("tenant-a", "session-a") is None
    projection = RemoteSessionEventStore(
        "http://session.test",
        service_identity=ServiceIdentity.PROJECTION_WORKER,
        bearer_token="projection-token",
        transport=httpx.ASGITransport(app=app),
    )
    claimed = await projection.claim_outbox(
        "projection",
        "projection-1",
        limit=10,
        claim_ttl=timedelta(seconds=30),
    )
    assert [record.event_id for record in claimed] == [result.events[0].event_id]
    assert await projection.disposition_outbox(
        "projection",
        "projection-1",
        claimed[0].outbox_id,
        claimed[0].claim_token,
        "ack",
    )
    assert not await projection.claim_outbox(
        "projection",
        "projection-1",
        limit=10,
        claim_ttl=timedelta(seconds=30),
    )
    with pytest.raises(AuraClawError, match="restricted to projection-worker"):
        await remote.claim_outbox(
            "projection",
            "task-api",
            limit=1,
            claim_ttl=timedelta(seconds=30),
        )
    await projection.aclose()
    await remote.aclose()


@pytest.mark.asyncio
async def test_session_http_rejects_unknown_workload_token() -> None:
    store = InMemoryEventStore()
    verifier = LeaseAssertionVerifier(
        {"test": b"test-session-lease-signing-key-0001"},
        ledger=InMemoryFencingTokenLedger(),
    )
    service = SessionInternalService(store, lease_verifier=verifier)
    app = create_contract_app(
        "session",
        session_routes(service),
        workload_identities={"expected-token": ServiceIdentity.TASK_API},
    )
    remote = RemoteSessionEventStore(
        "http://session.test",
        service_identity=ServiceIdentity.TASK_API,
        bearer_token="wrong-token",
        transport=httpx.ASGITransport(app=app),
    )
    with pytest.raises(AuraClawError, match="workload identity"):
        await remote.load("tenant-a", "session-a")
    await remote.aclose()


def test_production_task_api_uses_remote_session_and_fails_closed() -> None:
    app = create_service_app(
        "api",
        _settings(
            deployment_profile="production",
            storage_backend="memory",
        ),
    )
    assert app.state.session_access == "http"
    with TestClient(app) as client:
        readiness = client.get("/health/ready")
        assert readiness.status_code == 503
        assert readiness.json()["status"] == "degraded"


@pytest.mark.asyncio
async def test_runtime_model_client_has_only_http_contract_and_workload_identity() -> None:
    app = create_contract_app(
        "model-gateway",
        model_routes(ModelGatewayInternalService(_DeterministicModel())),
        workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
    )
    remote = RemoteModelClient(
        "http://model.test",
        bearer_token="runtime-token",
        transport=httpx.ASGITransport(app=app),
    )
    response = await remote.generate(
        ModelRequest(
            model_call_id="model-call-1",
            tenant_id="tenant-a",
            run_id="run-a",
            messages=({"role": "user", "content": "hello"},),
        )
    )
    assert response.completed_output == "remote result"
    assert response.provider == "test-provider"
    await remote.aclose()

    denied = RemoteModelClient(
        "http://model.test",
        bearer_token="wrong-token",
        transport=httpx.ASGITransport(app=app),
    )
    with pytest.raises(AuraClawError, match="workload identity"):
        await denied.generate(
            ModelRequest(
                model_call_id="model-call-2",
                tenant_id="tenant-a",
                run_id="run-a",
                messages=(),
            )
        )
    await denied.aclose()


@pytest.mark.asyncio
async def test_runtime_claims_signed_assignment_only_through_control_api() -> None:
    key = b"test-control-lease-signing-key-0001"
    store = InMemoryControlStateStore()
    lease = await store.acquire_lease(
        "session:tenant-a:session-a",
        "orchestrator-1",
        ttl=timedelta(minutes=1),
    )
    assert lease is not None
    assert await store.enqueue(
        RunnableItem(
            task_id="task-a",
            tenant_id="tenant-a",
            root_session_id="session-a",
            session_id="session-a",
            run_id="run-a",
            source_version=1,
        )
    )
    runnable_claim = (await store.claim("orchestrator-1"))[0]
    assignment = RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="session-a",
        session_id="session-a",
        run_id="run-a",
        runtime_id="runtime-a",
        lease_id=lease.lease_id,
        fencing_token=lease.fencing_token,
        role="root",
        resource_profile={},
        lease_expires_at=lease.expires_at,
    )
    assert await store.assign(
        "task-a", assignment, claim_token=runnable_claim.claim_token
    )
    service = ControlInternalService(
        store,
        lease_verifier=LeaseAssertionVerifier(
            {"test": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("control", "runtime"),
        ),
        lease_signer=LeaseAssertionSigner(key_id="test", signing_key=key),
    )
    app = create_contract_app(
        "orchestrator",
        control_routes(service),
        workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
    )
    context = InternalRequestContext(
        tenant_id="tenant-a",
        service_identity=ServiceIdentity.AGENT_RUNTIME,
        request_id="request-a",
        correlation_id="run-a",
        causation_id="request-a",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control.test"
    ) as raw:
        client = HttpContractClient(raw, bearer_token="runtime-token")
        registered = await client.call(
            "/internal/v1/control/runtimes/register",
            RuntimeRegistrationRequest(
                context=context,
                runtime_id="runtime-a",
                runtime_type="agent",
                role="root",
                node_id="node-a",
                capacity=1,
            ),
            RuntimeHeartbeatResponse,
        )
        assert registered.accepted
        claimed = await client.call(
            "/internal/v1/control/assignments/claim",
            AssignmentClaimRequest(
                context=context,
                runtime_id="runtime-a",
                role="root",
            ),
            AssignmentClaimResponse,
        )
        assert len(claimed.assignments) == 1
        capability = claimed.assignments[0].lease_assertion
        assert capability.audience == "runtime"
        assert capability.signature
        completed = await client.call(
            "/internal/v1/control/assignments/disposition",
            AssignmentDispositionRequest(
                context=context,
                task_id="task-a",
                runtime_id="runtime-a",
                lease_id=lease.lease_id,
                fencing_token=lease.fencing_token,
                disposition="finish",
            ),
            AssignmentDispositionResponse,
        )
        assert completed.accepted


@pytest.mark.asyncio
async def test_remote_runtime_executes_with_no_control_or_session_store() -> None:
    key = b"test-runtime-capability-signing-key-001"
    control_store = InMemoryControlStateStore()
    lease = await control_store.acquire_lease(
        "session:tenant-a:session-b",
        "orchestrator-1",
        ttl=timedelta(minutes=1),
    )
    assert lease is not None
    assert await control_store.enqueue(
        RunnableItem(
            task_id="task-b",
            tenant_id="tenant-a",
            root_session_id="session-b",
            session_id="session-b",
            run_id="run-b",
            source_version=1,
        )
    )
    runnable_claim = (await control_store.claim("orchestrator-1"))[0]
    assert await control_store.assign(
        "task-b",
        RuntimeAssignment(
            tenant_id="tenant-a",
            root_session_id="session-b",
            session_id="session-b",
            run_id="run-b",
            runtime_id="runtime-b",
            lease_id=lease.lease_id,
            fencing_token=lease.fencing_token,
            role="root",
            resource_profile={},
            lease_expires_at=lease.expires_at,
        ),
        claim_token=runnable_claim.claim_token,
    )
    control_service = ControlInternalService(
        control_store,
        lease_verifier=LeaseAssertionVerifier(
            {"test": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("control", "runtime"),
        ),
        lease_signer=LeaseAssertionSigner(key_id="test", signing_key=key),
    )
    control_app = create_contract_app(
        "orchestrator",
        control_routes(control_service),
        workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
    )
    control = RemoteRuntimeControlClient(
        "http://control.test",
        bearer_token="runtime-token",
        runtime_id="runtime-b",
        role="root",
        node_id="node-b",
        capacity=1,
        transport=httpx.ASGITransport(app=control_app),
    )
    await control.register()
    assignments = await control.claim()
    assert len(assignments) == 1
    assignment = assignments[0]
    assert assignment.lease_assertion is not None
    assert assignment.lease_assertion.audience == "runtime"
    await control.assert_fencing(
        "session:tenant-a:session-b", assignment.fencing_token
    )

    session_store = InMemoryEventStore()
    session_service = SessionInternalService(
        session_store,
        lease_verifier=LeaseAssertionVerifier(
            {"test": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("session", "runtime"),
        ),
    )
    session_app = create_contract_app(
        "session",
        session_routes(session_service),
        workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
    )
    session = RemoteRuntimeSessionClient(
        "http://session.test",
        bearer_token="runtime-token",
        transport=httpx.ASGITransport(app=session_app),
    )
    appended = await session.append(
        assignment,
        (NewEvent(type="run.started", payload={"run_id": "run-b"}),),
        command_id="runtime:run.started:run-b",
        operation="runtime.run.started",
    )
    assert appended[0].actor == Actor(type="runtime", id="runtime-b")
    await control.finish_assignment("task-b", "completed")
    await session.aclose()
    await control.aclose()


def test_production_runtime_composition_is_remote_only_and_has_no_provider_secret() -> None:
    app = create_service_app(
        "runtime",
        _settings(
            deployment_profile="production",
            storage_backend="memory",
            runtime_event_backend="memory",
            runtime_workload_token="runtime-token",
        ),
    )
    assert app.state.data_access == "remote-only"
    assert app.state.config_ready is True
    assert app.state.dependencies["provider_secret_isolation"] == "ready"
    assert all("Store" not in type(item).__name__ for item in app.state.closeables)


def test_production_hands_requires_signed_runtime_lease_capability() -> None:
    key = b"test-hands-capability-signing-key-0001"
    app = create_service_app(
        "hands",
        _settings(
            deployment_profile="production",
            storage_backend="memory",
            runtime_id="runtime-a",
            runtime_workload_token="runtime-token",
            lease_signing_key=key.decode(),
        ),
    )
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"protocolVersion": MCP_PROTOCOL_VERSION},
    }
    capability = LeaseAssertionSigner(key_id="development", signing_key=key).sign(
        LeaseAssertion(
            key_id="pending",
            audience="runtime",
            tenant_id="tenant-a",
            root_session_id="root-a",
            session_id="session-a",
            run_id="run-a",
            runtime_id="runtime-a",
            lease_id="lease-a",
            fencing_token=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
            signature="",
        )
    )
    with TestClient(app) as client:
        missing = client.post(
            "/mcp",
            json=request,
            headers={"Authorization": "Bearer runtime-token"},
        )
        assert missing.status_code == 401
        accepted = client.post(
            "/mcp",
            json=request,
            headers={
                "Authorization": "Bearer runtime-token",
                "X-AuraClaw-Lease-Assertion": capability.model_dump_json(),
            },
        )
        assert accepted.status_code == 200
        assert accepted.json()["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_hands_uses_authenticated_remote_policy_and_credential_boundaries() -> None:
    policy_app = create_contract_app(
        "policy",
        policy_routes(PolicyInternalService(version="production-v1")),
        workload_identities={
            "hands-token": ServiceIdentity.ACTION_HANDS,
            "credential-token": ServiceIdentity.CREDENTIAL_PROXY,
        },
    )
    policy = RemotePolicyClient(
        "http://policy.test",
        bearer_token="hands-token",
        transport=httpx.ASGITransport(app=policy_app),
    )
    capability = ToolCapability(
        name="managed",
        version="1",
        description="managed credential call",
        input_schema={},
        output_schema={},
        permission=ToolPermission.WRITE_AUTONOMOUS,
        risk_level=RiskLevel.MEDIUM,
        runtime_location="credential_proxy",
    )
    invocation = ToolInvocation(
        tool_invocation_id="tool-a",
        tenant_id="tenant-a",
        root_session_id="root-a",
        session_id="session-a",
        run_id="run-a",
        tool_name="managed",
        tool_version="1",
        arguments={"message": "hello"},
        expected_side_effect="send",
        idempotency_key="idem-a",
        deadline=None,
        fencing_token=1,
        actor_id="runtime-a",
        credential_ref="cred-a",
    )
    evaluation = await policy.evaluate(capability, invocation)
    assert evaluation.decision.value == "allow"
    assert evaluation.policy_version == "production-v1"

    vault = InMemoryVault({"cred-a": "super-secret"})
    proxy = CredentialProxy(vault)
    proxy.register_reference(
        "tenant-a",
        CredentialReference(
            credential_ref="cred-a",
            provider="managed",
            account_scope="account-a",
            allowed_operations=("send",),
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        ),
    )

    async def managed(request: dict[str, object], secret: str) -> dict[str, object]:
        return {"accepted": request["message"], "authorization": f"Bearer {secret}"}

    policy_validator = RemotePolicyClient(
        "http://policy.test",
        bearer_token="credential-token",
        service_identity=ServiceIdentity.CREDENTIAL_PROXY,
        transport=httpx.ASGITransport(app=policy_app),
    )
    credential_service = CredentialProxyInternalService(
        proxy, adapters={"managed": managed}, policy=policy_validator
    )
    credential_app = create_contract_app(
        "credential-proxy",
        credential_routes(credential_service),
        workload_identities={"hands-token": ServiceIdentity.ACTION_HANDS},
    )
    credential = RemoteCredentialProxy(
        "http://credential.test",
        bearer_token="hands-token",
        transport=httpx.ASGITransport(app=credential_app),
    )
    result = await credential.invoke(
        tenant_id="tenant-a",
        session_id="session-a",
        tool_name="managed",
        credential_ref="cred-a",
        operation="send",
        request={"message": "hello"},
        policy_decision_id=evaluation.decision_id,
    )
    assert result == {"accepted": "hello", "authorization": "Bearer [REDACTED]"}
    with pytest.raises(AuraClawError, match="policy decision"):
        await credential.invoke(
            tenant_id="tenant-a",
            session_id="session-a",
            tool_name="managed",
            credential_ref="cred-a",
            operation="send",
            request={"message": "hello"},
            policy_decision_id="forged-decision",
        )
    await credential.aclose()
    await policy_validator.aclose()
    await policy.aclose()


@pytest.mark.asyncio
async def test_policy_approval_is_bound_to_action_and_human_response_identity() -> None:
    app = create_contract_app(
        "policy",
        policy_routes(PolicyInternalService(version="production-v1")),
        workload_identities={
            "hands-token": ServiceIdentity.ACTION_HANDS,
            "task-token": ServiceIdentity.TASK_API,
        },
    )
    hands = RemotePolicyClient(
        "http://policy.test",
        bearer_token="hands-token",
        transport=httpx.ASGITransport(app=app),
    )
    task = RemotePolicyClient(
        "http://policy.test",
        bearer_token="task-token",
        service_identity=ServiceIdentity.TASK_API,
        transport=httpx.ASGITransport(app=app),
    )
    record = ApprovalRecord(
        approval_id="approval-a",
        tenant_id="tenant-a",
        session_id="session-a",
        run_id="run-a",
        action_digest="digest-a",
        tool_name="managed",
        redacted_arguments={"target": "safe"},
        risk=RiskLevel.HIGH,
        reason="write requires approval",
        expected_effect="write",
        allowed_decisions=("approve", "reject"),
        assigned_approvers=(),
        policy_version="production-v1",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    await hands.request_approval(record)
    assert not await hands.validate_approval(
        tenant_id="tenant-a",
        approval_id="approval-a",
        session_id="session-a",
        run_id="run-a",
        action_digest="digest-a",
        policy_version="production-v1",
    )
    await task.record_human_response(record, decision="approved", feedback=None)
    assert await hands.validate_approval(
        tenant_id="tenant-a",
        approval_id="approval-a",
        session_id="session-a",
        run_id="run-a",
        action_digest="digest-a",
        policy_version="production-v1",
    )
    assert not await hands.validate_approval(
        tenant_id="tenant-a",
        approval_id="approval-a",
        session_id="session-a",
        run_id="run-a",
        action_digest="changed-digest",
        policy_version="production-v1",
    )
    assert not await hands.validate_approval(
        tenant_id="tenant-b",
        approval_id="approval-a",
        session_id="session-a",
        run_id="run-a",
        action_digest="digest-a",
        policy_version="production-v1",
    )
    expired = replace(
        record,
        approval_id="approval-expired",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    await hands.request_approval(expired)
    assert not await hands.validate_approval(
        tenant_id="tenant-a",
        approval_id="approval-expired",
        session_id="session-a",
        run_id="run-a",
        action_digest="digest-a",
        policy_version="production-v1",
    )
    await task.aclose()
    await hands.aclose()


@pytest.mark.asyncio
async def test_artifact_service_presigns_seaweedfs_upload_and_hands_has_no_s3_secret() -> None:
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="seaweed-access",
            secret_key="seaweed-secret",
            bucket="artifacts",
            region="us-east-1",
        )
    )
    app = create_contract_app(
        "artifact-service",
        artifact_routes(service),
        workload_identities={"hands-token": ServiceIdentity.ACTION_HANDS},
    )

    async def uploaded(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.host == "seaweed.test"
        assert "X-Amz-Signature" in request.url.params
        return httpx.Response(200)

    writer = RemoteArtifactWriter(
        "http://artifact.test",
        bearer_token="hands-token",
        transport=httpx.ASGITransport(app=app),
        object_transport=httpx.MockTransport(uploaded),
    )
    ref = await writer.put(
        tenant_id="tenant-a",
        root_session_id="root-a",
        session_id="session-a",
        content=b"artifact body",
        artifact_type="tool-output",
        media_type="text/plain",
        name="result.txt",
        producer="tool:test",
    )
    assert ref.content_hash
    assert ref.size == 13
    await writer.aclose()


def test_compose_injects_secrets_only_into_their_owner_services() -> None:
    compose = (ROOT / "compose.services.yml").read_text()
    assert "env_file:" not in compose

    def count_key(key: str) -> int:
        return len(re.findall(rf"^\s+{key}:", compose, re.MULTILINE))

    assert count_key("SEAWEEDFS_SECRET_KEY") == 1
    assert count_key("SEAWEEDFS_ACCESS_KEY") == 1
    assert count_key("AURACLAW_MODEL_API_KEY") == 1
    assert count_key("AURACLAW_LEASE_SIGNING_KEY") == 3
    runtime = compose.split("  agent-runtime:", 1)[1].split("  model-gateway:", 1)[0]
    assert "DATABASE_URL" not in runtime
    assert "MODEL_API_KEY" not in runtime
    assert "SEAWEEDFS" not in runtime
    assert "platform" not in runtime
    assert "internal: true" in compose


def test_s3_database_roles_and_ops_clients_preserve_owner_boundaries() -> None:
    roles = (ROOT / "deploy/postgres/roles.sql").read_text()
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT" in roles
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA projection TO auraclaw_task_query_ro" in roles
    assert "session_core TO auraclaw_session" in roles
    assert "hands TO auraclaw_hands" in roles
    assert "credential TO auraclaw_credential" in roles
    assert "streaming TO auraclaw_streaming" in roles
    assert "model_gateway TO auraclaw_model" in roles
    cli = (ROOT / "src/auraclaw/composition/cli.py").read_text()
    assert "RemoteAdminClient" in cli
    assert "PostgresOperationsStore" not in cli
