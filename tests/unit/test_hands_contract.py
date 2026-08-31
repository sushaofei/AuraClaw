from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from auraclaw.action.hands import HandsGateway
from auraclaw.action.hands_http import (
    SignedLeaseHandsAuthenticator,
    StaticHandsAuthenticator,
    create_hands_http_app,
)
from auraclaw.action.mcp_primitives import (
    HandsPromptRegistry,
    HandsResourceRegistry,
    RegisteredPrompt,
    RegisteredResource,
    RegisteredResourceTemplate,
)
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.hands import (
    HANDS_CONTRACT_VERSION,
    HANDS_MAX_REQUEST_BYTES,
    HANDS_TOOLS_CALL,
    HANDS_TOOLS_LIST,
    HandsPromptArgument,
    HandsPromptDescriptor,
    HandsPromptMessage,
    HandsPromptResult,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsToolCall,
    HandsTrustedContext,
)
from auraclaw.contracts.internal import INTERNAL_API_VERSION, LeaseAssertion
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolPermission,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.ports import ToolCall

SIGNING_KEY = b"hands-contract-signing-key-00000001"


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


class _DenyPolicy(PolicyEngine):
    def evaluate(self, capability: ToolCapability, invocation: object = None) -> Any:
        del capability, invocation
        from auraclaw.contracts.tools import PolicyDecision

        return PolicyDecision.DENY


class _RequireApprovalPolicy(PolicyEngine):
    def evaluate(self, capability: ToolCapability, invocation: object = None) -> Any:
        del capability, invocation
        from auraclaw.contracts.tools import PolicyDecision

        return PolicyDecision.REQUIRE_APPROVAL


def _assignment(
    *,
    tenant_id: str = "tenant-a",
    runtime_id: str = "runtime-a",
    user_id: str | None = "user-101",
) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id=tenant_id,
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id=runtime_id,
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
        deadline=datetime.now(UTC) + timedelta(minutes=1),
        user_id=user_id,
    )


def _trusted(assignment: RuntimeAssignment) -> HandsTrustedContext:
    return HandsTrustedContext(
        tenant_id=assignment.tenant_id,
        root_session_id=assignment.root_session_id,
        session_id=assignment.session_id,
        run_id=assignment.run_id,
        runtime_id=assignment.runtime_id,
        lease_id=assignment.lease_id,
        fencing_token=assignment.fencing_token,
        deadline=assignment.deadline,
        user_id=assignment.user_id,
    )


def _gateway(
    *,
    hands: Any | None = None,
    policy: PolicyEngine | None = None,
    permission: ToolPermission = ToolPermission.READ_ONLY,
    runtime_location: str = "hands",
) -> tuple[HandsGateway, _RecordingHands]:
    capability = ToolCapability(
        name="lookup",
        version="1",
        description="lookup managed data",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        permission=permission,
        risk_level=RiskLevel.LOW,
        runtime_location=runtime_location,
    )
    registry = ToolRegistry((capability,))
    recorder = hands or _RecordingHands()
    resources = HandsResourceRegistry(
        resources=(
            RegisteredResource(
                descriptor=HandsResourceDescriptor(
                    uri="memory://a",
                    name="a",
                    mime_type="text/plain",
                ),
                contents=(
                    HandsResourceContent(
                        uri="memory://a",
                        mime_type="text/plain",
                        text="alpha",
                    ),
                ),
                tenant_ids=("tenant-a",),
            ),
            RegisteredResource(
                descriptor=HandsResourceDescriptor(
                    uri="memory://shared",
                    name="shared",
                    mime_type="text/plain",
                ),
                contents=(
                    HandsResourceContent(
                        uri="memory://shared",
                        mime_type="text/plain",
                        text="shared",
                    ),
                ),
            ),
        ),
        templates=(
            RegisteredResourceTemplate(
                descriptor=HandsResourceDescriptor(
                    uri_template="memory://items/{id}",
                    name="items",
                ),
            ),
        ),
    )
    prompts = HandsPromptRegistry(
        (
            RegisteredPrompt(
                descriptor=HandsPromptDescriptor(
                    name="review",
                    arguments=(HandsPromptArgument(name="target", required=True),),
                ),
                renderer=lambda arguments, trusted: HandsPromptResult(
                    messages=(
                        HandsPromptMessage(
                            role="user",
                            content={
                                "text": (
                                    f"Review {arguments['target']} for {trusted.tenant_id}"
                                )
                            },
                        ),
                    )
                ),
                tenant_ids=("tenant-a",),
            ),
        )
    )
    gateway = ToolGateway(
        registry=registry,
        policy=policy or PolicyEngine(version="hands-v1"),
        approvals=_ApprovalReader(),
        hands=recorder,
        artifacts=ArtifactStore(
            InMemoryObjectStorage(), signing_key=b"hands-contract-artifact-key"
        ),
    )
    return (
        HandsGateway(
            registry=registry,
            gateway=gateway,
            resources=resources,
            prompts=prompts,
            page_size=1,
        ),
        recorder,
    )


async def _call_with_client(kind: str, gateway: HandsGateway, assignment: RuntimeAssignment):
    if kind == "in-process":
        yield InProcessHandsClient(gateway)
        return
    token = "runtime-token"
    app = create_hands_http_app(
        gateway,
        authenticator=StaticHandsAuthenticator({token: _trusted(assignment)}),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://hands",
    ) as raw:
        yield HttpHandsClient(raw, bearer_tokens={assignment.runtime_id: token})


@pytest.mark.parametrize("kind", ["in-process", "http"])
def test_hands_list_call_resource_prompt_and_idempotency(kind: str) -> None:
    async def scenario() -> None:
        gateway, recorder = _gateway()
        assignment = _assignment()
        async for client in _call_with_client(kind, gateway, assignment):
            tools = await client.list_tools(assignment)
            assert [item.name for item in tools.items] == ["lookup"]
            assert tools.next_cursor is None

            first = await client.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="tool-stable-1",
                    name="lookup",
                    arguments={"tenant_id": "attacker-controlled"},
                ),
            )
            assert first.status == "success"
            status = await client.get_invocation_status(
                assignment, "tool-stable-1"
            )
            assert status.found
            assert status.status == "success"
            assert recorder.invocations[0].tenant_id == "tenant-a"
            assert recorder.invocations[0].user_id == "user-101"
            repeated = await client.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="tool-stable-1",
                    name="lookup",
                    arguments={"tenant_id": "attacker-controlled"},
                ),
            )
            assert repeated == first
            assert len(recorder.invocations) == 1

            resources = await client.list_resources(assignment)
            assert [item.uri for item in resources.items] == ["memory://a"]
            assert resources.next_cursor is not None
            rest = await client.list_resources(assignment, cursor=resources.next_cursor)
            assert [item.uri for item in rest.items] == ["memory://shared"]
            contents = await client.read_resource(assignment, "memory://a")
            assert contents[0].text == "alpha"

            templates = await client.list_resource_templates(assignment)
            assert templates.items[0].uri_template == "memory://items/{id}"
            prompt = await client.get_prompt(
                assignment, "review", arguments={"target": "pull request"}
            )
            assert prompt.messages[0].content["text"] == (
                "Review pull request for tenant-a"
            )
            other = _assignment(tenant_id="tenant-b", runtime_id="runtime-b")
            if kind == "in-process":
                visible = await client.list_resources(other)
                assert [item.uri for item in visible.items] == ["memory://shared"]
                with pytest.raises(KeyError):
                    await client.read_resource(other, "memory://a")

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["in-process", "http"])
def test_hands_policy_deny_and_approval_required(kind: str) -> None:
    async def scenario() -> None:
        denied_gateway, _denied = _gateway(policy=_DenyPolicy())
        assignment = _assignment()
        async for client in _call_with_client(kind, denied_gateway, assignment):
            denied = await client.call_tool(
                assignment,
                HandsToolCall(tool_invocation_id="deny-1", name="lookup", arguments={}),
            )
            assert denied.status == "denied"
            assert denied.error_code == "policy_denied"

        approval_gateway, _approval = _gateway(
            permission=ToolPermission.WRITE_WITH_APPROVAL
        )
        async for client in _call_with_client(kind, approval_gateway, assignment):
            pending = await client.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="approve-1",
                    name="lookup",
                    arguments={"value": 1},
                    expected_side_effect="write",
                ),
            )
            assert pending.status == "denied"
            assert pending.error_code == "approval_required"

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["in-process", "http"])
def test_read_only_remote_mcp_tool_does_not_require_approval(kind: str) -> None:
    async def scenario() -> None:
        gateway, recorder = _gateway(
            policy=_RequireApprovalPolicy(),
            permission=ToolPermission.READ_ONLY,
            runtime_location="remote-mcp",
        )
        assignment = _assignment()
        async for client in _call_with_client(kind, gateway, assignment):
            result = await client.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="read-mcp-1",
                    name="lookup",
                    arguments={},
                    expected_side_effect="read",
                ),
            )
            assert result.status == "success"
            assert len(recorder.invocations) == 1

    asyncio.run(scenario())


def test_http_hands_rejects_auth_version_size_and_schema_errors() -> None:
    async def scenario() -> None:
        gateway, _recorder = _gateway()
        assignment = _assignment()
        app = create_hands_http_app(
            gateway,
            authenticator=StaticHandsAuthenticator(
                {"runtime-token": _trusted(assignment)}
            ),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://hands",
        ) as raw:
            unauthenticated = await raw.post(HANDS_TOOLS_LIST, json={})
            assert unauthenticated.status_code == 401
            forbidden = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={"Authorization": "Bearer wrong"},
            )
            assert forbidden.status_code == 403
            version = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={
                    "Authorization": "Bearer runtime-token",
                    "X-AuraClaw-Contract-Version": "1999-01-01",
                },
            )
            assert version.status_code == 426
            invalid = await raw.post(
                HANDS_TOOLS_CALL,
                json={"name": "lookup"},
                headers={"Authorization": "Bearer runtime-token"},
            )
            assert invalid.status_code == 422
            oversized = await raw.post(
                HANDS_TOOLS_LIST,
                content=b"{}",
                headers={
                    "Authorization": "Bearer runtime-token",
                    "Content-Length": str(HANDS_MAX_REQUEST_BYTES + 1),
                    "Content-Type": "application/json",
                },
            )
            assert oversized.status_code == 413

            client = HttpHandsClient(
                raw, bearer_tokens={assignment.runtime_id: "runtime-token"}
            )
            adapter = HandsRuntimeAdapter(client)
            result = await adapter.execute(
                assignment,
                ToolCall(tool_invocation_id="http-1", name="lookup", arguments={}),
            )
            assert result["status"] == "success"

    asyncio.run(scenario())


def test_http_hands_retries_transient_connection_failures() -> None:
    async def scenario() -> None:
        assignment = _assignment()
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise httpx.ConnectError("temporary refusal", request=request)
            return httpx.Response(
                200,
                json={"status": "success", "content": {"value": "ready"}},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://hands",
        ) as raw:
            client = HttpHandsClient(
                raw,
                bearer_tokens={assignment.runtime_id: "runtime-token"},
                connect_retry_delay_seconds=0,
            )
            result = await client.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="retry-connect-1",
                    name="lookup",
                    idempotency_key="retry-connect-1",
                ),
            )

        assert result.status == "success"
        assert result.content == {"value": "ready"}
        assert attempts == 3

    asyncio.run(scenario())


def test_http_hands_reports_exhausted_connection_retries() -> None:
    async def scenario() -> None:
        assignment = _assignment()
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectError("connection refused", request=request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://hands",
        ) as raw:
            client = HttpHandsClient(
                raw,
                bearer_tokens={assignment.runtime_id: "runtime-token"},
                connect_attempts=2,
                connect_retry_delay_seconds=0,
            )
            with pytest.raises(
                AuraClawError,
                match="Action Hands is unavailable after 2 connection attempts",
            ) as raised:
                await client.call_tool(
                    assignment,
                    HandsToolCall(
                        tool_invocation_id="retry-connect-2",
                        name="lookup",
                        idempotency_key="retry-connect-2",
                    ),
                )

        assert raised.value.detail == (
            "transport=ConnectError; path=/internal/v1/hands/tools/call"
        )
        assert isinstance(raised.value.__cause__, httpx.ConnectError)
        assert attempts == 2

    asyncio.run(scenario())


def test_http_hands_replicas_share_gateway_without_sticky_sessions() -> None:
    async def scenario() -> None:
        gateway, recorder = _gateway()
        assignment = _assignment()
        token = "runtime-token"
        authenticator = StaticHandsAuthenticator({token: _trusted(assignment)})
        replica_a = create_hands_http_app(gateway, authenticator=authenticator)
        replica_b = create_hands_http_app(gateway, authenticator=authenticator)
        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=replica_a),
                base_url="http://hands-a",
            ) as raw_a,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=replica_b),
                base_url="http://hands-b",
            ) as raw_b,
        ):
            client_a = HttpHandsClient(
                raw_a, bearer_tokens={assignment.runtime_id: token}
            )
            client_b = HttpHandsClient(
                raw_b, bearer_tokens={assignment.runtime_id: token}
            )
            first = await client_a.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="replica-1",
                    name="lookup",
                    arguments={"value": 1},
                ),
            )
            second = await client_b.call_tool(
                assignment,
                HandsToolCall(
                    tool_invocation_id="replica-1",
                    name="lookup",
                    arguments={"value": 1},
                ),
            )
            assert first.status == "success"
            assert second == first
            assert len(recorder.invocations) == 1
            assert HANDS_CONTRACT_VERSION

    asyncio.run(scenario())


def test_signed_lease_rejects_expired_and_mismatched_runtime() -> None:
    async def scenario() -> None:
        gateway, _recorder = _gateway()
        signer = LeaseAssertionSigner(key_id="development", signing_key=SIGNING_KEY)
        verifier = LeaseAssertionVerifier(
            {"development": SIGNING_KEY},
            ledger=InMemoryFencingTokenLedger(),
            audience="runtime",
        )
        app = create_hands_http_app(
            gateway,
            authenticator=SignedLeaseHandsAuthenticator(
                {"runtime-token": "runtime-a"},
                verifier=verifier,
            ),
        )
        expired = signer.sign(
            LeaseAssertion(
                key_id="pending",
                audience="runtime",
                tenant_id="tenant-a",
                root_session_id="session-root",
                session_id="session-child",
                run_id="run-1",
                runtime_id="runtime-a",
                lease_id="lease-1",
                fencing_token=1,
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
                signature="",
            )
        )
        mismatched = signer.sign(
            LeaseAssertion(
                key_id="pending",
                audience="runtime",
                tenant_id="tenant-a",
                root_session_id="session-root",
                session_id="session-child",
                run_id="run-1",
                runtime_id="runtime-b",
                lease_id="lease-1",
                fencing_token=1,
                expires_at=datetime.now(UTC) + timedelta(minutes=1),
                signature="",
            )
        )
        valid = signer.sign(
            LeaseAssertion(
                key_id="pending",
                audience="runtime",
                tenant_id="tenant-a",
                root_session_id="session-root",
                session_id="session-child",
                run_id="run-1",
                runtime_id="runtime-a",
                lease_id="lease-1",
                fencing_token=1,
                expires_at=datetime.now(UTC) + timedelta(minutes=1),
                signature="",
            )
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://hands",
        ) as raw:
            missing = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={"Authorization": "Bearer runtime-token"},
            )
            assert missing.status_code == 401
            stale = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={
                    "Authorization": "Bearer runtime-token",
                    "X-AuraClaw-Lease-Assertion": expired.model_dump_json(),
                    "X-AuraClaw-Contract-Version": INTERNAL_API_VERSION,
                },
            )
            assert stale.status_code == 403
            wrong_runtime = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={
                    "Authorization": "Bearer runtime-token",
                    "X-AuraClaw-Lease-Assertion": mismatched.model_dump_json(),
                },
            )
            assert wrong_runtime.status_code == 403
            accepted = await raw.post(
                HANDS_TOOLS_LIST,
                json={},
                headers={
                    "Authorization": "Bearer runtime-token",
                    "X-AuraClaw-Lease-Assertion": valid.model_dump_json(),
                },
            )
            assert accepted.status_code == 200
            assert accepted.json()["items"][0]["name"] == "lookup"

    asyncio.run(scenario())


def test_hands_resource_content_requires_exactly_one_payload() -> None:
    with pytest.raises(ValidationError):
        HandsResourceContent(uri="memory://invalid")
    with pytest.raises(ValidationError):
        HandsResourceContent(uri="memory://invalid", text="x", blob="eA==")


class _TimeoutHands:
    async def execute(self, invocation: Any, capability: ToolCapability) -> dict[str, Any]:
        del invocation, capability
        raise TimeoutError("downstream timed out")


def test_hands_timeout_is_unknown_side_effect() -> None:
    async def scenario() -> None:
        gateway, _recorder = _gateway(hands=_TimeoutHands())
        assignment = _assignment()
        result = await InProcessHandsClient(gateway).call_tool(
            assignment,
            HandsToolCall(tool_invocation_id="timeout-1", name="lookup", arguments={}),
        )
        assert result.status == "timeout"
        assert result.side_effect_status == "unknown"
        assert result.error_code == "tool_timeout"

    asyncio.run(scenario())


class _LargeHands:
    async def execute(self, invocation: Any, capability: ToolCapability) -> dict[str, Any]:
        del invocation, capability
        return {"blob": "x" * 20_000}


def test_hands_large_result_returns_artifact_reference() -> None:
    async def scenario() -> None:
        capability = ToolCapability(
            name="lookup",
            version="1",
            description="lookup",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
        )
        registry = ToolRegistry((capability,))
        gateway = ToolGateway(
            registry=registry,
            policy=PolicyEngine(),
            approvals=_ApprovalReader(),
            hands=_LargeHands(),
            artifacts=ArtifactStore(
                InMemoryObjectStorage(), signing_key=b"hands-large-artifact-key"
            ),
            max_inline_bytes=64,
        )
        client = InProcessHandsClient(
            HandsGateway(registry=registry, gateway=gateway)
        )
        result = await client.call_tool(
            _assignment(),
            HandsToolCall(tool_invocation_id="large-1", name="lookup", arguments={}),
        )
        assert result.status == "success"
        assert isinstance(result.content, dict)
        assert "artifact_ref" in result.content

    asyncio.run(scenario())
