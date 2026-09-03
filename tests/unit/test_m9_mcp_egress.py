from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    McpAuthStrategy,
    McpOAuthConfiguration,
    McpServerDefinition,
)
from auraclaw.contracts.errors import CredentialAccessError, McpTransportError, PolicyDeniedError
from auraclaw.contracts.tools import CredentialReference, PolicyDecision
from auraclaw.infrastructure.connectors.mcp.transport import ManagedRemoteMcpTransport
from auraclaw.infrastructure.connectors.mcp.wire import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
    McpJsonRpcRequest,
    McpTrustedContext,
)
from auraclaw.infrastructure.credentials.mcp_egress import (
    ManagedMcpEgressAdapter,
    McpEgressResponse,
)
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault


class _Resolver:
    def __init__(self, addresses: tuple[str, ...] = ("93.184.216.34",)) -> None:
        self.addresses = addresses
        self.calls: list[tuple[str, int]] = []

    async def resolve(self, host: str, port: int) -> tuple[str, ...]:
        self.calls.append((host, port))
        return self.addresses


class _Sender:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.redirect = False
        self.sse = False
        self.require_oauth_bearer = True

    async def send(self, **request: object) -> McpEgressResponse:
        self.calls.append(request)
        url = str(request["url"])
        if self.redirect:
            return McpEgressResponse(
                status_code=302,
                headers={"location": "https://attacker.example/mcp"},
                content=b"",
            )
        if url == "https://mcp.example/.well-known/oauth-protected-resource":
            return McpEgressResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                content=(
                    b'{"resource":"https://mcp.example/v1/mcp",'
                    b'"authorization_servers":["https://auth.example"]}'
                ),
            )
        if url == "https://auth.example/.well-known/oauth-authorization-server":
            return McpEgressResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                content=(
                    b'{"issuer":"https://auth.example",'
                    b'"token_endpoint":"https://auth.example/oauth/token",'
                    b'"grant_types_supported":["client_credentials"]}'
                ),
            )
        if url == "https://auth.example/oauth/token":
            return McpEgressResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                content=(
                    b'{"access_token":"remote-access-token",'
                    b'"token_type":"Bearer","expires_in":3600}'
                ),
            )
        authorization = str(request["headers"])  # type: ignore[index]
        if self.require_oauth_bearer:
            assert "Bearer remote-access-token" in authorization
        if self.sse:
            return McpEgressResponse(
                status_code=200,
                headers={"content-type": "text/event-stream"},
                content=(
                    b'data: {"jsonrpc":"2.0","method":'
                    b'"notifications/tools/list_changed","params":{}}\n\n'
                    b'data: {"jsonrpc":"2.0","id":1,"result":{"tools":[]}}\n\n'
                ),
            )
        return McpEgressResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=(
                b'{"jsonrpc":"2.0","id":1,"result":{"value":"remote-access-token must not escape"}}'
            ),
        )


def _server() -> McpServerDefinition:
    return McpServerDefinition(
        server_id="github-mcp",
        tenant_id="tenant-a",
        title="GitHub MCP",
        endpoint="https://mcp.example/v1/mcp",
        credential_ref="vault/github-mcp#client_secret",
        oauth=McpOAuthConfiguration(
            protected_resource_metadata_url=(
                "https://mcp.example/.well-known/oauth-protected-resource"
            ),
            authorization_server_metadata_url=(
                "https://auth.example/.well-known/oauth-authorization-server"
            ),
            issuer="https://auth.example",
            token_endpoint="https://auth.example/oauth/token",
            client_id="auraclaw-hands",
            resource="https://mcp.example/v1/mcp",
            scopes=("tools.read", "tools.write"),
        ),
        allowed_resource_schemes=("github",),
        allowed_prompt_prefixes=("github.",),
        status=CapabilityStatus.ACTIVE,
        enabled=True,
    )


def _request() -> dict[str, object]:
    return {
        "server_id": "github-mcp",
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "github.issue.get",
            "arguments": {"number": 21},
            "_meta": {
                MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                MCP_CLIENT_CAPABILITIES_META_KEY: {},
            },
        },
    }


class _AllowPolicy:
    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        assert arguments["resource"] == "mcp:github-mcp"
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="policy-remote-1",
            policy_version="m9-v1",
        )


class _PermissionPolicy:
    def __init__(self) -> None:
        self.attributes: list[dict[str, object]] = []

    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        attributes = arguments["attributes"]
        assert isinstance(attributes, dict)
        self.attributes.append(attributes)
        return PolicyEvaluation(
            decision=(
                PolicyDecision.ALLOW
                if attributes.get("permission") == "read-only"
                else PolicyDecision.REQUIRE_APPROVAL
            ),
            decision_id="policy-read-only",
            policy_version="m9-v2",
        )


class _Credentials:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.notifications = False

    async def invoke(self, **arguments: object) -> dict[str, object]:
        self.calls.append(arguments)
        response: dict[str, object] = {
            "jsonrpc": "2.0",
            "id": 7,
            "result": {"tools": []},
        }
        if self.notifications:
            response["_auraclaw_notifications"] = [
                {
                    "method": "notifications/tools/list_changed",
                    "params": {},
                }
            ]
        return response

    def redact(self, value: object) -> object:
        return value


def _trusted(tenant_id: str = "tenant-a") -> McpTrustedContext:
    return McpTrustedContext(
        tenant_id=tenant_id,
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
    )


def test_mcp_egress_uses_resource_indicator_pins_dns_and_hides_tokens() -> None:
    async def scenario() -> None:
        resolver = _Resolver()
        sender = _Sender()
        adapter = ManagedMcpEgressAdapter(
            _server(),
            resolver=resolver,
            sender=sender,
        )
        proxy = CredentialProxy(
            InMemoryVault({"vault/github-mcp#client_secret": "oauth-client-secret"})
        )
        proxy.register_reference(
            "tenant-a",
            CredentialReference(
                credential_ref="vault/github-mcp#client_secret",
                provider="github-mcp",
                account_scope="https://mcp.example/v1/mcp",
                allowed_operations=("mcp.invoke",),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        )
        result = await proxy.invoke(
            tenant_id="tenant-a",
            session_id="session-1",
            tool_name="github-mcp",
            credential_ref="vault/github-mcp#client_secret",
            operation="mcp.invoke",
            request=_request(),
            adapter=adapter,
            policy_decision_id="policy-1",
        )

        assert result["result"]["value"] == "[REDACTED] must not escape"
        assert [call["approved_ip"] for call in sender.calls] == [
            "93.184.216.34",
            "93.184.216.34",
            "93.184.216.34",
            "93.184.216.34",
        ]
        token_body = parse_qs(bytes(sender.calls[2]["content"]).decode())
        assert token_body["resource"] == ["https://mcp.example/v1/mcp"]
        assert token_body["client_secret"] == ["oauth-client-secret"]
        assert sender.calls[3]["url"] == "https://mcp.example/v1/mcp"
        request_headers = sender.calls[3]["headers"]
        assert isinstance(request_headers, dict)
        assert request_headers["MCP-Protocol-Version"] == MCP_PROTOCOL_VERSION
        assert request_headers["Mcp-Method"] == "tools/call"
        assert request_headers["Mcp-Name"] == "github.issue.get"
        assert resolver.calls == [
            ("mcp.example", 443),
            ("auth.example", 443),
            ("auth.example", 443),
            ("mcp.example", 443),
        ]

        await proxy.invoke(
            tenant_id="tenant-a",
            session_id="session-1",
            tool_name="github-mcp",
            credential_ref="vault/github-mcp#client_secret",
            operation="mcp.invoke",
            request=_request(),
            adapter=adapter,
        )
        assert (
            len(
                [call for call in sender.calls if call["url"] == "https://auth.example/oauth/token"]
            )
            == 1
        )
        sender.sse = True
        streamed = await adapter(_request(), "oauth-client-secret")
        assert streamed["result"] == {"tools": []}
        assert streamed["_auraclaw_notifications"][0]["method"] == (
            "notifications/tools/list_changed"
        )
        with pytest.raises(CredentialAccessError):
            await proxy.invoke(
                tenant_id="tenant-b",
                session_id="session-1",
                tool_name="github-mcp",
                credential_ref="vault/github-mcp#client_secret",
                operation="mcp.invoke",
                request=_request(),
                adapter=adapter,
            )
        wrong_scope_proxy = CredentialProxy(
            InMemoryVault({"vault/github-mcp#client_secret": "oauth-client-secret"})
        )
        wrong_scope_proxy.register_reference(
            "tenant-a",
            CredentialReference(
                credential_ref="vault/github-mcp#client_secret",
                provider="github-mcp",
                account_scope="https://other.example/mcp",
                allowed_operations=("mcp.invoke",),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        )
        with pytest.raises(CredentialAccessError, match="scope"):
            await wrong_scope_proxy.invoke(
                tenant_id="tenant-a",
                session_id="session-1",
                tool_name="github-mcp",
                credential_ref="vault/github-mcp#client_secret",
                operation="mcp.invoke",
                request=_request(),
                adapter=adapter,
            )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "egress_request",
    (
        {**_request(), "target_url": "https://attacker.example/mcp"},
        {
            **_request(),
            "params": {
                "name": "github.issue.get",
                "arguments": {"access_token": "passthrough"},
            },
        },
        {
            **_request(),
            "params": {"name": "unregistered.issue.get", "arguments": {}},
        },
    ),
)
def test_mcp_egress_rejects_caller_targets_tokens_and_unlisted_tools(
    egress_request: dict[str, object],
) -> None:
    async def scenario() -> None:
        adapter = ManagedMcpEgressAdapter(
            _server(),
            resolver=_Resolver(),
            sender=_Sender(),
        )
        with pytest.raises(CredentialAccessError):
            await adapter(egress_request, "client-secret")

    asyncio.run(scenario())


def test_mcp_egress_rejects_private_dns_and_redirects() -> None:
    async def scenario() -> None:
        private_sender = _Sender()
        private = ManagedMcpEgressAdapter(
            _server(),
            resolver=_Resolver(("127.0.0.1",)),
            sender=private_sender,
        )
        with pytest.raises(CredentialAccessError, match="non-public"):
            await private(_request(), "client-secret")
        assert private_sender.calls == []

        redirect_sender = _Sender()
        redirect_sender.redirect = True
        redirect = ManagedMcpEgressAdapter(
            _server(),
            resolver=_Resolver(),
            sender=redirect_sender,
        )
        with pytest.raises(CredentialAccessError, match="redirects"):
            await redirect(_request(), "client-secret")

    asyncio.run(scenario())


def test_mcp_egress_allows_loopback_http_when_private_host_allowlisted() -> None:
    async def scenario() -> None:
        class LoopbackSender:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            async def send(self, **request: object) -> McpEgressResponse:
                self.calls.append(request)
                return McpEgressResponse(
                    status_code=200,
                    headers={"content-type": "application/json"},
                    content=b'{"jsonrpc":"2.0","id":1,"result":{"ok":true}}',
                )

        sender = LoopbackSender()
        adapter = ManagedMcpEgressAdapter(
            McpServerDefinition(
                server_id="java-mcp",
                tenant_id="development",
                title="Java Agent Runtime MCP Gateway",
                endpoint="http://127.0.0.1:48080/rpc-api/agent-runtime/mcp",
                credential_ref="vault/java-mcp#client_secret",
                allowed_private_hosts=("127.0.0.1",),
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            ),
            resolver=_Resolver(("127.0.0.1",)),
            sender=sender,
        )
        await adapter(
            {
                "server_id": "java-mcp",
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "agent.runtime.ping",
                    "arguments": {},
                    "_meta": {
                        MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                        MCP_CLIENT_CAPABILITIES_META_KEY: {},
                    },
                },
            },
            "local-java-mcp-debug",
        )
        assert sender.calls
        assert sender.calls[0]["url"] == "http://127.0.0.1:48080/rpc-api/agent-runtime/mcp"
        assert sender.calls[0]["approved_ip"] == "127.0.0.1"

    asyncio.run(scenario())


def test_mcp_egress_rejects_public_http_even_when_host_allowlisted() -> None:
    async def scenario() -> None:
        sender = _Sender()
        server = McpServerDefinition(
            server_id="github-mcp",
            tenant_id="tenant-a",
            title="Public HTTP MCP",
            endpoint="http://mcp.example.com/mcp",
            credential_ref="vault/github-mcp#client_secret",
            auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
            allowed_private_hosts=("mcp.example.com",),
            status=CapabilityStatus.ACTIVE,
            enabled=True,
        )
        adapter = ManagedMcpEgressAdapter(
            server,
            resolver=_Resolver(("93.184.216.34",)),
            sender=sender,
        )

        with pytest.raises(CredentialAccessError, match="requires HTTPS"):
            await adapter(_request(), "client-secret")
        assert sender.calls == []

    asyncio.run(scenario())


def test_hands_remote_transport_passes_only_reference_and_policy_evidence() -> None:
    async def scenario() -> None:
        credentials = _Credentials()
        transport = ManagedRemoteMcpTransport(
            _server(),
            credentials=credentials,
            policy=_AllowPolicy(),
        )
        notifications: list[tuple[str, str]] = []

        async def handle(
            server_id: str,
            method: str,
            params: dict[str, object],
        ) -> bool:
            del params
            notifications.append((server_id, method))
            return True

        transport.set_notification_handler(handle)
        credentials.notifications = True
        response = await transport.send(
            McpJsonRpcRequest(id=7, method="tools/list"),
            trusted_context=_trusted(),
        )
        assert response.result == {"tools": []}
        call = credentials.calls[0]
        assert call["credential_ref"] == "vault/github-mcp#client_secret"
        assert call["policy_decision_id"] == "policy-remote-1"
        assert call["tool_name"] == "mcp:github-mcp"
        assert "oauth-client-secret" not in repr(call)
        assert notifications == [("github-mcp", "notifications/tools/list_changed")]

        with pytest.raises(PolicyDeniedError, match="tenant scope"):
            await transport.send(
                McpJsonRpcRequest(id=8, method="tools/list"),
                trusted_context=_trusted("tenant-b"),
            )

    asyncio.run(scenario())


def test_hands_remote_transport_rejects_mismatched_response_id() -> None:
    class MismatchedCredentials(_Credentials):
        async def invoke(self, **arguments: object) -> dict[str, object]:
            del arguments
            return {"jsonrpc": "2.0", "id": "wrong", "result": {}}

    async def scenario() -> None:
        transport = ManagedRemoteMcpTransport(
            _server(),
            credentials=MismatchedCredentials(),
            policy=_AllowPolicy(),
        )
        with pytest.raises(McpTransportError, match="response id"):
            await transport.send(
                McpJsonRpcRequest(id="expected", method="tools/list"),
                trusted_context=_trusted(),
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["resources/read", "prompts/get", "tools/call"])
def test_hands_remote_transport_skips_approval_for_read_only_mcp_calls(
    method: str,
) -> None:
    async def scenario() -> None:
        credentials = _Credentials()
        policy = _PermissionPolicy()
        transport = ManagedRemoteMcpTransport(
            _server(),
            credentials=credentials,
            policy=policy,
        )
        response = await transport.send(
            McpJsonRpcRequest(id=7, method=method),
            trusted_context=_trusted(),
            read_only=True,
        )
        assert response.result == {"tools": []}
        assert credentials.calls[0]["policy_decision_id"] == "policy-read-only"
        assert policy.attributes[0]["permission"] == "read-only"

        with pytest.raises(PolicyDeniedError, match="policy denied"):
            await transport.send(
                McpJsonRpcRequest(id=8, method="tools/call"),
                trusted_context=_trusted(),
            )

    asyncio.run(scenario())


def test_mcp_egress_sends_department_snapshot_headers() -> None:
    async def scenario() -> None:
        sender = _Sender()
        sender.require_oauth_bearer = False
        server = McpServerDefinition(
            server_id="github-mcp",
            tenant_id="tenant-a",
            title="ChainTower MCP",
            endpoint="https://mcp.example/v1/mcp",
            credential_ref="vault/chaintower-mcp#workload",
            auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
            status=CapabilityStatus.ACTIVE,
            enabled=True,
        )
        adapter = ManagedMcpEgressAdapter(
            server,
            resolver=_Resolver(),
            sender=sender,
        )
        await adapter(
            {
                **_request(),
                "_auraclaw_identity": {
                    "tenant_id": "1",
                    "user_id": "101",
                    "dept_id": "9",
                    "session_id": "ses-1",
                },
            },
            "w" * 48,
        )
        headers = sender.calls[-1]["headers"]
        assert isinstance(headers, dict)
        assert headers["X-CT-Tenant-ID"] == "1"
        assert headers["X-CT-User-ID"] == "101"
        assert headers["X-CT-Dept-ID"] == "9"
        assert headers["X-CT-Session-ID"] == "ses-1"

        await adapter(
            {
                **_request(),
                "id": 2,
                "_auraclaw_identity": {
                    "tenant_id": "1",
                    "user_id": "101",
                    "dept_id": None,
                    "session_id": "ses-1",
                },
            },
            "w" * 48,
        )
        missing = sender.calls[-1]["headers"]
        assert isinstance(missing, dict)
        assert "X-CT-Dept-ID" not in missing
        assert missing["X-CT-User-ID"] == "101"

    asyncio.run(scenario())


def test_mcp_server_configuration_is_typed_and_secret_free() -> None:
    server = _server()
    serialized = server.model_dump_json()
    assert "oauth-client-secret" not in serialized
    assert "vault/github-mcp#client_secret" in serialized


@pytest.mark.parametrize("name", ["github.issue.get", "outside.issue.get", "lookup"])
def test_mcp_egress_does_not_restrict_tool_name_prefix(name: str) -> None:
    async def scenario() -> None:
        sender = _Sender()
        adapter = ManagedMcpEgressAdapter(_server(), resolver=_Resolver(), sender=sender)
        request = _request()
        request["params"]["name"] = name
        await adapter(request, "oauth-client-secret")
        assert json.loads(sender.calls[-1]["content"])["params"]["name"] == name
        assert sender.calls[-1]["headers"]["Mcp-Name"] == name

    asyncio.run(scenario())


@pytest.mark.parametrize("name", [None, "", "   ", 42, [], {}])
def test_mcp_egress_rejects_invalid_tool_name_before_network(name: object) -> None:
    async def scenario() -> None:
        sender = _Sender()
        adapter = ManagedMcpEgressAdapter(_server(), resolver=_Resolver(), sender=sender)
        request = _request()
        request["params"]["name"] = name
        with pytest.raises(CredentialAccessError, match="non-empty string"):
            await adapter(request, "oauth-client-secret")
        assert sender.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "method,params",
    [
        ("resources/read", {"uri": "https://outside.example/data"}),
        ("prompts/get", {"name": "outside.review"}),
    ],
)
def test_mcp_egress_keeps_resource_and_prompt_filters(method: str, params: dict) -> None:
    async def scenario() -> None:
        sender = _Sender()
        adapter = ManagedMcpEgressAdapter(_server(), resolver=_Resolver(), sender=sender)
        request = _request()
        request["method"] = method
        request["params"].update(params)
        with pytest.raises(CredentialAccessError, match="outside .*allowlist"):
            await adapter(request, "oauth-client-secret")
        assert sender.calls == []

    asyncio.run(scenario())
