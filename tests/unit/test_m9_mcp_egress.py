from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.action.remote_mcp import ManagedRemoteMcpTransport
from auraclaw.config import Settings
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpOAuthConfiguration,
    McpServerDefinition,
)
from auraclaw.contracts.errors import CredentialAccessError, PolicyDeniedError
from auraclaw.contracts.mcp import (
    McpJsonRpcRequest,
    McpTrustedContext,
)
from auraclaw.contracts.tools import CredentialReference, PolicyDecision
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

    async def send(self, **request: object) -> McpEgressResponse:
        self.calls.append(request)
        url = str(request["url"])
        if self.redirect:
            return McpEgressResponse(
                status_code=302,
                headers={"location": "https://attacker.example/mcp"},
                content=b"",
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
        assert "Bearer remote-access-token" in authorization
        return McpEgressResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=(
                b'{"jsonrpc":"2.0","id":1,"result":'
                b'{"value":"remote-access-token must not escape"}}'
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
            token_endpoint="https://auth.example/oauth/token",
            client_id="auraclaw-hands",
            resource="https://mcp.example/v1/mcp",
            scopes=("tools.read", "tools.write"),
        ),
        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
        allowed_tool_prefixes=("github.",),
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
        "params": {"name": "github.issue.get", "arguments": {"number": 21}},
    }


class _AllowPolicy:
    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        assert arguments["resource"] == "mcp:github-mcp"
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="policy-remote-1",
            policy_version="m9-v1",
        )


class _Credentials:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def invoke(self, **arguments: object) -> dict[str, object]:
        self.calls.append(arguments)
        return {"jsonrpc": "2.0", "id": 7, "result": {"tools": []}}

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
            InMemoryVault(
                {"vault/github-mcp#client_secret": "oauth-client-secret"}
            )
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
        ]
        token_body = parse_qs(bytes(sender.calls[0]["content"]).decode())
        assert token_body["resource"] == ["https://mcp.example/v1/mcp"]
        assert token_body["client_secret"] == ["oauth-client-secret"]
        assert sender.calls[1]["url"] == "https://mcp.example/v1/mcp"
        assert resolver.calls == [
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
        assert len(
            [
                call
                for call in sender.calls
                if call["url"] == "https://auth.example/oauth/token"
            ]
        ) == 1
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
            InMemoryVault(
                {"vault/github-mcp#client_secret": "oauth-client-secret"}
            )
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


def test_hands_remote_transport_passes_only_reference_and_policy_evidence() -> None:
    async def scenario() -> None:
        credentials = _Credentials()
        transport = ManagedRemoteMcpTransport(
            _server(),
            credentials=credentials,
            policy=_AllowPolicy(),
        )
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

        with pytest.raises(PolicyDeniedError, match="tenant scope"):
            await transport.send(
                McpJsonRpcRequest(id=8, method="tools/list"),
                trusted_context=_trusted("tenant-b"),
            )

    asyncio.run(scenario())


def test_mcp_egress_server_configuration_is_typed_and_secret_free() -> None:
    server = _server()
    settings = Settings(
        mcp_egress_servers_json=json.dumps(
            [server.model_dump(mode="json")]
        )
    )
    assert settings.mcp_egress_servers == (server,)
    serialized = settings.mcp_egress_servers_json
    assert "oauth-client-secret" not in serialized
    assert "vault/github-mcp#client_secret" in serialized
