from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from auraclaw.composition.services import create_service_app
from auraclaw.config import Settings
from auraclaw.contracts.internal import (
    InternalRequestContext,
    PolicyEvaluateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.policy.internal_service import PolicyInternalService

_MCP_SERVER = {
    "server_id": "java-mcp",
    "tenant_id": "development",
    "title": "Java MCP Server",
    "endpoint": "https://java-mcp.example.com/mcp",
    "protocol_revision": "2026-07-28",
    "credential_ref": "vault/java-mcp#client_secret",
    "oauth": {
        "protected_resource_metadata_url": (
            "https://java-mcp.example.com/.well-known/oauth-protected-resource"
        ),
        "authorization_server_metadata_url": (
            "https://auth.example.com/.well-known/oauth-authorization-server"
        ),
        "issuer": "https://auth.example.com",
        "token_endpoint": "https://auth.example.com/oauth/token",
        "client_id": "auraclaw-hands",
        "resource": "https://java-mcp.example.com/mcp",
        "scopes": ["tools.read"],
    },
    "trust_level": "tenant_verified",
    "allowed_tool_prefixes": ["order."],
    "status": "active",
    "enabled": True,
}


def _settings(**values: object) -> Settings:
    defaults: dict[str, object] = {
        "storage_backend": "memory",
        "artifact_backend": "local",
    }
    defaults.update(values)
    return Settings(_env_file=None, **defaults)


def test_mcp_egress_servers_can_load_from_file(tmp_path) -> None:
    path = tmp_path / "java-mcp-servers.json"
    path.write_text(json.dumps([_MCP_SERVER]), encoding="utf-8")
    settings = _settings(mcp_egress_servers_file=str(path))
    servers = settings.mcp_egress_servers
    assert len(servers) == 1
    assert servers[0].server_id == "java-mcp"
    assert servers[0].endpoint == "https://java-mcp.example.com/mcp"


def test_development_hands_starts_catalog_reconciler_when_mcp_configured() -> None:
    settings = _settings(
        deployment_profile="development",
        mcp_egress_servers_json=json.dumps([_MCP_SERVER]),
        policy_base_url="http://127.0.0.1:9",
        credential_proxy_base_url="http://127.0.0.1:9",
        mcp_reconcile_interval_seconds=3600,
    )
    app = create_service_app("hands", settings)
    with TestClient(app):
        assert getattr(app.state, "catalog_reconciler", None) is not None
        assert "java-mcp" in app.state.capability_connectors


def test_policy_allows_mcp_remote_invoke_without_tool_permission() -> None:
    async def scenario() -> None:
        service = PolicyInternalService(version="s3-v1")
        response = await service.evaluate(
            PolicyEvaluateRequest(
                context=InternalRequestContext(
                    tenant_id="development",
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id="request-mcp-debug",
                    correlation_id="run-mcp-debug",
                    causation_id="run-mcp-debug",
                ),
                subject="action-hands-reconciler",
                action="mcp.remote.invoke",
                resource="mcp:java-mcp",
                input_digest="digest",
                attributes={
                    "method": "server/discover",
                    "server_id": "java-mcp",
                    "trust_level": "tenant_verified",
                },
            )
        )
        assert response.decision == PolicyDecision.ALLOW.value
        assert response.expires_at > datetime.now(UTC) - timedelta(seconds=1)

    asyncio.run(scenario())


def test_debug_mcp_credential_scope_matches_egress_adapter() -> None:
    from auraclaw.composition.services import _seed_managed_connector_credentials
    from auraclaw.contracts.capabilities import (
        CapabilityStatus,
        CapabilityTrustLevel,
        McpServerDefinition,
    )
    from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

    server = McpServerDefinition(
        server_id="java-mcp",
        tenant_id="local",
        title="Java Agent Runtime MCP Gateway",
        endpoint="http://127.0.0.1:48080/rpc-api/agent-runtime/mcp",
        protocol_revision="2025-06-18",
        credential_ref="vault/java-mcp#client_secret",
        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
        allowed_tool_prefixes=("",),
        allowed_private_hosts=("127.0.0.1",),
        status=CapabilityStatus.ACTIVE,
        enabled=True,
    )
    adapter = ManagedMcpEgressAdapter(server)
    settings = _settings(
        deployment_profile="development",
        mcp_egress_servers_json=json.dumps([server.model_dump(mode="json")]),
        debug_vault_secrets_json=json.dumps(
            {"vault/java-mcp#client_secret": "local-java-mcp-debug"}
        ),
    )
    proxy = CredentialProxy(InMemoryVault(settings.debug_vault_secrets))
    _seed_managed_connector_credentials(
        proxy, settings, mcp_adapters={f"mcp:{server.server_id}": adapter}
    )
    local_ref = proxy._references[("local", "vault/java-mcp#client_secret")]
    assert local_ref.account_scope == adapter.credential_scope
    assert local_ref.account_scope == "http://127.0.0.1:48080"
    assert ("development", "vault/java-mcp#client_secret") in proxy._references


def test_credential_proxy_accepts_mcp_invoke_for_remote_policy_decision() -> None:
    import httpx

    from auraclaw.contracts.internal import (
        CredentialInvokeRequest,
        PolicyValidateDecisionRequest,
    )
    from auraclaw.contracts.tools import CredentialReference
    from auraclaw.credential_proxy.internal_service import CredentialProxyInternalService
    from auraclaw.infrastructure.clients.policy import RemotePolicyClient
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
    from auraclaw.internal.http import create_contract_app
    from auraclaw.internal.routes import policy_routes

    async def scenario() -> None:
        policy_service = PolicyInternalService(version="s3-v1")
        policy_app = create_contract_app(
            "policy",
            policy_routes(policy_service),
            workload_identities={
                "hands-token": ServiceIdentity.ACTION_HANDS,
                "credential-token": ServiceIdentity.CREDENTIAL_PROXY,
            },
        )
        evaluated = await policy_service.evaluate(
            PolicyEvaluateRequest(
                context=InternalRequestContext(
                    tenant_id="local",
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id="req-mcp",
                    correlation_id="run-mcp",
                    causation_id="run-mcp",
                ),
                subject="action-hands-reconciler",
                action="mcp.remote.invoke",
                resource="mcp:java-mcp",
                input_digest="digest",
                attributes={"method": "initialize", "server_id": "java-mcp"},
            )
        )
        mismatched = await policy_service.validate_decision(
            PolicyValidateDecisionRequest(
                context=InternalRequestContext(
                    tenant_id="local",
                    service_identity=ServiceIdentity.CREDENTIAL_PROXY,
                    request_id="req-validate",
                    correlation_id=evaluated.decision_id,
                    causation_id=evaluated.decision_id,
                ),
                decision_id=evaluated.decision_id,
                action="mcp.invoke",
                resource="mcp:java-mcp",
            )
        )
        assert mismatched.valid is False

        async def adapter(request: dict[str, object], secret: str) -> dict[str, object]:
            del request, secret
            return {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}}

        proxy = CredentialProxy(InMemoryVault({"vault/java-mcp#client_secret": "secret"}))
        proxy.register_reference(
            "local",
            CredentialReference(
                credential_ref="vault/java-mcp#client_secret",
                provider="java-mcp",
                account_scope="http://127.0.0.1:48080",
                allowed_operations=("mcp.invoke",),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        )
        policy_validator = RemotePolicyClient(
            "http://policy.test",
            bearer_token="credential-token",
            service_identity=ServiceIdentity.CREDENTIAL_PROXY,
            transport=httpx.ASGITransport(app=policy_app),
        )
        service = CredentialProxyInternalService(
            proxy,
            adapters={"mcp:java-mcp": adapter},
            policy=policy_validator,
        )
        response = await service.invoke(
            CredentialInvokeRequest(
                context=InternalRequestContext(
                    tenant_id="local",
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id="req-invoke",
                    correlation_id="catalog:java-mcp",
                    causation_id=evaluated.decision_id,
                ),
                session_id="catalog:java-mcp",
                credential_ref="vault/java-mcp#client_secret",
                operation="mcp.invoke",
                target="mcp:java-mcp",
                method="mcp.invoke",
                policy_decision_id=evaluated.decision_id,
                request={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "server_id": "java-mcp",
                },
            )
        )
        assert response.status == "completed"
        await policy_validator.aclose()

    asyncio.run(scenario())
