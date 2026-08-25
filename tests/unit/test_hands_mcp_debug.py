from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
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


def _settings(**values: object) -> Settings:
    defaults: dict[str, object] = {
        "storage_backend": "memory",
        "artifact_backend": "local",
    }
    defaults.update(values)
    return Settings(_env_file=None, **defaults)


def test_development_hands_starts_catalog_reconciler() -> None:
    settings = _settings(
        deployment_profile="development",
        policy_base_url="http://127.0.0.1:9",
        credential_proxy_base_url="http://127.0.0.1:9",
        mcp_reconcile_interval_seconds=3600,
    )
    app = create_service_app("hands", settings)
    with TestClient(app):
        assert getattr(app.state, "catalog_reconciler", None) is not None
        assert getattr(app.state, "mcp_connection_manager", None) is not None
        assert app.state.capability_connectors == {}


def test_hands_retries_transient_mcp_restore_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from auraclaw.action.mcp_connection_manager import McpConnectionManager

    attempts = 0

    async def flaky_restore(_manager: McpConnectionManager) -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("dependency is starting")
        return 0

    async def no_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(McpConnectionManager, "restore", flaky_restore)
    monkeypatch.setattr("auraclaw.composition.services.asyncio.sleep", no_delay)
    app = create_service_app(
        "hands",
        _settings(
            deployment_profile="development",
            runtime_workload_token="runtime-token",
            action_hands_workload_token="hands-token",
            lease_signing_key="test-hands-lease-key-with-32-bytes",
        ),
    )

    with TestClient(app) as client:
        assert client.get("/health/live").status_code == 200

    assert attempts == 2


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
    from auraclaw.contracts.capabilities import (
        CapabilityTrustLevel,
        McpAuthStrategy,
        McpNetworkMode,
    )
    from auraclaw.contracts.mcp_registry import (
        McpActiveSnapshotEntry,
        McpDesiredState,
        McpObservedState,
        McpServerConfig,
    )
    from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

    async def scenario() -> None:
        config = McpServerConfig(
            server_id="java-mcp",
            tenant_id="local",
            title="Java Agent Runtime MCP Gateway",
            endpoint="http://127.0.0.1:48080/rpc-api/agent-runtime/mcp",
            protocol_revision="2025-06-18",
            network_mode=McpNetworkMode.LOOPBACK,
            auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
            credential_ref="vault/java-mcp#client_secret",
            trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
            allowed_tool_prefixes=("",),
        )
        proxy = CredentialProxy(
            InMemoryVault({"vault/java-mcp#client_secret": "local-java-mcp-debug"})
        )
        adapters: dict[str, object] = {}
        manager = McpEgressManager(adapters=adapters, proxy=proxy)
        await manager.apply(
            McpActiveSnapshotEntry(
                server_id="java-mcp",
                tenant_id="local",
                revision=1,
                config=config,
                desired_state=McpDesiredState.ENABLED,
                observed_state=McpObservedState.ACTIVE,
            )
        )
        adapter = adapters["mcp:java-mcp"]
        local_ref = proxy._references[("local", "vault/java-mcp#client_secret")]
        assert local_ref.account_scope == adapter.credential_scope
        assert local_ref.account_scope == "http://127.0.0.1:48080"
        assert ("development", "vault/java-mcp#client_secret") in proxy._references

    asyncio.run(scenario())


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
