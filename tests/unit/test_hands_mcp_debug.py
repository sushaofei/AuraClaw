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
        "model_skill_mysql_host": None,
        "model_skill_mysql_user": None,
        "model_skill_mysql_password": None,
        "model_skill_mysql_database": None,
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
