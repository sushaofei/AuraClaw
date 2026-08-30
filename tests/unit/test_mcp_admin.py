from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_registry import (
    InMemoryMcpServerRegistryStore,
    McpServerRegistryService,
)
from auraclaw.api.routes.admin_mcp import create_mcp_admin_router
from auraclaw.composition.api import create_app
from auraclaw.composition.services import create_service_app
from auraclaw.config import Settings
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.mcp_registry import McpServerConfig, McpServerWriteCommand


class _NoOpObservability:
    async def record_span(self, **kwargs: object) -> None:
        del kwargs

    async def metric(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


def _task_app() -> FastAPI:
    app = create_app(profile="task-api")
    app.state.observability_service = _NoOpObservability()
    return app


def _seed() -> tuple[McpServerRegistryService, CapabilityCatalog]:
    registry = McpServerRegistryService(
        InMemoryMcpServerRegistryStore(), allow_private_auth_none=True
    )
    catalog = CapabilityCatalog(InMemoryCapabilityCatalogStore())

    async def scenario() -> None:
        await registry.create(
            McpServerWriteCommand(
                command_id="cmd-create",
                tenant_id="tenant-a",
                actor_id="admin-1",
                correlation_id="corr-1",
                causation_id="cause-1",
                expected_revision=0,
                config=McpServerConfig.model_validate(
                    {
                        "server_id": "local-order-mcp",
                        "tenant_id": "tenant-a",
                        "title": "Local Order MCP",
                        "endpoint": "http://127.0.0.1:48080/mcp",
                        "network_mode": "loopback",
                        "auth_strategy": "none",
                        "allowed_tool_prefixes": ("order.",),
                        "trust_level": "tenant_verified",
                    }
                ),
            )
        )
        await catalog.register_server(
            McpServerDefinition(
                server_id="local-order-mcp",
                tenant_id="tenant-a",
                title="Local Order MCP",
                endpoint="https://order.example/mcp",
                trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                status=CapabilityStatus.QUARANTINED,
                enabled=False,
            )
        )
        await catalog.replace_server_capabilities(
            "local-order-mcp",
            (
                CapabilityDescriptor(
                    capability_id="cap-order-create",
                    kind=CapabilityKind.TOOL,
                    server_id="local-order-mcp",
                    canonical_name="order.create",
                    version="1",
                    content_digest="sha256:order.create",
                    title="Create order",
                    description="Create a governed order",
                    tenant_id="tenant-a",
                    trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                    status=CapabilityStatus.ACTIVE,
                    updated_at=datetime.now(UTC),
                ),
                CapabilityDescriptor(
                    capability_id="cap-order-docs",
                    kind=CapabilityKind.RESOURCE,
                    server_id="local-order-mcp",
                    canonical_name="order.docs",
                    version="1",
                    content_digest="sha256:order.docs",
                    title="Order docs",
                    tenant_id="tenant-a",
                    trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                    status=CapabilityStatus.ACTIVE,
                    updated_at=datetime.now(UTC),
                ),
            ),
        )

    asyncio.run(scenario())
    return registry, catalog


def test_mcp_admin_lists_catalogued_tools() -> None:
    registry, catalog = _seed()
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry, catalog=catalog))
    app.state.config_ready = True
    headers = {"X-Tenant-ID": "tenant-a", "X-Actor-ID": "admin-1"}
    with TestClient(app) as client:
        missing = client.get("/v1/admin/mcp-servers/missing/tools", headers=headers)
        assert missing.status_code == 404

        listed = client.get(
            "/v1/admin/mcp-servers/local-order-mcp/tools", headers=headers
        )
        assert listed.status_code == 200, listed.text
        payload = listed.json()
        assert payload["server_id"] == "local-order-mcp"
        assert [item["canonical_name"] for item in payload["tools"]] == ["order.create"]
        assert payload["tools"][0]["title"] == "Create order"

        outsider = client.get(
            "/v1/admin/mcp-servers/local-order-mcp/tools",
            headers={"X-Tenant-ID": "other", "X-Actor-ID": "admin-2"},
        )
        assert outsider.status_code == 403


def test_mcp_admin_hard_deletes_server_via_gateway_alias() -> None:
    registry, catalog = _seed()
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry, catalog=catalog))
    app.state.config_ready = True
    headers = {
        "X-Tenant-ID": "tenant-a",
        "X-Actor-ID": "admin-1",
        "Idempotency-Key": "cmd-delete-alias",
        "X-Expected-Revision": "1",
    }
    with TestClient(app) as client:
        deleted = client.post(
            "/v1/admin/mcp-servers/lifecycle",
            headers=headers,
            json={
                "server_id": "local-order-mcp",
                "operation": "drop",
            },
        )
        assert deleted.status_code == 202, deleted.text
        payload = deleted.json()
        assert payload["operation"] == "delete"
        assert payload["status"] == "succeeded"
        assert payload["result"]["deleted"] is True

        missing = client.get("/v1/admin/mcp-servers/local-order-mcp", headers=headers)
        assert missing.status_code == 404


def test_mcp_admin_hard_deletes_server() -> None:
    registry, catalog = _seed()
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry, catalog=catalog))
    app.state.config_ready = True
    headers = {
        "X-Tenant-ID": "tenant-a",
        "X-Actor-ID": "admin-1",
        "Idempotency-Key": "cmd-delete",
        "X-Expected-Revision": "1",
    }
    with TestClient(app) as client:
        deleted = client.put(
            "/v1/admin/mcp-servers/local-order-mcp",
            headers=headers,
            json={
                "server_id": "local-order-mcp",
                "metadata": {"mcp_action": "delete"},
            },
        )
        assert deleted.status_code == 202, deleted.text
        payload = deleted.json()
        assert payload["operation"] == "delete"
        assert payload["status"] == "succeeded"
        assert payload["result"]["deleted"] is True

        missing = client.get("/v1/admin/mcp-servers/local-order-mcp", headers=headers)
        assert missing.status_code == 404


def test_task_api_exposes_mcp_tools_route() -> None:
    settings = Settings(
        _env_file=None,
        storage_backend="postgres",
        db_host="localhost",
        db_user="auraclaw",
        db_password="auraclaw",
        db_name="auraclaw",
        allow_insecure_identity_headers=True,
        task_api_workload_token="task-api-token",
    )
    paths = set(create_service_app("api", settings).openapi()["paths"])
    assert "/v1/admin/mcp-servers/{server_id}/tools" in paths
