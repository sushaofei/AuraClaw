from __future__ import annotations

import asyncio
from datetime import UTC, datetime

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
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.mcp_registry import McpServerConfig, McpServerWriteCommand


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
    app = create_app(profile="task-api")
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


def test_task_api_exposes_mcp_tools_route() -> None:
    paths = set(create_service_app("api").openapi()["paths"])
    assert "/v1/admin/mcp-servers/{server_id}/tools" in paths
