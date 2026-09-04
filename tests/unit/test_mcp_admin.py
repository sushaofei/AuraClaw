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
                    status=CapabilityStatus.ACTIVE,
                    permission="write-with-approval",
                    risk_level="medium",
                    updated_at=datetime.now(UTC),
                    metadata={
                        "source": {
                            "inputSchema": {
                                "type": "object",
                                "properties": {"order_id": {"type": "string"}},
                                "required": ["order_id"],
                            },
                            "outputSchema": {"type": "object"},
                        }
                    },
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
                    status=CapabilityStatus.ACTIVE,
                    permission="read-only",
                    updated_at=datetime.now(UTC),
                    metadata={
                        "source": {
                            "name": "docs",
                            "uri": "order://docs",
                            "mime_type": "text/markdown",
                        }
                    },
                ),
                CapabilityDescriptor(
                    capability_id="cap-order-review",
                    kind=CapabilityKind.PROMPT,
                    server_id="local-order-mcp",
                    canonical_name="order.review",
                    version="1",
                    content_digest="sha256:order.review",
                    title="Review order",
                    tenant_id="tenant-a",
                    status=CapabilityStatus.ACTIVE,
                    permission="read-only",
                    updated_at=datetime.now(UTC),
                    metadata={
                        "source": {
                            "name": "order.review",
                            "arguments": [
                                {
                                    "name": "order_id",
                                    "description": "Order identifier",
                                    "required": True,
                                }
                            ],
                        }
                    },
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


def test_mcp_admin_lists_groupable_capabilities_with_schemas() -> None:
    registry, catalog = _seed()
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry, catalog=catalog))
    app.state.config_ready = True
    headers = {"X-Tenant-ID": "tenant-a", "X-Actor-ID": "admin-1"}
    with TestClient(app) as client:
        listed = client.get(
            "/v1/admin/mcp-servers/local-order-mcp/capabilities",
            headers=headers,
        )
        assert listed.status_code == 200, listed.text
        capabilities = listed.json()["capabilities"]
        assert [item["kind"] for item in capabilities] == [
            "tool",
            "resource",
            "prompt",
        ]
        tool = next(item for item in capabilities if item["kind"] == "tool")
        assert tool["input_schema"]["required"] == ["order_id"]
        assert tool["output_schema"] == {"type": "object"}
        assert tool["read_only"] is False
        assert tool["enabled"] is True
        resource = next(item for item in capabilities if item["kind"] == "resource")
        assert resource["uri"] == "order://docs"
        prompt = next(item for item in capabilities if item["kind"] == "prompt")
        assert prompt["arguments"][0]["name"] == "order_id"


def test_mcp_admin_invokes_capability_test_with_simulated_input() -> None:
    registry, catalog = _seed()

    class CapabilityTestOps:
        def __getattr__(self, name: str) -> object:
            return getattr(registry, name)

        async def test_capability(self, **kwargs: object) -> dict[str, object]:
            assert kwargs["tenant_id"] == "tenant-a"
            assert kwargs["actor_id"] == "admin-1"
            assert kwargs["dept_id"] == "dept-a"
            assert kwargs["server_id"] == "local-order-mcp"
            assert kwargs["capability_id"] == "cap-order-create"
            assert kwargs["input_payload"] == {"order_id": "order-42"}
            assert kwargs["expected_output"] == {"accepted": True}
            return {
                "status": "passed",
                "kind": "tool",
                "output": {"accepted": True, "order_id": "order-42"},
                "schema_valid": True,
                "expectation_matched": True,
                "duration_ms": 7,
                "error": None,
            }

    app = _task_app()
    app.include_router(
        create_mcp_admin_router(
            registry,
            lifecycle=CapabilityTestOps(),  # type: ignore[arg-type]
            catalog=catalog,
        )
    )
    app.state.config_ready = True
    headers = {
        "X-Tenant-ID": "tenant-a",
        "X-Actor-ID": "admin-1",
        "X-Dept-ID": "dept-a",
    }
    with TestClient(app) as client:
        tested = client.post(
            "/v1/admin/mcp-servers/local-order-mcp/capabilities/cap-order-create:test",
            headers=headers,
            json={
                "input": {"order_id": "order-42"},
                "expected_output": {"accepted": True},
            },
        )
    assert tested.status_code == 200, tested.text
    assert tested.json()["status"] == "passed"
    assert tested.json()["schema_valid"] is True
    assert tested.json()["expectation_matched"] is True


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


def test_mcp_admin_rejects_retired_tool_prefix_write_and_omits_it_on_read() -> None:
    registry, catalog = _seed()
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry, catalog=catalog))
    app.state.config_ready = True
    headers = {
        "X-Tenant-ID": "tenant-a", "X-Actor-ID": "admin-1",
        "Idempotency-Key": "create-retired-field", "X-Expected-Revision": "0",
    }
    with TestClient(app) as client:
        response = client.get("/v1/admin/mcp-servers/local-order-mcp", headers=headers)
        assert response.status_code == 200
        config = response.json()["latest_config"]
        assert "allowed_tool_prefixes" not in config
        assert "allowed_tool_prefixes" not in str(app.openapi())
        config["server_id"] = "new-server"
        config["allowed_tool_prefixes"] = ["old."]
        rejected = client.post("/v1/admin/mcp-servers", headers=headers, json=config)
        assert rejected.status_code == 422, rejected.text
        assert "allowed_tool_prefixes" in rejected.text
        headers["X-Expected-Revision"] = "1"
        rejected_update = client.put(
            "/v1/admin/mcp-servers/local-order-mcp", headers=headers, json=config
        )
        assert rejected_update.status_code == 422, rejected_update.text


def test_upstream_authorized_tenant_can_manage_shared_mcp_without_platform_identity() -> None:
    registry = McpServerRegistryService(
        InMemoryMcpServerRegistryStore(), allow_private_auth_none=True
    )
    app = _task_app()
    app.include_router(create_mcp_admin_router(registry))
    app.state.config_ready = True
    headers = {"X-Tenant-ID": "1", "X-Actor-ID": "1", "X-Dept-ID": "100"}
    config = {
        "server_id": "shared-mcp", "tenant_id": None, "title": "Shared MCP",
        "endpoint": "http://127.0.0.1:48080/mcp", "network_mode": "loopback",
        "auth_strategy": "none",
    }
    with TestClient(app) as client:
        created = client.post(
            "/v1/admin/mcp-servers", json=config,
            headers={**headers, "Idempotency-Key": "shared-create"},
        )
        assert created.status_code == 202, created.text
        updated = client.put(
            "/v1/admin/mcp-servers/shared-mcp",
            json={**config, "auth_strategy": "workload_trusted_context",
                  "credential_ref": "vault/shared-mcp#workload"},
            headers={**headers, "Idempotency-Key": "shared-update",
                     "X-Expected-Revision": "1"},
        )
        assert updated.status_code == 202, updated.text
        assert updated.json()["tenant_id"] == "1"
        assert updated.json()["actor_id"] == "1"
        conflict = client.put(
            "/v1/admin/mcp-servers/shared-mcp", json=config,
            headers={**headers, "Idempotency-Key": "shared-stale",
                     "X-Expected-Revision": "1"},
        )
        assert conflict.status_code == 409
        fetched = client.get("/v1/admin/mcp-servers/shared-mcp", headers=headers)
        assert fetched.status_code == 200
        assert fetched.json()["tenant_id"] is None
        assert fetched.json()["latest_revision"] == 2
