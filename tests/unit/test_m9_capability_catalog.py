from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from auraclaw.action.capability_catalog import (
    CAPABILITY_SEARCH_TOOL_NAME,
    CapabilityCatalog,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
    capability_search_tool,
)
from auraclaw.action.hands import HandsGateway
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.ports import ToolCall


class _ApprovalReader:
    async def get(self, tenant_id: str, approval_id: str) -> None:
        del tenant_id, approval_id
        return None

    async def find_approved(
        self,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
    ) -> None:
        del tenant_id, session_id, digest, policy_version
        return None


class _FailingHands:
    async def execute(self, invocation: Any, capability: Any) -> Any:
        raise AssertionError(f"unexpected default Hands route: {invocation}, {capability}")


def _assignment(tenant_id: str = "tenant-a") -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id=tenant_id,
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id=f"runtime-{tenant_id}",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def _descriptor(
    capability_id: str,
    canonical_name: str,
    *,
    tenant_id: str | None = None,
    status: CapabilityStatus = CapabilityStatus.ACTIVE,
    permission: str = "read-only",
) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capability_id=capability_id,
        kind=CapabilityKind.TOOL,
        server_id="server-global" if tenant_id is None else "server-tenant-a",
        canonical_name=canonical_name,
        version="1",
        content_digest=f"sha256:{capability_id}",
        title=canonical_name.replace(".", " "),
        description=f"Managed capability for {canonical_name}",
        tags=("github", "issue"),
        tenant_id=tenant_id,
        trust_level=(
            CapabilityTrustLevel.PLATFORM
            if tenant_id is None
            else CapabilityTrustLevel.TENANT_VERIFIED
        ),
        permission=permission,
        risk_level="low",
        status=status,
        updated_at=datetime.now(UTC),
    )


def test_catalog_filters_tenant_status_kind_permission_and_query() -> None:
    async def scenario() -> None:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-global",
                title="Platform",
                endpoint="https://platform.example/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-tenant-a",
                tenant_id="tenant-a",
                title="Tenant A",
                endpoint="https://tenant-a.example/mcp",
                trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "server-global",
            (
                _descriptor("cap-global", "github.issue.read"),
                _descriptor(
                    "cap-retired",
                    "github.issue.retired",
                    status=CapabilityStatus.RETIRED,
                ),
            ),
        )
        await catalog.replace_server_capabilities(
            "server-tenant-a",
            (
                _descriptor(
                    "cap-tenant-write",
                    "github.issue.create",
                    tenant_id="tenant-a",
                    permission="write-with-approval",
                ),
            ),
        )

        tenant_a = await catalog.search(
            tenant_id="tenant-a",
            query="github issue",
            kinds=(CapabilityKind.TOOL,),
            limit=10,
        )
        assert [item.capability_id for item in tenant_a] == [
            "cap-tenant-write",
            "cap-global",
        ]
        assert [
            item.capability_id
            for item in await catalog.search(
                tenant_id="tenant-a",
                required_permissions=("read-only",),
            )
        ] == ["cap-global"]
        assert [
            item.capability_id
            for item in await catalog.search(tenant_id="tenant-b")
        ] == ["cap-global"]
        global_server = await store.get_server("server-global")
        assert global_server is not None
        await catalog.register_server(
            global_server.model_copy(update={"enabled": False})
        )
        assert await catalog.search(tenant_id="tenant-b") == ()

    asyncio.run(scenario())


def test_capability_search_runs_through_tool_policy_and_trusted_tenant() -> None:
    async def scenario() -> None:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-global",
                title="Platform",
                endpoint="https://platform.example/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "server-global",
            (_descriptor("cap-global", "github.issue.read"),),
        )
        registry = ToolRegistry((capability_search_tool(),))
        gateway = ToolGateway(
            registry=registry,
            policy=PolicyEngine(version="m9-v1"),
            approvals=_ApprovalReader(),
            hands=RoutedHandsExecutor(
                _FailingHands(),
                {
                    CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(catalog)
                },
            ),
            artifacts=ArtifactStore(
                InMemoryObjectStorage(),
                signing_key=b"m9-capability-artifact-key-0001",
            ),
        )
        client = HandsRuntimeAdapter(
            InProcessHandsClient(
                HandsGateway(registry=registry, gateway=gateway)
            )
        )
        assignment = _assignment()
        result = await client.execute(
            assignment,
            ToolCall(
                tool_invocation_id="search-1",
                name=CAPABILITY_SEARCH_TOOL_NAME,
                arguments={"query": "github", "limit": 5},
            ),
        )
        assert result["status"] == "success"
        assert result["content"]["capabilities"][0]["capability_id"] == "cap-global"
        assert "tenant_id" not in result["content"]["capabilities"][0]

    asyncio.run(scenario())


def test_catalog_search_matches_chinese_query_without_year_token() -> None:
    async def scenario() -> None:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="java-mcp",
                tenant_id="1",
                title="Java MCP",
                endpoint="https://java-mcp.example.com/mcp",
                trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "java-mcp",
            (
                _descriptor(
                    "cap-price-profile",
                    "procurement.price.dataset.profile",
                    tenant_id="1",
                ).model_copy(
                    update={
                        "server_id": "java-mcp",
                        "tags": ("价格洞察", "price_insight"),
                        "description": "Profile a governed procurement price dataset.",
                    }
                ),
            ),
        )
        matches = await catalog.search(
            tenant_id="1",
            query="价格洞察 2024",
            kinds=(CapabilityKind.TOOL,),
            limit=10,
        )
        assert [item.capability_id for item in matches] == ["cap-price-profile"]

    asyncio.run(scenario())


def test_catalog_lists_tools_without_enabled_filter() -> None:
    async def scenario() -> None:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-tenant-a",
                tenant_id="tenant-a",
                title="Tenant A",
                endpoint="https://tenant-a.example/mcp",
                trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                status=CapabilityStatus.QUARANTINED,
                enabled=False,
            )
        )
        await catalog.replace_server_capabilities(
            "server-tenant-a",
            (
                _descriptor("cap-tool", "order.create", tenant_id="tenant-a"),
                _descriptor(
                    "cap-resource",
                    "order.docs",
                    tenant_id="tenant-a",
                ).model_copy(update={"kind": CapabilityKind.RESOURCE}),
            ),
        )
        assert await catalog.search(tenant_id="tenant-a") == ()
        tools = await catalog.list_server_tools(
            tenant_id="tenant-a", server_id="server-tenant-a"
        )
        assert [item.canonical_name for item in tools] == ["order.create"]
        assert (
            await catalog.list_server_tools(
                tenant_id="other", server_id="server-tenant-a"
            )
            == ()
        )

    asyncio.run(scenario())
