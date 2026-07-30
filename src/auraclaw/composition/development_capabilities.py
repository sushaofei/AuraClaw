from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from auraclaw.action.capability_catalog import (
    CAPABILITY_LOAD_TOOL_NAME,
    CAPABILITY_SEARCH_TOOL_NAME,
    SKILL_RESOLVE_TOOL_NAME,
    CapabilityCatalog,
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
    SkillResolveExecutor,
    capability_load_tool,
    capability_search_tool,
    skill_resolve_tool,
)
from auraclaw.action.mcp import HandsMcpServer
from auraclaw.action.mcp_primitives import McpResourceRegistry
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.ports import PriceInsightSource
from auraclaw.action.price_insight import (
    PriceInsightService,
    PriceInsightToolExecutor,
    price_insight_tool_descriptors,
    price_insight_tools,
)
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
    SkillResolver,
)
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.composition.business_skills import (
    PRICE_INSIGHT_SERVER_ID,
    PRICE_INSIGHT_SKILL_DIR,
    price_insight_resource_descriptors,
    price_insight_resources,
    signed_price_insight_package,
)
from auraclaw.config import Settings
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.mcp import (
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
from auraclaw.infrastructure.artifacts.store import (
    ArtifactStore,
    InMemoryObjectStorage,
)
from auraclaw.infrastructure.price_insight import (
    JsonPriceInsightSource,
    MySqlPriceInsightSource,
)
from auraclaw.runtime.mcp_client import HandsMcpClient


class _EmptyApprovalReader:
    async def get(self, tenant_id: str, approval_id: str) -> None:
        del tenant_id, approval_id

    async def find_approved(
        self,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
    ) -> None:
        del tenant_id, session_id, digest, policy_version


class _InitializedInProcessTransport:
    """Lazily publishes development capabilities before the first MCP request."""

    def __init__(
        self,
        server: HandsMcpServer,
        initialize: Callable[[], Awaitable[None]],
    ) -> None:
        self._server = server
        self._initialize = initialize
        self._initialized = False
        self._lock = asyncio.Lock()

    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    await self._initialize()
                    self._initialized = True
        return await self._server.handle(request, trusted_context=trusted_context)


def build_development_capability_client(
    settings: Settings,
) -> HandsMcpClient | None:
    """Build the governed in-process capability plane for the combined dev server."""
    source_kind = settings.resolved_price_insight_source
    if source_kind == "disabled":
        return None
    source: PriceInsightSource
    if source_kind == "fixture":
        source = JsonPriceInsightSource(
            PRICE_INSIGHT_SKILL_DIR / "tests" / "golden-data.json"
        )
    elif source_kind == "mysql":
        password = settings.price_insight_mysql_password
        if (
            not settings.price_insight_mysql_configured
            or settings.price_insight_mysql_host is None
            or settings.price_insight_mysql_user is None
            or password is None
            or settings.price_insight_mysql_database is None
        ):
            raise ValueError("Price Insight MySQL source configuration is incomplete")
        source = MySqlPriceInsightSource(
            host=settings.price_insight_mysql_host,
            port=settings.price_insight_mysql_port,
            user=settings.price_insight_mysql_user,
            password=password.get_secret_value(),
            database=settings.price_insight_mysql_database,
        )
    else:
        raise ValueError(f"Unsupported development Price Insight source: {source_kind}")

    tenant_id = settings.price_insight_target_tenant_id
    catalog_store = InMemoryCapabilityCatalogStore()
    catalog = CapabilityCatalog(catalog_store)
    resources = McpResourceRegistry(price_insight_resources(tenant_id))
    artifacts = ArtifactStore(
        InMemoryObjectStorage(),
        signing_key=b"auraclaw-development-price-insight-artifact-key",
    )
    signer = HmacSkillSignatureVerifier(
        {"platform": b"auraclaw-development-platform-skill-key"}
    )
    skills = SkillPackageRegistry(
        artifacts=artifacts,
        signature_verifier=signer,
        resources=resources,
    )
    resolver = SkillResolver(skills, catalog_store)
    price_executor = PriceInsightToolExecutor(PriceInsightService(source))
    tools = price_insight_tools()
    registry = ToolRegistry(
        (
            capability_search_tool(),
            capability_load_tool(),
            skill_resolve_tool(),
            *tools,
        )
    )
    routed = RoutedHandsExecutor(
        price_executor,
        {
            CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(
                catalog,
                skills=skills,
            ),
            CAPABILITY_LOAD_TOOL_NAME: CapabilityLoadExecutor(
                catalog,
                skills=skills,
            ),
            SKILL_RESOLVE_TOOL_NAME: SkillResolveExecutor(resolver),
        },
    )
    gateway = ToolGateway(
        registry=registry,
        policy=PolicyEngine(),
        approvals=_EmptyApprovalReader(),
        hands=routed,
        artifacts=artifacts,
    )

    async def initialize() -> None:
        await catalog.register_server(
            McpServerDefinition(
                server_id=PRICE_INSIGHT_SERVER_ID,
                tenant_id=tenant_id,
                title="AuraClaw Procurement Price Insight",
                endpoint="https://price-insight.internal/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
                allowed_tool_prefixes=("procurement.price_insight.",),
                allowed_resource_schemes=("repo",),
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            PRICE_INSIGHT_SERVER_ID,
            (
                *price_insight_tool_descriptors(
                    server_id=PRICE_INSIGHT_SERVER_ID,
                    tenant_id=tenant_id,
                ),
                *price_insight_resource_descriptors(tenant_id),
            ),
        )
        await skills.publish(tenant_id, signed_price_insight_package(signer))

    server = HandsMcpServer(
        registry=registry,
        gateway=gateway,
        resources=resources,
    )
    return HandsMcpClient(_InitializedInProcessTransport(server, initialize))
