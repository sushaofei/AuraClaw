from __future__ import annotations

import asyncio
from typing import Any

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
from auraclaw.action.price_insight import (
    PRICE_INSIGHT_SNAPSHOT_TOOL,
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
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.skills import ResolvedSkillTool, SkillBinding
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.infrastructure.artifacts.store import (
    ArtifactStore,
    InMemoryObjectStorage,
)
from auraclaw.infrastructure.price_insight import JsonPriceInsightSource
from auraclaw.internal.mcp import InProcessMcpTransport
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.mcp_client import HandsMcpClient
from auraclaw.runtime.ports import ToolCall


class _NoApprovals:
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


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="development",
        root_session_id="root-price",
        session_id="session-price",
        run_id="run-price",
        runtime_id="runtime-price",
        lease_id="lease-price",
        fencing_token=1,
        role="root",
        resource_profile={},
        budget=RuntimeBudget(),
    )


def test_price_insight_skill_runs_search_load_activate_and_tool_flow() -> None:
    async def scenario() -> None:
        tenant_id = "development"
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id=PRICE_INSIGHT_SERVER_ID,
                tenant_id=tenant_id,
                title="Price Insight",
                endpoint="https://price-insight.internal/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
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
        resources = McpResourceRegistry(price_insight_resources(tenant_id))
        artifacts = ArtifactStore(
            InMemoryObjectStorage(),
            signing_key=b"m12-price-artifact-key",
        )
        signer = HmacSkillSignatureVerifier(
            {"platform": b"m12-platform-skill-signing-key"}
        )
        skills = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=signer,
            resources=resources,
        )
        await skills.publish(tenant_id, signed_price_insight_package(signer))
        resolver = SkillResolver(skills, store)
        price_executor = PriceInsightToolExecutor(
            PriceInsightService(
                JsonPriceInsightSource(
                    PRICE_INSIGHT_SKILL_DIR / "tests" / "golden-data.json"
                )
            )
        )
        tool_registry = ToolRegistry(
            (
                capability_search_tool(),
                capability_load_tool(),
                skill_resolve_tool(),
                *price_insight_tools(),
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
            registry=tool_registry,
            policy=PolicyEngine(),
            approvals=_NoApprovals(),
            hands=routed,
            artifacts=artifacts,
        )
        controller = RuntimeCapabilityController(
            HandsMcpClient(
                InProcessMcpTransport(
                    HandsMcpServer(
                        registry=tool_registry,
                        gateway=gateway,
                        resources=resources,
                    )
                )
            )
        )
        searched = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="search-price-skill",
                name="auraclaw.capabilities.search",
                arguments={"query": "采购 价格洞察", "kinds": ["skill"]},
            ),
            controller.empty_state(),
        )
        skill_id = next(iter(searched.state["candidates"]))
        loaded = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="load-price-skill",
                name="auraclaw.capabilities.load",
                arguments={"capability_ids": [skill_id]},
            ),
            searched.state,
        )
        activated = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="activate-price-skill",
                name="auraclaw.skills.activate",
                arguments={"capability_id": skill_id, "inputs": {}},
            ),
            loaded.state,
        )

        assert activated.result["status"] == "activated"
        assert len(activated.result["loaded_dependency_ids"]) == 6
        assert len(activated.state["loaded"]) == 7
        assert PRICE_INSIGHT_SNAPSHOT_TOOL in {
            tool["function"]["name"]
            for tool in controller.model_tools(activated.state)
        }

        snapshot = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="price-snapshot",
                name=PRICE_INSIGHT_SNAPSHOT_TOOL,
                arguments={
                    "filter": {
                        "period_from": "2026-01",
                        "period_to": "2026-02",
                        "anchor": "market",
                    }
                },
            ),
            activated.state,
        )
        assert snapshot.result["status"] == "success"
        result = snapshot.result["content"]
        assert len(result["kpis"]) == 8
        assert result["data_quality"]["status"] == "pass"
        assert result["analytics"]["price_impact"]["anchors"]["market"][
            "total_pos_amount"
        ] == 40000
        assert result["analytics"]["price_impact"]["anchors"]["market"][
            "total_neg_amount"
        ] == 40000

    asyncio.run(scenario())


class _SecondScenarioClient:
    async def execute(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
    ) -> dict[str, Any]:
        del assignment
        if call.name == "auraclaw.capabilities.load":
            return {
                "capabilities": [
                    {
                        "capability_id": "cap-inventory-tool",
                        "kind": "tool",
                        "canonical_name": "inventory.health.snapshot",
                        "version": "1.0.0",
                        "permission": "read-only",
                        "model_tool": {
                            "type": "function",
                            "function": {
                                "name": "inventory.health.snapshot",
                                "description": "Inventory health",
                                "parameters": {"type": "object"},
                            },
                        },
                    }
                ]
            }
        raise AssertionError(call.name)

    async def resolve_skill(
        self,
        assignment: RuntimeAssignment,
        *,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding:
        del assignment, active_skill_names
        return SkillBinding(
            skill_name=name,
            skill_version=version,
            publisher=publisher or "platform",
            package_digest=f"sha256:{'a' * 64}",
            artifact_ref=ArtifactRef(
                artifact_id="inventory-skill",
                version=1,
                content_hash=f"sha256:{'b' * 64}",
                media_type="application/json",
                size=1,
            ),
            resolved_tools=(
                ResolvedSkillTool(
                    capability_id="cap-inventory-tool",
                    canonical_name="inventory.health.snapshot",
                    version="1.0.0",
                    schema_digest=f"sha256:{'c' * 64}",
                ),
            ),
            policy_version="policy-1",
            max_steps=10,
            timeout_seconds=60,
        )

    async def load_skill_part(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        return [{"text": "Inspect inventory health."}]

    async def read_resource(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        return []

    async def list_tools(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        return []

    async def list_resources(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        return []

    async def list_resource_templates(
        self, *args: Any, **kwargs: Any
    ) -> list[dict[str, Any]]:
        del args, kwargs
        return []

    async def list_prompts(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]:
        del assignment
        return []

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        del assignment, name, arguments
        return {}

    async def load_skill_manifest(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        del assignment, publisher, name, version
        return {}


def test_unrelated_skill_auto_loads_dependencies_without_runtime_branch() -> None:
    async def scenario() -> None:
        controller = RuntimeCapabilityController(_SecondScenarioClient())
        state = controller.empty_state()
        state["loaded"] = {
            "cap-inventory-skill": {
                "capability_id": "cap-inventory-skill",
                "kind": "skill",
                "skill": {
                    "publisher": "platform",
                    "name": "inventory.health.summarize",
                    "version": "1.0.0",
                    "input_schema": {"type": "object"},
                },
            }
        }
        activated = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="activate-inventory",
                name="auraclaw.skills.activate",
                arguments={"capability_id": "cap-inventory-skill", "inputs": {}},
            ),
            state,
        )

        assert activated.result["status"] == "activated"
        assert "cap-inventory-tool" in activated.state["loaded"]
        assert "inventory.health.snapshot" in {
            tool["function"]["name"]
            for tool in controller.model_tools(activated.state)
        }

    asyncio.run(scenario())
