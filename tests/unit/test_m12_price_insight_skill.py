from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
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
    PRICE_DATASET_PROFILE_TOOL,
    PRICE_DATASET_QUALITY_CHECK_TOOL,
    PRICE_INSIGHT_METRIC_KEYS,
    PRICE_METRIC_EVIDENCE_LIST_TOOL,
    PRICE_METRIC_TOOLS,
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
    signed_price_insight_dependency_packages,
    signed_price_insight_package,
)
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.price_insight import (
    PriceInsightDataset,
    PriceInsightFilter,
)
from auraclaw.contracts.skills import ResolvedSkillTool, SkillBinding
from auraclaw.contracts.tools import (
    ArtifactRef,
    PolicyDecision,
    ToolInvocation,
    ToolResultStatus,
)
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.infrastructure.artifacts.store import (
    ArtifactStore,
    InMemoryObjectStorage,
)
from auraclaw.infrastructure.price_insight import (
    PRICE_INSIGHT_DWD_TABLES,
    JsonPriceInsightSource,
    _select_benchmark_statement,
    _select_rule_statement,
    _select_statement,
)
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


class _CountingPriceSource:
    def __init__(self) -> None:
        self.calls = 0

    async def load_dataset(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> PriceInsightDataset:
        del filters
        self.calls += 1
        return PriceInsightDataset(
            tenant_id=tenant_id,
            source_revision="replay-safe-v1",
        )


class _DenyPolicy:
    version = "price-deny-v1"

    def evaluate(
        self,
        capability: Any,
        invocation: ToolInvocation | None = None,
    ) -> PolicyDecision:
        del capability, invocation
        return PolicyDecision.DENY


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


def test_price_insight_sql_is_restricted_to_governed_dwd_tables() -> None:
    assert PRICE_INSIGHT_DWD_TABLES == {
        "dwd_pr_price_event_detail_di",
        "dwd_pr_industry_price_benchmark_di",
        "dwd_pr_price_compare_pair_di",
        "dwd_pr_price_insight_rule_di",
    }

    try:
        _select_statement(
            table="unrelated_business_table",
            columns=("id",),
            tenant_id="development",
            filters=PriceInsightFilter(
                period_from="2026-01",
                period_to="2026-01",
            ),
            include_benchmark=False,
        )
    except ValueError as exc:
        assert "outside the Price Insight DWD allowlist" in str(exc)
    else:
        raise AssertionError("an unrelated table must not be queryable")


def test_price_insight_data_interface_covers_benchmark_and_rule_tables() -> None:
    filters = PriceInsightFilter(
        period_from="2026-01",
        period_to="2026-02",
        benchmark_version="benchmark-v1",
        rule_version="rule-v1",
    )
    benchmark_sql, benchmark_args = _select_benchmark_statement(
        tenant_id="development",
        filters=filters,
    )
    rule_sql, rule_args = _select_rule_statement(
        tenant_id="development",
        filters=filters,
    )

    assert "`dwd_pr_industry_price_benchmark_di`" in benchmark_sql
    assert benchmark_args == (
        "development",
        "2026-01",
        "2026-02",
        "benchmark-v1",
    )
    assert "`dwd_pr_price_insight_rule_di`" in rule_sql
    assert rule_args == ("development", "rule-v1")


def test_dwd_rule_drives_default_threshold_and_request_can_override(tmp_path: Any) -> None:
    async def scenario() -> None:
        payload = json.loads(
            (
                PRICE_INSIGHT_SKILL_DIR / "tests" / "golden-data.json"
            ).read_text()
        )
        payload["rules"] = [
            {
                "rule_version": "rule-v1",
                "rule_code": "MARKET_DEFAULT",
                "anchor_type": "MARKET",
                "deviation_threshold_pct": "12",
                "min_benchmark_sample_count": 30,
                "min_material_match_score": "0.9",
                "enabled": True,
            }
        ]
        fixture = tmp_path / "price-rule.json"
        fixture.write_text(json.dumps(payload))
        service = PriceInsightService(JsonPriceInsightSource(fixture))

        governed = await service.metric(
            tenant_id="development",
            filters=PriceInsightFilter(
                period_from="2026-01",
                period_to="2026-02",
                anchor="market",
            ),
            metric_key="deviation_cnt",
        )
        assert governed["metric"]["value"] == 0
        assert governed["context"]["threshold_pct"] == 12
        assert (
            governed["context"]["effective_rule"]["threshold_source"]
            == "dwd_rule"
        )

        overridden = await service.metric(
            tenant_id="development",
            filters=PriceInsightFilter(
                period_from="2026-01",
                period_to="2026-02",
                anchor="market",
                deviation_threshold_pct=8,
            ),
            metric_key="deviation_cnt",
        )
        assert overridden["metric"]["value"] == 2
        assert (
            overridden["context"]["effective_rule"]["threshold_source"]
            == "request"
        )

    asyncio.run(scenario())


def test_dwd_rule_quality_minimums_exclude_weak_market_matches(
    tmp_path: Any,
) -> None:
    async def scenario() -> None:
        payload = json.loads(
            (
                PRICE_INSIGHT_SKILL_DIR / "tests" / "golden-data.json"
            ).read_text()
        )
        payload["rules"] = [
            {
                "rule_version": "rule-v1",
                "rule_code": "MARKET_STRICT",
                "anchor_type": "MARKET",
                "deviation_threshold_pct": "8",
                "min_benchmark_sample_count": 50,
                "min_material_match_score": "0.9",
                "enabled": True,
            }
        ]
        fixture = tmp_path / "price-strict-rule.json"
        fixture.write_text(json.dumps(payload))
        service = PriceInsightService(JsonPriceInsightSource(fixture))
        filters = PriceInsightFilter(
            period_from="2026-01",
            period_to="2026-02",
            anchor="market",
        )

        quality = await service.data_quality(
            tenant_id="development",
            filters=filters,
        )
        assert quality["status"] == "warning"
        assert {
            finding["code"] for finding in quality["findings"]
        } >= {"BENCHMARK_SAMPLE_BELOW_RULE_MINIMUM"}
        metric = await service.metric(
            tenant_id="development",
            filters=filters,
            metric_key="market_dev_pct",
        )
        assert metric["context"]["matched_line_count"] == 0

    asyncio.run(scenario())


def test_simulated_market_benchmark_blocks_authoritative_insight(
    tmp_path: Any,
) -> None:
    async def scenario() -> None:
        payload = json.loads(
            (
                PRICE_INSIGHT_SKILL_DIR / "tests" / "golden-data.json"
            ).read_text()
        )
        for event in payload["events"]:
            event["tax_basis_code"] = "UNKNOWN"
            event["data_quality_status"] = (
                "FINAL_TRANSACTION_STATUS_UNCONFIRMED;TAX_BASIS_UNKNOWN"
            )
        for comparison in payload["comparisons"]:
            comparison["tax_basis_code"] = "UNKNOWN"
            comparison["data_quality_status"] = (
                "FINAL_TRANSACTION_STATUS_UNCONFIRMED;"
                "TAX_BASIS_UNKNOWN;"
                "INDUSTRY_BENCHMARK_SIMULATED"
            )
        fixture = tmp_path / "simulated-market.json"
        fixture.write_text(json.dumps(payload))
        service = PriceInsightService(JsonPriceInsightSource(fixture))

        quality = await service.data_quality(
            tenant_id="development",
            filters=PriceInsightFilter(
                period_from="2026-01",
                period_to="2026-02",
                anchor="market",
            ),
        )

        assert quality["status"] == "blocked"
        assert {
            finding["code"] for finding in quality["findings"]
        } >= {
            "FINAL_TRANSACTION_STATUS_UNCONFIRMED",
            "TAX_BASIS_UNKNOWN",
            "SIMULATED_INDUSTRY_BENCHMARK",
        }

    asyncio.run(scenario())


def test_price_tool_replay_returns_cached_result_without_rereading_dwd() -> None:
    async def scenario() -> None:
        source = _CountingPriceSource()
        gateway = ToolGateway(
            registry=ToolRegistry(price_insight_tools()),
            policy=PolicyEngine(),
            approvals=_NoApprovals(),
            hands=PriceInsightToolExecutor(PriceInsightService(source)),
            artifacts=ArtifactStore(
                InMemoryObjectStorage(),
                signing_key=b"price-replay-artifact-key",
            ),
        )
        invocation = ToolInvocation(
            tool_invocation_id="price-profile-replay",
            tenant_id="development",
            root_session_id="root-price",
            session_id="session-price",
            run_id="run-price",
            tool_name=PRICE_DATASET_PROFILE_TOOL,
            tool_version="1.0.0",
            arguments={
                "filter": {
                    "period_from": "2026-01",
                    "period_to": "2026-02",
                }
            },
            expected_side_effect="read",
            idempotency_key="price-profile-replay-key",
            deadline=datetime.now(UTC) + timedelta(minutes=1),
            fencing_token=1,
            actor_id="runtime-price",
        )

        first = await gateway.execute(invocation)
        replay = await gateway.execute(invocation)

        assert first.status is ToolResultStatus.SUCCESS
        assert replay == first
        assert source.calls == 1

    asyncio.run(scenario())


def test_price_tool_policy_deny_happens_before_dwd_read() -> None:
    async def scenario() -> None:
        source = _CountingPriceSource()
        gateway = ToolGateway(
            registry=ToolRegistry(price_insight_tools()),
            policy=_DenyPolicy(),
            approvals=_NoApprovals(),
            hands=PriceInsightToolExecutor(PriceInsightService(source)),
            artifacts=ArtifactStore(
                InMemoryObjectStorage(),
                signing_key=b"price-deny-artifact-key",
            ),
        )
        denied = await gateway.execute(
            ToolInvocation(
                tool_invocation_id="price-profile-denied",
                tenant_id="development",
                root_session_id="root-price",
                session_id="session-price",
                run_id="run-price",
                tool_name=PRICE_DATASET_PROFILE_TOOL,
                tool_version="1.0.0",
                arguments={
                    "filter": {
                        "period_from": "2026-01",
                        "period_to": "2026-02",
                    }
                },
                expected_side_effect="read",
                idempotency_key="price-profile-denied-key",
                deadline=datetime.now(UTC) + timedelta(minutes=1),
                fencing_token=1,
                actor_id="runtime-price",
            )
        )

        assert denied.status is ToolResultStatus.DENIED
        assert denied.error_code == "policy_denied"
        assert source.calls == 0

    asyncio.run(scenario())


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
        for package in signed_price_insight_dependency_packages(signer):
            await skills.publish(tenant_id, package)
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
        skill_id = next(
            capability_id
            for capability_id, candidate in searched.state["candidates"].items()
            if candidate["canonical_name"] == "procurement.price-insight.generate"
        )
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
        assert len(activated.result["loaded_dependency_ids"]) == 16
        assert len(activated.state["loaded"]) == 17
        assert PRICE_METRIC_TOOLS["history_dev_pct"] in {
            tool["function"]["name"]
            for tool in controller.model_tools(activated.state)
        }
        trusted = await controller.trusted_messages(
            _assignment(),
            activated.state,
        )
        assert len(trusted) == 3
        assert "procurement.price-data.validate" in trusted[0]["content"]
        assert "procurement.price-metrics.analyze" in trusted[1]["content"]
        assert "procurement.price-insight.generate" in trusted[2]["content"]

        profile = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="price-scope-profile",
                name=PRICE_DATASET_PROFILE_TOOL,
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
        assert profile.result["status"] == "success"
        revision = profile.result["content"]["source_revision"]
        assert profile.result["content"]["tables_read"] == [
            "dwd_pr_price_event_detail_di",
            "dwd_pr_industry_price_benchmark_di",
            "dwd_pr_price_compare_pair_di",
            "dwd_pr_price_insight_rule_di",
        ]

        quality = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="price-quality-check",
                name=PRICE_DATASET_QUALITY_CHECK_TOOL,
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
        assert quality.result["content"]["status"] == "pass"
        assert quality.result["content"]["source_revision"] == revision

        metrics = {}
        for metric_key in PRICE_INSIGHT_METRIC_KEYS:
            computed = await controller.execute(
                _assignment(),
                ToolCall(
                    tool_invocation_id=f"price-metric-{metric_key}",
                    name=PRICE_METRIC_TOOLS[metric_key],
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
            content = computed.result["content"]
            assert content["source_revision"] == revision
            assert content["metric"]["key"] == metric_key
            metrics[metric_key] = content["metric"]["value"]

        assert len(metrics) == 8
        assert metrics["impact_amount"] == 40000
        assert metrics["impact_neg_amount"] == 40000

        evidence = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="price-market-evidence",
                name=PRICE_METRIC_EVIDENCE_LIST_TOOL,
                arguments={
                    "filter": {
                        "period_from": "2026-01",
                        "period_to": "2026-02",
                        "anchor": "market",
                    },
                    "metric_key": "market_dev_pct",
                    "limit": 2,
                },
            ),
            activated.state,
        )
        assert evidence.result["content"]["metric_key"] == "market_dev_pct"
        assert evidence.result["content"]["source_revision"] == revision
        assert len(evidence.result["content"]["rows"]) <= 2

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
