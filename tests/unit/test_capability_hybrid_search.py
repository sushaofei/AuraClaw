from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.action.capability_catalog import (
    CAPABILITY_SEARCH_TOOL_NAME,
    CapabilityCatalog,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
    capability_search_tool,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.tools import ToolInvocation


class SemanticFixture:
    model_version = "fixture-multilingual-v1:dim-3:l2"
    dimensions = 3

    def __init__(self) -> None:
        self.fail = False
        self.batch_sizes: list[int] = []
        self.block = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def embed(
        self,
        texts: Sequence[str],
        *,
        timeout_seconds: float | None = None,
    ) -> tuple[tuple[float, ...], ...]:
        del timeout_seconds
        self.batch_sizes.append(len(texts))
        if self.fail:
            raise TimeoutError("fixture timeout")
        if self.block:
            self.started.set()
            await self.release.wait()
        return tuple(self._vector(text) for text in texts)

    @staticmethod
    def _vector(text: str) -> tuple[float, ...]:
        folded = text.casefold()
        if "github.issue.create" in folded or "程序缺陷" in folded or "bug" in folded:
            return (1.0, 0.0, 0.0)
        if "supplier.risk.profile" in folded or "经营风险" in folded:
            return (0.0, 1.0, 0.0)
        return (0.0, 0.0, 1.0)


class Metrics:
    def __init__(self) -> None:
        self.points: list[MetricPoint] = []

    async def write_metric(self, metric: MetricPoint) -> None:
        self.points.append(metric)


class CountingCatalogStore(InMemoryCapabilityCatalogStore):
    def __init__(self) -> None:
        super().__init__()
        self.list_calls = 0
        self.canonical_name_calls = 0

    async def list_capabilities(self, tenant_id: str) -> tuple[CapabilityDescriptor, ...]:
        self.list_calls += 1
        return await super().list_capabilities(tenant_id)

    async def find_capabilities_by_canonical_name(
        self, tenant_id: str, canonical_name: str
    ) -> tuple[CapabilityDescriptor, ...]:
        self.canonical_name_calls += 1
        capabilities = await InMemoryCapabilityCatalogStore.list_capabilities(
            self, tenant_id
        )
        return tuple(
            capability
            for capability in capabilities
            if capability.canonical_name == canonical_name
        )


def descriptor(
    capability_id: str,
    name: str,
    *,
    tenant_id: str,
    description: str,
    metadata: dict[str, object] | None = None,
) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capability_id=capability_id,
        kind=CapabilityKind.TOOL,
        server_id=f"server-{tenant_id}",
        canonical_name=name,
        version="1",
        content_digest="sha256:" + capability_id.ljust(64, "0")[:64],
        title=name,
        description=description,
        tags=(),
        tenant_id=tenant_id,
        permission="write-with-approval" if name.endswith("create") else "read-only",
        risk_level="high" if name.endswith("create") else "low",
        status=CapabilityStatus.ACTIVE,
        updated_at=datetime.now(UTC),
        metadata=metadata or {},
    )


async def seeded_catalog(
    *, provider: SemanticFixture | None = None, metrics: Metrics | None = None
) -> tuple[CapabilityCatalog, InMemoryCapabilityCatalogStore]:
    store = InMemoryCapabilityCatalogStore()
    catalog = CapabilityCatalog(
        store,
        embedding_provider=provider,
        metric_writer=metrics,
        environment="production",
        semantic_min_similarity=0.5,
    )
    for tenant in ("a", "b"):
        await catalog.register_server(
            McpServerDefinition(
                server_id=f"server-{tenant}",
                tenant_id=tenant,
                title=f"Tenant {tenant}",
                endpoint=f"https://{tenant}.example/mcp",
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
    await catalog.replace_server_capabilities(
        "server-a",
        (
            descriptor(
                "cap-github",
                "github.issue.create",
                tenant_id="a",
                description="Create an issue in a governed source repository.",
            ),
            descriptor(
                "cap-supplier",
                "supplier.risk.profile",
                tenant_id="a",
                description="Inspect recent operational risk for a supplier.",
            ),
            descriptor(
                "cap-hidden-role",
                "finance.payment.create",
                tenant_id="a",
                description="Create a governed payment.",
                metadata={"allowed_roles": ["finance-reviewer"]},
            ),
        ),
    )
    await catalog.replace_server_capabilities(
        "server-b",
        (
            descriptor(
                "cap-b-secret",
                "tenant.secret.lookup",
                tenant_id="b",
                description="Confidential tenant B lookup.",
            ),
        ),
    )
    return catalog, store


def test_hybrid_search_cross_language_exact_precedence_and_tenant_filtering() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        catalog, _ = await seeded_catalog(provider=provider)
        assert provider.batch_sizes[:2] == [1, 2]
        issue = await catalog.search_with_evidence(
            tenant_id="a",
            query="登记一个程序缺陷",
            actor_role="worker",
        )
        assert issue.matches[0].capability.capability_id == "cap-github"
        assert issue.matches[0].match_reasons == ("semantic:cosine",)
        assert not issue.semantic_degraded

        supplier = await catalog.search_with_evidence(
            tenant_id="a",
            query="查一下供应商最近是否有经营风险",
            actor_role="worker",
        )
        assert supplier.matches[0].capability.capability_id == "cap-supplier"
        assert all(match.capability.tenant_id == "a" for match in supplier.matches)
        assert all(
            match.capability.capability_id != "cap-hidden-role" for match in supplier.matches
        )

        exact = await catalog.search_with_evidence(
            tenant_id="a",
            query="supplier.risk.profile",
            actor_role="worker",
        )
        assert exact.matches[0].capability.capability_id == "cap-supplier"
        assert "exact:canonical_name" in exact.matches[0].match_reasons

        filtered = await catalog.search_with_evidence(
            tenant_id="a",
            query="completely unrelated wording",
            capability_id="cap-github",
            actor_role="worker",
        )
        assert filtered.matches[0].capability.capability_id == "cap-github"
        assert "exact:capability_id_filter" in filtered.matches[0].match_reasons

    asyncio.run(scenario())


def test_search_cache_reuses_query_and_invalidates_on_catalog_generation() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        metrics = Metrics()
        catalog, store = await seeded_catalog(provider=provider, metrics=metrics)
        indexed_batches = len(provider.batch_sizes)

        first = await catalog.search_with_evidence(
            tenant_id="a", query="登记一个程序缺陷", actor_role="worker"
        )
        second = await catalog.search_with_evidence(
            tenant_id="a", query="登记一个程序缺陷", actor_role="worker"
        )
        assert second == first
        assert len(provider.batch_sizes) == indexed_batches + 1

        snapshot = await store.read_committed_snapshot("a", "server-a")
        assert snapshot is not None
        await catalog.replace_server_capabilities(
            "server-a",
            tuple(
                item.model_copy(
                    update={
                        "description": item.description + " refreshed",
                        "updated_at": item.updated_at + timedelta(seconds=1),
                        "metadata": {
                            key: value
                            for key, value in item.metadata.items()
                            if key not in {"catalog_generation", "_capability_search_index"}
                        },
                    }
                )
                for item in snapshot.capabilities
            ),
        )
        batches_after_publish = len(provider.batch_sizes)
        await catalog.search_with_evidence(
            tenant_id="a", query="登记一个程序缺陷", actor_role="worker"
        )
        assert len(provider.batch_sizes) == batches_after_publish + 1
        cache_outcomes = [
            point.labels["outcome"]
            for point in metrics.points
            if point.name == "capability_search_cache_requests_total"
        ]
        assert cache_outcomes == ["miss", "hit", "miss"]

    asyncio.run(scenario())


def test_search_cache_coalesces_concurrent_misses() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        metrics = Metrics()
        catalog, _ = await seeded_catalog(provider=provider, metrics=metrics)
        indexed_batches = len(provider.batch_sizes)
        provider.started.clear()
        provider.release.clear()
        provider.block = True

        first = asyncio.create_task(
            catalog.search_with_evidence(
                tenant_id="a", query="登记一个程序缺陷", actor_role="worker"
            )
        )
        await provider.started.wait()
        second = asyncio.create_task(
            catalog.search_with_evidence(
                tenant_id="a", query="登记一个程序缺陷", actor_role="worker"
            )
        )
        await asyncio.sleep(0)
        provider.release.set()
        first_result, second_result = await asyncio.gather(first, second)
        assert first_result == second_result
        assert len(provider.batch_sizes) == indexed_batches + 1
        cache_outcomes = {
            point.labels["outcome"]
            for point in metrics.points
            if point.name == "capability_search_cache_requests_total"
        }
        assert cache_outcomes == {"miss", "coalesced"}

    asyncio.run(scenario())


def test_exact_canonical_name_uses_targeted_store_lookup() -> None:
    async def scenario() -> None:
        store = CountingCatalogStore()
        catalog = CapabilityCatalog(store, environment="production")
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-a",
                tenant_id="a",
                title="Tenant a",
                endpoint="https://a.example/mcp",
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "server-a",
            (
                descriptor(
                    "cap-github",
                    "github.issue.create",
                    tenant_id="a",
                    description="Create an issue.",
                ),
            ),
        )

        result = await catalog.search_with_evidence(
            tenant_id="a",
            query="github.issue.create",
            canonical_name="github.issue.create",
            actor_role="worker",
        )
        assert result.matches[0].capability.capability_id == "cap-github"
        assert store.canonical_name_calls == 1
        assert store.list_calls == 0

    asyncio.run(scenario())


def test_embedding_failure_falls_back_to_lexical_and_emits_controlled_contract() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        metrics = Metrics()
        catalog, _ = await seeded_catalog(provider=provider, metrics=metrics)
        provider.fail = True
        result = await CapabilitySearchExecutor(catalog).execute(
            ToolInvocation(
                tool_invocation_id="search-1",
                tenant_id="a",
                root_session_id="root",
                session_id="session",
                run_id="run",
                tool_name=CAPABILITY_SEARCH_TOOL_NAME,
                tool_version="1",
                arguments={"query": "github issue", "limit": 5},
                expected_side_effect="read",
                idempotency_key="search-1",
                deadline=None,
                fencing_token=1,
                actor_id="runtime",
                actor_role="worker",
            ),
            capability_search_tool(),
        )
        assert result["capabilities"][0]["capability_id"] == "cap-github"
        assert result["semantic_degraded"] is True
        assert result["semantic_degraded_reason"] == "embedding_query_failed"
        assert result["search_policy_version"] == "capability-hybrid-rrf-v1"
        assert result["capabilities"][0]["match_reasons"] == ["lexical:bm25"]
        names = {point.name for point in metrics.points}
        assert "capability_search_requests_total" in names
        assert "capability_search_semantic_degraded_total" in names
        assert "capability_search_candidate_count" in names

    asyncio.run(scenario())


def test_failed_index_build_preserves_last_known_good_and_releases_lease() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        catalog, store = await seeded_catalog(provider=provider)
        before = await store.read_committed_snapshot("a", "server-a")
        assert before is not None
        provider.fail = True
        changed = descriptor(
            "cap-github-v2",
            "github.issue.create",
            tenant_id="a",
            description="Changed source contract.",
        )
        try:
            await catalog.replace_server_capabilities("server-a", (changed,))
        except TimeoutError:
            pass
        else:
            raise AssertionError("index build failure must reject publication")
        after = await store.read_committed_snapshot("a", "server-a")
        assert after is not None
        assert after.generation == before.generation
        assert {item.capability_id for item in after.capabilities} == {
            "cap-github",
            "cap-supplier",
            "cap-hidden-role",
        }
        provider.fail = False
        committed = await catalog.replace_server_capabilities("server-a", (changed,))
        assert committed.committed
        assert committed.generation == before.generation + 1

    asyncio.run(scenario())


def test_model_version_change_publishes_once_and_stale_index_degrades() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        catalog, store = await seeded_catalog(provider=provider)
        snapshot = await store.read_committed_snapshot("a", "server-a")
        assert snapshot is not None
        unchanged = await catalog.replace_server_capabilities(
            "server-a",
            tuple(
                item.model_copy(
                    update={
                        "metadata": {
                            key: value
                            for key, value in item.metadata.items()
                            if key not in {"catalog_generation", "_capability_search_index"}
                        }
                    }
                )
                for item in snapshot.capabilities
            ),
        )
        assert not unchanged.committed
        provider.model_version = "fixture-multilingual-v2:dim-3:l2"
        upgraded = await catalog.replace_server_capabilities(
            "server-a",
            tuple(
                item.model_copy(
                    update={
                        "metadata": {
                            key: value
                            for key, value in item.metadata.items()
                            if key not in {"catalog_generation", "_capability_search_index"}
                        }
                    }
                )
                for item in snapshot.capabilities
            ),
        )
        assert upgraded.committed
        assert upgraded.generation == snapshot.generation + 1

    asyncio.run(scenario())


def test_multi_replica_direct_publish_waits_for_owner_and_reuses_committed_index() -> None:
    async def scenario() -> None:
        provider = SemanticFixture()
        first, store = await seeded_catalog(provider=provider)
        second = CapabilityCatalog(
            store,
            embedding_provider=provider,
            environment="production",
            index_embedding_timeout_seconds=2,
        )
        snapshot = await store.read_committed_snapshot("a", "server-a")
        assert snapshot is not None

        def changed(offset: int) -> tuple[CapabilityDescriptor, ...]:
            return tuple(
                item.model_copy(
                    update={
                        "description": item.description + " changed",
                        "updated_at": item.updated_at + timedelta(seconds=offset),
                        "metadata": {
                            key: value
                            for key, value in item.metadata.items()
                            if key not in {"catalog_generation", "_capability_search_index"}
                        },
                    }
                )
                for item in snapshot.capabilities
            )

        provider.block = True
        owner = asyncio.create_task(
            first.replace_server_capabilities("server-a", changed(1))
        )
        await provider.started.wait()
        follower = asyncio.create_task(
            second.replace_server_capabilities("server-a", changed(2))
        )
        await asyncio.sleep(0.05)
        assert not follower.done()
        provider.release.set()
        owner_result, follower_result = await asyncio.gather(owner, follower)
        assert owner_result.committed
        assert not follower_result.committed
        assert follower_result.generation == owner_result.generation

    asyncio.run(scenario())


def test_search_aliases_require_tenant_actor_source_and_revision() -> None:
    with pytest.raises(ValueError, match="search aliases require"):
        McpServerDefinition(
            server_id="server-a",
            tenant_id="a",
            title="Tenant A",
            endpoint="https://a.example/mcp",
            metadata={"search_aliases": ["ungoverned alias"]},
        )
