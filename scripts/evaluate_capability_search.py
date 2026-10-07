#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.capability_search_quality import (
    GoldenSearchCase,
    SearchObservation,
    evaluate_capability_search,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.infrastructure.model.capability_embeddings import (
    OpenAICompatibleCapabilityEmbeddingProvider,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the versioned capability-search quality gate")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("tests/fixtures/capability_search_golden_v1.json"),
    )
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", default="BAAI/bge-m3")
    parser.add_argument("--dimensions", type=int, default=1024)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--query-timeout", type=float, default=12.0)
    parser.add_argument("--semantic-min-similarity", type=float, default=0.50)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def descriptor(item: dict[str, Any], *, tenant_id: str, server_id: str) -> CapabilityDescriptor:
    name = str(item["canonical_name"])
    digest = hashlib.sha256(
        json.dumps(item, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    metadata: dict[str, object] = {"source_type": "quality-gate"}
    if isinstance(item.get("allowed_roles"), list):
        metadata["allowed_roles"] = item["allowed_roles"]
    return CapabilityDescriptor(
        capability_id=f"quality:{tenant_id}:{name}",
        kind=CapabilityKind.TOOL,
        server_id=server_id,
        canonical_name=name,
        version="1",
        content_digest=f"sha256:{digest}",
        title=str(item["title"]),
        description=str(item["description"]),
        tags=tuple(str(value) for value in item.get("tags", ())),
        tenant_id=tenant_id,
        permission=str(item["permission"]),
        risk_level=str(item["risk_level"]),
        status=CapabilityStatus.ACTIVE,
        updated_at=datetime.now(UTC),
        metadata=metadata,
    )


async def run(args: argparse.Namespace) -> dict[str, object]:
    dataset = json.loads(args.dataset.read_text())
    provider = OpenAICompatibleCapabilityEmbeddingProvider(
        endpoint=args.endpoint,
        model=args.model,
        dimensions=args.dimensions,
        api_key=args.api_key,
        timeout_seconds=args.query_timeout,
    )
    try:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(
            store,
            embedding_provider=provider,
            environment="production",
            semantic_min_similarity=args.semantic_min_similarity,
            # Quality and latency gates must measure every query rather than
            # reusing a prior result from the serving-path L1.
            search_cache_max_entries=0,
        )
        tenant_id = str(dataset["tenant_id"])
        server_id = "quality-gate-tenant-a"
        hidden_server_id = "quality-gate-tenant-b"
        for current_tenant, current_server in (
            (tenant_id, server_id),
            ("tenant-b", hidden_server_id),
        ):
            await catalog.register_server(
                McpServerDefinition(
                    server_id=current_server,
                    tenant_id=current_tenant,
                    title=current_server,
                    endpoint=f"https://{current_server}.invalid/mcp",
                    status=CapabilityStatus.ACTIVE,
                    enabled=True,
                )
            )
        await catalog.replace_server_capabilities(
            server_id,
            tuple(
                descriptor(item, tenant_id=tenant_id, server_id=server_id)
                for item in dataset["capabilities"]
            ),
        )
        await catalog.replace_server_capabilities(
            hidden_server_id,
            (
                descriptor(
                    dataset["hidden_tenant_capability"],
                    tenant_id="tenant-b",
                    server_id=hidden_server_id,
                ),
            ),
        )
        observations: list[SearchObservation] = []
        case_results: list[dict[str, object]] = []
        for raw in dataset["cases"]:
            case = GoldenSearchCase(
                case_id=str(raw["id"]),
                tenant_id=tenant_id,
                query=str(raw["query"]),
                expected=tuple(str(value) for value in raw.get("expected", ())),
                forbidden=tuple(str(value) for value in raw.get("forbidden", ())),
                dangerous=tuple(str(value) for value in raw.get("dangerous", ())),
                expect_empty=bool(raw.get("expect_empty", False)),
            )
            started = time.monotonic()
            outcome = await catalog.search_with_evidence(
                tenant_id=tenant_id,
                query=case.query,
                kinds=(CapabilityKind.TOOL,),
                actor_role="worker",
                limit=5,
            )
            latency = time.monotonic() - started
            ranking = tuple(match.capability.canonical_name for match in outcome.matches)
            observations.append(
                SearchObservation(
                    case=case,
                    ranking=ranking,
                    latency_seconds=latency,
                    semantic_degraded=outcome.semantic_degraded,
                    index_generation_lag=outcome.generation_lag,
                )
            )
            case_results.append(
                {
                    "case_id": case.case_id,
                    "ranking": ranking,
                    "latency_seconds": round(latency, 6),
                    "semantic_degraded": outcome.semantic_degraded,
                }
            )
        report = evaluate_capability_search(
            tuple(observations),
            dataset=str(dataset["dataset"]),
            dataset_version=str(dataset["version"]),
        )
        return {
            "dataset": report.dataset,
            "dataset_version": report.dataset_version,
            "threshold_version": report.threshold_version,
            "embedding_model_version": provider.model_version,
            "search_policy_version": "capability-hybrid-rrf-v1",
            "semantic_min_similarity": args.semantic_min_similarity,
            "metrics": report.metrics,
            "passed": report.passed,
            "failures": report.failures,
            "cases": case_results,
        }
    finally:
        await provider.close()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(encoded)
    print(encoded, end="")
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
