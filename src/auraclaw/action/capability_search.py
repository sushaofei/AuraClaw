from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from auraclaw.contracts.capabilities import CapabilityDescriptor

SEARCH_POLICY_VERSION = "capability-hybrid-rrf-v1"
SEARCH_INDEX_METADATA_KEY = "_capability_search_index"
SEARCH_INPUT_TEMPLATE_VERSION = "capability-summary-v1"
_LATIN = re.compile(r"[A-Za-z0-9_.-]+")
_CJK = re.compile(r"[\u3400-\u9FFF\uF900-\uFAFF]+")


class CapabilityEmbeddingProvider(Protocol):
    model_version: str
    dimensions: int

    async def embed(
        self,
        texts: Sequence[str],
        *,
        timeout_seconds: float | None = None,
    ) -> tuple[tuple[float, ...], ...]: ...


@dataclass(frozen=True)
class SearchIndexEntry:
    capability_id: str
    catalog_generation: int
    content_digest: str
    embedding_model_version: str
    search_policy_version: str
    input_template_version: str
    source_snapshot_digest: str
    document_digest: str
    vector: tuple[float, ...]

    def as_metadata(self) -> dict[str, object]:
        return {
            "capability_id": self.capability_id,
            "catalog_generation": self.catalog_generation,
            "content_digest": self.content_digest,
            "embedding_model_version": self.embedding_model_version,
            "search_policy_version": self.search_policy_version,
            "input_template_version": self.input_template_version,
            "source_snapshot_digest": self.source_snapshot_digest,
            "document_digest": self.document_digest,
            "normalization": "l2",
            "dimensions": len(self.vector),
            "vector": list(self.vector),
        }


@dataclass(frozen=True)
class SearchMatch:
    capability: CapabilityDescriptor
    match_reasons: tuple[str, ...]
    exact_rank: int | None
    lexical_rank: int | None
    semantic_rank: int | None
    lexical_score: float
    semantic_score: float | None
    fused_score: float


@dataclass(frozen=True)
class SearchOutcome:
    matches: tuple[SearchMatch, ...]
    semantic_degraded: bool
    degraded_reason: str | None
    search_policy_version: str
    candidate_counts: Mapping[str, int]
    generation_lag: int


def controlled_search_document(capability: CapabilityDescriptor) -> str:
    """Build the only text allowed to leave the catalog for embedding."""
    aliases = governed_search_aliases(capability)
    values = {
        "canonical_name": capability.canonical_name,
        "title": capability.title,
        "description": capability.description,
        "tags": " | ".join(capability.tags),
        "aliases": " | ".join(str(value) for value in aliases[:32]),
        "server_title": str(capability.metadata.get("server_title", "")),
        "source_type": str(capability.metadata.get("source_type", "")),
        "kind": capability.kind.value,
        "permission": capability.permission or "",
    }
    return "\n".join(f"{name}: {_clean_text(value)}" for name, value in values.items())


def governed_search_aliases(capability: CapabilityDescriptor) -> tuple[str, ...]:
    aliases = capability.metadata.get("search_aliases", ())
    governance = capability.metadata.get("search_alias_governance")
    if not isinstance(aliases, (list, tuple)) or not isinstance(governance, dict):
        return ()
    expected_tenant = capability.tenant_id or "platform"
    if (
        str(governance.get("tenant_id", "")) != expected_tenant
        or not str(governance.get("actor_id", "")).strip()
        or not str(governance.get("source", "")).strip()
        or not isinstance(governance.get("revision"), int)
        or isinstance(governance.get("revision"), bool)
        or int(governance["revision"]) < 1
    ):
        return ()
    return tuple(dict.fromkeys(str(value).strip() for value in aliases[:32] if str(value).strip()))


async def index_capabilities(
    capabilities: Sequence[CapabilityDescriptor],
    *,
    generation: int,
    provider: CapabilityEmbeddingProvider,
    source_snapshot_digest: str,
    timeout_seconds: float | None = None,
    policy_version: str = SEARCH_POLICY_VERSION,
) -> tuple[CapabilityDescriptor, ...]:
    if not capabilities:
        return ()
    documents = tuple(controlled_search_document(item) for item in capabilities)
    vector_batch: list[tuple[float, ...]] = []
    # Prime the single-input inference shape used by online queries while the
    # reconcile lease is still allowed to absorb model warm-up latency.
    vector_batch.extend(
        await provider.embed(
            documents[:1],
            timeout_seconds=timeout_seconds,
        )
    )
    for offset in range(1, len(documents), 128):
        vector_batch.extend(
            await provider.embed(
                documents[offset : offset + 128],
                timeout_seconds=timeout_seconds,
            )
        )
    vectors = tuple(vector_batch)
    if len(vectors) != len(capabilities):
        raise ValueError("Embedding response count does not match capability count")
    indexed: list[CapabilityDescriptor] = []
    for capability, document, raw_vector in zip(capabilities, documents, vectors, strict=True):
        vector = normalize_vector(raw_vector, dimensions=provider.dimensions)
        entry = SearchIndexEntry(
            capability_id=capability.capability_id,
            catalog_generation=generation,
            content_digest=capability.content_digest,
            embedding_model_version=provider.model_version,
            search_policy_version=policy_version,
            input_template_version=SEARCH_INPUT_TEMPLATE_VERSION,
            source_snapshot_digest=source_snapshot_digest,
            document_digest="sha256:" + hashlib.sha256(document.encode()).hexdigest(),
            vector=vector,
        )
        indexed.append(
            capability.model_copy(
                update={
                    "metadata": {
                        **capability.metadata,
                        SEARCH_INDEX_METADATA_KEY: entry.as_metadata(),
                    }
                }
            )
        )
    return tuple(indexed)


def read_search_index(
    capability: CapabilityDescriptor,
    *,
    provider: CapabilityEmbeddingProvider,
    policy_version: str = SEARCH_POLICY_VERSION,
) -> SearchIndexEntry | None:
    raw = capability.metadata.get(SEARCH_INDEX_METADATA_KEY)
    if not isinstance(raw, dict):
        return None
    generation = capability.metadata.get("catalog_generation")
    try:
        entry = SearchIndexEntry(
            capability_id=str(raw["capability_id"]),
            catalog_generation=int(raw["catalog_generation"]),
            content_digest=str(raw["content_digest"]),
            embedding_model_version=str(raw["embedding_model_version"]),
            search_policy_version=str(raw["search_policy_version"]),
            input_template_version=str(raw["input_template_version"]),
            source_snapshot_digest=str(raw["source_snapshot_digest"]),
            document_digest=str(raw["document_digest"]),
            vector=tuple(float(value) for value in raw["vector"]),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if (
        entry.capability_id != capability.capability_id
        or entry.catalog_generation != generation
        or entry.content_digest != capability.content_digest
        or entry.embedding_model_version != provider.model_version
        or entry.search_policy_version != policy_version
        or entry.input_template_version != SEARCH_INPUT_TEMPLATE_VERSION
    ):
        return None
    try:
        normalized = normalize_vector(entry.vector, dimensions=provider.dimensions)
    except ValueError:
        return None
    if any(abs(left - right) > 1e-5 for left, right in zip(entry.vector, normalized, strict=True)):
        return None
    return entry


def lexical_scores(query: str, capabilities: Sequence[CapabilityDescriptor]) -> dict[str, float]:
    query_terms = search_terms(query)
    if not query_terms or not capabilities:
        return {}
    documents = {
        item.capability_id: search_terms(controlled_search_document(item)) for item in capabilities
    }
    document_frequency: Counter[str] = Counter()
    for terms in documents.values():
        document_frequency.update(set(terms))
    query_counts = Counter(query_terms)
    scores: dict[str, float] = {}
    for capability in capabilities:
        terms = documents[capability.capability_id]
        term_counts = Counter(terms)
        score = 0.0
        for term, query_frequency in query_counts.items():
            frequency = term_counts.get(term, 0)
            if frequency == 0:
                continue
            inverse_frequency = math.log(
                1.0
                + (len(documents) - document_frequency[term] + 0.5)
                / (document_frequency[term] + 0.5)
            )
            # Typed capability summaries are short; governance fields must not create a
            # length penalty that changes ordering for otherwise equivalent matches.
            denominator = frequency + 1.2
            score += query_frequency * inverse_frequency * frequency * 2.2 / denominator
        if score > 0:
            scores[capability.capability_id] = score
    return scores


def exact_reasons(query: str, capability: CapabilityDescriptor) -> tuple[str, ...]:
    value = query.casefold().strip()
    if not value:
        return ()
    reasons: list[str] = []
    if value == capability.capability_id.casefold():
        reasons.append("exact:capability_id")
    if value == capability.canonical_name.casefold():
        reasons.append("exact:canonical_name")
    if value == capability.server_id.casefold():
        reasons.append("exact:server_id")
    return tuple(reasons)


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        raise ValueError("Embedding vectors have incompatible dimensions")
    return sum(a * b for a, b in zip(left, right, strict=True))


def reciprocal_rank_fusion(
    *,
    exact: Sequence[str],
    lexical: Sequence[str],
    semantic: Sequence[str],
    rank_constant: int = 60,
) -> dict[str, float]:
    scores: dict[str, float] = {}
    for weight, ranking in ((4.0, exact), (1.0, lexical), (1.0, semantic)):
        for rank, capability_id in enumerate(ranking, start=1):
            scores[capability_id] = scores.get(capability_id, 0.0) + weight / (rank_constant + rank)
    return scores


def normalize_vector(vector: Sequence[float], *, dimensions: int) -> tuple[float, ...]:
    parsed = tuple(float(value) for value in vector)
    if len(parsed) != dimensions:
        raise ValueError("Embedding vector has an unexpected dimension")
    if not all(math.isfinite(value) for value in parsed):
        raise ValueError("Embedding vector contains a non-finite value")
    norm = math.sqrt(sum(value * value for value in parsed))
    if norm <= 1e-12:
        raise ValueError("Embedding vector has zero norm")
    return tuple(value / norm for value in parsed)


def search_terms(value: str) -> tuple[str, ...]:
    terms: list[str] = []
    for token in _LATIN.findall(value):
        folded = token.casefold()
        terms.append(folded)
        terms.extend(part for part in re.split(r"[_.-]+", folded) if part and part != folded)
    for run in _CJK.findall(value):
        terms.append(run)
        if len(run) >= 2:
            terms.extend(run[index : index + 2] for index in range(len(run) - 1))
    return tuple(terms)


def search_index_digest(capabilities: Sequence[CapabilityDescriptor]) -> str:
    payload = [
        {
            key: raw.get(key)
            for key in (
                "capability_id",
                "content_digest",
                "embedding_model_version",
                "search_policy_version",
                "input_template_version",
                "source_snapshot_digest",
                "document_digest",
                "normalization",
                "dimensions",
            )
        }
        for item in sorted(capabilities, key=lambda value: value.capability_id)
        if isinstance((raw := item.metadata.get(SEARCH_INDEX_METADATA_KEY)), dict)
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def capability_source_digest(capabilities: Sequence[CapabilityDescriptor]) -> str:
    payload = []
    for item in sorted(
        capabilities,
        key=lambda value: (
            value.kind.value,
            value.canonical_name,
            value.version,
            value.capability_id,
        ),
    ):
        value = item.model_copy(
            update={
                "metadata": {
                    key: value
                    for key, value in item.metadata.items()
                    if key not in {SEARCH_INDEX_METADATA_KEY, "catalog_generation"}
                }
            }
        ).model_dump(mode="json")
        value.pop("updated_at", None)
        payload.append(value)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _clean_text(value: object) -> str:
    text = " ".join(str(value).split())
    return "".join(
        character for character in text[:4096] if character >= " " and character != "\x7f"
    )
