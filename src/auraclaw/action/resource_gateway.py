from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlsplit

from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.ports import (
    ArtifactWriter,
    CapabilityCatalogStore,
    CapabilityConnector,
    ResourcePolicyEvaluator,
)
from auraclaw.action.skill_packages import SkillDependencyAvailability
from auraclaw.contracts.capabilities import CapabilityDescriptor, CapabilityKind
from auraclaw.contracts.errors import (
    PolicyDeniedError,
    ResourceBusyError,
    SchemaValidationError,
)
from auraclaw.contracts.hands import (
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsTrustedContext,
)
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.tools import PolicyDecision

_SECRET_PATTERN = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)"
    r"\s*[:=]\s*[^\s,;]+"
)
_PROMPT_INJECTION_PATTERNS = (
    re.compile(r"(?i)\bignore (all |any )?(previous|prior) instructions\b"),
    re.compile(r"(?i)\breveal (the )?(system prompt|hidden instructions)\b"),
)


class ResourceContentScanner(Protocol):
    def scan(self, content: bytes, *, media_type: str) -> tuple[str, ...]: ...


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


class DefaultResourceContentScanner:
    def scan(self, content: bytes, *, media_type: str) -> tuple[str, ...]:
        if media_type.startswith("text/") or media_type in {
            "application/json",
            "application/xml",
            "application/yaml",
        } or (
            media_type.startswith("application/") and media_type.endswith("+json")
        ):
            if b"\x00" in content:
                raise SchemaValidationError("text Resource contains null bytes")
            text = content.decode("utf-8", errors="replace")
            if _SECRET_PATTERN.search(text):
                raise PolicyDeniedError("Resource content contains secret-like data")
            if any(pattern.search(text) for pattern in _PROMPT_INJECTION_PATTERNS):
                return ("prompt_injection",)
        return ()


@dataclass(frozen=True)
class _CacheEntry:
    expires_at: float
    contents: tuple[HandsResourceContent, ...]


@dataclass(frozen=True)
class _ResolvedResource:
    descriptor: HandsResourceDescriptor
    load: Callable[[], Awaitable[tuple[HandsResourceContent, ...]]]


class ManagedResourceGateway:
    def __init__(
        self,
        registry: HandsResourceRegistry,
        *,
        artifacts: ArtifactWriter,
        policy: ResourcePolicyEvaluator | None = None,
        scanner: ResourceContentScanner | None = None,
        allowed_schemes: tuple[str, ...] = ("artifact", "memory", "repo", "skill"),
        allowed_media_types: tuple[str, ...] = (
            "text/*",
            "application/json",
            "application/schema+json",
            "application/xml",
            "application/yaml",
            "application/octet-stream",
        ),
        max_inline_bytes: int = 64 * 1024,
        max_resource_bytes: int = 8 * 1024 * 1024,
        cache_ttl_seconds: float = 30.0,
        max_concurrent: int = 32,
        max_queued: int = 128,
        queue_timeout_seconds: float = 5.0,
        metric_writer: MetricWriter | None = None,
        catalog_store: CapabilityCatalogStore | None = None,
        connectors: Mapping[str, CapabilityConnector] | None = None,
        miss_loader: Callable[[str, str], Awaitable[object]] | None = None,
    ) -> None:
        if max_inline_bytes < 1 or max_resource_bytes < max_inline_bytes:
            raise ValueError("Resource size limits are invalid")
        if cache_ttl_seconds < 0:
            raise ValueError("Resource cache TTL cannot be negative")
        if max_concurrent < 1 or max_queued < 1 or queue_timeout_seconds <= 0:
            raise ValueError("Resource gateway capacity limits must be positive")
        self._registry = registry
        self._artifacts = artifacts
        self._policy = policy
        self._scanner = scanner or DefaultResourceContentScanner()
        self._allowed_schemes = frozenset(allowed_schemes)
        self._allowed_media_types = allowed_media_types
        self._max_inline_bytes = max_inline_bytes
        self._max_resource_bytes = max_resource_bytes
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[tuple[str, str, str, str, str | None], _CacheEntry] = {}
        self._loads: dict[
            tuple[str, str, str, str, str | None],
            asyncio.Task[tuple[HandsResourceContent, ...]],
        ] = {}
        self._generations: dict[tuple[str, str], int] = {}
        self._state_lock = asyncio.Lock()
        self._capacity = asyncio.Semaphore(max_concurrent)
        self._max_queued = max_queued
        self._queue_timeout_seconds = queue_timeout_seconds
        self._queued = 0
        self._inflight = 0
        self._metric_writer = metric_writer
        self._catalog_store = catalog_store
        self._skill_availability = SkillDependencyAvailability(catalog_store)
        self._connectors = connectors if connectors is not None else {}
        self._miss_loader = miss_loader

    async def is_available(
        self, tenant_id: str, capability: CapabilityDescriptor
    ) -> bool:
        if capability.kind is CapabilityKind.SKILL:
            available = await self._skill_availability.is_available(
                tenant_id, capability
            )
            if not available:
                await self._emit(
                    "capability.catalog.backing_missing",
                    1.0,
                    tenant_id,
                    server_id=capability.server_id,
                    kind=capability.kind.value,
                )
            return available
        if capability.kind not in {
            CapabilityKind.RESOURCE,
            CapabilityKind.RESOURCE_TEMPLATE,
        }:
            return True
        source = capability.metadata.get("source", {})
        if not isinstance(source, dict):
            return False
        uri = source.get("uri")
        available = (
            isinstance(uri, str)
            and self._registry.has_resource(tenant_id, uri)
        ) or capability.server_id in self._connectors
        if not available:
            await self._emit(
                "capability.catalog.backing_missing",
                1.0,
                tenant_id,
                server_id=capability.server_id,
                kind=capability.kind.value,
            )
        return available

    async def read(
        self,
        trusted_context: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        parsed = urlsplit(uri)
        if not parsed.scheme or parsed.scheme not in self._allowed_schemes:
            raise PolicyDeniedError("Resource URI scheme is not allowed")
        resource = await self._resolve_resource(trusted_context, uri)
        source_revision = (
            None
            if resource.descriptor.source_revision is None
            else str(resource.descriptor.source_revision)
        )
        cache_key = (
            trusted_context.tenant_id,
            trusted_context.root_session_id,
            trusted_context.session_id,
            uri,
            source_revision,
        )
        async with self._state_lock:
            cached = self._cache.get(cache_key)
            if cached is not None and cached.expires_at > time.monotonic():
                return tuple(_with_cache_hit(content, True) for content in cached.contents)
            load = self._loads.get(cache_key)
            queue_full = False
            if load is None:
                if self._queued >= self._max_queued:
                    queue_full = True
                    queue_depth = None
                else:
                    self._queued += 1
                    generation = self._generations.get(
                        (trusted_context.tenant_id, uri), 0
                    )
                    load = asyncio.create_task(
                        self._load(
                            trusted_context,
                            uri,
                            resource,
                            cache_key,
                            generation,
                        )
                    )
                    self._loads[cache_key] = load
                    queue_depth = self._queued
            else:
                queue_depth = None
        if queue_full:
            await self._emit(
                "resource.gateway.backpressure.count",
                1.0,
                trusted_context.tenant_id,
                reason="queue_full",
            )
            raise ResourceBusyError()
        if queue_depth is not None:
            await self._emit(
                "resource.gateway.queue.depth",
                float(queue_depth),
                trusted_context.tenant_id,
            )
        assert load is not None
        return await asyncio.shield(load)

    async def _resolve_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> _ResolvedResource:
        try:
            registered = self._registry.get_resource(trusted.tenant_id, uri)

            async def load_local() -> tuple[HandsResourceContent, ...]:
                return registered.contents

            return _ResolvedResource(registered.descriptor, load_local)
        except KeyError:
            pass
        if self._miss_loader is not None and uri.startswith("skill://"):
            await self._miss_loader(trusted.tenant_id, uri)
            try:
                registered = self._registry.get_resource(trusted.tenant_id, uri)

                async def load_rebuilt() -> tuple[HandsResourceContent, ...]:
                    return registered.contents

                return _ResolvedResource(registered.descriptor, load_rebuilt)
            except KeyError:
                pass
        if self._catalog_store is not None:
            capabilities = await self._catalog_store.list_capabilities(trusted.tenant_id)
            for capability in capabilities:
                if capability.kind not in {
                    CapabilityKind.RESOURCE,
                    CapabilityKind.RESOURCE_TEMPLATE,
                }:
                    continue
                connector = self._connectors.get(capability.server_id)
                descriptor = _resource_descriptor(capability)
                if connector is None or descriptor is None or not _matches_uri(
                    descriptor, uri
                ):
                    continue
                selected_connector = connector

                async def load_remote(
                    connector: CapabilityConnector = selected_connector,
                ) -> tuple[HandsResourceContent, ...]:
                    return await connector.read_resource(trusted, uri)

                return _ResolvedResource(descriptor, load_remote)
        await self._emit(
            "resource.read.not_found",
            1.0,
            trusted.tenant_id,
            scheme=urlsplit(uri).scheme,
        )
        raise KeyError(f"Resource not found: {uri}")

    async def invalidate(self, uri: str, *, tenant_id: str | None = None) -> int:
        async with self._state_lock:
            affected_tenants = {
                key[0]
                for key in (*self._cache.keys(), *self._loads.keys())
                if key[3] == uri and (tenant_id is None or key[0] == tenant_id)
            }
            for affected_tenant in affected_tenants:
                generation_key = (affected_tenant, uri)
                self._generations[generation_key] = (
                    self._generations.get(generation_key, 0) + 1
                )
            keys = [
                key
                for key in self._cache
                if key[3] == uri and (tenant_id is None or key[0] == tenant_id)
            ]
            for key in keys:
                self._cache.pop(key, None)
            return len(keys)

    async def _load(
        self,
        trusted: HandsTrustedContext,
        uri: str,
        resource: _ResolvedResource,
        cache_key: tuple[str, str, str, str, str | None],
        generation: int,
    ) -> tuple[HandsResourceContent, ...]:
        queued_at = time.monotonic()
        acquired = False
        started = False
        try:
            try:
                await asyncio.wait_for(
                    self._capacity.acquire(), timeout=self._queue_timeout_seconds
                )
            except TimeoutError as exc:
                await self._emit(
                    "resource.gateway.backpressure.count",
                    1.0,
                    trusted.tenant_id,
                    reason="queue_timeout",
                )
                raise ResourceBusyError("resource gateway queue wait timed out") from exc
            acquired = True
            async with self._state_lock:
                self._queued -= 1
                self._inflight += 1
                started = True
                queue_depth = self._queued
                inflight = self._inflight
            await self._emit(
                "resource.gateway.queue.latency.seconds",
                time.monotonic() - queued_at,
                trusted.tenant_id,
            )
            await self._emit(
                "resource.gateway.queue.depth", float(queue_depth), trusted.tenant_id
            )
            await self._emit(
                "resource.gateway.in_flight", float(inflight), trusted.tenant_id
            )
            classification = resource.descriptor.classification or "internal"
            policy_decision_id = await self._authorize(
                trusted,
                uri,
                classification=classification,
                media_type=resource.descriptor.mime_type,
            )
            normalized = tuple(
                [
                    await self._normalize(
                        trusted,
                        content,
                        classification=classification,
                        source_revision=resource.descriptor.source_revision,
                        policy_decision_id=policy_decision_id,
                    )
                    for content in await resource.load()
                ]
            )
            async with self._state_lock:
                generation_matches = self._generations.get(
                    (trusted.tenant_id, uri), 0
                ) == generation
                revision_matches = False
                try:
                    current = self._registry.get_resource(trusted.tenant_id, uri)
                    current_revision = (
                        None
                        if current.descriptor.source_revision is None
                        else str(current.descriptor.source_revision)
                    )
                    revision_matches = current_revision == cache_key[4]
                except KeyError:
                    pass
                if (
                    self._cache_ttl_seconds > 0
                    and generation_matches
                    and revision_matches
                ):
                    self._cache[cache_key] = _CacheEntry(
                        expires_at=time.monotonic() + self._cache_ttl_seconds,
                        contents=normalized,
                    )
            return normalized
        finally:
            if acquired:
                self._capacity.release()
            async with self._state_lock:
                if not started:
                    self._queued -= 1
                else:
                    self._inflight -= 1
                current_task = asyncio.current_task()
                if self._loads.get(cache_key) is current_task:
                    self._loads.pop(cache_key, None)
                generation_key = (trusted.tenant_id, uri)
                if not any(
                    key[0] == trusted.tenant_id and key[3] == uri
                    for key in (*self._loads.keys(), *self._cache.keys())
                ):
                    self._generations.pop(generation_key, None)
                queue_depth = self._queued
                inflight = self._inflight
            await self._emit(
                "resource.gateway.queue.depth", float(queue_depth), trusted.tenant_id
            )
            await self._emit(
                "resource.gateway.in_flight", float(inflight), trusted.tenant_id
            )

    async def _emit(
        self,
        name: str,
        value: float,
        tenant_id: str,
        **labels: str,
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                        labels=labels,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return

    async def _authorize(
        self,
        trusted: HandsTrustedContext,
        uri: str,
        *,
        classification: str,
        media_type: str | None,
    ) -> str | None:
        if self._policy is None:
            return None
        evaluation = await self._policy.evaluate_action(
            tenant_id=trusted.tenant_id,
            subject=trusted.runtime_id,
            action="resource.read",
            resource=uri,
            input_digest=hashlib.sha256(uri.encode()).hexdigest(),
            correlation_id=trusted.run_id,
            attributes={
                "permission": "read-only",
                "classification": classification,
                "mime_type": media_type or "application/octet-stream",
                "session_id": trusted.session_id,
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise PolicyDeniedError("Resource policy denied access")
        return evaluation.decision_id

    async def _normalize(
        self,
        trusted: HandsTrustedContext,
        content: HandsResourceContent,
        *,
        classification: str,
        source_revision: object,
        policy_decision_id: str | None,
    ) -> HandsResourceContent:
        media_type = content.mime_type or "application/octet-stream"
        if not _media_type_allowed(media_type, self._allowed_media_types):
            raise PolicyDeniedError("Resource media type is not allowed")
        payload = _content_bytes(content)
        if len(payload) > self._max_resource_bytes:
            raise PolicyDeniedError("Resource exceeds the maximum allowed size")
        findings = self._scanner.scan(payload, media_type=media_type)
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        revision = None if source_revision is None else str(source_revision)
        if len(payload) <= self._max_inline_bytes:
            return content.model_copy(
                update={
                    "mime_type": media_type,
                    "content_digest": digest,
                    "source_revision": revision,
                    "classification": classification,
                    "policy_decision_id": policy_decision_id,
                    "security_findings": tuple(findings),
                    "cache_hit": False,
                    "inline": True,
                }
            )
        artifact = await self._artifacts.put(
            tenant_id=trusted.tenant_id,
            root_session_id=trusted.root_session_id,
            session_id=trusted.session_id,
            content=payload,
            artifact_type="hands-resource",
            media_type=media_type,
            name=_resource_name(content.uri),
            producer="hands-resource-gateway",
            classification=classification,
            acl=(trusted.runtime_id,),
        )
        artifact_payload = json.dumps(
            {"artifact_ref": artifact.as_dict()},
            separators=(",", ":"),
        )
        return HandsResourceContent(
            uri=content.uri,
            mime_type="application/vnd.auraclaw.artifact-ref+json",
            text=artifact_payload,
            artifact_ref=artifact,
            content_digest=digest,
            source_revision=revision,
            classification=classification,
            policy_decision_id=policy_decision_id,
            security_findings=tuple(findings),
            cache_hit=False,
            inline=False,
        )


def _content_bytes(content: HandsResourceContent) -> bytes:
    if content.text is not None:
        return content.text.encode()
    assert content.blob is not None
    try:
        return base64.b64decode(content.blob, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SchemaValidationError("Resource blob is not valid base64") from exc


def _media_type_allowed(media_type: str, allowed: tuple[str, ...]) -> bool:
    normalized = media_type.split(";", 1)[0].strip().lower()
    structured_json = (
        normalized.startswith("application/") and normalized.endswith("+json")
    )
    return any(
        candidate.lower() == normalized
        or (
            candidate.endswith("/*")
            and normalized.startswith(candidate.removesuffix("*").lower())
        )
        or (candidate.lower() == "application/json" and structured_json)
        for candidate in allowed
    )


def _resource_descriptor(
    capability: CapabilityDescriptor,
) -> HandsResourceDescriptor | None:
    source = capability.metadata.get("source")
    if not isinstance(source, dict):
        return None
    try:
        return HandsResourceDescriptor.model_validate(source)
    except ValueError:
        return None


def _matches_uri(descriptor: HandsResourceDescriptor, uri: str) -> bool:
    if descriptor.uri == uri:
        return True
    if descriptor.uri_template is None:
        return False
    escaped = re.escape(descriptor.uri_template)
    pattern = re.sub(r"\\\{[A-Za-z0-9_.-]+\\\}", r"[^/]+", escaped)
    return re.fullmatch(pattern, uri) is not None


def _resource_name(uri: str) -> str:
    path = urlsplit(uri).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or "resource"


def _with_cache_hit(
    content: HandsResourceContent,
    cache_hit: bool,
) -> HandsResourceContent:
    return content.model_copy(update={"cache_hit": cache_hit})
