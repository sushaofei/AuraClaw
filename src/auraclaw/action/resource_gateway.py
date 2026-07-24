from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlsplit

from auraclaw.action.mcp_primitives import McpResourceRegistry
from auraclaw.action.ports import ArtifactWriter, ResourcePolicyEvaluator
from auraclaw.contracts.errors import PolicyDeniedError, SchemaValidationError
from auraclaw.contracts.mcp import McpResourceContent, McpTrustedContext
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


class DefaultResourceContentScanner:
    def scan(self, content: bytes, *, media_type: str) -> tuple[str, ...]:
        if media_type.startswith("text/") or media_type in {
            "application/json",
            "application/xml",
            "application/yaml",
        }:
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
    contents: tuple[McpResourceContent, ...]


class ManagedResourceGateway:
    def __init__(
        self,
        registry: McpResourceRegistry,
        *,
        artifacts: ArtifactWriter,
        policy: ResourcePolicyEvaluator | None = None,
        scanner: ResourceContentScanner | None = None,
        allowed_schemes: tuple[str, ...] = ("artifact", "memory", "repo", "skill"),
        allowed_media_types: tuple[str, ...] = (
            "text/*",
            "application/json",
            "application/xml",
            "application/yaml",
            "application/octet-stream",
        ),
        max_inline_bytes: int = 64 * 1024,
        max_resource_bytes: int = 8 * 1024 * 1024,
        cache_ttl_seconds: float = 30.0,
    ) -> None:
        if max_inline_bytes < 1 or max_resource_bytes < max_inline_bytes:
            raise ValueError("Resource size limits are invalid")
        if cache_ttl_seconds < 0:
            raise ValueError("Resource cache TTL cannot be negative")
        self._registry = registry
        self._artifacts = artifacts
        self._policy = policy
        self._scanner = scanner or DefaultResourceContentScanner()
        self._allowed_schemes = frozenset(allowed_schemes)
        self._allowed_media_types = allowed_media_types
        self._max_inline_bytes = max_inline_bytes
        self._max_resource_bytes = max_resource_bytes
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[
            tuple[str, str, str, str], _CacheEntry
        ] = {}
        self._lock = asyncio.Lock()

    async def read(
        self,
        trusted_context: McpTrustedContext,
        uri: str,
    ) -> tuple[McpResourceContent, ...]:
        parsed = urlsplit(uri)
        if not parsed.scheme or parsed.scheme not in self._allowed_schemes:
            raise PolicyDeniedError("Resource URI scheme is not allowed")
        self._registry.get_resource(trusted_context.tenant_id, uri)
        cache_key = (
            trusted_context.tenant_id,
            trusted_context.root_session_id,
            trusted_context.session_id,
            uri,
        )
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and cached.expires_at > now:
            return tuple(_with_cache_hit(content, True) for content in cached.contents)

        async with self._lock:
            resource = self._registry.get_resource(trusted_context.tenant_id, uri)
            cached = self._cache.get(cache_key)
            if cached is not None and cached.expires_at > time.monotonic():
                return tuple(_with_cache_hit(content, True) for content in cached.contents)
            classification = str(
                _auraclaw_meta(resource.descriptor.meta).get(
                    "classification", "internal"
                )
            )
            source_revision = _auraclaw_meta(resource.descriptor.meta).get(
                "sourceRevision"
            )
            policy_decision_id = await self._authorize(
                trusted_context,
                uri,
                classification=classification,
                media_type=resource.descriptor.mime_type,
            )
            normalized: tuple[McpResourceContent, ...] = tuple(
                [
                    await self._normalize(
                        trusted_context,
                        content,
                        classification=classification,
                        source_revision=source_revision,
                        policy_decision_id=policy_decision_id,
                    )
                    for content in resource.contents
                ]
            )
            if self._cache_ttl_seconds > 0:
                self._cache[cache_key] = _CacheEntry(
                    expires_at=time.monotonic() + self._cache_ttl_seconds,
                    contents=normalized,
                )
            return normalized

    async def invalidate(self, uri: str, *, tenant_id: str | None = None) -> int:
        async with self._lock:
            keys = [
                key
                for key in self._cache
                if key[3] == uri and (tenant_id is None or key[0] == tenant_id)
            ]
            for key in keys:
                self._cache.pop(key, None)
            return len(keys)

    async def _authorize(
        self,
        trusted: McpTrustedContext,
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
        trusted: McpTrustedContext,
        content: McpResourceContent,
        *,
        classification: str,
        source_revision: object,
        policy_decision_id: str | None,
    ) -> McpResourceContent:
        media_type = content.mime_type or "application/octet-stream"
        if not _media_type_allowed(media_type, self._allowed_media_types):
            raise PolicyDeniedError("Resource media type is not allowed")
        payload = _content_bytes(content)
        if len(payload) > self._max_resource_bytes:
            raise PolicyDeniedError("Resource exceeds the maximum allowed size")
        findings = self._scanner.scan(payload, media_type=media_type)
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        metadata = _merge_metadata(
            content.meta,
            {
                "contentDigest": digest,
                "sourceRevision": source_revision,
                "classification": classification,
                "retrievedAt": datetime.now(UTC).isoformat(),
                "securityFindings": list(findings),
                "policyDecisionId": policy_decision_id,
                "cacheHit": False,
            },
        )
        if len(payload) <= self._max_inline_bytes:
            return content.model_copy(update={"mime_type": media_type, "meta": metadata})
        artifact = await self._artifacts.put(
            tenant_id=trusted.tenant_id,
            root_session_id=trusted.root_session_id,
            session_id=trusted.session_id,
            content=payload,
            artifact_type="mcp-resource",
            media_type=media_type,
            name=_resource_name(content.uri),
            producer="mcp-resource-gateway",
            classification=classification,
            acl=(trusted.runtime_id,),
        )
        artifact_payload = json.dumps(
            {"artifact_ref": artifact.as_dict()},
            separators=(",", ":"),
        )
        metadata = _merge_metadata(
            metadata,
            {"artifactRef": artifact.as_dict(), "inline": False},
        )
        return McpResourceContent(
            uri=content.uri,
            mime_type="application/vnd.auraclaw.artifact-ref+json",
            text=artifact_payload,
            annotations=content.annotations,
            meta=metadata,
        )


def _content_bytes(content: McpResourceContent) -> bytes:
    if content.text is not None:
        return content.text.encode()
    assert content.blob is not None
    try:
        return base64.b64decode(content.blob, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SchemaValidationError("Resource blob is not valid base64") from exc


def _media_type_allowed(media_type: str, allowed: tuple[str, ...]) -> bool:
    return any(
        candidate == media_type
        or candidate.endswith("/*")
        and media_type.startswith(candidate.removesuffix("*"))
        for candidate in allowed
    )


def _resource_name(uri: str) -> str:
    path = urlsplit(uri).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or "resource"


def _auraclaw_meta(meta: dict[str, object]) -> dict[str, object]:
    value = meta.get("auraclaw", {})
    return dict(value) if isinstance(value, dict) else {}


def _merge_metadata(
    meta: dict[str, object],
    values: dict[str, object],
) -> dict[str, object]:
    result = dict(meta)
    auraclaw = _auraclaw_meta(result)
    auraclaw.update({key: value for key, value in values.items() if value is not None})
    result["auraclaw"] = auraclaw
    return result


def _with_cache_hit(
    content: McpResourceContent,
    cache_hit: bool,
) -> McpResourceContent:
    return content.model_copy(
        update={"meta": _merge_metadata(content.meta, {"cacheHit": cache_hit})}
    )
