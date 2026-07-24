from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Protocol

from auraclaw.action.mcp_primitives import McpResourceRegistry, RegisteredResource
from auraclaw.action.ports import (
    ArtifactWriter,
    CapabilityCatalogStore,
    ResourcePolicyEvaluator,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
)
from auraclaw.contracts.errors import (
    NotFoundError,
    PolicyDeniedError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.mcp import McpResourceContent, McpResourceDescriptor
from auraclaw.contracts.skills import (
    PublishedSkill,
    ResolvedSkillResource,
    ResolvedSkillTool,
    SkillBinding,
    SkillManifest,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import PolicyDecision

_VERSION_CLAUSE = re.compile(
    r"^(>=|<=|>|<|==|=)?(0|[1-9]\d*)"
    r"(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?$"
)


@dataclass(frozen=True)
class SkillPackage:
    manifest: SkillManifest
    files: Mapping[str, bytes]

    @classmethod
    def from_files(cls, files: Mapping[str, bytes]) -> SkillPackage:
        manifest_bytes = files.get("manifest.json")
        if manifest_bytes is None:
            raise SchemaValidationError("Skill package is missing manifest.json")
        try:
            manifest = SkillManifest.model_validate_json(manifest_bytes)
        except (ValueError, UnicodeDecodeError) as exc:
            raise SchemaValidationError("Skill manifest is invalid") from exc
        return cls(manifest=manifest, files=dict(files))


class SkillSignatureVerifier(Protocol):
    def verify(self, package: SkillPackage) -> bool: ...


class HmacSkillSignatureVerifier:
    def __init__(self, publisher_keys: Mapping[str, bytes]) -> None:
        self._publisher_keys = {publisher: bytes(key) for publisher, key in publisher_keys.items()}
        if any(len(key) < 16 for key in self._publisher_keys.values()):
            raise ValueError("Skill publisher keys must contain at least 16 bytes")

    def verify(self, package: SkillPackage) -> bool:
        key = self._publisher_keys.get(package.manifest.publisher)
        if key is None:
            return False
        expected = hmac.new(key, skill_signing_payload(package), hashlib.sha256).hexdigest()
        return hmac.compare_digest(package.manifest.signature, f"hmac-sha256:{expected}")

    def sign(self, manifest: SkillManifest, files: Mapping[str, bytes]) -> str:
        key = self._publisher_keys.get(manifest.publisher)
        if key is None:
            raise KeyError(f"Skill publisher key not found: {manifest.publisher}")
        unsigned = manifest.model_copy(update={"signature": f"hmac-sha256:{'0' * 64}"})
        package = SkillPackage(manifest=unsigned, files=dict(files))
        digest = hmac.new(key, skill_signing_payload(package), hashlib.sha256).hexdigest()
        return f"hmac-sha256:{digest}"


class SkillPackageRegistry:
    def __init__(
        self,
        *,
        artifacts: ArtifactWriter,
        signature_verifier: SkillSignatureVerifier,
        resources: McpResourceRegistry | None = None,
        max_package_bytes: int = 16 * 1024 * 1024,
        max_files: int = 512,
    ) -> None:
        self._artifacts = artifacts
        self._signature_verifier = signature_verifier
        self._resources = resources
        self._max_package_bytes = max_package_bytes
        self._max_files = max_files
        self._packages: dict[tuple[str, str, str, str], SkillPackage] = {}
        self._publications: dict[tuple[str, str, str, str], PublishedSkill] = {}

    async def publish(self, tenant_id: str, package: SkillPackage) -> PublishedSkill:
        normalized = _validate_package(package, self._max_package_bytes, self._max_files)
        if not self._signature_verifier.verify(normalized):
            raise PolicyDeniedError("Skill package signature is invalid")
        key = _package_key(tenant_id, normalized.manifest)
        digest = skill_package_digest(normalized)
        existing = self._publications.get(key)
        if existing is not None:
            if existing.package_digest != digest:
                raise VersionConflictError("Skill version is immutable")
            if existing.status == SkillPublicationStatus.REVOKED:
                reactivated = existing.model_copy(
                    update={"status": SkillPublicationStatus.ACTIVE}
                )
                self._publications[key] = reactivated
                self._packages[key] = normalized
                if self._resources is not None:
                    for resource in _package_resources(
                        tenant_id,
                        normalized,
                        digest,
                    ):
                        self._resources.register_resource(resource)
                return reactivated
            return existing
        archive = _package_archive(normalized)
        artifact_ref = await self._artifacts.put(
            tenant_id=tenant_id,
            root_session_id="skill-registry",
            session_id="skill-registry",
            content=archive,
            artifact_type="skill-package",
            media_type="application/vnd.auraclaw.skill-package+json",
            name=(
                f"{normalized.manifest.publisher}."
                f"{normalized.manifest.name}-{normalized.manifest.version}"
            ),
            producer="skill-registry",
            classification=normalized.manifest.data_classification,
        )
        publication = PublishedSkill(
            tenant_id=tenant_id,
            manifest=normalized.manifest,
            package_digest=digest,
            artifact_ref=artifact_ref,
        )
        self._packages[key] = normalized
        self._publications[key] = publication
        if self._resources is not None:
            for resource in _package_resources(tenant_id, normalized, digest):
                self._resources.register_resource(resource)
        return publication

    def revoke(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> PublishedSkill:
        key = (tenant_id, publisher, name, version)
        publication = self._publications.get(key)
        if publication is None:
            raise NotFoundError("Skill publication not found")
        revoked = publication.model_copy(update={"status": SkillPublicationStatus.REVOKED})
        self._publications[key] = revoked
        if self._resources is not None:
            package = self._packages[key]
            for resource in _package_resources(
                tenant_id,
                package,
                publication.package_digest,
            ):
                self._resources.unregister_resource(resource.descriptor.uri)
        return revoked

    def candidates(
        self,
        tenant_id: str,
        name: str,
        *,
        publisher: str | None = None,
    ) -> tuple[PublishedSkill, ...]:
        values = [
            publication
            for (
                candidate_tenant,
                candidate_publisher,
                candidate_name,
                _version,
            ), publication in self._publications.items()
            if candidate_tenant == tenant_id
            and candidate_name == name
            and (publisher is None or candidate_publisher == publisher)
            and publication.status == SkillPublicationStatus.ACTIVE
        ]
        return tuple(
            sorted(
                values,
                key=lambda item: _semver(item.manifest.version),
                reverse=True,
            )
        )

    def load_part(
        self,
        tenant_id: str,
        *,
        publisher: str,
        name: str,
        version: str,
        package_digest: str,
        path: str,
    ) -> bytes:
        key = (tenant_id, publisher, name, version)
        publication = self._publications.get(key)
        package = self._packages.get(key)
        if publication is None or package is None:
            raise NotFoundError("Skill package not found")
        if publication.status != SkillPublicationStatus.ACTIVE:
            raise PolicyDeniedError("Skill package is revoked")
        if publication.package_digest != package_digest:
            raise VersionConflictError("Skill package digest does not match the binding")
        normalized_path = _safe_path(path)
        try:
            return package.files[normalized_path]
        except KeyError as exc:
            raise NotFoundError(f"Skill package part not found: {path}") from exc


class SkillResolver:
    def __init__(
        self,
        registry: SkillPackageRegistry,
        catalog: CapabilityCatalogStore,
        policy: ResourcePolicyEvaluator | None = None,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._policy = policy

    async def resolve(
        self,
        *,
        tenant_id: str,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        role: str,
        policy_version: str,
        subject: str = "agent-runtime",
        correlation_id: str = "skill.resolve",
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding:
        publication = next(
            (
                candidate
                for candidate in self._registry.candidates(tenant_id, name, publisher=publisher)
                if version_satisfies(candidate.manifest.version, version)
            ),
            None,
        )
        if publication is None:
            raise NotFoundError("No active Skill version satisfies the request")
        manifest = publication.manifest
        if role not in manifest.allowed_roles:
            raise PolicyDeniedError("Runtime role is not allowed to activate Skill")
        capabilities = tuple(
            capability
            for capability in await self._catalog.list_capabilities(tenant_id)
            if capability.status in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
        )
        resolved_tools = tuple(
            _resolve_tool(requirement.name, requirement.version, capabilities)
            for requirement in manifest.required_tools
        )
        resolved_resources = tuple(
            _resolve_resource(requirement.uri_template, capabilities)
            for requirement in manifest.required_resources
        )
        policy_decision_id: str | None = None
        if self._policy is not None:
            evaluation = await self._policy.evaluate_action(
                tenant_id=tenant_id,
                subject=subject,
                action="skill.activate",
                resource=(
                    f"skill:{manifest.publisher}/{manifest.name}/"
                    f"{manifest.version}"
                ),
                input_digest=publication.package_digest.removeprefix("sha256:"),
                correlation_id=correlation_id,
                attributes={
                    "active_skill_names": list(active_skill_names),
                    "classification": manifest.data_classification,
                    "required_resources": [
                        item.uri_template
                        for item in manifest.required_resources
                    ],
                    "required_tools": [
                        item.name for item in manifest.required_tools
                    ],
                    "risk_level": manifest.risk_level,
                    "role": role,
                },
            )
            if evaluation.decision not in {
                PolicyDecision.ALLOW,
                PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            }:
                raise PolicyDeniedError("Skill activation policy denied binding")
            policy_version = evaluation.policy_version
            policy_decision_id = evaluation.decision_id
        return SkillBinding(
            skill_name=manifest.name,
            skill_version=manifest.version,
            publisher=manifest.publisher,
            package_digest=publication.package_digest,
            artifact_ref=publication.artifact_ref,
            resolved_tools=resolved_tools,
            resolved_resources=resolved_resources,
            policy_version=policy_version,
            policy_decision_id=policy_decision_id,
            max_steps=manifest.max_steps,
            timeout_seconds=manifest.timeout_seconds,
        )


def skill_signing_payload(package: SkillPackage) -> bytes:
    manifest = package.manifest.model_dump(mode="json")
    manifest["signature"] = None
    file_digests = {
        path: hashlib.sha256(content).hexdigest()
        for path, content in sorted(package.files.items())
        if path != "manifest.json"
    }
    return json.dumps(
        {"files": file_digests, "manifest": manifest},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def skill_package_digest(package: SkillPackage) -> str:
    return f"sha256:{hashlib.sha256(_package_archive(package)).hexdigest()}"


def version_satisfies(version: str, constraint: str) -> bool:
    if constraint.strip() in {"", "*"}:
        return True
    current = _semver(version)
    for raw_clause in constraint.split(","):
        clause = raw_clause.strip()
        match = _VERSION_CLAUSE.fullmatch(clause)
        if match is None:
            raise SchemaValidationError(f"Unsupported version constraint: {clause}")
        operator = match.group(1) or "="
        requested = tuple(int(value or 0) for value in match.groups()[1:])
        if operator in {"=", "=="} and current != requested:
            return False
        if operator == ">=" and current < requested:
            return False
        if operator == "<=" and current > requested:
            return False
        if operator == ">" and current <= requested:
            return False
        if operator == "<" and current >= requested:
            return False
    return True


def _validate_package(
    package: SkillPackage,
    max_package_bytes: int,
    max_files: int,
) -> SkillPackage:
    files: dict[str, bytes] = {}
    for path, content in package.files.items():
        normalized_path = _safe_path(path)
        if normalized_path != path:
            raise SchemaValidationError(f"Skill package path is not canonical: {path}")
        if normalized_path not in {"manifest.json", "SKILL.md"} and (
            PurePosixPath(normalized_path).parts[0]
            not in {"references", "assets", "tests"}
        ):
            raise SchemaValidationError(
                f"Skill package path is outside allowed directories: {path}"
            )
        files[normalized_path] = bytes(content)
    if len(files) > max_files:
        raise PolicyDeniedError("Skill package contains too many files")
    if sum(len(content) for content in files.values()) > max_package_bytes:
        raise PolicyDeniedError("Skill package exceeds the maximum allowed size")
    if "manifest.json" not in files or "SKILL.md" not in files:
        raise SchemaValidationError("Skill package requires manifest.json and SKILL.md")
    for path, content in files.items():
        if (
            path in {"manifest.json", "SKILL.md"}
            or path.endswith(".md")
            or path.endswith(".json")
        ):
            try:
                content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise SchemaValidationError(
                    f"Skill text file is not valid UTF-8: {path}"
                ) from exc
    parsed = SkillPackage.from_files(files)
    if parsed.manifest != package.manifest:
        raise SchemaValidationError("Skill manifest does not match manifest.json")
    return parsed


def _safe_path(path: str) -> str:
    candidate = PurePosixPath(path)
    normalized = str(candidate)
    if (
        not path
        or candidate.is_absolute()
        or normalized in {".", ".."}
        or ".." in candidate.parts
        or "\\" in path
    ):
        raise SchemaValidationError(f"Unsafe Skill package path: {path}")
    return normalized


def _package_archive(package: SkillPackage) -> bytes:
    payload = {
        "files": {
            path: base64.b64encode(content).decode()
            for path, content in sorted(package.files.items())
        }
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _package_key(tenant_id: str, manifest: SkillManifest) -> tuple[str, str, str, str]:
    return tenant_id, manifest.publisher, manifest.name, manifest.version


def _package_resources(
    tenant_id: str,
    package: SkillPackage,
    package_digest: str,
) -> tuple[RegisteredResource, ...]:
    prefix = (
        f"skill://{package.manifest.publisher}/{package.manifest.name}/{package.manifest.version}"
    )
    aliases = {"manifest.json": "manifest"}
    return tuple(
        RegisteredResource(
            descriptor=McpResourceDescriptor(
                uri=f"{prefix}/{aliases.get(path, path)}",
                name=path,
                mime_type=(
                    "application/json"
                    if path.endswith(".json")
                    else "text/markdown"
                    if path.endswith(".md")
                    else "application/octet-stream"
                ),
                size=len(content),
                meta={
                    "auraclaw": {
                        "classification": package.manifest.data_classification,
                        "packageDigest": package_digest,
                        "sourceRevision": package.manifest.version,
                    }
                },
            ),
            contents=(
                McpResourceContent(
                    uri=f"{prefix}/{aliases.get(path, path)}",
                    mime_type=(
                        "application/json"
                        if path.endswith(".json")
                        else "text/markdown"
                        if path.endswith(".md")
                        else "application/octet-stream"
                    ),
                    **(
                        {"text": content.decode()}
                        if path.endswith(".json") or path.endswith(".md")
                        else {"blob": base64.b64encode(content).decode()}
                    ),
                ),
            ),
            tenant_ids=(tenant_id,),
        )
        for path, content in sorted(package.files.items())
    )


def _resolve_tool(
    name: str,
    constraint: str,
    capabilities: tuple[CapabilityDescriptor, ...],
) -> ResolvedSkillTool:
    candidates = [
        capability
        for capability in capabilities
        if capability.kind == CapabilityKind.TOOL
        and capability.canonical_name == name
        and version_satisfies(capability.version, constraint)
    ]
    if not candidates:
        raise NotFoundError(f"Skill Tool dependency is unavailable: {name} {constraint}")
    selected = max(candidates, key=lambda item: _semver(item.version))
    return ResolvedSkillTool(
        capability_id=selected.capability_id,
        canonical_name=selected.canonical_name,
        version=selected.version,
        schema_digest=selected.content_digest,
    )


def _resolve_resource(
    uri_template: str,
    capabilities: tuple[CapabilityDescriptor, ...],
) -> ResolvedSkillResource:
    candidates = [
        capability
        for capability in capabilities
        if capability.kind in {CapabilityKind.RESOURCE, CapabilityKind.RESOURCE_TEMPLATE}
        and (
            capability.metadata.get("uri_template") == uri_template
            or capability.canonical_name == uri_template
        )
    ]
    if not candidates:
        raise NotFoundError(f"Skill Resource dependency is unavailable: {uri_template}")
    selected = max(candidates, key=lambda item: _semver(item.version))
    return ResolvedSkillResource(
        capability_id=selected.capability_id,
        server_id=selected.server_id,
        uri_template=uri_template,
        content_digest=selected.content_digest,
    )


def _semver(value: str) -> tuple[int, int, int]:
    core = value.split("-", 1)[0]
    parts = core.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise SchemaValidationError(f"Capability version is not semantic: {value}")
    return tuple(int(part) for part in parts)  # type: ignore[return-value]
