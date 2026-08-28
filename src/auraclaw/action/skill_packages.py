from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Protocol, TypeVar

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

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
    CapabilityTrustLevel,
)
from auraclaw.contracts.errors import (
    NotFoundError,
    PolicyDeniedError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.hands import HandsResourceContent, HandsResourceDescriptor
from auraclaw.contracts.skills import (
    PublishedSkill,
    ResolvedSkillDependency,
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


_CapabilityDependency = TypeVar("_CapabilityDependency")


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


class Ed25519SkillSignatureVerifier:
    """Verify an offline-signed package against an explicitly trusted public key."""

    def __init__(self, publisher_keys: Mapping[tuple[str, str], bytes]) -> None:
        self._publisher_keys: dict[tuple[str, str], Ed25519PublicKey] = {}
        for identity, value in publisher_keys.items():
            if len(value) != 32:
                raise ValueError("Ed25519 public keys must contain exactly 32 bytes")
            self._publisher_keys[identity] = Ed25519PublicKey.from_public_bytes(value)

    def verify(self, package: SkillPackage) -> bool:
        manifest = package.manifest
        if manifest.signature_key_id is None or not manifest.signature.startswith(
            "ed25519:"
        ):
            return False
        key = self._publisher_keys.get(
            (manifest.publisher, manifest.signature_key_id)
        )
        if key is None:
            return False
        try:
            encoded = manifest.signature.removeprefix("ed25519:")
            signature = base64.b64decode(
                encoded + "=" * (-len(encoded) % 4),
                altchars=b"-_",
                validate=True,
            )
            if len(signature) != 64:
                return False
            key.verify(signature, skill_signing_payload(package))
        except (InvalidSignature, ValueError, binascii.Error):
            return False
        return True


class SkillPackageRegistry:
    def __init__(
        self,
        *,
        artifacts: ArtifactWriter,
        signature_verifier: SkillSignatureVerifier,
        resources: McpResourceRegistry | None = None,
        max_package_bytes: int = 16 * 1024 * 1024,
        max_files: int = 512,
        package_retention: timedelta = timedelta(days=90),
    ) -> None:
        self._artifacts = artifacts
        self._signature_verifier = signature_verifier
        self._resources = resources
        self._max_package_bytes = max_package_bytes
        self._max_files = max_files
        self._package_retention = package_retention
        self._packages: dict[tuple[str, str, str, str], SkillPackage] = {}
        self._publications: dict[tuple[str, str, str, str], PublishedSkill] = {}
        self._discoverable: set[tuple[str, str, str, str]] = set()

    @property
    def resources(self) -> McpResourceRegistry | None:
        return self._resources

    def validate(self, package: SkillPackage) -> SkillPackage:
        normalized = self.validate_content(package)
        if not self._signature_verifier.verify(normalized):
            raise PolicyDeniedError("Skill package signature is invalid")
        return normalized

    def validate_content(self, package: SkillPackage) -> SkillPackage:
        """Validate package structure after a caller performed governed signature checks."""
        normalized = _validate_package(
            package, self._max_package_bytes, self._max_files
        )
        validate_skill_test_vectors(normalized)
        return normalized

    def restore(
        self,
        tenant_id: str,
        package: SkillPackage,
        publication: PublishedSkill,
        *,
        signature_verified: bool = False,
    ) -> PublishedSkill:
        """Restore validated persisted state without creating another Artifact."""
        normalized = (
            self.validate_content(package)
            if signature_verified
            else self.validate(package)
        )
        if publication.tenant_id != tenant_id:
            raise PolicyDeniedError("Skill publication tenant does not match")
        if publication.manifest != normalized.manifest:
            raise VersionConflictError("Skill publication manifest mismatch")
        if publication.package_digest != skill_package_digest(normalized):
            raise VersionConflictError("Skill publication package digest mismatch")
        key = _package_key(tenant_id, normalized.manifest)
        current = self._publications.get(key)
        if current == publication:
            if publication.status is SkillPublicationStatus.ACTIVE:
                self._discoverable.add(key)
            return current
        self._packages[key] = normalized
        self._publications[key] = publication
        if publication.status is SkillPublicationStatus.ACTIVE:
            self._discoverable.add(key)
        if (
            self._resources is not None
            and publication.status is SkillPublicationStatus.ACTIVE
        ):
            for resource in _package_resources(
                tenant_id, normalized, publication.package_digest
            ):
                uri = resource.descriptor.uri
                if uri is not None:
                    self._resources.unregister_resource(uri)
                self._resources.register_resource(resource)
        return publication

    def replace_tenant(
        self,
        tenant_id: str,
        entries: tuple[tuple[SkillPackage, PublishedSkill], ...],
        *,
        discoverable: frozenset[tuple[str, str, str]] | None = None,
        signatures_verified: bool = False,
    ) -> None:
        normalized_entries: list[tuple[SkillPackage, PublishedSkill]] = []
        for package, publication in entries:
            normalized = (
                self.validate_content(package)
                if signatures_verified
                else self.validate(package)
            )
            if publication.tenant_id != tenant_id:
                raise PolicyDeniedError("Skill publication tenant does not match")
            if publication.manifest != normalized.manifest:
                raise VersionConflictError("Skill publication manifest mismatch")
            if publication.package_digest != skill_package_digest(normalized):
                raise VersionConflictError("Skill publication package digest mismatch")
            normalized_entries.append((normalized, publication))
        old_keys = [key for key in self._packages if key[0] == tenant_id]
        if self._resources is not None:
            for key in old_keys:
                old_package = self._packages[key]
                old_publication = self._publications[key]
                for resource in _package_resources(
                    tenant_id, old_package, old_publication.package_digest
                ):
                    uri = resource.descriptor.uri
                    if uri is not None:
                        self._resources.unregister_resource(uri)
        for key in old_keys:
            self._packages.pop(key, None)
            self._publications.pop(key, None)
            self._discoverable.discard(key)
        for package, publication in normalized_entries:
            key = _package_key(tenant_id, package.manifest)
            self._packages[key] = package
            self._publications[key] = publication
            is_discoverable = (
                publication.status is SkillPublicationStatus.ACTIVE
                and (
                    discoverable is None
                    or key[1:] in discoverable
                )
            )
            if is_discoverable:
                self._discoverable.add(key)
            if (
                self._resources is not None
                and is_discoverable
            ):
                for resource in _package_resources(
                    tenant_id, package, publication.package_digest
                ):
                    self._resources.register_resource(resource)

    async def publish(
        self,
        tenant_id: str,
        package: SkillPackage,
        *,
        status: SkillPublicationStatus = SkillPublicationStatus.ACTIVE,
        signature_verified: bool = False,
    ) -> PublishedSkill:
        if status not in {
            SkillPublicationStatus.STAGED,
            SkillPublicationStatus.ACTIVE,
        }:
            raise ValueError("New Skill packages can only be staged or activated")
        normalized = (
            self.validate_content(package)
            if signature_verified
            else self.validate(package)
        )
        key = _package_key(tenant_id, normalized.manifest)
        digest = skill_package_digest(normalized)
        existing = self._publications.get(key)
        if existing is not None:
            if existing.package_digest != digest:
                raise VersionConflictError("Skill version is immutable")
            if (
                status is SkillPublicationStatus.ACTIVE
                and existing.status is SkillPublicationStatus.STAGED
            ):
                reactivated = existing.model_copy(
                    update={"status": SkillPublicationStatus.ACTIVE}
                )
                self._publications[key] = reactivated
                self._packages[key] = normalized
                self._discoverable.add(key)
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
            retention_until=datetime.now(UTC) + self._package_retention,
        )
        publication = PublishedSkill(
            tenant_id=tenant_id,
            manifest=normalized.manifest,
            package_digest=digest,
            artifact_ref=artifact_ref,
            status=status,
        )
        self._packages[key] = normalized
        self._publications[key] = publication
        if status is SkillPublicationStatus.ACTIVE:
            self._discoverable.add(key)
        if (
            self._resources is not None
            and status is SkillPublicationStatus.ACTIVE
        ):
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
        self._discoverable.discard(key)
        if self._resources is not None:
            package = self._packages[key]
            for resource in _package_resources(
                tenant_id,
                package,
                publication.package_digest,
            ):
                uri = resource.descriptor.uri
                if uri is not None:
                    self._resources.unregister_resource(uri)
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
            and (
                candidate_tenant,
                candidate_publisher,
                candidate_name,
                _version,
            )
            in self._discoverable
        ]
        return tuple(
            sorted(
                values,
                key=lambda item: _semver(item.manifest.version),
                reverse=True,
            )
        )

    def capability_descriptors(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            skill_capability_descriptor(publication)
            for publication in sorted(
                self._publications.values(),
                key=lambda item: (
                    item.manifest.name,
                    _semver(item.manifest.version),
                ),
            )
            if publication.tenant_id == tenant_id
            and publication.status == SkillPublicationStatus.ACTIVE
            and _package_key(tenant_id, publication.manifest) in self._discoverable
        )

    def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None:
        return next(
            (
                descriptor
                for descriptor in self.capability_descriptors(tenant_id)
                if descriptor.capability_id == capability_id
            ),
            None,
        )

    def set_skill_discoverable(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        *,
        discoverable: bool,
    ) -> None:
        keys = [
            key
            for key in self._publications
            if key[0] == tenant_id and key[1] == publisher and key[2] == name
        ]
        if not keys:
            raise NotFoundError("Skill publication not found")
        for key in keys:
            publication = self._publications[key]
            package = self._packages[key]
            should_expose = (
                discoverable
                and publication.status is SkillPublicationStatus.ACTIVE
            )
            if should_expose:
                self._discoverable.add(key)
            else:
                self._discoverable.discard(key)
            if self._resources is None:
                continue
            for resource in _package_resources(
                tenant_id,
                package,
                publication.package_digest,
            ):
                uri = resource.descriptor.uri
                if uri is None:
                    continue
                self._resources.unregister_resource(uri)
                if should_expose:
                    self._resources.register_resource(resource)

    def list_publications(self, tenant_id: str) -> tuple[PublishedSkill, ...]:
        return tuple(
            sorted(
                (
                    publication
                    for publication in self._publications.values()
                    if publication.tenant_id == tenant_id
                ),
                key=lambda item: (
                    item.manifest.publisher,
                    item.manifest.name,
                    _semver(item.manifest.version),
                ),
                reverse=True,
            )
        )

    def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str | None = None,
    ) -> PublishedSkill:
        matches = [
            publication
            for publication in self.list_publications(tenant_id)
            if publication.manifest.publisher == publisher
            and publication.manifest.name == name
            and (version is None or publication.manifest.version == version)
        ]
        if not matches:
            raise NotFoundError("Skill publication not found")
        if version is None:
            return max(matches, key=lambda item: _semver(item.manifest.version))
        return matches[0]

    def skill_markdown(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> str | None:
        key = (tenant_id, publisher, name, version)
        package = self._packages.get(key)
        if package is None:
            raise NotFoundError("Skill package not found")
        raw = package.files.get("SKILL.md")
        if raw is None:
            return None
        return raw.decode()

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
        if publication.status not in {
            SkillPublicationStatus.ACTIVE,
            SkillPublicationStatus.RESTORING,
            SkillPublicationStatus.RETIRED,
        }:
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
        _dependency_path: tuple[str, ...] = (),
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
        if manifest.name in _dependency_path:
            path = " -> ".join((*_dependency_path, manifest.name))
            raise SchemaValidationError(f"Skill dependency cycle detected: {path}")
        if role not in manifest.allowed_roles:
            raise PolicyDeniedError("Runtime role is not allowed to activate Skill")
        capabilities = tuple(
            capability
            for capability in await self._catalog.list_capabilities(tenant_id)
            if capability.status in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
        )
        own_tools = tuple(
            _resolve_tool(requirement.name, requirement.version, capabilities)
            for requirement in manifest.required_tools
        )
        own_resources = tuple(
            _resolve_resource(requirement.uri_template, capabilities)
            for requirement in manifest.required_resources
        )
        child_bindings = tuple(
            [
                await self.resolve(
                    tenant_id=tenant_id,
                    name=requirement.name,
                    version=requirement.version,
                    publisher=requirement.publisher,
                    role=role,
                    policy_version=policy_version,
                    subject=subject,
                    correlation_id=correlation_id,
                    active_skill_names=tuple(
                        dict.fromkeys(
                            (
                                *active_skill_names,
                                *_dependency_path,
                                manifest.name,
                            )
                        )
                    ),
                    _dependency_path=(*_dependency_path, manifest.name),
                )
                for requirement in manifest.required_skills
            ]
        )
        resolved_tools = _unique_by_capability_id(
            (
                *own_tools,
                *(
                    tool
                    for binding in child_bindings
                    for tool in binding.resolved_tools
                ),
            ),
            key=lambda item: item.capability_id,
        )
        resolved_resources = _unique_by_capability_id(
            (
                *own_resources,
                *(
                    resource
                    for binding in child_bindings
                    for resource in binding.resolved_resources
                ),
            ),
            key=lambda item: item.capability_id,
        )
        direct_skills = tuple(
            ResolvedSkillDependency(
                capability_id=_skill_capability_id(
                    tenant_id,
                    binding.publisher,
                    binding.skill_name,
                    binding.skill_version,
                ),
                skill_name=binding.skill_name,
                skill_version=binding.skill_version,
                publisher=binding.publisher,
                package_digest=binding.package_digest,
                artifact_ref=binding.artifact_ref,
            )
            for binding in child_bindings
        )
        resolved_skills = _unique_by_capability_id(
            (
                *direct_skills,
                *(
                    child
                    for binding in child_bindings
                    for child in binding.resolved_skills
                ),
            ),
            key=lambda item: item.capability_id,
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
                    "required_skills": [
                        {
                            "name": item.name,
                            "publisher": item.publisher,
                            "version": item.version,
                        }
                        for item in manifest.required_skills
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
            resolved_skills=resolved_skills,
            policy_version=policy_version,
            policy_decision_id=policy_decision_id,
            max_steps=manifest.max_steps,
            timeout_seconds=manifest.timeout_seconds,
        )


def skill_signing_payload(package: SkillPackage) -> bytes:
    manifest = package.manifest.model_dump(mode="json")
    manifest["signature"] = None
    if manifest.get("signature_key_id") is None:
        manifest.pop("signature_key_id", None)
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


def skill_package_archive(package: SkillPackage) -> bytes:
    """Return the canonical immutable archive used for digest and Artifact storage."""
    return _package_archive(package)


def validate_skill_test_vectors(package: SkillPackage) -> int:
    """Validate declarative fixtures without executing package-supplied code."""
    count = 0
    for path, content in package.files.items():
        if not path.startswith("tests/"):
            continue
        if not path.endswith(".json"):
            raise SchemaValidationError("Skill tests may only contain JSON vectors")
        try:
            vector = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaValidationError("Skill test vector is invalid JSON") from exc
        if not isinstance(vector, dict):
            raise SchemaValidationError("Skill test vector must be an object")
        if set(vector) - {"name", "input", "expected_output"}:
            raise SchemaValidationError("Skill test vector contains unsupported fields")
        if not isinstance(vector.get("name"), str) or not vector["name"].strip():
            raise SchemaValidationError("Skill test vector requires a name")
        if not isinstance(vector.get("input"), dict):
            raise SchemaValidationError("Skill test vector input must be an object")
        expected = vector.get("expected_output")
        if expected is not None and not isinstance(expected, dict):
            raise SchemaValidationError(
                "Skill test vector expected_output must be an object"
            )
        count += 1
    return count


def skill_package_from_archive(
    content: bytes,
    *,
    max_encoded_bytes: int = 24 * 1024 * 1024,
    max_files: int = 512,
) -> SkillPackage:
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaValidationError("Skill package archive is invalid") from exc
    raw_files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(raw_files, dict) or not raw_files:
        raise SchemaValidationError("Skill package archive has no files")
    if len(raw_files) > max_files:
        raise SchemaValidationError("Skill package contains too many files")
    if any(not isinstance(path, str) for path in raw_files):
        raise SchemaValidationError("Skill package archive path is invalid")
    if any(not isinstance(value, str) for value in raw_files.values()):
        raise SchemaValidationError("Skill package archive content is invalid")
    if sum(len(value) for value in raw_files.values()) > max_encoded_bytes:
        raise SchemaValidationError("Skill package archive is too large")
    try:
        files = {
            path: base64.b64decode(value, validate=True)
            for path, value in raw_files.items()
        }
    except (ValueError, binascii.Error) as exc:
        raise SchemaValidationError("Skill package archive base64 is invalid") from exc
    return SkillPackage.from_files(files)


def version_satisfies(version: str, constraint: str) -> bool:
    if constraint.strip() in {"", "*"}:
        return True
    if constraint.strip() == version:
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


def skill_capability_descriptor(
    publication: PublishedSkill,
    *,
    server_id: str = "auraclaw-skill-registry",
) -> CapabilityDescriptor:
    manifest = publication.manifest
    return CapabilityDescriptor(
        capability_id=_skill_capability_id(
            publication.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
        ),
        kind=CapabilityKind.SKILL,
        server_id=server_id,
        canonical_name=manifest.name,
        version=manifest.version,
        content_digest=publication.package_digest,
        title=manifest.name,
        description=manifest.description,
        tags=tuple(manifest.applies_when),
        tenant_id=publication.tenant_id,
        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
        classification=manifest.data_classification,
        permission="read-only",
        risk_level=manifest.risk_level,
        status=CapabilityStatus.ACTIVE,
        source_revision=publication.package_digest,
        updated_at=datetime.now(UTC),
        metadata={
            "model_contract": {
                "publisher": manifest.publisher,
                "name": manifest.name,
                "version": manifest.version,
                "applies_when": list(manifest.applies_when),
                "not_when": list(manifest.not_when),
                "input_schema": manifest.input_schema,
                "required_skills": [
                    requirement.model_dump(mode="json")
                    for requirement in manifest.required_skills
                ],
                "allowed_roles": list(manifest.allowed_roles),
                "max_steps": manifest.max_steps,
                "timeout_seconds": manifest.timeout_seconds,
            }
        },
    )


def _skill_capability_id(
    tenant_id: str,
    publisher: str,
    name: str,
    version: str,
) -> str:
    identity = f"{tenant_id}:{publisher}:{name}:{version}"
    return f"cap_{hashlib.sha256(identity.encode()).hexdigest()[:32]}"


def _unique_by_capability_id(
    items: tuple[_CapabilityDependency, ...],
    *,
    key: Callable[[_CapabilityDependency], str],
) -> tuple[_CapabilityDependency, ...]:
    unique: dict[str, _CapabilityDependency] = {}
    for item in items:
        capability_id = key(item)
        unique.setdefault(capability_id, item)
    return tuple(unique.values())


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
            descriptor=HandsResourceDescriptor(
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
                classification=package.manifest.data_classification,
                content_digest=f"sha256:{package_digest}",
                source_revision=package.manifest.version,
            ),
            contents=(
                HandsResourceContent(
                    uri=f"{prefix}/{aliases.get(path, path)}",
                    mime_type=(
                        "application/json"
                        if path.endswith(".json")
                        else "text/markdown"
                        if path.endswith(".md")
                        else "application/octet-stream"
                    ),
                    classification=package.manifest.data_classification,
                    source_revision=package.manifest.version,
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
