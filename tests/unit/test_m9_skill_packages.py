from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.hands import HandsGateway
from auraclaw.action.mcp_primitives import McpResourceRegistry
from auraclaw.action.ports import PolicyEvaluation
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    SkillResolver,
    skill_capability_descriptor,
    skill_signing_payload,
)
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.errors import (
    PolicyDeniedError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    SkillManifest,
    SkillRequirement,
    SkillResourceRequirement,
    SkillToolRequirement,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import (
    ArtifactStore,
    InMemoryObjectStorage,
)
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter

_PUBLISHER_KEY = b"platform-skill-signing-key-v1"


class _UnusedGateway:
    async def execute(self, invocation: object) -> object:
        raise AssertionError(f"unexpected Tool call: {invocation}")

    async def cancel(self, tool_invocation_id: str) -> bool:
        del tool_invocation_id
        return False


class _SkillPolicy:
    def __init__(self) -> None:
        self.attributes: dict[str, object] = {}

    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        self.attributes = dict(arguments["attributes"])  # type: ignore[arg-type]
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="skill-policy-1",
            policy_version="policy-43",
        )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-a",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
    )


def _artifacts() -> ArtifactStore:
    return ArtifactStore(
        InMemoryObjectStorage(),
        signing_key=b"skill-package-artifact-key-v1",
    )


def _package(
    verifier: HmacSkillSignatureVerifier,
    *,
    version: str = "1.4.0",
    instructions: str = "# Release\n\nPrepare the audited release.",
) -> SkillPackage:
    unsigned = SkillManifest(
        name="release.prepare",
        version=version,
        description="Prepare an auditable release",
        applies_when=("repository release requested",),
        not_when=("production rollback",),
        required_tools=(SkillToolRequirement(name="github.pull_request.get", version=">=2,<3"),),
        required_resources=(SkillResourceRequirement(uri_template="repo://{repo}/release-policy"),),
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    content_files = {
        "SKILL.md": instructions.encode(),
        "references/checklist.md": b"# Checklist",
    }
    signature = verifier.sign(unsigned, content_files)
    manifest = unsigned.model_copy(update={"signature": signature})
    return SkillPackage(
        manifest=manifest,
        files={
            "manifest.json": manifest.model_dump_json().encode(),
            **content_files,
        },
    )


def test_legacy_hmac_signing_payload_omits_empty_key_identity() -> None:
    verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
    payload = json.loads(skill_signing_payload(_package(verifier)))
    assert "signature_key_id" not in payload["manifest"]


def _descriptor(
    capability_id: str,
    kind: CapabilityKind,
    canonical_name: str,
    version: str,
    *,
    metadata: dict[str, object] | None = None,
) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capability_id=capability_id,
        kind=kind,
        server_id="server-platform",
        canonical_name=canonical_name,
        version=version,
        content_digest=f"sha256:{capability_id.encode().hex().ljust(64, '0')[:64]}",
        title=canonical_name,
        trust_level=CapabilityTrustLevel.PLATFORM,
        status=CapabilityStatus.ACTIVE,
        updated_at=datetime.now(UTC),
        metadata=metadata or {},
    )


def _dependency_package(
    verifier: HmacSkillSignatureVerifier,
    *,
    name: str,
    required_skills: tuple[SkillRequirement, ...] = (),
    required_tools: tuple[SkillToolRequirement, ...] = (),
) -> SkillPackage:
    unsigned = SkillManifest(
        name=name,
        version="1.0.0",
        description=f"Composable test Skill {name}",
        required_skills=required_skills,
        required_tools=required_tools,
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": f"# {name}".encode()}
    manifest = unsigned.model_copy(update={"signature": verifier.sign(unsigned, files)})
    return SkillPackage(
        manifest=manifest,
        files={
            "manifest.json": manifest.model_dump_json().encode(),
            **files,
        },
    )


def test_skill_package_publish_is_signed_immutable_and_progressively_loadable() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        resources = McpResourceRegistry()
        registry = SkillPackageRegistry(
            artifacts=_artifacts(),
            signature_verifier=verifier,
            resources=resources,
        )
        package = _package(verifier)
        publication = await registry.publish("tenant-a", package)
        model_contract = skill_capability_descriptor(publication).metadata[
            "model_contract"
        ]

        assert publication.package_digest.startswith("sha256:")
        assert model_contract["required_tools"] == [
            {"name": "github.pull_request.get", "version": ">=2,<3"}
        ]
        assert model_contract["required_resources"] == [
            {"uri_template": "repo://{repo}/release-policy"}
        ]
        assert publication.artifact_ref.media_type == (
            "application/vnd.auraclaw.skill-package+json"
        )
        assert [resource.uri for resource in resources.discover_resources("tenant-a")] == [
            "skill://platform/release.prepare/1.4.0/SKILL.md",
            "skill://platform/release.prepare/1.4.0/manifest",
            "skill://platform/release.prepare/1.4.0/references/checklist.md",
        ]
        assert resources.discover_resources("tenant-b") == []
        manifest_payload = resources.read(
            "tenant-a", "skill://platform/release.prepare/1.4.0/manifest"
        )[0]
        assert json.loads(manifest_payload.text or "{}")["name"] == "release.prepare"
        client = HandsRuntimeAdapter(
            InProcessHandsClient(
                HandsGateway(
                    registry=ToolRegistry(),
                    gateway=_UnusedGateway(),  # type: ignore[arg-type]
                    resources=resources,
                )
            )
        )
        assignment = _assignment()
        loaded_manifest = await client.load_skill_manifest(
            assignment,
            publisher="platform",
            name="release.prepare",
            version="1.4.0",
        )
        assert loaded_manifest["version"] == "1.4.0"
        loaded_part = await client.load_skill_part(
            assignment,
            publisher="platform",
            name="release.prepare",
            version="1.4.0",
            path="references/checklist.md",
        )
        assert loaded_part[0]["text"] == "# Checklist"
        assert (
            registry.load_part(
                "tenant-a",
                publisher="platform",
                name="release.prepare",
                version="1.4.0",
                package_digest=publication.package_digest,
                path="references/checklist.md",
            )
            == b"# Checklist"
        )

        assert await registry.publish("tenant-a", package) == publication
        changed = _package(verifier, instructions="# Changed")
        with pytest.raises(VersionConflictError):
            await registry.publish("tenant-a", changed)

        registry.revoke("tenant-a", "platform", "release.prepare", "1.4.0")
        with pytest.raises(KeyError):
            resources.read(
                "tenant-a",
                "skill://platform/release.prepare/1.4.0/SKILL.md",
            )
        with pytest.raises(PolicyDeniedError):
            registry.load_part(
                "tenant-a",
                publisher="platform",
                name="release.prepare",
                version="1.4.0",
                package_digest=publication.package_digest,
                path="SKILL.md",
            )

    asyncio.run(scenario())


def test_skill_package_rejects_invalid_signature_and_unsafe_paths() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        registry = SkillPackageRegistry(
            artifacts=_artifacts(),
            signature_verifier=verifier,
        )
        package = _package(verifier)
        tampered = SkillPackage(
            manifest=package.manifest,
            files={**package.files, "SKILL.md": b"tampered"},
        )
        with pytest.raises(PolicyDeniedError):
            await registry.publish("tenant-a", tampered)
        unsafe = SkillPackage(
            manifest=package.manifest,
            files={**package.files, "../escape": b"blocked"},
        )
        with pytest.raises(SchemaValidationError, match="Unsafe Skill package path"):
            await registry.publish("tenant-a", unsafe)

    asyncio.run(scenario())


def test_skill_resolver_pins_highest_compatible_dependencies() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        registry = SkillPackageRegistry(
            artifacts=_artifacts(),
            signature_verifier=verifier,
        )
        await registry.publish("tenant-a", _package(verifier, version="1.4.0"))
        await registry.publish("tenant-a", _package(verifier, version="1.5.0"))

        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-platform",
                title="Platform capabilities",
                endpoint="https://platform.example/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "server-platform",
            (
                _descriptor(
                    "cap-tool-2-1",
                    CapabilityKind.TOOL,
                    "github.pull_request.get",
                    "2.1.0",
                ),
                _descriptor(
                    "cap-tool-2-4",
                    CapabilityKind.TOOL,
                    "github.pull_request.get",
                    "2.4.0",
                ),
                _descriptor(
                    "cap-tool-3",
                    CapabilityKind.TOOL,
                    "github.pull_request.get",
                    "3.0.0",
                ),
                _descriptor(
                    "cap-resource",
                    CapabilityKind.RESOURCE_TEMPLATE,
                    "repo.release-policy",
                    "1.0.0",
                    metadata={"uri_template": "repo://{repo}/release-policy"},
                ),
            ),
        )
        skill_policy = _SkillPolicy()
        binding = await SkillResolver(
            registry,
            store,
            policy=skill_policy,
        ).resolve(
            tenant_id="tenant-a",
            name="release.prepare",
            version=">=1.4,<2",
            role="repair",
            policy_version="policy-42",
            subject="runtime-1",
            correlation_id="run-1",
            active_skill_names=("audit.prepare",),
        )

        assert binding.skill_version == "1.5.0"
        assert binding.resolved_tools[0].version == "2.4.0"
        assert binding.resolved_resources[0].capability_id == "cap-resource"
        assert binding.policy_version == "policy-43"
        assert binding.policy_decision_id == "skill-policy-1"
        assert skill_policy.attributes["active_skill_names"] == ["audit.prepare"]
        assert skill_policy.attributes["role"] == "worker"
        assert skill_policy.attributes["assignment_role"] == "repair"
        assert skill_policy.attributes["effective_skill_role"] == "worker"

        denied_package = _package(verifier, version="2.0.0")
        denied_manifest = denied_package.manifest.model_copy(
            update={"allowed_roles": ("coordinator",)}
        )
        denied_signature = verifier.sign(
            denied_manifest,
            {
                path: content
                for path, content in denied_package.files.items()
                if path != "manifest.json"
            },
        )
        denied_manifest = denied_manifest.model_copy(update={"signature": denied_signature})
        await registry.publish(
            "tenant-a",
            SkillPackage(
                manifest=denied_manifest,
                files={
                    **denied_package.files,
                    "manifest.json": denied_manifest.model_dump_json().encode(),
                },
            ),
        )
        with pytest.raises(PolicyDeniedError):
            await SkillResolver(registry, store).resolve(
                tenant_id="tenant-a",
                name="release.prepare",
                version="2.0.0",
                role="reviewer",
                policy_version="policy-42",
            )

    asyncio.run(scenario())


def test_skill_resolver_flattens_child_skills_and_rejects_cycles() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        registry = SkillPackageRegistry(
            artifacts=_artifacts(),
            signature_verifier=verifier,
        )
        child = _dependency_package(
            verifier,
            name="data.validate",
            required_tools=(SkillToolRequirement(name="data.scope.profile", version="1.0.0"),),
        )
        parent = _dependency_package(
            verifier,
            name="scenario.analyze",
            required_skills=(
                SkillRequirement(
                    name="data.validate",
                    version="1.0.0",
                    publisher="platform",
                ),
            ),
        )
        await registry.publish("tenant-a", child)
        await registry.publish("tenant-a", parent)
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        await catalog.register_server(
            McpServerDefinition(
                server_id="server-platform",
                title="Platform capabilities",
                endpoint="https://platform.example/mcp",
                trust_level=CapabilityTrustLevel.PLATFORM,
                status=CapabilityStatus.ACTIVE,
                enabled=True,
            )
        )
        await catalog.replace_server_capabilities(
            "server-platform",
            (
                _descriptor(
                    "cap-data-profile",
                    CapabilityKind.TOOL,
                    "data.scope.profile",
                    "1.0.0",
                ),
            ),
        )
        binding = await SkillResolver(registry, store).resolve(
            tenant_id="tenant-a",
            name="scenario.analyze",
            role="repair",
            policy_version="policy-1",
        )
        assert [item.skill_name for item in binding.resolved_skills] == ["data.validate"]
        assert [item.canonical_name for item in binding.resolved_tools] == ["data.scope.profile"]

        cycle_a = _dependency_package(
            verifier,
            name="cycle.a",
            required_skills=(SkillRequirement(name="cycle.b"),),
        )
        cycle_b = _dependency_package(
            verifier,
            name="cycle.b",
            required_skills=(SkillRequirement(name="cycle.a"),),
        )
        await registry.publish("tenant-a", cycle_a)
        await registry.publish("tenant-a", cycle_b)
        with pytest.raises(SchemaValidationError, match="cycle detected"):
            await SkillResolver(registry, store).resolve(
                tenant_id="tenant-a",
                name="cycle.a",
                role="worker",
                policy_version="policy-1",
            )

    asyncio.run(scenario())
