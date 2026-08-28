from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.action.skill_reconciler import SkillPackageReconciler
from auraclaw.contracts.capabilities import CapabilityKind, McpServerDefinition
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsTrustedContext,
)
from auraclaw.contracts.skills import SkillManifest
from auraclaw.contracts.tools import ArtifactRef

_PUBLISHER_KEY = b"platform-skill-signing-key-v1"


class _SkillMcpConnector:
    connector_id = "mcp:skill-server"

    def __init__(self, package_resources: tuple[tuple[str, str], ...]) -> None:
        self._package_resources = package_resources

    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
        del trusted
        return CapabilitySnapshot(
            connector_id=self.connector_id,
            tools=(),
            resources=tuple(
                HandsResourceDescriptor(
                    uri=uri,
                    name=uri.rsplit("/", 1)[-1],
                    mime_type="application/json" if uri.endswith("manifest") else "text/markdown",
                )
                for uri, _text in self._package_resources
            ),
            resource_templates=(),
            prompts=(),
        )

    async def read_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        del trusted
        for resource_uri, text in self._package_resources:
            if resource_uri == uri:
                mime_type = (
                    "application/json"
                    if uri.endswith("manifest")
                    else "text/markdown"
                )
                return (
                    HandsResourceContent(
                        uri=uri,
                        mime_type=mime_type,
                        text=text,
                    ),
                )
        raise KeyError(uri)

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> object:
        raise NotImplementedError(name)

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        *,
        name: str,
        arguments: dict[str, object],
        invocation_id: str,
    ) -> object:
        raise NotImplementedError(name)

    async def aclose(self) -> None:
        return None


class _Store:
    def __init__(self, server: McpServerDefinition) -> None:
        self._server = server

    async def get_server(self, server_id: str) -> McpServerDefinition | None:
        if server_id == self._server.server_id:
            return self._server
        return None


class _Artifacts:
    def __init__(self) -> None:
        self.contents: dict[str, bytes] = {}

    async def put(self, **kwargs: Any) -> ArtifactRef:
        content = kwargs["content"]
        assert isinstance(content, bytes)
        artifact_id = f"art_{len(self.contents) + 1}"
        self.contents[artifact_id] = content
        return ArtifactRef(
            artifact_id=artifact_id,
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type=str(kwargs["media_type"]),
            size=len(content),
        )

    async def read(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> bytes:
        del tenant_id, actor_id, correlation_id
        return self.contents[artifact_ref.artifact_id]


def _package(verifier: HmacSkillSignatureVerifier) -> SkillPackage:
    unsigned = SkillManifest(
        name="release.prepare",
        version="1.4.0",
        description="Prepare an auditable release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    content_files = {
        "SKILL.md": b"# Release\n\nPrepare the audited release.",
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


def test_skill_reconciler_downloads_signed_package_from_mcp_resources() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        package = _package(verifier)
        prefix = (
            f"skill://{package.manifest.publisher}/"
            f"{package.manifest.name}/{package.manifest.version}"
        )
        resources = (
            (f"{prefix}/manifest", package.files["manifest.json"].decode()),
            (f"{prefix}/SKILL.md", package.files["SKILL.md"].decode()),
        )
        server = McpServerDefinition(
            server_id="skill-server",
            tenant_id="tenant-a",
            title="Skill MCP",
            endpoint="https://skills.example/mcp",
            allowed_resource_schemes=("skill",),
            enabled=True,
            metadata={"skill_publisher_allowlist": ["platform"]},
        )
        artifacts = _Artifacts()
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=verifier,
        )
        catalog_store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(catalog_store)
        publication = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
        )
        rebuilder = SkillStateRebuilder(
            lifecycle=lifecycle,
            artifacts=artifacts,
            registry=registry,
            catalog=catalog,
        )
        reconciler = SkillPackageReconciler(
            store=_Store(server),
            connectors={"skill-server": _SkillMcpConnector(resources)},
            lifecycle=lifecycle,
            publication=publication,
            rebuilder=rebuilder,
        )

        published = await reconciler.reconcile_all()

        assert published == 1
        candidates = registry.candidates(
            "tenant-a",
            package.manifest.name,
            publisher=package.manifest.publisher,
        )
        assert len(candidates) == 1
        manifest = json.loads(
            registry.load_part(
                "tenant-a",
                publisher=package.manifest.publisher,
                name=package.manifest.name,
                version=package.manifest.version,
                package_digest=candidates[0].package_digest,
                path="manifest.json",
            ).decode()
        )
        assert manifest["name"] == package.manifest.name
        matches = await catalog.search(
            tenant_id="tenant-a",
            query="release",
            kinds=(CapabilityKind.SKILL,),
        )
        assert len(matches) == 1
        publications = await lifecycle.list_publications("tenant-a")
        assert len(publications) == 1
        source = await lifecycle.get_source(
            "tenant-a", publications[0].source_id
        )
        assert source is not None
        assert source.publisher_allowlist == ("platform",)
        sync_state = await lifecycle.get_sync_state("tenant-a", source.source_id)
        assert sync_state is not None
        assert sync_state.complete_snapshot
        assert sync_state.generation == 1
        assert sync_state.consecutive_failures == 0

    asyncio.run(scenario())


def test_skill_reconciler_persists_safe_failure_and_recovers() -> None:
    class FailingOnceConnector(_SkillMcpConnector):
        def __init__(self) -> None:
            super().__init__(())
            self.failed = False

        async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
            if not self.failed:
                self.failed = True
                raise RuntimeError("sensitive upstream response")
            return await super().snapshot(trusted)

    async def scenario() -> None:
        server = McpServerDefinition(
            server_id="recovering-skill-server",
            tenant_id="tenant-a",
            title="Recovering Skill MCP",
            endpoint="https://skills.example/mcp",
            enabled=True,
            metadata={"skill_publisher_allowlist": ["platform"]},
        )
        lifecycle = InMemorySkillLifecycleStore()
        artifacts = _Artifacts()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier(
                {"platform": _PUBLISHER_KEY}
            ),
        )
        catalog = CapabilityCatalog(InMemoryCapabilityCatalogStore())
        reconciler = SkillPackageReconciler(
            store=_Store(server),
            connectors={server.server_id: FailingOnceConnector()},
            lifecycle=lifecycle,
            publication=SkillPublicationService(
                registry=registry,
                lifecycle=lifecycle,
            ),
            rebuilder=SkillStateRebuilder(
                lifecycle=lifecycle,
                artifacts=artifacts,
                registry=registry,
                catalog=catalog,
            ),
            owner="hands-a",
        )

        failed = await reconciler.reconcile_server(server)
        source_id = next(iter(lifecycle._sources.values())).source_id
        failed_state = await lifecycle.get_sync_state("tenant-a", source_id)
        assert failed.error == "RuntimeError"
        assert failed_state is not None
        assert failed_state.safe_error_code == "RuntimeError"
        assert "sensitive" not in failed_state.safe_error_code
        assert failed_state.consecutive_failures == 1
        assert not failed_state.complete_snapshot

        recovered = await reconciler.reconcile_server(server)
        recovered_state = await lifecycle.get_sync_state("tenant-a", source_id)
        assert recovered.error is None
        assert recovered_state is not None and recovered_state.complete_snapshot
        assert recovered_state.safe_error_code is None
        assert recovered_state.consecutive_failures == 0
        assert recovered_state.generation == 2

    asyncio.run(scenario())


def test_skill_reconciler_requires_configured_publisher_allowlist() -> None:
    async def scenario() -> None:
        verifier = HmacSkillSignatureVerifier({"platform": _PUBLISHER_KEY})
        artifacts = _Artifacts()
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=verifier,
        )
        catalog = CapabilityCatalog(InMemoryCapabilityCatalogStore())
        publication = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
        )
        server = McpServerDefinition(
            server_id="untrusted-skill-server",
            tenant_id="tenant-a",
            title="Untrusted Skill MCP",
            endpoint="https://skills.example/mcp",
            enabled=True,
        )
        reconciler = SkillPackageReconciler(
            store=_Store(server),
            connectors={
                server.server_id: _SkillMcpConnector(())
            },
            lifecycle=lifecycle,
            publication=publication,
            rebuilder=SkillStateRebuilder(
                lifecycle=lifecycle,
                artifacts=artifacts,
                registry=registry,
                catalog=catalog,
            ),
        )

        result = await reconciler.reconcile_server(server)

        assert result.published_count == 0
        assert result.error == "ValueError"
        assert await lifecycle.list_publications("tenant-a") == ()

    asyncio.run(scenario())
