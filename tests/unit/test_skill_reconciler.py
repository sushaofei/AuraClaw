from __future__ import annotations

import asyncio
import json

from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_reconciler import SkillPackageReconciler
from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsTrustedContext,
)
from auraclaw.contracts.skills import SkillManifest
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage

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
        )
        registry = SkillPackageRegistry(
            artifacts=ArtifactStore(
                InMemoryObjectStorage(),
                signing_key=b"skill-reconciler-artifact-key",
            ),
            signature_verifier=verifier,
        )
        reconciler = SkillPackageReconciler(
            store=_Store(server),
            connectors={"skill-server": _SkillMcpConnector(resources)},
            registry=registry,
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

    asyncio.run(scenario())
