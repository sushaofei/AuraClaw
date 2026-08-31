from __future__ import annotations

import asyncio
import base64
import hashlib
from datetime import UTC, datetime

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
)
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.contracts.capabilities import CapabilityKind
from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    ChangeSkillPublisherStatusCommand,
    PublishSkillCommand,
    RegisterSkillPublisherCommand,
    RestoreSkillPublicationCommand,
    RevokeSkillPublicationCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillInstallationOperation,
    SkillManifest,
    SkillPublicationStatus,
    SkillPublisherStatusOperation,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.clients.skill_publication import (
    RemoteSkillPublicationClient,
)
from auraclaw.infrastructure.identity.verifier import DevelopmentHeaderIdentityVerifier
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import skill_publication_routes

_KEY = b"internal-skill-publication-key"


class _ArtifactWriter:
    def __init__(self) -> None:
        self.calls = 0
        self.contents: dict[str, bytes] = {}

    async def put(self, **kwargs: object) -> ArtifactRef:
        self.calls += 1
        content = kwargs["content"]
        assert isinstance(content, bytes)
        self.contents["art_persisted_skill"] = content
        return ArtifactRef(
            artifact_id="art_persisted_skill",
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
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


def _package() -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="2.0.0",
        description="Prepare a release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": b"# Release\n"}
    manifest = unsigned.model_copy(
        update={"signature": verifier.sign(unsigned, files)}
    )
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_task_api_client_publishes_through_action_hands_service() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        artifacts = _ArtifactWriter()
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
            resources=HandsResourceRegistry(),
        )
        publication = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
            artifacts=artifacts,
            bootstrap_sources=(
                SkillSourceRecord(
                    source_id="sks_admin_upload",
                    tenant_id="*",
                    kind=SkillSourceKind.ADMIN_UPLOAD,
                    desired_state=SkillSourceDesiredState.ENABLED,
                    publisher_allowlist=("platform",),
                    created_by="system",
                    updated_by="system",
                    created_at=now,
                    updated_at=now,
                ),
            ),
        )
        catalog_store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(catalog_store)
        rebuilder = SkillStateRebuilder(
            lifecycle=lifecycle,
            artifacts=artifacts,
            registry=registry,
            catalog=catalog,
        )
        management = SkillManagementService(
            lifecycle=lifecycle,
            projector=rebuilder,
            retired_activator=publication,
        )
        publishers = SkillPublisherService(InMemorySkillPublisherStore())
        contract = create_contract_app(
            "skill-publication-test",
            skill_publication_routes(
                SkillPublicationInternalService(
                    publication,
                    management=management,
                    rebuilder=rebuilder,
                    publishers=publishers,
                    admissions=lifecycle,
                    artifacts=artifacts,
                )
            ),
            workload_identities={"task-token": ServiceIdentity.TASK_API},
        )
        app = FastAPI()
        app.mount("/internal/v1/skill-publications", contract)
        task_api_registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
            resources=HandsResourceRegistry(),
        )
        client = RemoteSkillPublicationClient(
            "http://hands",
            bearer_token="task-token",
            transport=httpx.ASGITransport(app=app),
        )
        try:
            registered, _ = await client.register_publisher(
                RegisterSkillPublisherCommand(
                    tenant_id="tenant-a",
                    actor_id="security-admin",
                    publisher="acme",
                    display_name="Acme Skills",
                    command_id="publisher-register-1",
                    correlation_id="corr-publisher",
                    causation_id="publisher-register-1",
                )
            )
            public_key = base64.urlsafe_b64encode(
                Ed25519PrivateKey.generate().public_key().public_bytes(
                    serialization.Encoding.Raw,
                    serialization.PublicFormat.Raw,
                )
            ).rstrip(b"=").decode()
            registered, keys = await client.rotate_publisher_key(
                RotateSkillPublisherKeyCommand(
                    tenant_id="tenant-a",
                    actor_id="security-admin",
                    publisher="acme",
                    key_id="key-a",
                    public_key=public_key,
                    command_id="publisher-rotate-1",
                    expected_revision=registered.revision,
                    correlation_id="corr-publisher",
                    causation_id="publisher-rotate-1",
                )
            )
            assert (await client.get_publisher("tenant-a", "acme"))[1] == keys
            suspended, _ = await client.change_publisher_status(
                ChangeSkillPublisherStatusCommand(
                    tenant_id="tenant-a",
                    actor_id="security-admin",
                    publisher="acme",
                    operation=SkillPublisherStatusOperation.SUSPEND,
                    reason_code="publisher_under_review",
                    command_id="publisher-suspend-1",
                    expected_revision=registered.revision,
                    correlation_id="corr-publisher",
                    causation_id="publisher-suspend-1",
                )
            )
            assert suspended.status.value == "suspended"
            resumed, _ = await client.change_publisher_status(
                ChangeSkillPublisherStatusCommand(
                    tenant_id="tenant-a",
                    actor_id="security-admin",
                    publisher="acme",
                    operation=SkillPublisherStatusOperation.RESUME,
                    reason_code="review_completed",
                    command_id="publisher-resume-1",
                    expected_revision=suspended.revision,
                    correlation_id="corr-publisher",
                    causation_id="publisher-resume-1",
                )
            )
            assert resumed.status.value == "active"
            _registered, keys = await client.revoke_publisher_key(
                RevokeSkillPublisherKeyCommand(
                    tenant_id="tenant-a",
                    actor_id="security-admin",
                    publisher="acme",
                    key_id="key-a",
                    reason_code="key_compromised",
                    command_id="publisher-revoke-1",
                    expected_revision=keys[0].revision,
                    correlation_id="corr-publisher",
                    causation_id="publisher-revoke-1",
                )
            )
            assert keys[0].status.value == "revoked"
            result = await client.publish(
                PublishSkillCommand(
                    tenant_id="tenant-a",
                    actor_id="admin-a",
                    source_id="sks_admin_upload",
                    command_id="publish-internal-1",
                    expected_revision=0,
                    correlation_id="corr-1",
                    causation_id="publish-internal-1",
                ),
                _package(),
            )
            staged_retry = await client.publish_artifact(
                PublishSkillCommand(
                    tenant_id="tenant-a",
                    actor_id="admin-a",
                    source_id="sks_admin_upload",
                    command_id="publish-internal-staged-retry",
                    expected_revision=1,
                    correlation_id="corr-staged-retry",
                    causation_id="publish-internal-staged-retry",
                ),
                result.artifact_ref,
                result.package_digest,
            )
            assert staged_retry == result
            valid = _package()
            unsafe = SkillPackage(
                manifest=valid.manifest,
                files={**valid.files, "../escape.md": b"unsafe"},
            )
            with pytest.raises(SchemaValidationError):
                await client.publish(
                    PublishSkillCommand(
                        tenant_id="tenant-a",
                        actor_id="admin-a",
                        source_id="sks_admin_upload",
                        command_id="publish-internal-unsafe",
                        expected_revision=0,
                        correlation_id="corr-unsafe",
                        causation_id="publish-internal-unsafe",
                    ),
                    unsafe,
                )
            rejected_page = await client.page_admissions(
                "tenant-a",
                outcome="rejected",
                content_policy_version="skill-content-v1",
            )
            rejected_admissions = rejected_page.admissions
            assert len(rejected_admissions) == 1
            assert rejected_admissions[0].command_id == "publish-internal-unsafe"
            assert rejected_admissions[0].content_policy_version == "skill-content-v1"
            assert (await client.page_admissions("tenant-b")).admissions == ()
            admission_metrics = await client.admission_metrics("tenant-a")
            assert {(item.outcome, item.count) for item in admission_metrics} == {
                ("accepted", 2),
                ("rejected", 1),
            }
            active_publication = await lifecycle.get_publication(
                "tenant-a", "platform", "release.prepare", "2.0.0"
            )
            assert active_publication is not None
            await lifecycle.put_publication(
                active_publication.model_copy(
                    update={
                        "status": SkillPublicationStatus.RETIRED,
                        "revision": 2,
                        "updated_by": "source-reconciler",
                        "updated_at": datetime.now(UTC),
                        "reason_code": "source_missing_confirmed",
                    }
                ),
                expected_revision=1,
            )
            restored = await client.restore_publication(
                RestoreSkillPublicationCommand(
                    tenant_id="tenant-a",
                    actor_id="reviewer-a",
                    publisher="platform",
                    name="release.prepare",
                    version="2.0.0",
                    reason_code="source_inventory_reviewed",
                    command_id="restore-internal-1",
                    expected_revision=2,
                    correlation_id="corr-restore",
                    causation_id="restore-internal-1",
                )
            )
            assert restored.status is SkillPublicationStatus.ACTIVE
            assert restored.revision == 4
            installation_state = await client.get_installation(
                "tenant-a", "platform", "release.prepare"
            )
            assert installation_state.revision == 1
            disabled = await client.change_installation(
                ChangeSkillInstallationCommand(
                    tenant_id="tenant-a",
                    actor_id="admin-b",
                    publisher="platform",
                    name="release.prepare",
                    operation=SkillInstallationOperation.DISABLE,
                    reason_code="tenant_disabled",
                    command_id="disable-internal-1",
                    expected_revision=1,
                    correlation_id="corr-disable",
                    causation_id="disable-internal-1",
                )
            )
            assert disabled.status.value == "disabled"
            assert await catalog.search(
                tenant_id="tenant-a", kinds=(CapabilityKind.SKILL,)
            ) == ()
            revoked = await client.revoke_publication(
                RevokeSkillPublicationCommand(
                    tenant_id="tenant-a",
                    actor_id="security-a",
                    publisher="platform",
                    name="release.prepare",
                    version="2.0.0",
                    reason_code="publisher_key_compromised",
                    command_id="revoke-internal-1",
                    expected_revision=4,
                    correlation_id="corr-revoke",
                    causation_id="revoke-internal-1",
                )
            )
            assert revoked.status.value == "revoked"
            publication_state = await client.get_publication(
                "tenant-a", "platform", "release.prepare", "2.0.0"
            )
            assert publication_state.revision == 5
            assert publication_state.updated_by == "security-a"
            package_state = await client.get_package(
                "tenant-a", "platform", "release.prepare", "2.0.0"
            )
            assert package_state.retention_revision == 1
            assert package_state.retention_updated_by == "admin-a"
            assert (
                await client.get_skill_markdown(
                    "tenant-a", "platform", "release.prepare", "2.0.0"
                )
                == "# Release\n"
            )
            assert len(await client.list_packages("tenant-a")) == 1
            assert len(await client.list_publications("tenant-a")) == 1
            assert len(await client.list_installations("tenant-a")) == 1
            listed_publishers = await client.list_publishers("tenant-a")
            assert listed_publishers[0][0].publisher == "acme"

            task_api = FastAPI()
            task_api.state.identity_verifier = DevelopmentHeaderIdentityVerifier()
            task_api.include_router(
                create_skill_admin_router(
                    task_api_registry,
                    management_service=client,
                    content_reader=client,
                )
            )
            async with httpx.AsyncClient(
                base_url="http://task-api",
                transport=httpx.ASGITransport(app=task_api),
                headers={"X-Tenant-ID": "tenant-a", "X-Actor-ID": "admin-a"},
            ) as admin:
                listed = await admin.get("/v1/admin/skills")
                assert listed.status_code == 200
                assert listed.json()["skills"] == listed.json()["items"]
                assert listed.json()["items"][0]["version"] == "2.0.0"
                detail = await admin.get(
                    "/v1/admin/skills/platform/release.prepare"
                )
                assert detail.status_code == 200
                assert detail.json()["skill_markdown"] == "# Release\n"
                version = await admin.get(
                    "/v1/admin/skills/platform/release.prepare/versions/2.0.0"
                )
                assert version.status_code == 200
                assert version.json()["package_digest"] == result.package_digest
                publications = await admin.get("/v1/admin/skill-publications")
                assert publications.status_code == 200
                assert publications.json()["publications"][0]["version"] == "2.0.0"
        finally:
            await client.aclose()

        assert result.artifact_ref.artifact_id == "art_persisted_skill"
        assert artifacts.calls == 1
        assert task_api_registry.list_publications("tenant-a") == ()
        stored = await lifecycle.get_publication(
            "tenant-a", "platform", "release.prepare", "2.0.0"
        )
        assert stored is not None
        assert stored.created_by == "admin-a"
        assert stored.updated_by == "security-a"
        projected = await catalog.search(
            tenant_id="tenant-a", kinds=(CapabilityKind.SKILL,)
        )
        assert projected == ()

    asyncio.run(scenario())
