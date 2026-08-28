from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    skill_signing_payload,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
    SkillPublisherTrustService,
)
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.errors import PolicyDeniedError, VersionConflictError
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    PublishSkillCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillManifest,
    SkillPublisherKeyStatus,
    SkillPublisherStatus,
    SkillPublisherStatusOperation,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class _ArtifactReader:
    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    async def read(self, **kwargs: object) -> bytes:
        tenant_id = str(kwargs["tenant_id"])
        actor_id = str(kwargs["actor_id"])
        artifact_ref = kwargs["artifact_ref"]
        assert isinstance(artifact_ref, ArtifactRef)
        token = await self._store.issue_download_token(
            tenant_id=tenant_id,
            artifact_id=artifact_ref.artifact_id,
            actor_id=actor_id,
        )
        return await self._store.download(
            token=token, tenant_id=tenant_id, actor_id=actor_id
        )


def _publisher_command(command_id: str = "publisher-1") -> RegisterSkillPublisherCommand:
    return RegisterSkillPublisherCommand(
        tenant_id="tenant-a",
        actor_id="security-admin",
        publisher="acme",
        display_name="Acme Skills",
        command_id=command_id,
        correlation_id="corr-a",
        causation_id=command_id,
    )


def _rotate(
    key_id: str, public_key: str, revision: int, command_id: str
) -> RotateSkillPublisherKeyCommand:
    return RotateSkillPublisherKeyCommand(
        tenant_id="tenant-a",
        actor_id="security-admin",
        publisher="acme",
        key_id=key_id,
        public_key=public_key,
        command_id=command_id,
        expected_revision=revision,
        correlation_id="corr-a",
        causation_id=command_id,
    )


def _package(private_key: Ed25519PrivateKey, key_id: str, version: str) -> SkillPackage:
    unsigned = SkillManifest(
        name="release.prepare",
        version=version,
        description="Prepare a release",
        publisher="acme",
        signature_key_id=key_id,
        signature="ed25519:unsigned",
    )
    files = {"SKILL.md": b"# Release\n"}
    unsigned_package = SkillPackage(manifest=unsigned, files=files)
    signature = _b64(private_key.sign(skill_signing_payload(unsigned_package)))
    manifest = unsigned.model_copy(update={"signature": f"ed25519:{signature}"})
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_publisher_rotation_admission_restore_and_revocation() -> None:
    async def scenario() -> None:
        store = InMemorySkillPublisherStore()
        service = SkillPublisherService(store)
        trust = SkillPublisherTrustService(store)
        publisher, keys = await service.register(_publisher_command())
        assert publisher.revision == 1 and keys == ()
        await service.register(
            _publisher_command("publisher-tenant-b").model_copy(
                update={"tenant_id": "tenant-b"}
            )
        )
        first_private = Ed25519PrivateKey.generate()
        first_public = _b64(
            first_private.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        )
        publisher, keys = await service.rotate_key(
            _rotate("key-2026-a", first_public, 1, "rotate-1")
        )
        first_package = _package(first_private, "key-2026-a", "1.0.0")
        assert await trust.verify_for_admission("tenant-a", first_package) == "key-2026-a"
        with pytest.raises(PolicyDeniedError, match="not trusted"):
            await trust.verify_for_admission("tenant-b", first_package)
        tampered = SkillPackage(
            manifest=first_package.manifest,
            files={**first_package.files, "SKILL.md": b"# Tampered\n"},
        )
        with pytest.raises(PolicyDeniedError, match="signature is invalid"):
            await trust.verify_for_admission("tenant-a", tampered)

        second_private = Ed25519PrivateKey.generate()
        second_public = _b64(
            second_private.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        )
        publisher, keys = await service.rotate_key(
            _rotate("key-2026-b", second_public, publisher.revision, "rotate-2")
        )
        assert [key.status for key in keys] == [
            SkillPublisherKeyStatus.RETIRING,
            SkillPublisherKeyStatus.ACTIVE,
        ]
        with pytest.raises(PolicyDeniedError, match="not trusted"):
            await trust.verify_for_admission("tenant-a", first_package)
        assert await trust.verify_for_restore("tenant-a", first_package) == "key-2026-a"

        suspended, _keys = await service.change_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                operation=SkillPublisherStatusOperation.SUSPEND,
                reason_code="publisher_under_review",
                command_id="suspend-1",
                expected_revision=publisher.revision,
                correlation_id="corr-a",
                causation_id="suspend-1",
            )
        )
        assert suspended.status is SkillPublisherStatus.SUSPENDED
        assert suspended.status_reason_code == "publisher_under_review"
        tenant_b, _tenant_b_keys = await service.get("tenant-b", "acme")
        assert tenant_b.status is SkillPublisherStatus.ACTIVE
        with pytest.raises(PolicyDeniedError, match="not trusted"):
            await trust.verify_for_restore("tenant-a", first_package)
        with pytest.raises(PolicyDeniedError, match="not active"):
            await service.rotate_key(
                _rotate(
                    "key-2026-c",
                    second_public,
                    suspended.revision,
                    "rotate-suspended",
                )
            )
        resumed, _keys = await service.change_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                operation=SkillPublisherStatusOperation.RESUME,
                reason_code="review_completed",
                command_id="resume-1",
                expected_revision=suspended.revision,
                correlation_id="corr-a",
                causation_id="resume-1",
            )
        )
        assert resumed.status is SkillPublisherStatus.ACTIVE
        assert resumed.status_reason_code is None
        assert await trust.verify_for_restore("tenant-a", first_package) == "key-2026-a"

        first_key = keys[0]
        await service.revoke_key(
            RevokeSkillPublisherKeyCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                key_id=first_key.key_id,
                reason_code="key_compromised",
                command_id="revoke-1",
                expected_revision=first_key.revision,
                correlation_id="corr-a",
                causation_id="revoke-1",
            )
        )
        with pytest.raises(PolicyDeniedError, match="not trusted"):
            await trust.verify_for_restore("tenant-a", first_package)
        with pytest.raises(VersionConflictError, match="command id was reused"):
            await service.register(
                _publisher_command().model_copy(
                    update={"display_name": "Different Name"}
                )
            )

    asyncio.run(scenario())


def test_ed25519_publisher_can_publish_and_key_id_is_persisted() -> None:
    async def scenario() -> None:
        private_key = Ed25519PrivateKey.generate()
        public_key = _b64(
            private_key.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        )
        publisher_store = InMemorySkillPublisherStore()
        publishers = SkillPublisherService(publisher_store)
        await publishers.register(_publisher_command())
        await publishers.rotate_key(_rotate("key-2026-a", public_key, 1, "rotate-1"))
        lifecycle = InMemorySkillLifecycleStore()
        now = datetime.now(UTC)
        source = SkillSourceRecord(
            source_id="sks_admin_upload",
            tenant_id="tenant-a",
            kind=SkillSourceKind.ADMIN_UPLOAD,
            desired_state=SkillSourceDesiredState.ENABLED,
            publisher_allowlist=("platform",),
            created_by="system",
            updated_by="system",
            created_at=now,
            updated_at=now,
        )
        artifacts = ArtifactStore(
            InMemoryObjectStorage(), signing_key=b"artifact-signing-key"
        )
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier(
                {"platform": b"platform-signing-key"}
            ),
            resources=HandsResourceRegistry(),
        )
        publication = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
            publisher_trust=SkillPublisherTrustService(publisher_store),
            bootstrap_sources=(source,),
        )
        result = await publication.publish(
            PublishSkillCommand(
                tenant_id="tenant-a",
                actor_id="publisher-admin",
                source_id=source.source_id,
                command_id="publish-ed25519",
                correlation_id="corr-a",
                causation_id="publish-ed25519",
            ),
            _package(private_key, "key-2026-a", "1.0.0"),
        )
        record = await lifecycle.get_package(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert result.manifest.publisher == "acme"
        assert record is not None and record.signature_key_id == "key-2026-a"

        second_private = Ed25519PrivateKey.generate()
        second_public = _b64(
            second_private.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        )
        publisher, keys = await publishers.get("tenant-a", "acme")
        await publishers.rotate_key(
            _rotate("key-2026-b", second_public, publisher.revision, "rotate-2")
        )
        catalog = CapabilityCatalog(InMemoryCapabilityCatalogStore())
        rebuilder = SkillStateRebuilder(
            lifecycle=lifecycle,
            artifacts=_ArtifactReader(artifacts),
            registry=registry,
            catalog=catalog,
            publisher_trust=SkillPublisherTrustService(publisher_store),
        )
        restored = await rebuilder.rebuild_tenant("tenant-a")
        assert restored == (1, ())
        current_publisher, _current_keys = await publishers.get("tenant-a", "acme")
        suspended, _current_keys = await publishers.change_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                operation=SkillPublisherStatusOperation.SUSPEND,
                reason_code="publisher_under_review",
                command_id="suspend-after-publish",
                expected_revision=current_publisher.revision,
                correlation_id="corr-a",
                causation_id="suspend-after-publish",
            )
        )
        assert await rebuilder.rebuild_tenant("tenant-a") == (
            0,
            ("package_restore_PolicyDeniedError",),
        )
        assert registry.list_publications("tenant-a") == ()
        await publishers.change_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                operation=SkillPublisherStatusOperation.RESUME,
                reason_code="review_completed",
                command_id="resume-after-publish",
                expected_revision=suspended.revision,
                correlation_id="corr-a",
                causation_id="resume-after-publish",
            )
        )
        assert await rebuilder.rebuild_tenant("tenant-a") == (1, ())
        _publisher, keys = await publishers.get("tenant-a", "acme")
        first_key = next(key for key in keys if key.key_id == "key-2026-a")
        await publishers.revoke_key(
            RevokeSkillPublisherKeyCommand(
                tenant_id="tenant-a",
                actor_id="security-admin",
                publisher="acme",
                key_id=first_key.key_id,
                reason_code="key_compromised",
                command_id="revoke-after-publish",
                expected_revision=first_key.revision,
                correlation_id="corr-a",
                causation_id="revoke-after-publish",
            )
        )
        count, failures = await rebuilder.rebuild_tenant("tenant-a")
        assert count == 0
        assert failures == ("package_restore_PolicyDeniedError",)
        assert registry.list_publications("tenant-a") == ()

    asyncio.run(scenario())
