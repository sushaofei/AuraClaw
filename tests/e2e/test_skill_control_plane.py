from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import sys
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
    SkillBindingStatusExecutor,
    skill_binding_status_tool,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    skill_package_digest,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.contracts.capabilities import CapabilityKind
from auraclaw.contracts.skills import (
    SkillManifest,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.identity import DevelopmentHeaderIdentityVerifier

_PLATFORM_KEY = b"auraclaw-development-platform-skill-key"


class _ArtifactReader:
    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    async def read(self, **kwargs: object) -> bytes:
        artifact_ref = kwargs["artifact_ref"]
        assert isinstance(artifact_ref, ArtifactRef)
        tenant_id = str(kwargs["tenant_id"])
        actor_id = str(kwargs["actor_id"])
        token = await self._store.issue_download_token(
            tenant_id=tenant_id,
            artifact_id=artifact_ref.artifact_id,
            actor_id=actor_id,
        )
        return await self._store.download(
            token=token,
            tenant_id=tenant_id,
            actor_id=actor_id,
        )


def _package() -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _PLATFORM_KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="9.0.0",
        description="Prepare a governed release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {
        "SKILL.md": b"# Release\n\nPrepare the governed release.\n",
        "tests/basic.json": json.dumps(
            {"name": "basic", "input": {}, "expected_output": {}}
        ).encode(),
    }
    manifest = unsigned.model_copy(
        update={"signature": verifier.sign(unsigned, files)}
    )
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_real_cli_admin_api_catalog_and_lifecycle_e2e(tmp_path) -> None:
    package = _package()
    for relative_path, content in package.files.items():
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    environment = {
        **os.environ,
        "AURACLAW_SKILL_SIGNING_KEY": _PLATFORM_KEY.decode(),
    }
    validated = subprocess.run(
        [sys.executable, "-m", "auraclaw", "skills", "validate", str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    validation = json.loads(validated.stdout)
    assert validation == {
        "name": "release.prepare",
        "package_digest": skill_package_digest(package),
        "publisher": "platform",
        "version": "9.0.0",
    }
    tested = subprocess.run(
        [sys.executable, "-m", "auraclaw", "skills", "test", str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert json.loads(tested.stdout) == {"declarative_test_vectors": 1}

    artifacts = ArtifactStore(
        InMemoryObjectStorage(), signing_key=b"e2e-artifact-signing-key"
    )
    registry = SkillPackageRegistry(
        artifacts=artifacts,
        signature_verifier=HmacSkillSignatureVerifier({"platform": _PLATFORM_KEY}),
        resources=HandsResourceRegistry(),
    )
    lifecycle = InMemorySkillLifecycleStore()
    catalog = CapabilityCatalog(InMemoryCapabilityCatalogStore())
    now = datetime.now(UTC)
    publication = SkillPublicationService(
        registry=registry,
        lifecycle=lifecycle,
        bootstrap_sources=(
            SkillSourceRecord(
                source_id="sks_admin_upload",
                tenant_id="tenant-a",
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
    rebuilder = SkillStateRebuilder(
        lifecycle=lifecycle,
        artifacts=_ArtifactReader(artifacts),
        registry=registry,
        catalog=catalog,
    )
    management = SkillManagementService(lifecycle=lifecycle, projector=rebuilder)
    app = FastAPI()
    app.state.identity_verifier = DevelopmentHeaderIdentityVerifier()
    app.include_router(
        create_skill_admin_router(
            registry,
            publication_service=publication,
            management_service=management,
        )
    )
    identity = {"X-Tenant-ID": "tenant-a", "X-Actor-ID": "admin-a"}
    with TestClient(app) as client:
        published = client.post(
            "/v1/admin/skill-publications",
            headers={
                **identity,
                "Idempotency-Key": "e2e-publish",
                "X-Expected-Revision": "0",
            },
            json={
                "source_id": "sks_admin_upload",
                "activate": True,
                "files": {
                    path: base64.b64encode(content).decode()
                    for path, content in package.files.items()
                },
            },
        )
        assert published.status_code == 201, published.text
        assert published.json()["package_digest"] == validation["package_digest"]
        assert asyncio.run(rebuilder.rebuild_tenant("tenant-a")) == (1, ())
        matches = asyncio.run(
            catalog.search(
                tenant_id="tenant-a",
                query="release",
                kinds=(CapabilityKind.SKILL,),
            )
        )
        assert len(matches) == 1
        assert matches[0].canonical_name == "release.prepare"
        assert asyncio.run(
            catalog.search(
                tenant_id="tenant-b",
                query="release",
                kinds=(CapabilityKind.SKILL,),
            )
        ) == ()

        forced = client.post(
            "/v1/admin/skills/platform/release.prepare:uninstall?force=true",
            headers={
                **identity,
                "Idempotency-Key": "e2e-uninstall",
                "X-Expected-Revision": "1",
                "X-Reason-Code": "e2e_force_uninstall",
            },
        )
        assert forced.status_code == 202, forced.text
        assert forced.json()["installation"]["status"] == "uninstalled"
        assert asyncio.run(
            catalog.search(
                tenant_id="tenant-a",
                query="release",
                kinds=(CapabilityKind.SKILL,),
            )
        ) == ()

        installed = client.post(
            "/v1/admin/skills/platform/release.prepare:install",
            headers={
                **identity,
                "Idempotency-Key": "e2e-install",
                "X-Expected-Revision": "2",
            },
        )
        assert installed.status_code == 202, installed.text
        assert len(
            asyncio.run(
                catalog.search(
                    tenant_id="tenant-a",
                    query="release",
                    kinds=(CapabilityKind.SKILL,),
                )
            )
        ) == 1

        revoked = client.post(
            "/v1/admin/skill-publications/platform/release.prepare/versions/9.0.0:revoke",
            headers={
                **identity,
                "Idempotency-Key": "e2e-revoke",
                "X-Expected-Revision": "1",
                "X-Reason-Code": "e2e_security_revoke",
                "X-Skill-Revocation-Action": "cancel",
            },
        )
        assert revoked.status_code == 202, revoked.text

    asyncio.run(rebuilder.rebuild_tenant("tenant-a"))
    assert asyncio.run(
        catalog.search(
            tenant_id="tenant-a",
            query="release",
            kinds=(CapabilityKind.SKILL,),
        )
    ) == ()
    assert asyncio.run(
        catalog.search(
            tenant_id="tenant-b",
            query="release",
            kinds=(CapabilityKind.SKILL,),
        )
    ) == ()
    disposition = asyncio.run(
        SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="e2e-binding-status",
                tenant_id="tenant-a",
                root_session_id="root-e2e",
                session_id="session-e2e",
                run_id="run-e2e",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "9.0.0",
                    "package_digest": validation["package_digest"],
                },
                expected_side_effect="read",
                idempotency_key="e2e-binding-status",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-e2e",
            ),
            skill_binding_status_tool(),
        )
    )
    assert disposition["action"] == "cancel"
    assert disposition["reason_code"] == "e2e_security_revoke"
