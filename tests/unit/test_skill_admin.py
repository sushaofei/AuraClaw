from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime

from fastapi.testclient import TestClient

import auraclaw.composition.services as services
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import (
    InProcessSkillStateProjector,
    SkillManagementService,
)
from auraclaw.action.skill_packages import HmacSkillSignatureVerifier, SkillPackage
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.composition.api import create_app
from auraclaw.composition.providers import (
    get_approval_projection,
    get_event_store,
    get_task_projection,
    get_task_service,
)
from auraclaw.composition.services import create_service_app
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    SkillManifest,
    SkillResourceRequirement,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillToolRequirement,
)

_PLATFORM_KEY = b"auraclaw-development-platform-skill-key"


def setup_function() -> None:
    get_settings().storage_backend = "memory"
    get_settings().runtime_event_backend = "memory"
    get_settings().allow_insecure_identity_headers = True
    get_task_service.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_approval_projection.cache_clear()
    services._SKILL_PACKAGE_REGISTRY = None


def _package() -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _PLATFORM_KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="1.4.0",
        description="Prepare an auditable release",
        applies_when=("repository release requested",),
        required_tools=(
            SkillToolRequirement(name="github.pull_request.get", version=">=2,<3"),
        ),
        required_resources=(
            SkillResourceRequirement(uri_template="repo://{repo}/release-policy"),
        ),
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": b"# Release\n\nPrepare the audited release."}
    signature = verifier.sign(unsigned, files)
    manifest = unsigned.model_copy(update={"signature": signature})
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_skill_admin_manages_installation_and_revocation_separately() -> None:
    app = create_app(profile="task-api")
    registry = services._skill_registry_service(get_settings())
    lifecycle = InMemorySkillLifecycleStore()
    now = datetime.now(UTC)
    publication_service = SkillPublicationService(
        registry=registry,
        lifecycle=lifecycle,
        bootstrap_sources=(
            SkillSourceRecord(
                source_id="sks_admin_upload",
                tenant_id="tenant-1",
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
    management = SkillManagementService(
        lifecycle=lifecycle,
        projector=InProcessSkillStateProjector(
            lifecycle=lifecycle,
            registry=registry,
        ),
    )
    app.include_router(
        create_skill_admin_router(
            registry,
            publication_service=publication_service,
            management_service=management,
        )
    )
    asyncio.run(
        publication_service.publish(
            PublishSkillCommand(
                tenant_id="tenant-1",
                actor_id="admin-1",
                source_id="sks_admin_upload",
                command_id="publish-1",
                correlation_id="corr-1",
                causation_id="publish-1",
            ),
            _package(),
        )
    )
    headers = {"X-Tenant-ID": "tenant-1", "X-Actor-ID": "admin-1"}
    with TestClient(app) as client:
        listed = client.get("/v1/admin/skills", headers=headers)
        assert listed.status_code == 200, listed.text
        skills = listed.json()["skills"]
        assert len(skills) == 1
        assert skills[0]["name"] == "release.prepare"
        assert skills[0]["status"] == "active"

        detail = client.get(
            "/v1/admin/skills/platform/release.prepare", headers=headers
        )
        assert detail.status_code == 200
        assert "Prepare the audited release" in detail.json()["skill_markdown"]

        installation_state = client.get(
            "/v1/admin/skills/platform/release.prepare/installation",
            headers=headers,
        )
        assert installation_state.status_code == 200
        assert installation_state.json()["installation"]["revision"] == 1

        disabled = client.post(
            "/v1/admin/skills/platform/release.prepare:disable",
            headers={
                **headers,
                "Idempotency-Key": "skill-disable-1",
                "X-Expected-Revision": "1",
                "X-Reason-Code": "tenant_disabled",
            },
        )
        assert disabled.status_code == 202, disabled.text
        assert disabled.json()["installation"]["status"] == "disabled"
        assert registry.get_publication(
            "tenant-1", "platform", "release.prepare", "1.4.0"
        ).status.value == "active"

        enabled = client.post(
            "/v1/admin/skills/platform/release.prepare:enable",
            headers={
                **headers,
                "Idempotency-Key": "skill-enable-1",
                "X-Expected-Revision": "2",
            },
        )
        assert enabled.status_code == 202
        assert enabled.json()["installation"]["status"] == "active"

        uninstalled = client.post(
            "/v1/admin/skills/platform/release.prepare:uninstall",
            headers={
                **headers,
                "Idempotency-Key": "skill-uninstall-1",
                "X-Expected-Revision": "3",
                "X-Reason-Code": "no_longer_needed",
            },
        )
        assert uninstalled.status_code == 202, uninstalled.text
        assert uninstalled.json()["installation"]["status"] == "uninstalled"

        installed = client.post(
            "/v1/admin/skills/platform/release.prepare:install",
            headers={
                **headers,
                "Idempotency-Key": "skill-install-1",
                "X-Expected-Revision": "4",
            },
        )
        assert installed.status_code == 202, installed.text
        assert installed.json()["installation"]["status"] == "active"

        revoked = client.post(
            "/v1/admin/skill-publications/platform/release.prepare/versions/1.4.0:revoke",
            headers={
                **headers,
                "Idempotency-Key": "skill-revoke-1",
                "X-Expected-Revision": "1",
                "X-Reason-Code": "publisher_key_compromised",
            },
        )
        assert revoked.status_code == 202, revoked.text
        assert revoked.json()["publication"]["status"] == "revoked"
        publication_state = client.get(
            "/v1/admin/skill-publications/platform/release.prepare/versions/1.4.0",
            headers=headers,
        )
        assert publication_state.status_code == 200
        assert publication_state.json()["publication"]["revision"] == 2


def test_task_api_service_exposes_skill_admin_routes() -> None:
    settings = Settings(
        _env_file=None,
        storage_backend="postgres",
        db_host="localhost",
        db_user="auraclaw",
        db_password="auraclaw",
        db_name="auraclaw",
        allow_insecure_identity_headers=True,
        task_api_workload_token="task-api-token",
    )
    paths = set(create_service_app("api", settings).openapi()["paths"])
    assert "/v1/admin/skills" in paths
    assert "/v1/admin/skills/{publisher}/{name}:enable" in paths
    assert "/v1/admin/skills/{publisher}/{name}:disable" in paths
    assert "/v1/admin/skills/{publisher}/{name}:install" in paths
    assert "/v1/admin/skills/{publisher}/{name}:uninstall" in paths
    assert "/v1/admin/skills/{publisher}/{name}/installation" in paths
    assert (
        "/v1/admin/skill-publications/{publisher}/{name}/versions/{version}"
        in paths
    )
    assert (
        "/v1/admin/skill-publications/{publisher}/{name}/versions/{version}:revoke"
        in paths
    )
    assert "/v1/admin/skill-publications" in paths


def test_skill_admin_publishes_base64_package_through_application_service() -> None:
    app = create_app(profile="task-api")
    registry = services._skill_registry_service(get_settings())
    now = datetime.now(UTC)
    lifecycle = InMemorySkillLifecycleStore()
    publication_service = SkillPublicationService(
        registry=registry,
        lifecycle=lifecycle,
        bootstrap_sources=(
            SkillSourceRecord(
                source_id="sks_admin_upload",
                tenant_id="tenant-1",
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
    app.include_router(
        create_skill_admin_router(
            registry,
            publication_service=publication_service,
        )
    )
    package = _package()
    payload = {
        "source_id": "sks_admin_upload",
        "activate": True,
        "files": {
            path: base64.b64encode(content).decode()
            for path, content in package.files.items()
        },
    }
    headers = {
        "X-Tenant-ID": "tenant-1",
        "X-Actor-ID": "admin-1",
        "Idempotency-Key": "skill-publish-1",
        "X-Expected-Revision": "0",
    }
    with TestClient(app) as client:
        response = client.post(
            "/v1/admin/skill-publications", json=payload, headers=headers
        )
    assert response.status_code == 201, response.text
    assert response.json()["status"] == "active"
    assert response.json()["publisher"] == "platform"
