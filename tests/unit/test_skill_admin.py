from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

import auraclaw.composition.services as services
from auraclaw.action.skill_packages import HmacSkillSignatureVerifier, SkillPackage
from auraclaw.composition.providers import (
    get_approval_projection,
    get_event_store,
    get_task_projection,
    get_task_service,
)
from auraclaw.composition.services import create_service_app
from auraclaw.config import get_settings
from auraclaw.contracts.skills import (
    SkillManifest,
    SkillResourceRequirement,
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


def test_skill_admin_lists_and_toggles_publication() -> None:
    app = create_service_app("api")
    registry = services._skill_registry_service(get_settings())
    asyncio.run(registry.publish("tenant-1", _package()))
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

        disabled = client.post(
            "/v1/admin/skills/platform/release.prepare:disable",
            headers={**headers, "Idempotency-Key": "skill-disable-1"},
        )
        assert disabled.status_code == 202, disabled.text
        assert disabled.json()["skills"][0]["status"] == "revoked"

        enabled = client.post(
            "/v1/admin/skills/platform/release.prepare:enable",
            headers={**headers, "Idempotency-Key": "skill-enable-1"},
        )
        assert enabled.status_code == 202
        assert enabled.json()["skills"][0]["status"] == "active"


def test_task_api_service_exposes_skill_admin_routes() -> None:
    paths = set(create_service_app("api").openapi()["paths"])
    assert "/v1/admin/skills" in paths
    assert "/v1/admin/skills/{publisher}/{name}:enable" in paths
    assert "/v1/admin/skills/{publisher}/{name}:disable" in paths
