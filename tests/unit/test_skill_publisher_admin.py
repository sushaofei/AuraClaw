from __future__ import annotations

import base64

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

import auraclaw.composition.services as services
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
)
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.composition.api import create_app
from auraclaw.composition.providers import (
    get_approval_projection,
    get_event_store,
    get_task_projection,
    get_task_service,
)
from auraclaw.config import get_settings


def setup_function() -> None:
    get_settings().storage_backend = "memory"
    get_settings().runtime_event_backend = "memory"
    get_settings().allow_insecure_identity_headers = True
    get_task_service.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_approval_projection.cache_clear()
    services._SKILL_PACKAGE_REGISTRY = None


def _public_key() -> str:
    raw = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def test_admin_manages_tenant_publisher_and_key_rotation() -> None:
    app = create_app(profile="task-api")
    publishers = SkillPublisherService(InMemorySkillPublisherStore())
    app.include_router(
        create_skill_admin_router(
            services._skill_registry_service(get_settings()),
            publisher_service=publishers,
        )
    )
    identity = {"X-Tenant-ID": "tenant-a", "X-Actor-ID": "security-admin"}
    with TestClient(app) as client:
        registered = client.post(
            "/v1/admin/skill-publishers/acme",
            headers={**identity, "Idempotency-Key": "register-1"},
            json={"display_name": "Acme Skills"},
        )
        assert registered.status_code == 201, registered.text
        assert registered.json()["publisher"]["revision"] == 1

        rotated = client.post(
            "/v1/admin/skill-publishers/acme/keys:rotate",
            headers={
                **identity,
                "Idempotency-Key": "rotate-1",
                "X-Expected-Revision": "1",
            },
            json={"key_id": "key-2026-a", "public_key": _public_key()},
        )
        assert rotated.status_code == 202, rotated.text
        assert rotated.json()["keys"][0]["status"] == "active"

        suspended = client.post(
            "/v1/admin/skill-publishers/acme/status:suspend",
            headers={
                **identity,
                "Idempotency-Key": "suspend-1",
                "X-Expected-Revision": "2",
                "X-Reason-Code": "publisher_under_review",
            },
        )
        assert suspended.status_code == 202, suspended.text
        assert suspended.json()["publisher"]["status"] == "suspended"
        assert suspended.json()["publisher"]["security_action"] == "pause"
        assert (
            suspended.json()["publisher"]["security_policy_version"]
            == "skill-revocation-v1"
        )
        resumed = client.post(
            "/v1/admin/skill-publishers/acme/status:resume",
            headers={
                **identity,
                "Idempotency-Key": "resume-1",
                "X-Expected-Revision": "3",
                "X-Reason-Code": "review_completed",
            },
        )
        assert resumed.status_code == 202, resumed.text
        assert resumed.json()["publisher"]["status"] == "active"

        state = client.get(
            "/v1/admin/skill-publishers/acme", headers=identity
        )
        assert state.status_code == 200
        key = state.json()["keys"][0]
        revoked = client.post(
            "/v1/admin/skill-publishers/acme/keys/key-2026-a:revoke",
            headers={
                **identity,
                "Idempotency-Key": "revoke-1",
                "X-Expected-Revision": str(key["revision"]),
                "X-Reason-Code": "key_compromised",
            },
        )
        assert revoked.status_code == 202, revoked.text
        assert revoked.json()["keys"][0]["status"] == "revoked"
        assert revoked.json()["keys"][0]["revocation_action"] == "cancel"

        publisher_revoked = client.post(
            "/v1/admin/skill-publishers/acme/status:revoke",
            headers={
                **identity,
                "Idempotency-Key": "publisher-revoke-1",
                "X-Expected-Revision": "4",
                "X-Reason-Code": "publisher_compromised",
                "X-Revocation-Action": "cancel",
                "X-Policy-Version": "skill-revocation-v2",
                "X-Policy-Decision-ID": "decision-publisher-revoke",
            },
        )
        assert publisher_revoked.status_code == 202, publisher_revoked.text
        publisher_payload = publisher_revoked.json()["publisher"]
        assert publisher_payload["status"] == "revoked"
        assert publisher_payload["security_action"] == "cancel"
        assert publisher_payload["security_policy_version"] == "skill-revocation-v2"

        rejected_resume = client.post(
            "/v1/admin/skill-publishers/acme/status:resume",
            headers={
                **identity,
                "Idempotency-Key": "resume-revoked",
                "X-Expected-Revision": "5",
                "X-Reason-Code": "unsafe_resume",
            },
        )
        assert rejected_resume.status_code == 403, rejected_resume.text
