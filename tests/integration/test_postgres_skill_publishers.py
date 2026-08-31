from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from auraclaw.config import get_settings
from auraclaw.contracts.errors import PolicyDeniedError, VersionConflictError
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillPublisherKeyStatus,
    SkillPublisherStatus,
    SkillPublisherStatusOperation,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_publishers import (
    PostgresSkillPublisherStore,
)

SETTINGS = get_settings()
DATABASE_URL = (
    asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
)
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = "\n".join(
    (
        (ROOT / "migrations/0027_skill_publisher_registry.sql").read_text(),
        (ROOT / "migrations/0030_skill_publisher_suspension.sql").read_text(),
        (ROOT / "migrations/0039_skill_publisher_runtime_revocation.sql").read_text(),
    )
)
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None, reason="PostgreSQL test URL not configured"
)


async def _ensure_skill_publisher_schema(connection: asyncpg.Connection) -> None:
    status_constraint = await connection.fetchval(
        """SELECT pg_get_constraintdef(constraint_record.oid)
            FROM pg_constraint constraint_record
            JOIN pg_class relation ON relation.oid=constraint_record.conrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE constraint_record.conname='skill_publisher_status_evidence_check'
              AND namespace.nspname='hands' AND relation.relname='skill_publisher'"""
    )
    key_constraint = await connection.fetchval(
        """SELECT EXISTS(
            SELECT 1 FROM pg_constraint constraint_record
            JOIN pg_class relation ON relation.oid=constraint_record.conrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE constraint_record.conname='skill_publisher_key_revocation_policy_check'
              AND namespace.nspname='hands' AND relation.relname='skill_publisher_key'
        )"""
    )
    current = (
        key_constraint
        and status_constraint is not None
        and "revoked" in str(status_constraint)
        and "security_action" in str(status_constraint)
    )
    if current:
        return
    await connection.execute(
        """ALTER TABLE IF EXISTS hands.skill_publisher_key
        DROP CONSTRAINT IF EXISTS skill_publisher_key_revocation_policy_check"""
    )
    await connection.execute(MIGRATION)


def _public_key() -> str:
    raw = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def test_postgres_publisher_rotation_is_atomic_and_idempotent() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await _ensure_skill_publisher_schema(connection)
        suffix = uuid4().hex
        tenant_id = f"tenant-publisher-{suffix}"
        publisher = f"publisher-{suffix}"
        store_a = PostgresSkillPublisherStore(DATABASE_URL)
        store_b = PostgresSkillPublisherStore(DATABASE_URL)
        register = RegisterSkillPublisherCommand(
            tenant_id=tenant_id,
            actor_id="security-admin",
            publisher=publisher,
            display_name="Integration Publisher",
            command_id=f"register-{suffix}",
            correlation_id=f"corr-{suffix}",
            causation_id=f"register-{suffix}",
        )
        try:
            created = await store_a.register_publisher(register)
            assert await store_b.register_publisher(register) == created
            first = RotateSkillPublisherKeyCommand(
                tenant_id=tenant_id,
                actor_id="security-admin",
                publisher=publisher,
                key_id="key-a",
                public_key=_public_key(),
                command_id=f"rotate-a-{suffix}",
                expected_revision=1,
                correlation_id=f"corr-{suffix}",
                causation_id=f"rotate-a-{suffix}",
            )
            publisher_record, first_key = await store_a.rotate_key(first)
            assert await store_b.rotate_key(first) == (publisher_record, first_key)
            with pytest.raises(VersionConflictError, match="key already exists"):
                await store_b.rotate_key(
                    first.model_copy(
                        update={
                            "command_id": f"rotate-duplicate-{suffix}",
                            "causation_id": f"rotate-duplicate-{suffix}",
                            "expected_revision": publisher_record.revision,
                        }
                    )
                )
            assert (await store_a.list_keys(tenant_id, publisher))[0].status is (
                SkillPublisherKeyStatus.ACTIVE
            )
            assert await store_a.get_publisher(tenant_id, publisher) == publisher_record
            second = first.model_copy(
                update={
                    "key_id": "key-b",
                    "public_key": _public_key(),
                    "command_id": f"rotate-b-{suffix}",
                    "causation_id": f"rotate-b-{suffix}",
                    "expected_revision": publisher_record.revision,
                }
            )
            publisher_record, second_key = await store_b.rotate_key(second)
            keys = await store_a.list_keys(tenant_id, publisher)
            assert [key.status for key in keys] == [
                SkillPublisherKeyStatus.RETIRING,
                SkillPublisherKeyStatus.ACTIVE,
            ]
            with pytest.raises(VersionConflictError, match="command id was reused"):
                await store_a.rotate_key(
                    second.model_copy(update={"public_key": _public_key()})
                )
            suspend = ChangeSkillPublisherStatusCommand(
                tenant_id=tenant_id,
                actor_id="security-admin",
                publisher=publisher,
                operation=SkillPublisherStatusOperation.SUSPEND,
                reason_code="publisher_under_review",
                command_id=f"suspend-{suffix}",
                expected_revision=publisher_record.revision,
                correlation_id=f"corr-{suffix}",
                causation_id=f"suspend-{suffix}",
            )
            suspended = await store_a.change_status(suspend)
            assert await store_b.change_status(suspend) == suspended
            assert suspended.status is SkillPublisherStatus.SUSPENDED
            assert suspended.status_reason_code == "publisher_under_review"
            assert suspended.security_action.value == "pause"
            assert suspended.security_policy_version == "skill-revocation-v1"
            with pytest.raises(PolicyDeniedError, match="not active"):
                await store_b.rotate_key(
                    second.model_copy(
                        update={
                            "key_id": "key-c",
                            "public_key": _public_key(),
                            "command_id": f"rotate-suspended-{suffix}",
                            "causation_id": f"rotate-suspended-{suffix}",
                            "expected_revision": suspended.revision,
                        }
                    )
                )
            resume = suspend.model_copy(
                update={
                    "operation": SkillPublisherStatusOperation.RESUME,
                    "reason_code": "review_completed",
                    "command_id": f"resume-{suffix}",
                    "causation_id": f"resume-{suffix}",
                    "expected_revision": suspended.revision,
                }
            )
            resumed = await store_b.change_status(resume)
            assert resumed.status is SkillPublisherStatus.ACTIVE
            assert resumed.status_reason_code is None
            revoked = await store_a.revoke_key(
                RevokeSkillPublisherKeyCommand(
                    tenant_id=tenant_id,
                    actor_id="security-admin",
                    publisher=publisher,
                    key_id=second_key.key_id,
                    reason_code="key_compromised",
                    command_id=f"revoke-{suffix}",
                    expected_revision=second_key.revision,
                    correlation_id=f"corr-{suffix}",
                    causation_id=f"revoke-{suffix}",
                )
            )
            assert revoked.status is SkillPublisherKeyStatus.REVOKED
            assert revoked.revocation_action.value == "cancel"
            assert revoked.revocation_policy_version == "skill-revocation-v1"
            assert (await store_b.get_publisher(tenant_id, publisher)) == resumed

            revoke_publisher = ChangeSkillPublisherStatusCommand(
                tenant_id=tenant_id,
                actor_id="security-admin",
                publisher=publisher,
                operation=SkillPublisherStatusOperation.REVOKE,
                reason_code="publisher_compromised",
                revocation_action="cancel",
                policy_version="skill-revocation-v2",
                policy_decision_id="decision-permanent-revoke",
                command_id=f"revoke-publisher-{suffix}",
                expected_revision=resumed.revision,
                correlation_id=f"corr-{suffix}",
                causation_id=f"revoke-publisher-{suffix}",
            )
            permanently_revoked = await store_a.change_status(revoke_publisher)
            assert await store_b.change_status(revoke_publisher) == permanently_revoked
            assert permanently_revoked.status is SkillPublisherStatus.REVOKED
            assert permanently_revoked.security_action.value == "cancel"
            assert permanently_revoked.security_policy_version == "skill-revocation-v2"
            with pytest.raises(PolicyDeniedError, match="cannot change status"):
                await store_b.change_status(
                    resume.model_copy(
                        update={
                            "command_id": f"resume-revoked-{suffix}",
                            "causation_id": f"resume-revoked-{suffix}",
                            "expected_revision": permanently_revoked.revision,
                        }
                    )
                )
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.skill_publisher_command WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_publisher_key WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_publisher WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
