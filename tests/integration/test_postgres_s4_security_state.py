import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    PolicyEvaluateRequest,
    PolicyValidateDecisionRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_credential_registry import (
    PostgresCredentialRegistry,
)
from auraclaw.infrastructure.persistence.postgres_policy_store import (
    PostgresPolicyStateStore,
)
from auraclaw.policy.internal_service import PolicyInternalService

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0009_s3_owner_boundaries.sql").read_text()
POLICY_VERSION_MIGRATION = (ROOT / "migrations/0014_s4_policy_version.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_policy_and_credential_replicas_share_decisions_revocation_and_audit() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        await connection.execute(POLICY_VERSION_MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-security-s4-{suffix}"
        credential_ref = f"credential-{suffix}"
        policy_a = PostgresPolicyStateStore(DATABASE_URL)
        policy_b = PostgresPolicyStateStore(DATABASE_URL)
        registry_a = PostgresCredentialRegistry(DATABASE_URL)
        registry_b = PostgresCredentialRegistry(DATABASE_URL)
        context = InternalRequestContext(
            tenant_id=tenant_id,
            service_identity=ServiceIdentity.ACTION_HANDS,
            request_id=f"request-{suffix}",
            correlation_id=f"run-{suffix}",
            causation_id=f"run-{suffix}",
        )
        try:
            decision = await PolicyInternalService(
                version="s3-v1", store=policy_a
            ).evaluate(
                PolicyEvaluateRequest(
                    context=context,
                    subject="action-hands",
                    action="read",
                    resource="managed-tool",
                    input_digest="digest",
                    attributes={"permission": "read-only", "risk_level": "low"},
                )
            )
            validated = await PolicyInternalService(
                version="s3-v1", store=policy_b
            ).validate_decision(
                PolicyValidateDecisionRequest(
                    context=context,
                    decision_id=decision.decision_id,
                    action="read",
                    resource="managed-tool",
                )
            )
            assert validated.valid
            assert validated.policy_version == "s3-v1"
            assert not await policy_b.ensure_active_version("s4-drifted")

            vault = InMemoryVault({credential_ref: "top-secret"})
            proxy_a = CredentialProxy(vault, registry=registry_a)
            await proxy_a.save_reference(
                tenant_id,
                CredentialReference(
                    credential_ref=credential_ref,
                    provider="managed",
                    account_scope="account",
                    allowed_operations=("read",),
                    expires_at=datetime.now(UTC) + timedelta(minutes=5),
                ),
            )
            response = await proxy_a.invoke(
                tenant_id=tenant_id,
                session_id=f"session-{suffix}",
                tool_name="managed",
                credential_ref=credential_ref,
                operation="read",
                request={},
                adapter=lambda request, secret: {"ok": bool(secret), **request},
                policy_decision_id=decision.decision_id,
                usage_id=f"usage-{suffix}",
            )
            assert response == {"ok": True}
            audit = await connection.fetchrow(
                "SELECT status,side_effect_status FROM credential.usage_audit WHERE usage_id=$1",
                f"usage-{suffix}",
            )
            assert audit is not None
            assert dict(audit) == {
                "status": "completed",
                "side_effect_status": "completed",
            }

            await registry_b.revoke_reference(tenant_id, credential_ref)
            with pytest.raises(CredentialAccessError, match="not valid"):
                await proxy_a.invoke(
                    tenant_id=tenant_id,
                    session_id=f"session-{suffix}",
                    tool_name="managed",
                    credential_ref=credential_ref,
                    operation="read",
                    request={},
                    adapter=lambda request, secret: request,
                )
        finally:
            await policy_a.close()
            await policy_b.close()
            await registry_a.close()
            await registry_b.close()
            await connection.execute(
                "DELETE FROM credential.usage_audit WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM credential.reference WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM policy.decision WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
