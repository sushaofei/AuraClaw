import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.admin.internal_service import (
    OwnerAdminService,
    admin_operation_request_digest,
)
from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.config import get_settings
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.internal import (
    AdminOperationRequest,
    InternalRequestContext,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import (
    CredentialReference,
    ToolInvocation,
    ToolResult,
    ToolResultStatus,
)
from auraclaw.infrastructure.persistence.postgres_admin_store import (
    AdminSchema,
    PostgresAdminOperationStore,
)
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_credential_registry import (
    PostgresCredentialRegistry,
)
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
)
from auraclaw.infrastructure.persistence.postgres_policy_store import (
    PostgresPolicyStateStore,
)
from auraclaw.infrastructure.persistence.postgres_tool_registry import (
    PostgresToolRegistryStore,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = "\n".join(
    (ROOT / path).read_text()
    for path in (
        "migrations/0009_s3_owner_boundaries.sql",
        "migrations/0013_s4_artifact_lifecycle.sql",
        "migrations/0043_admin_operation_claims.sql",
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_s3_owner_state_survives_process_local_clients() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-s3-{suffix}"
        invocation_id = f"tool-{suffix}"
        decision_id = f"decision-{suffix}"
        artifact_id = f"art-{suffix}"
        credential_ref = f"cred-{suffix}"
        invocation_store = PostgresInvocationStore(DATABASE_URL)
        policy_store = PostgresPolicyStateStore(DATABASE_URL)
        credential_store = PostgresCredentialRegistry(DATABASE_URL)
        artifact_store = PostgresArtifactRepository(DATABASE_URL)
        admin_store = PostgresAdminOperationStore(DATABASE_URL, schema="projection")
        admin_store_b = PostgresAdminOperationStore(DATABASE_URL, schema="projection")
        tool_registry_store = PostgresToolRegistryStore(DATABASE_URL)
        try:
            invocation = ToolInvocation(
                tool_invocation_id=invocation_id,
                tenant_id=tenant_id,
                root_session_id="root-s3",
                session_id="session-s3",
                run_id="run-s3",
                tool_name="managed",
                tool_version="1",
                arguments={"value": 1},
                expected_side_effect="write",
                idempotency_key=f"idem-{suffix}",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-s3",
            )
            assert not (await invocation_store.begin(invocation, "digest-a")).conflict
            result = ToolResult(
                status=ToolResultStatus.SUCCESS,
                content={"accepted": True},
                side_effect_status="completed",
            )
            await invocation_store.complete(invocation, result)
            cached = await invocation_store.begin(invocation, "digest-a")
            assert cached.cached_result == result
            assert (await invocation_store.begin(invocation, "digest-b")).conflict
            interrupted = ToolInvocation(
                **{
                    **invocation.__dict__,
                    "tool_invocation_id": f"interrupted-{suffix}",
                    "idempotency_key": f"interrupted-idem-{suffix}",
                }
            )
            await invocation_store.begin(interrupted, "digest-interrupted")
            recovered = await invocation_store.begin(interrupted, "digest-interrupted")
            assert recovered.cached_result.status is ToolResultStatus.UNKNOWN

            context = InternalRequestContext(
                tenant_id=tenant_id,
                service_identity=ServiceIdentity.ACTION_HANDS,
                request_id=f"request-{suffix}",
                correlation_id="run-s3",
                causation_id=invocation_id,
            )
            policy_request = PolicyEvaluateRequest(
                context=context,
                subject="runtime-s3",
                action="write",
                resource="managed",
                input_digest="digest-a",
                attributes={"permission": "write-autonomous"},
            )
            policy_response = PolicyEvaluateResponse(
                decision_id=decision_id,
                decision="allow",
                policy_version="s3-v1",
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
            await policy_store.record_decision(policy_request, policy_response)
            validated = await policy_store.validate_decision(
                PolicyValidateDecisionRequest(
                    context=context,
                    decision_id=decision_id,
                    action="write",
                    resource="managed",
                )
            )
            assert validated.valid

            reference = CredentialReference(
                credential_ref=credential_ref,
                provider="managed",
                account_scope="account-s3",
                allowed_operations=("write",),
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
            await credential_store.save_reference(tenant_id, reference)
            stored_reference = await credential_store.get_reference(
                tenant_id, credential_ref
            )
            assert stored_reference == reference
            await credential_store.revoke_reference(tenant_id, credential_ref)
            assert await credential_store.get_reference(tenant_id, credential_ref) is None

            pending = PendingUpload(
                tenant_id=tenant_id,
                artifact_id=artifact_id,
                upload_id=f"upload-{suffix}",
                object_key=f"tenants/{tenant_id}/artifacts/{artifact_id}/v1/object",
                root_session_id="root-s3",
                session_id="session-s3",
                name="result.json",
                media_type="application/json",
                expected_size=2,
                expected_checksum="checksum-s3",
                classification="internal",
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
            await artifact_store.save_pending(pending)
            await artifact_store.mark_ready(pending, 1)
            assert await artifact_store.get_ready(tenant_id, artifact_id, 1) == pending

            await connection.execute(
                """INSERT INTO hands.tool_capability
                (tool_name,version,description,input_schema,output_schema,permission,
                 risk_level,runtime_location,owner,allowed_credential_operations)
                VALUES ($1,'1','integration','{}','{}','read-only','low','hands',
                        'integration','[]')""",
                f"tool-{suffix}",
            )
            registry = ToolRegistry()
            assert await tool_registry_store.load_into(registry) >= 1
            assert registry.get(f"tool-{suffix}", "1").owner == "integration"

            calls = 0
            handler_started = asyncio.Event()
            release_handler = asyncio.Event()

            async def admin_handler(parameters: dict[str, object]) -> dict[str, object]:
                nonlocal calls
                calls += 1
                handler_started.set()
                await release_handler.wait()
                return parameters

            admin_request = AdminOperationRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=f"admin-{suffix}",
                    correlation_id=f"admin-{suffix}",
                    causation_id=f"admin-{suffix}",
                ),
                operation_id=f"admin-{suffix}",
                owner_service=ServiceIdentity.PROJECTION_WORKER,
                operation="status",
                parameters={"tenant_id": tenant_id},
            )
            first_admin = OwnerAdminService(
                ServiceIdentity.PROJECTION_WORKER,
                {"status": admin_handler},
                store=admin_store,
                instance_id="projection-a",
            )
            restarted_admin = OwnerAdminService(
                ServiceIdentity.PROJECTION_WORKER,
                {"status": admin_handler},
                store=admin_store_b,
                instance_id="projection-b",
            )
            executing = asyncio.create_task(first_admin.execute(admin_request))
            await handler_started.wait()
            concurrent = await restarted_admin.execute(admin_request)
            assert concurrent.status == "running"
            release_handler.set()
            completed = await executing
            assert completed.status == "completed"
            replayed = await restarted_admin.execute(admin_request)
            assert replayed == completed
            assert calls == 1
            with pytest.raises(VersionConflictError):
                await restarted_admin.execute(
                    admin_request.model_copy(
                        update={"parameters": {"tenant_id": "different"}}
                    )
                )

            abandoned_request = admin_request.model_copy(
                update={"operation_id": f"admin-abandoned-{suffix}"}
            )
            await admin_store.claim(
                abandoned_request,
                request_digest=admin_operation_request_digest(abandoned_request),
                claimed_by="projection-crashed",
                claim_token="abandoned-claim",
                claim_ttl=timedelta(microseconds=1),
            )
            await asyncio.sleep(0.01)
            recovery = await restarted_admin.execute(abandoned_request)
            assert recovery.status == "failed"
            assert recovery.result["error_code"] == "unknown_side_effect"
            assert calls == 1

            assert not await connection.fetchval(
                "SELECT has_schema_privilege('public', 'hands', 'CREATE')"
            )
        finally:
            await invocation_store.close()
            await policy_store.close()
            await credential_store.close()
            await artifact_store.close()
            await admin_store.close()
            await admin_store_b.close()
            await tool_registry_store.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM policy.decision WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM credential.reference WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM artifact.metadata WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM projection.admin_operation WHERE operation_id=$1",
                f"admin-{suffix}",
            )
            await connection.execute(
                "DELETE FROM projection.admin_operation WHERE operation_id=$1",
                f"admin-abandoned-{suffix}",
            )
            await connection.execute(
                "DELETE FROM hands.tool_capability WHERE tool_name=$1",
                f"tool-{suffix}",
            )
            await connection.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("schema", "owner"),
    [
        ("projection", ServiceIdentity.PROJECTION_WORKER),
        ("delivery", ServiceIdentity.DELIVERY_WORKER),
        ("artifact", ServiceIdentity.ARTIFACT_SERVICE),
    ],
)
def test_admin_operation_claim_is_atomic_for_each_owner_schema(
    schema: AdminSchema,
    owner: ServiceIdentity,
) -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        operation_id = f"admin-claim-{suffix}"
        store_a = PostgresAdminOperationStore(DATABASE_URL, schema=schema)
        store_b = PostgresAdminOperationStore(DATABASE_URL, schema=schema)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def handler(parameters: dict[str, object]) -> dict[str, object]:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return parameters

        request = AdminOperationRequest(
            context=InternalRequestContext(
                tenant_id=f"tenant-admin-{suffix}",
                service_identity=ServiceIdentity.TASK_API,
                request_id=operation_id,
                correlation_id=operation_id,
                causation_id=operation_id,
            ),
            operation_id=operation_id,
            owner_service=owner,
            operation="maintenance",
            parameters={"scope": suffix},
        )
        service_a = OwnerAdminService(
            owner,
            {"maintenance": handler},
            store=store_a,
            instance_id=f"{schema}-a",
        )
        service_b = OwnerAdminService(
            owner,
            {"maintenance": handler},
            store=store_b,
            instance_id=f"{schema}-b",
        )
        try:
            executing = asyncio.create_task(service_a.execute(request))
            await started.wait()
            assert (await service_b.execute(request)).status == "running"
            release.set()
            completed = await executing
            assert completed.status == "completed"
            assert await service_b.execute(request) == completed
            assert calls == 1
        finally:
            release.set()
            await store_a.close()
            await store_b.close()
            await connection.execute(
                f"DELETE FROM {schema}.admin_operation WHERE operation_id=$1",
                operation_id,
            )
            await connection.close()

    asyncio.run(scenario())


def test_s3_expand_migration_preserves_legacy_owner_state() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        suffix = uuid4().hex
        tenant_id = f"tenant-migrate-{suffix}"
        tool_name = f"tool-migrate-{suffix}"
        credential_ref = f"cred-migrate-{suffix}"
        invocation_id = f"invocation-migrate-{suffix}"
        approval_id = f"approval-migrate-{suffix}"
        try:
            await connection.execute(MIGRATION)
            await connection.execute(
                """INSERT INTO security.tool_capability
                (tool_name,version,description,input_schema,output_schema,permission,
                 risk_level,runtime_location,owner,enabled)
                VALUES ($1,'1','legacy','{}','{}','read-only','low','hands',
                        'legacy-owner',true)""",
                tool_name,
            )
            await connection.execute(
                """INSERT INTO security.credential_reference
                (tenant_id,credential_ref,provider,account_scope,allowed_operations,
                 expires_at,revoked_at)
                VALUES ($1,$2,'legacy-provider','legacy-account','["read"]',
                        now() + interval '5 minutes',NULL)""",
                tenant_id,
                credential_ref,
            )
            await connection.execute(
                """INSERT INTO security.tool_invocation_dedup
                (tenant_id,idempotency_key,action_digest,tool_invocation_id,
                 normalized_result,side_effect_status)
                VALUES ($1,$2,'legacy-digest',$3,
                        '{"status":"success","content":{"ok":true},
                          "summary":"","metadata":{},"error_code":null,
                          "side_effect_status":"completed"}',
                        'completed')""",
                tenant_id,
                f"idem-{suffix}",
                invocation_id,
            )
            await connection.execute(
                """INSERT INTO projection.approval_view
                (tenant_id,approval_id,session_id,run_id,action_digest,tool_name,
                 redacted_arguments,risk,reason,expected_effect,allowed_decisions,
                 assigned_approvers,policy_version,expires_at,status,decision,
                 feedback,source_version,source_event_id,projected_at)
                VALUES ($1,$2,'session','run','digest',$3,'{}','high','legacy',
                        'write','["approve"]','[]','legacy-v1',
                        now() + interval '5 minutes','approved','approve',NULL,
                        1,$4,now())""",
                tenant_id,
                approval_id,
                tool_name,
                f"event-{suffix}",
            )
            usage_id = await connection.fetchval(
                """INSERT INTO security.credential_usage_audit
                (tenant_id,session_id,tool_name,credential_ref,operation,
                 policy_version)
                VALUES ($1,'session',$2,$3,'read','legacy-v1')
                RETURNING usage_id""",
                tenant_id,
                tool_name,
                credential_ref,
            )

            await connection.execute(MIGRATION)

            tool = await connection.fetchrow(
                "SELECT * FROM hands.tool_capability WHERE tool_name=$1", tool_name
            )
            assert tool is not None and tool["owner"] == "legacy-owner"
            credential = await connection.fetchrow(
                """SELECT * FROM credential.reference
                WHERE tenant_id=$1 AND credential_ref=$2""",
                tenant_id,
                credential_ref,
            )
            assert credential is not None
            assert credential["provider"] == "legacy-provider"
            assert credential["account_scope"] == "legacy-account"
            invocation = await connection.fetchrow(
                """SELECT * FROM hands.invocation
                WHERE tenant_id=$1 AND tool_invocation_id=$2""",
                tenant_id,
                invocation_id,
            )
            assert invocation is not None
            assert invocation["argument_digest"] == "legacy-digest"
            assert await connection.fetchval(
                """SELECT count(*) FROM policy.approval
                WHERE tenant_id=$1 AND approval_id=$2 AND status='approved'""",
                tenant_id,
                approval_id,
            ) == 1
            assert await connection.fetchval(
                "SELECT count(*) FROM credential.usage_audit WHERE usage_id=$1",
                f"legacy:{usage_id}",
            ) == 1
        finally:
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.tool_capability WHERE tool_name=$1", tool_name
            )
            await connection.execute(
                "DELETE FROM policy.approval WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM credential.usage_audit WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM credential.reference WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM security.tool_invocation_dedup WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM security.tool_capability WHERE tool_name=$1", tool_name
            )
            await connection.execute(
                "DELETE FROM security.credential_usage_audit WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM security.credential_reference WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM projection.approval_view WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
