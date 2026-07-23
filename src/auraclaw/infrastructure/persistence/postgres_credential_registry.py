from __future__ import annotations

import uuid

from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads


class PostgresCredentialRegistry(LazyPool):
    async def get_reference(
        self, tenant_id: str, credential_ref: str
    ) -> CredentialReference | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM credential.reference
            WHERE tenant_id=$1 AND credential_ref=$2 AND revoked_at IS NULL""",
            tenant_id,
            credential_ref,
        )
        if row is None:
            return None
        return CredentialReference(
            credential_ref=str(row["credential_ref"]),
            provider=str(row["provider"]),
            account_scope=str(row["account_scope"]),
            allowed_operations=tuple(json_loads(row["allowed_operations"])),
            expires_at=row["expires_at"],
        )

    async def save_reference(
        self, tenant_id: str, reference: CredentialReference
    ) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO credential.reference
            (tenant_id,credential_ref,resource,provider,account_scope,
             allowed_operations,expires_at,revoked_at)
            VALUES ($1,$2,$3,$3,$4,$5::jsonb,$6,NULL)
            ON CONFLICT (tenant_id,credential_ref) DO UPDATE SET
              resource=EXCLUDED.resource,provider=EXCLUDED.provider,
              account_scope=EXCLUDED.account_scope,
              allowed_operations=EXCLUDED.allowed_operations,
              expires_at=EXCLUDED.expires_at,revoked_at=NULL""",
            tenant_id,
            reference.credential_ref,
            reference.provider,
            reference.account_scope,
            json_dumps(reference.allowed_operations),
            reference.expires_at,
        )

    async def revoke_reference(self, tenant_id: str, credential_ref: str) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE credential.reference SET revoked_at=now()
            WHERE tenant_id=$1 AND credential_ref=$2""",
            tenant_id,
            credential_ref,
        )

    async def record_usage(self, record: dict[str, str]) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO credential.usage_audit
            (usage_id,tenant_id,session_id,target,credential_ref,operation,
             policy_decision_id,status,side_effect_status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
            str(uuid.uuid4()),
            record["tenant_id"],
            record["session_id"],
            record["tool_name"],
            record["credential_ref"],
            record["operation"],
            record["policy_decision_id"],
            record["status"],
            record["side_effect_status"],
        )
