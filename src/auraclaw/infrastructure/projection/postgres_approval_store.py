from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.tools import ApprovalRecord, ApprovalStatus, RiskLevel
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads
from auraclaw.projection.approval.projector import APPROVAL_EVENTS


class PostgresApprovalProjection(LazyPool):
    """Rebuildable Approval view; Canonical Session Events remain the fact source."""

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        pool = await self.pool()
        for event in events:
            if event.type not in APPROVAL_EVENTS or event.type == "human.response.recorded":
                continue
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """INSERT INTO projection.processed_event (projector_id, event_id)
                    VALUES ('approval', $1) ON CONFLICT DO NOTHING RETURNING event_id""",
                    event.event_id,
                )
                if inserted is None:
                    continue
                payload = event.payload
                if event.type == "approval.requested":
                    await connection.execute(
                        """INSERT INTO projection.approval_view
                        (tenant_id, approval_id, session_id, run_id, action_digest,
                         tool_name, redacted_arguments, risk, reason, expected_effect,
                         allowed_decisions, assigned_approvers, policy_version, expires_at,
                         status, source_version, source_event_id, projected_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11::jsonb,
                                $12::jsonb,$13,$14,$15,$16,$17,$18)
                        ON CONFLICT (tenant_id, approval_id) DO NOTHING""",
                        event.tenant_id,
                        str(payload["approval_id"]),
                        event.session_id,
                        str(payload.get("run_id") or event.run_id or ""),
                        str(payload["action_digest"]),
                        str(payload["tool_name"]),
                        json_dumps(payload.get("redacted_arguments", {})),
                        str(payload["risk"]),
                        str(payload.get("reason", "")),
                        str(payload.get("expected_effect", "")),
                        json_dumps(payload.get("allowed_decisions", [])),
                        json_dumps(payload.get("assigned_approvers", [])),
                        str(payload["policy_version"]),
                        datetime.fromisoformat(str(payload["expires_at"])),
                        str(payload.get("status", "waiting")),
                        event.aggregate_version,
                        event.event_id,
                        event.occurred_at,
                    )
                else:
                    status = event.type.split(".", 1)[1]
                    await connection.execute(
                        """UPDATE projection.approval_view
                        SET status=$3, decision=$4, feedback=$5, source_version=$6,
                            source_event_id=$7, projected_at=$8
                        WHERE tenant_id=$1 AND approval_id=$2""",
                        event.tenant_id,
                        str(payload["approval_id"]),
                        status,
                        payload.get("decision"),
                        payload.get("feedback"),
                        event.aggregate_version,
                        event.event_id,
                        event.occurred_at,
                    )

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM projection.approval_view
            WHERE tenant_id=$1 AND approval_id=$2""",
            tenant_id,
            approval_id,
        )
        return self._record(row) if row is not None else None

    async def find_approved(
        self, tenant_id: str, session_id: str, digest: str, policy_version: str
    ) -> ApprovalRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM projection.approval_view
            WHERE tenant_id=$1 AND session_id=$2 AND action_digest=$3
              AND policy_version=$4 AND status='approved' AND expires_at > now()
            ORDER BY projected_at DESC LIMIT 1""",
            tenant_id,
            session_id,
            digest,
            policy_version,
        )
        return self._record(row) if row is not None else None

    @staticmethod
    def _record(row: asyncpg.Record) -> ApprovalRecord:
        return ApprovalRecord(
            approval_id=str(row["approval_id"]),
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            action_digest=str(row["action_digest"]),
            tool_name=str(row["tool_name"]),
            redacted_arguments=dict(json_loads(row["redacted_arguments"])),
            risk=RiskLevel(str(row["risk"])),
            reason=str(row["reason"]),
            expected_effect=str(row["expected_effect"]),
            allowed_decisions=tuple(json_loads(row["allowed_decisions"])),
            assigned_approvers=tuple(json_loads(row["assigned_approvers"])),
            policy_version=str(row["policy_version"]),
            expires_at=row["expires_at"],
            status=ApprovalStatus(str(row["status"])),
            decision=row["decision"],
            feedback=row["feedback"],
        )
