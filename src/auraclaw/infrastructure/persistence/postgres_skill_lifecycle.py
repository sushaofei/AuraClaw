from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]

from auraclaw.action.skill_lifecycle import (
    SkillAdmissionAuditRecord,
    SkillAdmissionMetricRecord,
    SkillAdmissionPage,
    SkillInstallationCommit,
    SkillLifecycleStore,
    SkillOutboxRecord,
    SkillPublishCommit,
    SkillPublishCommitResult,
    SkillRestoreCommit,
    _publish_outbox_payload,
    decode_skill_admission_cursor,
    encode_skill_admission_cursor,
    is_replaced_package,
    validate_upgrade,
)
from auraclaw.contracts.errors import NotFoundError, VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillUpgradeState,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
    retry_serializable_transaction,
)
from auraclaw.infrastructure.persistence.postgres_skill_installation_records import (
    installation_from_row,
    installation_values,
)
from auraclaw.infrastructure.persistence.postgres_skill_package_records import (
    artifact_payload,
    package_from_row,
)
from auraclaw.infrastructure.persistence.postgres_skill_publication_records import (
    publication_from_row,
    publication_values,
)


class PostgresSkillLifecycleStore(LazyPool, SkillLifecycleStore):
    async def list_upgrades(self, tenant_id: str) -> tuple[SkillUpgradeState, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            "SELECT * FROM hands.skill_upgrade_current WHERE tenant_id=$1", tenant_id
        )
        return tuple(SkillUpgradeState.model_validate(dict(row)) for row in rows)

    async def claim_upgrade(self, state: SkillUpgradeState, *, ttl: timedelta) -> str | None:
        pool = await self.pool()
        token = uuid4().hex
        row = await pool.fetchrow(
            """INSERT INTO hands.skill_upgrade_claim
            (tenant_id,publisher,name,generation,token,expires_at)
            SELECT tenant_id,publisher,name,generation,$5,now()+$6::interval
            FROM hands.skill_upgrade_current WHERE tenant_id=$1 AND publisher=$2 AND name=$3
              AND generation=$4 AND phase <> 'completed'
            ON CONFLICT (tenant_id,publisher,name) DO UPDATE SET
              generation=excluded.generation,token=excluded.token,expires_at=excluded.expires_at
            WHERE hands.skill_upgrade_claim.expires_at <= now()
            RETURNING token""",
            state.tenant_id,
            state.publisher,
            state.name,
            state.generation,
            token,
            ttl,
        )
        return str(row["token"]) if row is not None else None

    async def renew_upgrade(self, state: SkillUpgradeState, token: str, *, ttl: timedelta) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE hands.skill_upgrade_claim claim SET expires_at=now()+$6::interval
            FROM hands.skill_upgrade_current current
            WHERE claim.tenant_id=$1 AND claim.publisher=$2 AND claim.name=$3
              AND claim.generation=$4 AND claim.token=$5 AND claim.expires_at>now()
              AND current.tenant_id=claim.tenant_id AND current.publisher=claim.publisher
              AND current.name=claim.name AND current.generation=claim.generation""",
            state.tenant_id,
            state.publisher,
            state.name,
            state.generation,
            token,
            ttl,
        )
        return str(result) == "UPDATE 1"

    async def set_upgrade_phase(
        self, state: SkillUpgradeState, token: str, *, phase: str, reason: str | None = None
    ) -> bool:
        if phase not in {"draining", "deleting", "completed", "blocked"}:
            raise ValueError("Invalid Skill upgrade phase")
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            changed = await connection.fetchval(
                """UPDATE hands.skill_upgrade_current current SET phase=$6,reason_code=$7,
                updated_at=now() FROM hands.skill_upgrade_claim claim
                WHERE current.tenant_id=$1 AND current.publisher=$2 AND current.name=$3
                  AND current.generation=$4 AND claim.tenant_id=current.tenant_id
                  AND claim.publisher=current.publisher AND claim.name=current.name
                  AND claim.generation=current.generation AND claim.token=$5
                  AND claim.expires_at>now()
                RETURNING true""",
                state.tenant_id,
                state.publisher,
                state.name,
                state.generation,
                token,
                phase,
                reason,
            )
            if changed and phase != "deleting":
                await connection.execute(
                    "DELETE FROM hands.skill_upgrade_claim WHERE token=$1", token
                )
            return bool(changed)

    async def remove_replaced_package(
        self, state: SkillUpgradeState, token: str, package: SkillPackageRecord
    ) -> bool:
        if package.legal_hold or not is_replaced_package(state, package):
            return False
        pool = await self.pool()
        identity = (state.tenant_id, state.publisher, state.name, package.manifest.version)
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"skill:{state.tenant_id}:{state.publisher}:{state.name}",
            )
            claim = await connection.fetchrow(
                """SELECT claim.token FROM hands.skill_upgrade_claim claim
                JOIN hands.skill_upgrade_current current USING (tenant_id,publisher,name,generation)
                WHERE claim.tenant_id=$1 AND claim.publisher=$2 AND claim.name=$3
                  AND claim.generation=$4 AND claim.token=$5 AND claim.expires_at>now()
                  AND current.phase='deleting' FOR UPDATE OF claim,current""",
                state.tenant_id,
                state.publisher,
                state.name,
                state.generation,
                token,
            )
            if claim is None:
                return False
            pinned = await connection.fetchval(
                """SELECT pinned_package_digest FROM hands.skill_installation
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 FOR UPDATE""",
                *identity[:3],
            )
            if pinned == package.package_digest:
                return False
            current = await connection.fetchrow(
                """SELECT * FROM hands.skill_package WHERE tenant_id=$1 AND publisher=$2
                AND name=$3 AND version=$4 FOR UPDATE""",
                *identity,
            )
            if current is not None and current["package_digest"] == package.package_digest:
                publication = await connection.fetchrow(
                    """SELECT status,revocation_action FROM hands.skill_publication
                    WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4 FOR UPDATE""",
                    *identity,
                )
                if current["legal_hold"] or (
                    publication is not None
                    and (
                        publication["status"] != "revoked"
                        or publication["revocation_action"] != "cancel"
                    )
                ):
                    return False
                await connection.execute(
                    """DELETE FROM hands.skill_publication_restore_command
                    WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
                    *identity,
                )
                await connection.execute(
                    """DELETE FROM hands.skill_publication WHERE tenant_id=$1 AND publisher=$2
                    AND name=$3 AND version=$4""",
                    *identity,
                )
                await connection.execute(
                    """DELETE FROM hands.skill_package WHERE tenant_id=$1 AND publisher=$2
                    AND name=$3 AND version=$4""",
                    *identity,
                )
            tombstone_hold = await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM hands.skill_package_tombstone
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
                  AND package_digest=$5 AND legal_hold)""",
                *identity,
                package.package_digest,
            )
            if tombstone_hold:
                raise VersionConflictError("Skill package tombstone is under legal hold")
            await connection.execute(
                """DELETE FROM hands.skill_package_tombstone WHERE tenant_id=$1 AND publisher=$2
                AND name=$3 AND version=$4 AND package_digest=$5""",
                *identity,
                package.package_digest,
            )
            await connection.execute(
                """DELETE FROM hands.skill_outbox WHERE (tenant_id,command_id) IN (
                  SELECT tenant_id,command_id FROM hands.skill_command WHERE tenant_id=$1
                    AND publisher=$2 AND name=$3 AND version=$4 AND package_digest=$5)""",
                *identity,
                package.package_digest,
            )
            await connection.execute(
                """UPDATE hands.skill_command SET version=NULL,package_digest=NULL
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3
                  AND version=$4 AND package_digest=$5""",
                *identity,
                package.package_digest,
            )
            await connection.execute(
                """DELETE FROM hands.skill_admission_audit WHERE tenant_id=$1 AND publisher=$2
                AND name=$3 AND version=$4 AND package_digest=$5""",
                *identity,
                package.package_digest,
            )
            return True

    async def get_publish_command_digest(self, tenant_id: str, command_id: str) -> str | None:
        pool = await self.pool()
        value = await pool.fetchval(
            "SELECT request_digest FROM hands.skill_command "
            "WHERE tenant_id=$1 AND command_id=$2 AND command_type='publish'",
            tenant_id,
            command_id,
        )
        return str(value) if value is not None else None

    async def get_upgrade(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillUpgradeState | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM hands.skill_upgrade_current "
            "WHERE tenant_id=$1 AND publisher=$2 AND name=$3",
            tenant_id,
            publisher,
            name,
        )
        return SkillUpgradeState.model_validate(dict(row)) if row is not None else None

    async def list_pending_upgrades(self, *, limit: int = 100) -> tuple[SkillUpgradeState, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            "SELECT * FROM hands.skill_upgrade_current WHERE phase <> 'completed' "
            "ORDER BY updated_at LIMIT $1",
            limit,
        )
        return tuple(SkillUpgradeState.model_validate(dict(row)) for row in rows)

    async def record_admission(self, record: SkillAdmissionAuditRecord) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO hands.skill_admission_audit
            (admission_id,tenant_id,command_id,operation,actor_id,
             correlation_id,causation_id,publisher,name,version,package_digest,
             artifact_id,outcome,stage,safe_error_code,duration_ms,occurred_at,
             content_policy_version)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)""",
            record.admission_id,
            record.tenant_id,
            record.command_id,
            record.operation,
            record.actor_id,
            record.correlation_id,
            record.causation_id,
            record.publisher,
            record.name,
            record.version,
            record.package_digest,
            record.artifact_id,
            record.outcome,
            record.stage,
            record.safe_error_code,
            record.duration_ms,
            record.occurred_at,
            record.content_policy_version,
        )

    async def list_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        limit: int = 100,
    ) -> tuple[SkillAdmissionAuditRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_admission_audit
            WHERE tenant_id=$1
              AND ($2::text IS NULL OR outcome=$2)
              AND ($3::text IS NULL OR stage=$3)
              AND ($4::text IS NULL OR content_policy_version=$4)
            ORDER BY occurred_at DESC, admission_id DESC LIMIT $5""",
            tenant_id,
            outcome,
            stage,
            content_policy_version,
            limit,
        )
        return tuple(SkillAdmissionAuditRecord(**dict(row)) for row in rows)

    async def admission_metrics(
        self, tenant_id: str, *, since: datetime | None = None
    ) -> tuple[SkillAdmissionMetricRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT outcome,content_policy_version,count(*) AS count,
                      avg(duration_ms)::double precision AS average_duration_ms
            FROM hands.skill_admission_audit
            WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR occurred_at >= $2)
            GROUP BY outcome,content_policy_version
            ORDER BY outcome,content_policy_version""",
            tenant_id,
            since,
        )
        return tuple(SkillAdmissionMetricRecord(**dict(row)) for row in rows)

    async def page_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SkillAdmissionPage:
        cursor_at, cursor_id = decode_skill_admission_cursor(cursor) if cursor else (None, None)
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_admission_audit
            WHERE tenant_id=$1
              AND ($2::text IS NULL OR outcome=$2)
              AND ($3::text IS NULL OR stage=$3)
              AND ($4::text IS NULL OR content_policy_version=$4)
              AND ($5::timestamptz IS NULL OR occurred_at >= $5)
              AND ($6::timestamptz IS NULL OR (occurred_at,admission_id) < ($6,$7))
            ORDER BY occurred_at DESC,admission_id DESC LIMIT $8""",
            tenant_id,
            outcome,
            stage,
            content_policy_version,
            since,
            cursor_at,
            cursor_id,
            limit + 1,
        )
        records = tuple(SkillAdmissionAuditRecord(**dict(row)) for row in rows[:limit])
        return SkillAdmissionPage(
            admissions=records,
            next_cursor=(
                encode_skill_admission_cursor(records[-1])
                if len(rows) > limit and records
                else None
            ),
        )

    async def delete_admissions_before(self, cutoff: datetime, *, limit: int = 1000) -> int:
        pool = await self.pool()
        deleted = await pool.fetchval(
            """WITH candidates AS (
                SELECT admission_id FROM hands.skill_admission_audit
                WHERE occurred_at < $1
                ORDER BY occurred_at,admission_id
                LIMIT $2 FOR UPDATE SKIP LOCKED
            ), removed AS (
                DELETE FROM hands.skill_admission_audit AS audit
                USING candidates
                WHERE audit.admission_id=candidates.admission_id
                RETURNING 1
            )
            SELECT count(*) FROM removed""",
            cutoff,
            limit,
        )
        return int(deleted or 0)

    @retry_serializable_transaction("skill.publish")
    async def commit_publish(self, commit: SkillPublishCommit) -> SkillPublishCommitResult:
        pool = await self.pool()
        package = commit.package
        publication = commit.publication
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"{package.tenant_id}:{commit.command_id}",
            )
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"skill:{package.tenant_id}:{package.manifest.publisher}:{package.manifest.name}",
            )
            replay = await connection.fetchrow(
                """SELECT request_digest FROM hands.skill_command
                WHERE tenant_id=$1 AND command_id=$2""",
                package.tenant_id,
                commit.command_id,
            )
            if replay is not None:
                if str(replay["request_digest"]) != commit.request_digest:
                    raise VersionConflictError("Skill command id was reused")
                return await _load_publish_result(connection, commit, replayed=True)

            if commit.upgrade is not None:
                rows = await connection.fetch(
                    "SELECT * FROM hands.skill_publication "
                    "WHERE tenant_id=$1 AND publisher=$2 AND name=$3",
                    package.tenant_id,
                    package.manifest.publisher,
                    package.manifest.name,
                )
                current = await connection.fetchrow(
                    "SELECT * FROM hands.skill_installation "
                    "WHERE tenant_id=$1 AND publisher=$2 AND name=$3 FOR UPDATE",
                    package.tenant_id,
                    package.manifest.publisher,
                    package.manifest.name,
                )
                validate_upgrade(
                    commit,
                    tuple(publication_from_row(dict(row)) for row in rows),
                    installation_from_row(dict(current)) if current is not None else None,
                )
            committed_installation: SkillInstallationRecord | None
            if commit.replace_purged:
                (
                    package,
                    committed_publication,
                    committed_installation,
                ) = await _replace_purged_package_transaction(connection, commit)
            else:
                await _put_package_transaction(connection, package)
                committed_publication = await _put_publication_transaction(
                    connection,
                    publication,
                    expected_revision=commit.expected_publication_revision,
                )
                committed_installation = await _put_installation_transaction(
                    connection,
                    commit.installation,
                    expected_revision=(
                        commit.expected_installation_revision if commit.upgrade else None
                    ),
                )
            if commit.upgrade is not None:
                await connection.execute(
                    """UPDATE hands.skill_publication SET status='revoked',
                    revocation_action='continue', revocation_policy_version='skill-upgrade-v1',
                    reason_code='skill_version_replaced', revision=revision+1,
                    updated_by=$5, updated_at=$6
                    WHERE tenant_id=$1 AND publisher=$2 AND name=$3
                    AND version<>$4 AND status='active'""",
                    package.tenant_id,
                    package.manifest.publisher,
                    package.manifest.name,
                    package.manifest.version,
                    commit.actor_id,
                    commit.occurred_at,
                )
                await _upsert_upgrade(connection, commit.upgrade)
            result = SkillPublishCommitResult(
                package=package,
                publication=committed_publication,
                installation=committed_installation,
            )
            await connection.execute(
                """INSERT INTO hands.skill_command
                (tenant_id,command_id,command_type,request_digest,actor_id,
                 correlation_id,causation_id,publisher,name,version,package_digest,
                 status,created_at,completed_at)
                VALUES ($1,$2,'publish',$3,$4,$5,$6,$7,$8,$9,$10,
                        'succeeded',$11,$11)""",
                package.tenant_id,
                commit.command_id,
                commit.request_digest,
                commit.actor_id,
                commit.correlation_id,
                commit.causation_id,
                publication.publisher,
                publication.name,
                publication.version,
                publication.package_digest,
                commit.occurred_at,
            )
            await connection.execute(
                """INSERT INTO hands.skill_outbox
                (tenant_id,command_id,event_type,payload,created_at)
                VALUES ($1,$2,'skill.publication.committed',$3::jsonb,$4)
                ON CONFLICT (tenant_id,command_id,event_type) DO NOTHING""",
                package.tenant_id,
                commit.command_id,
                json_dumps(_publish_outbox_payload(result)),
                commit.occurred_at,
            )
            return result

    @retry_serializable_transaction("skill.restore")
    async def commit_restore(self, commit: SkillRestoreCommit) -> SkillPublicationRecord:
        record = commit.publication
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"skill-restore:{record.tenant_id}:{commit.command_id}",
            )
            replay = await connection.fetchrow(
                """SELECT request_digest
                FROM hands.skill_publication_restore_command
                WHERE tenant_id=$1 AND command_id=$2""",
                record.tenant_id,
                commit.command_id,
            )
            if replay is not None:
                if str(replay["request_digest"]) != commit.request_digest:
                    raise VersionConflictError("Skill restore command id was reused")
                current = await connection.fetchrow(
                    """SELECT * FROM hands.skill_publication
                    WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
                    record.tenant_id,
                    record.publisher,
                    record.name,
                    record.version,
                )
                if current is None:
                    raise VersionConflictError("Skill restore command result is incomplete")
                return publication_from_row(dict(current))
            current_row = await connection.fetchrow(
                """SELECT * FROM hands.skill_publication
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
                FOR UPDATE""",
                record.tenant_id,
                record.publisher,
                record.name,
                record.version,
            )
            if current_row is None:
                raise NotFoundError("Skill publication was not found")
            current = publication_from_row(dict(current_row))
            if current.revision != commit.expected_revision:
                raise VersionConflictError("Skill publication revision conflict")
            if current.status is not SkillPublicationStatus.RETIRED:
                raise VersionConflictError("Skill publication is not retired")
            if (
                record.status is not SkillPublicationStatus.RESTORING
                or record.revision != current.revision + 1
                or record.package_digest != current.package_digest
                or record.publication_id != current.publication_id
            ):
                raise VersionConflictError("Skill restore transition is invalid")
            updated = await connection.fetchrow(
                """UPDATE hands.skill_publication SET status='restoring',
                revision=$5,updated_by=$6,updated_at=$7,reason_code=$8
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
                  AND revision=$9 AND status='retired' RETURNING *""",
                record.tenant_id,
                record.publisher,
                record.name,
                record.version,
                record.revision,
                commit.actor_id,
                commit.occurred_at,
                commit.reason_code,
                commit.expected_revision,
            )
            if updated is None:
                raise VersionConflictError("Skill publication revision conflict")
            await connection.execute(
                """INSERT INTO hands.skill_publication_restore_command
                (tenant_id,command_id,request_digest,publisher,name,version,
                 actor_id,reason_code,correlation_id,causation_id,
                 previous_revision,restoring_revision,created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)""",
                record.tenant_id,
                commit.command_id,
                commit.request_digest,
                record.publisher,
                record.name,
                record.version,
                commit.actor_id,
                commit.reason_code,
                commit.correlation_id,
                commit.causation_id,
                commit.expected_revision,
                record.revision,
                commit.occurred_at,
            )
            return publication_from_row(dict(updated))

    async def claim_outbox(
        self,
        *,
        owner: str,
        limit: int = 100,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> tuple[SkillOutboxRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """UPDATE hands.skill_outbox target SET claimed_by=$1,
                   claim_expires_at=now()+$3::interval,claim_heartbeat_at=now(),
                   attempt=attempt+1
               WHERE outbox_id IN (
                   SELECT outbox_id FROM hands.skill_outbox
                   WHERE published_at IS NULL AND available_at <= now()
                     AND (claim_expires_at IS NULL OR claim_expires_at <= now())
                   ORDER BY outbox_id FOR UPDATE SKIP LOCKED LIMIT $2
               ) RETURNING *""",
            owner,
            limit,
            claim_ttl,
        )
        return tuple(
            SkillOutboxRecord(
                outbox_id=str(row["outbox_id"]),
                tenant_id=str(row["tenant_id"]),
                command_id=str(row["command_id"]),
                event_type=str(row["event_type"]),
                payload=dict(json_loads(row["payload"])),
                attempt=int(row["attempt"]),
            )
            for row in rows
        )

    async def renew_outbox(self, *, outbox_id: str, owner: str, claim_ttl: timedelta) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.skill_outbox
               SET claim_expires_at=now()+$3::interval,claim_heartbeat_at=now()
               WHERE outbox_id=$1 AND claimed_by=$2
                 AND published_at IS NULL AND claim_expires_at > now()""",
            int(outbox_id),
            owner,
            claim_ttl,
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

    async def complete_outbox(self, *, outbox_id: str, owner: str) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.skill_outbox SET published_at=now(),claimed_by=NULL,
            claim_expires_at=NULL,claim_heartbeat_at=NULL,last_error=NULL WHERE outbox_id=$1
              AND claimed_by=$2 AND claim_expires_at > now()""",
            int(outbox_id),
            owner,
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

    async def fail_outbox(self, *, outbox_id: str, owner: str, safe_error_code: str) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.skill_outbox SET claimed_by=NULL,claim_expires_at=NULL,
            claim_heartbeat_at=NULL,last_error=$3,available_at=now()+
              (LEAST(300, power(2, LEAST(attempt, 8)))::text || ' seconds')::interval
            WHERE outbox_id=$1 AND claimed_by=$2""",
            int(outbox_id),
            owner,
            safe_error_code[:128],
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

    async def has_artifact_reference(self, tenant_id: str, artifact_id: str, version: int) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM hands.skill_package
                WHERE tenant_id=$1 AND artifact_ref->>'artifact_id'=$2
                  AND (artifact_ref->>'version')::integer=$3
                  AND retention_status='retained')""",
                tenant_id,
                artifact_id,
                version,
            )
        )

    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord:
        pool = await self.pool()
        manifest = record.manifest
        try:
            row = await pool.fetchrow(
                """INSERT INTO hands.skill_package
                (tenant_id,publisher,name,version,package_digest,manifest_json,
                 artifact_ref,signature_key_id,retention_status,retention_until,
                 legal_hold,retention_revision,retention_updated_by,
                 retention_updated_at,created_at,purged_at)
                VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,
                        $13,$14,$15,$16)
                ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
                RETURNING *""",
                record.tenant_id,
                manifest.publisher,
                manifest.name,
                manifest.version,
                record.package_digest,
                json_dumps(manifest.model_dump(mode="json")),
                json_dumps(artifact_payload(record.artifact_ref)),
                record.signature_key_id,
                record.retention_status.value,
                record.retention_until,
                record.legal_hold,
                record.retention_revision,
                record.retention_updated_by,
                record.retention_updated_at,
                record.created_at,
                record.purged_at,
            )
        except asyncpg.UniqueViolationError as exc:
            raise VersionConflictError("Skill package digest belongs to another version") from exc
        if row is not None:
            return package_from_row(dict(row))
        existing = await self.get_package(
            record.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
        )
        if existing is None or existing.package_digest != record.package_digest:
            raise VersionConflictError("Skill version is immutable")
        return existing

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_package
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
            tenant_id,
            publisher,
            name,
            version,
        )
        return None if row is None else package_from_row(dict(row))

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_package WHERE tenant_id=$1
            ORDER BY publisher,name,version""",
            tenant_id,
        )
        return tuple(package_from_row(dict(row)) for row in rows)

    async def list_package_tombstones(
        self, tenant_id: str, publisher: str, name: str
    ) -> tuple[SkillPackageRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_package_tombstone
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3
            ORDER BY archived_at DESC,tombstone_id DESC""",
            tenant_id,
            publisher,
            name,
        )
        return tuple(package_from_row(dict(row)) for row in rows)

    async def update_package_retention(
        self, record: SkillPackageRecord, *, expected_revision: int
    ) -> SkillPackageRecord:
        if record.retention_revision != expected_revision + 1:
            raise VersionConflictError("Skill package retention next revision is invalid")
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE hands.skill_package SET
            retention_status=$1,retention_until=$2,legal_hold=$3,
            retention_revision=$4,retention_updated_by=$5,
            retention_updated_at=$6,purged_at=$7
            WHERE tenant_id=$8 AND publisher=$9 AND name=$10 AND version=$11
              AND package_digest=$12 AND retention_revision=$13
            RETURNING *""",
            record.retention_status.value,
            record.retention_until,
            record.legal_hold,
            record.retention_revision,
            record.retention_updated_by,
            record.retention_updated_at,
            record.purged_at,
            record.tenant_id,
            record.manifest.publisher,
            record.manifest.name,
            record.manifest.version,
            record.package_digest,
            expected_revision,
        )
        if row is None:
            raise VersionConflictError("Skill package retention revision conflict")
        return package_from_row(dict(row))

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord:
        _require_next_revision(record.revision, expected_revision, "publication")
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            package_exists = await connection.fetchval(
                """SELECT true FROM hands.skill_package
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
                  AND package_digest=$5""",
                record.tenant_id,
                record.publisher,
                record.name,
                record.version,
                record.package_digest,
            )
            if package_exists is None:
                raise NotFoundError("Skill package was not found")
            if expected_revision == 0:
                row = await connection.fetchrow(
                    """INSERT INTO hands.skill_publication
                    (publication_id,tenant_id,publisher,name,version,package_digest,
                     status,revision,created_by,updated_by,created_at,
                     updated_at,reason_code,revocation_action,
                     revocation_policy_version,revocation_policy_decision_id)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                            $14,$15,$16)
                    ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
                    RETURNING *""",
                    *publication_values(record),
                )
            else:
                row = await connection.fetchrow(
                    """UPDATE hands.skill_publication SET
                    status=$1,revision=$2,updated_by=$3,updated_at=$4,
                    reason_code=$5,revocation_action=$6,
                    revocation_policy_version=$7,revocation_policy_decision_id=$8
                    WHERE tenant_id=$9 AND publisher=$10 AND name=$11 AND version=$12
                      AND publication_id=$13 AND package_digest=$14 AND revision=$15
                    RETURNING *""",
                    record.status.value,
                    record.revision,
                    record.updated_by,
                    record.updated_at,
                    record.reason_code,
                    (
                        record.revocation_action.value
                        if record.revocation_action is not None
                        else None
                    ),
                    record.revocation_policy_version,
                    record.revocation_policy_decision_id,
                    record.tenant_id,
                    record.publisher,
                    record.name,
                    record.version,
                    record.publication_id,
                    record.package_digest,
                    expected_revision,
                )
            if row is None:
                raise VersionConflictError("Skill publication revision conflict")
        return publication_from_row(dict(row))

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_publication
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
            tenant_id,
            publisher,
            name,
            version,
        )
        return None if row is None else publication_from_row(dict(row))

    async def list_publications(self, tenant_id: str) -> tuple[SkillPublicationRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_publication WHERE tenant_id=$1
            ORDER BY publisher,name,version""",
            tenant_id,
        )
        return tuple(publication_from_row(dict(row)) for row in rows)

    async def list_tenants(self) -> tuple[str, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT tenant_id FROM hands.skill_publication
            UNION SELECT tenant_id FROM hands.skill_installation
            ORDER BY tenant_id"""
        )
        return tuple(str(row["tenant_id"]) for row in rows)

    async def put_installation(
        self, record: SkillInstallationRecord, *, expected_revision: int
    ) -> SkillInstallationRecord:
        _require_next_revision(record.revision, expected_revision, "installation")
        pool = await self.pool()
        if expected_revision == 0:
            row = await pool.fetchrow(
                """INSERT INTO hands.skill_installation
                (installation_id,tenant_id,publisher,name,version_constraint,
                 pinned_package_digest,status,auto_upgrade,revision,
                 created_by,updated_by,created_at,updated_at,reason_code,
                 uninstall_action,uninstall_policy_version,
                 uninstall_policy_decision_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                        $15,$16,$17)
                ON CONFLICT (tenant_id,publisher,name) DO NOTHING RETURNING *""",
                *installation_values(record),
            )
        else:
            row = await pool.fetchrow(
                """UPDATE hands.skill_installation SET
                version_constraint=$1,pinned_package_digest=$2,status=$3,
                auto_upgrade=$4,revision=$5,updated_by=$6,updated_at=$7,reason_code=$8,
                uninstall_action=$9,uninstall_policy_version=$10,
                uninstall_policy_decision_id=$11
                WHERE tenant_id=$12 AND publisher=$13 AND name=$14
                  AND installation_id=$15 AND revision=$16 RETURNING *""",
                record.version_constraint,
                record.pinned_package_digest,
                record.status.value,
                record.auto_upgrade,
                record.revision,
                record.updated_by,
                record.updated_at,
                record.reason_code,
                (record.uninstall_action.value if record.uninstall_action is not None else None),
                record.uninstall_policy_version,
                record.uninstall_policy_decision_id,
                record.tenant_id,
                record.publisher,
                record.name,
                record.installation_id,
                expected_revision,
            )
        if row is None:
            raise VersionConflictError("Skill installation revision conflict")
        return installation_from_row(dict(row))

    @retry_serializable_transaction("skill.installation.change")
    async def commit_installation_change(
        self, commit: SkillInstallationCommit
    ) -> SkillInstallationRecord:
        record = commit.installation
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            replay = await connection.fetchrow(
                """SELECT request_digest,publisher,name
                FROM hands.skill_installation_command
                WHERE tenant_id=$1 AND command_id=$2""",
                record.tenant_id,
                commit.command_id,
            )
            if replay is not None:
                if str(replay["request_digest"]) != commit.request_digest:
                    raise VersionConflictError("Skill installation command id was reused")
                current = await connection.fetchrow(
                    """SELECT * FROM hands.skill_installation
                    WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
                    record.tenant_id,
                    str(replay["publisher"]),
                    str(replay["name"]),
                )
                if current is None:
                    raise VersionConflictError("Skill installation command result is incomplete")
                return installation_from_row(dict(current))
            _require_next_revision(record.revision, commit.expected_revision, "installation")
            row = await connection.fetchrow(
                """UPDATE hands.skill_installation SET
                version_constraint=$1,pinned_package_digest=$2,status=$3,
                auto_upgrade=$4,revision=$5,updated_by=$6,updated_at=$7,reason_code=$8,
                uninstall_action=$9,uninstall_policy_version=$10,
                uninstall_policy_decision_id=$11
                WHERE tenant_id=$12 AND publisher=$13 AND name=$14
                  AND installation_id=$15 AND revision=$16 RETURNING *""",
                record.version_constraint,
                record.pinned_package_digest,
                record.status.value,
                record.auto_upgrade,
                record.revision,
                record.updated_by,
                record.updated_at,
                record.reason_code,
                (record.uninstall_action.value if record.uninstall_action is not None else None),
                record.uninstall_policy_version,
                record.uninstall_policy_decision_id,
                record.tenant_id,
                record.publisher,
                record.name,
                record.installation_id,
                commit.expected_revision,
            )
            if row is None:
                concurrent = await connection.fetchrow(
                    """SELECT request_digest,publisher,name
                    FROM hands.skill_installation_command
                    WHERE tenant_id=$1 AND command_id=$2""",
                    record.tenant_id,
                    commit.command_id,
                )
                if (
                    concurrent is not None
                    and str(concurrent["request_digest"]) == commit.request_digest
                ):
                    current = await connection.fetchrow(
                        """SELECT * FROM hands.skill_installation
                        WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
                        record.tenant_id,
                        str(concurrent["publisher"]),
                        str(concurrent["name"]),
                    )
                    if current is not None:
                        return installation_from_row(dict(current))
                raise VersionConflictError("Skill installation revision conflict")
            await connection.execute(
                """INSERT INTO hands.skill_installation_command
                (tenant_id,command_id,request_digest,publisher,name,operation,
                 force_uninstall,actor_id,correlation_id,causation_id,reason_code,
                 previous_revision,resulting_revision,created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                record.tenant_id,
                commit.command_id,
                commit.request_digest,
                record.publisher,
                record.name,
                commit.operation,
                commit.force_uninstall,
                commit.actor_id,
                commit.correlation_id,
                commit.causation_id,
                commit.reason_code,
                commit.expected_revision,
                record.revision,
                commit.occurred_at,
            )
            return installation_from_row(dict(row))

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_installation
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
            tenant_id,
            publisher,
            name,
        )
        return None if row is None else installation_from_row(dict(row))

    async def list_installations(self, tenant_id: str) -> tuple[SkillInstallationRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_installation
            WHERE tenant_id=$1 ORDER BY publisher,name""",
            tenant_id,
        )
        return tuple(installation_from_row(dict(row)) for row in rows)


async def _put_package_transaction(
    connection: asyncpg.Connection, record: SkillPackageRecord
) -> None:
    manifest = record.manifest
    try:
        row = await connection.fetchrow(
            """INSERT INTO hands.skill_package
            (tenant_id,publisher,name,version,package_digest,manifest_json,
             artifact_ref,signature_key_id,retention_status,retention_until,
             legal_hold,retention_revision,retention_updated_by,
             retention_updated_at,created_at,purged_at)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,
                    $13,$14,$15,$16)
            ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
            RETURNING package_digest""",
            record.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
            record.package_digest,
            json_dumps(manifest.model_dump(mode="json")),
            json_dumps(artifact_payload(record.artifact_ref)),
            record.signature_key_id,
            record.retention_status.value,
            record.retention_until,
            record.legal_hold,
            record.retention_revision,
            record.retention_updated_by,
            record.retention_updated_at,
            record.created_at,
            record.purged_at,
        )
    except asyncpg.UniqueViolationError as exc:
        raise VersionConflictError("Skill package digest belongs to another version") from exc
    if row is not None:
        return
    digest = await connection.fetchval(
        """SELECT package_digest FROM hands.skill_package
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
        record.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
    )
    if digest is None or str(digest) != record.package_digest:
        raise VersionConflictError("Skill version is immutable")


async def _replace_purged_package_transaction(
    connection: asyncpg.Connection, commit: SkillPublishCommit
) -> tuple[SkillPackageRecord, SkillPublicationRecord, SkillInstallationRecord]:
    package = commit.package
    publication = commit.publication
    installation = commit.installation
    manifest = package.manifest
    if installation is None or commit.expected_installation_revision is None:
        raise VersionConflictError("Purged Skill replacement requires an installation")

    await connection.execute("SET CONSTRAINTS hands.skill_publication_package_digest_fk DEFERRED")
    package_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_package
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
        FOR UPDATE""",
        package.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
    )
    publication_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_publication
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
        FOR UPDATE""",
        package.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
    )
    installation_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_installation
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3
        FOR UPDATE""",
        package.tenant_id,
        manifest.publisher,
        manifest.name,
    )
    if package_row is None or publication_row is None or installation_row is None:
        raise VersionConflictError("Purged Skill replacement state is incomplete")

    previous_package = package_from_row(dict(package_row))
    previous_publication = publication_from_row(dict(publication_row))
    previous_installation = installation_from_row(dict(installation_row))
    if (
        package.retention_status is not SkillPackageRetentionStatus.RETAINED
        or publication.status is not SkillPublicationStatus.ACTIVE
        or publication.package_digest != package.package_digest
        or installation.status is not SkillInstallationStatus.ACTIVE
        or installation.pinned_package_digest != package.package_digest
        or previous_package.retention_status is not SkillPackageRetentionStatus.PURGED
        or previous_package.legal_hold
        or previous_publication.status is not SkillPublicationStatus.REVOKED
        or previous_installation.status is not SkillInstallationStatus.UNINSTALLED
    ):
        raise VersionConflictError("Only a fully purged Skill can be republished")
    if previous_publication.revision != commit.expected_publication_revision:
        raise VersionConflictError("Skill publication revision conflict")
    if previous_installation.revision != commit.expected_installation_revision:
        raise VersionConflictError("Skill installation revision conflict")
    _require_next_revision(publication.revision, previous_publication.revision, "publication")
    _require_next_revision(installation.revision, previous_installation.revision, "installation")

    await connection.execute(
        """INSERT INTO hands.skill_package_tombstone
        (tenant_id,publisher,name,version,package_digest,manifest_json,
         artifact_ref,signature_key_id,retention_status,retention_until,
         legal_hold,retention_revision,retention_updated_by,
         retention_updated_at,created_at,purged_at,
         replacement_package_digest,archived_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)""",
        previous_package.tenant_id,
        previous_package.manifest.publisher,
        previous_package.manifest.name,
        previous_package.manifest.version,
        previous_package.package_digest,
        package_row["manifest_json"],
        package_row["artifact_ref"],
        previous_package.signature_key_id,
        previous_package.retention_status.value,
        previous_package.retention_until,
        previous_package.legal_hold,
        previous_package.retention_revision,
        previous_package.retention_updated_by,
        previous_package.retention_updated_at,
        previous_package.created_at,
        previous_package.purged_at,
        package.package_digest,
        commit.occurred_at,
    )
    updated_package_row = await connection.fetchrow(
        """UPDATE hands.skill_package SET
        package_digest=$1,manifest_json=$2::jsonb,artifact_ref=$3::jsonb,
        signature_key_id=$4,retention_status=$5,retention_until=$6,
        legal_hold=$7,retention_revision=$8,retention_updated_by=$9,
        retention_updated_at=$10,created_at=$11,purged_at=$12
        WHERE tenant_id=$13 AND publisher=$14 AND name=$15 AND version=$16
          AND package_digest=$17 AND retention_status='purged'
        RETURNING *""",
        package.package_digest,
        json_dumps(manifest.model_dump(mode="json")),
        json_dumps(artifact_payload(package.artifact_ref)),
        package.signature_key_id,
        package.retention_status.value,
        package.retention_until,
        package.legal_hold,
        package.retention_revision,
        package.retention_updated_by,
        package.retention_updated_at,
        package.created_at,
        package.purged_at,
        package.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
        previous_package.package_digest,
    )
    updated_publication_row = await connection.fetchrow(
        """UPDATE hands.skill_publication SET
        package_digest=$1,status=$2,revision=$3,
        updated_by=$4,updated_at=$5,reason_code=$6,revocation_action=$7,
        revocation_policy_version=$8,revocation_policy_decision_id=$9
        WHERE tenant_id=$10 AND publisher=$11 AND name=$12 AND version=$13
          AND publication_id=$14 AND package_digest=$15 AND revision=$16
          AND status='revoked'
        RETURNING *""",
        publication.package_digest,
        publication.status.value,
        publication.revision,
        publication.updated_by,
        publication.updated_at,
        publication.reason_code,
        (
            publication.revocation_action.value
            if publication.revocation_action is not None
            else None
        ),
        publication.revocation_policy_version,
        publication.revocation_policy_decision_id,
        publication.tenant_id,
        publication.publisher,
        publication.name,
        publication.version,
        previous_publication.publication_id,
        previous_publication.package_digest,
        previous_publication.revision,
    )
    updated_installation_row = await connection.fetchrow(
        """UPDATE hands.skill_installation SET
        version_constraint=$1,pinned_package_digest=$2,status=$3,
        auto_upgrade=$4,revision=$5,updated_by=$6,updated_at=$7,
        reason_code=$8,uninstall_action=$9,uninstall_policy_version=$10,
        uninstall_policy_decision_id=$11
        WHERE tenant_id=$12 AND publisher=$13 AND name=$14
          AND installation_id=$15 AND revision=$16 AND status='uninstalled'
        RETURNING *""",
        installation.version_constraint,
        installation.pinned_package_digest,
        installation.status.value,
        installation.auto_upgrade,
        installation.revision,
        installation.updated_by,
        installation.updated_at,
        installation.reason_code,
        (
            installation.uninstall_action.value
            if installation.uninstall_action is not None
            else None
        ),
        installation.uninstall_policy_version,
        installation.uninstall_policy_decision_id,
        installation.tenant_id,
        installation.publisher,
        installation.name,
        previous_installation.installation_id,
        previous_installation.revision,
    )
    if (
        updated_package_row is None
        or updated_publication_row is None
        or updated_installation_row is None
    ):
        raise VersionConflictError("Purged Skill replacement revision conflict")
    return (
        package_from_row(dict(updated_package_row)),
        publication_from_row(dict(updated_publication_row)),
        installation_from_row(dict(updated_installation_row)),
    )


async def _put_publication_transaction(
    connection: asyncpg.Connection,
    record: SkillPublicationRecord,
    *,
    expected_revision: int,
) -> SkillPublicationRecord:
    if expected_revision == 0:
        _require_next_revision(record.revision, expected_revision, "publication")
        row = await connection.fetchrow(
            """INSERT INTO hands.skill_publication
            (publication_id,tenant_id,publisher,name,version,package_digest,
             status,revision,created_by,updated_by,created_at,
             updated_at,reason_code,revocation_action,
             revocation_policy_version,revocation_policy_decision_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                    $14,$15,$16)
            ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
            RETURNING *""",
            *publication_values(record),
        )
    elif record.revision == expected_revision:
        row = await connection.fetchrow(
            """SELECT * FROM hands.skill_publication
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
              AND publication_id=$5 AND package_digest=$6 AND revision=$7
              AND status=$8""",
            record.tenant_id,
            record.publisher,
            record.name,
            record.version,
            record.publication_id,
            record.package_digest,
            expected_revision,
            record.status.value,
        )
    else:
        _require_next_revision(record.revision, expected_revision, "publication")
        row = await connection.fetchrow(
            """UPDATE hands.skill_publication SET status=$1,
            revision=$2,updated_by=$3,updated_at=$4,reason_code=$5,
            revocation_action=$6,revocation_policy_version=$7,
            revocation_policy_decision_id=$8
            WHERE tenant_id=$9 AND publisher=$10 AND name=$11 AND version=$12
              AND publication_id=$13 AND package_digest=$14 AND revision=$15
            RETURNING *""",
            record.status.value,
            record.revision,
            record.updated_by,
            record.updated_at,
            record.reason_code,
            (record.revocation_action.value if record.revocation_action is not None else None),
            record.revocation_policy_version,
            record.revocation_policy_decision_id,
            record.tenant_id,
            record.publisher,
            record.name,
            record.version,
            record.publication_id,
            record.package_digest,
            expected_revision,
        )
    if row is None:
        concurrent = await connection.fetchrow(
            """SELECT * FROM hands.skill_publication
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
            record.tenant_id,
            record.publisher,
            record.name,
            record.version,
        )
        if concurrent is not None:
            current = publication_from_row(dict(concurrent))
            if current.package_digest == record.package_digest and current.status is record.status:
                return current
        raise VersionConflictError("Skill publication revision conflict")
    return publication_from_row(dict(row))


async def _put_installation_transaction(
    connection: asyncpg.Connection,
    record: SkillInstallationRecord | None,
    *,
    expected_revision: int | None = None,
) -> SkillInstallationRecord | None:
    if record is None:
        return None
    if expected_revision is not None:
        row = await connection.fetchrow(
            """UPDATE hands.skill_installation SET
            version_constraint=$4, pinned_package_digest=$5, auto_upgrade=false,
            revision=$6, updated_by=$7, updated_at=$8
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND revision=$9 RETURNING *""",
            record.tenant_id,
            record.publisher,
            record.name,
            record.version_constraint,
            record.pinned_package_digest,
            record.revision,
            record.updated_by,
            record.updated_at,
            expected_revision,
        )
        if row is None:
            raise VersionConflictError("Skill installation revision conflict")
        return installation_from_row(dict(row))
    row = await connection.fetchrow(
        """INSERT INTO hands.skill_installation
        (installation_id,tenant_id,publisher,name,version_constraint,
         pinned_package_digest,status,auto_upgrade,revision,
         created_by,updated_by,created_at,updated_at,reason_code,
         uninstall_action,uninstall_policy_version,
         uninstall_policy_decision_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                $15,$16,$17)
        ON CONFLICT (tenant_id,publisher,name) DO NOTHING RETURNING *""",
        *installation_values(record),
    )
    if row is not None:
        return installation_from_row(dict(row))
    current_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_installation
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
        record.tenant_id,
        record.publisher,
        record.name,
    )
    if current_row is None:
        raise VersionConflictError("Skill installation revision conflict")
    current = installation_from_row(dict(current_row))
    if (
        current.status is not record.status
        or current.pinned_package_digest != record.pinned_package_digest
    ):
        raise VersionConflictError("Skill installation revision conflict")
    return current


async def _load_publish_result(
    connection: asyncpg.Connection,
    commit: SkillPublishCommit,
    *,
    replayed: bool,
) -> SkillPublishCommitResult:
    record = commit.package
    manifest = record.manifest
    package_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_package
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
        record.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
    )
    publication_row = await connection.fetchrow(
        """SELECT * FROM hands.skill_publication
        WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
        record.tenant_id,
        manifest.publisher,
        manifest.name,
        manifest.version,
    )
    if package_row is None or publication_row is None:
        raise VersionConflictError("Skill command result is incomplete")
    installation = None
    if commit.installation is not None:
        installation_row = await connection.fetchrow(
            """SELECT * FROM hands.skill_installation
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
            record.tenant_id,
            manifest.publisher,
            manifest.name,
        )
        if installation_row is None:
            raise VersionConflictError("Skill command result is incomplete")
        installation = installation_from_row(dict(installation_row))
    return SkillPublishCommitResult(
        package=package_from_row(dict(package_row)),
        publication=publication_from_row(dict(publication_row)),
        installation=installation,
        replayed=replayed,
    )


def _require_next_revision(revision: int, expected_revision: int, label: str) -> None:
    if revision != expected_revision + 1:
        raise VersionConflictError(f"Skill {label} next revision is invalid")


async def _upsert_upgrade(connection: asyncpg.Connection, state: SkillUpgradeState) -> None:
    await connection.execute(
        """INSERT INTO hands.skill_upgrade_current
        (tenant_id,publisher,name,operation_id,command_id,current_version,package_digest,generation,
         phase,reason_code,actor_id,correlation_id,causation_id,updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (tenant_id,publisher,name) DO UPDATE SET
        operation_id=EXCLUDED.operation_id,command_id=EXCLUDED.command_id,
        current_version=EXCLUDED.current_version,package_digest=EXCLUDED.package_digest,
        generation=EXCLUDED.generation,phase=EXCLUDED.phase,reason_code=EXCLUDED.reason_code,
        actor_id=EXCLUDED.actor_id,correlation_id=EXCLUDED.correlation_id,
        causation_id=EXCLUDED.causation_id,updated_at=EXCLUDED.updated_at""",
        state.tenant_id,
        state.publisher,
        state.name,
        state.operation_id,
        state.command_id,
        state.current_version,
        state.package_digest,
        state.generation,
        state.phase,
        state.reason_code,
        state.actor_id,
        state.correlation_id,
        state.causation_id,
        state.updated_at,
    )
