from __future__ import annotations

import hashlib

import asyncpg  # type: ignore[import-untyped]

from auraclaw.action.skill_publishers import SkillPublisherStore
from auraclaw.contracts.errors import (
    NotFoundError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
    SkillPublisherStatus,
    SkillPublisherStatusOperation,
)
from auraclaw.infrastructure.persistence.postgres_common import LazyPool


class PostgresSkillPublisherStore(LazyPool, SkillPublisherStore):
    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> SkillPublisherRecord:
        digest = _digest(
            "register",
            command.publisher,
            command.display_name,
            str(command.expected_revision),
        )
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await _lock(connection, command.tenant_id, command.command_id)
            replay = await _command_replay(
                connection, command.tenant_id, command.command_id, digest
            )
            if replay is not None:
                record = await _publisher(connection, command.tenant_id, command.publisher)
                if record is None:
                    raise VersionConflictError("Skill Publisher command result is incomplete")
                return record
            if command.expected_revision != 0:
                raise VersionConflictError("Skill Publisher revision conflict")
            row = await connection.fetchrow(
                """INSERT INTO hands.skill_publisher
                (tenant_id,publisher,display_name,status,revision,created_by,
                 updated_by,created_at,updated_at)
                VALUES ($1,$2,$3,'active',1,$4,$4,now(),now())
                ON CONFLICT (tenant_id,publisher) DO NOTHING RETURNING *""",
                command.tenant_id,
                command.publisher,
                command.display_name,
                command.actor_id,
            )
            if row is None:
                raise VersionConflictError("Skill Publisher already exists")
            await _record_command(connection, command, "register", digest, None)
            return _publisher_record(row)

    async def rotate_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, SkillPublisherKeyRecord]:
        digest = _digest(
            "rotate",
            command.publisher,
            command.key_id,
            command.public_key,
            str(command.expected_revision),
        )
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await _lock(connection, command.tenant_id, command.command_id)
            replay = await _command_replay(
                connection, command.tenant_id, command.command_id, digest
            )
            if replay is not None:
                publisher = await _publisher(connection, command.tenant_id, command.publisher)
                key = await _key(connection, command.tenant_id, command.publisher, command.key_id)
                if publisher is None or key is None:
                    raise VersionConflictError("Skill Publisher command result is incomplete")
                return publisher, key
            row = await connection.fetchrow(
                """SELECT * FROM hands.skill_publisher
                WHERE tenant_id=$1 AND publisher=$2 FOR UPDATE""",
                command.tenant_id,
                command.publisher,
            )
            if row is None:
                raise NotFoundError("Skill Publisher not found")
            if row["status"] != "active":
                raise PolicyDeniedError("Skill Publisher is not active")
            if int(row["revision"]) != command.expected_revision:
                raise VersionConflictError("Skill Publisher revision conflict")
            await connection.execute(
                """UPDATE hands.skill_publisher_key
                SET status='retiring',revision=revision+1,retired_at=now(),
                    updated_by=$3,updated_at=now()
                WHERE tenant_id=$1 AND publisher=$2 AND status='active'""",
                command.tenant_id,
                command.publisher,
                command.actor_id,
            )
            try:
                key_row = await connection.fetchrow(
                    """INSERT INTO hands.skill_publisher_key
                    (tenant_id,publisher,key_id,algorithm,public_key,status,revision,
                     activated_at,created_by,updated_by,created_at,updated_at)
                    VALUES ($1,$2,$3,'ed25519',$4,'active',1,now(),$5,$5,now(),now())
                    RETURNING *""",
                    command.tenant_id,
                    command.publisher,
                    command.key_id,
                    command.public_key,
                    command.actor_id,
                )
            except asyncpg.UniqueViolationError as exc:
                raise VersionConflictError("Skill Publisher key already exists") from exc
            publisher_row = await connection.fetchrow(
                """UPDATE hands.skill_publisher SET revision=revision+1,
                updated_by=$3,updated_at=now()
                WHERE tenant_id=$1 AND publisher=$2 RETURNING *""",
                command.tenant_id,
                command.publisher,
                command.actor_id,
            )
            assert publisher_row is not None and key_row is not None
            await _record_command(connection, command, "rotate", digest, command.key_id)
            return _publisher_record(publisher_row), _key_record(key_row)

    async def revoke_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> SkillPublisherKeyRecord:
        digest = _digest(
            "revoke",
            command.publisher,
            command.key_id,
            command.reason_code,
            str(command.expected_revision),
        )
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await _lock(connection, command.tenant_id, command.command_id)
            replay = await _command_replay(
                connection, command.tenant_id, command.command_id, digest
            )
            if replay is not None:
                record = await _key(
                    connection,
                    command.tenant_id,
                    command.publisher,
                    command.key_id,
                )
                if record is None:
                    raise VersionConflictError("Skill Publisher command result is incomplete")
                return record
            row = await connection.fetchrow(
                """UPDATE hands.skill_publisher_key
                SET status='revoked',revision=revision+1,revoked_at=now(),
                    reason_code=$4,updated_by=$5,updated_at=now()
                WHERE tenant_id=$1 AND publisher=$2 AND key_id=$3
                  AND revision=$6 AND status<>'revoked' RETURNING *""",
                command.tenant_id,
                command.publisher,
                command.key_id,
                command.reason_code,
                command.actor_id,
                command.expected_revision,
            )
            if row is None:
                current = await _key(
                    connection,
                    command.tenant_id,
                    command.publisher,
                    command.key_id,
                )
                if current is None:
                    raise NotFoundError("Skill Publisher key not found")
                raise VersionConflictError("Skill Publisher key revision conflict")
            await _record_command(connection, command, "revoke", digest, command.key_id)
            return _key_record(row)

    async def change_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> SkillPublisherRecord:
        digest = _digest(
            "status",
            command.publisher,
            command.operation.value,
            command.reason_code,
            str(command.expected_revision),
        )
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await _lock(connection, command.tenant_id, command.command_id)
            replay = await _command_replay(
                connection, command.tenant_id, command.command_id, digest
            )
            if replay is not None:
                record = await _publisher(
                    connection, command.tenant_id, command.publisher
                )
                if record is None:
                    raise VersionConflictError(
                        "Skill Publisher command result is incomplete"
                    )
                return record
            target = (
                SkillPublisherStatus.SUSPENDED
                if command.operation is SkillPublisherStatusOperation.SUSPEND
                else SkillPublisherStatus.ACTIVE
            )
            row = await connection.fetchrow(
                """SELECT * FROM hands.skill_publisher
                WHERE tenant_id=$1 AND publisher=$2 FOR UPDATE""",
                command.tenant_id,
                command.publisher,
            )
            if row is None:
                raise NotFoundError("Skill Publisher not found")
            current = _publisher_record(row)
            if current.revision != command.expected_revision:
                raise VersionConflictError("Skill Publisher revision conflict")
            if current.status is target:
                await _record_command(
                    connection,
                    command,
                    command.operation.value,
                    digest,
                    None,
                )
                return current
            updated_row = await connection.fetchrow(
                """UPDATE hands.skill_publisher SET status=$3,
                status_reason_code=$4,status_changed_at=now(),revision=revision+1,
                updated_by=$5,updated_at=now()
                WHERE tenant_id=$1 AND publisher=$2 AND revision=$6
                RETURNING *""",
                command.tenant_id,
                command.publisher,
                target.value,
                (
                    command.reason_code
                    if target is SkillPublisherStatus.SUSPENDED
                    else None
                ),
                command.actor_id,
                command.expected_revision,
            )
            if updated_row is None:
                raise VersionConflictError("Skill Publisher revision conflict")
            await _record_command(
                connection,
                command,
                command.operation.value,
                digest,
                None,
            )
            return _publisher_record(updated_row)

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> SkillPublisherRecord | None:
        pool = await self.pool()
        async with pool.acquire() as connection:
            return await _publisher(connection, tenant_id, publisher)

    async def get_key(
        self, tenant_id: str, publisher: str, key_id: str
    ) -> SkillPublisherKeyRecord | None:
        pool = await self.pool()
        async with pool.acquire() as connection:
            return await _key(connection, tenant_id, publisher, key_id)

    async def list_keys(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherKeyRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_publisher_key
            WHERE tenant_id=$1 AND publisher=$2 ORDER BY created_at,key_id""",
            tenant_id,
            publisher,
        )
        return tuple(_key_record(row) for row in rows)


async def _lock(connection: asyncpg.Connection, tenant_id: str, command_id: str) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        f"publisher:{tenant_id}:{command_id}",
    )


async def _command_replay(
    connection: asyncpg.Connection, tenant_id: str, command_id: str, digest: str
) -> asyncpg.Record | None:
    row = await connection.fetchrow(
        """SELECT * FROM hands.skill_publisher_command
        WHERE tenant_id=$1 AND command_id=$2""",
        tenant_id,
        command_id,
    )
    if row is not None and row["request_digest"] != digest:
        raise VersionConflictError("Skill Publisher command id was reused")
    return row


async def _record_command(
    connection: asyncpg.Connection,
    command: (
        RegisterSkillPublisherCommand
        | RotateSkillPublisherKeyCommand
        | RevokeSkillPublisherKeyCommand
        | ChangeSkillPublisherStatusCommand
    ),
    command_type: str,
    digest: str,
    key_id: str | None,
) -> None:
    await connection.execute(
        """INSERT INTO hands.skill_publisher_command
        (tenant_id,command_id,command_type,request_digest,publisher,key_id,
         actor_id,correlation_id,causation_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
        command.tenant_id,
        command.command_id,
        command_type,
        digest,
        command.publisher,
        key_id,
        command.actor_id,
        command.correlation_id,
        command.causation_id,
    )


async def _publisher(
    connection: asyncpg.Connection, tenant_id: str, publisher: str
) -> SkillPublisherRecord | None:
    row = await connection.fetchrow(
        "SELECT * FROM hands.skill_publisher WHERE tenant_id=$1 AND publisher=$2",
        tenant_id,
        publisher,
    )
    return None if row is None else _publisher_record(row)


async def _key(
    connection: asyncpg.Connection, tenant_id: str, publisher: str, key_id: str
) -> SkillPublisherKeyRecord | None:
    row = await connection.fetchrow(
        """SELECT * FROM hands.skill_publisher_key
        WHERE tenant_id=$1 AND publisher=$2 AND key_id=$3""",
        tenant_id,
        publisher,
        key_id,
    )
    return None if row is None else _key_record(row)


def _publisher_record(row: asyncpg.Record) -> SkillPublisherRecord:
    return SkillPublisherRecord.model_validate(dict(row))


def _key_record(row: asyncpg.Record) -> SkillPublisherKeyRecord:
    return SkillPublisherKeyRecord.model_validate(dict(row))


def _digest(*parts: str) -> str:
    return "sha256:" + hashlib.sha256("\0".join(parts).encode()).hexdigest()
