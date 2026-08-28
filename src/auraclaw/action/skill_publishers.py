from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from auraclaw.action.skill_packages import SkillPackage, skill_signing_payload
from auraclaw.contracts.errors import (
    NotFoundError,
    PolicyDeniedError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillPublisherKeyRecord,
    SkillPublisherKeyStatus,
    SkillPublisherRecord,
    SkillPublisherStatus,
    SkillPublisherStatusOperation,
)


class SkillPublisherStore(Protocol):
    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> SkillPublisherRecord: ...

    async def rotate_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, SkillPublisherKeyRecord]: ...

    async def revoke_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> SkillPublisherKeyRecord: ...

    async def change_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> SkillPublisherRecord: ...

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> SkillPublisherRecord | None: ...

    async def get_key(
        self, tenant_id: str, publisher: str, key_id: str
    ) -> SkillPublisherKeyRecord | None: ...

    async def list_keys(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherKeyRecord, ...]: ...


@dataclass
class InMemorySkillPublisherStore:
    publishers: dict[tuple[str, str], SkillPublisherRecord] = field(
        default_factory=dict
    )
    keys: dict[tuple[str, str, str], SkillPublisherKeyRecord] = field(
        default_factory=dict
    )
    commands: dict[tuple[str, str], tuple[str, object]] = field(
        default_factory=dict
    )

    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> SkillPublisherRecord:
        request = f"register:{command.publisher}:{command.display_name}:{command.expected_revision}"
        replay = self._replay(command.tenant_id, command.command_id, request)
        if replay is not None:
            assert isinstance(replay, SkillPublisherRecord)
            return replay
        key = (command.tenant_id, command.publisher)
        current = self.publishers.get(key)
        if current is not None:
            if command.expected_revision == current.revision:
                return current
            raise VersionConflictError("Skill Publisher revision conflict")
        if command.expected_revision != 0:
            raise VersionConflictError("Skill Publisher revision conflict")
        now = datetime.now(UTC)
        record = SkillPublisherRecord(
            tenant_id=command.tenant_id,
            publisher=command.publisher,
            display_name=command.display_name,
            status=SkillPublisherStatus.ACTIVE,
            revision=1,
            created_by=command.actor_id,
            updated_by=command.actor_id,
            created_at=now,
            updated_at=now,
        )
        self.publishers[key] = record
        self.commands[(command.tenant_id, command.command_id)] = (request, record)
        return record

    async def rotate_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, SkillPublisherKeyRecord]:
        request = (
            f"rotate:{command.publisher}:{command.key_id}:"
            f"{command.public_key}:{command.expected_revision}"
        )
        replay = self._replay(command.tenant_id, command.command_id, request)
        if replay is not None:
            assert isinstance(replay, tuple)
            return replay
        publisher_key = (command.tenant_id, command.publisher)
        publisher = self.publishers.get(publisher_key)
        if publisher is None:
            raise NotFoundError("Skill Publisher not found")
        if publisher.status is not SkillPublisherStatus.ACTIVE:
            raise PolicyDeniedError("Skill Publisher is not active")
        if publisher.revision != command.expected_revision:
            raise VersionConflictError("Skill Publisher revision conflict")
        key = (command.tenant_id, command.publisher, command.key_id)
        if key in self.keys:
            raise VersionConflictError("Skill Publisher key already exists")
        now = datetime.now(UTC)
        for current_key, record in tuple(self.keys.items()):
            if current_key[:2] == publisher_key and record.status is SkillPublisherKeyStatus.ACTIVE:
                self.keys[current_key] = record.model_copy(
                    update={
                        "status": SkillPublisherKeyStatus.RETIRING,
                        "revision": record.revision + 1,
                        "retired_at": now,
                        "updated_by": command.actor_id,
                        "updated_at": now,
                    }
                )
        rotated_publisher = publisher.model_copy(
            update={
                "revision": publisher.revision + 1,
                "updated_by": command.actor_id,
                "updated_at": now,
            }
        )
        new_key = SkillPublisherKeyRecord(
            tenant_id=command.tenant_id,
            publisher=command.publisher,
            key_id=command.key_id,
            public_key=command.public_key,
            activated_at=now,
            created_by=command.actor_id,
            updated_by=command.actor_id,
            created_at=now,
            updated_at=now,
        )
        self.publishers[publisher_key] = rotated_publisher
        self.keys[key] = new_key
        result = (rotated_publisher, new_key)
        self.commands[(command.tenant_id, command.command_id)] = (request, result)
        return result

    async def revoke_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> SkillPublisherKeyRecord:
        request = (
            f"revoke:{command.publisher}:{command.key_id}:"
            f"{command.reason_code}:{command.expected_revision}"
        )
        replay = self._replay(command.tenant_id, command.command_id, request)
        if replay is not None:
            assert isinstance(replay, SkillPublisherKeyRecord)
            return replay
        key = (command.tenant_id, command.publisher, command.key_id)
        record = self.keys.get(key)
        if record is None:
            raise NotFoundError("Skill Publisher key not found")
        if record.revision != command.expected_revision:
            raise VersionConflictError("Skill Publisher key revision conflict")
        if record.status is SkillPublisherKeyStatus.REVOKED:
            return record
        now = datetime.now(UTC)
        revoked = record.model_copy(
            update={
                "status": SkillPublisherKeyStatus.REVOKED,
                "revision": record.revision + 1,
                "revoked_at": now,
                "reason_code": command.reason_code,
                "updated_by": command.actor_id,
                "updated_at": now,
            }
        )
        self.keys[key] = revoked
        self.commands[(command.tenant_id, command.command_id)] = (request, revoked)
        return revoked

    async def change_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> SkillPublisherRecord:
        request = (
            f"status:{command.publisher}:{command.operation.value}:"
            f"{command.reason_code}:{command.expected_revision}"
        )
        replay = self._replay(command.tenant_id, command.command_id, request)
        if replay is not None:
            assert isinstance(replay, SkillPublisherRecord)
            return replay
        key = (command.tenant_id, command.publisher)
        current = self.publishers.get(key)
        if current is None:
            raise NotFoundError("Skill Publisher not found")
        if current.revision != command.expected_revision:
            raise VersionConflictError("Skill Publisher revision conflict")
        target = (
            SkillPublisherStatus.SUSPENDED
            if command.operation is SkillPublisherStatusOperation.SUSPEND
            else SkillPublisherStatus.ACTIVE
        )
        if current.status is target:
            self.commands[(command.tenant_id, command.command_id)] = (
                request,
                current,
            )
            return current
        now = datetime.now(UTC)
        updated = current.model_copy(
            update={
                "status": target,
                "status_reason_code": (
                    command.reason_code
                    if target is SkillPublisherStatus.SUSPENDED
                    else None
                ),
                "status_changed_at": now,
                "revision": current.revision + 1,
                "updated_by": command.actor_id,
                "updated_at": now,
            }
        )
        self.publishers[key] = updated
        self.commands[(command.tenant_id, command.command_id)] = (request, updated)
        return updated

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> SkillPublisherRecord | None:
        return self.publishers.get((tenant_id, publisher))

    async def get_key(
        self, tenant_id: str, publisher: str, key_id: str
    ) -> SkillPublisherKeyRecord | None:
        return self.keys.get((tenant_id, publisher, key_id))

    async def list_keys(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherKeyRecord, ...]:
        return tuple(
            record
            for key, record in sorted(self.keys.items())
            if key[:2] == (tenant_id, publisher)
        )

    def _replay(self, tenant_id: str, command_id: str, request: str) -> object | None:
        existing = self.commands.get((tenant_id, command_id))
        if existing is None:
            return None
        if existing[0] != request:
            raise VersionConflictError("Skill Publisher command id was reused")
        return existing[1]


class SkillPublisherService:
    def __init__(self, store: SkillPublisherStore) -> None:
        self._store = store

    async def register(
        self, command: RegisterSkillPublisherCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        record = await self._store.register_publisher(command)
        return record, await self._store.list_keys(command.tenant_id, command.publisher)

    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        return await self.register(command)

    async def rotate_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        try:
            _decode_public_key(command.public_key)
        except ValueError as exc:
            raise SchemaValidationError("Ed25519 public key is invalid") from exc
        publisher, _key = await self._store.rotate_key(command)
        return publisher, await self._store.list_keys(command.tenant_id, command.publisher)

    async def rotate_publisher_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        return await self.rotate_key(command)

    async def revoke_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        await self._store.revoke_key(command)
        return await self.get(command.tenant_id, command.publisher)

    async def revoke_publisher_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        return await self.revoke_key(command)

    async def change_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        await self._store.change_status(command)
        return await self.get(command.tenant_id, command.publisher)

    async def change_publisher_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        return await self.change_status(command)

    async def get(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        record = await self._store.get_publisher(tenant_id, publisher)
        if record is None:
            raise NotFoundError("Skill Publisher not found")
        return record, await self._store.list_keys(tenant_id, publisher)

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]:
        return await self.get(tenant_id, publisher)


class SkillPublisherTrustService:
    def __init__(self, store: SkillPublisherStore) -> None:
        self._store = store

    async def verify_for_admission(
        self, tenant_id: str, package: SkillPackage
    ) -> str:
        return await self._verify(tenant_id, package, allow_retiring=False)

    async def verify_for_restore(self, tenant_id: str, package: SkillPackage) -> str:
        return await self._verify(tenant_id, package, allow_retiring=True)

    async def _verify(
        self, tenant_id: str, package: SkillPackage, *, allow_retiring: bool
    ) -> str:
        manifest = package.manifest
        if not manifest.signature.startswith("ed25519:") or manifest.signature_key_id is None:
            raise PolicyDeniedError("External Skill requires Ed25519 key identity")
        publisher = await self._store.get_publisher(tenant_id, manifest.publisher)
        if publisher is None or publisher.status is not SkillPublisherStatus.ACTIVE:
            raise PolicyDeniedError("Skill Publisher is not trusted")
        key = await self._store.get_key(
            tenant_id, manifest.publisher, manifest.signature_key_id
        )
        allowed = {SkillPublisherKeyStatus.ACTIVE}
        if allow_retiring:
            allowed.add(SkillPublisherKeyStatus.RETIRING)
        if key is None or key.status not in allowed:
            raise PolicyDeniedError("Skill Publisher key is not trusted")
        try:
            signature = _decode_urlsafe(manifest.signature.removeprefix("ed25519:"))
            if len(signature) != 64:
                raise ValueError
            Ed25519PublicKey.from_public_bytes(_decode_public_key(key.public_key)).verify(
                signature, skill_signing_payload(package)
            )
        except (InvalidSignature, ValueError) as exc:
            raise PolicyDeniedError("Skill package signature is invalid") from exc
        return key.key_id


def _decode_public_key(value: str) -> bytes:
    decoded = _decode_urlsafe(value)
    if len(decoded) != 32:
        raise ValueError("Ed25519 public key must contain exactly 32 bytes")
    Ed25519PublicKey.from_public_bytes(decoded)
    return decoded


def _decode_urlsafe(value: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Invalid base64url value") from exc
