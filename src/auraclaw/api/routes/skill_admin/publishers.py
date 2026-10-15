from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Header, Query, status

from auraclaw.action.skill_admin_queries import SkillAdminPublisherQueryService
from auraclaw.api.routes.skill_admin.contracts import (
    Identity,
    RegisterSkillPublisherRequest,
    RotateSkillPublisherKeyRequest,
    SkillPublisherManager,
)
from auraclaw.api.routes.skill_admin.presentation import (
    paginate,
    publisher_summary,
    require_publisher_service,
)
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillPublisherStatusOperation,
    SkillRevocationAction,
)


def create_publishers_router(*, publisher_service: SkillPublisherManager | None) -> APIRouter:
    router = APIRouter()
    query_service = (
        SkillAdminPublisherQueryService(publisher_service)
        if publisher_service is not None
        else None
    )

    @router.get("/skill-publishers")
    async def list_skill_publishers(
        identity: Identity,
        status_filter: Annotated[str | None, Query(alias="status", max_length=64)] = None,
        q: Annotated[str | None, Query(max_length=256)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        require_publisher_service(publisher_service)
        assert query_service is not None
        records = [
            publisher_summary(record, keys)
            for record, keys in await query_service.list_publishers(
                identity.tenant_id, status=status_filter, text=q
            )
        ]
        page, next_cursor = paginate(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (str(item["publisher"]["publisher"]),),
        )
        return {"publishers": page, "next_cursor": next_cursor}

    @router.get("/skill-publishers/{publisher}")
    async def get_skill_publisher(publisher: str, identity: Identity) -> dict[str, Any]:
        service = require_publisher_service(publisher_service)
        record, keys = await service.get_publisher(identity.tenant_id, publisher)
        return publisher_summary(record, keys)

    @router.post("/skill-publishers/{publisher}", status_code=status.HTTP_201_CREATED)
    async def register_skill_publisher(
        publisher: str,
        payload: RegisterSkillPublisherRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(default=0, alias="X-Expected-Revision", ge=0),
    ) -> dict[str, Any]:
        service = require_publisher_service(publisher_service)
        record, keys = await service.register_publisher(
            RegisterSkillPublisherCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                display_name=payload.display_name,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return publisher_summary(record, keys)

    @router.post(
        "/skill-publishers/{publisher}/keys:rotate",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def rotate_skill_publisher_key(
        publisher: str,
        payload: RotateSkillPublisherKeyRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        service = require_publisher_service(publisher_service)
        record, keys = await service.rotate_publisher_key(
            RotateSkillPublisherKeyCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                key_id=payload.key_id,
                public_key=payload.public_key,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return publisher_summary(record, keys)

    @router.post(
        "/skill-publishers/{publisher}/keys/{key_id}:revoke",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def revoke_skill_publisher_key(
        publisher: str,
        key_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
        revocation_action: Annotated[
            Literal["pause", "cancel"], Header(alias="X-Revocation-Action")
        ] = "cancel",
        policy_version: str = Header(
            default="skill-revocation-v1",
            alias="X-Policy-Version",
            min_length=1,
            max_length=128,
        ),
        policy_decision_id: str | None = Header(
            default=None, alias="X-Policy-Decision-ID", max_length=256
        ),
    ) -> dict[str, Any]:
        service = require_publisher_service(publisher_service)
        record, keys = await service.revoke_publisher_key(
            RevokeSkillPublisherKeyCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                key_id=key_id,
                reason_code=reason_code,
                revocation_action=SkillRevocationAction(revocation_action),
                policy_version=policy_version,
                policy_decision_id=policy_decision_id,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return publisher_summary(record, keys)

    async def _change_publisher_status(
        publisher: str,
        operation: SkillPublisherStatusOperation,
        identity: Identity,
        command_id: str,
        expected_revision: int,
        reason_code: str,
        revocation_action: SkillRevocationAction | None = None,
        policy_version: str | None = None,
        policy_decision_id: str | None = None,
    ) -> dict[str, Any]:
        service = require_publisher_service(publisher_service)
        record, keys = await service.change_publisher_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                operation=operation,
                reason_code=reason_code,
                revocation_action=revocation_action,
                policy_version=policy_version,
                policy_decision_id=policy_decision_id,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return publisher_summary(record, keys)

    @router.post(
        "/skill-publishers/{publisher}/status:suspend",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def suspend_skill_publisher(
        publisher: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await _change_publisher_status(
            publisher,
            SkillPublisherStatusOperation.SUSPEND,
            identity,
            command_id,
            expected_revision,
            reason_code,
            SkillRevocationAction.PAUSE,
            "skill-revocation-v1",
        )

    @router.post(
        "/skill-publishers/{publisher}/status:resume",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def resume_skill_publisher(
        publisher: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await _change_publisher_status(
            publisher,
            SkillPublisherStatusOperation.RESUME,
            identity,
            command_id,
            expected_revision,
            reason_code,
        )

    @router.post(
        "/skill-publishers/{publisher}/status:revoke",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def revoke_skill_publisher(
        publisher: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
        revocation_action: Annotated[
            Literal["pause", "cancel"], Header(alias="X-Revocation-Action")
        ] = "cancel",
        policy_version: str = Header(
            default="skill-revocation-v1",
            alias="X-Policy-Version",
            min_length=1,
            max_length=128,
        ),
        policy_decision_id: str | None = Header(
            default=None, alias="X-Policy-Decision-ID", max_length=256
        ),
    ) -> dict[str, Any]:
        return await _change_publisher_status(
            publisher,
            SkillPublisherStatusOperation.REVOKE,
            identity,
            command_id,
            expected_revision,
            reason_code,
            SkillRevocationAction(revocation_action),
            policy_version,
            policy_decision_id,
        )

    return router
