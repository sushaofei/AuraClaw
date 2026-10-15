from __future__ import annotations

import base64
import binascii
import json
from typing import Any

from fastapi import HTTPException, status

from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.api.dependencies import RequestIdentity
from auraclaw.api.routes.skill_admin.contracts import (
    SkillAdmissionReader,
    SkillContentReader,
    SkillManager,
    SkillPublisherManager,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishedSkill,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
)


def summary(publication: PublishedSkill, *, skill_markdown: str | None = None) -> dict[str, Any]:
    manifest = publication.manifest
    payload: dict[str, Any] = {
        "publisher": manifest.publisher,
        "name": manifest.name,
        "version": manifest.version,
        "status": publication.status.value,
        "description": manifest.description,
        "risk_level": manifest.risk_level,
        "package_digest": publication.package_digest,
        "required_tools": [item.model_dump(mode="json") for item in manifest.required_tools],
        "required_resources": [
            item.model_dump(mode="json") for item in manifest.required_resources
        ],
        "required_skills": [item.model_dump(mode="json") for item in manifest.required_skills],
    }
    if publication.upgrade is not None:
        payload["upgrade"] = publication.upgrade.model_dump(
            mode="json",
            include={"operation_id", "current_version", "generation", "phase", "reason_code"},
        )
    if skill_markdown is not None:
        payload["skill_markdown"] = skill_markdown
    return payload


def require_publisher_service(
    service: SkillPublisherManager | None,
) -> SkillPublisherManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill Publisher Registry is not configured",
        )
    return service


def require_management_service(service: SkillManager | None) -> SkillManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill management service is not configured",
        )
    return service


def require_admission_reader(
    reader: SkillAdmissionReader | None,
) -> SkillAdmissionReader:
    if reader is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill admission reader is not configured",
        )
    return reader


def publisher_summary(
    publisher: SkillPublisherRecord,
    keys: tuple[SkillPublisherKeyRecord, ...],
) -> dict[str, Any]:
    return {
        "publisher": publisher.model_dump(mode="json"),
        "keys": [key.model_dump(mode="json") for key in keys],
    }


async def change_installation(
    service: SkillManager | None,
    identity: RequestIdentity,
    publisher: str,
    name: str,
    operation: SkillInstallationOperation,
    command_id: str,
    expected_revision: int,
    reason_code: str | None = None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill management service is not configured",
        )
    installation = await service.change_installation(
        ChangeSkillInstallationCommand(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            publisher=publisher,
            name=name,
            operation=operation,
            force=force,
            reason_code=reason_code,
            command_id=command_id,
            expected_revision=expected_revision,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
        )
    )
    return {"installation": installation_summary(installation)}


def installation_summary(
    installation: SkillInstallationRecord,
) -> dict[str, Any]:
    return {
        "publisher": installation.publisher,
        "name": installation.name,
        "version_constraint": installation.version_constraint,
        "pinned_package_digest": installation.pinned_package_digest,
        "status": installation.status.value,
        "auto_upgrade": installation.auto_upgrade,
        "revision": installation.revision,
        "reason_code": installation.reason_code,
        "uninstall_action": (
            installation.uninstall_action.value
            if installation.uninstall_action is not None
            else None
        ),
        "uninstall_policy_version": installation.uninstall_policy_version,
        "uninstall_policy_decision_id": installation.uninstall_policy_decision_id,
        "updated_by": installation.updated_by,
        "updated_at": installation.updated_at.isoformat(),
    }


def publication_state_summary(
    publication: SkillPublicationRecord,
) -> dict[str, Any]:
    return {
        "publisher": publication.publisher,
        "name": publication.name,
        "version": publication.version,
        "package_digest": publication.package_digest,
        "status": publication.status.value,
        "revision": publication.revision,
        "reason_code": publication.reason_code,
        "revocation_action": (
            publication.revocation_action.value
            if publication.revocation_action is not None
            else None
        ),
        "revocation_policy_version": publication.revocation_policy_version,
        "revocation_policy_decision_id": (publication.revocation_policy_decision_id),
        "updated_by": publication.updated_by,
        "updated_at": publication.updated_at.isoformat(),
    }


def package_state_summary(package: SkillPackageRecord) -> dict[str, Any]:
    return {
        "publisher": package.manifest.publisher,
        "name": package.manifest.name,
        "version": package.manifest.version,
        "package_digest": package.package_digest,
        "retention_status": package.retention_status.value,
        "retention_until": package.retention_until.isoformat(),
        "legal_hold": package.legal_hold,
        "retention_revision": package.retention_revision,
        "retention_updated_by": package.retention_updated_by,
        "retention_updated_at": package.retention_updated_at.isoformat(),
        "purged_at": (None if package.purged_at is None else package.purged_at.isoformat()),
    }


async def skill_markdown(
    reader: SkillContentReader | None,
    registry: SkillPackageRegistry,
    tenant_id: str,
    publisher: str,
    name: str,
    version: str,
) -> str | None:
    if reader is not None:
        return await reader.get_skill_markdown(tenant_id, publisher, name, version)
    return registry.skill_markdown(tenant_id, publisher, name, version)


def skill_item_key(item: dict[str, Any]) -> tuple[str, ...]:
    return (str(item["publisher"]), str(item["name"]))


def paginate(
    records: list[dict[str, Any]],
    *,
    cursor: str | None,
    limit: int,
    key: Any,
) -> tuple[list[dict[str, Any]], str | None]:
    ordered = sorted(records, key=key)
    after: tuple[str, ...] | None = None
    if cursor:
        try:
            padding = "=" * (-len(cursor) % 4)
            decoded = base64.urlsafe_b64decode(f"{cursor}{padding}").decode()
            payload = json.loads(decoded)
            values = payload["after"]
            if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                raise ValueError
            after = tuple(values)
        except (
            ValueError,
            KeyError,
            binascii.Error,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as exc:
            raise HTTPException(status_code=422, detail="Invalid Skill page cursor") from exc
    if after is not None:
        ordered = [item for item in ordered if tuple(key(item)) > after]
    page = ordered[:limit]
    next_cursor = None
    if len(ordered) > limit and page:
        encoded = json.dumps({"after": list(key(page[-1]))}, separators=(",", ":")).encode()
        next_cursor = base64.urlsafe_b64encode(encoded).decode().rstrip("=")
    return page, next_cursor
