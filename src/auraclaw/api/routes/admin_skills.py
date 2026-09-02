from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import AwareDatetime, Field, model_validator

from auraclaw.action.skill_admin_catalog import (
    SkillAdminCatalogQueryService,
    SkillCatalogQuery,
)
from auraclaw.action.skill_admin_catalog import (
    published_skill as _published_skill,
)
from auraclaw.action.skill_admin_catalog import (
    semver_key as _semver_key,
)
from auraclaw.action.skill_lifecycle import (
    SkillAdmissionMetricRecord,
    SkillAdmissionPage,
)
from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.api.dependencies import RequestIdentity, request_identity
from auraclaw.contracts.capabilities import CapabilityDescriptor
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.internal import (
    ArtifactFinalizeResponse,
    ContractModel,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    ChangeSkillPublisherStatusCommand,
    ConfigureSkillSourceCommand,
    PublishedSkill,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RegisterSkillPublisherCommand,
    RestoreSkillPublicationCommand,
    RetireSkillSourceCommand,
    RevokeSkillPublicationCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
    SkillPublisherStatusOperation,
    SkillRevocationAction,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef

Identity = Annotated[RequestIdentity, Depends(request_identity)]

_MAX_UPLOAD_FILES = 512
_MAX_ENCODED_UPLOAD_BYTES = 24 * 1024 * 1024


class PublishSkillRequest(ContractModel):
    source_id: str = Field(min_length=1, max_length=128)
    activate: bool = True
    files: dict[str, str] | None = Field(default=None, min_length=1, max_length=_MAX_UPLOAD_FILES)
    artifact_ref: dict[str, Any] | None = None
    expected_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_source(self) -> PublishSkillRequest:
        direct = self.files is not None
        staged = self.artifact_ref is not None or self.expected_digest is not None
        if direct == staged:
            raise ValueError("Supply either files or artifact_ref with expected_digest")
        if staged and (self.artifact_ref is None or self.expected_digest is None):
            raise ValueError("Staged publication requires artifact_ref and expected_digest")
        return self


class SkillSourceRequest(ContractModel):
    source_id: str = Field(pattern=r"^sks_[A-Za-z0-9_.-]+$")
    kind: Literal["mcp"] = "mcp"
    desired_state: Literal["enabled", "disabled"] = "enabled"
    publisher_allowlist: tuple[str, ...] = Field(min_length=1, max_length=256)
    credential_ref: str | None = Field(default=None, max_length=512)
    config_metadata: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-1000, le=1000)


class RegisterSkillPublisherRequest(ContractModel):
    display_name: str = Field(min_length=1, max_length=256)


class RotateSkillPublisherKeyRequest(ContractModel):
    key_id: str = Field(min_length=1, max_length=128)
    public_key: str = Field(min_length=43, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")


class SkillPublisher(Protocol):
    async def publish(
        self, command: PublishSkillCommand, package: SkillPackage
    ) -> PublishedSkill: ...

    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill: ...


class SkillPackageUploadManager(Protocol):
    async def stage(
        self,
        *,
        tenant_id: str,
        name: str,
        content: bytes,
        checksum: str,
        correlation_id: str,
        command_id: str,
    ) -> ArtifactFinalizeResponse: ...


class SkillCapabilityAvailability(Protocol):
    async def is_available(
        self, tenant_id: str, capability: CapabilityDescriptor
    ) -> bool: ...


class SkillManager(Protocol):
    async def get_admin_snapshot(
        self, tenant_id: str
    ) -> tuple[
        tuple[SkillPackageRecord, ...],
        tuple[SkillPublicationRecord, ...],
        tuple[SkillInstallationRecord, ...],
    ]: ...

    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord: ...

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord: ...

    async def list_installations(
        self, tenant_id: str
    ) -> tuple[SkillInstallationRecord, ...]: ...

    async def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPublicationRecord: ...

    async def list_publications(
        self, tenant_id: str
    ) -> tuple[SkillPublicationRecord, ...]: ...

    async def change_installation(
        self, command: ChangeSkillInstallationCommand
    ) -> SkillInstallationRecord: ...

    async def revoke_publication(
        self, command: RevokeSkillPublicationCommand
    ) -> SkillPublicationRecord: ...

    async def restore_publication(
        self, command: RestoreSkillPublicationCommand
    ) -> SkillPublicationRecord: ...

    async def purge_package(self, command: PurgeSkillPackageCommand) -> SkillPackageRecord: ...


class SkillContentReader(Protocol):
    async def get_skill_markdown(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> str | None: ...


class SkillPublisherManager(Protocol):
    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def rotate_publisher_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def revoke_publisher_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def change_publisher_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def list_publishers(
        self, tenant_id: str
    ) -> tuple[
        tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]], ...
    ]: ...


class SkillAdmissionReader(Protocol):
    async def page_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        since: AwareDatetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SkillAdmissionPage: ...

    async def admission_metrics(
        self, tenant_id: str, *, since: datetime | None = None
    ) -> tuple[SkillAdmissionMetricRecord, ...]: ...


class SkillSourceManager(Protocol):
    async def configure_source(
        self, command: ConfigureSkillSourceCommand
    ) -> SkillSourceRecord: ...

    async def retire_source(
        self, command: RetireSkillSourceCommand
    ) -> SkillSourceRecord: ...

    async def list_sources(self, tenant_id: str) -> tuple[SkillSourceRecord, ...]: ...

    async def get_source(self, tenant_id: str, source_id: str) -> SkillSourceRecord: ...

    async def sync_source(
        self, tenant_id: str, source_id: str
    ) -> dict[str, object]: ...

    async def get_source_sync_state(
        self, tenant_id: str, source_id: str
    ) -> object | None: ...


def _summary(publication: PublishedSkill, *, skill_markdown: str | None = None) -> dict[str, Any]:
    manifest = publication.manifest
    payload = {
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
    if skill_markdown is not None:
        payload["skill_markdown"] = skill_markdown
    return payload


def create_skill_admin_router(
    registry: SkillPackageRegistry,
    *,
    publication_service: SkillPublisher | None = None,
    management_service: SkillManager | None = None,
    content_reader: SkillContentReader | None = None,
    upload_service: SkillPackageUploadManager | None = None,
    publisher_service: SkillPublisherManager | None = None,
    admission_reader: SkillAdmissionReader | None = None,
    source_service: SkillSourceManager | None = None,
    capability_availability: SkillCapabilityAvailability | None = None,
    admission_metrics_window_hours: int = 24,
    admission_quarantine_alert_ratio: float = 0.25,
    admission_quarantine_alert_min_samples: int = 20,
) -> APIRouter:
    if not 1 <= admission_metrics_window_hours <= 2160:
        raise ValueError("Skill admission metrics window must be between 1 and 2160 hours")
    if not 0 <= admission_quarantine_alert_ratio <= 1:
        raise ValueError("Skill admission quarantine alert ratio must be between 0 and 1")
    if admission_quarantine_alert_min_samples < 1:
        raise ValueError("Skill admission quarantine alert minimum samples must be positive")
    router = APIRouter(prefix="/v1/admin", tags=["skill-admin"])
    catalog_query_service = (
        SkillAdminCatalogQueryService(management_service, capability_availability)
        if management_service is not None
        else None
    )

    @router.get("/skill-sources")
    async def list_skill_sources(identity: Identity) -> dict[str, Any]:
        service = _require_source_service(source_service)
        return {
            "sources": [
                _source_summary(source)
                for source in await service.list_sources(identity.tenant_id)
            ]
        }

    @router.get("/skill-sources/{source_id}")
    async def get_skill_source(source_id: str, identity: Identity) -> dict[str, Any]:
        return {
            "source": _source_summary(
                await _require_source_service(source_service).get_source(
                    identity.tenant_id, source_id
                )
            )
        }

    @router.get("/skill-sources/{source_id}/sync-state")
    async def get_skill_source_sync_state(
        source_id: str, identity: Identity
    ) -> dict[str, Any]:
        service = _require_source_service(source_service)
        state = await service.get_source_sync_state(identity.tenant_id, source_id)
        return {
            "source_id": source_id,
            "sync_state": (
                None
                if state is None
                else state.model_dump(mode="json")
                if hasattr(state, "model_dump")
                else state
            ),
        }

    async def _configure_source(
        request: SkillSourceRequest,
        identity: Identity,
        command_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        result = await _require_source_service(source_service).configure_source(
            ConfigureSkillSourceCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                source_id=request.source_id,
                kind=SkillSourceKind(request.kind),
                desired_state=SkillSourceDesiredState(request.desired_state),
                publisher_allowlist=request.publisher_allowlist,
                credential_ref=request.credential_ref,
                config_metadata=request.config_metadata,
                priority=request.priority,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"source": _source_summary(result)}

    @router.post("/skill-sources", status_code=status.HTTP_201_CREATED)
    async def create_skill_source(
        request: SkillSourceRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return await _configure_source(request, identity, command_id, 0)

    @router.patch("/skill-sources/{source_id}")
    async def update_skill_source(
        source_id: str,
        request: SkillSourceRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        if request.source_id != source_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Skill Source path and body id differ",
            )
        return await _configure_source(
            request, identity, command_id, expected_revision
        )

    @router.post("/skill-sources/{source_id}:sync", status_code=status.HTTP_202_ACCEPTED)
    async def sync_skill_source(source_id: str, identity: Identity) -> dict[str, Any]:
        return {
            "sync": await _require_source_service(source_service).sync_source(
                identity.tenant_id, source_id
            )
        }

    @router.delete("/skill-sources/{source_id}", status_code=status.HTTP_202_ACCEPTED)
    async def retire_skill_source(
        source_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        result = await _require_source_service(source_service).retire_source(
            RetireSkillSourceCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                source_id=source_id,
                reason_code=reason_code,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"source": _source_summary(result)}

    @router.get("/skills")
    async def list_skills(
        identity: Identity,
        q: Annotated[str | None, Query(max_length=256)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        risk_level: Annotated[str | None, Query(max_length=64)] = None,
        publication_status: Annotated[str | None, Query(max_length=64)] = None,
        installation_status: Annotated[str | None, Query(max_length=64)] = None,
        source_id: Annotated[str | None, Query(max_length=128)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        _require_management_service(management_service)
        assert catalog_query_service is not None
        catalog_items = await catalog_query_service.list_latest(
            identity.tenant_id,
            SkillCatalogQuery(
                text=q,
                publisher=publisher,
                risk_level=risk_level,
                publication_status=publication_status,
                installation_status=installation_status,
                source_id=source_id,
            ),
        )
        items: list[dict[str, Any]] = []
        for catalog_item in catalog_items:
            publication = catalog_item.publication
            manifest = publication.manifest
            installation = catalog_item.installation
            current_state = catalog_item.publication_state
            publication_payload = {
                "status": publication.status.value,
                "revision": current_state.revision,
                "source_id": current_state.source_id,
            }
            installation_payload = (
                None if installation is None else _installation_summary(installation)
            )
            items.append(
                {
                    **_summary(publication),
                    "latest_version": manifest.version,
                    "publication": publication_payload,
                    "installation": installation_payload,
                    "availability": catalog_item.availability,
                }
            )
        page, next_cursor = _page(items, cursor=cursor, limit=limit, key=_skill_item_key)
        return {"skills": page, "items": page, "next_cursor": next_cursor}

    @router.get("/skill-admissions")
    async def list_skill_admissions(
        identity: Identity,
        outcome: Annotated[
            Literal["accepted", "rejected", "quarantined"] | None,
            Query(),
        ] = None,
        stage: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        content_policy_version: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        since: AwareDatetime | None = None,
        cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        reader = _require_admission_reader(admission_reader)
        page = await reader.page_admissions(
            identity.tenant_id,
            outcome=outcome,
            stage=stage,
            content_policy_version=content_policy_version,
            since=since,
            cursor=cursor,
            limit=limit,
        )
        return {
            "admissions": [
                {
                    **asdict(record),
                    "occurred_at": record.occurred_at.isoformat(),
                }
                for record in page.admissions
            ],
            "next_cursor": page.next_cursor,
        }

    @router.get("/skill-admissions/metrics")
    async def skill_admission_metrics(
        identity: Identity,
        window_hours: Annotated[int | None, Query(ge=1, le=2160)] = None,
    ) -> dict[str, Any]:
        reader = _require_admission_reader(admission_reader)
        selected_window = window_hours or admission_metrics_window_hours
        observed_at = datetime.now(UTC)
        since = observed_at - timedelta(hours=selected_window)
        groups = await reader.admission_metrics(identity.tenant_id, since=since)
        metrics: list[dict[str, Any]] = []
        for group in groups:
            labels = {
                "outcome": group.outcome,
                "content_policy_version": group.content_policy_version,
                "window_hours": str(selected_window),
            }
            metrics.extend(
                (
                    {
                        "name": "skill.admission.count",
                        "value": group.count,
                        "labels": labels,
                    },
                    {
                        "name": "skill.admission.duration_ms.average",
                        "value": group.average_duration_ms,
                        "labels": labels,
                    },
                )
            )
        sample_count = sum(group.count for group in groups)
        quarantined_count = sum(group.count for group in groups if group.outcome == "quarantined")
        quarantine_ratio = quarantined_count / sample_count if sample_count else 0.0
        ratio_labels = {"window_hours": str(selected_window)}
        metrics.append(
            {
                "name": "skill.admission.quarantine_ratio",
                "value": quarantine_ratio,
                "labels": ratio_labels,
            }
        )
        alert_status = (
            "insufficient_data"
            if sample_count < admission_quarantine_alert_min_samples
            else "firing"
            if quarantine_ratio > admission_quarantine_alert_ratio
            else "ok"
        )
        return {
            "window": {
                "hours": selected_window,
                "since": since.isoformat(),
                "observed_at": observed_at.isoformat(),
            },
            "metrics": metrics,
            "alerts": [
                {
                    "rule": "skill.admission.quarantine_ratio",
                    "status": alert_status,
                    "value": quarantine_ratio,
                    "threshold": admission_quarantine_alert_ratio,
                    "sample_count": sample_count,
                    "minimum_samples": admission_quarantine_alert_min_samples,
                }
            ],
        }

    @router.get("/skill-publishers")
    async def list_skill_publishers(
        identity: Identity,
        status_filter: Annotated[
            str | None, Query(alias="status", max_length=64)
        ] = None,
        q: Annotated[str | None, Query(max_length=256)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        normalized_query = (q or "").casefold()
        records = [
            _publisher_summary(record, keys)
            for record, keys in await service.list_publishers(identity.tenant_id)
            if (not status_filter or record.status.value == status_filter)
            and (
                not normalized_query
                or normalized_query
                in f"{record.publisher} {record.display_name}".casefold()
            )
        ]
        page, next_cursor = _page(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (str(item["publisher"]["publisher"]),),
        )
        return {"publishers": page, "next_cursor": next_cursor}

    @router.get("/skill-publishers/{publisher}")
    async def get_skill_publisher(publisher: str, identity: Identity) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        record, keys = await service.get_publisher(identity.tenant_id, publisher)
        return _publisher_summary(record, keys)

    @router.post("/skill-publishers/{publisher}", status_code=status.HTTP_201_CREATED)
    async def register_skill_publisher(
        publisher: str,
        payload: RegisterSkillPublisherRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(default=0, alias="X-Expected-Revision", ge=0),
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
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
        return _publisher_summary(record, keys)

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
        service = _require_publisher_service(publisher_service)
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
        return _publisher_summary(record, keys)

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
        service = _require_publisher_service(publisher_service)
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
        return _publisher_summary(record, keys)

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
        service = _require_publisher_service(publisher_service)
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
        return _publisher_summary(record, keys)

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

    @router.get("/skill-installations")
    async def list_skill_installations(
        identity: Identity,
        status_filter: Annotated[
            str | None, Query(alias="status", max_length=64)
        ] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        source_id: Annotated[str | None, Query(max_length=128)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        service = _require_management_service(management_service)
        records = [
            _installation_summary(item)
            for item in await service.list_installations(identity.tenant_id)
            if (not status_filter or item.status.value == status_filter)
            and (not publisher or item.publisher == publisher)
            and (not source_id or item.source_id == source_id)
        ]
        page, next_cursor = _page(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (str(item["publisher"]), str(item["name"])),
        )
        return {"installations": page, "next_cursor": next_cursor}

    @router.get("/skill-publications")
    async def list_skill_publications(
        identity: Identity,
        status_filter: Annotated[
            str | None, Query(alias="status", max_length=64)
        ] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        name: Annotated[str | None, Query(max_length=256)] = None,
        source_id: Annotated[str | None, Query(max_length=128)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        service = _require_management_service(management_service)
        records = [
            _publication_state_summary(item)
            for item in await service.list_publications(identity.tenant_id)
            if (not status_filter or item.status.value == status_filter)
            and (not publisher or item.publisher == publisher)
            and (not name or item.name == name)
            and (not source_id or item.source_id == source_id)
        ]
        page, next_cursor = _page(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (
                str(item["publisher"]),
                str(item["name"]),
                str(item["version"]),
            ),
        )
        return {"publications": page, "next_cursor": next_cursor}

    @router.get("/skill-packages")
    async def list_skill_packages(
        identity: Identity,
        retention_status: Annotated[str | None, Query(max_length=64)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        name: Annotated[str | None, Query(max_length=256)] = None,
        legal_hold: bool | None = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        service = _require_management_service(management_service)
        records = [
            _package_state_summary(item)
            for item in await service.list_packages(identity.tenant_id)
            if (not retention_status or item.retention_status.value == retention_status)
            and (not publisher or item.manifest.publisher == publisher)
            and (not name or item.manifest.name == name)
            and (legal_hold is None or item.legal_hold is legal_hold)
        ]
        page, next_cursor = _page(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (
                str(item["publisher"]),
                str(item["name"]),
                str(item["version"]),
            ),
        )
        return {"packages": page, "next_cursor": next_cursor}

    @router.get("/skills/{publisher}/{name}")
    async def get_skill(publisher: str, name: str, identity: Identity) -> dict[str, Any]:
        service = _require_management_service(management_service)
        records = [
            item
            for item in await service.list_publications(identity.tenant_id)
            if item.publisher == publisher and item.name == name
        ]
        if not records:
            raise NotFoundError("Skill publication not found")
        state = max(records, key=lambda item: _semver_key(item.version))
        package = await service.get_package(
            identity.tenant_id, publisher, name, state.version
        )
        publication = _published_skill(package, state)
        markdown = await _skill_markdown(
            content_reader,
            registry,
            identity.tenant_id,
            publisher,
            name,
            state.version,
        )
        versions = sorted((item.version for item in records), key=_semver_key, reverse=True)
        payload = _summary(publication, skill_markdown=markdown)
        payload["versions"] = versions
        return payload

    @router.get("/skills/{publisher}/{name}/management")
    async def get_skill_management_view(
        publisher: str, name: str, identity: Identity
    ) -> dict[str, Any]:
        service = _require_management_service(management_service)
        publications = tuple(
            item
            for item in await service.list_publications(identity.tenant_id)
            if item.publisher == publisher and item.name == name
        )
        packages = {
            item.manifest.version: item
            for item in await service.list_packages(identity.tenant_id)
            if item.manifest.publisher == publisher and item.manifest.name == name
        }
        try:
            installation = await service.get_installation(
                identity.tenant_id, publisher, name
            )
        except NotFoundError:
            installation = None
        versions = [
            {
                "publication": _publication_state_summary(item),
                "package": (
                    None
                    if item.version not in packages
                    else _package_state_summary(packages[item.version])
                ),
            }
            for item in publications
        ]
        return {
            "publisher": publisher,
            "name": name,
            "installation": (
                None if installation is None else _installation_summary(installation)
            ),
            "versions": versions,
        }

    @router.get("/skills/{publisher}/{name}/versions/{version}")
    async def get_skill_version(
        publisher: str, name: str, version: str, identity: Identity
    ) -> dict[str, Any]:
        service = _require_management_service(management_service)
        state = await service.get_publication(
            identity.tenant_id, publisher, name, version
        )
        package = await service.get_package(identity.tenant_id, publisher, name, version)
        publication = _published_skill(package, state)
        markdown = await _skill_markdown(
            content_reader,
            registry,
            identity.tenant_id,
            publisher,
            name,
            version,
        )
        return _summary(publication, skill_markdown=markdown)

    @router.get("/skills/{publisher}/{name}/installation")
    async def get_skill_installation(
        publisher: str,
        name: str,
        identity: Identity,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        installation = await management_service.get_installation(
            identity.tenant_id,
            publisher,
            name,
        )
        return {"installation": _installation_summary(installation)}

    @router.get("/skill-publications/{publisher}/{name}/versions/{version}")
    async def get_skill_publication_state(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        publication = await management_service.get_publication(
            identity.tenant_id,
            publisher,
            name,
            version,
        )
        return {"publication": _publication_state_summary(publication)}

    @router.get("/skill-packages/{publisher}/{name}/versions/{version}")
    async def get_skill_package_state(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        package = await management_service.get_package(identity.tenant_id, publisher, name, version)
        return {"package": _package_state_summary(package)}

    @router.post(
        "/skill-package-uploads",
        status_code=status.HTTP_201_CREATED,
    )
    async def proxy_skill_package_upload(
        request: Request,
        identity: Identity,
        command_id: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ],
        upload_name: Annotated[
            str,
            Header(alias="X-Upload-Name", min_length=1, max_length=512),
        ],
        expected_checksum: Annotated[
            str,
            Header(alias="X-Content-SHA256", pattern=r"^[0-9a-f]{64}$"),
        ],
    ) -> dict[str, Any]:
        if upload_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill package upload service is not configured",
            )
        if request.headers.get("content-type", "").split(";", 1)[0].strip() != (
            "application/vnd.auraclaw.skill-package+json"
        ):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Skill package upload content type is invalid",
            )
        content = bytearray()
        async for chunk in request.stream():
            if len(content) + len(chunk) > _MAX_ENCODED_UPLOAD_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="Skill package upload exceeds 24 MiB",
                )
            content.extend(chunk)
        if not content:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Skill package upload is empty",
            )
        checksum = hashlib.sha256(content).hexdigest()
        if checksum != expected_checksum:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Skill package upload checksum mismatch",
            )
        result = await upload_service.stage(
            tenant_id=identity.tenant_id,
            name=upload_name,
            content=bytes(content),
            checksum=checksum,
            correlation_id=identity.correlation_id,
            command_id=command_id,
        )
        return result.model_dump(mode="json")

    @router.post(
        "/skill-publications",
        status_code=status.HTTP_201_CREATED,
    )
    async def publish_skill(
        payload: PublishSkillRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(default=0, alias="X-Expected-Revision", ge=0),
    ) -> dict[str, Any]:
        if publication_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill publication service is not configured",
            )
        command = PublishSkillCommand(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            source_id=payload.source_id,
            activate=payload.activate,
            command_id=command_id,
            expected_revision=expected_revision,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
        )
        if payload.files is not None:
            encoded_size = sum(len(value) for value in payload.files.values())
            if encoded_size > _MAX_ENCODED_UPLOAD_BYTES:
                raise HTTPException(status_code=413, detail="Skill package is too large")
            try:
                files = {
                    path: base64.b64decode(content, validate=True)
                    for path, content in payload.files.items()
                }
            except (binascii.Error, ValueError) as exc:
                raise HTTPException(
                    status_code=422,
                    detail="Skill package files must use valid base64",
                ) from exc
            publication = await publication_service.publish(command, SkillPackage.from_files(files))
        else:
            assert payload.artifact_ref is not None
            assert payload.expected_digest is not None
            publication = await publication_service.publish_artifact(
                command,
                ArtifactRef(**payload.artifact_ref),
                payload.expected_digest,
            )
        return _summary(publication)

    @router.post(
        "/skills/{publisher}/{name}:install",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def install_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        return await _change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.INSTALL,
            command_id,
            expected_revision,
        )

    @router.post(
        "/skills/{publisher}/{name}:enable",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def enable_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        return await _change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.ENABLE,
            command_id,
            expected_revision,
        )

    @router.post(
        "/skills/{publisher}/{name}:disable",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def disable_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await _change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.DISABLE,
            command_id,
            expected_revision,
            reason_code,
        )

    @router.post(
        "/skills/{publisher}/{name}:uninstall",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def uninstall_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
        force: bool = Query(default=False),
    ) -> dict[str, Any]:
        return await _change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.UNINSTALL,
            command_id,
            expected_revision,
            reason_code,
            force=force,
        )

    @router.post(
        "/skill-publications/{publisher}/{name}/versions/{version}:revoke",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def revoke_skill_publication(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
        revocation_action: Annotated[
            SkillRevocationAction,
            Header(alias="X-Skill-Revocation-Action"),
        ] = SkillRevocationAction.CANCEL,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        publication = await management_service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                name=name,
                version=version,
                reason_code=reason_code,
                revocation_action=revocation_action,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"publication": _publication_state_summary(publication)}

    @router.post(
        "/skill-publications/{publisher}/{name}/versions/{version}:restore",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def restore_skill_publication(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        publication = await management_service.restore_publication(
            RestoreSkillPublicationCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                name=name,
                version=version,
                reason_code=reason_code,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"publication": _publication_state_summary(publication)}

    @router.post(
        "/skill-packages/{publisher}/{name}/versions/{version}:purge",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def purge_skill_package(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        package = await management_service.purge_package(
            PurgeSkillPackageCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                name=name,
                version=version,
                reason_code=reason_code,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"package": _package_state_summary(package)}

    return router


def _require_publisher_service(
    service: SkillPublisherManager | None,
) -> SkillPublisherManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill Publisher Registry is not configured",
        )
    return service


def _require_management_service(service: SkillManager | None) -> SkillManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill management service is not configured",
        )
    return service


def _require_admission_reader(
    reader: SkillAdmissionReader | None,
) -> SkillAdmissionReader:
    if reader is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill admission reader is not configured",
        )
    return reader


def _require_source_service(
    service: SkillSourceManager | None,
) -> SkillSourceManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill Source service is not configured",
        )
    return service


def _source_summary(source: SkillSourceRecord) -> dict[str, Any]:
    return source.model_dump(mode="json")


def _publisher_summary(
    publisher: SkillPublisherRecord,
    keys: tuple[SkillPublisherKeyRecord, ...],
) -> dict[str, Any]:
    return {
        "publisher": publisher.model_dump(mode="json"),
        "keys": [key.model_dump(mode="json") for key in keys],
    }


async def _change_installation(
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
    return {"installation": _installation_summary(installation)}


def _installation_summary(
    installation: SkillInstallationRecord,
) -> dict[str, Any]:
    return {
        "publisher": installation.publisher,
        "name": installation.name,
        "version_constraint": installation.version_constraint,
        "pinned_package_digest": installation.pinned_package_digest,
        "status": installation.status.value,
        "source_id": installation.source_id,
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


def _publication_state_summary(
    publication: SkillPublicationRecord,
) -> dict[str, Any]:
    return {
        "publisher": publication.publisher,
        "name": publication.name,
        "version": publication.version,
        "package_digest": publication.package_digest,
        "status": publication.status.value,
        "source_id": publication.source_id,
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


def _package_state_summary(package: SkillPackageRecord) -> dict[str, Any]:
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


async def _skill_markdown(
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


def _skill_item_key(item: dict[str, Any]) -> tuple[str, ...]:
    return (str(item["publisher"]), str(item["name"]))


def _page(
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
            if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values
            ):
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
        encoded = json.dumps(
            {"after": list(key(page[-1]))}, separators=(",", ":")
        ).encode()
        next_cursor = base64.urlsafe_b64encode(encoded).decode().rstrip("=")
    return page, next_cursor
