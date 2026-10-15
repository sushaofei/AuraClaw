from __future__ import annotations

from fastapi import APIRouter

from auraclaw.action.skill_admin_catalog import (
    SkillAdminCatalogQueryService,
)
from auraclaw.action.skill_admin_queries import SkillAdminManagementQueryService
from auraclaw.action.skill_admission_query import (
    SkillAdmissionMetricsPolicy,
    SkillAdmissionMetricsService,
)
from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.api.routes.skill_admin.admissions import create_admissions_router
from auraclaw.api.routes.skill_admin.catalog import create_catalog_router
from auraclaw.api.routes.skill_admin.contracts import (
    PublishSkillRequest,
    RegisterSkillPublisherRequest,
    RotateSkillPublisherKeyRequest,
    SkillAdmissionReader,
    SkillCapabilityAvailability,
    SkillContentReader,
    SkillManager,
    SkillPackageUploadManager,
    SkillPublisher,
    SkillPublisherManager,
)
from auraclaw.api.routes.skill_admin.installations import create_installations_router
from auraclaw.api.routes.skill_admin.publications import create_publications_router
from auraclaw.api.routes.skill_admin.publishers import create_publishers_router

__all__ = [
    "create_skill_admin_router",
    "PublishSkillRequest",
    "RegisterSkillPublisherRequest",
    "RotateSkillPublisherKeyRequest",
    "SkillPublisher",
    "SkillPackageUploadManager",
    "SkillCapabilityAvailability",
    "SkillManager",
    "SkillContentReader",
    "SkillPublisherManager",
    "SkillAdmissionReader",
]


def create_skill_admin_router(
    registry: SkillPackageRegistry,
    *,
    publication_service: SkillPublisher | None = None,
    management_service: SkillManager | None = None,
    content_reader: SkillContentReader | None = None,
    upload_service: SkillPackageUploadManager | None = None,
    publisher_service: SkillPublisherManager | None = None,
    admission_reader: SkillAdmissionReader | None = None,
    capability_availability: SkillCapabilityAvailability | None = None,
    admission_metrics_window_hours: int = 24,
    admission_quarantine_alert_ratio: float = 0.25,
    admission_quarantine_alert_min_samples: int = 20,
) -> APIRouter:
    policy = SkillAdmissionMetricsPolicy(
        window_hours=admission_metrics_window_hours,
        quarantine_alert_ratio=admission_quarantine_alert_ratio,
        quarantine_alert_min_samples=admission_quarantine_alert_min_samples,
    )
    router = APIRouter(prefix="/v1/admin", tags=["skill-admin"])
    catalog_query_service = (
        SkillAdminCatalogQueryService(management_service, capability_availability)
        if management_service is not None
        else None
    )
    management_query = (
        SkillAdminManagementQueryService(management_service)
        if management_service is not None
        else None
    )
    metrics_service = (
        SkillAdmissionMetricsService(admission_reader, policy)
        if admission_reader is not None
        else None
    )
    router.include_router(
        create_catalog_router(
            registry,
            management_service=management_service,
            management_query=management_query,
            content_reader=content_reader,
            catalog_query_service=catalog_query_service,
        )
    )
    router.include_router(
        create_admissions_router(
            admission_reader=admission_reader,
            metrics_service=metrics_service,
        )
    )
    router.include_router(create_publishers_router(publisher_service=publisher_service))
    router.include_router(
        create_installations_router(
            management_service=management_service, management_query=management_query
        )
    )
    router.include_router(
        create_publications_router(
            publication_service=publication_service,
            management_service=management_service,
            management_query=management_query,
            upload_service=upload_service,
        )
    )
    return router
