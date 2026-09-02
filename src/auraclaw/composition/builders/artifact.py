from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import FastAPI

from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.artifact.internal_service import ArtifactInternalService
from auraclaw.composition.object_storage import build_object_storage, object_storage_closeables
from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _require_production_security_configuration,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.persistence.postgres_admin_store import PostgresAdminOperationStore
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import admin_routes, artifact_routes


def build_artifact_service_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    _require_production_security_configuration(
        settings,
        spec.name,
        (
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.TASK_API,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.ARTIFACT_SERVICE,
        ),
        requires_policy=True,
    )
    storage = build_object_storage(settings)
    repository = (
        PostgresArtifactRepository(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    admin_store = (
        PostgresAdminOperationStore(settings.resolved_database_url, schema="artifact")
        if settings.sql_storage_enabled
        else None
    )
    policy: RemotePolicyClient | None = None
    token = settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
    if token:
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token,
            service_identity=ServiceIdentity.ARTIFACT_SERVICE,
        )
    closeables: tuple[Any, ...] = (
        *((repository,) if repository is not None else ()),
        *((admin_store,) if admin_store is not None else ()),
        *object_storage_closeables(storage),
        *((policy,) if policy is not None else ()),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=(
            storage.verifier.readiness if storage.verifier is not None else None
        ),
    )
    service = ArtifactInternalService(
        storage.presigner,
        repository=repository,
        object_verifier=storage.verifier,
        policy=policy,
        multipart=storage.multipart,
        multipart_threshold=settings.artifact_multipart_threshold,
        multipart_part_size=settings.artifact_multipart_part_size,
        claim_ttl=timedelta(seconds=settings.artifact_claim_ttl_seconds),
        orphan_claim_limit=settings.artifact_orphan_claim_limit,
    )

    async def artifact_status(parameters: dict[str, Any]) -> dict[str, Any]:
        del parameters
        return {"storage": storage.backend, "metadata_owner": "artifact-service"}

    async def artifact_retention(parameters: dict[str, Any]) -> dict[str, Any]:
        del parameters
        deleted = await service.cleanup_expired()
        return {"expired_uploads_deleted": deleted}

    routes = artifact_routes(service)
    routes.update(
        admin_routes(
            OwnerAdminService(
                ServiceIdentity.ARTIFACT_SERVICE,
                {"status": artifact_status, "retention": artifact_retention},
                store=admin_store,
            )
        )
    )
    contract_app = create_contract_app(
        "artifact-service",
        routes,
        workload_identities=_configured_identities(
            settings,
            (
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.TASK_API,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        ),
    )
    app.mount("/", contract_app)
    return app
