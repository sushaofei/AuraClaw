from __future__ import annotations

from dataclasses import dataclass

from auraclaw.artifact.ports import ObjectMultipartClient, ObjectVerifier
from auraclaw.config import Settings
from auraclaw.infrastructure.artifacts.s3 import (
    S3CompatibleMultipartClient,
    S3CompatibleObjectVerifier,
    S3CompatiblePresigner,
)


@dataclass(frozen=True)
class ObjectStorageBundle:
    backend: str
    presigner: S3CompatiblePresigner
    verifier: S3CompatibleObjectVerifier | None
    multipart: S3CompatibleMultipartClient | None


def build_object_storage(settings: Settings) -> ObjectStorageBundle:
    backend = settings.resolved_artifact_backend
    if settings.deployment_profile == "production":
        if backend == "local":
            raise ValueError("artifact-service production requires persistent object storage")
        if backend == "obs" and (settings.obs_ak is None or settings.obs_sk is None):
            raise ValueError("artifact-service production OBS credentials are missing")
        if backend == "seaweedfs" and (
            settings.seaweedfs_access_key is None
            or settings.seaweedfs_secret_key is None
        ):
            raise ValueError("artifact-service production SeaweedFS credentials are missing")

    if backend == "obs":
        access_key = (
            settings.obs_ak.get_secret_value()
            if settings.obs_ak is not None
            else "development-access-key"
        )
        secret_key = (
            settings.obs_sk.get_secret_value()
            if settings.obs_sk is not None
            else "development-secret-key"
        )
        presigner = S3CompatiblePresigner(
            settings.obs_s3_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=settings.obs_bucket,
            region=settings.obs_region,
            path_style=settings.obs_path_style,
        )
    elif backend == "seaweedfs":
        access_key = (
            settings.seaweedfs_access_key.get_secret_value()
            if settings.seaweedfs_access_key is not None
            else "development-access-key"
        )
        secret_key = (
            settings.seaweedfs_secret_key.get_secret_value()
            if settings.seaweedfs_secret_key is not None
            else "development-secret-key"
        )
        presigner = S3CompatiblePresigner(
            settings.seaweedfs_s3_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=settings.seaweedfs_bucket,
            region=settings.seaweedfs_region,
            path_style=settings.seaweedfs_path_style,
        )
    else:
        presigner = S3CompatiblePresigner(
            settings.seaweedfs_s3_endpoint,
            access_key="development-access-key",
            secret_key="development-secret-key",
            bucket=settings.seaweedfs_bucket,
            region=settings.seaweedfs_region,
            path_style=settings.seaweedfs_path_style,
        )

    if not settings.object_storage_enabled:
        return ObjectStorageBundle(
            backend=backend,
            presigner=presigner,
            verifier=None,
            multipart=None,
        )

    verifier = S3CompatibleObjectVerifier(presigner)
    multipart = S3CompatibleMultipartClient(presigner)
    return ObjectStorageBundle(
        backend=backend,
        presigner=presigner,
        verifier=verifier,
        multipart=multipart,
    )


def object_storage_closeables(
    bundle: ObjectStorageBundle,
) -> tuple[ObjectVerifier | ObjectMultipartClient, ...]:
    closeables: list[ObjectVerifier | ObjectMultipartClient] = []
    if bundle.verifier is not None:
        closeables.append(bundle.verifier)
    if bundle.multipart is not None:
        closeables.append(bundle.multipart)
    return tuple(closeables)
