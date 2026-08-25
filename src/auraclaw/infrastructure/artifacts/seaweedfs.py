from __future__ import annotations

from auraclaw.infrastructure.artifacts.s3 import (
    S3CompatibleMultipartClient,
    S3CompatibleObjectVerifier,
    S3CompatiblePresigner,
)

SeaweedFSS3Presigner = S3CompatiblePresigner
SeaweedFSMultipartClient = S3CompatibleMultipartClient
SeaweedFSObjectVerifier = S3CompatibleObjectVerifier

__all__ = [
    "SeaweedFSMultipartClient",
    "SeaweedFSObjectVerifier",
    "SeaweedFSS3Presigner",
    "S3CompatibleMultipartClient",
    "S3CompatibleObjectVerifier",
    "S3CompatiblePresigner",
]
