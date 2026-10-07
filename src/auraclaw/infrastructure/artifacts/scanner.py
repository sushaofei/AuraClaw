from __future__ import annotations

import re
from typing import Literal, cast

import httpx

from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.artifact.ports import ArtifactScanResult
from auraclaw.contracts.errors import ArtifactAccessError

_SAFE_VALUE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


class RemoteArtifactContentScanner:
    """Fail-closed adapter for an external malware/DLP content scanner."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        policy_version: str,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._policy_version = policy_version
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
            headers={"Authorization": f"Bearer {bearer_token}"},
        )

    async def scan(
        self, pending: PendingUpload, *, download_url: str
    ) -> ArtifactScanResult:
        response = await self._client.post(
            "/v1/artifacts:scan",
            json={
                "tenant_id": pending.tenant_id,
                "artifact_id": pending.artifact_id,
                "version": pending.version,
                "download_url": download_url,
                "media_type": pending.media_type,
                "expected_size": pending.expected_size,
                "expected_checksum": pending.expected_checksum,
                "classification": pending.classification,
                "policy_version": self._policy_version,
            },
        )
        if response.status_code != 200:
            raise ArtifactAccessError("artifact content scanner request failed")
        try:
            payload = response.json()
            verdict = str(payload["verdict"])
            policy_version = str(payload["policy_version"])
            finding = payload.get("finding_code")
            finding_code = str(finding) if finding is not None else None
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactAccessError("artifact content scanner response is invalid") from exc
        if verdict not in {"clean", "quarantined"}:
            raise ArtifactAccessError("artifact content scanner response is invalid")
        if policy_version != self._policy_version or not _SAFE_VALUE.fullmatch(policy_version):
            raise ArtifactAccessError("artifact content scanner policy version mismatch")
        if verdict == "quarantined" and (
            finding_code is None or not _SAFE_VALUE.fullmatch(finding_code)
        ):
            raise ArtifactAccessError("artifact content scanner finding is invalid")
        return ArtifactScanResult(
            verdict=cast(Literal["clean", "quarantined"], verdict),
            policy_version=policy_version,
            finding_code=finding_code,
        )

    async def readiness(self) -> tuple[bool, str]:
        try:
            response = await self._client.get("/health/ready")
        except httpx.HTTPError:
            return False, "unavailable"
        ready = response.status_code == 200
        return ready, "ready" if ready else "unavailable"

    async def aclose(self) -> None:
        await self._client.aclose()
