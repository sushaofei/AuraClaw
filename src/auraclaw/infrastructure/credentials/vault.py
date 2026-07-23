from __future__ import annotations

from urllib.parse import quote

import httpx

from auraclaw.contracts.errors import CredentialAccessError


class HashiCorpVault:
    """Minimal Vault KV v2 resolver; values never leave Credential Proxy."""

    def __init__(
        self,
        address: str,
        *,
        token: str,
        mount: str = "secret",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._mount = quote(mount.strip("/"), safe="")
        self._client = httpx.AsyncClient(
            base_url=address.rstrip("/"),
            headers={"X-Vault-Token": token},
            timeout=5.0,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def resolve(self, credential_ref: str) -> str:
        path, separator, field = credential_ref.rpartition("#")
        if not separator:
            path = credential_ref
            field = "value"
        if not path or not field:
            raise CredentialAccessError("credential vault reference is invalid")
        encoded_path = quote(path, safe="")
        try:
            response = await self._client.get(
                f"/v1/{self._mount}/data/{encoded_path}"
            )
        except httpx.HTTPError as exc:
            raise CredentialAccessError("credential vault is unavailable") from exc
        if response.is_error:
            raise CredentialAccessError("credential is unavailable or revoked")
        payload = response.json()
        value = payload.get("data", {}).get("data", {}).get(field)
        if not isinstance(value, str) or not value:
            raise CredentialAccessError("credential vault response has no usable value")
        return value

    async def revoke(self, credential_ref: str) -> None:
        del credential_ref
        # AuraClaw revokes the reference immediately. Vault secret lifecycle remains platform-owned.

    async def readiness(self) -> tuple[bool, str]:
        try:
            response = await self._client.get("/v1/sys/health")
        except httpx.HTTPError as exc:
            return False, type(exc).__name__
        ready = response.status_code in {200, 429, 472, 473}
        return ready, f"HTTP {response.status_code}"
