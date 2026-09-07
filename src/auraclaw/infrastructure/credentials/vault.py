from __future__ import annotations

import asyncio
from urllib.parse import quote

import httpx

from auraclaw.contracts.errors import CredentialAccessError


class HashiCorpVault:
    """Minimal Vault KV v2 resolver; values never leave Credential Proxy."""

    def __init__(
        self,
        address: str,
        *,
        token: str | None = None,
        approle_role_id: str | None = None,
        approle_secret_id: str | None = None,
        approle_mount: str = "approle",
        mount: str = "secret",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        approle_configured = bool(approle_role_id) or bool(approle_secret_id)
        if approle_configured and not (approle_role_id and approle_secret_id):
            raise ValueError("Vault AppRole requires both role_id and secret_id")
        if not token and not approle_configured:
            raise ValueError("Vault authentication is required")
        self._mount = quote(mount.strip("/"), safe="")
        self._approle_mount = quote(approle_mount.strip("/"), safe="")
        self._approle_role_id = approle_role_id
        self._approle_secret_id = approle_secret_id
        self._token = token
        self._auth_lock = asyncio.Lock()
        self._client = httpx.AsyncClient(
            base_url=address.rstrip("/"),
            timeout=5.0,
            transport=transport,
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _login_approle(self) -> str:
        if self._approle_role_id is None or self._approle_secret_id is None:
            raise CredentialAccessError("credential vault authentication is unavailable")
        try:
            response = await self._client.post(
                f"/v1/auth/{self._approle_mount}/login",
                json={
                    "role_id": self._approle_role_id,
                    "secret_id": self._approle_secret_id,
                },
            )
        except httpx.HTTPError as exc:
            raise CredentialAccessError("credential vault is unavailable") from exc
        if response.is_error:
            raise CredentialAccessError("credential vault authentication failed")
        try:
            payload = response.json()
        except ValueError as exc:
            raise CredentialAccessError(
                "credential vault authentication response is invalid"
            ) from exc
        auth = payload.get("auth") if isinstance(payload, dict) else None
        token = auth.get("client_token") if isinstance(auth, dict) else None
        if not isinstance(token, str) or not token:
            raise CredentialAccessError("credential vault authentication response is invalid")
        self._token = token
        return token

    async def _authenticated_get(self, path: str) -> httpx.Response:
        async with self._auth_lock:
            token = self._token
            if token is None:
                token = await self._login_approle()
        try:
            response = await self._client.get(path, headers={"X-Vault-Token": token})
        except httpx.HTTPError as exc:
            raise CredentialAccessError("credential vault is unavailable") from exc
        if response.status_code != 403 or self._approle_role_id is None:
            return response
        if path != "/v1/auth/token/lookup-self":
            try:
                token_status = await self._client.get(
                    "/v1/auth/token/lookup-self",
                    headers={"X-Vault-Token": token},
                )
            except httpx.HTTPError as exc:
                raise CredentialAccessError("credential vault is unavailable") from exc
            if token_status.status_code != 403:
                return response
        async with self._auth_lock:
            if self._token == token:
                self._token = None
                token = await self._login_approle()
            else:
                token = self._token
                assert token is not None
        try:
            return await self._client.get(path, headers={"X-Vault-Token": token})
        except httpx.HTTPError as exc:
            raise CredentialAccessError("credential vault is unavailable") from exc

    async def resolve(self, credential_ref: str) -> str:
        path, separator, field = credential_ref.rpartition("#")
        if not separator:
            path = credential_ref
            field = "value"
        if not path or not field:
            raise CredentialAccessError("credential vault reference is invalid")
        encoded_path = quote(path, safe="")
        response = await self._authenticated_get(
            f"/v1/{self._mount}/data/{encoded_path}"
        )
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
        if response.status_code not in {200, 429, 472, 473}:
            return False, f"health HTTP {response.status_code}"
        try:
            auth_response = await self._authenticated_get("/v1/auth/token/lookup-self")
        except CredentialAccessError as exc:
            return False, exc.code
        ready = not auth_response.is_error
        return ready, f"health HTTP {response.status_code}; auth HTTP {auth_response.status_code}"
