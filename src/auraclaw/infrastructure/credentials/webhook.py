from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from auraclaw.contracts.errors import CredentialAccessError


class ManagedWebhookCredentialAdapter:
    """Credential-domain webhook egress; the caller never receives the signing secret."""

    def __init__(
        self,
        *,
        allowed_hosts: tuple[str, ...],
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._allowed_hosts = tuple(host.lower().strip(".") for host in allowed_hosts)
        self._client = client

    async def __call__(self, request: dict[str, Any], secret: str) -> dict[str, Any]:
        target = str(request.get("target_url", ""))
        parsed = urlsplit(target)
        host = (parsed.hostname or "").lower().strip(".")
        if parsed.scheme != "https" or not host or not self._allowed(host):
            raise CredentialAccessError("webhook target is outside egress allowlist")
        try:
            if ipaddress.ip_address(host).is_private:
                raise CredentialAccessError("private webhook target is forbidden")
        except ValueError:
            pass
        payload = dict(request.get("payload", {}))
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        timestamp = str(int(datetime.now(UTC).timestamp()))
        signature = hmac.new(
            secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": str(request.get("delivery_id", "")),
            "X-AuraClaw-Timestamp": timestamp,
            "X-AuraClaw-Signature": f"sha256={signature}",
        }
        if self._client is not None:
            response = await self._client.post(target, content=body, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(target, content=body, headers=headers)
        return {
            "succeeded": 200 <= response.status_code < 300,
            "retryable": response.status_code == 429 or response.status_code >= 500,
            "summary": f"HTTP {response.status_code}",
        }

    def _allowed(self, host: str) -> bool:
        return any(
            host == allowed or host.endswith(f".{allowed}")
            for allowed in self._allowed_hosts
        )
