from __future__ import annotations

import ipaddress
import json
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from auraclaw.contracts.capabilities import JavaApiOperationDefinition, JavaApiServerDefinition
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.infrastructure.credentials.mcp_egress import (
    HttpxPinnedMcpSender,
    McpDnsResolver,
    McpEgressResponse,
    McpPinnedSender,
    SystemMcpDnsResolver,
)

_FORBIDDEN_REQUEST_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "base_url",
    "bearer",
    "client_secret",
    "credential",
    "credential_ref",
    "endpoint",
    "headers",
    "host",
    "method",
    "port",
    "refresh_token",
    "scheme",
    "secret",
    "token",
    "url",
    "uri",
}
_ALLOWED_REQUEST_KEYS = {
    "body",
    "idempotency_key",
    "operation_id",
    "path",
    "query",
    "server_id",
}


class ManagedJavaApiEgressAdapter:
    """Credential-domain Java API connector with DNS/IP pinning and no redirects."""

    def __init__(
        self,
        server: JavaApiServerDefinition,
        *,
        resolver: McpDnsResolver | None = None,
        sender: McpPinnedSender | None = None,
        max_response_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        if not server.enabled:
            raise ValueError("Java API egress server must be enabled")
        if server.credential_ref is None:
            raise ValueError("Java API egress server requires a credential_ref")
        self._server = server
        self._operations = {item.operation_id: item for item in server.operations}
        self._resolver = resolver or SystemMcpDnsResolver()
        self._sender = sender or HttpxPinnedMcpSender()
        self._max_response_bytes = max_response_bytes

    @property
    def target(self) -> str:
        return f"java-api:{self._server.server_id}"

    async def __call__(
        self,
        request: dict[str, Any],
        secret: str,
    ) -> dict[str, Any]:
        if set(request).difference(_ALLOWED_REQUEST_KEYS):
            raise CredentialAccessError("Java API egress request contains unsupported fields")
        if _contains_forbidden_key(request):
            raise CredentialAccessError(
                "Java API egress request may not carry credentials or targets"
            )
        if request.get("server_id") != self._server.server_id:
            raise CredentialAccessError("Java API egress server binding does not match")
        operation_id = str(request.get("operation_id", ""))
        operation = self._operations.get(operation_id)
        if operation is None:
            raise CredentialAccessError("Java API operation is not registered")
        path = str(request.get("path", ""))
        _validate_relative_path(path, operation)
        query = request.get("query", {})
        body = request.get("body", {})
        if not isinstance(query, dict) or not isinstance(body, dict):
            raise CredentialAccessError("Java API query and body must be objects")
        if _contains_forbidden_key(query) or _contains_forbidden_key(body):
            raise CredentialAccessError("Java API fields may not carry credentials or targets")
        url = _join_url(self._server.base_url, path, query if operation.method == "GET" else {})
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {secret}",
        }
        content = b""
        if operation.method != "GET":
            headers["Content-Type"] = "application/json"
            encoded = json.dumps(body, separators=(",", ":")).encode()
            if len(encoded) > 256 * 1024:
                raise CredentialAccessError("Java API request exceeds the configured limit")
            content = encoded
        idempotency_key = request.get("idempotency_key")
        if operation.idempotent and isinstance(idempotency_key, str) and idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        response = await self._send_pinned(operation.method, url, headers, content)
        return _decode_json_response(response, self._max_response_bytes, secret)

    async def aclose(self) -> None:
        close = getattr(self._sender, "aclose", None)
        if close is not None:
            await close()

    async def _send_pinned(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        content: bytes,
    ) -> McpEgressResponse:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        if hostname is None:
            raise CredentialAccessError("Java API URL is missing a hostname")
        port = parsed.port or 443
        addresses = await self._resolver.resolve(hostname, port)
        approved = _approved_addresses(
            addresses,
            hostname=hostname,
            allowed_private_hosts=self._server.allowed_private_hosts,
        )
        if not approved:
            raise CredentialAccessError("Java API DNS did not return an approved address")
        return await self._sender.send(
            method=method,
            url=url,
            server_hostname=hostname,
            approved_ip=approved[0],
            headers=headers,
            content=content,
        )


def catalog_server_id(server: JavaApiServerDefinition) -> str:
    return server.server_id


def _validate_relative_path(path: str, operation: JavaApiOperationDefinition) -> None:
    if (
        not path.startswith("/")
        or "://" in path
        or ".." in path
        or "//" in path
        or any(character.isspace() for character in path)
    ):
        raise CredentialAccessError("Java API path is unsafe")
    template_prefix = operation.path_template.split("{", 1)[0]
    if not path.startswith(template_prefix):
        raise CredentialAccessError("Java API path is outside the registered template")


def _join_url(base_url: str, path: str, query: dict[str, Any]) -> str:
    parsed = urlsplit(base_url)
    encoded_query = urlencode(
        {str(key): str(value) for key, value in query.items()},
        doseq=True,
    )
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            encoded_query,
            "",
        )
    )


def _approved_addresses(
    addresses: tuple[str, ...],
    *,
    hostname: str,
    allowed_private_hosts: tuple[str, ...],
) -> tuple[str, ...]:
    allow_private = hostname.lower() in {item.lower() for item in allowed_private_hosts}
    approved: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise CredentialAccessError("Java API DNS returned an invalid address") from exc
        if address.is_global:
            approved.append(address.compressed)
            continue
        if allow_private and (address.is_private or address.is_loopback):
            approved.append(address.compressed)
            continue
        raise CredentialAccessError("Java API DNS resolved to a non-public address")
    return tuple(sorted(set(approved)))


def _decode_json_response(
    response: McpEgressResponse,
    max_response_bytes: int,
    secret: str,
) -> dict[str, Any]:
    if len(response.content) > max_response_bytes:
        raise CredentialAccessError("Java API response exceeds the configured limit")
    if not 200 <= response.status_code < 300:
        raise CredentialAccessError(f"Java API server returned HTTP {response.status_code}")
    try:
        payload = json.loads(response.content)
    except json.JSONDecodeError as exc:
        raise CredentialAccessError("Java API response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise CredentialAccessError("Java API response must contain a JSON object")
    return dict(_redact(payload, secret))


def _redact(value: Any, secret: str) -> Any:
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]") if secret else value
    if isinstance(value, dict):
        return {str(key): _redact(item, secret) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secret) for item in value]
    return value


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if lowered in _FORBIDDEN_REQUEST_KEYS or "header" in lowered:
                return True
            if _contains_forbidden_key(item):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False
