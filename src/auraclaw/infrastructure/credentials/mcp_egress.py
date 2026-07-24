from __future__ import annotations

import asyncio
import ipaddress
import json
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.errors import CredentialAccessError

_FORBIDDEN_REQUEST_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "bearer",
    "client_secret",
    "credential",
    "credential_ref",
    "headers",
    "refresh_token",
    "secret",
    "token",
}
_METHODS = {
    "initialize",
    "notifications/initialized",
    "ping",
    "resources/list",
    "resources/read",
    "resources/subscribe",
    "resources/templates/list",
    "tools/list",
    "tools/call",
    "prompts/list",
    "prompts/get",
}


class McpDnsResolver(Protocol):
    async def resolve(self, host: str, port: int) -> tuple[str, ...]: ...


class SystemMcpDnsResolver:
    async def resolve(self, host: str, port: int) -> tuple[str, ...]:
        loop = asyncio.get_running_loop()
        records = await loop.getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
        return tuple(sorted({str(record[4][0]) for record in records}))


@dataclass(frozen=True)
class McpEgressResponse:
    status_code: int
    headers: dict[str, str]
    content: bytes


class McpPinnedSender(Protocol):
    async def send(
        self,
        *,
        method: str,
        url: str,
        server_hostname: str,
        approved_ip: str,
        headers: dict[str, str],
        content: bytes,
    ) -> McpEgressResponse: ...


class HttpxPinnedMcpSender:
    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._owned = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        )

    async def aclose(self) -> None:
        if self._owned:
            await self._client.aclose()

    async def send(
        self,
        *,
        method: str,
        url: str,
        server_hostname: str,
        approved_ip: str,
        headers: dict[str, str],
        content: bytes,
    ) -> McpEgressResponse:
        parsed = urlsplit(url)
        ip_host = f"[{approved_ip}]" if ":" in approved_ip else approved_ip
        port = parsed.port or 443
        pinned_url = urlunsplit(
            (
                parsed.scheme,
                f"{ip_host}:{port}",
                parsed.path,
                parsed.query,
                "",
            )
        )
        request = self._client.build_request(
            method,
            pinned_url,
            headers={**headers, "Host": _authority(parsed)},
            content=content,
        )
        request.extensions["sni_hostname"] = server_hostname.encode()
        response = await self._client.send(request, follow_redirects=False)
        return McpEgressResponse(
            status_code=response.status_code,
            headers={key.lower(): value for key, value in response.headers.items()},
            content=response.content,
        )


@dataclass(frozen=True)
class _CachedToken:
    value: str
    expires_at: datetime


class ManagedMcpEgressAdapter:
    """Credential-domain MCP connector with OAuth and DNS/IP pinning."""

    def __init__(
        self,
        server: McpServerDefinition,
        *,
        resolver: McpDnsResolver | None = None,
        sender: McpPinnedSender | None = None,
        max_response_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        if not server.enabled:
            raise ValueError("MCP egress server must be enabled")
        if server.oauth is None or server.credential_ref is None:
            raise ValueError("MCP egress server requires managed OAuth configuration")
        _validate_https_url(server.endpoint)
        _validate_https_url(server.oauth.token_endpoint)
        _validate_https_url(server.oauth.resource)
        if _origin(server.endpoint) != _origin(server.oauth.resource):
            raise ValueError("OAuth Resource Indicator must match MCP server origin")
        self._server = server
        self._resolver = resolver or SystemMcpDnsResolver()
        self._sender = sender or HttpxPinnedMcpSender()
        self._max_response_bytes = max_response_bytes
        self._token: _CachedToken | None = None
        self._token_lock = asyncio.Lock()

    @property
    def credential_provider(self) -> str:
        return self._server.server_id

    @property
    def credential_scope(self) -> str:
        oauth = self._server.oauth
        assert oauth is not None
        return oauth.resource

    async def __call__(
        self,
        request: dict[str, Any],
        client_secret: str,
    ) -> dict[str, Any]:
        if set(request).difference({"id", "jsonrpc", "method", "params", "server_id"}):
            raise CredentialAccessError("MCP egress request contains unsupported fields")
        if _contains_forbidden_key(request):
            raise CredentialAccessError("MCP egress request may not carry credentials or targets")
        if request.get("server_id") != self._server.server_id:
            raise CredentialAccessError("MCP egress server binding does not match")
        method = str(request.get("method", ""))
        params = request.get("params", {})
        if method not in _METHODS or not isinstance(params, dict):
            raise CredentialAccessError("MCP method is not allowlisted")
        self._authorize_method(method, params)
        token = await self._access_token(client_secret)
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "method": method,
                "params": params,
            },
            separators=(",", ":"),
        ).encode()
        response = await self._send_pinned(
            "POST",
            self._server.endpoint,
            headers={
                "Accept": "application/json, text/event-stream",
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "MCP-Protocol-Version": self._server.protocol_revision,
                "Origin": _origin(self._server.endpoint),
            },
            content=payload,
        )
        result = _decode_mcp_response(response, self._max_response_bytes)
        return dict(_redact_exact(result, token))

    async def aclose(self) -> None:
        close = getattr(self._sender, "aclose", None)
        if close is not None:
            await close()

    async def _access_token(self, client_secret: str) -> str:
        now = datetime.now(UTC)
        if self._token is not None and self._token.expires_at > now:
            return self._token.value
        async with self._token_lock:
            now = datetime.now(UTC)
            if self._token is not None and self._token.expires_at > now:
                return self._token.value
            oauth = self._server.oauth
            assert oauth is not None
            body = urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": oauth.client_id,
                    "client_secret": client_secret,
                    "resource": oauth.resource,
                    **({"scope": " ".join(oauth.scopes)} if oauth.scopes else {}),
                }
            ).encode()
            response = await self._send_pinned(
                "POST",
                oauth.token_endpoint,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                content=body,
            )
            if not 200 <= response.status_code < 300:
                raise CredentialAccessError("MCP OAuth token exchange failed")
            if len(response.content) > self._max_response_bytes:
                raise CredentialAccessError("MCP OAuth response exceeds the configured limit")
            try:
                payload = json.loads(response.content)
            except json.JSONDecodeError as exc:
                raise CredentialAccessError("MCP OAuth response is invalid") from exc
            token = payload.get("access_token")
            token_type = str(payload.get("token_type", "")).casefold()
            if not isinstance(token, str) or not token or token_type != "bearer":
                raise CredentialAccessError("MCP OAuth response has no bearer token")
            try:
                expires_in = int(payload.get("expires_in", 300))
            except (TypeError, ValueError) as exc:
                raise CredentialAccessError("MCP OAuth expiry is invalid") from exc
            if expires_in < 1:
                raise CredentialAccessError("MCP OAuth token is already expired")
            self._token = _CachedToken(
                value=token,
                expires_at=now + timedelta(seconds=max(1, expires_in - 30)),
            )
            return token

    async def _send_pinned(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        content: bytes,
    ) -> McpEgressResponse:
        parsed = urlsplit(url)
        host = parsed.hostname
        if host is None:
            raise CredentialAccessError("MCP egress URL has no host")
        addresses = await self._resolver.resolve(host, parsed.port or 443)
        approved = _approved_addresses(addresses)
        if not approved:
            raise CredentialAccessError("MCP egress DNS has no public address")
        response = await self._sender.send(
            method=method,
            url=url,
            server_hostname=host,
            approved_ip=approved[0],
            headers=headers,
            content=content,
        )
        if 300 <= response.status_code < 400:
            raise CredentialAccessError("MCP egress redirects are forbidden")
        return response

    def _authorize_method(self, method: str, params: dict[str, Any]) -> None:
        if method == "tools/call":
            name = str(params.get("name", ""))
            if not _prefix_allowed(name, self._server.allowed_tool_prefixes):
                raise CredentialAccessError("MCP Tool is outside server allowlist")
        elif method == "resources/read":
            parsed = urlsplit(str(params.get("uri", "")))
            if parsed.scheme not in self._server.allowed_resource_schemes:
                raise CredentialAccessError("MCP Resource scheme is outside allowlist")
        elif method == "prompts/get":
            name = str(params.get("name", ""))
            if not _prefix_allowed(name, self._server.allowed_prompt_prefixes):
                raise CredentialAccessError("MCP Prompt is outside server allowlist")


def _validate_https_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("MCP egress URL must be an absolute HTTPS URL without userinfo")


def _approved_addresses(addresses: tuple[str, ...]) -> tuple[str, ...]:
    approved: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise CredentialAccessError("MCP DNS returned an invalid address") from exc
        if not address.is_global:
            raise CredentialAccessError("MCP DNS resolved to a non-public address")
        approved.append(address.compressed)
    return tuple(sorted(set(approved)))


def _decode_mcp_response(
    response: McpEgressResponse,
    max_response_bytes: int,
) -> dict[str, Any]:
    if len(response.content) > max_response_bytes:
        raise CredentialAccessError("MCP response exceeds the configured limit")
    if not 200 <= response.status_code < 300:
        raise CredentialAccessError(f"MCP server returned HTTP {response.status_code}")
    content_type = response.headers.get("content-type", "").split(";", 1)[0]
    try:
        if content_type == "text/event-stream":
            data = b"\n".join(
                line.removeprefix(b"data:").strip()
                for line in response.content.splitlines()
                if line.startswith(b"data:")
            )
            payload = json.loads(data)
        elif content_type in {"application/json", ""}:
            payload = json.loads(response.content)
        else:
            raise CredentialAccessError("MCP response Content-Type is not supported")
    except json.JSONDecodeError as exc:
        raise CredentialAccessError("MCP response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise CredentialAccessError("MCP response must contain a JSON object")
    return payload


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            str(key).casefold() in _FORBIDDEN_REQUEST_KEYS
            or _contains_forbidden_key(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def _prefix_allowed(value: str, prefixes: tuple[str, ...]) -> bool:
    return bool(value) and any(value.startswith(prefix) for prefix in prefixes)


def _origin(value: str) -> str:
    parsed = urlsplit(value)
    return f"{parsed.scheme}://{_authority(parsed)}"


def _authority(parsed: Any) -> str:
    host = parsed.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if parsed.port is not None and parsed.port != 443:
        return f"{host}:{parsed.port}"
    return host


def _redact_exact(value: Any, secret: str) -> Any:
    if isinstance(value, dict):
        return {str(key): _redact_exact(item, secret) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_exact(item, secret) for item in value]
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]")
    return value
