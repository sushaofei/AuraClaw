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

from auraclaw.contracts.capabilities import (
    McpAuthStrategy,
    McpNetworkMode,
    McpServerDefinition,
)
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.infrastructure.connectors.mcp.wire import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
)

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
    "server/discover",
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
        port = parsed.port or _default_port(parsed.scheme)
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
        if parsed.scheme == "https":
            request.extensions["sni_hostname"] = server_hostname.encode()
        try:
            response = await self._client.send(request, follow_redirects=False)
        except httpx.RequestError as exc:
            raise CredentialAccessError(
                "MCP egress target is unreachable",
                detail=type(exc).__name__,
            ) from exc
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
        auth = server.resolved_auth_strategy
        if auth is McpAuthStrategy.NONE:
            if server.resolved_network_mode is McpNetworkMode.PUBLIC:
                raise ValueError("public MCP egress cannot use auth_strategy none")
        elif server.credential_ref is None:
            raise ValueError("MCP egress server requires a credential_ref")
        if auth is McpAuthStrategy.OAUTH_CLIENT_CREDENTIALS:
            if server.oauth is None:
                raise ValueError("MCP egress server requires managed OAuth configuration")
            _validate_https_url(server.oauth.protected_resource_metadata_url)
            _validate_https_url(server.oauth.authorization_server_metadata_url)
            _validate_https_url(server.oauth.issuer)
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
        self._discovery_complete = False

    @property
    def secret_required(self) -> bool:
        return self._server.resolved_auth_strategy is not McpAuthStrategy.NONE

    @property
    def config_revision(self) -> int | None:
        return self._server.config_revision

    @property
    def credential_provider(self) -> str:
        return self._server.server_id

    @property
    def credential_scope(self) -> str:
        oauth = self._server.oauth
        if oauth is not None:
            return oauth.resource
        return _origin(self._server.endpoint)

    async def __call__(
        self,
        request: dict[str, Any],
        client_secret: str,
    ) -> dict[str, Any]:
        payload = dict(request)
        if payload.keys() - {
            "id",
            "jsonrpc",
            "method",
            "params",
            "server_id",
            "config_revision",
            "_auraclaw_identity",
        }:
            raise CredentialAccessError("MCP egress request contains unsupported fields")
        identity = payload.pop("_auraclaw_identity", None)
        if identity is not None and not isinstance(identity, dict):
            raise CredentialAccessError("MCP trusted identity is invalid")
        if _contains_forbidden_key(payload) or (
            isinstance(identity, dict) and _contains_forbidden_key(identity)
        ):
            raise CredentialAccessError("MCP egress request may not carry credentials or targets")
        if payload.get("server_id") != self._server.server_id:
            raise CredentialAccessError("MCP egress server binding does not match")
        requested_revision = payload.get("config_revision")
        if self._server.config_revision is not None:
            if requested_revision != self._server.config_revision:
                raise CredentialAccessError("MCP config revision mismatch")
        method = str(payload.get("method", ""))
        params = payload.get("params", {})
        if method not in _METHODS or not isinstance(params, dict):
            raise CredentialAccessError("MCP method is not allowlisted")
        if self._server.protocol_revision == MCP_PROTOCOL_VERSION:
            raw_meta = params.get("_meta")
            meta = raw_meta if isinstance(raw_meta, dict) else {}
            if (
                meta.get(MCP_PROTOCOL_VERSION_META_KEY) != MCP_PROTOCOL_VERSION
                or not isinstance(
                    meta.get(MCP_CLIENT_CAPABILITIES_META_KEY), dict
                )
            ):
                raise CredentialAccessError(
                    "modern MCP request metadata is missing or invalid"
                )
        self._authorize_method(method, params)
        token = await self._access_token(client_secret)
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": self._server.protocol_revision,
            "Mcp-Method": method,
            **(
                {"Mcp-Name": name}
                if (name := _request_target_name(method, params)) is not None
                else {}
            ),
            "Origin": _origin(self._server.endpoint),
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if (
            self._server.resolved_auth_strategy
            is McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
            and isinstance(identity, dict)
        ):
            tenant_id = identity.get("tenant_id")
            user_id = identity.get("user_id")
            dept_id = identity.get("dept_id")
            session_id = identity.get("session_id")
            if tenant_id:
                headers["X-CT-Tenant-ID"] = str(tenant_id)
            if user_id:
                headers["X-CT-User-ID"] = str(user_id)
            if dept_id:
                headers["X-CT-Dept-ID"] = str(dept_id)
            if session_id:
                headers["X-CT-Session-ID"] = str(session_id)
        jsonrpc_body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "method": method,
                "params": params,
            },
            separators=(",", ":"),
        ).encode()
        response = await self._send_pinned(
            "POST",
            self._server.endpoint,
            headers=headers,
            content=jsonrpc_body,
        )
        result = _decode_mcp_response(response, self._max_response_bytes)
        return dict(_redact_exact(result, token) if token else result)

    async def aclose(self) -> None:
        close = getattr(self._sender, "aclose", None)
        if close is not None:
            await close()

    async def _access_token(self, client_secret: str) -> str:
        if self._server.resolved_auth_strategy is McpAuthStrategy.NONE:
            return ""
        if (
            self._server.resolved_auth_strategy
            is McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
        ):
            if not client_secret:
                raise CredentialAccessError("MCP workload credential is unavailable")
            return client_secret
        await self._discover_oauth()
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

    async def _discover_oauth(self) -> None:
        if self._discovery_complete:
            return
        async with self._token_lock:
            if self._discovery_complete:
                return
            oauth = self._server.oauth
            assert oauth is not None
            protected = await self._send_pinned(
                "GET",
                oauth.protected_resource_metadata_url,
                headers={"Accept": "application/json"},
                content=b"",
            )
            protected_payload = _oauth_metadata(
                protected,
                self._max_response_bytes,
                "protected Resource",
            )
            if protected_payload.get("resource") != oauth.resource:
                raise CredentialAccessError(
                    "MCP protected Resource metadata does not match resource"
                )
            authorization_servers = protected_payload.get(
                "authorization_servers", []
            )
            if (
                not isinstance(authorization_servers, list)
                or oauth.issuer not in authorization_servers
            ):
                raise CredentialAccessError(
                    "MCP protected Resource metadata does not trust issuer"
                )
            authorization = await self._send_pinned(
                "GET",
                oauth.authorization_server_metadata_url,
                headers={"Accept": "application/json"},
                content=b"",
            )
            authorization_payload = _oauth_metadata(
                authorization,
                self._max_response_bytes,
                "authorization server",
            )
            if authorization_payload.get("issuer") != oauth.issuer:
                raise CredentialAccessError("MCP OAuth issuer metadata does not match")
            if authorization_payload.get("token_endpoint") != oauth.token_endpoint:
                raise CredentialAccessError(
                    "MCP OAuth token endpoint metadata does not match"
                )
            grants = authorization_payload.get(
                "grant_types_supported",
                ["client_credentials"],
            )
            if not isinstance(grants, list) or "client_credentials" not in grants:
                raise CredentialAccessError(
                    "MCP OAuth server does not support client credentials"
                )
            self._discovery_complete = True

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
        addresses = await self._resolver.resolve(
            host, parsed.port or _default_port(parsed.scheme)
        )
        if self._server.network_mode is None:
            approved = _legacy_approved_addresses(
                addresses,
                hostname=host,
                allowed_private_hosts=self._server.allowed_private_hosts,
                allow_global=parsed.scheme == "https",
            )
        else:
            approved = _approved_addresses(
                addresses,
                hostname=host,
                network_mode=self._server.resolved_network_mode,
                allowed_private_hosts=self._server.allowed_private_hosts,
                allowed_cidrs=self._server.allowed_cidrs,
                scheme=parsed.scheme,
            )
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


def _legacy_approved_addresses(
    addresses: tuple[str, ...],
    *,
    hostname: str,
    allowed_private_hosts: tuple[str, ...],
    allow_global: bool,
) -> tuple[str, ...]:
    allow_private = hostname.lower() in {item.lower() for item in allowed_private_hosts}
    approved: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise CredentialAccessError("MCP DNS returned an invalid address") from exc
        if address.is_global and allow_global:
            approved.append(address.compressed)
            continue
        if address.is_global:
            raise CredentialAccessError("public MCP egress requires HTTPS")
        if allow_private and (address.is_private or address.is_loopback):
            approved.append(address.compressed)
            continue
        raise CredentialAccessError("MCP DNS resolved to a non-public address")
    return tuple(sorted(set(approved)))


def _approved_addresses(
    addresses: tuple[str, ...],
    *,
    hostname: str,
    network_mode: McpNetworkMode,
    allowed_private_hosts: tuple[str, ...],
    allowed_cidrs: tuple[str, ...] = (),
    scheme: str,
) -> tuple[str, ...]:
    if not addresses:
        raise CredentialAccessError("MCP DNS returned no addresses")
    allowlisted = hostname.lower() in {item.lower() for item in allowed_private_hosts}
    networks = tuple(ipaddress.ip_network(item, strict=False) for item in allowed_cidrs)
    approved: list[str] = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise CredentialAccessError("MCP DNS returned an invalid address") from exc
        if address.is_link_local or address.is_multicast or address.is_unspecified:
            raise CredentialAccessError("MCP DNS resolved to a forbidden address")
        if address.is_reserved and not address.is_loopback:
            raise CredentialAccessError("MCP DNS resolved to a forbidden address")
        if network_mode is McpNetworkMode.PUBLIC:
            if scheme != "https":
                raise CredentialAccessError("public MCP egress requires HTTPS")
            if not address.is_global:
                raise CredentialAccessError("MCP DNS resolved to a non-public address")
            approved.append(address.compressed)
            continue
        if network_mode is McpNetworkMode.LOOPBACK:
            if not address.is_loopback:
                raise CredentialAccessError(
                    "loopback MCP DNS resolved outside loopback "
                    "(addresses are relative to the Credential Proxy network namespace)"
                )
            approved.append(address.compressed)
            continue
        if not allowlisted:
            raise CredentialAccessError("private MCP host is not allowlisted")
        if not networks:
            raise CredentialAccessError("private MCP egress requires allowed_cidrs")
        if not any(address in network for network in networks):
            raise CredentialAccessError("private MCP address is outside allowed_cidrs")
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
            events = [
                json.loads(line.removeprefix(b"data:").strip())
                for line in response.content.splitlines()
                if line.startswith(b"data:")
            ]
            notifications = [
                event
                for event in events
                if isinstance(event, dict)
                and isinstance(event.get("method"), str)
                and str(event["method"]).startswith("notifications/")
            ]
            responses = [
                event
                for event in events
                if isinstance(event, dict) and "id" in event
            ]
            if len(responses) != 1:
                raise CredentialAccessError(
                    "MCP event stream must contain exactly one response"
                )
            payload = dict(responses[0])
            if notifications:
                payload["_auraclaw_notifications"] = notifications
        elif content_type in {"application/json", ""}:
            payload = json.loads(response.content)
        else:
            raise CredentialAccessError("MCP response Content-Type is not supported")
    except json.JSONDecodeError as exc:
        raise CredentialAccessError("MCP response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise CredentialAccessError("MCP response must contain a JSON object")
    return payload


def _oauth_metadata(
    response: McpEgressResponse,
    max_response_bytes: int,
    kind: str,
) -> dict[str, Any]:
    if not 200 <= response.status_code < 300:
        raise CredentialAccessError(f"MCP OAuth {kind} discovery failed")
    if len(response.content) > max_response_bytes:
        raise CredentialAccessError(f"MCP OAuth {kind} metadata exceeds limit")
    if response.headers.get("content-type", "").split(";", 1)[0] not in {
        "",
        "application/json",
    }:
        raise CredentialAccessError(f"MCP OAuth {kind} metadata is not JSON")
    try:
        payload = json.loads(response.content)
    except json.JSONDecodeError as exc:
        raise CredentialAccessError(
            f"MCP OAuth {kind} metadata is invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise CredentialAccessError(f"MCP OAuth {kind} metadata is not an object")
    return dict(payload)


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


def _request_target_name(method: str, params: dict[str, Any]) -> str | None:
    key = {
        "tools/call": "name",
        "prompts/get": "name",
        "resources/read": "uri",
    }.get(method)
    value = params.get(key) if key is not None else None
    return str(value) if value is not None else None


def _default_port(scheme: str) -> int:
    return 80 if scheme == "http" else 443


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
