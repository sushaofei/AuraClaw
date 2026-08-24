from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from auraclaw.contracts.internal import ContractModel


class CapabilityKind(StrEnum):
    RESOURCE = "resource"
    RESOURCE_TEMPLATE = "resource_template"
    TOOL = "tool"
    PROMPT = "prompt"
    SKILL = "skill"


class CapabilityTrustLevel(StrEnum):
    PLATFORM = "platform"
    TENANT_VERIFIED = "tenant_verified"
    EXTERNAL_UNTRUSTED = "external_untrusted"


class CapabilityStatus(StrEnum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"
    RETIRED = "retired"


class McpAuthStrategy(StrEnum):
    OAUTH_CLIENT_CREDENTIALS = "oauth_client_credentials"
    WORKLOAD_TRUSTED_CONTEXT = "workload_trusted_context"


class McpOAuthConfiguration(ContractModel):
    protected_resource_metadata_url: str = Field(pattern=r"^https://")
    authorization_server_metadata_url: str = Field(pattern=r"^https://")
    issuer: str = Field(pattern=r"^https://")
    token_endpoint: str = Field(pattern=r"^https://")
    client_id: str = Field(min_length=1, max_length=512)
    resource: str = Field(pattern=r"^https://")
    scopes: tuple[str, ...] = ()


class McpServerDefinition(ContractModel):
    server_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    tenant_id: str | None = None
    title: str = Field(min_length=1, max_length=256)
    endpoint: str = Field(min_length=1, pattern=r"^https?://")
    protocol_revision: str = "2026-07-28"
    credential_ref: str | None = None
    oauth: McpOAuthConfiguration | None = None
    auth_strategy: McpAuthStrategy | None = None
    trust_level: CapabilityTrustLevel = CapabilityTrustLevel.EXTERNAL_UNTRUSTED
    allowed_tool_prefixes: tuple[str, ...] = ()
    allowed_resource_schemes: tuple[str, ...] = ()
    allowed_prompt_prefixes: tuple[str, ...] = ()
    allowed_private_hosts: tuple[str, ...] = ()
    status: CapabilityStatus = CapabilityStatus.QUARANTINED
    enabled: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_remote_auth(self) -> McpServerDefinition:
        if self.protocol_revision not in {
            "2026-07-28",
            "2025-11-25",
            "2025-06-18",
        }:
            raise ValueError("MCP server protocol revision is not supported")
        _validate_mcp_endpoint(self.endpoint, self.allowed_private_hosts)
        if self.oauth is not None and self.credential_ref is None:
            raise ValueError("OAuth MCP server requires a credential_ref")
        if (
            self.auth_strategy == McpAuthStrategy.OAUTH_CLIENT_CREDENTIALS
            and (self.oauth is None or self.credential_ref is None)
        ):
            raise ValueError(
                "OAuth MCP server requires oauth configuration and a credential_ref"
            )
        if (
            self.auth_strategy == McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
            and self.credential_ref is None
        ):
            raise ValueError(
                "workload trusted-context MCP server requires a credential_ref"
            )
        if "_auraclaw_oauth" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        if "_auraclaw_allowed_private_hosts" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        return self

    @property
    def resolved_auth_strategy(self) -> McpAuthStrategy:
        if self.auth_strategy is not None:
            return self.auth_strategy
        if self.oauth is not None:
            return McpAuthStrategy.OAUTH_CLIENT_CREDENTIALS
        return McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT


class JavaApiArgumentBinding(ContractModel):
    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    location: Literal["path", "query", "body"]
    required: bool = False


class JavaApiOperationDefinition(ContractModel):
    operation_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    tool_name: str = Field(min_length=1, max_length=256)
    version: str = "1"
    description: str = ""
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    path_template: str = Field(min_length=1, max_length=512)
    argument_bindings: tuple[JavaApiArgumentBinding, ...] = ()
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    idempotent: bool = False
    read_only: bool = False
    permission: str = "read-only"
    risk_level: str = "low"
    credential_ref: str | None = None

    @model_validator(mode="after")
    def validate_path_template(self) -> JavaApiOperationDefinition:
        path = self.path_template
        if (
            not path.startswith("/")
            or "://" in path
            or ".." in path
            or "//" in path
            or any(character.isspace() for character in path)
        ):
            raise ValueError("Java API path template must be a relative absolute path")
        names = {binding.name for binding in self.argument_bindings}
        if len(names) != len(self.argument_bindings):
            raise ValueError("Java API argument bindings must be unique")
        return self


class JavaApiServerDefinition(ContractModel):
    server_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    tenant_id: str | None = None
    title: str = Field(min_length=1, max_length=256)
    base_url: str = Field(min_length=1, pattern=r"^https://")
    credential_ref: str | None = None
    trust_level: CapabilityTrustLevel = CapabilityTrustLevel.TENANT_VERIFIED
    operations: tuple[JavaApiOperationDefinition, ...] = ()
    allowed_private_hosts: tuple[str, ...] = ()
    status: CapabilityStatus = CapabilityStatus.QUARANTINED
    enabled: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_server(self) -> JavaApiServerDefinition:
        if self.credential_ref is None and self.enabled:
            raise ValueError("enabled Java API server requires a credential_ref")
        ids = [item.operation_id for item in self.operations]
        names = [item.tool_name for item in self.operations]
        if len(ids) != len(set(ids)) or len(names) != len(set(names)):
            raise ValueError("Java API operations must have unique ids and tool names")
        return self


class CapabilityDescriptor(ContractModel):
    capability_id: str = Field(min_length=1, max_length=256)
    kind: CapabilityKind
    server_id: str = Field(min_length=1, max_length=128)
    canonical_name: str = Field(min_length=1, max_length=256)
    version: str = Field(min_length=1, max_length=128)
    content_digest: str = Field(min_length=1, max_length=256)
    title: str = Field(min_length=1, max_length=256)
    description: str = ""
    tags: tuple[str, ...] = ()
    tenant_id: str | None = None
    trust_level: CapabilityTrustLevel = CapabilityTrustLevel.EXTERNAL_UNTRUSTED
    classification: str = "internal"
    permission: str | None = None
    risk_level: str | None = None
    required_scopes: tuple[str, ...] = ()
    status: CapabilityStatus = CapabilityStatus.QUARANTINED
    source_revision: str | None = None
    updated_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    def as_search_result(self) -> dict[str, Any]:
        return {
            "capability_id": self.capability_id,
            "kind": self.kind.value,
            "canonical_name": self.canonical_name,
            "version": self.version,
            "content_digest": self.content_digest,
            "title": self.title,
            "description": self.description,
            "tags": list(self.tags),
            "trust_level": self.trust_level.value,
            "classification": self.classification,
            "permission": self.permission,
            "risk_level": self.risk_level,
            "status": self.status.value,
            "server_id": self.server_id,
            "source_revision": self.source_revision,
        }


def _validate_mcp_endpoint(endpoint: str, allowed_private_hosts: tuple[str, ...]) -> None:
    parsed = urlsplit(endpoint)
    hostname = (parsed.hostname or "").lower()
    allowlisted = hostname in {item.lower() for item in allowed_private_hosts}
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("MCP endpoint must be an absolute URL without userinfo")
    if parsed.scheme == "http" and not allowlisted:
        raise ValueError("HTTP MCP endpoints require an allowlisted private host")
