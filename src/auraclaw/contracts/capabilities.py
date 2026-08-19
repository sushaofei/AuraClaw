from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

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
    endpoint: str = Field(min_length=1, pattern=r"^https://")
    protocol_revision: str = "2026-07-28"
    credential_ref: str | None = None
    oauth: McpOAuthConfiguration | None = None
    trust_level: CapabilityTrustLevel = CapabilityTrustLevel.EXTERNAL_UNTRUSTED
    allowed_tool_prefixes: tuple[str, ...] = ()
    allowed_resource_schemes: tuple[str, ...] = ()
    allowed_prompt_prefixes: tuple[str, ...] = ()
    status: CapabilityStatus = CapabilityStatus.QUARANTINED
    enabled: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_remote_auth(self) -> McpServerDefinition:
        if self.protocol_revision not in {"2026-07-28", "2025-11-25"}:
            raise ValueError("MCP server protocol revision is not supported")
        if self.oauth is not None and self.credential_ref is None:
            raise ValueError("OAuth MCP server requires a credential_ref")
        if "_auraclaw_oauth" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
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
