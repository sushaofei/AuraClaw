from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from typing import Any
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    McpAuthStrategy,
    McpNetworkMode,
    McpOAuthConfiguration,
    McpServerDefinition,
)
from auraclaw.contracts.internal import ContractModel


class McpDesiredState(StrEnum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    RETIRED = "retired"


class McpObservedState(StrEnum):
    PENDING = "pending"
    LOADING = "loading"
    ACTIVE = "active"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"
    DISABLED = "disabled"
    UNAVAILABLE = "unavailable"


class McpRegistryOperationKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    TEST = "test"
    ENABLE = "enable"
    DISABLE = "disable"
    RECONCILE = "reconcile"
    RETIRE = "retire"
    DELETE = "delete"


class McpRegistryOperationStatus(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RECONCILING = "reconciling"
    UNKNOWN_SIDE_EFFECT = "unknown_side_effect"


class McpServerConfig(ContractModel):
    """Immutable administrator-authored MCP server configuration."""

    server_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    tenant_id: str | None = None
    title: str = Field(min_length=1, max_length=256)
    endpoint: str = Field(min_length=1, pattern=r"^https?://")
    network_mode: McpNetworkMode = McpNetworkMode.PUBLIC
    protocol_revision: str = "2026-07-28"
    auth_strategy: McpAuthStrategy = McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
    credential_ref: str | None = None
    oauth: McpOAuthConfiguration | None = None
    allowed_resource_schemes: tuple[str, ...] = ()
    allowed_prompt_prefixes: tuple[str, ...] = ()
    allowed_cidrs: tuple[str, ...] = ()
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_config(self) -> McpServerConfig:
        if self.protocol_revision not in {
            "2026-07-28",
            "2025-11-25",
            "2025-06-18",
        }:
            raise ValueError("MCP server protocol revision is not supported")
        validate_mcp_network_endpoint(
            self.endpoint,
            network_mode=self.network_mode,
            allowed_cidrs=self.allowed_cidrs,
        )
        if self.auth_strategy is McpAuthStrategy.NONE:
            if self.network_mode is McpNetworkMode.PUBLIC:
                raise ValueError("auth_strategy none is not allowed for public MCP servers")
            if self.credential_ref is not None or self.oauth is not None:
                raise ValueError("auth_strategy none must not carry credentials")
        elif self.auth_strategy is McpAuthStrategy.OAUTH_CLIENT_CREDENTIALS:
            if self.oauth is None or self.credential_ref is None:
                raise ValueError(
                    "OAuth MCP server requires oauth configuration and a credential_ref"
                )
        elif self.credential_ref is None:
            raise ValueError(
                "workload trusted-context MCP server requires a credential_ref"
            )
        if "_auraclaw_oauth" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        if "_auraclaw_allowed_private_hosts" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        if "_auraclaw_network_mode" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        if "_auraclaw_allowed_cidrs" in self.metadata:
            raise ValueError("MCP server metadata uses a reserved key")
        return self

    def config_digest(self) -> str:
        payload = self.model_dump_json()
        return sha256(payload.encode()).hexdigest()

    def materialize(
        self,
        *,
        revision: int,
        desired_state: McpDesiredState,
        observed_state: McpObservedState,
    ) -> McpServerDefinition:
        hostname = (urlsplit(self.endpoint).hostname or "").lower()
        private_hosts = (
            (hostname,)
            if self.network_mode in {McpNetworkMode.PRIVATE, McpNetworkMode.LOOPBACK}
            else ()
        )
        enabled = desired_state is McpDesiredState.ENABLED
        status = _observed_to_capability_status(observed_state, desired_state)
        credential_ref = self.credential_ref
        if self.auth_strategy is McpAuthStrategy.NONE:
            credential_ref = none_credential_ref(self.server_id)
        return McpServerDefinition(
            server_id=self.server_id,
            tenant_id=self.tenant_id,
            title=self.title,
            endpoint=self.endpoint,
            protocol_revision=self.protocol_revision,
            credential_ref=credential_ref,
            oauth=self.oauth,
            auth_strategy=self.auth_strategy,
            allowed_resource_schemes=self.allowed_resource_schemes,
            allowed_prompt_prefixes=self.allowed_prompt_prefixes,
            allowed_private_hosts=private_hosts,
            network_mode=self.network_mode,
            allowed_cidrs=self.allowed_cidrs,
            config_revision=revision,
            status=status,
            enabled=enabled,
            metadata=dict(self.metadata),
        )


class McpServerRevisionRecord(ContractModel):
    server_id: str
    revision: int = Field(ge=1)
    config: McpServerConfig
    config_digest: str
    created_by: str
    created_at: datetime


class McpServerRuntimeRecord(ContractModel):
    server_id: str
    instance_id: str = Field(default="legacy", min_length=1, max_length=256)
    loaded_revision: int | None = None
    observed_state: McpObservedState = McpObservedState.PENDING
    last_test_at: datetime | None = None
    last_sync_at: datetime | None = None
    consecutive_failures: int = 0
    safe_error_code: str | None = None
    updated_at: datetime


class McpServerRecord(ContractModel):
    server_id: str
    tenant_id: str | None
    desired_state: McpDesiredState
    latest_revision: int = Field(ge=0)
    active_revision: int | None = None
    created_by: str
    created_at: datetime
    updated_at: datetime
    latest_config: McpServerConfig | None = None
    active_config: McpServerConfig | None = None
    runtime: McpServerRuntimeRecord | None = None
    runtimes: tuple[McpServerRuntimeRecord, ...] = ()


class McpServerOperationRecord(ContractModel):
    operation_id: str
    server_id: str
    tenant_id: str | None
    target_revision: int | None = None
    command_id: str
    actor_id: str
    correlation_id: str
    causation_id: str
    operation: McpRegistryOperationKind
    status: McpRegistryOperationStatus
    safe_error_code: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    completed_at: datetime | None = None


class McpActiveSnapshotEntry(ContractModel):
    server_id: str
    tenant_id: str | None
    revision: int
    config: McpServerConfig
    desired_state: McpDesiredState
    observed_state: McpObservedState


class McpServerWriteCommand(ContractModel):
    command_id: str = Field(min_length=1, max_length=256)
    tenant_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)
    config: McpServerConfig


class McpServerLifecycleCommand(ContractModel):
    command_id: str = Field(min_length=1, max_length=256)
    tenant_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)
    target_revision: int | None = None


def none_credential_ref(server_id: str) -> str:
    return f"mcp:none:{server_id}"


def is_none_credential_ref(credential_ref: str | None) -> bool:
    return credential_ref is not None and credential_ref.startswith("mcp:none:")


def validate_mcp_network_endpoint(
    endpoint: str,
    *,
    network_mode: McpNetworkMode,
    allowed_cidrs: tuple[str, ...] = (),
) -> None:
    parsed = urlsplit(endpoint)
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("MCP endpoint must be an absolute URL without userinfo")
    if network_mode is McpNetworkMode.PUBLIC:
        if parsed.scheme != "https":
            raise ValueError("public MCP endpoints require HTTPS")
        if allowed_cidrs:
            raise ValueError("public MCP endpoints do not use allowed_cidrs")
        return
    if network_mode is McpNetworkMode.LOOPBACK:
        if hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError(
                "loopback MCP endpoints must use localhost, 127.0.0.1, or ::1 "
                "(relative to the Credential Proxy network namespace)"
            )
        if allowed_cidrs:
            raise ValueError("loopback MCP endpoints do not use allowed_cidrs")
        return
    if not allowed_cidrs:
        raise ValueError("private MCP endpoints require allowed_cidrs")


def _observed_to_capability_status(
    observed: McpObservedState,
    desired: McpDesiredState,
) -> CapabilityStatus:
    if desired is McpDesiredState.RETIRED:
        return CapabilityStatus.RETIRED
    if observed is McpObservedState.ACTIVE:
        return CapabilityStatus.ACTIVE
    if observed is McpObservedState.DEGRADED:
        return CapabilityStatus.DEGRADED
    if observed is McpObservedState.QUARANTINED:
        return CapabilityStatus.QUARANTINED
    if desired is McpDesiredState.DISABLED or observed is McpObservedState.DISABLED:
        return CapabilityStatus.QUARANTINED
    return CapabilityStatus.QUARANTINED
