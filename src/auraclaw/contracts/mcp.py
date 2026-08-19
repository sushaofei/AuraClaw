from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import Field, model_validator

from auraclaw.contracts.internal import ContractModel, LeaseAssertion

MCP_PROTOCOL_VERSION = "2026-07-28"
MCP_LEGACY_PROTOCOL_VERSION = "2025-11-25"
MCP_SUPPORTED_PROTOCOL_VERSIONS = (
    MCP_PROTOCOL_VERSION,
    MCP_LEGACY_PROTOCOL_VERSION,
)
MCP_JSONRPC_VERSION = "2.0"
MCP_PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"
MCP_CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"
MCP_CLIENT_CAPABILITIES_META_KEY = "io.modelcontextprotocol/clientCapabilities"
MCP_SERVER_INFO_META_KEY = "io.modelcontextprotocol/serverInfo"


class McpTrustedContext(ContractModel):
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int = Field(ge=1)
    deadline: datetime | None = None
    lease_assertion: LeaseAssertion | None = None


class McpAnnotations(ContractModel):
    audience: tuple[Literal["user", "assistant"], ...] = ()
    priority: float | None = Field(default=None, ge=0.0, le=1.0)
    last_modified: datetime | None = None

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.audience:
            result["audience"] = list(self.audience)
        if self.priority is not None:
            result["priority"] = self.priority
        if self.last_modified is not None:
            result["lastModified"] = self.last_modified.isoformat()
        return result


class McpResourceDescriptor(ContractModel):
    uri: str = Field(min_length=1)
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    size: int | None = Field(default=None, ge=0)
    annotations: McpAnnotations | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {"uri": self.uri, "name": self.name}
        _set_optional(result, "title", self.title)
        _set_optional(result, "description", self.description)
        _set_optional(result, "mimeType", self.mime_type)
        _set_optional(result, "size", self.size)
        if self.annotations is not None:
            result["annotations"] = self.annotations.as_mcp()
        if self.meta:
            result["_meta"] = dict(self.meta)
        return result


class McpResourceTemplateDescriptor(ContractModel):
    uri_template: str = Field(min_length=1)
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    annotations: McpAnnotations | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "uriTemplate": self.uri_template,
            "name": self.name,
        }
        _set_optional(result, "title", self.title)
        _set_optional(result, "description", self.description)
        _set_optional(result, "mimeType", self.mime_type)
        if self.annotations is not None:
            result["annotations"] = self.annotations.as_mcp()
        if self.meta:
            result["_meta"] = dict(self.meta)
        return result


class McpResourceContent(ContractModel):
    uri: str = Field(min_length=1)
    mime_type: str | None = None
    text: str | None = None
    blob: str | None = None
    annotations: McpAnnotations | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_content(self) -> McpResourceContent:
        if (self.text is None) == (self.blob is None):
            raise ValueError("resource content must define exactly one of text or blob")
        return self

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {"uri": self.uri}
        _set_optional(result, "mimeType", self.mime_type)
        _set_optional(result, "text", self.text)
        _set_optional(result, "blob", self.blob)
        if self.annotations is not None:
            result["annotations"] = self.annotations.as_mcp()
        if self.meta:
            result["_meta"] = dict(self.meta)
        return result


class McpPromptArgument(ContractModel):
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    required: bool = False

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {"name": self.name, "required": self.required}
        _set_optional(result, "title", self.title)
        _set_optional(result, "description", self.description)
        return result


class McpPromptDescriptor(ContractModel):
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    arguments: tuple[McpPromptArgument, ...] = ()
    meta: dict[str, Any] = Field(default_factory=dict)

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "arguments": [argument.as_mcp() for argument in self.arguments],
        }
        _set_optional(result, "title", self.title)
        _set_optional(result, "description", self.description)
        if self.meta:
            result["_meta"] = dict(self.meta)
        return result


class McpPromptMessage(ContractModel):
    role: Literal["user", "assistant"]
    content: dict[str, Any]

    def as_mcp(self) -> dict[str, Any]:
        return {"role": self.role, "content": dict(self.content)}


class McpPromptResult(ContractModel):
    description: str | None = None
    messages: tuple[McpPromptMessage, ...]
    meta: dict[str, Any] = Field(default_factory=dict)

    def as_mcp(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "messages": [message.as_mcp() for message in self.messages]
        }
        _set_optional(result, "description", self.description)
        if self.meta:
            result["_meta"] = dict(self.meta)
        return result


class McpJsonRpcRequest(ContractModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None = None
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class McpJsonRpcError(ContractModel):
    code: int
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class McpJsonRpcResponse(ContractModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None = None
    result: dict[str, Any] | None = None
    error: McpJsonRpcError | None = None


class McpTransport(Protocol):
    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse: ...


def _set_optional(target: dict[str, Any], key: str, value: Any | None) -> None:
    if value is not None:
        target[key] = value
