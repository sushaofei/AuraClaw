from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import httpx

from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.hands import (
    HANDS_CONTRACT_VERSION,
    HANDS_INVOCATIONS_CANCEL,
    HANDS_MAX_REQUEST_BYTES,
    HANDS_MAX_RESPONSE_BYTES,
    HANDS_PROMPTS_GET,
    HANDS_PROMPTS_LIST,
    HANDS_RESOURCE_TEMPLATES_LIST,
    HANDS_RESOURCES_LIST,
    HANDS_RESOURCES_READ,
    HANDS_TOOLS_CALL,
    HANDS_TOOLS_LIST,
    HandsCancelRequest,
    HandsCancelResponse,
    HandsGetPromptRequest,
    HandsListRequest,
    HandsPage,
    HandsPromptDescriptor,
    HandsPromptResult,
    HandsReadResourceRequest,
    HandsReadResourceResponse,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsToolCall,
    HandsToolDescriptor,
    HandsToolResult,
)
from auraclaw.contracts.internal import INTERNAL_API_VERSION, InternalError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.internal.http import raise_contract_error


class HttpHandsClient:
    """Runtime Hands client over the protocol-agnostic internal HTTP contract."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        bearer_tokens: Mapping[str, str],
        max_request_bytes: int = HANDS_MAX_REQUEST_BYTES,
        max_response_bytes: int = HANDS_MAX_RESPONSE_BYTES,
    ) -> None:
        self._client = client
        self._bearer_tokens = dict(bearer_tokens)
        self._max_request_bytes = max_request_bytes
        self._max_response_bytes = max_response_bytes

    def _headers(self, assignment: RuntimeAssignment) -> dict[str, str]:
        token = self._bearer_tokens.get(assignment.runtime_id)
        if token is None:
            raise RuntimeError("no workload token configured for Runtime")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "X-AuraClaw-Contract-Version": INTERNAL_API_VERSION,
            "X-AuraClaw-Hands-Contract": HANDS_CONTRACT_VERSION,
        }
        if assignment.lease_assertion is not None:
            headers["X-AuraClaw-Lease-Assertion"] = (
                assignment.lease_assertion.model_dump_json()
            )
        return headers

    async def list_tools(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsToolDescriptor]:
        payload = await self._post(
            HANDS_TOOLS_LIST,
            assignment,
            HandsListRequest(cursor=cursor).model_dump(mode="json"),
        )
        return HandsPage[HandsToolDescriptor].model_validate(payload)

    async def list_resources(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        payload = await self._post(
            HANDS_RESOURCES_LIST,
            assignment,
            HandsListRequest(cursor=cursor).model_dump(mode="json"),
        )
        return HandsPage[HandsResourceDescriptor].model_validate(payload)

    async def list_resource_templates(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        payload = await self._post(
            HANDS_RESOURCE_TEMPLATES_LIST,
            assignment,
            HandsListRequest(cursor=cursor).model_dump(mode="json"),
        )
        return HandsPage[HandsResourceDescriptor].model_validate(payload)

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        payload = await self._post(
            HANDS_RESOURCES_READ,
            assignment,
            HandsReadResourceRequest(uri=uri).model_dump(mode="json"),
        )
        return HandsReadResourceResponse.model_validate(payload).contents

    async def list_prompts(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsPromptDescriptor]:
        payload = await self._post(
            HANDS_PROMPTS_LIST,
            assignment,
            HandsListRequest(cursor=cursor).model_dump(mode="json"),
        )
        return HandsPage[HandsPromptDescriptor].model_validate(payload)

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        payload = await self._post(
            HANDS_PROMPTS_GET,
            assignment,
            HandsGetPromptRequest(name=name, arguments=dict(arguments or {})).model_dump(
                mode="json"
            ),
        )
        return HandsPromptResult.model_validate(payload)

    async def call_tool(
        self,
        assignment: RuntimeAssignment,
        call: HandsToolCall,
    ) -> HandsToolResult:
        payload = await self._post(
            HANDS_TOOLS_CALL,
            assignment,
            call.model_dump(mode="json"),
        )
        return HandsToolResult.model_validate(payload)

    async def cancel_invocation(
        self,
        assignment: RuntimeAssignment,
        tool_invocation_id: str,
    ) -> bool:
        payload = await self._post(
            HANDS_INVOCATIONS_CANCEL,
            assignment,
            HandsCancelRequest(tool_invocation_id=tool_invocation_id).model_dump(
                mode="json"
            ),
        )
        return HandsCancelResponse.model_validate(payload).cancelled

    async def _post(
        self,
        path: str,
        assignment: RuntimeAssignment,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        if len(encoded) > self._max_request_bytes:
            raise AuraClawError("Hands request exceeds the configured size limit")
        response = await self._client.post(
            path,
            content=encoded,
            headers={
                **self._headers(assignment),
                "Content-Type": "application/json",
            },
        )
        if len(response.content) > self._max_response_bytes:
            raise AuraClawError("Hands response exceeds the configured size limit")
        if response.is_error:
            try:
                InternalError.model_validate(response.json())
            except Exception:
                raise AuraClawError(
                    f"Hands contract call failed with HTTP {response.status_code}",
                    detail=response.text[:500] or None,
                ) from None
            raise_contract_error(response)
        parsed = response.json()
        if not isinstance(parsed, dict):
            raise AuraClawError("Hands response must be a JSON object")
        return parsed
