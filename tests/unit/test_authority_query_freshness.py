from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, cast

import pytest

from auraclaw.action.capability_catalog import capability_load_tool, skill_binding_status_tool
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.tools import ToolCapability, ToolInvocation, ToolPermission
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.persistence.postgres_invocation_store import PostgresInvocationStore
from auraclaw.internal.tool_client import GatewayToolClient
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.runtime.authority_queries import authority_request_id, binding_disposition_result
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.ports import CapabilityClient, ToolCall


class Authority:
    def __init__(self) -> None:
        self.action = "continue"
        self.calls: list[ToolInvocation] = []

    async def execute(self, invocation: ToolInvocation, capability: ToolCapability) -> object:
        del capability
        self.calls.append(invocation)
        return {"action": self.action}


def gateway(source: Authority, *, store: PostgresInvocationStore | None = None) -> ToolGateway:
    return ToolGateway(
        registry=ToolRegistry((skill_binding_status_tool(),)),
        policy=PolicyEngine(),
        hands=source,
        invocation_store=store,
        approvals=InMemoryApprovalProjection(),
        artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=b"authority-query-test-key"),
    )


def assignment(run_id: str = "run-a") -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a", root_session_id="root-a", session_id="session-a",
        run_id=run_id, runtime_id="runtime-a", lease_id="lease-a", fencing_token=1,
        role="worker", resource_profile={},
    )


def state() -> dict[str, Any]:
    return {"active_skills": [{"activation": {"skill_activation_id": "ska_a"}, "binding": {
        "publisher": "platform", "skill_name": "check", "skill_version": "1.0.0",
        "package_digest": "sha256:" + "1" * 64,
    }}]}


def test_runtime_checks_observe_revocation_and_recovery_through_gateway() -> None:
    async def scenario() -> None:
        source = Authority()
        client = GatewayToolClient(gateway(source))
        controller = RuntimeCapabilityController(cast(CapabilityClient, client))
        for action in ("continue", "pause", "cancel", "continue"):
            source.action = action
            observed = await controller.binding_disposition(assignment(), state())
            assert observed is not None and observed["action"] == action
        assert len({call.tool_invocation_id for call in source.calls}) == 4
    asyncio.run(scenario())


def test_replaying_legacy_query_id_never_reads_old_persistent_result() -> None:
    async def scenario() -> None:
        source = Authority()
        # An unreachable PostgreSQL backend proves neither begin/cached_result
        # nor other persistence operations are used by this authority query.
        store = PostgresInvocationStore("postgresql://unused:unused@127.0.0.1:1/unused")
        client = GatewayToolClient(gateway(source, store=store))
        call = ToolCall(
            tool_invocation_id="binding_status_legacy", name="auraclaw.skills.binding-status",
            arguments={"publisher": "platform", "name": "check", "version": "1.0.0",
                       "package_digest": "sha256:" + "1" * 64},
        )
        for run, action in (("run-a", "continue"), ("run-b", "cancel")):
            source.action = action
            result = await client.execute(assignment(run), call)
            assert result["content"] == {"action": action}
        # A different Hands instance must not restore the old continue either.
        fresh_client = GatewayToolClient(gateway(source, store=store))
        assert (await fresh_client.execute(assignment(), call))["content"] == {"action": "cancel"}
        assert len(source.calls) == 3
    asyncio.run(scenario())


def test_only_registered_read_only_queries_can_disable_replay() -> None:
    with pytest.raises(ValueError, match="read-only"):
        replace(skill_binding_status_tool(), permission=ToolPermission.WRITE_WITH_APPROVAL)
    assert not capability_load_tool().cache_result


@pytest.mark.parametrize("result", [
    {"status": "timeout", "content": {"action": "continue"}},
    {"status": "unknown", "content": {"action": "continue"}},
    {"status": "denied", "error_code": "policy_denied"},
    {"status": "success", "content": {"action": "invalid"}},
    {},
])
def test_authority_failure_pauses_instead_of_reusing_permission(result: dict[str, Any]) -> None:
    assert binding_disposition_result(result)["action"] == "pause"


def test_logical_query_ids_are_isolated_by_run_and_read() -> None:
    ids = {authority_request_id(assignment(run), "preload") for run in ("a", "a", "b")}
    assert len(ids) == 3
