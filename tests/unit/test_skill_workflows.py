from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.skills import (
    ResolvedSkillResource,
    ResolvedSkillTool,
    ResolvedSkillWorkflow,
    SkillActivation,
    SkillBinding,
    SkillManifest,
    SkillReferenceRequirement,
    SkillResourceRequirement,
    SkillToolRequirement,
    SkillWorkflowEntrypoint,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.domain.skill_workflows import compile_skill_workflow
from auraclaw.runtime.capability_controller import (
    CAPABILITY_LOAD,
    SKILL_ACTIVATE,
    RuntimeCapabilityController,
)
from auraclaw.runtime.ports import ToolCall
from auraclaw.runtime.skill_workflow import RuntimeSkillWorkflowExecutor, WorkflowStepProgress

_KEY = b"workflow-test-signing-key"


class _Artifacts:
    async def put(self, **kwargs: object) -> ArtifactRef:
        del kwargs
        return _artifact()


def _artifact() -> ArtifactRef:
    return ArtifactRef(
        artifact_id="art-workflow",
        version=1,
        content_hash="2" * 64,
        media_type="application/vnd.auraclaw.skill-package+json",
        size=100,
    )


def _workflow() -> dict[str, Any]:
    return {
        "apiVersion": "skills.auraclaw.io/v1alpha1",
        "kind": "Workflow",
        "references": [
            {"id": "mapping", "path": "references/mapping.json", "required": True}
        ],
        "steps": [
            {
                "id": "lookup",
                "operation": "tool.call",
                "capability": "inventory.lookup",
                "arguments": {
                    "sku": {"from": "$input.sku"},
                    "region": {"from": "$references.mapping.region"},
                },
                "result": "item",
                "timeout_seconds": 5,
            },
            {
                "id": "policy",
                "operation": "resource.read",
                "capability": "policy://{region}",
                "arguments": {"region": {"from": "$references.mapping.region"}},
                "result": "policy",
                "timeout_seconds": 5,
            },
        ],
        "outputs": {"item": {"from": "$state.item"}},
    }


def _package(*, workflow: dict[str, Any] | None = None) -> SkillPackage:
    document = workflow or _workflow()
    unsigned = SkillManifest(
        name="inventory.check",
        version="1.0.0",
        description="Check inventory through governed capabilities",
        input_schema={
            "type": "object",
            "properties": {"sku": {"type": "string"}},
            "required": ["sku"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"item": {"type": "object"}},
            "required": ["item"],
            "additionalProperties": False,
        },
        required_tools=(SkillToolRequirement(name="inventory.lookup", version="1.0.0"),),
        required_resources=(SkillResourceRequirement(uri_template="policy://{region}"),),
        workflow=SkillWorkflowEntrypoint(entrypoint="scripts/main.workflow.json"),
        required_references=(
            SkillReferenceRequirement(
                path="references/mapping.json",
                media_type="application/json",
                preload=True,
            ),
        ),
        max_steps=4,
        timeout_seconds=60,
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {
        "SKILL.md": b"# Inventory\nUse the governed workflow.",
        "references/mapping.json": b'{"region":"cn-east"}',
        "scripts/main.workflow.json": json.dumps(document).encode(),
    }
    verifier = HmacSkillSignatureVerifier({"platform": _KEY})
    signature = verifier.sign(unsigned, files)
    manifest = unsigned.model_copy(update={"signature": signature})
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="root-1",
        session_id="session-1",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
    )


def _binding(package: SkillPackage) -> SkillBinding:
    compiled = compile_skill_workflow(package.manifest, package.files)
    assert compiled is not None
    return SkillBinding(
        skill_name=package.manifest.name,
        skill_version=package.manifest.version,
        publisher=package.manifest.publisher,
        package_digest=f"sha256:{'1' * 64}",
        artifact_ref=_artifact(),
        resolved_tools=(
            ResolvedSkillTool(
                capability_id="cap-tool",
                canonical_name="inventory.lookup",
                version="1.0.0",
                schema_digest=f"sha256:{'3' * 64}",
                expected_side_effect="read",
            ),
        ),
        resolved_resources=(
            ResolvedSkillResource(
                capability_id="cap-resource",
                server_id="policy",
                uri_template="policy://{region}",
                content_digest=f"sha256:{'4' * 64}",
            ),
        ),
        resolved_workflow=ResolvedSkillWorkflow(
            api_version="skills.auraclaw.io/v1alpha1",
            entrypoint=compiled.entrypoint,
            workflow_digest=compiled.digest,
            reference_paths=compiled.reference_paths,
        ),
        policy_version="policy-v1",
        max_steps=4,
        timeout_seconds=60,
    )


def test_workflow_package_is_validated_and_executable_files_remain_denied() -> None:
    package = _package()
    registry = SkillPackageRegistry(
        artifacts=_Artifacts(),  # type: ignore[arg-type]
        signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
    )
    assert registry.validate(package).manifest.workflow is not None

    invalid = SkillPackage(
        manifest=package.manifest,
        files={**package.files, "scripts/escape.py": b"print('no')"},
    )
    with pytest.raises(SchemaValidationError, match="Workflow JSON"):
        registry.validate_content(invalid)


def test_workflow_rejects_undeclared_capabilities_and_forward_state() -> None:
    undeclared = _workflow()
    undeclared["steps"][0]["capability"] = "inventory.delete"
    with pytest.raises(SchemaValidationError, match="not declared"):
        compile_skill_workflow(_package().manifest, _package(workflow=undeclared).files)

    forward = _workflow()
    forward["steps"][0]["arguments"]["sku"] = {"from": "$state.item.sku"}
    with pytest.raises(SchemaValidationError, match="unavailable state"):
        compile_skill_workflow(_package().manifest, _package(workflow=forward).files)


class _Client:
    def __init__(self, package: SkillPackage, binding: SkillBinding) -> None:
        self.package = package
        self.binding = binding
        self.calls: list[ToolCall] = []
        self.resource_uris: list[str] = []

    async def load_skill_part(
        self, assignment: RuntimeAssignment, **kwargs: Any
    ) -> list[dict[str, Any]]:
        del assignment
        path = str(kwargs["path"])
        return [{"text": self.package.files[path].decode()}]

    async def execute(self, assignment: RuntimeAssignment, call: ToolCall) -> dict[str, Any]:
        del assignment
        self.calls.append(call)
        if call.name == CAPABILITY_LOAD:
            return {
                "content": {
                    "capabilities": [
                        {
                            "capability_id": "cap-tool",
                            "kind": "tool",
                            "canonical_name": "inventory.lookup",
                            "version": "1.0.0",
                            "permission": "read-only",
                            "model_tool": {"type": "function", "function": {}},
                        },
                        {
                            "capability_id": "cap-resource",
                            "kind": "resource_template",
                            "canonical_name": "policy://{region}",
                            "version": "1.0.0",
                            "resource": {"uri_template": "policy://{region}"},
                        },
                    ]
                }
            }
        return {"status": "success", "content": {"sku": call.arguments["sku"]}}

    async def read_resource(self, assignment: RuntimeAssignment, uri: str) -> list[dict[str, Any]]:
        del assignment
        self.resource_uris.append(uri)
        return [{"text": "policy"}]

    async def resolve_skill(self, assignment: RuntimeAssignment, **kwargs: Any) -> SkillBinding:
        del assignment, kwargs
        return self.binding


def test_workflow_executor_uses_stable_invocation_and_pinned_version() -> None:
    async def scenario() -> None:
        package = _package()
        binding = _binding(package)
        client = _Client(package, binding)
        executor = RuntimeSkillWorkflowExecutor(client)  # type: ignore[arg-type]
        activation = SkillActivation(
            skill_activation_id="ska_inventory",
            activation_key="activate-1",
            binding=binding,
            input_digest=f"sha256:{'5' * 64}",
        )
        loaded = {
            "cap-resource": {
                "kind": "resource_template",
                "resource": {"uri_template": "policy://{region}"},
            }
        }
        first = await executor.execute(
            _assignment(), activation, inputs={"sku": "A-1"}, loaded_capabilities=loaded
        )
        second = await executor.execute(
            _assignment(), activation, inputs={"sku": "A-1"}, loaded_capabilities=loaded
        )
        assert first.status == second.status == "completed"
        tool_calls = [call for call in client.calls if call.name == "inventory.lookup"]
        assert len(tool_calls) == 2
        assert tool_calls[0].tool_invocation_id == tool_calls[1].tool_invocation_id
        assert tool_calls[0].idempotency_key == tool_calls[0].tool_invocation_id
        assert tool_calls[0].version == "1.0.0"
        assert tool_calls[0].expected_side_effect == "read"
        assert client.resource_uris == ["policy://cn-east", "policy://cn-east"]

    asyncio.run(scenario())


def test_workflow_executor_resumes_from_step_checkpoint() -> None:
    class _ProcessDeath(BaseException):
        pass

    async def scenario() -> None:
        package = _package()
        binding = _binding(package)
        client = _Client(package, binding)
        executor = RuntimeSkillWorkflowExecutor(client)  # type: ignore[arg-type]
        activation = SkillActivation(
            skill_activation_id="ska_resume",
            activation_key="activate-resume",
            binding=binding,
            input_digest=f"sha256:{'6' * 64}",
        )
        loaded = {
            "cap-resource": {
                "kind": "resource_template",
                "resource": {"uri_template": "policy://{region}"},
            }
        }
        checkpoint: WorkflowStepProgress | None = None

        async def crash_after_first_step(progress: WorkflowStepProgress) -> None:
            nonlocal checkpoint
            checkpoint = progress
            raise _ProcessDeath()

        with pytest.raises(_ProcessDeath):
            await executor.execute(
                _assignment(),
                activation,
                inputs={"sku": "A-1"},
                loaded_capabilities=loaded,
                on_progress=crash_after_first_step,
            )
        assert checkpoint is not None
        result = await executor.execute(
            _assignment(),
            activation,
            inputs={"sku": "A-1"},
            loaded_capabilities=loaded,
            resume_state=checkpoint.state,
            start_step_index=checkpoint.next_step_index,
        )
        assert result.status == "completed"
        assert [call.name for call in client.calls] == ["inventory.lookup"]
        assert client.resource_uris == ["policy://cn-east"]

    asyncio.run(scenario())


def test_capability_controller_executes_workflow_on_skill_activation() -> None:
    async def scenario() -> None:
        package = _package()
        binding = _binding(package)
        client = _Client(package, binding)
        controller = RuntimeCapabilityController(client)  # type: ignore[arg-type]
        state = controller.empty_state()
        state["loaded"] = {
            "cap-skill": {
                "capability_id": "cap-skill",
                "kind": "skill",
                "skill": {
                    "publisher": "platform",
                    "name": "inventory.check",
                    "version": "1.0.0",
                    "input_schema": package.manifest.input_schema,
                    "output_schema": package.manifest.output_schema,
                    "required_references": [
                        item.model_dump(mode="json")
                        for item in package.manifest.required_references
                    ],
                },
            }
        }
        execution = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="activate-1",
                name=SKILL_ACTIVATE,
                arguments={"capability_id": "cap-skill", "inputs": {"sku": "A-1"}},
            ),
            state,
        )
        assert execution.result["status"] == "completed"
        assert [event.type for event in execution.events] == [
            "skill.activated",
            "skill.completed",
        ]
        assert execution.state["active_skills"][0]["workflow_status"] == "completed"
        assert execution.state["active_skills"][0]["model_reference_paths"] == [
            "references/mapping.json"
        ]

    asyncio.run(scenario())


class _ApprovalClient(_Client):
    async def execute(self, assignment: RuntimeAssignment, call: ToolCall) -> dict[str, Any]:
        if call.name in {CAPABILITY_LOAD}:
            return await super().execute(assignment, call)
        self.calls.append(call)
        if call.approval_id is None:
            return {
                "status": "denied",
                "error_code": "approval_required",
                "metadata": {
                    "approval_request": {
                        "approval_id": "approval-1",
                        "tool_name": call.name,
                    }
                },
            }
        return {"status": "success", "content": {"sku": call.arguments["sku"]}}


def test_workflow_approval_resume_reuses_nested_invocation_id() -> None:
    async def scenario() -> None:
        package = _package()
        binding = _binding(package)
        client = _ApprovalClient(package, binding)
        controller = RuntimeCapabilityController(client)  # type: ignore[arg-type]
        state = controller.empty_state()
        state["loaded"] = {
            "cap-skill": {
                "capability_id": "cap-skill",
                "kind": "skill",
                "skill": {
                    "publisher": "platform",
                    "name": "inventory.check",
                    "version": "1.0.0",
                    "input_schema": package.manifest.input_schema,
                    "output_schema": package.manifest.output_schema,
                    "required_references": [],
                },
            }
        }
        original = ToolCall(
            tool_invocation_id="activate-approval",
            name=SKILL_ACTIVATE,
            arguments={"capability_id": "cap-skill", "inputs": {"sku": "A-1"}},
        )
        waiting = await controller.execute(_assignment(), original, state)
        assert waiting.result["error_code"] == "approval_required"
        assert waiting.result["metadata"]["approval_request"]["approval_id"] == (
            "approval-1"
        )
        resumed = await controller.execute(
            _assignment(),
            ToolCall(**{**original.__dict__, "approval_id": "approval-1"}),
            waiting.state,
        )
        assert resumed.result["status"] == "completed"
        workflow_calls = [call for call in client.calls if call.name == "inventory.lookup"]
        assert len(workflow_calls) == 2
        assert workflow_calls[0].tool_invocation_id == workflow_calls[1].tool_invocation_id
        assert workflow_calls[1].approval_id == "approval-1"

    asyncio.run(scenario())
