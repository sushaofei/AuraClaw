from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from auraclaw.contracts.collaboration import PUBLISHABLE_CHILD_RESULT_FIELDS
from auraclaw.contracts.errors import AuthorizationError, CollaborationValidationError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import CollaborationClient, ToolCall

COLLABORATION_PREFIX = "auraclaw.collaboration."
GET_GRAPH = f"{COLLABORATION_PREFIX}get_graph"
CREATE_CHILD = f"{COLLABORATION_PREFIX}create_child"
SET_DEPENDENCIES = f"{COLLABORATION_PREFIX}set_dependencies"
REQUEST_REVIEW = f"{COLLABORATION_PREFIX}request_review"
CANCEL_CHILD = f"{COLLABORATION_PREFIX}cancel_child"
AWAIT_CHILDREN = f"{COLLABORATION_PREFIX}await_children"
JOIN = f"{COLLABORATION_PREFIX}join"
PUBLISH_RESULT = f"{COLLABORATION_PREFIX}publish_result"
PUBLISH_REVIEW = f"{COLLABORATION_PREFIX}publish_review"


@dataclass(frozen=True)
class CollaborationExecution:
    result: dict[str, Any]
    terminal: bool = False
    waiting_child_ids: tuple[str, ...] = ()


class RuntimeCollaborationController:
    """Role-scoped model tools backed by the fenced CollaborationClient."""

    def __init__(self, client: CollaborationClient) -> None:
        self._client = client

    @staticmethod
    def owns(name: str) -> bool:
        return name.startswith(COLLABORATION_PREFIX)

    @staticmethod
    def is_terminal(name: str) -> bool:
        return name in {JOIN, PUBLISH_RESULT, PUBLISH_REVIEW}

    def model_tools(self, assignment: RuntimeAssignment) -> tuple[dict[str, Any], ...]:
        if assignment.role in {"root", "coordinator"}:
            return self._coordinator_tools(
                tuple(
                    str(item)
                    for item in assignment.resource_profile.get("tool_permissions", ())
                )
            )
        if assignment.role in {"worker", "repair"}:
            return (self._publish_result_tool(assignment),)
        if assignment.role == "reviewer":
            return (self._publish_review_tool(),)
        return ()

    async def trusted_messages(
        self, assignment: RuntimeAssignment
    ) -> tuple[dict[str, Any], ...]:
        if assignment.role in {"root", "coordinator"}:
            graph = await self._client.execute(
                assignment,
                operation="get_graph",
                arguments={},
                command_id=f"runtime:collaboration:graph:{assignment.run_id}",
            )
            return (
                {
                    "role": "system",
                    "content": (
                        "You are the Coordinator for this Root Session. Use collaboration "
                        "tools only when decomposition adds value. Never invent Session ids, "
                        "actors, tenants, owners, or result lineage. If children are active, "
                        "await them; after required results and accepted reviews exist, call "
                        "join. Current authoritative collaboration graph:\n"
                        + json.dumps(graph, ensure_ascii=False, sort_keys=True)
                    ),
                },
            )
        if assignment.role in {"worker", "repair"}:
            return (
                {
                    "role": "system",
                    "content": (
                        "You are a Worker. Complete this Child goal with governed capabilities, "
                        "then call auraclaw.collaboration.publish_result. A normal text answer "
                        "without publishing the Output Contract is not success."
                    ),
                },
            )
        if assignment.role == "reviewer":
            return (
                {
                    "role": "system",
                    "content": (
                        "You are an independent Reviewer. Inspect the target evidence without "
                        "overwriting Worker artifacts, then call "
                        "auraclaw.collaboration.publish_review."
                    ),
                },
            )
        return ()

    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> CollaborationExecution:
        operation = call.name.removeprefix(COLLABORATION_PREFIX)
        if call.name == AWAIT_CHILDREN:
            waiting = self._strings(call.arguments, "child_session_ids")
            if not waiting:
                raise CollaborationValidationError(
                    "await_children requires child_session_ids"
                )
            graph = await self._client.execute(
                assignment,
                operation="get_graph",
                arguments={},
                command_id=f"runtime:collaboration:await:{call.tool_invocation_id}",
            )
            known = {
                str(child["session_id"])
                for child in graph.get("children", ())
                if isinstance(child, dict) and child.get("session_id")
            }
            if not set(waiting).issubset(known):
                raise CollaborationValidationError(
                    "await_children contains a Child outside the Root graph"
                )
            return CollaborationExecution(
                result={"status": "waiting", "child_session_ids": list(waiting)},
                waiting_child_ids=waiting,
            )
        self._authorize_tool(assignment.role, call.name)
        arguments = dict(call.arguments)
        if call.name == CREATE_CHILD:
            try:
                self._validate_child_request(assignment, arguments)
            except (AuthorizationError, CollaborationValidationError) as exc:
                return CollaborationExecution(
                    result={
                        "status": "denied",
                        "error_code": exc.code,
                        "summary": exc.message,
                    }
                )
        if call.name == PUBLISH_RESULT:
            try:
                self._validate_published_result(assignment, arguments)
            except CollaborationValidationError as exc:
                return CollaborationExecution(
                    result={
                        "status": "denied",
                        "error_code": exc.code,
                        "summary": exc.message,
                    }
                )
        result = await self._client.execute(
            assignment,
            operation=operation,
            arguments=arguments,
            command_id=f"runtime:collaboration:{call.tool_invocation_id}",
        )
        return CollaborationExecution(
            result=result,
            terminal=self.is_terminal(call.name),
        )

    async def active_child_ids(
        self, assignment: RuntimeAssignment
    ) -> tuple[str, ...]:
        _, active = await self.child_state(assignment)
        return active

    async def child_state(
        self, assignment: RuntimeAssignment
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if assignment.role not in {"root", "coordinator"}:
            return (), ()
        graph = await self._client.execute(
            assignment,
            operation="get_graph",
            arguments={},
            command_id=f"runtime:collaboration:active:{assignment.run_id}",
        )
        children = graph.get("children", ())
        all_children = tuple(
            str(child["session_id"])
            for child in children
            if isinstance(child, dict) and child.get("session_id")
        )
        active = tuple(
            str(child["session_id"])
            for child in children
            if isinstance(child, dict)
            and child.get("status") not in {"completed", "failed", "cancelled"}
        )
        return all_children, active

    @staticmethod
    def _authorize_tool(role: str, name: str) -> None:
        coordinator = {
            GET_GRAPH,
            CREATE_CHILD,
            SET_DEPENDENCIES,
            REQUEST_REVIEW,
            CANCEL_CHILD,
            JOIN,
        }
        if role in {"root", "coordinator"} and name in coordinator:
            return
        if role in {"worker", "repair"} and name == PUBLISH_RESULT:
            return
        if role == "reviewer" and name == PUBLISH_REVIEW:
            return
        raise AuthorizationError(f"role={role} cannot call {name}")

    @staticmethod
    def _validate_child_request(
        assignment: RuntimeAssignment, arguments: dict[str, Any]
    ) -> None:
        spec = arguments.get("spec")
        if not isinstance(spec, dict):
            raise CollaborationValidationError("create_child spec must be an object")
        requested = {str(item) for item in spec.get("tool_permissions", ())}
        allowed = {
            str(item)
            for item in assignment.resource_profile.get("tool_permissions", ())
        }
        if not requested.issubset(allowed):
            raise AuthorizationError("Child tool permissions exceed the Root grant")
        output_contract = spec.get("output_contract")
        if not isinstance(output_contract, dict):
            raise CollaborationValidationError(
                "create_child output_contract must be an object"
            )
        required = output_contract.get("required_fields", ())
        if not isinstance(required, (list, tuple)):
            raise CollaborationValidationError(
                "output_contract required_fields must be an array"
            )
        unsupported = sorted(
            {str(item) for item in required} - PUBLISHABLE_CHILD_RESULT_FIELDS
        )
        if unsupported:
            raise CollaborationValidationError(
                "unsupported Child Result fields: " + ", ".join(unsupported)
            )
        requires_artifacts = bool(output_contract.get("require_artifacts")) or (
            "artifact_refs" in {str(item) for item in required}
        )
        if requires_artifacts and not RuntimeCollaborationController._has_artifact_write(
            requested
        ):
            raise CollaborationValidationError(
                "artifact output contract requires a governed Artifact write permission"
            )

    @staticmethod
    def _has_artifact_write(permissions: set[str]) -> bool:
        write_markers = ("write", "create", "put", "upload", "persist")
        return any(
            ("artifact" in permission.lower() or "resource" in permission.lower())
            and any(marker in permission.lower() for marker in write_markers)
            for permission in permissions
        )

    @staticmethod
    def _result_ref(assignment: RuntimeAssignment) -> str:
        return (
            f"result://{assignment.tenant_id}/{assignment.session_id}/"
            f"{assignment.run_id}"
        )

    @classmethod
    def _validate_published_result(
        cls, assignment: RuntimeAssignment, arguments: dict[str, Any]
    ) -> None:
        expected = cls._result_ref(assignment)
        if arguments.get("result_ref") != expected:
            raise CollaborationValidationError(
                "result_ref must identify the current persisted Child Result"
            )
        artifact_refs = arguments.get("artifact_refs", ())
        if artifact_refs and not cls._has_artifact_write(
            {
                str(item)
                for item in assignment.resource_profile.get("tool_permissions", ())
            }
        ):
            raise CollaborationValidationError(
                "artifact_refs require a governed Artifact write permission"
            )

    @staticmethod
    def _strings(value: dict[str, Any], key: str) -> tuple[str, ...]:
        selected = value.get(key, ())
        if not isinstance(selected, (list, tuple)):
            raise CollaborationValidationError(f"{key} must be an array")
        return tuple(str(item) for item in selected)

    @staticmethod
    def _tool(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": parameters,
            },
        }

    @classmethod
    def _coordinator_tools(
        cls, child_tool_permissions: tuple[str, ...]
    ) -> tuple[dict[str, Any], ...]:
        object_schema = {"type": "object", "additionalProperties": False}
        permission_schema: dict[str, Any] = {
            "type": "array",
            "description": (
                "Optional exact tool permission grants for the Child. Omit this field when "
                "the Root has no matching grant; never use generic labels such as read-only."
            ),
            "items": {"type": "string", "enum": sorted(set(child_tool_permissions))},
            "uniqueItems": True,
            "maxItems": len(set(child_tool_permissions)),
        }
        child_spec_schema = {
            "type": "object",
            "properties": {
                "task_key": {
                    "type": "string",
                    "description": "Stable idempotency key within this Root Session.",
                },
                "role": {
                    "type": "string",
                    "enum": ["worker", "reviewer", "repair"],
                },
                "goal": {"type": "string"},
                "input_refs": {"type": "array", "items": {"type": "string"}},
                "output_contract": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "string"},
                        "required_fields": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": sorted(PUBLISHABLE_CHILD_RESULT_FIELDS),
                            },
                            "uniqueItems": True,
                        },
                        "require_artifacts": {"type": "boolean"},
                        "require_evidence": {"type": "boolean"},
                    },
                    "required": ["required_fields"],
                    "additionalProperties": False,
                },
                "dependency_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                **(
                    {"tool_permissions": permission_schema}
                    if child_tool_permissions
                    else {}
                ),
                "budget": {"type": "number", "exclusiveMinimum": 0},
                "runtime_budget": {
                    "type": "object",
                    "properties": {
                        "max_steps": {"type": "integer", "minimum": 1},
                        "max_output_tokens": {"type": "integer", "minimum": 1},
                        "deadline_seconds": {"type": "number", "exclusiveMinimum": 0},
                    },
                    "additionalProperties": False,
                },
            },
            "required": ["task_key", "role", "goal", "output_contract"],
            "additionalProperties": False,
        }
        return (
            cls._tool(GET_GRAPH, "Read the authoritative Child DAG and results.", object_schema),
            cls._tool(
                CREATE_CHILD,
                "Create one idempotent Child with a stable task_key and Output Contract.",
                {
                    "type": "object",
                    "properties": {
                        "parent_session_id": {"type": "string"},
                        "spec": child_spec_schema,
                    },
                    "required": ["spec"],
                    "additionalProperties": False,
                },
            ),
            cls._tool(
                SET_DEPENDENCIES,
                "Replace one Child's dependencies; the service rejects cycles.",
                {
                    "type": "object",
                    "properties": {
                        "child_session_id": {"type": "string"},
                        "dependency_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["child_session_id", "dependency_ids"],
                    "additionalProperties": False,
                },
            ),
            cls._tool(
                REQUEST_REVIEW,
                "Create an independent Reviewer Child for a completed Worker target.",
                {
                    "type": "object",
                    "properties": {
                        "task_key": {"type": "string"},
                        "target_session_id": {"type": "string"},
                        "goal": {"type": "string"},
                        "budget": {"type": "number", "exclusiveMinimum": 0},
                        "runtime_budget": {"type": "object"},
                    },
                    "required": ["task_key", "target_session_id", "goal"],
                    "additionalProperties": False,
                },
            ),
            cls._tool(
                CANCEL_CHILD,
                "Cancel one non-terminal Child.",
                {
                    "type": "object",
                    "properties": {
                        "child_session_id": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                    "required": ["child_session_id", "reason"],
                    "additionalProperties": False,
                },
            ),
            cls._tool(
                AWAIT_CHILDREN,
                "Suspend this Coordinator Run until all listed children are terminal.",
                {
                    "type": "object",
                    "properties": {
                        "child_session_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                        }
                    },
                    "required": ["child_session_ids"],
                    "additionalProperties": False,
                },
            ),
            cls._tool(
                JOIN,
                "Validate completed children and publish the terminal Root result with lineage.",
                {
                    "type": "object",
                    "properties": {
                        "child_session_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                        },
                        "result_summary": {"type": "string"},
                    },
                    "required": ["child_session_ids", "result_summary"],
                    "additionalProperties": False,
                },
            ),
        )

    @classmethod
    def _publish_result_tool(cls, assignment: RuntimeAssignment) -> dict[str, Any]:
        return cls._tool(
            PUBLISH_RESULT,
            "Publish this Worker's contract result and terminate the Child Run.",
            {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "result_ref": {
                        "type": "string",
                        "enum": [cls._result_ref(assignment)],
                        "description": (
                            "Canonical reference for the persisted Child Result; use this "
                            "exact value and never invent an Artifact reference."
                        ),
                    },
                    "artifact_refs": {"type": "array", "items": {"type": "string"}},
                    "evidence_refs": {"type": "array", "items": {"type": "string"}},
                    "limitations": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["summary", "result_ref"],
                "additionalProperties": False,
            },
        )

    @classmethod
    def _publish_review_tool(cls) -> dict[str, Any]:
        return cls._tool(
            PUBLISH_REVIEW,
            "Publish an evidence-backed review decision and terminate the Review Run.",
            {
                "type": "object",
                "properties": {
                    "decision": {
                        "type": "string",
                        "enum": ["accepted", "changes_requested", "rejected"],
                    },
                    "evidence_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "findings": {"type": "array", "items": {"type": "string"}},
                    "repair_suggestions": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["decision", "evidence_refs"],
                "additionalProperties": False,
            },
        )
