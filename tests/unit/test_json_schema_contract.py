from __future__ import annotations

import pytest

from auraclaw.action.json_schema import JsonSchemaValidator
from auraclaw.contracts.errors import InvalidToolSchemaError, SchemaValidationError

SCHEMA = {
    "type": "object",
    "$defs": {"nullable": {"type": ["string", "null"]}},
    "properties": {
        "input": {
            "type": "object",
            "properties": {
                "name": {"$ref": "#/$defs/nullable", "default": "must-not-inject"},
                "items": {
                    "type": "array",
                    "items": {"oneOf": [{"type": "integer"}, {"type": "boolean"}]},
                    "minItems": 1,
                },
                "count": {"allOf": [{"type": "number"}, {"minimum": 1}, {"maximum": 10}]},
                "extra": {"type": "object", "additionalProperties": {"type": "integer"}},
            },
            "required": ["items"],
            "additionalProperties": False,
        }
    },
    "required": ["input"],
}


def test_union_refs_composition_and_no_default_injection() -> None:
    value = {"input": {"name": None, "items": [2, True], "count": 3, "extra": {"a": 1}}}
    JsonSchemaValidator.validate(value, SCHEMA)
    minimal = {"input": {"items": [1]}}
    JsonSchemaValidator.validate(minimal, SCHEMA)
    assert minimal == {"input": {"items": [1]}}
    JsonSchemaValidator.validate({}, {"type": "object"})
    JsonSchemaValidator.validate("annotated", {"type": "string", "format": "email"})


@pytest.mark.parametrize(
    "value, keyword",
    [
        ({"input": {"items": []}}, "minItems"),
        ({"input": {"items": [None]}}, "oneOf"),
        ({"input": {"items": [1], "count": 0}}, "minimum"),
        ({"input": {"items": [1], "extra": {"a": "secret-value"}}}, "type"),
        ({"input": {"items": [1], "unexpected": 2}}, "additionalProperties"),
    ],
)
def test_constraints_report_paths_without_echoing_values(value, keyword) -> None:
    with pytest.raises(SchemaValidationError) as caught:
        JsonSchemaValidator.validate(value, SCHEMA)
    assert caught.value.validation_errors[0]["keyword"] == keyword
    assert caught.value.validation_errors[0]["instance_path"].startswith("/input")
    assert "secret-value" not in str(caught.value.validation_errors)


@pytest.mark.parametrize(
    "schema",
    [
        {"$ref": "https://untrusted.example/schema"},
        {"type": "nonsense"},
        {"$schema": "https://untrusted.example/dialect"},
        {"$ref": "#"},
    ],
)
def test_invalid_or_nonterminating_schema_fails_locally(schema) -> None:
    with pytest.raises(InvalidToolSchemaError):
        JsonSchemaValidator.validate({}, schema)


def test_boolean_subschemas_and_draft7_are_supported() -> None:
    JsonSchemaValidator.validate(
        {"permitted": 1},
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"permitted": True, "forbidden": False},
        },
    )
    with pytest.raises(SchemaValidationError):
        JsonSchemaValidator.validate({"forbidden": 1}, {"properties": {"forbidden": False}})


@pytest.mark.parametrize("arguments", ["", " ", "{", "[]", '"{\\"input\\":{}}"'])
def test_provider_never_replaces_invalid_or_nonobject_arguments_with_empty_input(arguments) -> None:
    from auraclaw.contracts.errors import ModelProviderError
    from auraclaw.infrastructure.model.openai_compatible import OpenAICompatibleProvider
    from auraclaw.runtime.ports import ModelRequest

    request = ModelRequest(
        model_call_id="schema-test", tenant_id="tenant", run_id="run", messages=()
    )
    with pytest.raises(ModelProviderError):
        OpenAICompatibleProvider._tool_calls(
            request, {0: {"id": "tool", "name": "query", "arguments": arguments}}
        )


def test_pattern_constraints_use_bounded_matching() -> None:
    schema = {
        "type": "object",
        "patternProperties": {"^code_": {"type": "integer"}},
        "additionalProperties": False,
    }
    JsonSchemaValidator.validate({"code_1": 1}, schema)
    with pytest.raises(SchemaValidationError):
        JsonSchemaValidator.validate({"other": 1}, schema)
    with pytest.raises(InvalidToolSchemaError, match="time limit"):
        JsonSchemaValidator.validate("a" * 10000 + "!", {"type": "string", "pattern": "(a+)+$"})
