"""Bounded, offline JSON Schema validation shared by tool execution and admin tests."""

from __future__ import annotations

import json
from contextvars import ContextVar
from functools import lru_cache
from itertools import islice
from typing import Any

import regex  # type: ignore[import-untyped]
from jsonschema import (  # type: ignore[import-untyped]
    Draft7Validator,
    Draft201909Validator,
    Draft202012Validator,
)
from jsonschema.exceptions import SchemaError, ValidationError  # type: ignore[import-untyped]
from jsonschema.validators import extend  # type: ignore[import-untyped]
from referencing import Registry
from referencing.exceptions import Unresolvable

from auraclaw.contracts.errors import InvalidToolSchemaError, SchemaValidationError

_WORK: ContextVar[list[int] | None] = ContextVar("schema_work", default=None)
_DIALECTS = {
    "https://json-schema.org/draft/2020-12/schema": Draft202012Validator,
    "https://json-schema.org/draft/2019-09/schema": Draft201909Validator,
    "http://json-schema.org/draft-07/schema#": Draft7Validator,
    "http://json-schema.org/draft-07/schema": Draft7Validator,
}


def _bounded_keyword(original: Any) -> Any:
    def validate(validator: Any, constraint: Any, instance: Any, schema: Any) -> Any:
        work = _WORK.get()
        if work is not None:
            work[0] += 1
            if work[0] > 10000:
                raise InvalidToolSchemaError("JSON Schema validation work limit exceeded")
        yield from original(validator, constraint, instance, schema)

    return validate


def _search(pattern: str, value: str) -> bool:
    work = _WORK.get()
    if work is not None:
        work[0] += 1
        if work[0] > 10000:
            raise InvalidToolSchemaError("JSON Schema validation work limit exceeded")
    if len(pattern) > 4096:
        raise InvalidToolSchemaError("JSON Schema pattern exceeds size limit")
    try:
        return regex.search(pattern, value, timeout=0.02) is not None
    except TimeoutError as exc:
        raise InvalidToolSchemaError("JSON Schema pattern exceeds time limit") from exc


def _pattern(validator: Any, constraint: Any, instance: Any, schema: Any) -> Any:
    if validator.is_type(instance, "string") and not _search(constraint, instance):
        yield ValidationError("string does not match the declared pattern")


def _pattern_properties(validator: Any, constraint: Any, instance: Any, schema: Any) -> Any:
    if not validator.is_type(instance, "object"):
        return
    for pattern, subschema in constraint.items():
        for key, value in instance.items():
            if _search(pattern, key):
                yield from validator.descend(value, subschema, path=key, schema_path=pattern)


def _additional_properties(validator: Any, constraint: Any, instance: Any, schema: Any) -> Any:
    if not validator.is_type(instance, "object"):
        return
    for key, value in instance.items():
        if key in schema.get("properties", {}) or any(
            _search(pattern, key) for pattern in schema.get("patternProperties", {})
        ):
            continue
        if constraint is False:
            yield ValidationError("object contains an unexpected property")
        elif isinstance(constraint, dict):
            yield from validator.descend(value, constraint, path=key)


def _check_bounds(value: Any, *, schema: bool) -> None:
    pending = [(value, 0)]
    count = 0
    has_patterns = False
    has_unevaluated = False
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 64 or count > (10000 if schema else 50000):
            raise InvalidToolSchemaError("JSON Schema or instance exceeds validation limits")
        if isinstance(item, dict):
            if schema:
                has_patterns |= "patternProperties" in item
                has_unevaluated |= "unevaluatedProperties" in item
                for keyword in ("$ref", "$dynamicRef", "$recursiveRef"):
                    ref = item.get(keyword)
                    if isinstance(ref, str) and not ref.startswith("#"):
                        raise InvalidToolSchemaError(
                            "external JSON Schema references are not allowed"
                        )
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    if has_patterns and has_unevaluated:
        raise InvalidToolSchemaError(
            "patternProperties combined with unevaluatedProperties "
            "is not supported within bounded validation"
        )


@lru_cache(maxsize=256)
def _validator(encoded: str) -> Any:
    schema = json.loads(encoded)
    _check_bounds(schema, schema=True)
    dialect = schema.get("$schema") if isinstance(schema, dict) else None
    implementation = Draft202012Validator if dialect is None else _DIALECTS.get(dialect)
    if implementation is None:
        raise InvalidToolSchemaError("unsupported JSON Schema dialect")
    try:
        implementation.check_schema(schema)
    except SchemaError as exc:
        raise InvalidToolSchemaError("tool declares an invalid JSON Schema") from exc
    bounded = extend(
        implementation,
        {
            name: _bounded_keyword(handler)
            for name, handler in {
                **implementation.VALIDATORS,
                "pattern": _pattern,
                "patternProperties": _pattern_properties,
                "additionalProperties": _additional_properties,
            }.items()
        },
    )
    return bounded(schema, registry=Registry())


def _pointer(path: Any) -> str:
    return (
        "/" + "/".join(str(part).replace("~", "~0").replace("/", "~1") for part in path)
        if path
        else ""
    )


def _detail(error: Any) -> dict[str, Any]:
    location = _pointer(error.absolute_path)
    keyword = str(error.validator)
    message = f"{location or '$'} violates {keyword}"
    if keyword == "type":
        message = f"{location or '$'} must be {error.validator_value}"
    elif keyword == "required" and isinstance(error.instance, dict):
        missing = [key for key in error.validator_value if key not in error.instance]
        message = f"{location or '$'} is missing required fields: {missing}"
    return {
        "instance_path": location,
        "schema_path": _pointer(error.absolute_schema_path),
        "keyword": keyword,
        "message": message[:512],
    }


class JsonSchemaValidator:
    @staticmethod
    def check_schema(schema: dict[str, Any] | bool) -> None:
        encoded = json.dumps(schema, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > 256 * 1024:
            raise InvalidToolSchemaError("JSON Schema exceeds size limit")
        _validator(encoded)

    @staticmethod
    def validate(value: Any, schema: dict[str, Any] | bool, *, path: str = "$") -> None:
        del path
        JsonSchemaValidator.check_schema(schema)
        _check_bounds(value, schema=False)
        validator = _validator(
            json.dumps(schema, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
        token = _WORK.set([0])
        try:
            errors = list(islice(validator.iter_errors(value), 8))
        except (Unresolvable, RecursionError) as exc:
            raise InvalidToolSchemaError(
                "JSON Schema reference cannot be resolved within limits"
            ) from exc
        finally:
            _WORK.reset(token)
        if errors:
            errors.sort(key=lambda error: (error.validator != "required", len(error.absolute_path)))
            details = [_detail(error) for error in errors]
            raise SchemaValidationError(details[0]["message"], validation_errors=details)
