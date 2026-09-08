"""Typed JSON access for canonical inputs (AAS-DATA-009).

``json.loads`` returns ``object``, and silently coercing that into a typed field
is how malformed evidence becomes a clean-looking canonical value. These
helpers narrow each value explicitly and raise typed schema-drift errors, so a
bad document fails at the boundary with a precise field name.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from aegis_alpha.data.canonical_records import CanonicalBuildError, CanonicalErrorCode


def _drift(name: str, expected: str, value: object) -> CanonicalBuildError:
    return CanonicalBuildError(
        CanonicalErrorCode.SCHEMA_DRIFT,
        f"{name} must be {expected}, received {type(value).__name__}",
    )


def as_mapping(name: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise _drift(name, "an object", value)
    if any(not isinstance(key, str) for key in value):
        raise _drift(name, "an object with string keys", value)
    return {str(key): item for key, item in value.items()}


def as_sequence(name: str, value: object) -> Sequence[object]:
    if not isinstance(value, list):
        raise _drift(name, "an array", value)
    return list(value)


def as_str(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise _drift(name, "a string", value)
    return value


def as_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise _drift(name, "a boolean", value)
    return value


def as_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _drift(name, "an integer", value)
    return value


def as_float(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _drift(name, "a number", value)
    number = float(value)
    if not math.isfinite(number):
        raise CanonicalBuildError(
            CanonicalErrorCode.SCHEMA_DRIFT,
            f"{name} must be a finite number, received {number}",
        )
    return number


def as_optional_str(name: str, value: object) -> str | None:
    return None if value is None else as_str(name, value)


def field(document: Mapping[str, object], name: str) -> object:
    if name not in document:
        raise CanonicalBuildError(
            CanonicalErrorCode.SCHEMA_DRIFT,
            f"required field is missing: {name}",
        )
    return document[name]


def str_tuple(name: str, value: object) -> tuple[str, ...]:
    return tuple(as_str(f"{name} item", item) for item in as_sequence(name, value))
