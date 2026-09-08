"""Deterministic aas-rowset-v1 logical hashes for typed observation rows.

The digest covers the format tag, field schema, and every row. Rows are sorted
by their encoded payload so caller order does not change the hash; duplicate
rows remain. Values are tagged and length-prefixed so types and concatenations
cannot collide. Decimal keeps its exact coefficient and scale. IEEE-754 -0.0 is
stored as +0.0; NaN and infinities are rejected.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Final

ROWSET_FORMAT: Final = "aas-rowset-v1"
HASH_ALGORITHM: Final = "sha256"
_MAGIC: Final = b"aas-rowset-v1\0"
_FIELD_SPEC_LEN: Final = 2
_U32: Final = struct.Struct(">I")
_I32: Final = struct.Struct(">i")
_I64: Final = struct.Struct(">q")
_F64: Final = struct.Struct(">d")
_INT64_MIN: Final = -(1 << 63)
_INT64_MAX: Final = (1 << 63) - 1

_TAG_NULL: Final = 0x00
_TAG_TEXT: Final = 0x01
_TAG_INT: Final = 0x02
_TAG_DECIMAL: Final = 0x03
_TAG_UTC_US: Final = 0x04
_TAG_DATE: Final = 0x05
_TAG_FLOAT: Final = 0x06
_TAG_BOOL: Final = 0x07

_TAGS: Final = {
    "text": _TAG_TEXT,
    "int": _TAG_INT,
    "decimal": _TAG_DECIMAL,
    "utc_us": _TAG_UTC_US,
    "date": _TAG_DATE,
    "float": _TAG_FLOAT,
    "bool": _TAG_BOOL,
}


def rowset_hash(schema: tuple[tuple[str, str], ...], rows: list[dict[str, object]]) -> str:
    """Return the SHA-256 hex digest of an aas-rowset-v1 encoding."""
    return hashlib.sha256(_encode_rowset(schema, rows)).hexdigest()


def _encode_rowset(
    schema: tuple[tuple[str, str], ...], rows: Sequence[Mapping[str, object]]
) -> bytes:
    fields = _validated_schema(schema)
    encoded_rows = sorted(_encode_row(fields, row) for row in rows)
    return b"".join(
        (
            _MAGIC,
            _U32.pack(len(fields)),
            b"".join(_encode_field_spec(name, type_name) for name, type_name in fields),
            _U32.pack(len(encoded_rows)),
            *encoded_rows,
        )
    )


def _validated_schema(schema: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    if not isinstance(schema, tuple) or not schema:
        raise ValueError("rowset schema must be a nonempty tuple of field types")
    names: list[str] = []
    fields: list[tuple[str, str]] = []
    for spec in schema:
        if not isinstance(spec, tuple) or len(spec) != _FIELD_SPEC_LEN:
            raise ValueError("each schema field must be a (name, type) pair")
        name, type_name = spec
        if not isinstance(name, str) or not name or name != name.strip() or "\0" in name:
            raise ValueError("schema field names must be exact nonempty text")
        if type_name not in _TAGS:
            raise ValueError(f"unsupported rowset type {type_name!r}")
        names.append(name)
        fields.append((name, type_name))
    if len(set(names)) != len(names):
        raise ValueError("rowset schema field names must be unique")
    return tuple(fields)


def _encode_field_spec(name: str, type_name: str) -> bytes:
    encoded_name = name.encode("utf-8")
    encoded_type = type_name.encode("ascii")
    return b"".join(
        (
            bytes((_TAGS[type_name],)),
            _U32.pack(len(encoded_name)),
            encoded_name,
            _U32.pack(len(encoded_type)),
            encoded_type,
        )
    )


def _encode_row(fields: tuple[tuple[str, str], ...], row: Mapping[str, object]) -> bytes:
    if not isinstance(row, Mapping):
        raise TypeError("each rowset row must be an object")
    keys = set(row)
    expected = {name for name, _type_name in fields}
    if keys != expected:
        raise ValueError("row fields must match the rowset schema exactly")
    return b"".join(_encode_value(type_name, row[name]) for name, type_name in fields)


def _encode_text(value: object) -> bytes:
    if type(value) is not str:
        raise TypeError("text fields must be str or null")
    encoded = value.encode("utf-8")
    return bytes((_TAG_TEXT,)) + _U32.pack(len(encoded)) + encoded


def _encode_int(tag: int, value: object) -> bytes:
    if type(value) is not int:
        raise TypeError("integer fields must be int or null, not bool")
    if value < _INT64_MIN or value > _INT64_MAX:
        raise ValueError("integer fields must fit in signed 64-bit")
    return bytes((tag,)) + _I64.pack(value)


def _encode_plain_int(value: object) -> bytes:
    return _encode_int(_TAG_INT, value)


def _encode_utc_us(value: object) -> bytes:
    return _encode_int(_TAG_UTC_US, value)


def _encode_decimal(value: object) -> bytes:
    if type(value) is not Decimal:
        raise TypeError("decimal fields must be Decimal or null")
    if not value.is_finite():
        raise ValueError("decimal fields must be finite")
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise TypeError("decimal exponent must be an integer")
    if value.is_zero():
        sign = 0
    coefficient = "".join(str(digit) for digit in digits).encode("ascii")
    return b"".join(
        (
            bytes((_TAG_DECIMAL, sign)),
            _I32.pack(exponent),
            _U32.pack(len(coefficient)),
            coefficient,
        )
    )


def _encode_date(value: object) -> bytes:
    if type(value) is not date:
        raise TypeError("date fields must be datetime.date or null")
    encoded = value.isoformat().encode("ascii")
    return bytes((_TAG_DATE,)) + _U32.pack(len(encoded)) + encoded


def _encode_float(value: object) -> bytes:
    if type(value) is not float:
        raise TypeError("float fields must be float or null")
    if not math.isfinite(value):
        raise ValueError("float fields must be finite")
    canonical = 0.0 if value == 0.0 else value
    return bytes((_TAG_FLOAT,)) + _F64.pack(canonical)


def _encode_bool(value: object) -> bytes:
    if type(value) is not bool:
        raise TypeError("bool fields must be bool or null")
    return bytes((_TAG_BOOL, 1 if value else 0))


_VALUE_ENCODERS: Final[dict[str, Callable[[object], bytes]]] = {
    "text": _encode_text,
    "int": _encode_plain_int,
    "utc_us": _encode_utc_us,
    "decimal": _encode_decimal,
    "date": _encode_date,
    "float": _encode_float,
    "bool": _encode_bool,
}


def _encode_value(type_name: str, value: object) -> bytes:
    if value is None:
        return bytes((_TAG_NULL,))
    return _VALUE_ENCODERS[type_name](value)
