"""The frozen hash formats a promotion writes, in Python and in DuckDB SQL.

Every format is canonical JSON (sorted keys, no whitespace, ASCII escapes, no NaN)
hashed with SHA-256, and each has a frozen digest in ``tests/storage/test_promotion_formats.py``.
A change of shape takes a new name (``-v2``); none of these is ever reinterpreted.

- ``source_row_hash``: ``["aas-source-row-v1", [[column, value], ...]]`` over one source
  row in source schema order, without the source library's ``_aas_ordinal``.
- ``revision_id``: ``["aas-revision-v1", dataset_id, record_id, op, supersedes, source_row_hash]``.
- TOMBSTONE ``source_row_hash``: ``["aas-tombstone-v1", source_id, table, digest]``.
- ``request_hash``: SHA-256 of the request document
  ``["aas-promotion-request-v1", spec_sha256, [table digest, ...], parent]``.
- ``dimensions_hash`` of a mapper that declares a fact's context:
  ``["aas-dimensions-v1", {name: text, ...}]``.

Source values keep the representation of ``source_library_digest.scalar`` (float as
``float_hex``, bytes as ``base64``) and add one tagged form for each temporal type, so
no value is ever rendered through a locale, a time zone or a float printer.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Final

SOURCE_ROW_FORMAT: Final = "aas-source-row-v1"
REVISION_FORMAT: Final = "aas-revision-v1"
TOMBSTONE_FORMAT: Final = "aas-tombstone-v1"
REQUEST_FORMAT: Final = "aas-promotion-request-v1"
DIMENSIONS_FORMAT: Final = "aas-dimensions-v1"
# Text that json.dumps writes verbatim between quotes: printable ASCII except '"' and '\\'.
JSON_PLAIN: Final = r"[\x{20}\x{21}\x{23}-\x{5b}\x{5d}-\x{7e}]*"
_PLAIN: Final = re.compile(r"[\x20\x21\x23-\x5b\x5d-\x7e]*")
_EPOCH: Final = datetime(1970, 1, 1)  # noqa: DTZ001 -- naive epoch for naive source timestamps
_MICROSECOND: Final = timedelta(microseconds=1)
_INTEGERS: Final = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
    }
)
SOURCE_ROW_TYPES: Final = frozenset(
    {
        *_INTEGERS,
        "BOOLEAN",
        "FLOAT",
        "DOUBLE",
        "VARCHAR",
        "BLOB",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
    }
)


def canonical(value: object) -> bytes:
    """Canonical JSON bytes: sorted keys, compact separators, ASCII escapes, finite numbers."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def source_value(value: object) -> object:
    """The JSON form one source cell takes inside ``aas-source-row-v1``."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return {"float_hex": value.hex()}
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return {"local_us": (value - _EPOCH) // _MICROSECOND}
        return {"utc_us": (value - datetime(1970, 1, 1, tzinfo=UTC)) // _MICROSECOND}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    raise TypeError(f"unsupported source value type {type(value).__name__}")


def source_row_hash(columns: Sequence[tuple[str, object]]) -> str:
    """``aas-source-row-v1`` of one source row, its columns in source schema order."""
    if any(name == "_aas_ordinal" for name, _ in columns):
        raise ValueError("the source library ordinal is not source content")
    return digest([SOURCE_ROW_FORMAT, [[name, source_value(value)] for name, value in columns]])


def revision_id(
    dataset_id: str, record_id: str, op: str, supersedes: str | None, row_hash: str
) -> str:
    """``aas-revision-v1``: unique per dataset, record, operation, predecessor and source row."""
    return digest([REVISION_FORMAT, dataset_id, record_id, op, supersedes, row_hash])


def tombstone_hash(source_id: str, table: str, table_digest: str) -> str:
    """The ``source_row_hash`` of a TOMBSTONE: the source table that proves the absence."""
    return digest([TOMBSTONE_FORMAT, source_id, table, table_digest])


def request_document(spec_sha256: str, source_digests: Sequence[str], parent: str | None) -> bytes:
    """The exact bytes whose SHA-256 is a promotion's ``request_hash``."""
    return canonical([REQUEST_FORMAT, spec_sha256, list(source_digests), parent])


def request_hash(spec_sha256: str, source_digests: Sequence[str], parent: str | None) -> str:
    return hashlib.sha256(request_document(spec_sha256, source_digests, parent)).hexdigest()


def dimensions_hash(dimensions: Mapping[str, str]) -> str:
    """``aas-dimensions-v1``: the context that tells apart facts of one concept and period."""
    if not dimensions or not all(isinstance(value, str) for value in dimensions.values()):
        raise ValueError("dimensions are a nonempty mapping of names to text")
    return digest([DIMENSIONS_FORMAT, dict(dimensions)])


def is_plain(text: str) -> bool:
    """Whether ``json.dumps`` writes ``text`` unchanged between its quotes."""
    return _PLAIN.fullmatch(text) is not None


def sql_literal(text: str) -> str:
    """A SQL string literal holding ``text`` exactly."""
    if "\x00" in text:
        raise ValueError("SQL literals cannot hold NUL")
    return "'" + text.replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("SQL identifier must be nonempty text without NUL")
    return '"' + name.replace('"', '""') + '"'


def float_hex_sql(expression: str) -> str:
    """Python's ``float.hex()`` of a DOUBLE expression, in SQL."""
    from aegis_alpha.storage.bulk_generation import float_bits_sql  # noqa: PLC0415 -- shared codec

    bits = float_bits_sql(f"abs({expression})")
    exponent = f"(({bits}) // 4503599627370496)"
    mantissa = f"lpad(lower(to_hex(({bits}) % 4503599627370496)), 13, '0')"
    unbiased = f"(CAST({exponent} AS BIGINT) - 1023)"
    sign = f"(CASE WHEN signbit({expression}) THEN '-' ELSE '' END)"
    finite = (
        f"CASE WHEN {expression} = 0 THEN {sign} || '0x0.0p+0'"
        f" WHEN {exponent} = 0 THEN {sign} || '0x0.' || {mantissa} || 'p-1022'"
        f" ELSE {sign} || '0x1.' || {mantissa} || 'p'"
        f" || CASE WHEN {unbiased} >= 0 THEN '+' ELSE '-' END"
        f" || CAST(abs({unbiased}) AS VARCHAR) END"
    )
    return (
        f"(CASE WHEN isnan({expression}) THEN 'nan'"
        f" WHEN isinf({expression}) THEN {sign} || 'inf' ELSE {finite} END)"
    )


def _value_sql(column: str, kind: str) -> tuple[str, str | None]:  # noqa: PLR0911 -- one JSON form per source type
    """(JSON text of one non-null cell, the column when it needs a plain-text check)."""
    if kind == "BOOLEAN":
        return f"CASE WHEN {column} THEN 'true' WHEN NOT {column} THEN 'false' END", None
    if kind in _INTEGERS:
        return f"CAST({column} AS VARCHAR)", None
    if kind in {"FLOAT", "DOUBLE"}:
        return '\'{"float_hex":"\' || ' + float_hex_sql(
            f"CAST({column} AS DOUBLE)"
        ) + " || '\"}'", None
    if kind == "VARCHAR":
        return f"'\"' || {column} || '\"'", column
    if kind == "BLOB":
        return '\'{"base64":"\' || to_base64(' + column + ") || '\"}'", None
    if kind == "DATE":
        return '\'{"date":"\' || CAST(' + column + " AS VARCHAR) || '\"}'", None
    if kind == "TIMESTAMP WITH TIME ZONE":
        return "'{\"utc_us\":' || CAST(epoch_us(" + column + ") AS VARCHAR) || '}'", None
    if kind == "TIMESTAMP":
        return "'{\"local_us\":' || CAST(epoch_us(" + column + ") AS VARCHAR) || '}'", None
    raise ValueError(f"source column type {kind} has no aas-source-row-v1 form")


def source_row_hash_sql(columns: Sequence[tuple[str, str]]) -> tuple[str, str, str]:
    """``aas-source-row-v1`` in SQL over a relation with these (name, DuckDB type) columns.

    Returns the hash expression, the condition under which that expression is exactly
    the Python digest, and the condition under which a row cannot be hashed at all. Text
    that JSON would escape fails the first condition and is hashed in Python; a date
    outside years 1..9999 (which Python cannot hold) fails the second.
    """
    if not columns:
        raise ValueError("a source row needs at least one column")
    parts = []
    plain = []
    invalid = []
    for name, kind in columns:
        column = quote_identifier(name)
        value, text = _value_sql(column, kind)
        parts.append(
            sql_literal("[" + json.dumps(name) + ",") + f" || coalesce({value}, 'null') || ']'"
        )
        if text is not None:
            plain.append(f"({text} IS NULL OR regexp_full_match({text}, '{JSON_PLAIN}'))")
        if kind == "DATE":
            invalid.append(f"({column} IS NOT NULL AND NOT (year({column}) BETWEEN 1 AND 9999))")
    document = (
        sql_literal('["' + SOURCE_ROW_FORMAT + '",[')
        + " || "
        + " || ',' || ".join(parts)
        + " || ']]'"
    )
    return (
        f"sha256({document})",
        " AND ".join(plain) or "true",
        " OR ".join(invalid) or "false",
    )


def source_row_fragments_sql(columns: Sequence[tuple[str, str]]) -> list[str]:
    """Per column: the raw text of a VARCHAR cell, or the JSON text of any other cell.

    The Python fallback for rows that ``source_row_hash_sql`` cannot hash exactly reads
    these, so it never has to convert a temporal value in Python.
    """
    fragments = []
    for name, kind in columns:
        column = quote_identifier(name)
        if kind == "VARCHAR":
            fragments.append(column)
        else:
            value, _ = _value_sql(column, kind)
            fragments.append(f"CASE WHEN {column} IS NULL THEN NULL ELSE {value} END")
    return fragments


def source_row_hash_from_fragments(
    columns: Sequence[tuple[str, str]], fragments: Sequence[object]
) -> str:
    """Assemble ``aas-source-row-v1`` from ``source_row_fragments_sql`` values."""
    parts = []
    for (name, kind), fragment in zip(columns, fragments, strict=True):
        if fragment is None:
            text = "null"
        elif kind == "VARCHAR":
            text = json.dumps(fragment)
        else:
            text = str(fragment)
        parts.append("[" + json.dumps(name) + "," + text + "]")
    document = '["' + SOURCE_ROW_FORMAT + '",[' + ",".join(parts) + "]]"
    return hashlib.sha256(document.encode()).hexdigest()


def revision_id_sql(dataset_id: str, record: str, op: str, supersedes: str, row_hash: str) -> str:
    """``aas-revision-v1`` in SQL; every operand but the dataset is a hex digest or an op."""
    if not is_plain(dataset_id):
        raise ValueError("dataset IDs are plain text")
    head = sql_literal('["' + REVISION_FORMAT + '",' + json.dumps(dataset_id) + ',"')
    return (
        f"sha256({head} || {record} || '\",\"' || {op} || '\",' || "
        f"coalesce('\"' || {supersedes} || '\"', 'null') || ',\"' || {row_hash} || '\"]')"
    )


def json_string_sql(expression: str) -> str:
    r"""``json.dumps`` of a VARCHAR expression, in SQL: quoted, with Python's ASCII escapes.

    Plain text passes through. Otherwise each code point is written as ``json.dumps``
    writes it: ``\"`` and ``\\``, the five short control escapes, printable ASCII as
    itself, any other code point below U+10000 as ``\uXXXX`` (lowercase hex) and a code
    point above it as its UTF-16 surrogate pair. NULL stays NULL.
    """
    short = " ".join(
        f"WHEN c = chr({code}) THEN {sql_literal(json.dumps(chr(code))[1:-1])}"
        for code in (0x22, 0x5C, 0x08, 0x0C, 0x0A, 0x0D, 0x09)
    )
    point = "unicode(c)"
    high = f"(55296 + (({point} - 65536) >> 10))"
    low = f"(56320 + (({point} - 65536) & 1023))"
    escaped = (
        f"array_to_string(list_transform(string_split({expression}, ''), lambda c: CASE {short} "
        f"WHEN {point} BETWEEN 32 AND 126 THEN c "
        f"WHEN {point} < 65536 THEN printf('\\u%04x', {point}) "
        f"ELSE printf('\\u%04x\\u%04x', {high}, {low}) END), '')"
    )
    return (
        f"('\"' || CASE WHEN regexp_full_match({expression}, '{JSON_PLAIN}') THEN {expression} "
        f"ELSE {escaped} END || '\"')"
    )


def dimensions_hash_sql(dimensions: Sequence[tuple[str, str]]) -> str:
    """``aas-dimensions-v1`` in SQL over (name, VARCHAR expression) pairs.

    Names are fixed code-owned text; any NULL value makes the hash NULL, so a fact whose
    context is not known has no dimensions rather than a smaller set.
    """
    names = [name for name, _ in dimensions]
    if not names or names != sorted(set(names)) or not all(is_plain(name) for name in names):
        raise ValueError("dimension names are distinct plain text in sorted order")
    members = " || ',' || ".join(
        sql_literal(json.dumps(name) + ":") + " || " + json_string_sql(value)
        for name, value in dimensions
    )
    head = sql_literal('["' + DIMENSIONS_FORMAT + '",{')
    return f"sha256({head} || {members} || '}}]')"
