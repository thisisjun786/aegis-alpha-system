from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree

SOURCE_TABLES = frozenset(
    {
        "source_snapshots",
        "source_snapshot_files",
        "dataset_versions",
        "dataset_sources",
        "dataset_input_files",
        "dataset_artifacts",
        "quality_results",
        "collection_run_plans",
        "collection_runs",
        "collection_run_events",
        "collection_run_receipts",
        "collection_watermarks",
        "collection_usage_records",
        "collection_usage_checkpoints",
        "identity_issuers",
        "identity_instruments",
        "identity_identifier_assertions",
        "identity_provider_mappings",
        "identity_mapping_conflicts",
    }
)
_HASH = re.compile(r"[0-9a-f]{64}")
_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")
_SOURCE_SERVER_MAJOR = 18
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_SNAPSHOT_BYTES = 1024 * 1024 * 1024


class SnapshotImportError(ValueError):
    """The supplied snapshot cannot be adopted without changing historical meaning."""


@dataclass(frozen=True, slots=True)
class SnapshotColumn:
    name: str
    sql_type: str
    generated: str
    generated_expression: str | None


@dataclass(frozen=True, slots=True)
class SnapshotTable:
    name: str
    columns: tuple[SnapshotColumn, ...]
    primary_key: tuple[str, ...]
    input_columns: tuple[str, ...]
    row_count: int
    input_sha256: str
    projection_sha256: str
    input_bytes: int
    projection_bytes: int

    def filename(self, kind: str) -> str:
        if kind not in {"input", "projection"}:
            raise SnapshotImportError("unknown snapshot projection")
        return f"{self.name}.{kind}.bin"


@dataclass(frozen=True, slots=True)
class LegacySnapshot:
    root: Path
    manifest_sha256: str
    source_system_identifier: str
    source_database: str
    source_alembic_head: str
    tables: tuple[SnapshotTable, ...]


def _object(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise SnapshotImportError("snapshot object has missing or unknown fields")
    return cast("dict[str, object]", value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise SnapshotImportError("snapshot field must be an array")
    return cast("list[object]", value)


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise SnapshotImportError("snapshot text field is invalid")
    return value


def _name(value: object) -> str:
    name = _text(value)
    if _NAME.fullmatch(name) is None:
        raise SnapshotImportError("snapshot identifier is invalid")
    return name


def _number(value: object) -> int:
    if type(value) is not int or value < 0:
        raise SnapshotImportError("snapshot counts must be nonnegative integers")
    return value


def _digest(value: object) -> str:
    digest = _text(value)
    if _HASH.fullmatch(digest) is None:
        raise SnapshotImportError("snapshot hash must be lowercase SHA-256")
    return digest


def _column(value: object) -> SnapshotColumn:
    row = _object(value, {"name", "type", "generated", "generated_expression"})
    generated = row["generated"]
    if generated not in ("", "s"):
        raise SnapshotImportError("unsupported generated column kind")
    expression = None if row["generated_expression"] is None else _text(row["generated_expression"])
    if bool(generated) != (expression is not None):
        raise SnapshotImportError("generated column expression is missing or unexpected")
    return SnapshotColumn(
        _name(row["name"]), _text(row["type"]), cast("str", generated), expression
    )


def _table(value: object) -> SnapshotTable:
    row = _object(
        value,
        {
            "name",
            "columns",
            "primary_key",
            "input_columns",
            "row_count",
            "input_file",
            "projection_file",
            "input_sha256",
            "projection_sha256",
            "input_bytes",
            "projection_bytes",
        },
    )
    name = _name(row["name"])
    if name not in SOURCE_TABLES:
        raise SnapshotImportError("snapshot table is not an admitted legacy table")
    columns = tuple(_column(c) for c in _array(row["columns"]))
    names = tuple(c.name for c in columns)
    primary = tuple(_name(c) for c in _array(row["primary_key"]))
    inputs = tuple(_name(c) for c in _array(row["input_columns"]))
    if not names or len(set(names)) != len(names) or not primary or not set(primary) <= set(names):
        raise SnapshotImportError("invalid or duplicate snapshot columns/primary key")
    if len(set(primary)) != len(primary) or inputs != tuple(
        c.name for c in columns if not c.generated
    ):
        raise SnapshotImportError("COPY input must exclude exactly the generated columns")
    for kind in ("input", "projection"):
        if row[kind + "_file"] != f"{name}.{kind}.bin":
            raise SnapshotImportError("snapshot filename is not bound to its table")
    return SnapshotTable(
        name,
        columns,
        primary,
        inputs,
        _number(row["row_count"]),
        _digest(row["input_sha256"]),
        _digest(row["projection_sha256"]),
        _number(row["input_bytes"]),
        _number(row["projection_bytes"]),
    )


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in values:
        if key in result:
            raise SnapshotImportError("duplicate snapshot JSON field")
        result[key] = value
    return result


def load_snapshot(root: Path, expected_sha256: str) -> LegacySnapshot:
    """Validate a caller-pinned bounded manifest and every referenced binary file."""
    expected = _digest(expected_sha256)
    with DescriptorTree.open_path(root) as tree:
        payload = tree.read_bytes("manifest.json", max_bytes=_MAX_MANIFEST_BYTES)
        if hashlib.sha256(payload).hexdigest() != expected:
            raise SnapshotImportError("snapshot manifest hash mismatch")
        row = _object(
            json.loads(payload, object_pairs_hook=_pairs),
            {
                "format",
                "source_system_identifier",
                "source_database",
                "source_alembic_head",
                "source_server_major",
                "source_encoding",
                "order_contract",
                "tables",
            },
        )
        if (
            row["format"] != "aas-legacy-copy-snapshot/v1"
            or row["source_server_major"] != _SOURCE_SERVER_MAJOR
            or type(row["source_server_major"]) is not int
            or row["source_encoding"] != "UTF8"
            or row["order_contract"] != "pk-as-text-C-v1"
            or row["source_alembic_head"] != "20260818_0004"
        ):
            raise SnapshotImportError("unsupported snapshot encoding/version/source schema")
        tables = tuple(_table(t) for t in _array(row["tables"]))
        if len(tables) != len(SOURCE_TABLES) or {t.name for t in tables} != SOURCE_TABLES:
            raise SnapshotImportError("snapshot must include every admitted table exactly once")
        if sum(t.input_bytes + t.projection_bytes for t in tables) > _MAX_SNAPSHOT_BYTES:
            raise SnapshotImportError("metadata snapshot exceeds the import byte budget")
        for table in tables:
            _verify_files(tree, table)
    identifier = _text(row["source_system_identifier"])
    if not identifier.isascii() or not identifier.isdigit():
        raise SnapshotImportError("source system identifier is invalid")
    return LegacySnapshot(
        root,
        expected,
        identifier,
        _name(row["source_database"]),
        _text(row["source_alembic_head"]),
        tables,
    )


def _verify_files(tree: DescriptorTree, table: SnapshotTable) -> None:
    for kind in ("input", "projection"):
        digest = table.input_sha256 if kind == "input" else table.projection_sha256
        size = table.input_bytes if kind == "input" else table.projection_bytes
        with tree.binary_reader(table.filename(kind)) as handle:
            result = hashlib.sha256()
            while chunk := handle.read(1024 * 1024):
                result.update(chunk)
            actual = result.hexdigest()
        if actual != digest or tree.stat(table.filename(kind)).st_size != size:
            raise SnapshotImportError("snapshot binary file hash/size mismatch")
