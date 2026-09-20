"""Strict portable input envelope for explicit offline market imports."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json

_REQUIRED = frozenset(
    {
        "schema_version",
        "dataset_id",
        "version",
        "generation_id",
        "operation_id",
        "parent_id",
        "domain",
        "provider",
        "publication_at_us",
        "normalizer_version",
        "transform_sha256",
        "rows",
    }
)
# transform_schema is how a native route declares the retained transform its
# publication was built from. The sealed document's SHA-256 is the marker request hash
# the generation chain covers, so that declaration is authenticated evidence rather
# than mutable catalog state. A generic offline import omits it, because it commits an
# opaque transform_sha256 whose preimage it never retained.
_OPTIONAL = frozenset({"instruments", "transform_schema"})
# The envelope carries a declaration only for a transform schema it knows, so an
# unrecognized value is refused here instead of being silently read as an opaque
# commitment by a verifier that expects retained bytes behind it.
_TRANSFORM_SCHEMAS = frozenset({"aas-observation-transform-v1"})
_MAX_IMPORT_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ImportDocument:
    payload: bytes
    sha256: str
    body: dict[str, object]
    rows: list[dict[str, object]]
    source_id: str


def read_import(path: Path, expected_sha256: str) -> ImportDocument:
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_IMPORT_BYTES)
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("import bytes do not match the expected SHA-256")
    return parse_import(raw)


def parse_import(raw: bytes) -> ImportDocument:  # noqa: C901 -- strict external envelope boundary
    value = decode_json(raw)
    if not isinstance(value, dict):
        raise TypeError("market import must be an object")
    body = cast("dict[str, object]", value)
    if not body.keys() >= _REQUIRED or body.keys() - (_REQUIRED | _OPTIONAL):
        raise ValueError("market import has missing or unknown fields")
    if body["schema_version"] != "aas-market-import-v1":
        raise ValueError("unsupported market import schema")
    for key in _REQUIRED - {"parent_id", "publication_at_us", "rows"}:
        item = body[key]
        if not isinstance(item, str) or not item.strip() or item != item.strip():
            raise ValueError("market import identities must be exact nonempty text")
    parent = body["parent_id"]
    if parent is not None and (not isinstance(parent, str) or not parent.strip()):
        raise ValueError("parent_id must be null or exact generation identity")
    publication = body["publication_at_us"]
    if publication is not None and (type(publication) is not int or publication < 0):
        raise ValueError("publication_at_us must be null or UTC microseconds")
    declared = body.get("transform_schema")
    if declared is not None and declared not in _TRANSFORM_SCHEMAS:
        raise ValueError("transform_schema must be a known retained transform schema")
    raw_rows = body["rows"]
    if not isinstance(raw_rows, list) or not raw_rows:
        raise ValueError("market import needs a nonempty array of typed rows")
    digest = hashlib.sha256(raw).hexdigest()
    source_id = "source-" + digest
    rows: list[dict[str, object]] = []
    for raw_row in raw_rows:
        if (
            not isinstance(raw_row, dict)
            or {"source_snapshot_id", "source_row_hash"} & raw_row.keys()
        ):
            raise ValueError("source lineage is assigned from imported bytes, not supplied rows")
        row = cast("dict[str, object]", raw_row)
        rows.append(
            {
                **row,
                "source_snapshot_id": source_id,
                "source_row_hash": hashlib.sha256(canonical_json_bytes(row)).hexdigest(),
            }
        )
    return ImportDocument(raw, digest, body, rows, source_id)
