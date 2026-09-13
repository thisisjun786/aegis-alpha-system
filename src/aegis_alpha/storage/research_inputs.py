"""Explicit retained-source price transforms; no provider, PIT or execution admission.

``register_price_input(workspace, spec_path, sha256)`` consumes exact UTF-8 JSON
with schema_version ``aas-price-transform-v1``. Required objects are:

* source: SourcePin's source_id, source_sha256, table and table_digest.
* dataset: dataset_id, version, generation_id, operation_id and nullable parent_id.
* columns: every COMMON and prices field mapped to a distinct retained column.
  record_id must be the existing market natural-key identity, not a ticker.
* instruments: explicit instrument_id/asset_type/venue records (no duplicate IDs).
* price: basis/currency/price_role, checked against every mapped source row.
* calendar: calendar_id/timezone/timezone_version (provenance, not session data).
* decimal_conversion: open/high/low/close/volume each declare decimal_string or
  ieee_float. Both require exact DECIMAL(38,12) admission, without rounding.

provider, normalizer_version and nullable publication_at_us are also required.
Unknown keys, incomplete OHLCV and fabricated revision links are not admitted.
The source's generation/ingestion/snapshot/hash remain in the pinned retained
source, addressed by the complete mapping in the exact raw transform. Publication
assigns NEW generation/ingestion/snapshot/row hashes to normalized import bytes;
those are deliberately not mislabeled as original source provenance. The catalog
transform_hash addresses the exact spec under raw/<first-two-hex>/<sha256>.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import generation_chain, normalize_rows
from aegis_alpha.storage.market_schema import COMMON, DOMAINS
from aegis_alpha.storage.publication import publish_document
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

_MAX_BYTES = 64 * 1024 * 1024
_NUMBERS = frozenset({"open", "high", "low", "close", "volume"})
_FIELDS = frozenset(name for name, _ in COMMON + DOMAINS["prices"])
_ROOT = frozenset(
    {
        "schema_version",
        "source",
        "dataset",
        "columns",
        "instruments",
        "price",
        "calendar",
        "publication_at_us",
        "provider",
        "normalizer_version",
        "decimal_conversion",
    }
)


def _object(value: object, keys: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("price transform object has missing or unknown fields")
    return cast("dict[str, object]", value)


def _text(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip() or "\x00" in value:
        raise ValueError("price transform identities must be exact nonempty text")
    return value


def _decimal(value: object, policy: str) -> str | None:
    if value is None:
        return None
    match (policy, value):
        case ("decimal_string", str()):
            try:
                decimal = Decimal(value)
            except InvalidOperation:
                raise ValueError("invalid exact decimal string") from None
        case ("ieee_float", float()):
            decimal = Decimal.from_float(value)  # noqa: FURB164 -- explicit contract conversion
        case _:
            raise TypeError("source number does not match its declared numeric policy")
    if not decimal.is_finite() or decimal.copy_abs() >= Decimal("1e26"):
        raise ValueError("number exceeds exact DECIMAL(38,12) representation")
    with localcontext() as context:
        context.prec = 50
        quantized = decimal.quantize(Decimal("0.000000000001"))
    if decimal != quantized:
        raise ValueError("number exceeds exact DECIMAL(38,12) representation")
    return str(quantized)


def _instruments(value: object) -> set[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("price transform requires an instruments array")
    identities = []
    for entry in value:
        item = _object(entry, frozenset({"instrument_id", "asset_type", "venue"}))
        for field in item.values():
            _text(field)
        identities.append(_text(item["instrument_id"]))
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate instrument identity")
    return set(identities)


def _destination(value: object) -> dict[str, object]:
    dataset = _object(
        value, frozenset({"dataset_id", "version", "generation_id", "operation_id", "parent_id"})
    )
    for key, item in dataset.items():
        if key != "parent_id" or item is not None:
            _text(item)
    return dataset


def _price_row(
    source: dict[str, object], price: dict[str, object], policies: dict[str, str]
) -> dict[str, object]:
    if any(source[key] != value for key, value in price.items()):
        raise ValueError("source currency, basis or price role conflicts with transform")
    row = {
        **source,
        **{field: _decimal(source[field], policy) for field, policy in policies.items()},
    }
    if any((row[field] is not None) != (row["value_state"] == "present") for field in _NUMBERS):
        raise ValueError("prices require complete OHLCV or whole-row missingness")
    # Validate original provenance too; normalization must not hide bad source fields.
    with localcontext() as context:
        context.prec = 50
        normalize_rows("prices", _text(row["generation_id"]), [row])
    return {
        key: value
        for key, value in row.items()
        if key
        not in {
            "generation_id",
            "source_snapshot_id",
            "source_row_hash",
        }
    }


def _check_price_parent(workspace: Workspace, parent: object, price: dict[str, object]) -> None:
    if parent is None:
        return
    for ancestor in generation_chain(workspace.market, _text(parent)):
        conflict = workspace.market.execute(
            "SELECT 1 FROM prices WHERE generation_id=? "
            "AND (basis<>? OR currency<>? OR price_role<>?) LIMIT 1",
            [ancestor["generation_id"], price["basis"], price["currency"], price["price_role"]],
        ).fetchone()
        if conflict:
            raise ValueError("price basis, currency and role changes require a separate dataset")


def _check_size(size: int) -> None:
    if size > _MAX_BYTES:
        raise ValueError("price transform exceeds bounded import size")


def register_price_input(workspace: Workspace, spec_path: Path, sha256: str) -> dict[str, object]:
    """Publish one explicit source delta while retaining exact transform provenance.

    Caller owns writable workspace admission (including strategy-source access).
    All numeric/source rows are checked before publication. A failed publication
    may leave unreferenced immutable spec bytes, never a successful dataset pin.
    Existing publication recovery/idempotency owns the cross-store transaction.
    """
    path = spec_path.absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_BYTES)
    digest = hashlib.sha256(raw).hexdigest()
    if digest != sha256:
        raise ValueError("price transform bytes do not match the expected SHA-256")
    body = _object(decode_json(raw), _ROOT)
    if body["schema_version"] != "aas-price-transform-v1":
        raise ValueError("unsupported price transform schema")
    source = _object(
        body["source"], frozenset({"source_id", "source_sha256", "table", "table_digest"})
    )
    pin = SourcePin(**{key: _text(value) for key, value in source.items()})
    dataset = _destination(body["dataset"])
    columns = {key: _text(value) for key, value in _object(body["columns"], _FIELDS).items()}
    if len(set(columns.values())) != len(columns):
        raise ValueError("source column mapping must not alias distinct market fields")
    price = _object(body["price"], frozenset({"basis", "currency", "price_role"}))
    calendar = _object(body["calendar"], frozenset({"calendar_id", "timezone", "timezone_version"}))
    for value in (*price.values(), *calendar.values()):
        _text(value)
    identities = _instruments(body["instruments"])
    policies = {
        key: _text(value) for key, value in _object(body["decimal_conversion"], _NUMBERS).items()
    }
    if set(policies.values()) - {"decimal_string", "ieee_float"}:
        raise ValueError("unsupported decimal conversion policy")
    rows: list[dict[str, object]] = []
    size = len(raw)
    for batch in iter_source_rows(workspace, pin, columns=list(columns.values())):
        for retained in batch:
            row = {field: retained[column] for field, column in columns.items()}
            if row["instrument_id"] not in identities:
                raise ValueError("source row lacks an explicitly supplied instrument identity")
            transformed = _price_row(row, price, policies)
            size += len(canonical_json_bytes(transformed))
            _check_size(size)
            rows.append(transformed)
    payload = canonical_json_bytes(
        {
            **dataset,
            "schema_version": "aas-market-import-v1",
            "domain": "prices",
            "provider": body["provider"],
            "publication_at_us": body["publication_at_us"],
            "normalizer_version": body["normalizer_version"],
            "transform_sha256": digest,
            "instruments": body["instruments"],
            "rows": rows,
        }
    )
    _check_size(len(payload))
    document = parse_import(payload)
    _check_price_parent(workspace, dataset["parent_id"], price)
    put_raw(workspace.paths.raw, raw)
    # The market validator's magnitude arithmetic also needs full storage precision.
    with localcontext() as context:
        context.prec = 50
        return {
            **publish_document(workspace, document),
            "transform_sha256": digest,
            "source_pin": source,
        }
