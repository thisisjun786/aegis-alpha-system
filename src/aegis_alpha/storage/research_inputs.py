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
import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import generation_chain, normalize_rows
from aegis_alpha.storage.market_schema import COMMON, DOMAINS
from aegis_alpha.storage.publication import publish_document
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows, resolve_source

if TYPE_CHECKING:
    from aegis_alpha.storage.import_document import ImportDocument
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


def _decode_transform(raw: bytes) -> object:
    _ = raw.decode("utf-8")  # The shared byte decoder also autodetects UTF-16/32.
    # BOM-less UTF-16/32 ASCII can pass UTF-8 decoding but contains literal NULs.
    if b"\x00" in raw:
        raise ValueError("research transform requires UTF-8 JSON without literal NUL bytes")
    return decode_json(raw)


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
    body = _object(_decode_transform(raw), _ROOT)
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
    if any(
        item["asset_type"] == "proxy"
        for item in cast("list[dict[str, object]]", body["instruments"])
    ):
        raise ValueError("proxy indices belong in feature_values, never executable OHLC")
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


@dataclass(frozen=True, slots=True)
class _Transform:
    raw: bytes
    sha256: str
    domain: str
    body: dict[str, object]
    source: SourcePin
    dataset: dict[str, object]
    columns: dict[str, str]


def _source_pin(value: object) -> SourcePin:
    fields = _object(value, frozenset({"source_id", "source_sha256", "table", "table_digest"}))
    return SourcePin(**{key: _text(item) for key, item in fields.items()})


def _read_transform(path: Path, sha256: str, kind: str) -> _Transform:
    path = path.absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=_MAX_BYTES)
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("transform bytes do not match the expected SHA-256")
    domain, extra = (
        ("calendar_sessions", "calendar") if kind == "sessions" else ("feature_values", "proxy")
    )
    keys = (_ROOT - {"price", "calendar", "decimal_conversion"}) | {extra}
    body = _object(_decode_transform(raw), keys)
    if body["schema_version"] != "aas-" + kind + "-transform-v1":
        raise ValueError("unsupported research transform schema")
    fields = frozenset(name for name, _ in COMMON + DOMAINS[domain])
    columns = {key: _text(value) for key, value in _object(body["columns"], fields).items()}
    if len(set(columns.values())) != len(columns):
        raise ValueError("source column mapping must not alias distinct market fields")
    return _Transform(
        raw,
        sha256,
        domain,
        body,
        _source_pin(body["source"]),
        _destination(body["dataset"]),
        columns,
    )


def _mapped_rows(workspace: Workspace, transform: _Transform) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    size = len(transform.raw)
    for batch in iter_source_rows(
        workspace, transform.source, columns=list(transform.columns.values())
    ):
        for retained in batch:
            row = {field: retained[column] for field, column in transform.columns.items()}
            size += len(canonical_json_bytes(row))
            _check_size(size)
            rows.append(row)
    return rows


def _input_document(transform: _Transform, rows: list[dict[str, object]]) -> ImportDocument:
    for row in rows:
        normalize_rows(transform.domain, _text(row["generation_id"]), [row])
    payload = canonical_json_bytes(
        {
            **transform.dataset,
            "schema_version": "aas-market-import-v1",
            "domain": transform.domain,
            **{
                key: transform.body[key]
                for key in ("provider", "publication_at_us", "normalizer_version", "instruments")
            },
            "transform_sha256": transform.sha256,
            "rows": [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"generation_id", "source_snapshot_id", "source_row_hash"}
                }
                for row in rows
            ],
        }
    )
    _check_size(len(payload))
    return parse_import(payload)


def register_sessions_input(
    workspace: Workspace, spec_path: Path, sha256: str
) -> dict[str, object]:
    """Publish aas-sessions-transform-v1 with every COMMON/calendar_sessions mapping.

    calendar fixes calendar_id, venue, timezone (IANA name), timezone_version.
    Open sessions require source open/close UTC microseconds; closed sessions
    have both null. Knowledge/publication times are never inferred from these.
    The exact raw transform preserves timezone and original source lineage.
    """
    transform = _read_transform(spec_path, sha256, "sessions")
    calendar = _object(
        transform.body["calendar"],
        frozenset({"calendar_id", "venue", "timezone", "timezone_version"}),
    )
    for value in calendar.values():
        _text(value)
    try:
        ZoneInfo(_text(calendar["timezone"]))
    except ZoneInfoNotFoundError:
        raise ValueError("unknown session timezone") from None
    if transform.body["instruments"] != []:
        raise ValueError("sessions require an empty instruments array")
    rows = _mapped_rows(workspace, transform)
    for row in rows:
        if any(row[key] != calendar[key] for key in ("calendar_id", "venue", "timezone_version")):
            raise ValueError("session source conflicts with calendar definition")
        match (row["status"], row["open_at_us"], row["close_at_us"]):
            case ("open", int() as opened, int() as closed) if 0 <= opened < closed:
                pass  # Exact int64 (including bool rejection) is checked by the market boundary.
            case ("closed", None, None):
                pass
            case _:
                raise ValueError(
                    "session status requires explicit ordered source times or closed nulls"
                )
    document = _input_document(transform, rows)
    put_raw(workspace.paths.raw, transform.raw)
    return {
        **publish_document(workspace, document),
        "transform_sha256": sha256,
        "source_pin": transform.body["source"],
    }


def _double(value: object, policy: str) -> float | None:
    if value is None:
        return None
    match (policy, value):
        case ("decimal_string", str()):
            try:
                decimal = Decimal(value)
            except InvalidOperation:
                raise ValueError("invalid proxy decimal string") from None
            if not decimal.is_finite():
                raise ValueError("proxy source number must be finite")
            result = float(decimal)
        case ("ieee_float", float()):
            result = value
        case _:
            raise TypeError("proxy number does not match its declared numeric policy")
    if not math.isfinite(result):
        raise ValueError("proxy value exceeds finite binary64 representation")
    return result


def _proxy_metadata(
    workspace: Workspace, definition: dict[str, object]
) -> list[tuple[str, str, str, str]]:
    for key in ("proxy_id", "version"):
        _text(definition[key])
    policy = _object(definition["normalization"], frozenset({"input_number", "output"}))
    if (
        policy["input_number"] not in ("decimal_string", "ieee_float")
        or policy["output"] != "ieee754_binary64"
    ):
        raise ValueError("proxy normalization must declare its source type and binary64 output")
    transition = _object(
        definition["transition"],
        frozenset(
            {
                "donor_id",
                "target_id",
                "logical_exposure_id",
                "switch_decision_date",
                "mode",
                "donor_source",
                "target_source",
                "basis_ref",
                "calendar_ref",
                "cost_ref",
            }
        ),
    )
    for key in ("donor_id", "target_id", "logical_exposure_id", "switch_decision_date"):
        _text(transition[key])
    decision = _text(transition["switch_decision_date"])
    if date.fromisoformat(decision).isoformat() != decision:
        raise ValueError("switch decision date must be ISO YYYY-MM-DD")
    if transition["mode"] not in ("signal_only", "observed_instrument_switch"):
        raise ValueError("unsupported proxy transition mode")
    inputs = []
    for key in ("donor_source", "target_source"):
        pin = _source_pin(transition[key])
        resolve_source(workspace, pin)
        inputs.append((key + ":" + pin.table, pin.source_id, pin.source_sha256, pin.table_digest))
    for key in ("basis_ref", "calendar_ref", "cost_ref"):
        ref = _object(transition[key], frozenset({"id", "version", "sha256"}))
        # Reuse SourcePin's lowercase digest admission without resolving a convention as a table.
        checked = SourcePin(
            _text(ref["id"]), _text(ref["sha256"]), _text(ref["version"]), _text(ref["sha256"])
        )
        if checked.table == "latest":
            raise ValueError("proxy convention references must be explicitly versioned")
        inputs.append((key, checked.source_id, checked.table, checked.source_sha256))
    return inputs


def _store_proxy_contract(
    workspace: Workspace, definition: dict[str, object], inputs: list[tuple[str, str, str, str]]
) -> None:
    payload = canonical_json_bytes(definition)
    values = (
        _text(definition["proxy_id"]),
        _text(definition["version"]),
        payload.decode(),
        "aas-market-rowset-v1",
        hashlib.sha256(payload).hexdigest(),
    )
    previous = workspace.state.execute(
        "SELECT name, version, definition, record_schema, content_hash FROM feature_contracts "
        "WHERE name=? AND version=?",
        values[:2],
    ).fetchone()
    if previous is not None:
        prior_inputs = workspace.state.execute(
            "SELECT ref_kind, ref_id, ref_version, content_hash FROM feature_inputs "
            "WHERE name=? AND version=? ORDER BY ordinal",
            values[:2],
        ).fetchall()
        if tuple(previous) != values or [tuple(row) for row in prior_inputs] != inputs:
            raise ValueError("proxy contract conflicts with registered definition or inputs")
        return
    with workspace.state:
        workspace.state.execute(
            "INSERT INTO feature_contracts(name, version, definition, record_schema, content_hash) "
            "VALUES (?, ?, ?, ?, ?)",
            values,
        )
        workspace.state.executemany(
            "INSERT INTO feature_inputs(name, version, ordinal, ref_kind, ref_id, ref_version, "
            "content_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(*values[:2], ordinal, *item) for ordinal, item in enumerate(inputs)],
        )


def register_proxy_input(workspace: Workspace, spec_path: Path, sha256: str) -> dict[str, object]:
    """Publish external aas-proxy-transform-v1 points as non-executable DOUBLE features.

    proxy fixes proxy_id/version, normalization {input_number, output}, and a
    transition with donor/target/logical exposure IDs, switch_decision_date,
    signal_only or observed_instrument_switch mode, donor_source/target_source
    SourcePins and basis_ref/calendar_ref/cost_ref {id, version, sha256} pins.
    contract_hash is SHA256(canonical proxy JSON); input_bundle_hash is SHA256
    of the ordered JSON array [donor_source,target_source,basis_ref,calendar_ref,cost_ref].
    Every COMMON/feature_values field maps to a distinct source column. Original
    numbers/hashes remain in the retained source; raw exact spec pins the mapping
    and normalizer. Binary64 normalization rounds decimal strings (including
    underflow); it rejects nonfinite/overflow values, not representational loss.

    Contract metadata is committed before publication so recovery cannot expose
    orphan feature points. A failed publication can leave an unused immutable
    contract, just as it can leave unused raw bytes. Retry the same pinned spec.
    Each contract version pins that exact transform in feature_inputs; changing
    its source, mapping, destination or normalizer requires a new proxy version.
    This registers caller-supplied definitions, not execution or PIT eligibility;
    convention hashes are preserved references, not provider certification.
    """
    transform = _read_transform(spec_path, sha256, "proxy")
    definition = _object(
        transform.body["proxy"], frozenset({"proxy_id", "version", "normalization", "transition"})
    )
    inputs = [
        *_proxy_metadata(workspace, definition),
        ("transform", sha256, "aas-proxy-transform-v1", sha256),
    ]
    transition = cast("dict[str, object]", definition["transition"])
    policy = cast("dict[str, object]", definition["normalization"])
    identities = _instruments(transform.body["instruments"])
    if identities != {transition["logical_exposure_id"]} or any(
        item["asset_type"] != "proxy"
        for item in cast("list[dict[str, object]]", transform.body["instruments"])
    ):
        raise ValueError("proxy requires its logical exposure instrument with asset_type proxy")
    contract_hash = hashlib.sha256(canonical_json_bytes(definition)).hexdigest()
    bundle_hash = hashlib.sha256(
        canonical_json_bytes(
            [
                transition[key]
                for key in (
                    "donor_source",
                    "target_source",
                    "basis_ref",
                    "calendar_ref",
                    "cost_ref",
                )
            ]
        )
    ).hexdigest()
    expected = {
        "contract_id": definition["proxy_id"],
        "contract_version": definition["version"],
        "contract_hash": contract_hash,
        "input_bundle_hash": bundle_hash,
        "instrument_id": transition["logical_exposure_id"],
    }
    rows = _mapped_rows(workspace, transform)
    for row in rows:
        if any(row[key] != value for key, value in expected.items()):
            raise ValueError("proxy source conflicts with its feature contract")
        row["value"] = _double(row["value"], _text(policy["input_number"]))
    document = _input_document(transform, rows)
    put_raw(workspace.paths.raw, transform.raw)
    _store_proxy_contract(workspace, definition, inputs)
    return {
        **publish_document(workspace, document),
        "transform_sha256": sha256,
        "source_pin": transform.body["source"],
        "non_executable": True,
    }
