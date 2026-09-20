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
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import generation_chain, normalize_rows
from aegis_alpha.storage.market_schema import COMMON, DOMAINS
from aegis_alpha.storage.publication import publish_document
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_library import admit_source_table
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
_KINDS = {
    "sessions": ("calendar_sessions", "calendar"),
    "proxy": ("feature_values", "proxy"),
    "observation": ("feature_values", "observation"),
}
OBSERVATION_DEFINITION_SCHEMA = "aas-observation-definition-v1"
_OBSERVATION = frozenset(
    {
        "series_id",
        "version",
        "observation_role",
        "basis",
        "currency",
        "price_role",
        "adjustment",
        "value_domain",
        "certified",
        "normalization",
        "observed_source",
        "calendar_ref",
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
    if dataset["version"] == "latest":
        raise ValueError("research destination version must be exact, not latest")
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
    document, source = _price_document(workspace, raw)
    put_raw(workspace.paths.raw, raw)
    # The market validator's magnitude arithmetic also needs full storage precision.
    with localcontext() as context:
        context.prec = 50
        return {
            **publish_document(workspace, document),
            "transform_sha256": digest,
            "source_pin": source,
        }


def _price_document(workspace: Workspace, raw: bytes) -> tuple[ImportDocument, dict[str, object]]:
    digest = hashlib.sha256(raw).hexdigest()
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
    return document, source


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
    return _parse_transform(raw, sha256, kind)


def _parse_transform(raw: bytes, sha256: str, kind: str) -> _Transform:
    domain, extra = _KINDS[kind]
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
    document = _sessions_document(workspace, transform)
    put_raw(workspace.paths.raw, transform.raw)
    return {
        **publish_document(workspace, document),
        "transform_sha256": sha256,
        "source_pin": transform.body["source"],
    }


def _sessions_document(workspace: Workspace, transform: _Transform) -> ImportDocument:
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
    return _input_document(transform, rows)


def native_input_document(
    workspace: Workspace,
    raw: bytes,
    *,
    expected_schema: str,
    budget: ComputeBudget,
    resolved: set[SourcePin] | None = None,
) -> tuple[ImportDocument, SourcePin]:
    """Reconstruct a native publication and its source pin without publishing.

    This is explicit native admission, not generic import classification. Size
    aggregates precede source resolution/digest/row materialization. Both readers
    and writers use the same transform parsers and normalization below.
    """
    available = budget.available_bytes
    if len(raw) * 32 > available:
        raise ComputeResourceError("native transform exceeds materialization budget")
    body = _decode_transform(raw)
    if (
        expected_schema
        not in {
            "aas-price-transform-v1",
            "aas-sessions-transform-v1",
            "aas-observation-transform-v1",
        }
        or not isinstance(body, dict)
        or body.get("schema_version") != expected_schema
    ):
        raise ValueError("native input does not match expected transform schema")
    source = _source_pin(body.get("source"))
    admit_source_table(
        workspace,
        source.source_id,
        source.table,
        max_materialization_bytes=available - len(raw) * 32,
    )
    if expected_schema == "aas-price-transform-v1":
        return _price_document(workspace, raw)[0], source
    if expected_schema == "aas-observation-transform-v1":
        # raw and its decoded body stay live through the rest of this call, so the
        # upstream admission below must see the remaining allowance, not the whole
        # lease. The root source above is already charged the same way.
        charged = replace(budget, reserved_bytes=budget.reserved_bytes + len(raw) * 32)
        document = _observation_document(
            workspace, raw, hashlib.sha256(raw).hexdigest(), resolved, charged
        )[0]
        return document, source
    transform = _parse_transform(raw, hashlib.sha256(raw).hexdigest(), "sessions")
    return _sessions_document(workspace, transform), source


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


def _store_feature_contract(
    workspace: Workspace,
    definition: dict[str, object],
    inputs: list[tuple[str, str, str, str]],
    *,
    name: str,
    record_schema: str,
) -> None:
    payload = canonical_json_bytes(definition)
    values = (
        _text(name),
        _text(definition["version"]),
        payload.decode(),
        record_schema,
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
            raise ValueError("feature contract conflicts with registered definition or inputs")
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
    _store_feature_contract(
        workspace,
        definition,
        inputs,
        name=_text(definition["proxy_id"]),
        record_schema="aas-market-rowset-v1",
    )
    return {
        **publish_document(workspace, document),
        "transform_sha256": sha256,
        "source_pin": transform.body["source"],
        "non_executable": True,
    }


def _check_identities(workspace: Workspace, supplied: object) -> None:
    """Refuse an identity conflict before any contract or immutable spec is written."""
    for item in cast("list[dict[str, object]]", supplied):
        previous = workspace.state.execute(
            "SELECT asset_type, venue FROM instruments WHERE instrument_id=?",
            (item["instrument_id"],),
        ).fetchone()
        if previous is not None and tuple(previous) != (item["asset_type"], item["venue"]):
            raise ValueError("instrument identity conflicts with registered definition")


def _admit_upstream(
    workspace: Workspace,
    pin: SourcePin,
    resolved: set[SourcePin] | None,
    budget: ComputeBudget | None,
) -> None:
    """Resolve one upstream panel pin at most once per pass, under admission.

    Resolving recomputes that table's digest, which is real work on a caller-owned
    lease, so it is admitted before it happens rather than after it has allocated.
    Every generation of one panel shares this pin, so a chunked panel would
    otherwise rehash the same upstream table once per generation.
    """
    if resolved is not None and pin in resolved:
        return
    if budget is not None:
        admit_source_table(
            workspace,
            pin.source_id,
            pin.table,
            max_materialization_bytes=budget.available_bytes,
        )
    resolve_source(workspace, pin)
    if resolved is not None:
        resolved.add(pin)


def _observation_definition(
    workspace: Workspace,
    body: dict[str, object],
    resolved: set[SourcePin] | None = None,
    budget: ComputeBudget | None = None,
) -> tuple[dict[str, object], str, list[tuple[str, str, str, str]]]:
    """Admit one observation contract: identity, price semantics and its pinned inputs.

    resolved carries the upstream pins a caller has already checked in this pass.
    Resolving recomputes the panel's table digest, and every generation of one panel
    shares that pin, so a chunked panel would otherwise rehash the same upstream
    table once per generation.
    """
    definition = _object(body["observation"], _OBSERVATION)
    for key in ("series_id", "version", "observation_role", "basis", "currency", "adjustment"):
        _text(definition[key])
    if definition["observation_role"] not in ("open", "close"):
        raise ValueError("observation role must be the observed open or close")
    if definition["price_role"] != "reference":
        raise ValueError("observed research prices must remain reference data")
    if definition["value_domain"] not in ("positive", "real"):
        raise ValueError("observation must declare a positive or real value domain")
    if definition["certified"] is not False:
        raise ValueError("observed research inputs are never certified")
    policy = _object(definition["normalization"], frozenset({"input_number", "output"}))
    if (
        policy["input_number"] not in ("decimal_string", "ieee_float")
        or policy["output"] != "ieee754_binary64"
    ):
        raise ValueError("observation normalization must declare its source type and output")
    pin = _source_pin(definition["observed_source"])
    _admit_upstream(workspace, pin, resolved, budget)
    inputs = [("observed_source:" + pin.table, pin.source_id, pin.source_sha256, pin.table_digest)]
    reference = _object(definition["calendar_ref"], frozenset({"id", "version", "sha256"}))
    # Reuse SourcePin's lowercase digest admission without resolving a convention as a table.
    checked = SourcePin(
        _text(reference["id"]),
        _text(reference["sha256"]),
        _text(reference["version"]),
        _text(reference["sha256"]),
    )
    if checked.table == "latest":
        raise ValueError("observation convention references must be explicitly versioned")
    inputs.append(("calendar_ref", checked.source_id, checked.table, checked.source_sha256))
    identity = _text(definition["series_id"]) + "/" + _text(definition["observation_role"])
    return definition, identity, inputs


def _observation_values(
    rows: list[dict[str, object]], expected: dict[str, object], definition: dict[str, object]
) -> None:
    """Normalize to binary64 and hold every row inside its declared contract and domain."""
    policy = cast("dict[str, object]", definition["normalization"])
    number = _text(policy["input_number"])
    positive = definition["value_domain"] == "positive"
    for row in rows:
        if any(row[key] != value for key, value in expected.items()):
            raise ValueError("observation source conflicts with its feature contract")
        if row["value_state"] not in ("present", "missing"):
            raise ValueError("observation points are present or missing only")
        row["value"] = _double(row["value"], number)
        value = row["value"]
        if (value is not None) != (row["value_state"] == "present"):
            raise ValueError("observation value and missing state disagree")
        if value is not None and positive and value <= 0:
            raise ValueError("observation value leaves its declared value domain")


def _ancestor_transform(workspace: Workspace, digest: str) -> object:
    """Read one ancestor's pinned transform from immutable raw bytes, hash-checked."""
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(digest[:2] + "/" + digest, max_bytes=_MAX_BYTES)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("observation ancestor transform bytes do not match the catalog")
    return _decode_transform(raw)


def _check_observation_parent(
    workspace: Workspace,
    parent: object,
    contract: str,
    version: object,
    definition: dict[str, object],
) -> None:
    """An extension continues one contract, published by this route, all the way down.

    Matching row identities is not enough: a generation written through the generic
    import route can carry the same contract columns without ever having an
    observation transform, and that chain only fails later, when it is read.
    """
    if parent is None:
        return
    for ancestor in generation_chain(workspace.market, _text(parent)):
        conflict = workspace.market.execute(
            "SELECT 1 FROM feature_values WHERE generation_id=? "
            "AND (contract_id<>? OR contract_version<>?) LIMIT 1",
            [ancestor["generation_id"], contract, version],
        ).fetchone()
        if conflict:
            raise ValueError("observation extensions must continue the same contract and version")
        catalog = workspace.state.execute(
            "SELECT transform_hash FROM dataset_versions WHERE generation_id=? AND status=?",
            (ancestor["generation_id"], "committed"),
        ).fetchone()
        if catalog is None:
            raise ValueError("observation ancestor has no committed catalog entry")
        body = _ancestor_transform(workspace, _text(catalog[0]))
        if (
            not isinstance(body, dict)
            or body.get("schema_version") != "aas-observation-transform-v1"
            or body.get("observation") != definition
        ):
            raise ValueError("observation ancestor was not published by this contract's route")


def _observation_document(
    workspace: Workspace,
    raw: bytes,
    sha256: str,
    resolved: set[SourcePin] | None = None,
    budget: ComputeBudget | None = None,
) -> tuple[ImportDocument, _Transform, dict[str, object], str, list[tuple[str, str, str, str]]]:
    """Rebuild one observation publication from its pinned source, without publishing.

    Every value is re-derived here through the same mapping and numeric policy the
    registration used, so a caller can compare the resulting bytes with what a
    generation actually committed instead of trusting its metadata.
    """
    transform = _parse_transform(raw, sha256, "observation")
    definition, contract, inputs = _observation_definition(
        workspace, transform.body, resolved, budget
    )
    identities = _instruments(transform.body["instruments"])
    if any(
        item["asset_type"] == "proxy"
        for item in cast("list[dict[str, object]]", transform.body["instruments"])
    ):
        raise ValueError("research return proxies belong in feature contracts of their own")
    expected = {
        "contract_id": contract,
        "contract_version": definition["version"],
        "contract_hash": hashlib.sha256(canonical_json_bytes(definition)).hexdigest(),
        "input_bundle_hash": hashlib.sha256(
            canonical_json_bytes([definition["observed_source"], definition["calendar_ref"]])
        ).hexdigest(),
    }
    rows = _mapped_rows(workspace, transform)
    for row in rows:
        if row["instrument_id"] not in identities:
            raise ValueError("source row lacks an explicitly supplied instrument identity")
    _observation_values(rows, expected, definition)
    return _input_document(transform, rows), transform, definition, contract, inputs


def register_observation_input(
    workspace: Workspace, spec_path: Path, sha256: str
) -> dict[str, object]:
    """Publish aas-observation-transform-v1 observed prices as non-executable DOUBLE features.

    This is the explicit observed research route for a retained panel carrying real
    session opens or closes without the complete OHLCV an executable bar needs. It
    never reaches the prices domain, so exact DECIMAL(38,12) admission and the
    complete-OHLCV gate keep refusing precisely what they refused before, while the
    retained binary64 bits survive unrounded in feature_values.

    observation fixes series_id/version, observation_role (open or close), basis,
    currency, price_role (always reference), adjustment, value_domain (positive or
    real), certified (always false), normalization {input_number, output}, the
    observed_source pin of the upstream retained panel, and calendar_ref
    {id, version, sha256}. observed_source is provenance for where the numbers came
    from; the transform's own root source pins the mapped point table, exactly as the
    proxy route separates its donor/target pins from its point table. The stored
    contract name is series_id + "/" + observation_role, so the two roles of one
    series stay distinct under the feature_values natural key and each carries its
    own missingness. contract_hash is SHA256(canonical observation JSON);
    input_bundle_hash is SHA256 of the ordered array [observed_source, calendar_ref].
    Neither hash covers an output value. Every row must name this contract, so a
    publication mixes no proxy or foreign observation points.

    feature_inputs pins observed_source and calendar_ref only, never one transform,
    because an observed panel legitimately arrives as several bounded generations and
    is extended by later ones. Each contributing generation's own transform is
    authenticated against the catalog and its sealed import when the series is read,
    so every chunk has to declare this same contract.

    publication_at_us stays exactly as the transform declares it, including null: an
    unknown publication time is never filled in from a session date. Knowledge
    columns come from the retained source, so a panel without knowledge times keeps
    NULL available_at_us/revision_known_at_us and strict PIT continues to select
    nothing from it. calendar_ref is a preserved reference, not a resolved calendar.
    Registration confers no provider authority, PIT eligibility or execution
    readiness, and asserts no equivalence to any certified price source.
    """
    read = _read_transform(spec_path, sha256, "observation")
    document, transform, definition, contract, inputs = _observation_document(
        workspace, read.raw, sha256
    )
    _check_identities(workspace, transform.body["instruments"])
    _check_observation_parent(
        workspace, transform.dataset["parent_id"], contract, definition["version"], definition
    )
    put_raw(workspace.paths.raw, transform.raw)
    _store_feature_contract(
        workspace, definition, inputs, name=contract, record_schema=OBSERVATION_DEFINITION_SCHEMA
    )
    return {
        **publish_document(workspace, document),
        "transform_sha256": sha256,
        "source_pin": transform.body["source"],
        "non_executable": True,
        "certified": False,
    }
