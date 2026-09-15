"""Immutable, whole-document convention pins on an admitted state connection."""

from __future__ import annotations

import hashlib
import math
import re
import sqlite3
from dataclasses import asdict, dataclass, fields
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.ensemble import EnsembleMembership
from aegis_alpha.engine.membership import MembershipRow
from aegis_alpha.engine.models import DerivedInputBinding, DerivedSeriesSpec
from aegis_alpha.storage import market
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, read_membership_pins

if TYPE_CHECKING:
    from aegis_alpha.engine.requirements import ExecutionDefinition
    from aegis_alpha.storage.workspace import Workspace
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.state import atomic

_MAX_BYTES = 1024 * 1024
_KINDS = frozenset({"calendar", "fx", "basis", "cost", "execution", "benchmark", "risk_free"})
_KEYS = frozenset({"schema", "hash_format", "kind", "id", "version", "payload"})
_SELECT = (
    "SELECT kind,convention_id,version,payload,content_hash FROM conventions "
    "WHERE kind=? AND convention_id=? AND version=?"
)


def _identity(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or re.search(r"[\x00-\x1f\x7f-\x9f]", value)
    ):
        raise ValueError("convention identity must be nonempty, trimmed and control-free")
    return value


def _kind(value: object) -> str:
    if not isinstance(value, str) or value not in _KINDS:
        raise ValueError("unsupported convention kind")
    return value


def _version(value: object) -> str:
    version = _identity(value)
    if version.casefold() == "latest":
        raise ValueError("convention version must be exact, not latest")
    return version


def _hash(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("convention hash must be lowercase SHA-256 hex")
    return value


@dataclass(frozen=True, slots=True)
class ConventionPin:
    kind: str
    id: str
    version: str
    hash: str

    def __post_init__(self) -> None:
        _kind(self.kind)
        _identity(self.id)
        _version(self.version)
        _hash(self.hash)


def _validate_document(document: object) -> tuple[str, str, str]:
    if not isinstance(document, dict) or document.keys() != _KEYS:
        raise ValueError("invalid convention envelope keys")
    if (
        document["schema"] != "aas-convention-v1"
        or document["hash_format"] != "aas-canonical-json-sha256-v1"
    ):
        raise ValueError("unsupported convention schema or hash format")
    kind = _kind(document["kind"])
    identity = _identity(document["id"])
    version = _version(document["version"])
    payload = document["payload"]
    if not isinstance(payload, dict):
        raise TypeError("convention payload must be an object")
    _identity(payload.get("schema"))
    if kind == "basis" and (
        payload.keys() != {"schema", "price_basis"}
        or payload["schema"] != "aas-basis-v1"
        or payload["price_basis"] not in ("capital", "total_return")
    ):
        raise ValueError("unsupported basis payload")
    return kind, identity, version


def _document(raw: bytes) -> tuple[ConventionPin, bytes]:
    if not isinstance(raw, bytes) or len(raw) > _MAX_BYTES:
        raise ValueError("convention must be bytes of at most 1 MiB")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError("convention UTF-8 must not have a BOM")
    # Literal NUL is invalid JSON text and enables the decoder's UTF-16/32 detection.
    if b"\x00" in raw:
        raise ValueError("convention JSON text must not contain literal NUL bytes")
    try:
        raw.decode("utf-8", errors="strict")
        document = decode_json(raw)
        kind, identity, version = _validate_document(document)
        canonical = canonical_json_bytes(document)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid convention document") from error
    if len(canonical) > _MAX_BYTES:
        raise ValueError("canonical convention exceeds 1 MiB")
    return ConventionPin(kind, identity, version, hashlib.sha256(canonical).hexdigest()), canonical


def _stored_document(row: sqlite3.Row) -> tuple[ConventionPin, bytes]:
    stored_pin = ConventionPin(row[0], row[1], row[2], row[4])
    try:
        raw = row[3].encode("utf-8")
    except UnicodeError as error:
        raise ValueError("invalid stored convention encoding") from error
    parsed_pin, canonical = _document(raw)
    if canonical != raw or parsed_pin != stored_pin:
        raise ValueError("stored convention is noncanonical or has mismatched identity/hash")
    return parsed_pin, canonical


def register_convention(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str
) -> ConventionPin:
    """Register one document; the file digest refers to exact incoming bytes."""
    _hash(expected_file_sha256)
    if not isinstance(raw, bytes) or len(raw) > _MAX_BYTES:
        raise ValueError("convention must be bytes of at most 1 MiB")
    if hashlib.sha256(raw).hexdigest() != expected_file_sha256:
        raise ValueError("convention file hash mismatch")
    pin, canonical = _document(raw)
    with atomic(connection):
        previous = connection.execute(_SELECT, (pin.kind, pin.id, pin.version)).fetchone()
        if previous is not None:
            if _stored_document(previous) != (pin, canonical):
                raise ValueError("convention ID/version already has different content")
        else:
            connection.execute(
                "INSERT INTO conventions(kind,convention_id,version,payload,content_hash) "
                "VALUES (?,?,?,?,?)",
                (pin.kind, pin.id, pin.version, canonical.decode("utf-8"), pin.hash),
            )
    return pin


def read_convention(connection: sqlite3.Connection, pin: ConventionPin) -> bytes:
    """Read canonical whole-document bytes using only the supplied state connection."""
    row = connection.execute(_SELECT, (pin.kind, pin.id, pin.version)).fetchone()
    if row is None:
        raise ValueError("convention ID/version is not registered")
    stored_pin, canonical = _stored_document(row)
    if stored_pin != pin:
        raise ValueError("convention hash does not match requested pin")
    return canonical


HASH_FORMAT = "aas-canonical-json-sha256-v1"
BUNDLE_SCHEMA = "aas-input-bundle-v1"
_DEFINITION_SCHEMAS = {
    "derived": "aas-derived-definition-v1",
    "membership": "aas-ensemble-membership-v1",
}
REF_FORMATS = {
    "generation": ("aas-generation-pin-v1", "aas-market-generation-chain-v1"),
    "identity": ("aas-identity-snapshot-v1", HASH_FORMAT),
    "universe": ("aas-universe-version-v1", HASH_FORMAT),
    **{kind: (schema, HASH_FORMAT) for kind, schema in _DEFINITION_SCHEMAS.items()},
    **{"convention:" + kind: ("aas-convention-v1", HASH_FORMAT) for kind in _KINDS},
}
_ROLES = {
    "signal_prices": ("generation", False),
    "execution_prices": ("generation", False),
    "sessions": ("generation", True),
    "identity": ("identity", True),
    "universe": ("universe", True),
    "membership": ("membership", True),
    "macro": ("generation", False),
    "derived": ("derived", False),
    "proxy": ("generation", False),
    **{kind: ("convention:" + kind, True) for kind in _KINDS},
}


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ValueError("pin text must be nonempty trimmed printable text")
    return value


def _exact_version(value: object) -> str:
    version = _text(value)
    if version == "latest":
        raise ValueError("pin version must be exact, not latest")
    return version


def _uint(value: object) -> int:
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("pin integer must be nonnegative int64, not bool or float")
    return value


def _object(value: object, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != keys:
        raise ValueError("pin document has missing or unknown fields")
    return cast("dict[str, object]", value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("pin document requires an array")
    return cast("list[object]", value)


def decode_pin_document(raw: bytes) -> dict[str, object]:
    """Shared bounded wire admission; existing hash/JSON codecs remain authoritative."""
    if type(raw) is not bytes or len(raw) > _MAX_BYTES:
        raise ValueError("pin document must be bytes of at most 1 MiB")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("pin document requires UTF-8 without BOM/NUL")
    try:
        raw.decode("utf-8", errors="strict")
        body = decode_json(raw)
        canonical = canonical_json_bytes(body)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid pin UTF-8 JSON document") from error
    if not isinstance(body, dict) or len(canonical) > _MAX_BYTES:
        raise ValueError("pin document must be a bounded object")
    return cast("dict[str, object]", body)


def _incoming(raw: bytes, expected: str) -> dict[str, object]:
    _hash(expected)
    if type(raw) is not bytes or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("pin incoming file hash mismatch")
    return decode_pin_document(raw)


def _admit_bytes(size: int, budget: ComputeBudget) -> None:
    if size > _MAX_BYTES or size * 128 + 65536 > budget.available_bytes:
        raise ComputeResourceError("pin document exceeds admitted materialization budget")


@dataclass(frozen=True, slots=True)
class DefinitionPin:
    kind: str
    id: str
    version: str
    hash: str

    def __post_init__(self) -> None:
        if self.kind not in _DEFINITION_SCHEMAS:
            raise ValueError("unsupported definition kind")
        _text(self.id)
        _exact_version(self.version)
        _hash(self.hash)


@dataclass(frozen=True, slots=True)
class InputBinding:
    role: str
    ordinal: int
    ref_kind: str
    ref_id: str
    ref_version: str
    hash: str

    def __post_init__(self) -> None:
        _text(self.role)
        _text(self.ref_kind)
        _text(self.ref_id)
        _uint(self.ordinal)
        _hash(self.hash)
        if self.role not in _ROLES or self.ref_kind != _ROLES[self.role][0]:
            raise ValueError("unsupported binding role/reference kind")
        if self.ref_kind.startswith("convention:"):
            _version(self.ref_version)
        else:
            _exact_version(self.ref_version)
        if self.ref_kind == "identity" and self.ref_version != "aas-identity-snapshot-v1":
            raise ValueError("identity binding version must be its format discriminator")


@dataclass(frozen=True, slots=True)
class InputBundleRef:
    bundle_id: str
    content_hash: str

    def __post_init__(self) -> None:
        _text(self.bundle_id)
        _hash(self.content_hash)


def binding_document(binding: InputBinding) -> dict[str, object]:
    schema, hash_format = REF_FORMATS[binding.ref_kind]
    return {**asdict(binding), "ref_schema": schema, "hash_format": hash_format}


def parse_bindings(value: object) -> tuple[InputBinding, ...]:
    """Validate before sorting; ordinals commit semantic assignment, not input order."""
    bindings: list[InputBinding] = []
    positions: set[tuple[str, int]] = set()
    references: set[tuple[str, str, str, str]] = set()
    for item in _array(value):
        row = _object(
            item,
            {
                "role",
                "ordinal",
                "ref_kind",
                "ref_id",
                "ref_version",
                "hash",
                "ref_schema",
                "hash_format",
            },
        )
        binding = InputBinding(
            _text(row["role"]),
            _uint(row["ordinal"]),
            _text(row["ref_kind"]),
            _text(row["ref_id"]),
            _text(row["ref_version"]),
            _hash(row["hash"]),
        )
        if (row["ref_schema"], row["hash_format"]) != REF_FORMATS[binding.ref_kind]:
            raise ValueError("binding schema/hash format mismatch")
        position = (binding.role, binding.ordinal)
        reference = (binding.role, binding.ref_kind, binding.ref_id, binding.ref_version)
        if position in positions or reference in references:
            raise ValueError("duplicate binding position or same-role reference")
        positions.add(position)
        references.add(reference)
        bindings.append(binding)
    bindings.sort(key=lambda item: (item.role, item.ordinal))
    next_ordinal: dict[str, int] = {}
    for binding in bindings:
        if binding.ordinal != next_ordinal.get(binding.role, 0) or (
            _ROLES[binding.role][1] and binding.ordinal != 0
        ):
            raise ValueError("binding ordinals must be contiguous; singleton role repeated")
        next_ordinal[binding.role] = binding.ordinal + 1
    return tuple(bindings)


def _generation(
    workspace: Workspace,
    reference: tuple[str, str, str],
    budget: ComputeBudget,
    supplied: GenerationPin | None = None,
) -> None:
    dataset, version, digest = reference
    row = workspace.state.execute(
        "SELECT generation_id,chain_hash,manifest_hash FROM dataset_versions "
        "WHERE dataset_id=? AND version=? AND status='committed'",
        (dataset, version),
    ).fetchone()
    if row is None:
        raise ValueError("generation reference is not registered")
    pin = GenerationPin(dataset, version, row[0], row[1], row[2])
    if pin.chain_hash != digest or (supplied is not None and supplied != pin):
        raise ValueError("generation pin mismatch")
    history = market.read_chain_rows(workspace.market, pin.generation_id, budget=budget)
    chain = market.generation_chain(workspace.market, pin.generation_id)
    for marker in chain:
        catalog = workspace.state.execute(
            "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash,row_count,"
            "parent_generation_id,sequence,record_schema FROM dataset_versions "
            "WHERE generation_id=? AND status='committed'",
            (marker["generation_id"],),
        ).fetchone()
        expected = tuple(
            marker[key]
            for key in (
                "dataset_id",
                "version",
                "generation_id",
                "chain_hash",
                "request_hash",
                "row_count",
                "parent_id",
                "sequence",
                "record_schema",
            )
        )
        if catalog is None or tuple(catalog) != expected or marker["dataset_id"] != dataset:
            raise ValueError("generation catalog/marker mismatch")
    if chain[-1]["domain"] == "feature_values":
        identities = {
            tuple(
                item[key]
                for key in ("contract_id", "contract_version", "contract_hash", "input_bundle_hash")
            )
            for item in history
        }
        if len(identities) != 1:
            raise ValueError("feature generation requires one unambiguous contract/input bundle")


def _verify_binding(workspace: Workspace, binding: InputBinding, budget: ComputeBudget) -> None:
    kind, identity, version, digest = (
        binding.ref_kind,
        binding.ref_id,
        binding.ref_version,
        binding.hash,
    )
    if kind == "generation":
        _generation(workspace, (identity, version, digest), budget)
    elif kind in ("identity", "universe"):
        read_membership_pins(
            workspace.state,
            IdentityPin(identity, digest) if kind == "identity" else None,
            UniversePin(identity, version, digest) if kind == "universe" else None,
            max_materialization_bytes=budget.available_bytes,
        )
    elif kind.startswith("convention:"):
        # read_convention decodes and re-canonicalizes the whole stored document,
        # so charge its bytes before reading rather than after materializing them.
        stored = workspace.state.execute(
            "SELECT coalesce(length(CAST(payload AS BLOB)),0) FROM conventions "
            "WHERE kind=? AND convention_id=? AND version=?",
            (kind.removeprefix("convention:"), identity, version),
        ).fetchone()
        _admit_bytes(stored[0] if stored is not None else 0, budget)
        read_convention(
            workspace.state,
            ConventionPin(kind.removeprefix("convention:"), identity, version, digest),
        )
    else:
        read_definition(workspace, DefinitionPin(kind, identity, version, digest), budget=budget)


def _generation_ref(value: object) -> GenerationPin:
    ref = _object(
        value, {"ref_kind", "ref_id", "ref_version", "hash", "schema", "hash_format", "pin"}
    )
    body = _object(
        ref["pin"], {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash"}
    )
    pin = GenerationPin(**{key: _text(item) for key, item in body.items()})
    if (
        ref["ref_kind"],
        ref["ref_id"],
        ref["ref_version"],
        ref["hash"],
        ref["schema"],
        ref["hash_format"],
    ) != ("generation", pin.dataset_id, pin.version, pin.chain_hash, *REF_FORMATS["generation"]):
        raise ValueError("derived generation descriptor mismatch")
    return pin


def _number(value: object) -> float:
    if type(value) not in (int, float):
        raise ValueError("number must be numeric, not bool")
    try:
        number = float(cast("int | float", value))
    except OverflowError as error:
        raise ValueError("number exceeds finite float") from error
    if not math.isfinite(number):
        raise ValueError("number must be finite")
    return number


def _derived(body: dict[str, object]) -> list[tuple[int, GenerationPin]]:
    spec = _object(body["definition"], {field.name for field in fields(DerivedSeriesSpec)})
    bindings: list[DerivedInputBinding] = []
    for value in _array(spec["input_bindings"]):
        row = _object(value, {"dataset_id", "dataset_version", "series", "field"})
        bindings.append(DerivedInputBinding(**{key: _text(item) for key, item in row.items()}))
    for key in ("consumes_capital", "consumes_totalreturn"):
        if type(spec[key]) is not bool:
            raise ValueError("derived consumption flag must be bool")
    model = DerivedSeriesSpec(
        series_id=_text(spec["series_id"]),
        operation=_text(spec["operation"]),
        trailing_months=_uint(spec["trailing_months"]),
        consumes_capital=cast("bool", spec["consumes_capital"]),
        consumes_totalreturn=cast("bool", spec["consumes_totalreturn"]),
        input_bindings=tuple(bindings),
        signal_lag_months=tuple(_uint(item) for item in _array(spec["signal_lag_months"])),
        signal_thresholds=tuple(_number(item) for item in _array(spec["signal_thresholds"])),
        reference_provenance=_text(spec["reference_provenance"]),
    )
    if body["id"] != model.series_id or _hash(spec["canonical_sha256"]) != model.canonical_sha256:
        raise ValueError("derived spec identity/hash mismatch")
    entries = [_object(item, {"ordinal", "field", "pin"}) for item in _array(body["inputs"])]
    entries.sort(key=lambda item: _uint(item["ordinal"]))
    if len(entries) != len(bindings):
        raise ValueError("derived inputs must match all spec bindings")
    pins: list[tuple[int, GenerationPin]] = []
    for ordinal, (entry, binding) in enumerate(zip(entries, bindings, strict=True)):
        pin = _generation_ref(entry["pin"])
        if entry["ordinal"] != ordinal or (entry["field"], pin.dataset_id, pin.version) != (
            binding.field,
            binding.dataset_id,
            binding.dataset_version,
        ):
            raise ValueError("derived transport ordinal/field/dataset mismatch")
        pins.append((ordinal, pin))
    body["inputs"] = entries
    return pins


def _definition(
    body: dict[str, object],
) -> tuple[DefinitionPin, bytes, list[tuple[int, GenerationPin]]]:
    kind = next(
        (kind for kind, schema in _DEFINITION_SCHEMAS.items() if body.get("schema") == schema), None
    )
    if kind is None:
        raise ValueError("unsupported definition schema")
    _object(
        body,
        {"schema", "hash_format", "id", "version"}
        | ({"definition", "inputs"} if kind == "derived" else {"membership_sha256", "rows"}),
    )
    if body["hash_format"] != HASH_FORMAT:
        raise ValueError("unsupported definition hash format")
    pins = []
    if kind == "derived":
        pins = _derived(body)
    else:
        rows = [_object(value, {"name", "weight"}) for value in _array(body["rows"])]
        try:
            membership = tuple(
                MembershipRow(_text(row["name"]), Decimal(_text(row["weight"]))) for row in rows
            )
            EnsembleMembership(membership, _hash(body["membership_sha256"]))
        except InvalidOperation as error:
            raise ValueError("invalid ensemble Decimal weight") from error
        body["rows"] = sorted(rows, key=lambda row: _text(row["name"]))
    canonical = canonical_json_bytes(body)
    pin = DefinitionPin(
        kind,
        _text(body["id"]),
        _exact_version(body["version"]),
        hashlib.sha256(canonical).hexdigest(),
    )
    return pin, canonical, pins


def read_definition(workspace: Workspace, pin: DefinitionPin, *, budget: ComputeBudget) -> bytes:
    size = workspace.state.execute(
        "SELECT length(CAST(definition AS BLOB)) FROM feature_contracts WHERE name=? AND version=?",
        (pin.id, pin.version),
    ).fetchone()
    if size is None:
        raise ValueError("definition is not registered")
    _admit_bytes(size[0], budget)
    row = workspace.state.execute(
        "SELECT definition,record_schema,content_hash FROM feature_contracts "
        "WHERE name=? AND version=?",
        (pin.id, pin.version),
    ).fetchone()
    raw = row[0].encode("utf-8")
    parsed, canonical, pins = _definition(decode_pin_document(raw))
    if (
        parsed != pin
        or raw != canonical
        or (row[1], row[2]) != (_DEFINITION_SCHEMAS[pin.kind], pin.hash)
    ):
        raise ValueError("stored definition canonical content/identity mismatch")
    count = workspace.state.execute(
        "SELECT count(*) FROM feature_inputs WHERE name=? AND version=?", (pin.id, pin.version)
    ).fetchone()[0]
    if count != len(pins):
        raise ValueError("stored definition child count mismatch")
    children = workspace.state.execute(
        "SELECT ordinal,ref_kind,ref_id,ref_version,content_hash FROM feature_inputs "
        "WHERE name=? AND version=? ORDER BY ordinal",
        (pin.id, pin.version),
    ).fetchall()
    if [tuple(row) for row in children] != [
        (ordinal, "generation", item.dataset_id, item.version, item.chain_hash)
        for ordinal, item in pins
    ]:
        raise ValueError("stored definition child mapping mismatch")
    for _, item in pins:
        _generation(workspace, (item.dataset_id, item.version, item.chain_hash), budget, item)
    return raw


def register_definition(
    workspace: Workspace, raw: bytes, *, expected_file_sha256: str, budget: ComputeBudget
) -> DefinitionPin:
    body = _incoming(raw, expected_file_sha256)
    pin, canonical, pins = _definition(body)
    _admit_bytes(len(canonical), budget)
    with atomic(workspace.state):
        previous = workspace.state.execute(
            "SELECT 1 FROM feature_contracts WHERE name=? AND version=?", (pin.id, pin.version)
        ).fetchone()
        if previous is None:
            for _, item in pins:
                _generation(
                    workspace, (item.dataset_id, item.version, item.chain_hash), budget, item
                )
            workspace.state.execute(
                "INSERT INTO feature_contracts(name,version,definition,record_schema,content_hash) "
                "VALUES (?,?,?,?,?)",
                (pin.id, pin.version, canonical.decode(), body["schema"], pin.hash),
            )
            workspace.state.executemany(
                "INSERT INTO feature_inputs VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        pin.id,
                        pin.version,
                        ordinal,
                        "generation",
                        item.dataset_id,
                        item.version,
                        item.chain_hash,
                    )
                    for ordinal, item in pins
                ],
            )
        if read_definition(workspace, pin, budget=budget) != canonical:
            raise ValueError("definition registration content mismatch")
    return pin


def _bundle(identity: str, bindings: tuple[InputBinding, ...]) -> bytes:
    return canonical_json_bytes(
        {
            "schema": BUNDLE_SCHEMA,
            "hash_format": HASH_FORMAT,
            "bundle_id": identity,
            "bindings": [binding_document(item) for item in bindings],
        }
    )


def read_input_bundle(workspace: Workspace, pin: InputBundleRef, *, budget: ComputeBudget) -> bytes:
    row = workspace.state.execute(
        "SELECT content_hash,record_schema FROM input_bundles WHERE bundle_id=?", (pin.bundle_id,)
    ).fetchone()
    if row is None or tuple(row) != (pin.content_hash, BUNDLE_SCHEMA):
        raise ValueError("input bundle header mismatch")
    size = workspace.state.execute(
        "SELECT count(*)*512+coalesce(sum(length(CAST("
        "role||ref_kind||ref_id||ref_version||content_hash AS BLOB))),0) "
        "FROM input_bindings WHERE bundle_id=?",
        (pin.bundle_id,),
    ).fetchone()[0]
    _admit_bytes(size + len(pin.bundle_id.encode()), budget)
    rows = workspace.state.execute(
        "SELECT role,ordinal,ref_kind,ref_id,ref_version,content_hash FROM input_bindings "
        "WHERE bundle_id=? ORDER BY role,ordinal",
        (pin.bundle_id,),
    ).fetchall()
    bindings = parse_bindings([binding_document(InputBinding(*row)) for row in rows])
    raw = _bundle(pin.bundle_id, bindings)
    if len(raw) > _MAX_BYTES or hashlib.sha256(raw).hexdigest() != pin.content_hash:
        raise ValueError("input bundle content mismatch")
    for binding in bindings:
        _verify_binding(workspace, binding, budget)
    return raw


def register_input_bundle(
    workspace: Workspace, raw: bytes, *, expected_file_sha256: str, budget: ComputeBudget
) -> InputBundleRef:
    body = _object(
        _incoming(raw, expected_file_sha256), {"schema", "hash_format", "bundle_id", "bindings"}
    )
    if (body["schema"], body["hash_format"]) != (BUNDLE_SCHEMA, HASH_FORMAT):
        raise ValueError("unsupported input bundle schema/hash format")
    bindings = parse_bindings(body["bindings"])
    canonical = _bundle(_text(body["bundle_id"]), bindings)
    _admit_bytes(len(canonical), budget)
    pin = InputBundleRef(_text(body["bundle_id"]), hashlib.sha256(canonical).hexdigest())
    with atomic(workspace.state):
        previous = workspace.state.execute(
            "SELECT 1 FROM input_bundles WHERE bundle_id=?", (pin.bundle_id,)
        ).fetchone()
        if previous is None:
            for binding in bindings:
                _verify_binding(workspace, binding, budget)
            workspace.state.execute(
                "INSERT INTO input_bundles VALUES (?,?,?)",
                (pin.bundle_id, pin.content_hash, BUNDLE_SCHEMA),
            )
            workspace.state.executemany(
                "INSERT INTO input_bindings VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        pin.bundle_id,
                        item.role,
                        item.ordinal,
                        item.ref_kind,
                        item.ref_id,
                        item.ref_version,
                        item.hash,
                    )
                    for item in bindings
                ],
            )
        if read_input_bundle(workspace, pin, budget=budget) != canonical:
            raise ValueError("input bundle registration mismatch")
    return pin


def _calendar_payload(payload: dict[str, object], definition: ExecutionDefinition) -> None:
    expected = asdict(definition.calendar)
    _object(payload, {"schema", "calendar_id", "venue", "timezone_version", *expected})
    if payload["schema"] != "aas-calendar-v1":
        raise ValueError("unsupported execution calendar schema")
    for key in ("calendar_id", "venue", "timezone_version"):
        _text(payload[key])
    for key in ("current_month_drop_before_day", "history_observations"):
        _uint(payload[key])
    if any(payload[key] != value for key, value in expected.items()):
        raise ValueError("calendar conventions disagree with definition")


def read_execution_conventions(
    state: sqlite3.Connection, pins: tuple[ConventionPin, ...], *, definition: ExecutionDefinition
) -> tuple[bytes, ...]:
    """Recognized convention semantics only; does not make a stored request executable."""
    if type(pins) is not tuple or len({pin.kind for pin in pins}) != len(pins):
        raise ValueError("execution conventions require unique kinds")
    if not set(definition.required_convention_roles) <= {pin.kind for pin in pins}:
        raise ValueError("missing required execution conventions")
    documents: list[bytes] = []
    for pin in sorted(pins, key=lambda item: item.kind):
        raw = read_convention(state, pin)
        payload = cast("dict[str, object]", decode_pin_document(raw)["payload"])
        if pin.kind == "calendar":
            _calendar_payload(payload, definition)
        elif pin.kind == "cost":
            _object(payload, {"schema", "model", "rate", "currency"})
            if (
                payload["schema"] != "aas-cost-v1"
                or payload["model"] != "proportional_traded_notional"
                or not 0 <= _number(payload["rate"]) < 1
            ):
                raise ValueError("unsupported execution cost")
            _text(payload["currency"])
        elif pin.kind == "execution":
            if payload != {
                "schema": "aas-execution-v1",
                "decision": "session_close",
                "execution": "next_session_open",
                "sizing": "fractional_long_only",
                "cash": "implicit_residual",
                "terminal": "mark_without_liquidation",
                "cashflows": "session_open_before_rebalance_existing_cash_withdrawals",
            }:
                raise ValueError("unsupported execution policy")
        elif (
            pin.kind == "basis"
            and payload["price_basis"] == "total_return"
            and any(requirement.basis == "capital" for requirement in definition.input_requirements)
        ):
            raise ValueError("capital-consuming definition requires capital basis")
        documents.append(raw)
    return tuple(documents)
