"""Pure, versioned request admission and exact legacy envelope export.

Application supplies verified content and compute identities. This module neither
resolves stored pins nor discovers a runtime, schedules decisions, or executes
accounting. Whole convention bytes retain their original JSON number identities.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from itertools import pairwise
from types import MappingProxyType
from typing import cast

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.engine.models import ENGINE_CONTRACT_VERSION_V1
from aegis_alpha.engine.requirements import ExecutionDefinition

type FrozenJSON = (
    bool | int | float | str | tuple[FrozenJSON, ...] | Mapping[str, FrozenJSON] | None
)
type EngineIdentity = Mapping[str, FrozenJSON]
type EnvironmentIdentity = Mapping[str, FrozenJSON]
type Record = dict[str, object]

_J = "aas-canonical-json-sha256-v1"
_B = "aas-sha256-bytes-v1"
_MINIMUM_SESSIONS = 2
_MAX_BYTES = 1024 * 1024
_MAX_ENVELOPE_BYTES = 64 * 1024 * 1024
_I64_MAX = 2**63 - 1
_CONVENTION_KINDS = ("calendar", "basis", "cost", "execution", "benchmark", "risk_free", "fx")
_LABELS = {
    "generation": ("aas-generation-pin-v1", "aas-market-generation-chain-v1"),
    "identity": ("aas-identity-snapshot-v1", _J),
    "universe": ("aas-universe-version-v1", _J),
    "derived": ("aas-derived-definition-v1", _J),
    "membership": ("aas-ensemble-membership-v1", _J),
    **{"convention:" + kind: ("aas-convention-v1", _J) for kind in _CONVENTION_KINDS},
}
_ROLE_KINDS = {
    "signal_prices": "generation",
    "execution_prices": "generation",
    "sessions": "generation",
    "identity": "identity",
    "universe": "universe",
    "membership": "membership",
    "macro": "generation",
    "derived": "derived",
    "proxy": "generation",
    **{kind: "convention:" + kind for kind in _CONVENTION_KINDS},
}
_MULTIPLE = frozenset({"signal_prices", "execution_prices", "macro", "derived", "proxy"})
_REQUIRED = frozenset(
    {
        "signal_prices",
        "execution_prices",
        "sessions",
        "identity",
        "universe",
        "membership",
        "calendar",
        "basis",
        "cost",
        "execution",
    }
)
_SELECTIONS = {
    "price_inputs": ("signal_prices", "execution_prices"),
    "macro_inputs": ("macro",),
    "derived_inputs": ("derived",),
    "proxy_rules": ("proxy",),
}
_VERSION_NAMES = frozenset({"python_implementation", "python_version"})
_INTEGER_SETTINGS = frozenset(
    {
        "float_radix",
        "float_mant_dig",
        "float_rounds",
        "decimal_precision",
        "decimal_emin",
        "decimal_emax",
        "decimal_capitals",
        "decimal_clamp",
    }
)
_SETTING_NAMES = _INTEGER_SETTINGS | {"decimal_rounding", "decimal_traps"}
_ROUNDINGS = (
    "ROUND_CEILING",
    "ROUND_DOWN",
    "ROUND_FLOOR",
    "ROUND_HALF_DOWN",
    "ROUND_HALF_EVEN",
    "ROUND_HALF_UP",
    "ROUND_UP",
    "ROUND_05UP",
)
_SIGNALS = frozenset(
    {
        "Clamped",
        "DivisionByZero",
        "FloatOperation",
        "Inexact",
        "InvalidOperation",
        "Overflow",
        "Rounded",
        "Subnormal",
        "Underflow",
    }
)
_EXECUTION = {
    "schema": "aas-execution-v1",
    "decision": "session_close",
    "execution": "next_session_open",
    "sizing": "fractional_long_only",
    "cash": "implicit_residual",
    "terminal": "mark_without_liquidation",
    "cashflows": "session_open_before_rebalance_existing_cash_withdrawals",
}


def _object(value: object) -> Record:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValueError("expected a JSON object with text keys")
    return dict(cast("Mapping[str, object]", value))


def _fields(value: object, fields: object) -> Record:
    row = _object(value)
    if row.keys() != fields:
        raise ValueError("missing or unknown fields")
    return row


def _array(value: object) -> tuple[object, ...]:
    if type(value) not in (list, tuple):
        raise ValueError("expected a JSON array")
    return tuple(cast("list[object] | tuple[object, ...]", value))


def _rows(value: object) -> tuple[Record, ...]:
    return tuple(_object(item) for item in _array(value))


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ValueError("expected nonempty trimmed printable scalar text")
    return value


def _hash(value: object) -> str:
    text = _text(value)
    if re.fullmatch(r"[0-9a-f]{64}", text) is None:
        raise ValueError("expected lowercase SHA-256 hex")
    return text


def _integer(value: object, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= _I64_MAX:
        raise ValueError("expected an int64 JSON integer with the required sign, not bool or float")
    return value


def _number(value: object) -> float:
    if type(value) not in (int, float):
        raise ValueError("expected a finite number, not bool")
    try:
        number = float(cast("int | float", value))
    except OverflowError as error:
        raise ValueError("number outside finite float range") from error
    if not math.isfinite(number):
        raise ValueError("expected a finite number")
    return number or 0.0


def _day(value: object) -> date:
    text = _text(value)
    result = date.fromisoformat(text)
    if result.isoformat() != text:
        raise ValueError("date must use YYYY-MM-DD")
    return result


def _freeze(value: object) -> FrozenJSON:
    if isinstance(value, Mapping):
        row = _object(value)
        return MappingProxyType({key: _freeze(item) for key, item in row.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, str):
        # Encoding, unlike normalization, also rejects escaped unpaired surrogates.
        value.encode("utf-8", errors="strict")
        return value
    if value is None or type(value) in (bool, int):
        return cast("FrozenJSON", value)
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError("expected finite JSON scalar values")


def _decode(raw: bytes) -> Record:
    if not isinstance(raw, bytes) or len(raw) > _MAX_BYTES:
        raise ValueError("request/convention must be bytes of at most 1 MiB")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("UTF-8 JSON must not contain BOM or literal NUL")
    try:
        raw.decode("utf-8", errors="strict")
        row = _object(decode_json(raw))
        _freeze(row)
        _bounded_bytes(row)
    except (UnicodeError, RecursionError) as error:
        raise ValueError("invalid UTF-8 JSON scalar document") from error
    return row


def _bounded_bytes(value: object, *, limit: int = _MAX_BYTES) -> bytes:
    raw = canonical_json_bytes(value)
    if len(raw) > limit:
        raise ValueError("canonical document exceeds byte limit")
    return raw


# These helpers compose the two concrete machine schemas only; they are not a
# schema validator/registry. Cross-reference and semantic admission is below.
def _shape(properties: Mapping[str, object]) -> Record:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _items(item: Mapping[str, object], *, minimum: int = 0) -> Record:
    return {"type": "array", "items": item, "minItems": minimum}


def _const(value: str) -> Record:
    return {"const": value}


def _nullable(schema: Record) -> Record:
    return {"anyOf": [schema, {"type": "null"}]}


_TEXT: Record = {
    "type": "string",
    "minLength": 1,
    "pattern": r"^(?!\s)(?![\s\S]*\s$)[^\x00-\x1f\x7f-\x9f\ud800-\udfff]+$",
}
_HASH: Record = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_UINT: Record = {"type": "integer", "minimum": 0, "maximum": _I64_MAX}
_INT: Record = {"type": "integer", "minimum": -(2**63), "maximum": _I64_MAX}
_NUMBER: Record = {"type": "number"}
_DATE: Record = {"type": "string", "format": "date", "pattern": r"^\d{4}-\d{2}-\d{2}$"}
_VERSION: Record = {**_TEXT, "not": _const("latest")}
_CONVENTION_VERSION: Record = {**_TEXT, "not": {"pattern": "^[lL][aA][tT][eE][sS][tT]$"}}
_KEY = _shape({"role": {"enum": list(_ROLE_KINDS)}, "ordinal": _UINT})
_STRATEGY = _shape(
    {
        "strategy_store_id": _TEXT,
        "strategy_id": _TEXT,
        "version": _VERSION,
        "raw_sha256": _HASH,
        "contract_sha256": _HASH,
        "schema": _const("aas-engine-bundle-v1"),
        "contract_version": _const(ENGINE_CONTRACT_VERSION_V1),
        "raw_hash_format": _const(_B),
        "contract_hash_format": _const(_J),
    }
)
_CONVENTION_PIN = _shape(
    {
        "kind": {"enum": list(_CONVENTION_KINDS)},
        "id": _TEXT,
        "version": _CONVENTION_VERSION,
        "hash": _HASH,
    }
)
_PIN_SHAPES = {
    "generation": _shape(
        {
            "dataset_id": _TEXT,
            "version": _VERSION,
            "generation_id": _TEXT,
            "chain_hash": _HASH,
            "manifest_hash": _HASH,
        }
    ),
    "identity": _shape({"snapshot_id": _TEXT, "content_hash": _HASH}),
    "universe": _shape({"universe_id": _TEXT, "version": _VERSION, "content_hash": _HASH}),
    **{
        kind: _shape({"kind": _const(kind), "id": _TEXT, "version": _VERSION, "hash": _HASH})
        for kind in ("derived", "membership")
    },
    **{
        "convention:" + kind: _shape(
            {"kind": _const(kind), "id": _TEXT, "version": _CONVENTION_VERSION, "hash": _HASH}
        )
        for kind in _CONVENTION_KINDS
    },
}


def _ref_properties(kind: str) -> Record:
    schema, fmt = _LABELS[kind]
    version = _CONVENTION_VERSION if kind.startswith("convention:") else _VERSION
    return {
        "ref_kind": _const(kind),
        "ref_id": _TEXT,
        "ref_version": _const(schema) if kind == "identity" else version,
        "hash": _HASH,
        "hash_format": _const(fmt),
    }


_REF = {
    "oneOf": [
        _shape(
            {**_ref_properties(kind), "schema": _const(_LABELS[kind][0]), "pin": _PIN_SHAPES[kind]}
        )
        for kind in _LABELS
    ]
}
_BINDING = {
    "oneOf": [
        _shape(
            {
                **_ref_properties(kind),
                "role": _const(role),
                "ordinal": _UINT if role in _MULTIPLE else {"const": 0},
                "ref_schema": _const(_LABELS[kind][0]),
            }
        )
        for role, kind in _ROLE_KINDS.items()
    ]
}
_BINDINGS = {
    **_items(_BINDING),
    "allOf": [
        {
            "contains": {
                "type": "object",
                "properties": {"role": _const(role)},
                "required": ["role"],
            },
            "minContains": 1,
            **({} if role in _MULTIPLE else {"maxContains": 1}),
        }
        for role in sorted(_REQUIRED)
    ],
}
_CONTENT_PROPERTIES = {
    "strategy": _STRATEGY,
    "bindings": _BINDINGS,
    "refs": _items(_REF, minimum=1),
    "price_inputs": _items(
        _shape(
            {
                "binding": _KEY,
                "instrument_ids": {**_items(_TEXT, minimum=1), "uniqueItems": True},
                "currency": _TEXT,
                "basis": {"enum": ["unadjusted", "split_adjusted", "total_return"]},
                "price_role": {"enum": ["canonical", "reference"]},
                "interval": _const("1d"),
            }
        ),
        minimum=2,
    ),
    "macro_inputs": _items(_shape({"binding": _KEY, "series_id": _TEXT, "unit": _TEXT})),
    "derived_inputs": _items(_shape({"binding": _KEY, "series_id": _TEXT})),
    "proxy_rules": _items(_shape({"binding": _KEY, "logical_exposure_id": _TEXT})),
    "period": _shape({"start": _DATE, "end": _DATE}),
    "history": _shape({"start": _DATE, "end": _DATE}),
    "cutoff": _shape(
        {
            "mode": {"enum": ["strict_pit", "observed_snapshot_research"]},
            "knowledge_cutoff_us": _UINT,
            "ingestion_cutoff_us": _nullable(_UINT),
        }
    ),
    "decision_latency_us": _UINT,
    "explicit_decision_dates": _nullable({**_items(_DATE), "uniqueItems": True}),
    "account": _shape(
        {
            "currency": _TEXT,
            "initial_cash": {**_NUMBER, "exclusiveMinimum": 0},
            "cashflows": _items(
                _shape({"date": _DATE, "amount": {**_NUMBER, "not": {"const": 0}}})
            ),
        }
    ),
    "comparison": _shape(
        {
            role: _nullable(_shape({"role": _const(role), "ordinal": {"const": 0}}))
            for role in ("benchmark", "risk_free", "fx")
        }
    ),
    "envelope": _shape(
        {
            "schema_version": {"enum": ["aas-etf-backtest-v1", "aas-etf-backtest-v2"]},
            "research_mode": {"enum": ["synthetic", "observed_etf_research"]},
        }
    ),
}
_ENGINE = _shape(
    {
        "schema": _const("aas-engine-identity-v1"),
        "hash_format": _const(_J),
        "package_version": _TEXT,
        "contract_version": _const(ENGINE_CONTRACT_VERSION_V1),
        "calculation_source_hash": _HASH,
    }
)
_ENVIRONMENT = _shape(
    {
        "schema": _const("aas-environment-identity-v1"),
        "hash_format": _const(_J),
        "versions": {
            **_items(_shape({"name": {"enum": sorted(_VERSION_NAMES)}, "version": _TEXT})),
            "minItems": len(_VERSION_NAMES),
            "maxItems": len(_VERSION_NAMES),
            "uniqueItems": True,
        },
        "settings": {
            **_items(
                {
                    "oneOf": [
                        _shape({"name": {"enum": sorted(_INTEGER_SETTINGS)}, "value": _INT}),
                        _shape(
                            {
                                "name": _const("decimal_rounding"),
                                "value": {"enum": list(_ROUNDINGS)},
                            }
                        ),
                        _shape({"name": _const("decimal_traps"), "value": {"type": "string"}}),
                    ]
                }
            ),
            "minItems": len(_SETTING_NAMES),
            "maxItems": len(_SETTING_NAMES),
            "uniqueItems": True,
        },
    }
)
_CONVENTION_EVIDENCE = _shape(
    {
        "pin": _CONVENTION_PIN,
        "schema": _const("aas-convention-v1"),
        "hash_format": _const(_J),
        "payload_hash": _HASH,
    }
)
_P_PROPERTIES = {
    "schema": _const("aas-backtest-request-v1"),
    "hash_format": _const(_J),
    **_CONTENT_PROPERTIES,
    "conventions": _items(_CONVENTION_EVIDENCE, minimum=4),
    "engine": _ENGINE,
    "environment": _ENVIRONMENT,
}
_PREPARE_PROPERTIES = {
    "schema": _const("aas-prepare-request-v1"),
    "hash_format": _const(_J),
    **_CONTENT_PROPERTIES,
    "metadata": _shape({"created_at_us": _nullable(_UINT)}),
}
_V1_NO_FLOWS = {
    "if": {
        "properties": {
            "envelope": {"properties": {"schema_version": _const("aas-etf-backtest-v1")}}
        }
    },
    "then": {"properties": {"account": {"properties": {"cashflows": {"maxItems": 0}}}}},
}
PREPARE_REQUEST_SCHEMA = cast(
    "Mapping[str, FrozenJSON]",
    _freeze(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            **_shape(_PREPARE_PROPERTIES),
            "allOf": [_V1_NO_FLOWS],
        }
    ),
)
BACKTEST_REQUEST_SCHEMA = cast(
    "Mapping[str, FrozenJSON]",
    _freeze(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            **_shape(_P_PROPERTIES),
            "allOf": [_V1_NO_FLOWS],
        }
    ),
)


def _version(value: object, *, convention: bool = False) -> str:
    text = _text(value)
    if (text.casefold() if convention else text) == "latest":
        raise ValueError("version must be explicit, not latest")
    return text


def _strategy(value: object) -> None:
    row = _fields(value, _object(_STRATEGY["properties"]).keys())
    for key in ("strategy_store_id", "strategy_id"):
        _text(row[key])
    _version(row["version"])
    for key in ("raw_sha256", "contract_sha256"):
        _hash(row[key])
    if (
        row["schema"],
        row["contract_version"],
        row["raw_hash_format"],
        row["contract_hash_format"],
    ) != ("aas-engine-bundle-v1", ENGINE_CONTRACT_VERSION_V1, _B, _J):
        raise ValueError("unsupported strategy identity")


def _ref_key(row: Record) -> tuple[str, str, str]:
    return _text(row["ref_kind"]), _text(row["ref_id"]), _text(row["ref_version"])


def _binding_key(row: Record) -> tuple[str, int]:
    return _text(row["role"]), _integer(row["ordinal"])


def _key(value: object) -> tuple[str, int]:
    return _binding_key(_fields(value, {"role", "ordinal"}))


def _ref(value: object) -> Record:
    row = _fields(
        value, {"ref_kind", "ref_id", "ref_version", "hash", "schema", "hash_format", "pin"}
    )
    kind, identity, version = _ref_key(row)
    if kind not in _LABELS or (row["schema"], row["hash_format"]) != _LABELS[kind]:
        raise ValueError("unsupported reference kind/schema/hash format")
    digest = _hash(row["hash"])
    pin = _fields(row["pin"], _object(_PIN_SHAPES[kind]["properties"]).keys())
    if kind == "generation":
        expected = (pin["dataset_id"], pin["version"], pin["chain_hash"])
        _text(pin["generation_id"])
        _hash(pin["manifest_hash"])
    elif kind == "identity":
        expected = (pin["snapshot_id"], "aas-identity-snapshot-v1", pin["content_hash"])
    elif kind == "universe":
        expected = (pin["universe_id"], pin["version"], pin["content_hash"])
    else:
        expected_kind = kind.removeprefix("convention:")
        if pin["kind"] != expected_kind:
            raise ValueError("reference kind disagrees with pin")
        expected = (pin["id"], pin["version"], pin["hash"])
    if (identity, version, digest) != expected:
        raise ValueError("reference identity disagrees with complete pin")
    if kind != "identity":
        _version(version, convention=kind.startswith("convention:"))
    return row


def _binding(value: object, refs: dict[tuple[str, str, str], Record]) -> Record:
    row = _fields(
        value,
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
    role, ordinal = _binding_key(row)
    if role not in _ROLE_KINDS or row["ref_kind"] != _ROLE_KINDS[role]:
        raise ValueError("unsupported role/reference pairing")
    if role not in _MULTIPLE and ordinal != 0:
        raise ValueError("singleton role requires ordinal zero")
    ref_key = _ref_key(row)
    if ref_key not in refs:
        raise ValueError("binding has no reference descriptor")
    ref = refs[ref_key]
    if (row["hash"], row["ref_schema"], row["hash_format"]) != (
        ref["hash"],
        ref["schema"],
        ref["hash_format"],
    ):
        raise ValueError("binding labels/hash disagree with reference")
    return row


def _bindings_and_refs(body: Record) -> dict[tuple[str, int], Record]:
    refs: dict[tuple[str, str, str], Record] = {}
    for item in _array(body["refs"]):
        ref = _ref(item)
        key = _ref_key(ref)
        if key in refs:
            raise ValueError("duplicate reference descriptor")
        refs[key] = ref
    bindings: dict[tuple[str, int], Record] = {}
    role_refs: set[tuple[str, tuple[str, str, str]]] = set()
    used: set[tuple[str, str, str]] = set()
    for item in _array(body["bindings"]):
        row = _binding(item, refs)
        role, _ = key = _binding_key(row)
        ref_key = _ref_key(row)
        if key in bindings or (role, ref_key) in role_refs:
            raise ValueError("duplicate binding key or same-role reference")
        bindings[key] = row
        role_refs.add((role, ref_key))
        used.add(ref_key)
    roles = {role for role, _ in bindings}
    if not roles >= _REQUIRED:
        raise ValueError("missing required executable bindings")
    if refs.keys() != used:
        raise ValueError("unused reference descriptors")
    for role in roles:
        ordinals = sorted(ordinal for selected_role, ordinal in bindings if selected_role == role)
        if ordinals != list(range(len(ordinals))):
            raise ValueError("binding ordinals must be contiguous from zero")
    body["bindings"] = [bindings[key] for key in sorted(bindings)]
    body["refs"] = [refs[key] for key in sorted(refs)]
    return bindings


def _selection_names(row: Record, field: str, binding: Record) -> tuple[str, ...]:
    if field != "price_inputs":
        names = (_text(row["logical_exposure_id" if field == "proxy_rules" else "series_id"]),)
        if field == "macro_inputs":
            _text(row["unit"])
        if field == "derived_inputs" and binding["ref_id"] != names[0]:
            raise ValueError("derived selection must match definition series ID")
        return names
    names = tuple(_text(item) for item in _array(row["instrument_ids"]))
    if not names or len(set(names)) != len(names):
        raise ValueError("price instruments must be nonempty and unique")
    row["instrument_ids"] = sorted(names)
    _text(row["currency"])
    if (
        row["basis"] not in ("unadjusted", "split_adjusted", "total_return")
        or row["price_role"] not in ("canonical", "reference")
        or row["interval"] != "1d"
    ):
        raise ValueError("unsupported price selection")
    if binding["role"] == "execution_prices" and (row["basis"], row["price_role"]) != (
        "unadjusted",
        "canonical",
    ):
        raise ValueError("execution requires canonical unadjusted prices")
    return names


def _selections(body: Record, bindings: dict[tuple[str, int], Record]) -> None:
    for field, roles in _SELECTIONS.items():
        selected: dict[tuple[str, int], Record] = {}
        identities: set[tuple[str, str]] = set()
        for row in _rows(body[field]):
            expected = (
                {"binding", "instrument_ids", "currency", "basis", "price_role", "interval"}
                if field == "price_inputs"
                else {"binding", "logical_exposure_id" if field == "proxy_rules" else "series_id"}
            )
            if field == "macro_inputs":
                expected.add("unit")
            _fields(row, expected)
            key = _key(row["binding"])
            if key not in bindings or key[0] not in roles or key in selected:
                raise ValueError("selection must address its own unique binding")
            names = _selection_names(row, field, bindings[key])
            for name in names:
                identity = key[0], name
                if identity in identities:
                    raise ValueError("overlapping selection identities within a role")
                identities.add(identity)
            selected[key] = row
        if selected.keys() != {key for key in bindings if key[0] in roles}:
            raise ValueError("each selected-input binding requires exactly one selection")
        body[field] = [selected[key] for key in sorted(selected)]


def _account(value: object, start: date, end: date, envelope: Record) -> Record:
    account = _fields(value, {"currency", "initial_cash", "cashflows"})
    _text(account["currency"])
    initial_cash = _number(account["initial_cash"])
    account["initial_cash"] = initial_cash
    if initial_cash <= 0:
        raise ValueError("initial cash must be positive")
    flows: list[Record] = []
    previous = start
    for item in _array(account["cashflows"]):
        flow = _fields(item, {"date", "amount"})
        day = _day(flow["date"])
        amount = _number(flow["amount"])
        if not previous < day <= end or amount == 0:
            raise ValueError("cashflows must be nonzero and unique increasing after baseline")
        previous = day
        flows.append({"date": day.isoformat(), "amount": amount})
    if envelope["schema_version"] == "aas-etf-backtest-v1" and flows:
        raise ValueError("v1 requires explicitly empty cashflows")
    account["cashflows"] = flows
    return account


def _account_and_dates(body: Record) -> None:
    period = _fields(body["period"], {"start", "end"})
    history = _fields(body["history"], {"start", "end"})
    start, end = _day(period["start"]), _day(period["end"])
    hstart, hend = _day(history["start"]), _day(history["end"])
    if not hstart <= start < end or hstart > hend:
        raise ValueError("invalid period/history ordering")
    cutoff = _fields(body["cutoff"], {"mode", "knowledge_cutoff_us", "ingestion_cutoff_us"})
    if cutoff["mode"] not in ("strict_pit", "observed_snapshot_research"):
        raise ValueError("unsupported cutoff mode")
    _integer(cutoff["knowledge_cutoff_us"])
    if cutoff["ingestion_cutoff_us"] is not None:
        _integer(cutoff["ingestion_cutoff_us"])
    _integer(body["decision_latency_us"])
    explicit = body["explicit_decision_dates"]
    if explicit is not None:
        days = tuple(_day(item) for item in _array(explicit))
        if any(a >= b for a, b in pairwise(days)) or any(not start <= day < end for day in days):
            raise ValueError(
                "explicit decisions must be unique increasing in-period dates before terminal"
            )
    envelope = _fields(body["envelope"], {"schema_version", "research_mode"})
    if envelope["schema_version"] not in ("aas-etf-backtest-v1", "aas-etf-backtest-v2") or envelope[
        "research_mode"
    ] not in ("synthetic", "observed_etf_research"):
        raise ValueError("unsupported legacy envelope provenance")
    body["account"] = _account(body["account"], start, end, envelope)
    metadata = _fields(body["metadata"], {"created_at_us"})
    if metadata["created_at_us"] is not None:
        _integer(metadata["created_at_us"])


def _prepare(document: object) -> Record:
    body = _fields(document, _PREPARE_PROPERTIES.keys())
    if (body["schema"], body["hash_format"]) != ("aas-prepare-request-v1", _J):
        raise ValueError("unsupported prepare request schema/hash format")
    _strategy(body["strategy"])
    bindings = _bindings_and_refs(body)
    _selections(body, bindings)
    _account_and_dates(body)
    comparison = _fields(body["comparison"], {"benchmark", "risk_free", "fx"})
    for role, value in comparison.items():
        if value is None:
            if (role, 0) in bindings:
                raise ValueError("null comparison forbids corresponding binding")
        elif _key(value) != (role, 0) or (role, 0) not in bindings:
            raise ValueError("comparison must address its matching convention")
    _bounded_bytes(body)
    return body


@dataclass(frozen=True, slots=True)
class ParsedPrepareRequest:
    """Validated, normalized detached JSON; caller mappings are never retained."""

    document: Mapping[str, FrozenJSON]

    def __post_init__(self) -> None:
        object.__setattr__(self, "document", _freeze(_prepare(self.document)))


def parse_prepare_request(raw: bytes) -> ParsedPrepareRequest:
    return ParsedPrepareRequest(cast("Mapping[str, FrozenJSON]", _freeze(_decode(raw))))


def _engine_identity(value: object) -> Record:
    row = _fields(value, _object(_ENGINE["properties"]).keys())
    if (row["schema"], row["hash_format"], row["contract_version"]) != (
        "aas-engine-identity-v1",
        _J,
        ENGINE_CONTRACT_VERSION_V1,
    ):
        raise ValueError("unsupported engine identity")
    _text(row["package_version"])
    _hash(row["calculation_source_hash"])
    return row


def _environment_value(name: str, value: object) -> None:
    if name in _INTEGER_SETTINGS:
        _integer(value, minimum=-(2**63))
    elif name == "decimal_traps":
        if not isinstance(value, str):
            raise ValueError("decimal traps must be a string")
        signals = value.split(",") if value else []
        if signals != sorted(set(signals)) or not set(signals) <= _SIGNALS:
            raise ValueError("decimal traps must be unique sorted signal names")
    elif name == "decimal_rounding":
        if value not in _ROUNDINGS:
            raise ValueError("unsupported Decimal rounding mode")
    else:
        _text(value)


def _environment_identity(value: object) -> Record:
    row = _fields(value, {"schema", "hash_format", "versions", "settings"})
    if (row["schema"], row["hash_format"]) != ("aas-environment-identity-v1", _J):
        raise ValueError("unsupported environment identity")
    for field, names, value_key in (
        ("versions", _VERSION_NAMES, "version"),
        ("settings", _SETTING_NAMES, "value"),
    ):
        by_name: dict[str, Record] = {}
        for item in _array(row[field]):
            entry = _fields(item, {"name", value_key})
            name = _text(entry["name"])
            if name not in names or name in by_name:
                raise ValueError("duplicate or unknown environment identity name")
            _environment_value(name, entry[value_key])
            by_name[name] = entry
        if by_name.keys() != names:
            raise ValueError("incomplete environment identity")
        row[field] = [by_name[name] for name in sorted(by_name)]
    return row


def _convention(raw: bytes) -> Record:
    row = _fields(_decode(raw), {"schema", "hash_format", "kind", "id", "version", "payload"})
    if (row["schema"], row["hash_format"]) != ("aas-convention-v1", _J) or row[
        "kind"
    ] not in _CONVENTION_KINDS:
        raise ValueError("unsupported convention document")
    _text(row["id"])
    _version(row["version"], convention=True)
    payload = _object(row["payload"])
    _text(payload.get("schema"))
    if canonical_json_bytes(row) != raw:
        raise ValueError("convention evidence must be exact canonical bytes")
    return row


def _convention_pin(row: Record) -> Record:
    return {
        "kind": row["kind"],
        "id": row["id"],
        "version": row["version"],
        "hash": content_sha256(row),
    }


def _cost(row: Record, currency: object) -> float:
    payload = _fields(row["payload"], {"schema", "model", "rate", "currency"})
    if (row["kind"], payload["schema"], payload["model"], payload["currency"]) != (
        "cost",
        "aas-cost-v1",
        "proportional_traded_notional",
        currency,
    ):
        raise ValueError("unsupported cost model or account currency disagreement")
    rate = _number(payload["rate"])
    if not 0 <= rate < 1:
        raise ValueError("cost rate must be in [0, 1)")
    return rate


def _calendar_payload(payload: object, definition: ExecutionDefinition) -> None:
    calendar_fields = asdict(definition.calendar)
    calendar = _fields(
        payload, {"schema", "calendar_id", "venue", "timezone_version", *calendar_fields}
    )
    if calendar["schema"] != "aas-calendar-v1":
        raise ValueError("unsupported calendar semantics")
    for key in ("calendar_id", "venue", "timezone_version"):
        _text(calendar[key])
    for key, value in calendar_fields.items():
        if isinstance(value, int):
            _integer(calendar[key])
        else:
            _text(calendar[key])
        if calendar[key] != value:
            raise ValueError("calendar does not match strategy conventions")


def _basis_prices(payload: object, body: Record, definition: ExecutionDefinition) -> None:
    basis = _fields(payload, {"schema", "price_basis"})
    if basis["schema"] != "aas-basis-v1" or basis["price_basis"] not in ("capital", "total_return"):
        raise ValueError("unsupported basis convention")
    for requirement in definition.input_requirements:
        if requirement.basis is not None and requirement.basis != basis["price_basis"]:
            raise ValueError("basis disagrees with execution requirement")
    expected_basis = "split_adjusted" if basis["price_basis"] == "capital" else "total_return"
    account = _object(body["account"])
    for selection in _rows(body["price_inputs"]):
        role, _ = _key(selection["binding"])
        if selection["currency"] != account["currency"]:
            raise ValueError("cross-currency prices are unsupported")
        if role == "signal_prices" and selection["basis"] != expected_basis:
            raise ValueError("signal basis disagrees with convention")


def _conventions(
    body: Record, docs: tuple[bytes, ...], definition: ExecutionDefinition
) -> tuple[list[Record], bytes, float]:
    refs = {
        str(ref["ref_kind"]).removeprefix("convention:"): ref
        for ref in _rows(body["refs"])
        if str(ref["ref_kind"]).startswith("convention:")
    }
    supplied: dict[str, Record] = {}
    raw_by_kind: dict[str, bytes] = {}
    for raw in docs:
        doc = _convention(raw)
        kind = _text(doc["kind"])
        if kind in supplied or kind not in refs or _convention_pin(doc) != refs[kind]["pin"]:
            raise ValueError("duplicate, extra or mismatched whole convention pin")
        supplied[kind] = doc
        raw_by_kind[kind] = raw
    if (
        supplied.keys() != refs.keys()
        or not set(definition.required_convention_roles) <= supplied.keys()
    ):
        raise ValueError("missing required convention documents")
    _calendar_payload(supplied["calendar"]["payload"], definition)
    _basis_prices(supplied["basis"]["payload"], body, definition)
    account = _object(body["account"])
    if _object(supplied["execution"]["payload"]) != _EXECUTION:
        raise ValueError("unsupported execution convention")
    rate = _cost(supplied["cost"], account["currency"])
    evidence: list[Record] = [
        {
            "pin": _convention_pin(supplied[kind]),
            "schema": "aas-convention-v1",
            "hash_format": _J,
            "payload_hash": content_sha256(supplied[kind]["payload"]),
        }
        for kind in sorted(supplied)
    ]
    return evidence, raw_by_kind["cost"], rate


def _definition(body: Record, definition: ExecutionDefinition) -> None:
    strategy = _object(body["strategy"])
    if (
        strategy["strategy_id"],
        strategy["version"],
        strategy["raw_sha256"],
        strategy["contract_sha256"],
    ) != (
        definition.bundle_id,
        definition.bundle_version,
        definition.source_sha256,
        definition.contract_sha256,
    ):
        raise ValueError("strategy and execution definition identity disagree")
    for requirement in definition.input_requirements:
        if requirement.role not in ("prices", "macro", "derived", "membership"):
            raise ValueError("unsupported execution requirement role")
    for role in ("macro", "derived"):
        required = {
            identity
            for requirement in definition.input_requirements
            if requirement.role == role
            for identity in requirement.identifiers
        }
        if {str(row["series_id"]) for row in _rows(body[role + "_inputs"])} != required:
            raise ValueError("unrequired macro/derived selection")
    proxy = {str(row["logical_exposure_id"]) for row in _rows(body["proxy_rules"])}
    prices = {
        role: {
            str(identity)
            for row in _rows(body["price_inputs"])
            if _key(row["binding"])[0] == role
            for identity in _array(row["instrument_ids"])
        }
        for role in ("signal_prices", "execution_prices")
    }
    if proxy & prices["signal_prices"] or prices["signal_prices"] | proxy != set(
        definition.price_asset_ids
    ):
        raise ValueError("explicit signal instruments/proxy exposures must match strategy assets")
    needed = set(definition.asset_ids) - set(definition.cash_asset_ids) - proxy
    if not needed <= prices["execution_prices"] or (
        not proxy and prices["execution_prices"] != needed
    ):
        raise ValueError("explicit execution instruments must match non-cash strategy assets")
    if prices["execution_prices"] & set(definition.cash_asset_ids):
        raise ValueError("cash is implicit residual, not an execution instrument")


def request_projection(
    request: ParsedPrepareRequest,
    *,
    definition: ExecutionDefinition,
    convention_documents: tuple[bytes, ...],
    engine_identity: EngineIdentity,
    environment_identity: EnvironmentIdentity,
) -> RequestProjection:
    body = _object(request.document)
    _definition(body, definition)
    evidence, cost_document, rate = _conventions(body, convention_documents, definition)
    projection = {key: body[key] for key in _CONTENT_PROPERTIES}
    projection.update(
        schema="aas-backtest-request-v1",
        hash_format=_J,
        conventions=evidence,
        engine=_engine_identity(engine_identity),
        environment=_environment_identity(environment_identity),
    )
    return RequestProjection(
        _bounded_bytes(projection), content_sha256(projection), rate, cost_document
    )


def _projection_request(body: Record) -> ParsedPrepareRequest:
    _fields(body, _P_PROPERTIES.keys())
    if (body["schema"], body["hash_format"]) != ("aas-backtest-request-v1", _J):
        raise ValueError("unsupported semantic request schema/hash format")
    request = {key: body[key] for key in _CONTENT_PROPERTIES}
    request.update(
        schema="aas-prepare-request-v1", hash_format=_J, metadata={"created_at_us": None}
    )
    return ParsedPrepareRequest(cast("Mapping[str, FrozenJSON]", _freeze(request)))


def _projection_evidence(body: Record) -> dict[str, Record]:
    refs = {
        str(ref["ref_kind"]).removeprefix("convention:"): ref["pin"]
        for ref in _rows(body["refs"])
        if str(ref["ref_kind"]).startswith("convention:")
    }
    evidence: dict[str, Record] = {}
    for item in _array(body["conventions"]):
        row = _fields(item, {"pin", "schema", "hash_format", "payload_hash"})
        pin = _fields(row["pin"], {"kind", "id", "version", "hash"})
        kind = _text(pin["kind"])
        _hash(row["payload_hash"])
        if (
            (row["schema"], row["hash_format"]) != ("aas-convention-v1", _J)
            or kind in evidence
            or refs.get(kind) != pin
        ):
            raise ValueError("invalid convention projection evidence")
        evidence[kind] = row
    if evidence.keys() != refs.keys() or list(evidence) != sorted(evidence):
        raise ValueError("convention projection must be complete and sorted")
    return evidence


@dataclass(frozen=True, slots=True)
class RequestProjection:
    canonical_bytes: bytes
    request_hash: str
    execution_cost: float
    cost_document: bytes

    def __post_init__(self) -> None:
        body = _decode(self.canonical_bytes)
        request = _projection_request(body)
        normalized = _object(request.document)
        for key in _CONTENT_PROPERTIES:
            if canonical_json_bytes(body[key]) != canonical_json_bytes(normalized[key]):
                raise ValueError("projection request fields are not normalized")
        for key, normalize in (
            ("engine", _engine_identity),
            ("environment", _environment_identity),
        ):
            if canonical_json_bytes(body[key]) != canonical_json_bytes(normalize(body[key])):
                raise ValueError("projection identity is not normalized")
        if _bounded_bytes(body) != self.canonical_bytes or content_sha256(body) != _hash(
            self.request_hash
        ):
            raise ValueError("projection must have exact canonical bytes and matching request hash")
        evidence = _projection_evidence(body)
        cost = _convention(self.cost_document)
        if (
            _convention_pin(cost) != evidence["cost"]["pin"]
            or content_sha256(cost["payload"]) != evidence["cost"]["payload_hash"]
        ):
            raise ValueError("cost document does not match projected pin/payload evidence")
        rate = _cost(cost, _object(body["account"])["currency"])
        if _number(self.execution_cost) != rate:
            raise ValueError("execution cost differs from validated cost document")
        object.__setattr__(self, "execution_cost", rate)


def parse_backtest_request(
    raw: bytes, *, definition: ExecutionDefinition, convention_documents: tuple[bytes, ...]
) -> tuple[ParsedPrepareRequest, RequestProjection]:
    body = _decode(raw)
    request = _projection_request(body)
    projection = request_projection(
        request,
        definition=definition,
        convention_documents=convention_documents,
        engine_identity=cast("EngineIdentity", body["engine"]),
        environment_identity=cast("EnvironmentIdentity", body["environment"]),
    )
    if projection.canonical_bytes != raw:
        raise ValueError("stored semantic projection is not exact validated canonical P")
    return request, projection


@dataclass(frozen=True, slots=True)
class EnvelopeExport:
    canonical_bytes: bytes
    envelope_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.canonical_bytes, bytes)
            or len(self.canonical_bytes) > _MAX_ENVELOPE_BYTES
        ):
            raise ValueError("envelope must be bytes within the legacy 64 MiB limit")
        if hashlib.sha256(self.canonical_bytes).hexdigest() != _hash(self.envelope_sha256):
            raise ValueError("envelope byte hash mismatch")


def _price_rows(rows: tuple[Mapping[str, float], ...], selected: set[str]) -> list[Record]:
    result: list[Record] = []
    for row in rows:
        parsed: Record = {}
        for symbol, value in _object(row).items():
            _text(symbol)
            number = _number(value)
            if symbol not in selected or number <= 0:
                raise ValueError("price requires a selected instrument and positive finite value")
            parsed[symbol] = number
        result.append(parsed)
    return result


def _source_pins(pins: tuple[Mapping[str, str], ...]) -> list[Record]:
    unique: dict[tuple[str, str, str, str], Record] = {}
    for item in pins:
        row = _fields(item, {"source_id", "source_sha256", "table", "table_digest"})
        key = (
            _text(row["source_id"]),
            _hash(row["source_sha256"]),
            _text(row["table"]),
            _hash(row["table_digest"]),
        )
        if key in unique:
            raise ValueError("duplicate source pin")
        unique[key] = row
    return [unique[key] for key in sorted(unique)]


def _export_dates(
    body: Record,
    dates: tuple[date, ...],
    opens: tuple[Mapping[str, float], ...],
    closes: tuple[Mapping[str, float], ...],
) -> None:
    if (
        not isinstance(dates, tuple)
        or len(dates) < _MINIMUM_SESSIONS
        or any(type(day) is not date for day in dates)
    ):
        raise ValueError("export requires at least two supplied date sessions")
    period = _object(body["period"])
    if (
        dates[0] != _day(period["start"])
        or dates[-1] != _day(period["end"])
        or any(a >= b for a, b in pairwise(dates))
    ):
        raise ValueError("export sessions must increase from baseline through final close")
    if (
        not isinstance(opens, tuple)
        or not isinstance(closes, tuple)
        or len(opens) != len(dates)
        or len(closes) != len(dates)
    ):
        raise ValueError("export prices and sessions must have equal lengths")


def _export_targets(
    targets: Mapping[date, Mapping[str, float]],
    dates: tuple[date, ...],
    selected: set[str],
    types: Record,
    opening: list[Record],
) -> Record:
    if not isinstance(targets, Mapping):
        raise TypeError("targets must map decision dates to weights")
    normalized_targets: Record = {}
    next_index = {day: index + 1 for index, day in enumerate(dates[:-1])}
    for day, weights in targets.items():
        if type(day) is not date or day not in next_index:
            raise ValueError("target decision requires a following supplied session")
        parsed_weights: Record = {}
        numbers: list[float] = []
        for symbol, value in _object(weights).items():
            _text(symbol)
            weight = _number(value)
            if symbol == "CASH" or not 0 <= weight <= 1:
                raise ValueError("cash is residual and target weights must be in [0, 1]")
            if weight > 0 and (symbol not in selected or types.get(symbol) != "ETF"):
                raise ValueError("positive target requires selected ETF execution instrument")
            if weight > 0 and symbol not in opening[next_index[day]]:
                raise ValueError("selected buy requires its next-session open")
            numbers.append(weight)
            parsed_weights[symbol] = weight
        if math.fsum(numbers) > 1:
            raise ValueError("target weights must sum to at most one")
        normalized_targets[day.isoformat()] = parsed_weights
    return normalized_targets


@dataclass(frozen=True, slots=True)
class EnvelopeInputs:
    """Explicit export values; boundary validation remains with export_envelope."""

    dates: tuple[date, ...]
    opens: tuple[Mapping[str, float], ...]
    closes: tuple[Mapping[str, float], ...]
    targets: Mapping[date, Mapping[str, float]]
    instrument_types: Mapping[str, str]
    source_pins: tuple[Mapping[str, str], ...]


def export_envelope(
    request: ParsedPrepareRequest,
    *,
    projection: RequestProjection,
    inputs: EnvelopeInputs,
) -> EnvelopeExport:
    """Validate supplied outcomes/targets, without inferring holdings or doing fills."""
    # Recheck public detached evidence at export too; no private trust token or
    # float-to-convention reconstruction can authenticate the registered preimage.
    checked = RequestProjection(
        projection.canonical_bytes,
        projection.request_hash,
        projection.execution_cost,
        projection.cost_document,
    )
    body = _object(request.document)
    projected = _decode(checked.canonical_bytes)
    if any(
        canonical_json_bytes(body[key]) != canonical_json_bytes(projected[key])
        for key in _CONTENT_PROPERTIES
    ):
        raise ValueError("projection belongs to a different request")
    _export_dates(body, inputs.dates, inputs.opens, inputs.closes)
    selected = {
        str(symbol)
        for row in _rows(body["price_inputs"])
        if _key(row["binding"])[0] == "execution_prices"
        for symbol in _array(row["instrument_ids"])
    }
    opening, closing = _price_rows(inputs.opens, selected), _price_rows(inputs.closes, selected)
    types = _object(inputs.instrument_types)
    for symbol, kind in types.items():
        _text(symbol)
        if kind not in ("ETF", "INDEX", "SPOT"):
            raise ValueError("unsupported legacy instrument type")
    normalized_targets = _export_targets(inputs.targets, inputs.dates, selected, types, opening)
    explicit = body["explicit_decision_dates"]
    if explicit is not None and set(normalized_targets) != set(_array(explicit)):
        raise ValueError("export must preserve exactly requested decision dates")
    account, envelope = _object(body["account"]), _object(body["envelope"])
    if any(_day(flow["date"]) not in inputs.dates[1:] for flow in _rows(account["cashflows"])):
        raise ValueError("cashflows must occur on supplied open sessions after baseline")
    result = {
        "schema_version": envelope["schema_version"],
        "module": "aegis",
        "instrument_types": types,
        "dates": inputs.dates,
        "opens": opening,
        "closes": closing,
        "targets": normalized_targets,
        "initial_cash": account["initial_cash"],
        "cost": checked.execution_cost,
        "source_pins": _source_pins(inputs.source_pins),
        "research_mode": envelope["research_mode"],
    }
    if envelope["schema_version"] == "aas-etf-backtest-v2":
        result["cashflows"] = account["cashflows"]
    raw = _bounded_bytes(result, limit=_MAX_ENVELOPE_BYTES)
    return EnvelopeExport(raw, hashlib.sha256(raw).hexdigest())
