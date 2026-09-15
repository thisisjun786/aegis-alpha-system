"""Complete synthetic request vectors and independent legacy accounting oracles."""

from __future__ import annotations

import hashlib
import json
import math
import operator
import subprocess
import sys
from collections.abc import MutableMapping
from dataclasses import asdict, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine import execution
from aegis_alpha.engine.backtest_request import (
    BACKTEST_REQUEST_SCHEMA,
    PREPARE_REQUEST_SCHEMA,
    EnvelopeExport,
    EnvelopeInputs,
    ParsedPrepareRequest,
    RequestProjection,
    export_envelope,
    parse_backtest_request,
    parse_prepare_request,
    request_projection,
)
from aegis_alpha.engine.ensemble import EnsembleMembership
from aegis_alpha.engine.membership import MembershipRow, membership_hash
from aegis_alpha.engine.requirements import ExecutionDefinition, derive_execution_definition
from aegis_alpha.engine.tolerance import long_only_sum_tolerance
from tests.engine.engine_support import bundle, contract
from tests.engine.test_requirements import rich_contract

J = "aas-canonical-json-sha256-v1"
B = "aas-sha256-bytes-v1"
DATES = (date(2026, 1, 30), date(2026, 2, 2))
type Document = dict[str, Any]


def identities() -> tuple[Document, Document]:
    """Explicit synthetic compute context, never discovered by the engine."""
    engine = {
        "schema": "aas-engine-identity-v1",
        "hash_format": J,
        "package_version": "0.0.1",
        "contract_version": "aas-engine-v1",
        "calculation_source_hash": hashlib.sha256(b"synthetic calculation inventory").hexdigest(),
    }
    settings = {
        "float_radix": 2,
        "float_mant_dig": 53,
        "float_rounds": 1,
        "decimal_precision": 28,
        "decimal_rounding": "ROUND_HALF_EVEN",
        "decimal_emin": -999999,
        "decimal_emax": 999999,
        "decimal_capitals": 1,
        "decimal_clamp": 0,
        "decimal_traps": "DivisionByZero,InvalidOperation,Overflow",
    }
    environment = {
        "schema": "aas-environment-identity-v1",
        "hash_format": J,
        "versions": [
            {"name": "python_implementation", "version": "CPython"},
            {"name": "python_version", "version": "3.12.12"},
        ],
        "settings": [{"name": name, "value": value} for name, value in settings.items()],
    }
    return engine, environment


def add_ref(body: Document, role: str, identity: str, *, ordinal: int = 0) -> Document:
    kind = {
        "identity": "identity",
        "universe": "universe",
        "membership": "membership",
        "derived": "derived",
    }.get(role, "generation")
    digest = hashlib.sha256(identity.encode()).hexdigest()
    if kind == "generation":
        schema, fmt = "aas-generation-pin-v1", "aas-market-generation-chain-v1"
        pin = {
            "dataset_id": identity,
            "version": "1",
            "generation_id": identity + "-g",
            "chain_hash": digest,
            "manifest_hash": hashlib.sha256((identity + "-m").encode()).hexdigest(),
        }
    elif kind == "identity":
        schema, fmt = "aas-identity-snapshot-v1", J
        pin = {"snapshot_id": identity, "content_hash": digest}
    elif kind == "universe":
        schema, fmt = "aas-universe-version-v1", J
        pin = {"universe_id": identity, "version": "1", "content_hash": digest}
    else:
        schema, fmt = (
            {"derived": "aas-derived-definition-v1", "membership": "aas-ensemble-membership-v1"}[
                kind
            ],
            J,
        )
        pin = {"kind": kind, "id": identity, "version": "1", "hash": digest}
    ref = {
        "ref_kind": kind,
        "ref_id": identity,
        "ref_version": schema if kind == "identity" else "1",
        "hash": digest,
        "schema": schema,
        "hash_format": fmt,
        "pin": pin,
    }
    body["refs"].append(ref)
    body["bindings"].append(
        {key: value for key, value in ref.items() if key not in ("pin", "schema")}
        | {"role": role, "ordinal": ordinal, "ref_schema": schema}
    )
    return {"role": role, "ordinal": ordinal}


def fixture(
    *, rich: bool = False, version: str = "v1", rate: float = 0
) -> tuple[Document, ExecutionDefinition, tuple[bytes, ...]]:
    value = rich_contract() if rich else contract()
    members = tuple(MembershipRow(record.name, Decimal(1)) for record in value.pack)
    value = replace(value, ensemble_membership_reference="ensemble:" + membership_hash(members))
    definition = derive_execution_definition(bundle(value))
    body: Document = {
        "schema": "aas-prepare-request-v1",
        "hash_format": J,
        "strategy": {
            "strategy_store_id": "synthetic-store",
            "strategy_id": definition.bundle_id,
            "version": definition.bundle_version,
            "raw_sha256": definition.source_sha256,
            "contract_sha256": definition.contract_sha256,
            "schema": "aas-engine-bundle-v1",
            "contract_version": "aas-engine-v1",
            "raw_hash_format": B,
            "contract_hash_format": J,
        },
        "bindings": [],
        "refs": [],
        "price_inputs": [],
        "macro_inputs": [],
        "derived_inputs": [],
        "proxy_rules": [],
        "period": {"start": "2026-01-30", "end": "2026-02-02"},
        "history": {"start": "2025-01-01", "end": "2026-01-30"},
        "cutoff": {
            "mode": "observed_snapshot_research",
            "knowledge_cutoff_us": 1769817600000000,
            "ingestion_cutoff_us": None,
        },
        "decision_latency_us": 0,
        "explicit_decision_dates": ["2026-01-30"],
        "account": {"currency": "USD", "initial_cash": 100, "cashflows": []},
        "comparison": {"benchmark": None, "risk_free": None, "fx": None},
        "envelope": {"schema_version": "aas-etf-backtest-" + version, "research_mode": "synthetic"},
        "metadata": {"created_at_us": 1},
    }
    for role in ("sessions", "identity", "universe", "membership"):
        add_ref(body, role, "synthetic-" + role)
    for role in ("signal_prices", "execution_prices"):
        key = add_ref(body, role, "synthetic-" + role)
        body["price_inputs"].append(
            {
                "binding": key,
                "instrument_ids": list(
                    definition.price_asset_ids if role == "signal_prices" else definition.asset_ids
                ),
                "currency": "USD",
                "basis": "split_adjusted" if role == "signal_prices" else "unadjusted",
                "price_role": "reference" if role == "signal_prices" else "canonical",
                "interval": "1d",
            }
        )
    for requirement in definition.input_requirements:
        if requirement.role in ("macro", "derived"):
            for series in requirement.identifiers:
                key = add_ref(body, requirement.role, series)
                selection = {"binding": key, "series_id": series}
                if requirement.role == "macro":
                    selection["unit"] = "ratio"
                body[requirement.role + "_inputs"].append(selection)
    payloads = {
        "calendar": {
            "schema": "aas-calendar-v1",
            "calendar_id": "synthetic-calendar",
            "venue": "SYN",
            "timezone_version": "synthetic-utc-1",
            **asdict(definition.calendar),
        },
        "basis": {"schema": "aas-basis-v1", "price_basis": "capital"},
        "cost": {
            "schema": "aas-cost-v1",
            "model": "proportional_traded_notional",
            "rate": rate,
            "currency": "USD",
        },
        "execution": {
            "schema": "aas-execution-v1",
            "decision": "session_close",
            "execution": "next_session_open",
            "sizing": "fractional_long_only",
            "cash": "implicit_residual",
            "terminal": "mark_without_liquidation",
            "cashflows": "session_open_before_rebalance_existing_cash_withdrawals",
        },
    }
    docs = []
    for kind, payload in payloads.items():
        doc = {
            "schema": "aas-convention-v1",
            "hash_format": J,
            "kind": kind,
            "id": "synthetic-" + kind,
            "version": "1",
            "payload": payload,
        }
        docs.append(canonical_json_bytes(doc))
        pin = {"kind": kind, "id": doc["id"], "version": "1", "hash": content_sha256(doc)}
        ref = {
            "ref_kind": "convention:" + kind,
            "ref_id": doc["id"],
            "ref_version": "1",
            "hash": pin["hash"],
            "schema": "aas-convention-v1",
            "hash_format": J,
            "pin": pin,
        }
        body["refs"].append(ref)
        body["bindings"].append(
            {key: value for key, value in ref.items() if key not in ("pin", "schema")}
            | {"role": kind, "ordinal": 0, "ref_schema": ref["schema"]}
        )
    membership_doc = {
        "schema": "aas-ensemble-membership-v1",
        "hash_format": J,
        "id": "synthetic-membership",
        "version": "1",
        "membership_sha256": membership_hash(members),
        "rows": [{"name": row.name, "weight": str(row.weight)} for row in members],
    }
    member_ref = next(item for item in body["refs"] if item["ref_kind"] == "membership")
    member_ref["hash"] = member_ref["pin"]["hash"] = content_sha256(membership_doc)
    next(item for item in body["bindings"] if item["role"] == "membership")["hash"] = member_ref[
        "hash"
    ]
    return body, definition, tuple(docs)


def project(
    body: Document, definition: ExecutionDefinition, docs: tuple[bytes, ...]
) -> tuple[ParsedPrepareRequest, RequestProjection]:

    parsed = parse_prepare_request(canonical_json_bytes(body))
    engine, environment = identities()
    return parsed, request_projection(
        parsed,
        definition=definition,
        convention_documents=docs,
        engine_identity=engine,
        environment_identity=environment,
    )


def emit(parsed: ParsedPrepareRequest, projection: RequestProjection) -> EnvelopeExport:

    return export_envelope(
        parsed,
        projection=projection,
        inputs=EnvelopeInputs(
            dates=DATES,
            opens=({}, {"ASSET_A": 10.0}),
            closes=({"ASSET_A": 10.0}, {"ASSET_A": 12.0}),
            targets={DATES[0]: {"ASSET_A": 0.5}},
            instrument_types={"ASSET_A": "ETF"},
            source_pins=(),
        ),
    )


def test_complete_canonicalization_vectors() -> None:
    body, definition, docs = fixture()
    parsed, baseline = project(body, definition, docs)
    reordered = json.loads(json.dumps(body))
    reordered["metadata"]["created_at_us"] = 2
    for field in ("refs", "bindings", "price_inputs"):
        reordered[field].reverse()
    for selection in reordered["price_inputs"]:
        selection["instrument_ids"].reverse()
    assert (
        project(reordered, definition, tuple(reversed(docs)))[1].canonical_bytes
        == baseline.canonical_bytes
    )
    assert (
        hashlib.sha256(json.dumps(reordered, indent=2).encode()).hexdigest()
        != hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    )
    body["cutoff"]["knowledge_cutoff_us"] += 1
    assert project(body, definition, docs)[1].request_hash != baseline.request_hash
    body, definition, docs = fixture()
    key = add_ref(body, "proxy", "synthetic-proxy")
    body["proxy_rules"].append({"binding": key, "logical_exposure_id": "ASSET_B"})
    body["price_inputs"][0]["instrument_ids"].remove("ASSET_B")
    proxy = project(body, definition, docs)[1]
    for ref in body["refs"]:
        if ref["ref_id"] == "synthetic-proxy":
            ref["hash"] = ref["pin"]["chain_hash"] = hashlib.sha256(b"proxy revision").hexdigest()
    for binding in body["bindings"]:
        if binding["role"] == "proxy":
            binding["hash"] = hashlib.sha256(b"proxy revision").hexdigest()
    assert project(body, definition, docs)[1].request_hash != proxy.request_hash
    assert baseline.request_hash == hashlib.sha256(baseline.canonical_bytes).hexdigest()
    assert emit(parsed, baseline).envelope_sha256 != baseline.request_hash


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_actual_emitted_legacy_api(version: str) -> None:
    body, definition, docs = fixture(version=version)
    parsed, projection = project(body, definition, docs)
    exported = emit(parsed, projection)
    assert exported.envelope_sha256 == hashlib.sha256(exported.canonical_bytes).hexdigest()
    result = cast("Document", run_document(exported.canonical_bytes, exported.envelope_sha256))
    account = result["result"] if version == "v1" else result["result"]["account"]
    assert account == {
        "nav": [
            {"date": "2026-01-30", "equity": 100.0, "cash": 100.0, "fee": 0.0},
            {"date": "2026-02-02", "equity": 110.0, "cash": 50.0, "fee": 0.0},
        ],
        "fills": [
            {
                "decision_date": "2026-01-30",
                "execution_date": "2026-02-02",
                "symbol": "ASSET_A",
                "shares": 5.0,
                "price": 10.0,
                "fee": 0.0,
            }
        ],
    }
    if version == "v1":
        assert (
            exported.envelope_sha256
            == "b7705529ddc8aeb9ab5e6b6b6c5cbb91daad7d75101a82e9e98fdb30cd53dbaa"
        )
    if version == "v2":
        assert exported.envelope_sha256 == (
            "291c2aaab431546d54939993111e1551af6f0b6ae56c34644d98a84946dab850"
        )
    for flag in (
        "source_pins_verified",
        "observed_prices_verified",
        "point_in_time_verified",
        "live_orders",
    ):
        assert result[flag] is False


@pytest.mark.parametrize(
    "field",
    ["cost", "calendar", "membership", "sessions", "identity", "universe", "execution", "basis"],
)
def test_missing_required_role_fails(field: str) -> None:
    body, definition, docs = fixture()
    body["bindings"] = [item for item in body["bindings"] if item["role"] != field]
    body["refs"] = [
        ref
        for ref in body["refs"]
        if any(item["ref_id"] == ref["ref_id"] for item in body["bindings"])
    ]
    with pytest.raises(ValueError, match=r".+"):
        project(body, definition, docs)


@pytest.mark.parametrize("rate", [0, 0.0, -0.0, 0.01])
def test_cost_document_cannot_be_detached_from_execution_scalar(rate: float) -> None:

    body, definition, docs = fixture(rate=rate)
    _, projection = project(body, definition, docs)
    assert projection.cost_document == next(
        raw for raw in docs if json.loads(raw)["kind"] == "cost"
    )
    with pytest.raises(ValueError, match=r".+"):
        RequestProjection(
            projection.canonical_bytes, projection.request_hash, 0.2, projection.cost_document
        )
    other_body, other_definition, other_docs = fixture(rate=0.0 if type(rate) is int else 0)
    other = project(other_body, other_definition, other_docs)[1]
    assert other.request_hash != projection.request_hash
    with pytest.raises(ValueError, match=r".+"):
        RequestProjection(
            projection.canonical_bytes,
            projection.request_hash,
            projection.execution_cost,
            other.cost_document,
        )


@pytest.mark.parametrize("field", ["macro_inputs", "derived_inputs"])
def test_definition_relative_requirements_not_bypassed(field: str) -> None:
    body, definition, docs = fixture(rich=True)
    project(body, definition, docs)
    body[field] = []
    with pytest.raises(ValueError, match=r".+"):
        project(body, definition, docs)


def test_backtest_fresh_read_and_cross_request_export() -> None:

    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    reread, reprojected = parse_backtest_request(
        projection.canonical_bytes, definition=definition, convention_documents=docs
    )
    assert json.loads(canonical_json_bytes(reread.document))["metadata"]["created_at_us"] is None
    assert reprojected == projection
    body["account"]["initial_cash"] = 101
    changed, _ = project(body, definition, docs)
    with pytest.raises(ValueError, match=r".+"):
        emit(changed, projection)
    with pytest.raises(ValueError, match=r".+"):
        parse_backtest_request(
            projection.canonical_bytes,
            definition=replace(definition, source_sha256="a" * 64),
            convention_documents=docs,
        )
    assert json.loads(canonical_json_bytes(parsed.document))["account"]["initial_cash"] == float(
        100
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("account", "initial_cash"), True),
        (("account", "initial_cash"), 0),
        (("account", "initial_cash"), "100"),
        (("account", "initial_cash"), float("inf")),
        (("decision_latency_us",), True),
        (("decision_latency_us",), -1),
        (("decision_latency_us",), 1.0),
        (("decision_latency_us",), 2**63),
        (("cutoff", "knowledge_cutoff_us"), False),
        (("cutoff", "ingestion_cutoff_us"), -1),
        (("period", "start"), "20260130"),
        (("period", "end"), "2026-01-30"),
        (("history", "start"), "2026-02-01"),
        (("history", "end"), "2024-01-01"),
        (("metadata", "created_at_us"), True),
        (("metadata", "created_at_us"), -1),
        (("strategy", "version"), "latest"),
        (("strategy", "raw_sha256"), "A" * 64),
        (("strategy", "raw_hash_format"), J),
        (("strategy", "strategy_id"), " untrimmed"),
        (("strategy", "strategy_id"), "bad\u200btext"),
        (("strategy", "strategy_id"), "\ud800"),
        (("envelope", "schema_version"), "aas-etf-backtest-v3"),
        (("explicit_decision_dates",), ["2026-01-30", "2026-01-30"]),
        (("explicit_decision_dates",), ["2026-02-02"]),
        (("account", "cashflows"), [{"date": "2026-02-02", "amount": 1}]),
        (("comparison", "benchmark"), {"role": "cost", "ordinal": 0}),
    ],
)
def test_malformed_scalar_values(path: tuple[str, ...], value: object) -> None:

    body, _, _ = fixture()
    target = body
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    raw = json.dumps(body).encode()
    with pytest.raises(ValueError, match=r".+"):
        parse_prepare_request(raw)


@pytest.mark.parametrize(
    "mutation",
    [
        "refs",
        "binding",
        "same-role",
        "gap",
        "bool",
        "pin",
        "label",
        "unknown",
        "nested-unknown",
        "overlap",
        "duplicate-selection",
    ],
)
def test_duplicate_and_corrupt_reference_values(mutation: str) -> None:

    body, _, _ = fixture()
    if mutation == "refs":
        body["refs"].append(dict(body["refs"][0]))
    elif mutation == "binding":
        body["bindings"].append(dict(body["bindings"][0]))
    elif mutation == "same-role":
        row = next(item for item in body["bindings"] if item["role"] == "signal_prices")
        body["bindings"].append(dict(row, ordinal=1))
    elif mutation in ("gap", "bool"):
        body["bindings"][0]["ordinal"] = 2 if mutation == "gap" else True
    elif mutation == "pin":
        body["refs"][0]["pin"]["manifest_hash"] = "bad"
    elif mutation == "label":
        body["bindings"][0]["hash_format"] = J
    elif mutation == "unknown":
        body["run_id"] = "not-a-request-field"
    elif mutation == "nested-unknown":
        body["account"]["cost"] = 0
    elif mutation == "overlap":
        body["price_inputs"][0]["instrument_ids"].append("ASSET_A")
    else:
        body["price_inputs"].append(dict(body["price_inputs"][0]))
    with pytest.raises(ValueError, match=r".+"):
        parse_prepare_request(canonical_json_bytes(body))


@pytest.mark.parametrize(
    "encoding", ["utf-8-sig", "utf-16", "utf-16le", "utf-16be", "utf-32", "utf-32le", "utf-32be"]
)
def test_strict_utf8_ingress(encoding: str) -> None:

    body, _, _ = fixture()
    with pytest.raises(ValueError, match=r".+"):
        parse_prepare_request(json.dumps(body).encode(encoding))


def test_duplicate_json_at_depth_and_size_reject_before_collapse() -> None:

    body, _, _ = fixture()
    raw = canonical_json_bytes(body)
    for malformed in (
        raw.replace(b'"initial_cash":100', b'"initial_cash":100,"initial_cash":200'),
        raw.replace(b'"ordinal":0', b'"ordinal":0,"ordinal":0', 1),
        raw + b" " * (1024 * 1024),
        b'{"x":"\xff"}',
    ):
        with pytest.raises(ValueError, match=r".+"):
            parse_prepare_request(malformed)


@pytest.mark.parametrize(
    ("role", "version", "accepted"),
    [
        ("sessions", "latest", False),
        ("sessions", "Latest", True),
        ("sessions", "LATEST", True),
        ("universe", "latest", False),
        ("universe", "Latest", True),
        ("universe", "LATEST", True),
        ("cost", "latest", False),
        ("cost", "Latest", False),
        ("cost", "LATEST", False),
        ("membership", "latest", False),
        ("membership", "LATEST", True),
    ],
)
def test_owner_specific_versions(role: str, version: str, *, accepted: bool) -> None:

    body, _, _ = fixture()
    binding = next(item for item in body["bindings"] if item["role"] == role)
    ref = next(item for item in body["refs"] if item["ref_id"] == binding["ref_id"])
    binding["ref_version"] = ref["ref_version"] = ref["pin"]["version"] = version
    if accepted:
        parse_prepare_request(canonical_json_bytes(body))
    else:
        with pytest.raises(ValueError, match=r".+"):
            parse_prepare_request(canonical_json_bytes(body))


def test_opaque_latest_identity_and_deep_immutability() -> None:

    body, _, _ = fixture()
    binding = next(item for item in body["bindings"] if item["role"] == "identity")
    ref = next(item for item in body["refs"] if item["ref_id"] == binding["ref_id"])
    binding["ref_id"] = ref["ref_id"] = ref["pin"]["snapshot_id"] = "latest"
    parsed = ParsedPrepareRequest(body)
    before = canonical_json_bytes(parsed.document)
    body["account"]["initial_cash"] = 1
    body["price_inputs"][0]["instrument_ids"].clear()
    assert canonical_json_bytes(parsed.document) == before
    account = parsed.document["account"]
    with pytest.raises(TypeError):
        operator.setitem(cast("MutableMapping[str, object]", account), "initial_cash", 200)


def independent_projection(body: Document, docs: tuple[bytes, ...]) -> bytes:
    """Handwritten field projection using stdlib JSON, not the production normalizer."""
    projected = json.loads(json.dumps(body))
    del projected["metadata"]
    projected["schema"] = "aas-backtest-request-v1"
    projected["bindings"].sort(key=lambda row: (row["role"], row["ordinal"]))
    projected["refs"].sort(key=lambda row: (row["ref_kind"], row["ref_id"], row["ref_version"]))
    for field in ("price_inputs", "macro_inputs", "derived_inputs", "proxy_rules"):
        projected[field].sort(key=lambda row: (row["binding"]["role"], row["binding"]["ordinal"]))
    for selection in projected["price_inputs"]:
        selection["instrument_ids"].sort()
    projected["account"]["initial_cash"] = float(projected["account"]["initial_cash"])
    for flow in projected["account"]["cashflows"]:
        flow["amount"] = float(flow["amount"])
    conventions = []
    for raw in sorted(docs, key=lambda item: json.loads(item)["kind"]):
        doc = json.loads(raw)
        payload = json.dumps(
            doc["payload"],
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        conventions.append(
            {
                "pin": {
                    "kind": doc["kind"],
                    "id": doc["id"],
                    "version": doc["version"],
                    "hash": hashlib.sha256(raw).hexdigest(),
                },
                "schema": "aas-convention-v1",
                "hash_format": J,
                "payload_hash": hashlib.sha256(payload).hexdigest(),
            }
        )
    projected["conventions"] = conventions
    projected["engine"], projected["environment"] = identities()
    for field in ("versions", "settings"):
        projected["environment"][field].sort(key=lambda item: item["name"])
    return json.dumps(
        projected, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()


def canonical_vectors() -> list[tuple[Document, ExecutionDefinition, tuple[bytes, ...]]]:
    body, definition, docs = fixture()
    values = [(body, definition, docs)]
    reorder = json.loads(json.dumps(body))
    reorder["metadata"]["created_at_us"] = 900
    reorder["bindings"].reverse()
    reorder["refs"].reverse()
    values.append((reorder, definition, docs))
    cutoff = json.loads(json.dumps(body))
    cutoff["cutoff"]["knowledge_cutoff_us"] += 1
    values.append((cutoff, definition, docs))
    proxy = json.loads(json.dumps(body))
    for ordinal, instrument in enumerate(("ASSET_A", "ASSET_B")):
        key = add_ref(proxy, "proxy", "proxy-" + instrument, ordinal=ordinal)
        proxy["proxy_rules"].append({"binding": key, "logical_exposure_id": instrument})
        proxy["price_inputs"][0]["instrument_ids"].remove(instrument)
    values.append((proxy, definition, docs))
    swapped = json.loads(json.dumps(proxy))
    for binding in swapped["bindings"]:
        if binding["role"] == "proxy":
            binding["ordinal"] = 1 - binding["ordinal"]
    # Keep selected logical assets at the same ordinals: reassigning pins is semantic.
    values.append((swapped, definition, docs))
    changed = json.loads(json.dumps(proxy))
    ref = next(item for item in changed["refs"] if item["ref_id"] == "proxy-ASSET_A")
    ref["hash"] = ref["pin"]["chain_hash"] = hashlib.sha256(b"corrected proxy").hexdigest()
    next(item for item in changed["bindings"] if item["ref_id"] == "proxy-ASSET_A")["hash"] = ref[
        "hash"
    ]
    values.append((changed, definition, docs))
    return values


@pytest.mark.parametrize("index", range(6))
def test_complete_independent_projection_bytes(index: int) -> None:
    body, definition, docs = canonical_vectors()[index]
    _, projection = project(body, definition, docs)
    expected = independent_projection(body, docs)
    assert projection.canonical_bytes == expected
    assert projection.request_hash == hashlib.sha256(expected).hexdigest()


def test_numeric_ordinals_not_lexicographic_or_reassigned() -> None:
    body, definition, docs = fixture()
    ids = ["ASSET_" + str(index) for index in range(12)]
    base = contract()
    strategy = replace(
        base.pack[0],
        offensive_config={
            "strategy_type": "relative",
            "assets": ids,
            "top_n": 1,
            "scoring": {"method": "return_rate", "horizon": 2},
            "reference_asset": "REF_X",
        },
    )
    definition = derive_execution_definition(bundle(replace(base, pack=(strategy,))))
    body["strategy"].update(
        raw_sha256=definition.source_sha256, contract_sha256=definition.contract_sha256
    )
    body["price_inputs"][0]["instrument_ids"] = ["REF_X"]
    body["price_inputs"][1]["instrument_ids"] = [*ids, "REF_X"]
    for ordinal, identity in enumerate(ids):
        key = add_ref(body, "proxy", "proxy-" + identity, ordinal=ordinal)
        body["proxy_rules"].append({"binding": key, "logical_exposure_id": identity})
    body["bindings"].reverse()
    _, projection = project(body, definition, docs)
    bindings = json.loads(projection.canonical_bytes)["bindings"]
    assert [row["ordinal"] for row in bindings if row["role"] == "proxy"] == list(range(12))
    assert projection.canonical_bytes == independent_projection(body, docs)


@pytest.mark.parametrize(
    ("kind", "field", "value"),
    [
        ("cost", "rate", True),
        ("cost", "rate", -0.1),
        ("cost", "rate", 1),
        ("cost", "rate", "0"),
        ("cost", "currency", "EUR"),
        ("cost", "model", "flat_fee"),
        ("calendar", "history_observations", True),
        ("calendar", "history_observations", 2),
        ("calendar", "evaluation_snap", "weekly"),
        ("calendar", "signal_date", "same_day"),
        ("calendar", "current_month_drop_before_day", 2),
        ("calendar", "history_authority", "other"),
        ("execution", "terminal", "liquidate"),
        ("execution", "cashflows", "after_rebalance"),
        ("basis", "price_basis", "total_return"),
    ],
)
def test_hash_correct_but_unsupported_conventions(kind: str, field: str, value: object) -> None:
    body, definition, docs = fixture()
    changed = json.loads(next(raw for raw in docs if json.loads(raw)["kind"] == kind))
    changed["payload"][field] = value
    raw = canonical_json_bytes(changed)
    docs = tuple(raw if json.loads(item)["kind"] == kind else item for item in docs)
    ref = next(item for item in body["refs"] if item["ref_kind"] == "convention:" + kind)
    ref["hash"] = ref["pin"]["hash"] = hashlib.sha256(raw).hexdigest()
    next(item for item in body["bindings"] if item["role"] == kind)["hash"] = ref["hash"]
    with pytest.raises(ValueError, match=r".+"):
        project(body, definition, docs)


@pytest.mark.parametrize(
    "mutation", ["duplicate", "missing", "noncanonical", "hash", "extra", "unknown", "bool"]
)
def test_convention_and_semantic_corruption(mutation: str) -> None:
    body, definition, docs = fixture()
    if mutation == "duplicate":
        docs = (*docs, docs[0])
    elif mutation == "missing":
        docs = docs[1:]
    elif mutation == "noncanonical":
        docs = (docs[0] + b"\n", *docs[1:])
    elif mutation == "hash":
        changed = json.loads(docs[0])
        changed["payload"]["venue"] = "OTHER"
        docs = (canonical_json_bytes(changed), *docs[1:])
    else:
        _, projection = project(body, definition, docs)
        changed = json.loads(projection.canonical_bytes)
        if mutation == "extra":
            changed["metadata"] = {"created_at_us": None}
        elif mutation == "unknown":
            changed["account"]["currency_alias"] = "USD"
        else:
            changed["account"]["initial_cash"] = True
        with pytest.raises(ValueError, match=r".+"):
            parse_backtest_request(
                canonical_json_bytes(changed), definition=definition, convention_documents=docs
            )
        return
    with pytest.raises(ValueError, match=r".+"):
        project(body, definition, docs)


@pytest.mark.parametrize(
    "mutation",
    ["missing", "duplicate", "unknown", "bool", "rounding", "traps", "engine", "version"],
)
def test_explicit_compute_identity_admission(mutation: str) -> None:
    body, definition, docs = fixture()
    parsed = parse_prepare_request(canonical_json_bytes(body))
    engine, environment = identities()
    if mutation == "missing":
        environment["settings"].pop()
    elif mutation == "duplicate":
        environment["versions"].append(dict(environment["versions"][0]))
    elif mutation == "unknown":
        environment["settings"][0]["name"] = "hostname"
    elif mutation == "bool":
        environment["settings"][0]["value"] = True
    elif mutation in ("rounding", "traps"):
        next(item for item in environment["settings"] if item["name"] == "decimal_" + mutation)[
            "value"
        ] = "invalid"
    elif mutation == "engine":
        engine["calculation_source_hash"] = "not-a-source-hash"
    else:
        environment["versions"][0]["version"] = ""
    with pytest.raises(ValueError, match=r".+"):
        request_projection(
            parsed,
            definition=definition,
            convention_documents=docs,
            engine_identity=engine,
            environment_identity=environment,
        )


def test_compute_identity_order_context_and_empty_traps() -> None:
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    engine, environment = identities()
    environment["versions"].reverse()
    environment["settings"].reverse()
    assert (
        request_projection(
            parsed,
            definition=definition,
            convention_documents=docs,
            engine_identity=engine,
            environment_identity=environment,
        )
        == projection
    )
    next(item for item in environment["settings"] if item["name"] == "decimal_traps")["value"] = ""
    changed = request_projection(
        parsed,
        definition=definition,
        convention_documents=docs,
        engine_identity=engine,
        environment_identity=environment,
    )
    assert changed.request_hash != projection.request_hash


@pytest.mark.parametrize(
    "flow",
    [
        {"date": "2026-02-02", "amount": True},
        {"date": "2026-02-02", "amount": 0},
        {"date": "2026-01-30", "amount": 1},
        {"date": "2026-02-03", "amount": 1},
        {"date": "2026-02-02", "amount": 10**400},
    ],
)
def test_v2_invalid_flow_values(flow: Document) -> None:
    body, _, _ = fixture(version="v2")
    body["account"]["cashflows"] = [flow]
    with pytest.raises(ValueError, match=r".+"):
        parse_prepare_request(json.dumps(body).encode())


def test_v2_nonempty_flows_and_independent_units() -> None:
    body, definition, docs = fixture(version="v2")
    body["account"]["cashflows"] = [{"date": "2026-02-02", "amount": 100}]
    parsed, projection = project(body, definition, docs)
    exported = emit(parsed, projection)
    result = cast("Document", run_document(exported.canonical_bytes, exported.envelope_sha256))[
        "result"
    ]
    assert result["account"]["nav"] == [
        {"date": "2026-01-30", "equity": 100.0, "cash": 100.0, "fee": 0.0},
        {"date": "2026-02-02", "equity": 220.0, "cash": 100.0, "fee": 0.0},
    ]
    assert result["account"]["fills"][0]["shares"] == float(10)
    assert result["unit_nav"] == [
        {"date": "2026-01-30", "unit_value": 1.0, "units": 100.0, "external_flow": 0.0},
        {"date": "2026-02-02", "unit_value": 1.1, "units": 200.0, "external_flow": 100.0},
    ]


def test_nonzero_cost_executes_admitted_rate() -> None:
    body, definition, docs = fixture(rate=0.1)
    parsed, projection = project(body, definition, docs)
    exported = emit(parsed, projection)
    account = cast("Document", run_document(exported.canonical_bytes, exported.envelope_sha256))[
        "result"
    ]
    # x = half of post-fee equity; x = .5 * (100 - .1*x), hence x=1000/21.
    assert account["fills"][0]["shares"] == pytest.approx(100 / 21)
    assert account["fills"][0]["fee"] == pytest.approx(100 / 21)
    assert account["nav"][-1]["cash"] == pytest.approx(1000 / 21)
    assert account["nav"][-1]["equity"] == pytest.approx(2200 / 21)


def test_no_accounting_during_export_and_missing_sale_mark_deferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)

    def unexpected(*_args: object, **_kwargs: object) -> None:
        pytest.fail("preparation/export executed accounting")

    with monkeypatch.context() as patch:
        patch.setattr(execution, "replay_next_open", unexpected)
        patch.setattr(execution, "replay_next_open_cashflows", unexpected)
        emit(parsed, projection)
    body["period"]["end"] = "2026-02-03"
    body["explicit_decision_dates"] = ["2026-01-30", "2026-02-02"]
    parsed, projection = project(body, definition, docs)
    exported = export_envelope(
        parsed,
        projection=projection,
        inputs=EnvelopeInputs(
            dates=(*DATES, date(2026, 2, 3)),
            opens=({}, {"ASSET_A": 10.0}, {}),
            closes=({"ASSET_A": 10.0}, {"ASSET_A": 12.0}, {}),
            targets={DATES[0]: {"ASSET_A": 0.5}, DATES[1]: {}},
            instrument_types={"ASSET_A": "ETF"},
            source_pins=(),
        ),
    )
    with pytest.raises(ValueError, match="held or traded"):
        run_document(exported.canonical_bytes, exported.envelope_sha256)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-buy",
        "index",
        "unknown-type",
        "bool",
        "negative",
        "sum",
        "terminal",
        "missing-target",
        "price-bool",
        "warmup",
        "cash",
        "duplicate-pin",
    ],
)
def test_export_rejects_incompatible_outcomes(mutation: str) -> None:
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    args: Document = {
        "dates": DATES,
        "opens": ({}, {"ASSET_A": 10.0}),
        "closes": ({"ASSET_A": 10.0}, {"ASSET_A": 12.0}),
        "targets": {DATES[0]: {"ASSET_A": 0.5}},
        "instrument_types": {"ASSET_A": "ETF"},
        "source_pins": (),
    }
    if mutation == "missing-buy":
        args["opens"] = ({}, {})
    elif mutation in ("index", "unknown-type"):
        args["instrument_types"]["ASSET_A"] = "INDEX" if mutation == "index" else "EQUITY"
    elif mutation in ("bool", "negative"):
        args["targets"][DATES[0]]["ASSET_A"] = True if mutation == "bool" else -1
    elif mutation == "sum":
        args["targets"][DATES[0]] = {"ASSET_A": 0.8, "ASSET_B": 0.8}
        args["opens"][1]["ASSET_B"] = 10.0
        args["instrument_types"]["ASSET_B"] = "ETF"
    elif mutation == "terminal":
        args["targets"] = {DATES[1]: {"ASSET_A": 0.5}}
    elif mutation == "missing-target":
        args["targets"] = {}
    elif mutation == "price-bool":
        args["opens"][1]["ASSET_A"] = True
    elif mutation == "warmup":
        args["dates"] = (date(2025, 12, 31), DATES[1])
    elif mutation == "cash":
        args["targets"] = {DATES[0]: {"CASH": 0.0}}
    else:
        pin = {
            "source_id": "source-a",
            "source_sha256": "a" * 64,
            "table": "prices",
            "table_digest": "b" * 64,
        }
        args["source_pins"] = (pin, pin)
    with pytest.raises(ValueError, match=r".+"):
        export_envelope(
            parsed,
            projection=projection,
            inputs=EnvelopeInputs(
                dates=args["dates"],
                opens=args["opens"],
                closes=args["closes"],
                targets=args["targets"],
                instrument_types=args["instrument_types"],
                source_pins=args["source_pins"],
            ),
        )


_SUPERVISOR = r"""
import ctypes
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(36, 1, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
command = sys.argv[1:]
child = subprocess.Popen(
    command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    start_new_session=True,
)
try:
    stdout, stderr = child.communicate(timeout=20)
except BaseException:
    os.killpg(child.pid, signal.SIGKILL)
    child.communicate()
    raise
finally:
    # The isolated supervisor has no children other than this CLI's tree. Any
    # adopted descendant is a leak, even when the CLI returned success.
    children_path = Path(f"/proc/self/task/{os.getpid()}/children")
    descendants = [int(pid) for pid in children_path.read_text().split()]
    for pid in descendants:
        os.kill(pid, signal.SIGKILL)
    reaped = []
    while True:
        try:
            pid, status = os.waitpid(-1, 0)
            reaped.append([pid, status])
        except ChildProcessError:
            break
    remaining = Path(f"/proc/self/task/{os.getpid()}/children").read_text().strip()
    if remaining or descendants:
        raise RuntimeError(
            f"CLI leaked descendants: {descendants}; remaining={remaining}; reaped={reaped}"
        )
print(json.dumps({"command": command, "pid": child.pid, "exit_code": child.returncode,
                  "stdout": stdout, "stderr": stderr, "subreaper": True,
                  "descendants": descendants, "remaining": remaining, "reaped": reaped}))
"""


def fresh_cli(path: Path, digest: str) -> Document:
    completed = subprocess.run(  # noqa: S603 -- owned synthetic CLI under isolated subreaper
        [
            sys.executable,
            "-c",
            _SUPERVISOR,
            sys.executable,
            "-m",
            "aegis_alpha",
            "backtest",
            "--input",
            str(path),
            "--sha256",
            digest,
        ],
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_actual_emitted_fresh_cli(version: str, tmp_path: Path) -> None:
    body, definition, docs = fixture(version=version)
    if version == "v2":
        body["account"]["cashflows"] = [{"date": "2026-02-02", "amount": 100}]
    parsed, projection = project(body, definition, docs)
    exported = emit(parsed, projection)
    path = tmp_path / (exported.envelope_sha256 + ".json")
    path.write_bytes(exported.canonical_bytes)
    expected_digest = (
        "b7705529ddc8aeb9ab5e6b6b6c5cbb91daad7d75101a82e9e98fdb30cd53dbaa"
        if version == "v1"
        else "89153e3aedd22da00967ae3b8f192400e618319eb241f8ec798cb0f694ee3442"
    )
    assert exported.envelope_sha256 == expected_digest
    receipt = fresh_cli(path, exported.envelope_sha256)
    assert receipt["exit_code"] == 0, receipt["stderr"]
    assert receipt["subreaper"] is True
    assert receipt["remaining"] == ""
    assert receipt["descendants"] == receipt["reaped"] == []
    response = json.loads(receipt["stdout"])
    result = response["result"] if version == "v1" else response["result"]["account"]
    assert [row["equity"] for row in result["nav"]] == (
        [100, 110] if version == "v1" else [100, 220]
    )
    assert result["fills"][0] == {
        "decision_date": "2026-01-30",
        "execution_date": "2026-02-02",
        "symbol": "ASSET_A",
        "shares": 5.0 if version == "v1" else 10.0,
        "price": 10.0,
        "fee": 0.0,
    }
    assert response == run_document(exported.canonical_bytes, exported.envelope_sha256)
    wrong = fresh_cli(path, hashlib.sha256(b"wrong file").hexdigest())
    assert wrong["exit_code"] == 1
    assert wrong["stdout"] == ""
    assert "error" in json.loads(wrong["stderr"])


def test_export_rechecks_tampered_detached_cost() -> None:
    body, definition, docs = fixture(rate=0.0)
    parsed, projection = project(body, definition, docs)
    object.__setattr__(projection, "execution_cost", 0.25)
    with pytest.raises(ValueError, match="execution cost differs"):
        emit(parsed, projection)


@pytest.mark.parametrize("role", ["benchmark", "risk_free", "fx"])
def test_opaque_comparisons_are_pinned_not_financial_defaults(role: str) -> None:
    body, definition, docs = fixture()
    doc = {
        "schema": "aas-convention-v1",
        "hash_format": J,
        "kind": role,
        "id": "synthetic-" + role,
        "version": "1",
        "payload": {"schema": "opaque-comparison-v7", "original_number": 1},
    }
    digest = content_sha256(doc)
    ref = {
        "ref_kind": "convention:" + role,
        "ref_id": doc["id"],
        "ref_version": "1",
        "hash": digest,
        "schema": "aas-convention-v1",
        "hash_format": J,
        "pin": {"kind": role, "id": doc["id"], "version": "1", "hash": digest},
    }
    body["refs"].append(ref)
    body["bindings"].append(
        {key: value for key, value in ref.items() if key not in ("pin", "schema")}
        | {"role": role, "ordinal": 0, "ref_schema": ref["schema"]}
    )
    body["comparison"][role] = {"role": role, "ordinal": 0}
    docs = (*docs, canonical_json_bytes(doc))
    parsed, projection = project(body, definition, docs)
    assert json.loads(projection.canonical_bytes)["comparison"][role] == {
        "role": role,
        "ordinal": 0,
    }
    assert role not in json.loads(emit(parsed, projection).canonical_bytes)
    body["comparison"][role] = None
    with pytest.raises(ValueError, match="null comparison"):
        project(body, definition, docs)


def test_whole_membership_pin_retains_legacy_rounding_collisions() -> None:
    body, definition, docs = fixture()
    legacy = hashlib.sha256(b'[["synthetic-choice",1.0]]').hexdigest()
    assert definition.ensemble_membership_reference == "ensemble:" + legacy
    request_hashes = []
    for weight in ("1.00000000001", "1.00000000002"):
        rows = (MembershipRow("synthetic-choice", Decimal(weight)),)
        membership = EnsembleMembership(rows, legacy)
        assert membership.membership_sha256 == legacy
        doc = {
            "schema": "aas-ensemble-membership-v1",
            "hash_format": J,
            "id": "synthetic-membership",
            "version": "1",
            "membership_sha256": legacy,
            "rows": [{"name": "synthetic-choice", "weight": weight}],
        }
        digest = content_sha256(doc)
        assert digest != legacy
        ref = next(item for item in body["refs"] if item["ref_kind"] == "membership")
        ref["hash"] = ref["pin"]["hash"] = digest
        next(item for item in body["bindings"] if item["role"] == "membership")["hash"] = digest
        _, projection = project(body, definition, docs)
        assert (
            next(
                item
                for item in json.loads(projection.canonical_bytes)["refs"]
                if item["ref_kind"] == "membership"
            )["hash"]
            == digest
        )
        request_hashes.append(projection.request_hash)
    assert request_hashes[0] != request_hashes[1]


def test_derived_pin_does_not_replace_existing_spec_hash_or_ordinals() -> None:
    body, definition, docs = fixture(rich=True)
    spec = definition.derived_series[0]
    inputs = []
    for ordinal, binding in enumerate(spec.input_bindings):
        transport: Document = {"bindings": [], "refs": []}
        add_ref(transport, "macro", binding.dataset_id)
        ref = transport["refs"][0]
        ref["ref_version"] = ref["pin"]["version"] = binding.dataset_version
        inputs.append({"ordinal": ordinal, "field": binding.field, "pin": ref})
    doc = {
        "schema": "aas-derived-definition-v1",
        "hash_format": J,
        "id": spec.series_id,
        "version": "1",
        "definition": asdict(spec),
        "inputs": inputs,
    }
    whole_hash = content_sha256(doc)
    assert whole_hash != spec.canonical_sha256
    ref = next(item for item in body["refs"] if item["ref_kind"] == "derived")
    ref["hash"] = ref["pin"]["hash"] = whole_hash
    next(item for item in body["bindings"] if item["role"] == "derived")["hash"] = whole_hash
    before = canonical_json_bytes(doc)
    _, projection = project(body, definition, docs)
    assert canonical_json_bytes(doc) == before
    assert spec.canonical_sha256 == rich_contract().derived_series[0].canonical_sha256
    assert [(row["ordinal"], row["field"]) for row in inputs] == [(0, "price"), (1, "addend_a")]
    assert (
        next(
            item
            for item in json.loads(projection.canonical_bytes)["refs"]
            if item["ref_kind"] == "derived"
        )["hash"]
        == whole_hash
    )


def test_same_ref_across_roles_is_one_descriptor() -> None:
    body, definition, docs = fixture()
    signal = next(item for item in body["bindings"] if item["role"] == "signal_prices")
    sessions = next(item for item in body["bindings"] if item["role"] == "sessions")
    old_id = sessions["ref_id"]
    # Wire-level reuse is legal; actual generation domains remain T18's admission.
    sessions.update({key: value for key, value in signal.items() if key not in ("role", "ordinal")})
    body["refs"] = [item for item in body["refs"] if item["ref_id"] != old_id]
    project(body, definition, docs)
    duplicate = next(item for item in body["refs"] if item["ref_id"] == signal["ref_id"])
    body["refs"].append(dict(duplicate))
    with pytest.raises(ValueError, match="duplicate reference"):
        project(body, definition, docs)


def test_exact_machine_schema_inventories_and_frozen_constants() -> None:
    body, definition, docs = fixture(rich=True)
    _, projection = project(body, definition, docs)
    prepare_schema = json.loads(canonical_json_bytes(PREPARE_REQUEST_SCHEMA))
    request_schema = json.loads(canonical_json_bytes(BACKTEST_REQUEST_SCHEMA))
    assert set(prepare_schema["required"]) == body.keys()
    assert set(request_schema["required"]) == json.loads(projection.canonical_bytes).keys()
    assert prepare_schema["properties"]["strategy"]["properties"]["raw_hash_format"] == {"const": B}
    assert request_schema["properties"]["conventions"]["items"]["properties"]["hash_format"] == {
        "const": J
    }
    assert set(prepare_schema["properties"]["history"]["required"]) == {"start", "end"}
    assert prepare_schema["additionalProperties"] is request_schema["additionalProperties"] is False
    with pytest.raises(TypeError):
        operator.setitem(
            cast("MutableMapping[str, object]", PREPARE_REQUEST_SCHEMA), "type", "array"
        )


def test_missing_history_and_v2_flow_order_are_not_repaired() -> None:
    body, _, _ = fixture(version="v2")
    del body["history"]
    with pytest.raises(ValueError, match="missing or unknown"):
        parse_prepare_request(canonical_json_bytes(body))
    body, _, _ = fixture(version="v2")
    body["account"]["cashflows"] = [{"date": "2026-02-02", "amount": 10}] * 2
    with pytest.raises(ValueError, match="unique increasing"):
        parse_prepare_request(canonical_json_bytes(body))


def test_source_pins_sorted_without_digest_reinterpretation() -> None:
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    first = {
        "source_id": "a",
        "source_sha256": "a" * 64,
        "table": "prices",
        "table_digest": "b" * 64,
    }
    second = {
        "source_id": "b",
        "source_sha256": "c" * 64,
        "table": "prices",
        "table_digest": "d" * 64,
    }
    exported = export_envelope(
        parsed,
        projection=projection,
        inputs=EnvelopeInputs(
            dates=DATES,
            opens=({}, {"ASSET_A": 10.0}),
            closes=({"ASSET_A": 10.0}, {"ASSET_A": 12.0}),
            targets={DATES[0]: {"ASSET_A": 0.5}},
            instrument_types={"ASSET_A": "ETF", "REFERENCE_INDEX": "INDEX"},
            source_pins=(second, first),
        ),
    )
    assert json.loads(exported.canonical_bytes)["source_pins"] == [first, second]
    result = run_document(exported.canonical_bytes, exported.envelope_sha256)
    assert result["source_pins"] == [first, second]
    assert result["source_pins_verified"] is False


def _two_asset_inputs(targets: dict[str, float]) -> EnvelopeInputs:
    prices = {"ASSET_A": 10.0, "ASSET_B": 10.0}
    return EnvelopeInputs(
        dates=DATES,
        opens=({}, dict(prices)),
        closes=(dict(prices), {"ASSET_A": 12.0, "ASSET_B": 12.0}),
        targets={DATES[0]: targets},
        instrument_types={"ASSET_A": "ETF", "ASSET_B": "ETF"},
        source_pins=(),
    )


def test_fully_invested_target_one_ulp_above_one_exports_and_replays() -> None:
    """Normalized ensemble weights can exceed one by a single binary64 ULP."""
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    targets = {"ASSET_A": 0.5, "ASSET_B": 0.5 + 2**-52}
    # This is exactly the input the previous strict comparison rejected.
    assert math.fsum(targets.values()) == 1 + 2**-52
    assert math.fsum(targets.values()) > 1
    exported = export_envelope(parsed, projection=projection, inputs=_two_asset_inputs(targets))
    # Replay has to accept the same bytes, or the rejection only moves downstream.
    replayed = run_document(exported.canonical_bytes, exported.envelope_sha256)
    # Replay buys both assets and invests the whole account, which is what a target
    # summing to one means; the extra ULP shows up as fractional shares, not a refusal.
    result = cast("dict[str, Any]", replayed["result"])
    assert [fill["symbol"] for fill in result["fills"]] == ["ASSET_A", "ASSET_B"]
    assert result["nav"][-1]["cash"] == 0.0
    assert result["nav"][-1]["equity"] > 0


def test_target_sum_above_the_shared_tolerance_is_still_rejected() -> None:
    """The tolerance admits rounding, not a real overweight."""
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    targets = {"ASSET_A": 0.5, "ASSET_B": 0.5 + 2e-12}
    assert math.fsum(targets.values()) > 1 + long_only_sum_tolerance(len(targets))
    with pytest.raises(ValueError, match="sum to at most one"):
        export_envelope(parsed, projection=projection, inputs=_two_asset_inputs(targets))


def test_export_rejects_what_replay_cannot_absorb() -> None:
    """Export must not accept an overshoot the self-financing check would refuse."""
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    # A relative overshoot equal to the accounting tolerance leaves no room for the
    # rounding that follows it, so an envelope carrying it failed replay after a
    # successful export. The accepted boundary now scales with the weight count.
    targets = {"ASSET_A": 0.5, "ASSET_B": 0.5 + 1e-12}
    assert math.fsum(targets.values()) > 1 + long_only_sum_tolerance(len(targets))
    with pytest.raises(ValueError, match="sum to at most one"):
        export_envelope(parsed, projection=projection, inputs=_two_asset_inputs(targets))


def test_accepted_overshoot_stays_inside_the_self_financing_tolerance() -> None:
    """Whatever export admits, replay must still be able to absorb."""
    for count in (2, 10, 100, 1000, 5000, 100_000):
        assert long_only_sum_tolerance(count) < execution._VALUE_RELATIVE_TOLERANCE  # noqa: SLF001


def test_zero_weight_padding_cannot_widen_the_accepted_band() -> None:
    """The entry count is caller-supplied, so it must not buy a wider tolerance."""
    body, definition, docs = fixture()
    parsed, projection = project(body, definition, docs)
    # Thousands of zero weights contribute no rounding but inflate the count. Left
    # unbounded the scaled tolerance passes 1e-12, and such an envelope exported
    # successfully and then failed replay with a self-financing error.
    padded = {"ASSET_A": 0.5, "ASSET_B": 0.5 + 1e-12}
    padded.update({f"PAD_{index}": 0.0 for index in range(4998)})
    assert long_only_sum_tolerance(len(padded)) < execution._VALUE_RELATIVE_TOLERANCE  # noqa: SLF001
    with pytest.raises(ValueError, match="sum to at most one"):
        export_envelope(parsed, projection=projection, inputs=_two_asset_inputs(padded))
