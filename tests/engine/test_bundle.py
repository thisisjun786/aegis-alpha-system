"""External bytes, explicit parameters, and independent allocation answers."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from engine_support import bundle, contract, raw_bundle, request

from aegis_alpha.engine import (
    BundleIdentityError,
    load_bundle,
    replay,
    serialize_bundle,
    sha256_bytes,
)


def test_complete_external_bundle_without_macro_or_momentum_recipe() -> None:
    source = bundle(contract())
    receipt = replay(source, request())
    assert dict(receipt.ensemble) == {"ASSET_A": 1.0}
    assert (receipt.bundle_id, receipt.bundle_version, receipt.source_sha256) == (
        "synthetic-probe",
        "1",
        sha256_bytes(raw_bundle(contract())),
    )
    assert receipt.contract_sha256 == source.contract_sha256
    encoded = serialize_bundle(source)
    reloaded = load_bundle(encoded, sha256_bytes(encoded), "synthetic-probe", "1")
    assert reloaded.contract == source.contract
    assert replay(reloaded, request()).ensemble == receipt.ensemble
    with pytest.raises(TypeError):
        source.contract.pack[0].offensive_config["top_n"] = 2  # ty: ignore[invalid-assignment] -- prove runtime immutability


def test_parameter_change_changes_answer() -> None:
    value = contract()
    record = replace(value.pack[0], offensive_config={**value.pack[0].offensive_config, "top_n": 2})
    result = replay(bundle(replace(value, pack=(record,))), request())
    assert dict(result.ensemble) == {"ASSET_A": 0.5, "ASSET_B": 0.5}


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("unexpected", True),
        ("assets", [True]),
        ("assets", ["A", "A"]),
        ("top_n", True),
        ("top_n", 0),
        ("strategy_type", "eval"),
        ("scoring", {"method": "return_rate", "horizon": 2, "unknown": 1}),
        ("scoring", {"method": "return_rate"}),
    ],
)
def test_nested_config_rejected(key: str, value: object) -> None:
    payload = json.loads(raw_bundle(contract()))
    payload["contract"]["pack"][0]["offensive_config"][key] = value
    raw = json.dumps(payload).encode()
    with pytest.raises((ValueError, TypeError)):
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


@pytest.mark.parametrize("field", ["pack", "feature_matrix", "macro_signals", "derived_series"])
def test_missing_contract_field_rejected(field: str) -> None:
    payload = json.loads(raw_bundle(contract()))
    del payload["contract"][field]
    raw = json.dumps(payload).encode()
    with pytest.raises(ValueError, match="required"):
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


@pytest.mark.parametrize(
    ("identity", "version", "digest"),
    [
        ("wrong", "1", None),
        ("synthetic-probe", "2", None),
        ("synthetic-probe", "1", "a" * 64),
    ],
)
def test_expected_identity_and_hash_enforced(
    identity: str, version: str, digest: str | None
) -> None:
    raw = raw_bundle(contract())
    with pytest.raises(ValueError, match=r"match"):
        load_bundle(raw, digest or sha256_bytes(raw), identity, version)


@pytest.mark.parametrize("kind", ["unknown_schema", "unknown_contract", "duplicate", "nonfinite"])
def test_unsupported_envelope_and_json_rejected(kind: str) -> None:
    payload = json.loads(raw_bundle(contract()))
    if kind == "unknown_schema":
        payload["schema_version"] = "unsupported"
    elif kind == "unknown_contract":
        payload["contract"]["contract_version"] = "unsupported"
    raw = json.dumps(payload).encode()
    if kind == "duplicate":
        raw = raw.replace(b'"bundle_version": "1"', b'"bundle_version": "1", "bundle_version": "2"')
    elif kind == "nonfinite":
        raw = raw.replace(b'"top_n": 1', b'"top_n": NaN')
    with pytest.raises(ValueError, match=r"version|schema|duplicate|non-finite"):
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


def test_huge_json_integer_rejected_as_contract_error() -> None:
    from aegis_alpha.engine import ContractParseError  # noqa: PLC0415 -- specific boundary error

    payload = json.loads(raw_bundle(contract()))
    payload["contract"]["feature_matrix"]["momentum_scores"] = [
        {"name": "synthetic", "return_months": [2], "weights": [10**400], "divisor": 1}
    ]
    raw = json.dumps(payload).encode()
    with pytest.raises(ContractParseError, match="finite range"):
        load_bundle(raw, sha256_bytes(raw), "synthetic-probe", "1")


def test_missing_expected_identity_has_typed_error() -> None:
    from aegis_alpha.engine import BundleIdentityError  # noqa: PLC0415 -- specific boundary error

    raw = raw_bundle(contract())
    with pytest.raises(BundleIdentityError, match="identity fields"):
        load_bundle(raw, sha256_bytes(raw), None, "1")  # ty: ignore[invalid-argument-type] -- untyped caller boundary


def test_engine_errors_survive_generator_context_manager() -> None:
    from collections.abc import Iterator  # noqa: PLC0415 -- exception protocol regression
    from contextlib import contextmanager  # noqa: PLC0415 -- exception protocol regression

    @contextmanager
    def scope() -> Iterator[None]:
        yield

    raw = raw_bundle(contract())
    with pytest.raises(BundleIdentityError, match="SHA-256"), scope():
        load_bundle(raw, "a" * 64, "synthetic-probe", "1")
