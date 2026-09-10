from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.application.etf_cli import run_document

_CAP = 256
_EXPECTED_FEE_SAVING_BPS = 15


def profile(
    instrument_id: str = "CURRENT",
    fee_bps: float = 20,
    tracking_value: float = 0.03,
    tracking_hash: str = "b" * 64,
    basis: str = "net_total_return",
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "exposure_id": "EXPOSURE",
        "currency": "USD",
        "hedged": False,
        "leverage": 1,
        "reset": "none",
        "fee_bps": fee_bps,
        "inception": "2010-01-01",
        "as_of": "2021-01-05",
        "source_hash": "a" * 64,
        "tracking": {
            "start": "2020-01-02",
            "end": "2020-12-31",
            "value": tracking_value,
            "source_hash": tracking_hash,
            "basis": basis,
        },
        "liquidity": 1000,
    }


def document() -> dict[str, object]:
    return {
        "schema_version": "aas-etf-comparison-v1",
        "module": "aegis",
        "action": "compare",
        "current": profile("CURRENT", 20),
        "candidates": [profile("CANDIDATE", 5)],
        "policy": {
            "as_of": "2021-01-05",
            "max_profile_age_days": 10,
            "min_liquidity": 100,
            "min_fee_saving_bps": 2,
            "max_tracking_error": 0.04,
            "tracking_start": "2020-01-02",
            "tracking_end": "2020-12-31",
            "tracking_basis": "net_total_return",
        },
    }


def run(body: dict[str, object]) -> dict[str, object]:
    raw = json.dumps(body).encode()
    return run_document(raw, hashlib.sha256(raw).hexdigest())


def test_cli_compares_supplied_evidence_with_research_flags(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = json.dumps(document()).encode()
    path = tmp_path / "etfs.json"
    path.write_bytes(raw)
    assert main(["etfs", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "aas-etf-comparison-result-v1"
    assert result["module"] == "aegis"
    assert result["action"] == "compare"
    assert result["research_only"] is True
    assert result["automatic_replacement"] is False
    assert result["source_pins_verified"] is False
    decisions = result["result"]["decisions"]
    assert decisions[0]["instrument_id"] == "CANDIDATE"
    assert decisions[0]["status"] == "matched"
    assert decisions[0]["fee_saving_bps"] == _EXPECTED_FEE_SAVING_BPS


def test_null_optional_evidence_is_insufficient_not_an_error() -> None:
    body = document()
    current = cast("dict[str, object]", body["current"])
    current["tracking"] = None
    result = json.loads(json.dumps(run(body)))
    decisions = result["result"]["decisions"]
    assert decisions[0]["status"] == "insufficient_evidence"
    assert decisions[0]["reason"] == "reference_missing_tracking"


def test_tracking_basis_mismatch_is_reported_not_ranked() -> None:
    body = document()
    candidates = cast("list[dict[str, object]]", body["candidates"])
    candidate_tracking = cast("dict[str, object]", candidates[0]["tracking"])
    candidate_tracking["basis"] = "fund_market_tr_after_expenses_vs_index_gross_tr"
    result = json.loads(json.dumps(run(body)))
    decisions = result["result"]["decisions"]
    assert decisions[0]["status"] == "insufficient_evidence"
    assert decisions[0]["reason"] == "candidate_tracking_basis_mismatch"


def test_duplicate_stable_identity_is_rejected() -> None:
    body = document()
    candidates = cast("list[dict[str, object]]", body["candidates"])
    candidates[0]["instrument_id"] = "CURRENT"
    with pytest.raises(ValueError, match="duplicate stable"):
        run(body)


@pytest.mark.parametrize(
    "path",
    [
        ("unknown",),
        ("current", "unknown"),
        ("policy", "unknown"),
    ],
)
def test_unknown_fields_are_rejected(path: tuple[str, ...]) -> None:
    body = document()
    target: dict[str, object] = body
    for key in path[:-1]:
        target = cast("dict[str, object]", target[key])
    target[path[-1]] = True
    with pytest.raises(ValueError, match="missing or unknown"):
        run(body)


def test_missing_field_is_rejected() -> None:
    body = document()
    current = cast("dict[str, object]", body["current"])
    del current["liquidity"]
    with pytest.raises(ValueError, match="missing or unknown"):
        run(body)


def test_hash_precedes_decode_and_duplicate_keys_are_rejected() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        run_document(b"not JSON", "0" * 64)
    raw = b'{"schema_version":"aas-etf-comparison-v1","schema_version":"aas-etf-comparison-v1"}'
    with pytest.raises(ValueError, match="duplicate"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


def test_unsupported_schema_module_or_action_is_rejected() -> None:
    for field, value in (
        ("schema_version", "aas-etf-comparison-v0"),
        ("module", "alpha"),
        ("action", "replace"),
    ):
        body = document()
        body[field] = value
        with pytest.raises(ValueError, match="unsupported"):
            run(body)


@pytest.mark.parametrize(
    ("path", "value", "error", "message"),
    [
        (("current", "hedged"), 1, TypeError, "boolean"),
        (("current", "leverage"), True, TypeError, "numbers"),
        (("current", "as_of"), "20210105", ValueError, "YYYY-MM-DD"),
        (("policy", "max_profile_age_days"), True, TypeError, "integer"),
    ],
)
def test_invalid_field_types_are_rejected(
    path: tuple[str, ...], value: object, error: type[Exception], message: str
) -> None:
    body = deepcopy(document())
    target: dict[str, object] = body
    for key in path[:-1]:
        target = cast("dict[str, object]", target[key])
    target[path[-1]] = value
    with pytest.raises(error, match=message):
        run(body)


def test_candidates_cap_and_input_size_are_rejected_before_generation() -> None:
    body = document()
    body["candidates"] = [profile(f"CANDIDATE-{index}", 5) for index in range(_CAP + 1)]
    with pytest.raises(ValueError, match="cap of 256"):
        run(body)
    raw = b"x" * (64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="byte limit"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


@pytest.mark.parametrize(("section", "key"), [("current", "fee_bps"), ("policy", "min_liquidity")])
def test_cli_oversized_integer_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], section: str, key: str
) -> None:
    body = document()
    cast("dict[str, object]", body[section])[key] = 10**400
    raw = json.dumps(body).encode()
    path = tmp_path / "overflow.json"
    path.write_bytes(raw)
    assert main(["etfs", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 1
    captured = capsys.readouterr()
    assert "error" in json.loads(captured.err)
    assert "Traceback" not in captured.err
    assert not captured.out
