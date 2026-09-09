from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.application.proxy_cli import run_document


def document() -> dict[str, object]:
    shared = {
        "currency": "USD",
        "return_kind": "total_return",
        "close_convention": "synthetic_session_close",
        "net_of_fees": True,
        "source_sha256": "a" * 64,
    }
    return {
        "schema_version": "aas-proxy-returns-v1",
        "module": "aegis",
        "target_type": "ETF",
        "donor": {
            **shared,
            "instrument_id": "DONOR",
            "anchor_date": "2024-01-01",
            "dates": ["2024-01-02"],
            "returns": [0.1],
        },
        "target": {
            **shared,
            "instrument_id": "TARGET",
            "anchor_date": "2024-01-02",
            "dates": ["2024-01-03"],
            "returns": [0.2],
        },
        "recipe": {
            "target_id": "TARGET",
            "donor_id": "DONOR",
            "switch_date": "2024-01-02",
            "annual_fee": 0,
            "fee_model": "already_net",
            "reason": "Synthetic equivalence exercise",
        },
    }


def run(body: dict[str, object]) -> dict[str, object]:
    raw = json.dumps(body).encode()
    return run_document(raw, hashlib.sha256(raw).hexdigest())


def test_cli_returns_index_without_trades_or_provenance_promotion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "proxy.json"
    path.write_text(json.dumps(document()))
    assert (
        main(
            [
                "proxy",
                "--input",
                str(path),
                "--sha256",
                hashlib.sha256(path.read_bytes()).hexdigest(),
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["result"]["points"][-1]["index_value"] == pytest.approx(132)
    assert result["result"]["points"][-1]["date"] == "2024-01-03"
    assert result["result"]["research_only"]
    assert result["result"]["non_executable"]
    assert not result["result"]["source_pins_verified"]
    assert not result["result"]["point_in_time_verified"]
    assert not result["instrument_type_verified"]
    assert not result["live_orders"]
    assert "fills" not in result["result"]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("module", "hedge", "schema or module"),
        ("target_type", "SPOT", "ETF"),
        ("extra", True, "unknown"),
    ],
)
def test_wrong_scope_or_unknown_fields_rejected(field: str, value: object, message: str) -> None:
    body = document()
    body[field] = value
    with pytest.raises(ValueError, match=message):
        run(body)


@pytest.mark.parametrize(
    ("field", "value"),
    [("return_kind", "price_return"), ("currency", "KRW"), ("close_convention", "different_close")],
)
def test_incompatible_series_rejected(field: str, value: str) -> None:
    body = document()
    cast("dict[str, object]", body["target"])[field] = value
    with pytest.raises(ValueError, match=r"match|differ|compatible"):
        run(body)


def test_hash_precedes_json_decode() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        run_document(b"not JSON", "0" * 64)


def test_nested_unknown_fields_rejected() -> None:
    body = document()
    cast("dict[str, object]", body["recipe"])["point_in_time_verified"] = True
    with pytest.raises(ValueError, match="unknown"):
        run(body)


def test_input_alias_rejected(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    raw = json.dumps(document()).encode()
    source, alias = tmp_path / "input.json", tmp_path / "alias.json"
    source.write_bytes(raw)
    alias.symlink_to(source)
    assert main(["proxy", "--input", str(alias), "--sha256", hashlib.sha256(raw).hexdigest()]) == 1
    assert capsys.readouterr().err


@pytest.mark.parametrize("value", [None, True, "0.1"])
def test_return_must_be_a_number(value: object) -> None:
    body = deepcopy(document())
    cast("dict[str, object]", body["donor"])["returns"] = [value]
    with pytest.raises(ValueError, match="finite"):
        run(body)
