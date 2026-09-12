"""ETF-only explicit input boundary with independently calculated results."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.cli import main


def document() -> dict[str, object]:
    return {
        "schema_version": "aas-etf-backtest-v1",
        "module": "aegis",
        "instrument_types": {"ETF-A": "ETF"},
        "dates": ["2024-01-02", "2024-01-03", "2024-01-04"],
        "opens": [{"ETF-A": 10.0}, {"ETF-A": 20.0}, {"ETF-A": 24.0}],
        "closes": [{"ETF-A": 10.0}, {"ETF-A": 22.0}, {"ETF-A": 26.0}],
        "targets": {"2024-01-02": {"ETF-A": 1.0}},
        "initial_cash": 100.0,
        "cost": 0.0,
        "source_pins": [],
        "research_mode": "synthetic",
    }


def test_cli_uses_next_open_and_does_not_certify_supplied_prices(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = json.dumps(document()).encode()
    path = tmp_path / "request.json"
    path.write_bytes(raw)
    assert (
        main(["backtest", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert [row["equity"] for row in result["result"]["nav"]] == [100.0, 110.0, 130.0]
    assert result["result"]["fills"][0]["price"] == 20.0  # noqa: PLR2004 -- independent fixture open
    assert result["result"]["fills"][0]["execution_date"] == "2024-01-03"
    assert result["source_pins_verified"] is False
    assert result["observed_prices_verified"] is False
    assert result["live_orders"] is False


@pytest.mark.parametrize("asset_type", ["INDEX", "SPOT", None])
def test_non_etf_positive_target_rejected(asset_type: str | None) -> None:
    body = document()
    body["instrument_types"] = {} if asset_type is None else {"ETF-A": asset_type}
    raw = json.dumps(body).encode()
    with pytest.raises(ValueError, match="ETF instrument type"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


@pytest.mark.parametrize(("field", "value"), [("initial_cash", True), ("cost", False)])
def test_boolean_numbers_rejected(field: str, *, value: bool) -> None:
    body = document()
    body[field] = value
    raw = json.dumps(body).encode()
    with pytest.raises(TypeError, match="not a boolean"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


def test_changed_bytes_and_unknown_fields_rejected() -> None:
    raw = json.dumps(document()).encode()
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        run_document(raw, "0" * 64)
    body = document()
    body["live_orders"] = True
    raw = json.dumps(body).encode()
    with pytest.raises(ValueError, match="unknown fields"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


def test_numerical_overflow_is_reported_as_cli_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    body = document()
    body["opens"] = [{"ETF-A": 1e-308}] * 3
    body["closes"] = [{"ETF-A": 1e308}] * 3
    raw = json.dumps(body).encode()
    path = tmp_path / "overflow.json"
    path.write_bytes(raw)
    assert (
        main(["backtest", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 1
    )
    captured = capsys.readouterr()
    assert not captured.out
    assert "error" in json.loads(captured.err)


def test_relative_input_matches_absolute_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = json.dumps(document()).encode()
    (tmp_path / "backtest.json").write_bytes(raw)
    monkeypatch.chdir(tmp_path)
    assert (
        main(["backtest", "--input", "backtest.json", "--sha256", hashlib.sha256(raw).hexdigest()])
        == 0
    )
    assert json.loads(capsys.readouterr().out) == run_document(raw, hashlib.sha256(raw).hexdigest())


@pytest.mark.parametrize("directory_alias", [False, True])
def test_relative_input_alias_still_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    directory_alias: bool,
) -> None:
    raw = json.dumps(document()).encode()
    source = tmp_path / "original"
    source.mkdir()
    (source / "input.json").write_bytes(raw)
    alias = tmp_path / "alias"
    alias.symlink_to(source if directory_alias else source / "input.json")
    monkeypatch.chdir(tmp_path)
    name = "alias/input.json" if directory_alias else "alias"
    assert main(["backtest", "--input", name, "--sha256", hashlib.sha256(raw).hexdigest()]) == 1
    assert "error" in json.loads(capsys.readouterr().err)
