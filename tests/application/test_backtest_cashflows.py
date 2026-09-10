"""Versioned cashflow inputs and complete legacy response compatibility."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from aegis_alpha.application import backtest_cli
from aegis_alpha.application.cli import main
from tests.application.test_backtest_cli import document


def _run(body: dict[str, object]) -> dict[str, object]:
    raw = json.dumps(body).encode()
    return backtest_cli.run_document(raw, hashlib.sha256(raw).hexdigest())


def _v2() -> dict[str, object]:
    body = document()
    body.update(
        schema_version="aas-etf-backtest-v2",
        opens=[{"ETF-A": 10.0}, {"ETF-A": 10.0}, {"ETF-A": 20.0}],
        closes=[{"ETF-A": 10.0}, {"ETF-A": 10.0}, {"ETF-A": 22.0}],
        targets={"2024-01-02": {"ETF-A": 1.0}, "2024-01-03": {"ETF-A": 1.0}},
        cashflows=[{"date": "2024-01-04", "amount": 100.0}],
    )
    return body


def test_v1_complete_response_stays_compatible() -> None:
    body = document()
    body["targets"] = {}
    digest = hashlib.sha256(json.dumps(body).encode()).hexdigest()
    assert _run(body) == {
        "module": "aegis",
        "input_sha256": digest,
        "research_mode": "synthetic",
        "execution_convention": "decision close to next supplied session open",
        "source_pins": [],
        "source_pins_verified": False,
        "observed_prices_verified": False,
        "point_in_time_verified": False,
        "live_orders": False,
        "result": {
            "nav": [
                {"date": day, "equity": 100.0, "cash": 100.0, "fee": 0.0}
                for day in ("2024-01-02", "2024-01-03", "2024-01-04")
            ],
            "fills": [],
        },
    }


def test_v2_cli_deposit_does_not_become_investment_return(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = json.dumps(_v2()).encode()
    path = tmp_path / "cashflows.json"
    path.write_bytes(raw)
    assert (
        main(["backtest", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 0
    )
    response = json.loads(capsys.readouterr().out)
    result = response["result"]
    assert [x["equity"] for x in result["account"]["nav"]] == pytest.approx([100, 100, 330])
    assert [x["unit_value"] for x in result["unit_nav"]] == pytest.approx([1, 1, 2.2])
    assert [x["units"] for x in result["unit_nav"]] == pytest.approx([100, 100, 150])
    assert result["cashflows"] == [{"date": "2024-01-04", "amount": 100.0}]
    assert "withdrawals use existing cash only" in response["cashflow_convention"]
    for flag in (
        "source_pins_verified",
        "observed_prices_verified",
        "point_in_time_verified",
        "live_orders",
    ):
        assert response[flag] is False


def test_v2_empty_flows_preserve_account_result() -> None:
    old = document()
    new = dict(old, schema_version="aas-etf-backtest-v2", cashflows=[])
    result = _run(new)["result"]
    assert isinstance(result, dict)
    assert result["account"] == _run(old)["result"]


@pytest.mark.parametrize("asset_type", ["INDEX", "SPOT", None])
def test_v2_still_requires_etf_targets(asset_type: str | None) -> None:
    body = _v2()
    body["instrument_types"] = {} if asset_type is None else {"ETF-A": asset_type}
    with pytest.raises(ValueError, match="ETF instrument type"):
        _run(body)


@pytest.mark.parametrize(
    "flows",
    [
        None,
        {},
        [{"date": "2024-01-04"}],
        [{"date": "2024-01-04", "amount": 100, "currency": "USD"}],
        [{"date": "2024-01-04", "amount": True}],
        [{"date": "2024-01-04", "amount": "100"}],
        [{"date": "20240104", "amount": 100}],
        [{"date": "2024-01-04T00:00:00", "amount": 100}],
        [{"date": "2024-01-02", "amount": 100}],
        [{"date": "2024-01-05", "amount": 100}],
        [{"date": "2024-01-04", "amount": 0}],
        [{"date": "2024-01-04", "amount": float("inf")}],
        [{"date": "2024-01-04", "amount": 10**400}],
        [{"date": "2024-01-03", "amount": 1}] * 2,
        [{"date": "2024-01-04", "amount": 1}, {"date": "2024-01-03", "amount": 1}],
    ],
)
def test_v2_rejects_ambiguous_or_invalid_flows(flows: object) -> None:
    body = _v2()
    body["cashflows"] = flows
    with pytest.raises((ValueError, TypeError)):
        _run(body)


def test_schema_fields_and_flow_count_fail_before_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from aegis_alpha.engine import execution  # noqa: PLC0415 -- replace constructor at boundary

    def unexpected(*_args: object) -> None:
        pytest.fail("cashflow constructed before count validation")

    monkeypatch.setattr(execution, "CashFlow", unexpected)
    body = _v2()
    body["cashflows"] = [None] * 3
    with pytest.raises(ValueError, match="session count"):
        _run(body)
    body = document()
    body["cashflows"] = []
    with pytest.raises(ValueError, match="unknown fields"):
        _run(body)
    body = _v2()
    del body["cashflows"]
    with pytest.raises(ValueError, match="unknown fields"):
        _run(body)


def test_v2_hash_and_duplicate_json_keys_fail() -> None:
    raw = json.dumps(_v2()).encode()
    with pytest.raises(ValueError, match="SHA-256"):
        backtest_cli.run_document(raw, "0" * 64)
    duplicate = raw.replace(b'"amount": 100.0', b'"amount": 100.0, "amount": 200.0')
    with pytest.raises(ValueError, match="duplicate"):
        backtest_cli.run_document(duplicate, hashlib.sha256(duplicate).hexdigest())


def test_v2_insufficient_cash_is_a_cli_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    body = _v2()
    body["cashflows"] = [{"date": "2024-01-04", "amount": -1}]
    raw = json.dumps(body).encode()
    path = tmp_path / "withdraw.json"
    path.write_bytes(raw)
    assert (
        main(["backtest", "--input", str(path), "--sha256", hashlib.sha256(raw).hexdigest()]) == 1
    )
    output = capsys.readouterr()
    assert not output.out
    assert "error" in json.loads(output.err)
