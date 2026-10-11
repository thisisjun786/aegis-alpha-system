"""Exchange-day splits/dividends and forex pair history: verified, normalized, never repaired."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_contracts import EOD_HISTORY_JSON_TOOL, QverisJob
from aegis_alpha.data.qveris_native import (
    normalize_bulk_actions,
    normalize_fx_history,
    normalize_price_history,
    read_completed_job,
)
from aegis_alpha.data.qveris_payloads import empty_action_day, validate_payload
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.qveris_support import (
    KR_IDENTITY,
    OBSERVED,
    ScriptedQveris,
    account_key,
    bar,
    bulk_job,
    complete,
    fx_job,
    history_job,
)

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

IDENTITIES = {
    symbol: {k: v for k, v in entry.items() if k != "name"} for symbol, entry in KR_IDENTITY.items()
}


def _read(root: Path, job: QverisJob):  # noqa: ANN202 -- CompletedHistory
    marker = root / "jobs" / job.fingerprint / "complete.json"
    return read_completed_job(
        root, job.fingerprint, hashlib.sha256(marker.read_bytes()).hexdigest()
    )


def test_forex_route_admits_only_currency_pairs_on_the_json_tool() -> None:
    assert fx_job().parameters["symbol"] == "USDKRW.FOREX"
    for symbol in ("USDKR.FOREX", "usdkrw.FOREX", "USDKRW.US"):
        with pytest.raises(ValueError, match=r"forex|currency pair"):
            QverisJob(
                "fx",
                EOD_HISTORY_JSON_TOOL,
                "eodhd",
                "FX",
                "fx_history",
                canonical_json_bytes(
                    {"symbol": symbol, "fmt": "json", "from": "2026-08-01", "order": "a"}
                ).decode(),
                OBSERVED,
            )
    with pytest.raises(ValueError, match="EODHD history"):
        QverisJob(
            "fx",
            EOD_HISTORY_JSON_TOOL,
            "eodhd",
            "FX",
            "price_history",
            fx_job().parameters_json,
            OBSERVED,
        )


def test_forex_history_keeps_the_pair_and_holds_bad_rows(tmp_path: Path) -> None:
    rows = [bar("2026-08-03", volume=0), bar("2026-08-04", low=20)]
    complete(tmp_path, fx_job(), rows)
    history = _read(tmp_path, fx_job())
    assert len(history.evidence) == 5  # noqa: PLR2004 -- complete.json and four page files
    normalized = normalize_fx_history(history)
    (row,) = normalized.rows
    assert (row["pair"], row["base_currency"], row["quote_currency"]) == ("USDKRW", "USD", "KRW")
    assert row["volume"] == 0
    assert "instrument_id" not in row
    (held,) = normalized.quarantine
    assert held["reason"] == "inconsistent_ohlc"
    with pytest.raises(ValueError, match="KR or US"):
        normalize_price_history(history, IDENTITIES)


def test_splits_keep_the_ratio_text_and_hold_unknown_identities(tmp_path: Path) -> None:
    job = bulk_job("US", "splits")
    rows = [
        {"code": "AAA", "exchange": "US", "date": "2026-08-31", "split": "1.000000/10.000000"},
        {"code": "ZZZ", "exchange": "US", "date": "2026-08-31", "split": "2/1"},
        {"code": "AAA2", "exchange": "US", "date": "2026-08-31", "split": "0/1"},
    ]
    identities = {**IDENTITIES, "AAA2.US": {**IDENTITIES["AAA.US"], "instrument_id": "two"}}
    complete(tmp_path, job, rows)
    normalized = normalize_bulk_actions(_read(tmp_path, job), identities)
    (row,) = normalized.rows
    assert row["split"] == "1.000000/10.000000"
    assert row["instrument_id"] == "synthetic-us-stock"
    assert [h["reason"] for h in normalized.quarantine] == [
        "explicit instrument identity is required",
        "invalid_split_ratio",
    ]


def test_dividends_keep_provider_text_and_hold_nonpositive_amounts(tmp_path: Path) -> None:
    job = bulk_job("US", "dividends")
    base = {"code": "AAA", "exchange": "US", "date": "2026-08-31", "currency": "USD"}
    rows = [
        {**base, "dividend": "0.27500", "unadjustedValue": "0.2750000000", "period": "Quarterly"},
        {**base, "code": "AAA2", "dividend": 0.069, "declarationDate": None},
        {**base, "code": "AAA3", "dividend": "-1"},
    ]
    identities = {
        **IDENTITIES,
        "AAA2.US": {**IDENTITIES["AAA.US"], "instrument_id": "two"},
        "AAA3.US": {**IDENTITIES["AAA.US"], "instrument_id": "three"},
    }
    complete(tmp_path, job, rows)
    normalized = normalize_bulk_actions(_read(tmp_path, job), identities)
    first, second = normalized.rows
    assert (first["dividend"], first["unadjusted_value"], first["period"]) == (
        "0.27500",
        "0.2750000000",
        "Quarterly",
    )
    assert (second["dividend"], second["declaration_date"]) == ("0.069", None)
    assert [h["reason"] for h in normalized.quarantine] == ["invalid_dividend"]


def test_an_empty_exchange_day_of_actions_is_a_completed_fact(tmp_path: Path) -> None:
    job = bulk_job("KO", "splits")
    complete(tmp_path, job, [])
    normalized = normalize_bulk_actions(_read(tmp_path, job), IDENTITIES)
    assert normalized.rows == normalized.quarantine == ()


def test_a_warned_download_is_held_whole_and_recorded(tmp_path: Path) -> None:
    job = bulk_job("US", "dividends")
    row = {
        "code": "AAA",
        "exchange": "US",
        "date": "2026-08-31",
        "dividend": "1",
        "currency": "USD",
    }
    complete(tmp_path, job, [row], warning=True)
    history = _read(tmp_path, job)
    assert history.provider_warning
    normalized = normalize_bulk_actions(history, IDENTITIES)
    assert not normalized.rows
    assert [h["reason"] for h in normalized.quarantine] == ["provider_reported_partial"]


def test_us_single_instrument_history_uses_the_shared_bar_rules(tmp_path: Path) -> None:
    job = history_job("AAA.US", "US")
    complete(tmp_path, job, [bar("2026-08-03"), bar("2026-08-04", volume=-1)])
    normalized = normalize_price_history(_read(tmp_path, job), IDENTITIES)
    assert [row["provider_symbol"] for row in normalized.rows] == ["AAA.US"]
    assert [h["reason"] for h in normalized.quarantine] == ["invalid_price_or_volume"]
    with pytest.raises(ValueError, match="splits or dividends"):
        normalize_bulk_actions(_read(tmp_path, job), IDENTITIES)


def _empty_result(root: Path, job: QverisJob) -> ScriptedQveris:
    client = ScriptedQveris({}, account_key(root))
    client.failure = "empty-result"
    acquire_jobs((job,), root, client)
    return client


@pytest.mark.parametrize("dataset", ["splits", "dividends"])
def test_an_uncharged_empty_result_is_an_exchange_day_without_actions(
    tmp_path: Path, dataset: str
) -> None:
    job = bulk_job("KO", dataset)
    client = _empty_result(tmp_path, job)
    marker = json.loads((tmp_path / "jobs" / job.fingerprint / "complete.json").read_bytes())
    assert (marker["status"], marker["rows"], marker["settled_credits"]) == (
        "RAW_ACQUIRED",
        0,
        "0.0",
    )
    assert "gateway_warnings" not in marker
    history = _read(tmp_path, job)
    assert not history.provider_warning
    normalized = normalize_bulk_actions(history, IDENTITIES)
    assert normalized.rows == normalized.quarantine == ()
    # The completion is reused from its pinned pages without another paid call.
    acquire_jobs((job,), tmp_path, client)
    assert client.execute_count == 1


def test_an_empty_result_for_prices_stays_a_failure(tmp_path: Path) -> None:
    job = bulk_job("KO", "prices")
    with pytest.raises(ValueError, match="did not succeed"):
        _empty_result(tmp_path, job)
    assert not (tmp_path / "jobs" / job.fingerprint / "complete.json").exists()


EMPTY_USAGE: dict[str, object] = {
    "execution_id": "exec-1",
    "outcome": "empty_result",
    "reason_code": "result.empty",
    "charge_outcome": "failed_not_charged",
    "actual_amount_credits": "0.0",
    "settled_amount_credits": "0.0",
}
EMPTY_RESPONSE: dict[str, object] = {
    "execution_id": "exec-1",
    "success": False,
    "result": {"status_code": 200, "data": []},
}


def test_only_the_exact_uncharged_empty_answer_is_admitted() -> None:
    job = bulk_job("KQ", "dividends")
    assert empty_action_day(job, EMPTY_RESPONSE, EMPTY_USAGE)
    assert validate_payload(job, job.parameters, EMPTY_RESPONSE, usage=EMPTY_USAGE).rows == 0
    with pytest.raises(ValueError, match="did not succeed"):
        validate_payload(job, job.parameters, EMPTY_RESPONSE)
    result = EMPTY_RESPONSE["result"]
    assert isinstance(result, dict)
    refused_responses = [
        {**EMPTY_RESPONSE, "success": True},
        {**EMPTY_RESPONSE, "execution_id": "exec-2"},
        {**EMPTY_RESPONSE, "result": {**result, "status_code": 500}},
        {**EMPTY_RESPONSE, "result": {**result, "data": "no rows"}},
        {
            **EMPTY_RESPONSE,
            "result": {
                **result,
                "data": [
                    {
                        "code": "A",
                        "exchange": "KQ",
                        "date": "2026-08-31",
                        "dividend": "1",
                        "currency": "KRW",
                    }
                ],
            },
        },
        {**EMPTY_RESPONSE, "result": {**result, "truncated": True}},
    ]
    for response in refused_responses:
        assert not empty_action_day(job, response, EMPTY_USAGE), response
    refused_usage = [
        {**EMPTY_USAGE, "outcome": "partial_success"},
        {**EMPTY_USAGE, "reason_code": "provider.error"},
        {**EMPTY_USAGE, "charge_outcome": "charged"},
        {**EMPTY_USAGE, "actual_amount_credits": "2.81"},
        {**EMPTY_USAGE, "settled_amount_credits": "0.01"},
        {**EMPTY_USAGE, "settled_amount_credits": None},
        {**EMPTY_USAGE, "execution_id": "exec-2"},
    ]
    for usage in refused_usage:
        assert not empty_action_day(job, EMPTY_RESPONSE, usage), usage
    for other in (bulk_job("KQ", "prices"), fx_job(), history_job()):
        assert not empty_action_day(other, EMPTY_RESPONSE, EMPTY_USAGE)
