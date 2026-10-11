"""Qveris's uncharged empty answer for an exchange day of actions: counted, never completed."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.application.maintain_qveris import raw_asks
from aegis_alpha.data.qveris_batch import collect_cohort
from aegis_alpha.data.qveris_contracts import QverisJob
from aegis_alpha.data.qveris_parallel_batch import collect_parallel_cohorts
from aegis_alpha.data.qveris_payloads import (
    QverisEmptyAnswerError,
    empty_action_day,
    validate_payload,
)
from tests.data.qveris_support import ScriptedQveris, account_key, bulk_job, fx_job, history_job
from tests.data.test_qveris_parallel import ConcurrentFake

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")


def _empty_client(root: Path) -> ScriptedQveris:
    client = ScriptedQveris({}, account_key(root))
    client.failure = "empty-result"
    return client


@pytest.mark.parametrize("dataset", ["splits", "dividends"])
def test_an_empty_answer_is_counted_empty_and_leaves_the_request_open(
    tmp_path: Path, dataset: str
) -> None:
    job = bulk_job("KO", dataset)
    client = _empty_client(tmp_path)
    result = collect_cohort((job,), tmp_path, client)
    assert (result["completed"], result["failed"], result["empty"], result["pending"]) == (
        0,
        0,
        1,
        0,
    )
    assert result["status"] == "RAW_ACQUIRED"
    assert result["failures"] == []
    assert result["empty_answers"] == [
        {"index": 0, "job_id": job.job_id, "fingerprint": job.fingerprint}
    ]
    assert not (tmp_path / "jobs" / job.fingerprint / "complete.json").exists()
    # Not a completion: the planner sees a settled ask without one and asks the request again.
    assert [ask.state for asks in raw_asks(tmp_path).values() for ask in asks] == ["failed"]
    # Resuming the same job re-reads its settled page and makes no second paid call.
    again = collect_cohort((job,), tmp_path, client)
    assert (again["empty"], again["failed"], client.execute_count) == (1, 0, 1)


def test_an_empty_answer_for_prices_stays_a_failure(tmp_path: Path) -> None:
    job = bulk_job("KO", "prices")
    result = collect_cohort((job,), tmp_path, _empty_client(tmp_path))
    assert (result["failed"], result["empty"], result["status"]) == (1, 0, "PARTIAL")
    assert result["empty_answers"] == []


def test_parallel_groups_count_empty_answers_apart_from_failures(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.account_key = account_key(tmp_path)
    client.failure = "empty-result"
    days = tuple(
        replace(bulk_job("KQ", "dividends", f"2026-08-{day}"), job_id=f"kq-div-{day}")
        for day in (27, 28)
    )
    result = collect_parallel_cohorts((days,), tmp_path, lambda: client, workers=2)
    assert (result["completed"], result["failed"], result["empty"], result["pending"]) == (
        0,
        0,
        2,
        0,
    )
    assert result["status"] == "RAW_ACQUIRED"
    assert not list((tmp_path / "jobs").rglob("complete.json"))


EMPTY_USAGE: dict[str, object] = {
    "execution_id": "exec-1",
    "outcome": "empty_result",
    "reason_code": "result.empty",
    "charge_outcome": "failed_not_charged",
    "success": False,
    "billable_success": False,
    "quantity": None,
    "actual_amount_credits": "0.0",
    "settled_amount_credits": "0.0",
}
EMPTY_RESPONSE: dict[str, object] = {
    "execution_id": "exec-1",
    "success": False,
    "result": {"status_code": 200, "data": []},
}
ROW = {"code": "A", "exchange": "KQ", "date": "2026-08-31", "dividend": "1", "currency": "KRW"}


def test_only_the_exact_uncharged_empty_answer_is_recognized() -> None:
    job = bulk_job("KQ", "dividends")
    assert empty_action_day(job, EMPTY_RESPONSE, EMPTY_USAGE)
    with pytest.raises(QverisEmptyAnswerError):
        validate_payload(job, job.parameters, EMPTY_RESPONSE, usage=EMPTY_USAGE)
    with pytest.raises(ValueError, match="did not succeed") as plain:
        validate_payload(job, job.parameters, EMPTY_RESPONSE)
    assert not isinstance(plain.value, QverisEmptyAnswerError)
    result = EMPTY_RESPONSE["result"]
    assert isinstance(result, dict)
    refused_responses: list[dict[str, object]] = [
        {**EMPTY_RESPONSE, "success": True},
        {**EMPTY_RESPONSE, "execution_id": "exec-2"},
        {**EMPTY_RESPONSE, "result": {**result, "status_code": 500}},
        {**EMPTY_RESPONSE, "result": {**result, "data": "no rows"}},
        {**EMPTY_RESPONSE, "result": {**result, "data": [ROW]}},
        {**EMPTY_RESPONSE, "result": {**result, "truncated": True}},
    ]
    for response in refused_responses:
        assert not empty_action_day(job, response, EMPTY_USAGE), response
    refused_usage: list[dict[str, object]] = [
        {**EMPTY_USAGE, "outcome": "partial_success"},
        {**EMPTY_USAGE, "reason_code": "provider.error"},
        {**EMPTY_USAGE, "charge_outcome": "charged"},
        {**EMPTY_USAGE, "success": True},
        {**EMPTY_USAGE, "billable_success": True},
        {**EMPTY_USAGE, "quantity": 1},
        {**EMPTY_USAGE, "actual_amount_credits": "2.81"},
        {**EMPTY_USAGE, "settled_amount_credits": "0.01"},
        {**EMPTY_USAGE, "settled_amount_credits": None},
        {**EMPTY_USAGE, "execution_id": "exec-2"},
    ]
    for usage in refused_usage:
        assert not empty_action_day(job, EMPTY_RESPONSE, usage), usage
    others: tuple[QverisJob, ...] = (bulk_job("KQ", "prices"), fx_job(), history_job())
    for other in others:
        assert not empty_action_day(other, EMPTY_RESPONSE, EMPTY_USAGE)
