from __future__ import annotations

import ssl
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_batch import _acquire_job, budget_stop, collect_cohort
from aegis_alpha.data.qveris_client import QverisResponse, QverisTransportError
from aegis_alpha.data.qveris_parallel_batch import collect_parallel_cohorts
from aegis_alpha.data.qveris_store import QverisStore
from tests.data.test_qveris_acquisition import FakeQveris, eod_job, fred_job

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")


@pytest.fixture
def pauses(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr("time.sleep", delays.append)
    return delays


class TransportQveris(FakeQveris):
    def __init__(self, endpoint: str, failures: int, chain: str = "direct") -> None:
        super().__init__()
        self.endpoint, self.remaining, self.chain = endpoint, failures, chain
        self.fail_after_executions = 0

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if (
            path == self.endpoint
            and self.remaining
            and self.execute_count >= self.fail_after_executions
        ):
            self.remaining -= 1
            self.calls.append(path)
            error = QverisTransportError(ssl.SSLEOFError("synthetic TLS drop"))
            if self.chain == "cause":
                raise RuntimeError("synthetic wrapper") from error
            if self.chain == "context":
                wrapper = RuntimeError("synthetic wrapper")
                wrapper.__context__ = error
                raise wrapper
            raise error
        try:
            return super().request(path, body=body, query=query)
        except TimeoutError as error:
            raise QverisTransportError(error) from error


@pytest.mark.parametrize("endpoint", ["/tools/by-ids", "/tools/probe"])
@pytest.mark.parametrize("chain", ["direct", "cause", "context"])
def test_preflight_transport_recovers_and_asks_next_job(
    tmp_path: Path, endpoint: str, chain: str, pauses: list[float]
) -> None:
    client = TransportQveris(endpoint, 1, chain)
    first = eod_job()
    second = replace(
        first, job_id="second", observation_date=first.observation_date + timedelta(days=1)
    )
    jobs = (first, second)
    snapshots: list[dict[str, object]] = []

    def progress(row: dict[str, object]) -> None:
        snapshots.append(row)
        assert client.execute_count == len(snapshots)
        assert len(client.usage) == len(client.ledger) == len(snapshots)
        assert client.balance == Decimal(100) - client.price * len(snapshots)

    result = collect_cohort(jobs, tmp_path, client, progress=progress)

    assert (result["completed"], result["failed"], result["stopped"]) == (2, 0, None)
    assert result["failures"] == []
    assert client.calls.count(endpoint) == len(snapshots) + 1
    assert client.execute_count == len(client.usage) == len(client.ledger) == len(jobs)
    assert len(list(tmp_path.rglob("*.billing.json"))) == len(jobs)
    assert pauses == [30.0]


def test_preflight_transport_stops_after_exactly_one_retry(
    tmp_path: Path, pauses: list[float]
) -> None:
    attempts = 2
    client = TransportQveris("/tools/probe", attempts)
    job = eod_job()

    result = collect_cohort((job, fred_job()), tmp_path, client)

    assert client.calls.count("/tools/probe") == attempts
    assert pauses == [30.0]
    assert result["stopped"] == "PENDING_OR_PREFLIGHT_FAILURE"
    assert (result["completed"], result["failed"], result["pending"]) == (0, 0, 2)
    assert result["failures"] == [
        {
            "index": 0,
            "job_id": job.job_id,
            "fingerprint": job.fingerprint,
            "error_class": "QverisTransportError",
            "reason": "Qveris transport failure: SSLEOFError",
            "known_settled_failure": False,
        }
    ]
    assert client.execute_count == len(client.usage) == len(client.ledger) == 0
    assert not list(tmp_path.rglob("*.intent.json"))


def test_cohort_continues_after_known_uncharged_failure_and_reuses_success(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "failed-not-charged"
    reports = []

    def progress(row: dict[str, object]) -> None:
        reports.append(row)
        client.failure = None

    result = collect_cohort((eod_job(), fred_job()), tmp_path, client, progress=progress)
    assert (result["completed"], result["failed"], result["pending"]) == (1, 1, 0)
    before = client.execute_count
    repeated = collect_cohort((eod_job(), fred_job()), tmp_path, client)
    assert repeated["reused"] == 1
    assert repeated["failed"] == 1
    assert client.execute_count == before


def test_cohort_stops_on_ambiguous_execution_preserving_pending_count(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "timeout"
    client.usage_ready = False
    result = collect_cohort((eod_job(), fred_job()), tmp_path, client)
    assert result["completed"] == 0
    assert result["failed"] == 0
    assert result["pending"] == len((eod_job(), fred_job()))
    assert result["stopped"] == "PENDING_OR_PREFLIGHT_FAILURE"
    assert client.execute_count == 1


def test_execute_transport_with_hidden_usage_never_retries(
    tmp_path: Path, pauses: list[float]
) -> None:
    client = TransportQveris("/tools/probe", 0)
    client.failure = "timeout"
    client.usage_ready = False
    job = eod_job()

    result = collect_cohort((job, fred_job()), tmp_path, client)

    assert result["stopped"] == "PENDING_OR_PREFLIGHT_FAILURE"
    assert client.calls.count("/tools/execute") == 1
    assert client.execute_count == len(client.usage) == len(client.ledger) == 1
    assert client.balance == Decimal(100) - client.price
    assert pauses == []
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.pending_pages() == (f"jobs/{job.fingerprint}/0000",)
        assert not store.exists(f"jobs/{job.fingerprint}/0000.billing.json")


def test_later_page_preflight_transport_reuses_billed_page(tmp_path: Path) -> None:
    client = TransportQveris("/tools/probe", 1)
    client.fail_after_executions = 1
    delays: list[float] = []

    result = collect_cohort(
        (fred_job(),), tmp_path, client, sleep=delays.append, retry_delay_seconds=0.25
    )

    assert (result["completed"], result["failed"], result["stopped"]) == (1, 0, None)
    pages = 2
    assert client.execute_count == len(client.usage) == len(client.ledger) == pages
    assert result["successful_job_calls_this_run"] == pages
    assert client.balance == Decimal(100) - client.price * pages
    assert delays == [0.25]


@pytest.mark.parametrize("route", ["direct", "parallel", "job"])
@pytest.mark.parametrize("existing_page", [False, True])
def test_retry_call_counts_include_only_this_runs_executions(
    tmp_path: Path, route: str, *, existing_page: bool, pauses: list[float]
) -> None:
    client = TransportQveris("/tools/probe", 1)
    client.fail_after_executions = 1
    job = fred_job()
    if existing_page:
        with pytest.raises(QverisTransportError):
            acquire_jobs((job,), tmp_path, client)
        client.remaining = 1
    before = client.execute_count

    if route == "parallel":
        result = collect_parallel_cohorts(((job,),), tmp_path, lambda: client)
        reported = result["provider_calls_this_run"]
    elif route == "job":
        result = _acquire_job(job, tmp_path, client, None, lambda: pauses.append(30.0))
        reported = result["provider_calls_this_run"]
        (item,) = cast("list[dict[str, object]]", result["jobs"])
        assert item["provider_calls_this_run"] == reported
    else:
        result = collect_cohort((job,), tmp_path, client)
        reported = result["successful_job_calls_this_run"]

    expected = 1 if existing_page else 2
    assert client.execute_count - before == expected
    assert reported == expected
    assert result["status"] == "RAW_ACQUIRED"
    assert pauses == [30.0]


@pytest.mark.parametrize("failure", ["schema", "failed-not-charged", "timeout"])
def test_nontransport_or_settled_failure_does_not_pause(
    tmp_path: Path, failure: str, pauses: list[float]
) -> None:
    client = TransportQveris("/tools/probe", 0)
    client.failure = failure

    result = collect_cohort((eod_job(),), tmp_path, client)

    assert pauses == []
    assert client.calls.count("/tools/probe") == 1
    assert result["failed"] == (0 if failure == "schema" else 1)
    assert result["stopped"] == ("PENDING_OR_PREFLIGHT_FAILURE" if failure == "schema" else None)


def test_transport_with_pending_parallel_group_does_not_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pauses: list[float]
) -> None:
    client = TransportQveris("/tools/probe", 1)
    with QverisStore(tmp_path, client.account_key) as store:
        store.publish_document("parallel-batches/synthetic/manifest.json", {"intents": []})

    def unavailable(_store: QverisStore) -> None:
        raise QverisTransportError(ssl.SSLEOFError("synthetic transport failure"))

    monkeypatch.setattr(QverisStore, "require_no_pending_batches", unavailable)
    result = collect_cohort((eod_job(),), tmp_path, client)

    assert result["stopped"] == "PENDING_OR_PREFLIGHT_FAILURE"
    assert pauses == []
    assert client.calls == []
    assert client.execute_count == 0


def test_budget_refusal_stops_without_an_attempt_or_a_failure_row(tmp_path: Path) -> None:
    from dataclasses import replace  # noqa: PLC0415 -- scenario imports
    from decimal import Decimal  # noqa: PLC0415

    from aegis_alpha.data.qveris import InvocationBudget  # noqa: PLC0415

    client = FakeQveris()
    second = replace(
        eod_job(), job_id="second", parameters_json=eod_job().parameters_json.replace("31", "30")
    )
    result = collect_cohort(
        (eod_job(), second), tmp_path, client, budget=InvocationBudget(1, Decimal(10))
    )
    assert (result["completed"], result["failed"], result["pending"]) == (1, 0, 1)
    assert result["stopped"] == "INVOCATION_CALL_LIMIT"
    assert result["status"] == "BUDGET_EXHAUSTED"
    assert result["failures"] == []
    assert client.execute_count == 1
    assert not (tmp_path / "jobs" / second.fingerprint).exists()


def test_a_provider_warning_completes_and_the_cohort_continues(tmp_path: Path) -> None:
    from decimal import Decimal  # noqa: PLC0415 -- scenario import

    client = FakeQveris()
    client.price = Decimal(0)
    client.failure = "included"
    result = collect_cohort((eod_job(),), tmp_path, client)
    assert (result["completed"], result["warned"], result["stopped"]) == (1, 1, None)
    assert result["status"] == "RAW_ACQUIRED"


def test_a_budget_code_with_unresolved_evidence_is_not_a_clean_stop(tmp_path: Path) -> None:
    client = FakeQveris()
    refusal = RuntimeError("INVOCATION_CALL_LIMIT: execute was not attempted")
    acquire_jobs((fred_job(),), tmp_path, client)
    assert budget_stop(refusal, tmp_path, client.account_key) == "INVOCATION_CALL_LIMIT"
    client.failure = "timeout"
    client.usage_ready = False
    with pytest.raises(RuntimeError):
        acquire_jobs((eod_job(),), tmp_path, client)
    # The pending page blocks the account, so the run must report an uncertain stop.
    assert budget_stop(refusal, tmp_path, client.account_key) is None
    assert budget_stop(ValueError("INVOCATION_CALL_LIMIT"), tmp_path, client.account_key) is None


def test_a_settled_malformed_payload_is_a_known_failure_and_the_cohort_continues(
    tmp_path: Path,
) -> None:
    from tests.data.qveris_support import ScriptedQveris, bar, bulk_job  # noqa: PLC0415

    broken, fine = bulk_job("US", "prices", "2026-08-31"), bulk_job("US", "prices", "2026-09-01")
    row = {"code": "AAA", "exchange_short_name": "US", **bar("2026-09-01")}
    client = ScriptedQveris({broken.parameters_json: "not an array", fine.parameters_json: [row]})
    result = collect_cohort((broken, fine), tmp_path, client)
    assert (result["failed"], result["completed"], result["stopped"]) == (1, 1, None)
    (failure,) = cast("list[dict[str, object]]", result["failures"])
    assert failure["error_class"] == "TypeError"
    assert failure["known_settled_failure"] is True
