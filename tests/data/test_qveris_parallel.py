from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from aegis_alpha.data import qveris_parallel
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import QverisJob, object_value
from aegis_alpha.data.qveris_parallel import acquire_parallel_jobs, quarantine_parallel_batch
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.test_qveris_acquisition import PRICE, FakeQveris, eod_job, fred_job


def jobs(count: int = 4) -> tuple[QverisJob, ...]:
    return tuple(
        replace(
            eod_job(),
            job_id=f"day-{i}",
            parameters_json=canonical_json_bytes(
                {"exchange": "US", "date": f"2026-08-{i + 1:02d}", "fmt": "json"}
            ).decode(),
        )
        for i in range(count)
    )


class ConcurrentFake(FakeQveris):
    def __init__(self, parties: int = 1) -> None:
        super().__init__()
        self.barrier = threading.Barrier(parties)
        self.lock = threading.Lock()
        self.fail_on: str | None = None

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if path == "/tools/execute":
            self.barrier.wait(timeout=5)
        with self.lock:
            return super().request(path, body=body, query=query)

    def _execute(self, body: dict[str, object], query: dict[str, str | int]) -> dict[str, object]:
        parameters = object_value(body["parameters"])
        failure = self.failure
        if self.fail_on == parameters["date"]:
            self.failure = "failed-not-charged"
        try:
            document = super()._execute(body, query)
        finally:
            self.failure = failure
        if document["success"]:
            object_value(document["result"])["data"] = [{**PRICE, "date": parameters["date"]}]
        return document


@pytest.mark.parametrize("workers", [4, 8, 16])
def test_execute_requests_overlap_and_replay_without_http(tmp_path: Path, workers: int) -> None:
    client = ConcurrentFake(workers)
    selected = jobs(workers)
    result = acquire_parallel_jobs(selected, tmp_path, lambda: client, workers=workers)
    assert result["provider_calls_this_run"] == workers
    rows = result["jobs"]
    assert isinstance(rows, list)
    assert all(object_value(x)["status"] == "RAW_ACQUIRED" for x in rows)
    before = len(client.calls)
    replay = acquire_parallel_jobs(selected, tmp_path, lambda: client, workers=workers)
    assert replay["provider_calls_this_run"] == 0
    assert len(client.calls) == before
    expected = {4: Decimal("88.76"), 8: Decimal("77.52"), 16: Decimal("55.04")}
    assert client.balance == expected[workers]


@pytest.mark.parametrize("workers", [0, 17, True])
def test_unreviewed_worker_count_rejected_before_client(tmp_path: Path, workers: int) -> None:
    def no_client() -> ConcurrentFake:
        pytest.fail("invalid worker count must not create client")

    with pytest.raises(ValueError, match="bound"):
        acquire_parallel_jobs(jobs(1), tmp_path, no_client, workers=workers)


def test_aggregate_budget_rejects_before_any_execute(tmp_path: Path) -> None:
    client = ConcurrentFake()
    client.balance = Decimal(10)
    with pytest.raises(RuntimeError, match="INSUFFICIENT"):
        acquire_parallel_jobs(jobs(), tmp_path, lambda: client)
    assert client.execute_count == 0
    assert not list(tmp_path.glob("parallel-batches/*/manifest.json"))


def test_unknown_paid_response_blocks_parallel_and_serial_retry(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.failure = "timeout"
    client.usage_ready = False
    selected = jobs(2)
    with pytest.raises(RuntimeError, match="PENDING_BATCH"):
        acquire_parallel_jobs(selected, tmp_path, lambda: client)
    assert client.execute_count == 2  # noqa: PLR2004
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.reserved_credits() == Decimal("5.62")
    with pytest.raises(RuntimeError, match="PENDING_BATCH"):
        acquire_jobs(selected, tmp_path, client)
    client.failure = None
    with pytest.raises(RuntimeError, match="PENDING_BATCH"):
        acquire_parallel_jobs(selected, tmp_path, lambda: client)
    assert client.execute_count == 2  # noqa: PLR2004


def test_mixed_settled_failure_preserves_success_and_never_reexecutes(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.fail_on = "2026-08-02"
    selected = jobs(2)
    result = acquire_parallel_jobs(selected, tmp_path, lambda: client)
    rows = result["jobs"]
    assert isinstance(rows, list)
    assert [object_value(x)["status"] for x in rows] == ["RAW_ACQUIRED", "FAILED"]
    replay = acquire_parallel_jobs(selected, tmp_path, lambda: client)
    assert replay["provider_calls_this_run"] == 0
    assert client.execute_count == 2  # noqa: PLR2004


@pytest.mark.parametrize("failure", ["over-quote", "wrong-session", "duplicate-usage"])
def test_bad_settlement_prevents_group_completion(tmp_path: Path, failure: str) -> None:
    client = ConcurrentFake(2)
    client.failure = failure
    with pytest.raises((ValueError, RuntimeError)):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    assert not list(tmp_path.glob("parallel-batches/*/complete.json"))
    assert not list(tmp_path.glob("jobs/*/complete.json"))


def test_unexpected_debit_shortfall_blocks_aggregate(tmp_path: Path) -> None:
    class ShortLedger(ConcurrentFake):
        def request(
            self,
            path: str,
            *,
            body: dict[str, object] | None = None,
            query: dict[str, str | int] | None = None,
        ) -> QverisResponse:
            if path in {"/auth/credits", "/auth/credits/ledger"} and self.execute_count:
                # Usage claims two charges, but account and ledger only contain one.
                self.ledger = self.ledger[:1]
                self.balance = Decimal(100) - self.price
            return super().request(path, body=body, query=query)

    client = ShortLedger(2)
    with pytest.raises(RuntimeError, match="aggregate"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)


def test_reject_duplicate_or_paginated_jobs_before_client_creation(tmp_path: Path) -> None:
    def no_client() -> FakeQveris:
        pytest.fail("no client should be created")

    for selected in ((eod_job(), eod_job()), (fred_job(),)):
        with pytest.raises(ValueError, match=r"duplicate|single-page"):
            acquire_parallel_jobs(selected, tmp_path, no_client)


def test_recovery_uses_saved_raw_without_reexecute(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.usage_ready = False
    selected = jobs(2)
    with pytest.raises(RuntimeError, match="usage"):
        acquire_parallel_jobs(selected, tmp_path, lambda: client)
    assert len(list(tmp_path.glob("jobs/*/0000.raw"))) == 2  # noqa: PLR2004
    client.usage_ready = True
    recovered = acquire_parallel_jobs(selected, tmp_path, lambda: client)
    assert recovered["provider_calls_this_run"] == 0
    assert client.execute_count == 2  # noqa: PLR2004
    assert len(list(tmp_path.glob("jobs/*/complete.json"))) == 2  # noqa: PLR2004


def test_incomplete_intent_set_retains_reservation_without_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = QverisStore.publish_document

    def crash(self: QverisStore, path: str, document: object) -> None:
        if path.endswith(".intent.json"):
            raise OSError("synthetic crash before intents")
        original(self, path, document)

    client = ConcurrentFake(2)
    monkeypatch.setattr(QverisStore, "publish_document", crash)
    with pytest.raises(OSError, match="synthetic crash"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    monkeypatch.setattr(QverisStore, "publish_document", original)
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.reserved_credits() == Decimal("5.62")
    before = len(client.calls)
    with pytest.raises(RuntimeError, match="incomplete durable"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    assert len(client.calls) == before
    assert client.execute_count == 0


def test_auth_failure_is_settled_but_stops_coordinator(tmp_path: Path) -> None:
    class Forbidden(ConcurrentFake):
        def _execute(
            self, body: dict[str, object], query: dict[str, str | int]
        ) -> dict[str, object]:
            result = super()._execute(body, query)
            result["success"] = False
            object_value(result["result"])["status_code"] = 403
            return result

    client = Forbidden()
    with pytest.raises(RuntimeError, match="PROVIDER_STOP"):
        acquire_parallel_jobs(jobs(1), tmp_path, lambda: client)
    assert client.execute_count == 1
    assert len(list(tmp_path.glob("parallel-batches/*/complete.json"))) == 1
    assert not list(tmp_path.glob("jobs/*/complete.json"))


def test_partial_billing_recovery_keeps_one_immutable_group_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = QverisStore.publish_document
    writes = []

    def crash(self: QverisStore, path: str, document: object) -> None:
        if path.endswith(".billing.json"):
            writes.append(path)
            if len(writes) == 2:  # noqa: PLR2004 -- fail second receipt after first completed
                raise OSError("crash during billing")
        original(self, path, document)

    client = ConcurrentFake(2)
    selected = jobs(2)
    monkeypatch.setattr(QverisStore, "publish_document", crash)
    with pytest.raises(OSError, match="crash during"):
        acquire_parallel_jobs(selected, tmp_path, lambda: client)
    monkeypatch.setattr(QverisStore, "publish_document", original)
    client.balance += 100
    client.ledger.append({"id": "later-deposit", "amount_credits": "100"})
    acquire_parallel_jobs(selected, tmp_path, lambda: client)
    receipts = [json.loads(f.read_text()) for f in tmp_path.glob("jobs/*/0000.billing.json")]
    assert len(receipts) == 2  # noqa: PLR2004
    assert {r["credits_after"] for r in receipts} == {"94.38"}
    assert {r["other_account_delta"] for r in receipts} == {"0.00"}
    assert receipts[0]["batch_settlement"] == receipts[1]["batch_settlement"]
    assert client.execute_count == 2  # noqa: PLR2004


def test_two_coordinators_share_root_without_duplicate_paid_calls(tmp_path: Path) -> None:
    client = ConcurrentFake(4)
    selected = jobs()
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(acquire_parallel_jobs, selected, tmp_path, lambda: client) for _ in range(2)
        ]
        results = [f.result(timeout=10) for f in futures]
    assert sorted(int(str(r["provider_calls_this_run"])) for r in results) == [0, 4]
    assert client.execute_count == 4  # noqa: PLR2004


def test_lost_raw_with_confirmed_charge_is_failed_without_blocking_account(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.failure = "timeout"
    result = acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    rows = result["jobs"]
    assert isinstance(rows, list)
    assert all(object_value(row)["status"] == "FAILED" for row in rows)
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.pending_batches() == ()
    assert acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)["provider_calls_this_run"] == 0
    assert client.execute_count == 2  # noqa: PLR2004


def test_explicit_quarantine_keeps_budget_and_unblocks_unrelated_jobs(tmp_path: Path) -> None:
    client = ConcurrentFake(2)
    client.failure = "timeout"
    client.usage_ready = False
    with pytest.raises(RuntimeError, match="PENDING_BATCH"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    with QverisStore(tmp_path, client.account_key) as store:
        group = store.pending_batches()[0]
    quarantine_parallel_batch(
        tmp_path, group, "isolate unknown requests, retain reservation", client
    )
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.reserved_credits() == Decimal("5.62")
        assert store.pending_pages() == ()
    with pytest.raises(RuntimeError, match="QUARANTINED"):
        acquire_jobs(jobs(2), tmp_path, client)
    client.failure = None
    client.usage_ready = True
    result = acquire_parallel_jobs(jobs(4)[2:], tmp_path, lambda: client)
    assert result["provider_calls_this_run"] == 2  # noqa: PLR2004
    repeated = acquire_parallel_jobs(jobs(2), tmp_path, lambda: client)
    assert repeated["provider_calls_this_run"] == 0


def test_write_failure_still_drains_and_preserves_other_paid_responses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = qveris_parallel._record_response  # noqa: SLF001 -- fault injection at write boundary
    writes = []

    def record(store: QverisStore, page: str, response: QverisResponse) -> None:
        writes.append(page)
        if len(writes) == 1:
            raise OSError("synthetic write failure")
        original(store, page, response)

    monkeypatch.setattr(qveris_parallel, "_record_response", record)
    client = ConcurrentFake(4)
    with pytest.raises(RuntimeError, match="RESPONSE_WRITE_FAILED"):
        acquire_parallel_jobs(jobs(), tmp_path, lambda: client)
    assert len(writes) == 4  # noqa: PLR2004
    assert len(list(tmp_path.glob("jobs/*/0000.raw"))) == 3  # noqa: PLR2004
    monkeypatch.setattr(qveris_parallel, "_record_response", original)
    acquire_parallel_jobs(jobs(), tmp_path, lambda: client)
    assert client.execute_count == 4  # noqa: PLR2004


def test_group_is_reserved_whole_or_not_at_all(tmp_path: Path) -> None:
    from aegis_alpha.data.qveris import InvocationBudget  # noqa: PLC0415 -- scenario import

    client = ConcurrentFake(2)
    budget = InvocationBudget(1, Decimal(100))
    with pytest.raises(RuntimeError, match="INVOCATION_CALL_LIMIT"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client, budget=budget)
    assert (budget.reserved_calls, budget.reserved_credits) == (0, 0)
    assert client.execute_count == 0
    assert not list(tmp_path.glob("parallel-batches/*/manifest.json"))
    short = InvocationBudget(2, Decimal("5.61"))
    with pytest.raises(RuntimeError, match="INVOCATION_CREDIT_LIMIT"):
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client, budget=short)
    assert short.reserved_calls == 0
    enough = InvocationBudget(2, Decimal("5.62"))
    result = acquire_parallel_jobs(jobs(2), tmp_path, lambda: client, budget=enough)
    assert result["provider_calls_this_run"] == 2  # noqa: PLR2004
    assert (enough.reserved_calls, enough.reserved_credits) == (2, Decimal("5.62"))
    replay = InvocationBudget(1, Decimal(0))
    assert (
        acquire_parallel_jobs(jobs(2), tmp_path, lambda: client, budget=replay)[
            "provider_calls_this_run"
        ]
        == 0
    )
    assert replay.reserved_calls == 0
