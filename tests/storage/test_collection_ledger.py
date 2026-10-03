"""The state collection ledger: reservations before calls, settled outcomes, quota counts."""

# ruff: noqa: PLR2004 -- synthetic counts are the expected values
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from aegis_alpha.storage import collection_ledger as ledger
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace

POLICY = "b" * 64


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, require_strategies=False) as workspace:
        yield workspace


def _job(fingerprint: str) -> ledger.Job:
    return ledger.Job("opendart", "fundamentals.kr.dart", fingerprint, POLICY, 10, 20)


def _attempts(ws: Workspace) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in ws.state.execute(
            "SELECT job_id,attempt,status,started_at_us,completed_at_us FROM collection_attempts "
            "ORDER BY job_id,attempt"
        )
    ]


def _usage(ws: Workspace) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in ws.state.execute(
            "SELECT job_id,attempt,kind,units,known_at_us FROM usage_events "
            "ORDER BY job_id,attempt,known_at_us,kind"
        )
    ]


def test_a_call_is_reserved_before_it_starts_and_settled_after(ws: Workspace) -> None:
    job = _job("a" * 64)
    first = ledger.reserve(ws.state, job, at_us=100)
    ledger.start(ws.state, first, at_us=101)
    ledger.succeed(ws.state, first, receipt_sha256="c" * 64, outcome="NO_DATA", at_us=102)
    # Asking the same request again is the next attempt of the same job.
    second = ledger.reserve(ws.state, job, at_us=200)
    assert (first.attempt, second.attempt) == (1, 2)
    assert _attempts(ws) == [
        (job.job_id, 1, "succeeded", 100, 102),
        (job.job_id, 2, "reserved", 200, None),
    ]
    assert _usage(ws) == [
        (job.job_id, 1, "reserved", "1", 100),
        (job.job_id, 1, "charged", "1", 102),
        (job.job_id, 2, "reserved", "1", 200),
    ]
    assert [
        tuple(row)
        for row in ws.state.execute(
            "SELECT provider,dataset_id,window_start_us,window_end_us,policy_hash,idempotency_key,"
            "status FROM collection_jobs"
        )
    ] == [("opendart", "fundamentals.kr.dart", 10, 20, POLICY, job.job_id, "no_data")]
    assert ledger.charged_receipts(ws.state, "opendart") == ["c" * 64]
    with pytest.raises(ValueError, match="cannot move"):
        ledger.succeed(ws.state, second, receipt_sha256="d" * 64, outcome="COMPLETED", at_us=201)
    with pytest.raises(sqlite3.DatabaseError, match="immutable"):
        ws.state.execute("DELETE FROM usage_events")


def test_recovery_never_turns_an_interrupted_call_into_a_success_or_a_non_call(
    ws: Workspace,
) -> None:
    never = ledger.reserve(ws.state, _job("a" * 64), at_us=100)
    maybe = ledger.reserve(ws.state, _job("b" * 64), at_us=100)
    ledger.start(ws.state, maybe, at_us=101)
    assert ledger.recover(ws.state, "opendart", at_us=300) == {"released": 1, "uncertain": 1}
    assert [row[2] for row in _attempts(ws)] == ["failed", "uncertain"]
    kinds = [(row[0], row[2]) for row in _usage(ws)]
    assert kinds == [
        (never.job_id, "reserved"),
        (never.job_id, "released"),
        (maybe.job_id, "reserved"),
        (maybe.job_id, "uncertain"),
    ]
    assert ledger.unanswered(ws.state, "opendart") == {"b" * 64: 300}
    assert ledger.recover(ws.state, "opendart", at_us=400) == {"released": 0, "uncertain": 0}


def test_the_quota_counts_every_unreleased_reservation_in_the_window(ws: Workspace) -> None:
    for index, at in enumerate((100, 200, 300)):
        attempt = ledger.reserve(ws.state, _job(f"{index:064x}"), at_us=at)
        ledger.start(ws.state, attempt, at_us=at)
        ledger.uncertain(ws.state, attempt, at_us=at + 1)
    ledger.reserve(ws.state, _job("f" * 64), at_us=250)
    ledger.recover(ws.state, "opendart", at_us=400)  # releases the call that never started
    assert ledger.used(ws.state, "opendart", since_us=0) == 3
    assert ledger.used(ws.state, "opendart", since_us=200) == 2
    assert ledger.used(ws.state, "kind", since_us=0) == 0


def test_a_job_keeps_its_dataset(ws: Workspace) -> None:
    ledger.reserve(ws.state, _job("a" * 64), at_us=100)
    other = ledger.Job("opendart", "filings.kr.dart", "a" * 64, POLICY)
    with pytest.raises(ValueError, match="another dataset"):
        ledger.reserve(ws.state, other, at_us=200)
    with pytest.raises(ValueError, match="SHA-256"):
        ledger.Job("opendart", "fundamentals.kr.dart", "A" * 64, POLICY)
