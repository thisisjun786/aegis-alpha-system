from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data import qveris_parallel_batch as batch
from aegis_alpha.data.qveris_contracts import QverisJob
from aegis_alpha.data.serialization import canonical_json_bytes
from tests.data.test_qveris_acquisition import FakeQveris, eod_job

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

if TYPE_CHECKING:
    from collections.abc import Callable

    from aegis_alpha.data.qveris_billing import QverisPort


def job(day: int) -> QverisJob:
    return replace(
        eod_job(),
        job_id=f"day-{day}",
        parameters_json=canonical_json_bytes(
            {"exchange": "US", "date": date(2026, 8, day), "fmt": "json"}
        ).decode(),
    )


def test_round_robin_prevents_long_first_cohort_starving_second() -> None:
    groups = ((job(1), job(2), job(3)), (job(4), job(5)))
    assert [j.job_id for j in batch.interleave_cohorts(groups)] == [
        "day-1",
        "day-4",
        "day-2",
        "day-5",
        "day-3",
    ]


def test_duplicate_requests_rejected_across_cohorts_without_creating_root(tmp_path: Path) -> None:
    root = tmp_path / "not-created"
    with pytest.raises(ValueError, match="duplicate"):
        batch.collect_parallel_cohorts(((job(1),), (job(1),)), root, FakeQveris)
    assert not root.exists()


def test_bounded_groups_stop_on_unknown_without_overwriting_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def acquire(
        jobs: tuple[QverisJob, ...],
        root: Path,
        factory: Callable[[], QverisPort],
        *,
        workers: int,
        budget: object = None,
    ) -> dict[str, object]:
        del root, factory, budget
        assert workers == 2  # noqa: PLR2004 -- test scenario
        calls.append([j.job_id for j in jobs])
        if len(calls) == 2:  # noqa: PLR2004 -- failure after one successful group
            raise RuntimeError("unknown settlement")
        return {
            "provider_calls_this_run": len(jobs),
            "jobs": [
                {"fingerprint": j.fingerprint, "status": "RAW_ACQUIRED", "reused": False}
                for j in jobs
            ],
        }

    monkeypatch.setattr(batch, "acquire_parallel_jobs", acquire)
    result = batch.collect_parallel_cohorts(
        ((job(1), job(2), job(3)), (job(4), job(5), job(6))), tmp_path, FakeQveris, workers=2
    )
    assert calls == [["day-1", "day-4"], ["day-2", "day-5"]]
    assert (result["processed"], result["pending"], result["stopped"]) == (2, 4, "RuntimeError")
    reports = list((tmp_path / "parallel-cohorts").glob("*/*-00000?.json"))
    assert len(reports) == 2  # noqa: PLR2004 -- separate success and failure records


def test_changed_result_identity_cannot_count_as_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        batch,
        "acquire_parallel_jobs",
        lambda *_args, **_kwargs: {
            "provider_calls_this_run": 1,
            "jobs": [{"fingerprint": job(2).fingerprint, "status": "RAW_ACQUIRED"}],
        },
    )
    result = batch.collect_parallel_cohorts(((job(1),),), tmp_path, FakeQveris)
    assert result["processed"] == 0
    assert result["stopped"] == "ValueError"


def test_warned_completions_count_as_completed_and_never_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def acquire(
        jobs: tuple[QverisJob, ...], *_args: object, **_kwargs: object
    ) -> dict[str, object]:
        return {
            "provider_calls_this_run": len(jobs),
            "jobs": [
                {"fingerprint": j.fingerprint, "status": "RAW_ACQUIRED_WITH_WARNINGS"} for j in jobs
            ],
        }

    monkeypatch.setattr(batch, "acquire_parallel_jobs", acquire)
    result = batch.collect_parallel_cohorts(((job(1), job(2)),), tmp_path, FakeQveris, workers=2)
    assert (result["completed"], result["warned"], result["stopped"]) == (2, 2, None)
    assert result["status"] == "RAW_ACQUIRED"


def test_budget_refusal_stops_scheduling_as_exhausted_not_uncertain(tmp_path: Path) -> None:
    from decimal import Decimal  # noqa: PLC0415 -- local to this scenario

    from aegis_alpha.data.qveris import InvocationBudget  # noqa: PLC0415
    from tests.data.test_qveris_parallel import ConcurrentFake  # noqa: PLC0415

    client = ConcurrentFake(2)
    budget = InvocationBudget(3, Decimal(100))
    result = batch.collect_parallel_cohorts(
        ((job(1), job(2), job(3), job(4)),),
        tmp_path,
        lambda: client,
        workers=2,
        budget=budget,
    )
    assert (result["processed"], result["pending"]) == (2, 2)
    assert result["stopped"] == "INVOCATION_CALL_LIMIT"
    assert result["status"] == "BUDGET_EXHAUSTED"
    assert client.execute_count == budget.reserved_calls == 2  # noqa: PLR2004
