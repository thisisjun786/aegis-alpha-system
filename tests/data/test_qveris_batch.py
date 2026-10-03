from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.data.qveris_batch import collect_cohort
from tests.data.test_qveris_acquisition import FakeQveris, eod_job, fred_job

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")


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
