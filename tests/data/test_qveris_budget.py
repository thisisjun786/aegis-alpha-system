from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from test_qveris_acquisition import FakeQveris, eod_job

from aegis_alpha.data.qveris import InvocationBudget, RequestBudgetPort
from aegis_alpha.data.qveris_acquisition import acquire_jobs


def test_credit_bound_prevents_intent_and_paid_execution(tmp_path: Path) -> None:
    client = FakeQveris()
    with pytest.raises(RuntimeError, match="INVOCATION_CREDIT_LIMIT"):
        acquire_jobs((eod_job(),), tmp_path, client, budget=InvocationBudget(1, Decimal(0)))
    assert not list(tmp_path.glob("jobs/*/*.intent.json"))
    assert not list(tmp_path.glob("jobs/*/*.raw"))


def test_call_bound_preserves_first_completion_and_blocks_second(tmp_path: Path) -> None:
    first = eod_job()
    second = replace(first, observation_date=first.observation_date.replace(day=7))
    budget = InvocationBudget(1, Decimal(100))
    with pytest.raises(RuntimeError, match="INVOCATION_CALL_LIMIT"):
        acquire_jobs((first, second), tmp_path, FakeQveris(), budget=budget)
    assert (tmp_path / "jobs" / first.fingerprint / "complete.json").exists()
    assert not (tmp_path / "jobs" / second.fingerprint / "0000.intent.json").exists()
    assert budget.reserved_calls == 1


def test_completed_replay_consumes_no_budget(tmp_path: Path) -> None:
    client = FakeQveris()
    acquire_jobs((eod_job(),), tmp_path, client, budget=InvocationBudget(1, Decimal(100)))
    budget = InvocationBudget(1, Decimal(0))
    report = acquire_jobs((eod_job(),), tmp_path, client, budget=budget)
    assert report["provider_calls_this_run"] == 0
    assert budget.reserved_calls == 0
    assert budget.reserved_credits == 0


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-1", True, None])
def test_invalid_credit_budget(amount: object) -> None:
    with pytest.raises((ValueError, TypeError)):
        InvocationBudget(1, amount)  # ty: ignore[invalid-argument-type] -- invalid boundary input


def test_http_bound_counts_audits_separately() -> None:
    client = FakeQveris()
    port = RequestBudgetPort(client, 1, 30)
    port.request("/auth/credits")
    with pytest.raises(RuntimeError, match="HTTP_LIMIT"):
        port.request("/auth/credits")
    assert port.http_requests == 1
    assert port.paid_executions == 0
    assert client.calls == ["/auth/credits"]


def test_elapsed_budget_prevents_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("aegis_alpha.data.qveris.time.monotonic", lambda: 100)
    client = FakeQveris()
    port = RequestBudgetPort(client, 10, 5)
    monkeypatch.setattr("aegis_alpha.data.qveris.time.monotonic", lambda: 105)
    with pytest.raises(RuntimeError, match="HTTP_LIMIT"):
        port.request("/auth/credits")
    assert client.calls == []
