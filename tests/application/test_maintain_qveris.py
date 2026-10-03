"""The Qveris maintenance planner: what an ask's raw evidence makes of its request, offline.

Each ask is written as the raw collection root keeps it (``jobs/<fingerprint>/`` with a
completion, or page intents with or without billing and quarantine records). Nothing here
calls a gateway; every request is synthetic.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from aegis_alpha.application.maintain_qveris import (
    QverisPolicy,
    forex_job,
    plan_requests,
    raw_asks,
    request_of,
    symbol_list_job,
)
from aegis_alpha.application.qveris_daily import daily_job
from aegis_alpha.data.qveris_contracts import QverisJob

TODAY = date(2026, 9, 17)
MON, TUE, WED = date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)
POLICY = QverisPolicy(exchanges=("US",), datasets=("prices",), since={"US": MON})


def _write(root: Path, relative: str, document: object) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document))


def _ask(root: Path, job: QverisJob, state: str, *, batch: str | None = None) -> None:
    """One ask of ``job`` in ``root`` whose evidence says ``state``."""
    base = f"jobs/{job.fingerprint}"
    if state in {"completed", "warned"}:
        status = "RAW_ACQUIRED" if state == "completed" else "RAW_ACQUIRED_WITH_WARNINGS"
        _write(root, f"{base}/complete.json", {"job": job.document(), "status": status})
        return
    intent: dict[str, object] = {"job": job.document()}
    if batch is not None:
        intent["batch_id"] = batch
    _write(root, f"{base}/0000.intent.json", intent)
    if state == "failed":
        _write(root, f"{base}/0000.billing.json", {"over_quote": False})
    elif state == "quarantined" and batch is None:
        _write(root, f"{base}/0000.quarantine.json", {"status": "SETTLEMENT_UNKNOWN"})


def _prices(day: date, observed: date) -> QverisJob:
    return daily_job("US", "prices", day, observed)


def test_each_ask_state_decides_its_request(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    _ask(root, _prices(MON, TUE), "warned")  # a provider warning is a completion
    _ask(root, _prices(TUE, WED), "failed")  # answered, kept, unusable: asked again
    _ask(root, _prices(WED, TODAY), "unsettled")  # settles before anything else
    _write(root, "parallel-batches/b1/quarantine.json", {"reason": "synthetic"})
    _write(root, "parallel-batches/b1/manifest.json", {"intents": []})
    asks = raw_asks(root)
    states = {request[4]: [ask.state for ask in found] for request, found in asks.items()}
    assert sorted(states.values()) == [["failed"], ["unsettled"], ["warned"]]
    plan = plan_requests(root, POLICY, today=TODAY)
    assert [(item.day, item.reason) for item in plan.planned] == [(TUE, "failed_retry")]
    assert plan.covered == 1
    assert [held["job_id"] for held in plan.held] == ["maintain-US-prices-2026-09-16"]
    # Both the unsettled ask and the billed one without a completion are finished on their
    # own jobs first, which executes nothing.
    assert {job.fingerprint for job in plan.resumable} == {
        _prices(TUE, WED).fingerprint,
        _prices(WED, TODAY).fingerprint,
    }
    # The planned retry is a new job of the day: the same request, a new fingerprint.
    (retry,) = plan.planned
    assert request_of(retry.job) == request_of(_prices(TUE, WED))
    assert retry.job.fingerprint != _prices(TUE, WED).fingerprint
    assert retry.job.job_id == "maintain-US-prices-2026-09-15"
    assert retry.dataset_id == "prices.us.eodhd"


def test_a_quarantined_ask_is_asked_again_the_next_day_and_not_the_same_day(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    _ask(root, _prices(MON, TUE), "quarantined")
    _ask(root, _prices(TUE, TODAY), "quarantined", batch="b1")
    _write(root, "parallel-batches/b1/quarantine.json", {"reason": "synthetic"})
    _write(root, "parallel-batches/b1/manifest.json", {"intents": []})
    plan = plan_requests(root, POLICY, today=TODAY)
    assert [(item.day, item.reason) for item in plan.planned] == [
        (WED, "new"),
        (MON, "uncertain_retry"),
    ]
    assert plan.waiting == 1  # the group quarantined today waits for tomorrow
    assert plan.held == []


def test_symbol_lists_refresh_and_forex_asks_daily(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    policy = QverisPolicy(
        exchanges=("US",),
        datasets=("prices",),
        since={"US": WED},
        symbol_lists=("KO",),
        symbol_list_days=7,
        forex=("USDKRW",),
    )
    _ask(root, symbol_list_job("KO", "0", TODAY - timedelta(days=3)), "completed")
    _ask(root, symbol_list_job("KO", "1", TODAY - timedelta(days=8)), "completed")
    _ask(root, forex_job("USDKRW", TODAY - timedelta(days=1), 10), "completed")
    plan = plan_requests(root, policy, today=TODAY)
    planned = [(item.job.job_id, item.reason, item.dataset_id) for item in plan.planned]
    assert planned == [
        ("maintain-US-prices-2026-09-16", "new", "prices.us.eodhd"),
        ("maintain-symbols-KO-1", "refresh", "identity.kr.eodhd"),
        ("maintain-fx-USDKRW", "new", "fx.usdkrw.eodhd"),
    ]
    assert plan.covered == 1
    (fx,) = [item.job for item in plan.planned if item.job.dataset == "fx_history"]
    assert fx.parameters["from"] == (TODAY - timedelta(days=10)).isoformat()


def test_the_window_starts_at_since_and_stays_inside_the_declaration(tmp_path: Path) -> None:
    policy = QverisPolicy(
        exchanges=("US", "KO", "KQ"),
        datasets=("prices", "splits"),
        since={"US": MON, "KO": TUE, "KQ": date(2020, 1, 1)},
    )
    plan = plan_requests(tmp_path / "absent", policy, today=TODAY)
    report = plan.report()
    gap = QverisPolicy(exchanges=("US",), datasets=("prices",), since={"US": WED},
                       extra_sessions={"US": (date(2026, 7, 28), date(2026, 7, 26))})  # fmt: skip
    # An explicit earlier gap is asked; a declared closed day (a Sunday) is not.
    assert [item.day for item in plan_requests(tmp_path / "absent", gap, today=TODAY)
            .planned] == [date(2026, 7, 28), WED]  # fmt: skip
    assert report["windows"] == {
        "US": {"from": "2026-09-14", "through": "2026-09-16"},
        "KO": {"from": "2026-09-15", "through": "2026-09-16"},
        # At most 366 days before the observation date.
        "KQ": {"from": "2025-09-17", "through": "2026-09-16"},
    }
    first = [(item.day, item.job.market, item.job.dataset) for item in plan.planned[:4]]
    # By date, exchange in the policy's order, then dataset.
    assert first[0][0] == date(2025, 9, 17)
    far = plan_requests(tmp_path / "absent", POLICY, today=date(2028, 3, 1))
    # The packaged XNYS declaration ends with 2027; later days are reported, never asked.
    assert far.undeclared["US"] > 0
    assert all(item.day is not None and item.day <= date(2027, 12, 31) for item in far.planned)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exchanges", ("US", "US")),
        ("datasets", ("prices", "bars")),
        ("since", {}),
        ("symbol_lists", ("US",)),
        ("forex", ("USD",)),
        ("symbol_list_days", 0),
    ],
)
def test_a_policy_outside_the_reviewed_requests_is_refused(field: str, value: object) -> None:
    arguments: dict[str, object] = {
        "exchanges": ("US",),
        "datasets": ("prices",),
        "since": {"US": MON},
        field: value,
    }
    with pytest.raises(ValueError, match="qveris"):
        QverisPolicy(**arguments)  # ty: ignore[invalid-argument-type]


def test_settled_failures_back_off_and_retry_after_the_first_asks(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    # A request the provider keeps failing: three settled failures in a row.
    for observed in (date(2026, 9, 11), date(2026, 9, 12), date(2026, 9, 14)):
        _ask(root, _prices(date(2026, 9, 10), observed), "failed")
    _ask(root, _prices(MON, TUE), "failed")  # one failure: retried the next day
    policy = QverisPolicy(exchanges=("US",), datasets=("prices",), since={"US": MON},
                          extra_sessions={"US": (date(2026, 9, 10),)})  # fmt: skip
    # The third failure in a row waits four days after its observation date.
    early = plan_requests(root, policy, today=TODAY)
    assert [(item.day, item.reason) for item in early.planned] == [
        (TUE, "new"),
        (WED, "new"),
        (MON, "failed_retry"),  # retries take what the caps leave after the first asks
    ]
    assert early.failing == [{"job_id": "maintain-US-prices-2026-09-10", "failed_asks": 3,
                              "retry_on": "2026-09-18"}]  # fmt: skip
    later = plan_requests(root, policy, today=date(2026, 9, 18))
    assert [(item.day, item.reason) for item in later.planned][-2:] == [
        (date(2026, 9, 10), "failed_retry"),
        (MON, "failed_retry"),
    ]
