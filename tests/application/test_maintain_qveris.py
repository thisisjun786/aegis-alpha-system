"""The Qveris maintenance planner: what an ask's raw evidence makes of its request, offline.

Each ask is written as the raw collection root keeps it (``jobs/<fingerprint>/`` with a
completion, or page intents with or without billing and quarantine records). Nothing here
calls a gateway; every request is synthetic.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

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
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_contracts import QverisJob, object_value
from tests.data.qveris_support import ScriptedQveris, account_key, bar

# Serial: the warned replays acquire through the scripted gateway, which takes the host-wide
# Qveris account lease (an abstract Unix socket named by the account).
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

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
    _ask(root, _prices(MON, TUE), "warned")  # a partial success whose rows cannot be read
    _ask(root, _prices(TUE, WED), "failed")  # answered, kept, unusable: asked again
    _ask(root, _prices(WED, TODAY), "unsettled")  # settles before anything else
    _write(root, "parallel-batches/b1/quarantine.json", {"reason": "synthetic"})
    _write(root, "parallel-batches/b1/manifest.json", {"intents": []})
    asks = raw_asks(root)
    states = {request[4]: [ask.state for ask in found] for request, found in asks.items()}
    assert sorted(states.values()) == [["failed"], ["unsettled"], ["warned"]]
    plan = plan_requests(root, POLICY, today=TODAY)
    assert [(item.day, item.reason) for item in plan.planned] == [(TUE, "failed_retry")]
    # Counted apart from a completion; unjudged, so the cheaper choice: not asked again.
    assert (plan.covered, plan.partial) == (0, 1)
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


def test_the_us_symbol_list_is_asked_listed_only_and_every_day(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    policy = QverisPolicy(exchanges=("US",), datasets=("prices",), since={"US": WED},
                          symbol_lists=("KO", "US"), symbol_list_days=7)  # fmt: skip
    yesterday = TODAY - timedelta(days=1)
    for exchange, delisted in (("KO", "0"), ("KO", "1"), ("US", "0")):
        _ask(root, symbol_list_job(exchange, delisted, yesterday), "completed")
    plan = plan_requests(root, policy, today=TODAY)
    planned = [(item.job.job_id, item.reason, item.dataset_id) for item in plan.planned]
    # The KR lists stay covered for a week; yesterday's US list is asked again today.
    assert planned == [
        ("maintain-US-prices-2026-09-16", "new", "prices.us.eodhd"),
        ("maintain-symbols-US-0", "refresh", "identity.us.eodhd"),
    ]
    (job,) = [item.job for item in plan.planned if item.job.dataset == "universe"]
    assert (job.market, job.parameters) == (
        "US", {"EXCHANGE_CODE": "US", "delisted": "0", "fmt": "json"})  # fmt: skip
    _ask(root, symbol_list_job("US", "0", TODAY), "completed")
    assert [item.job.job_id for item in plan_requests(root, policy, today=TODAY).planned] == [
        "maintain-US-prices-2026-09-16"
    ]


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
        ("symbol_lists", ("KS",)),
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


# --- warned asks: partial successes, judged row by row ---------------------------------------

KO = QverisPolicy(exchanges=("KO",), datasets=("prices",), since={"KO": MON})


class WarnedQveris(ScriptedQveris):
    """Every execute answered with a gateway warning; its usage carries recorded counts."""

    def __init__(self, rows: dict[str, object], key: str, counts: dict[str, object]) -> None:
        super().__init__(rows, key)
        self.price = Decimal(0)
        self.failure = "included"  # usage outcome partial_success, reason_code provider.error
        self.counts = counts

    def _execute(self, body: dict[str, object], query: dict[str, str | int]) -> dict[str, object]:
        document = super()._execute(body, query)
        data = object_value(document["result"])["data"]
        rows = len(data) if isinstance(data, list) else 0
        self.usage[-1].update({"raw_result_count": rows, "valid_result_count": rows, **self.counts})
        return document


def _ko(code: str, day: date, **values: object) -> dict[str, object]:
    return {"code": code, "exchange_short_name": "KO", **bar(day.isoformat(), **values)}


def _warned(root: Path, day: date, observed: date, rows: list[object], **counts: object) -> None:
    """Replay one warned KO bulk download of ``day`` asked on ``observed``."""
    job = daily_job("KO", "prices", day, observed)
    acquire_jobs((job,), root, WarnedQveris({job.parameters_json: rows}, account_key(root),
                                            counts))  # fmt: skip


def test_a_warned_ask_with_held_rows_is_asked_again_and_its_reasons_are_counted(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    # Monday: every row passes its value checks; Tuesday: one row has low above the open.
    _warned(root, MON, TUE, [_ko("000101", MON)], provider_status_code=118000)
    _warned(root, TUE, WED, [_ko("000101", TUE), _ko("000202", TUE, low=13)],
            provider_status_code=60310)  # fmt: skip
    assert [ask.state for asks in raw_asks(root).values() for ask in asks] == ["warned"] * 2
    plan = plan_requests(root, KO, today=TODAY)
    # Tuesday's ask held a row: asked again the day after it, like a first settled failure,
    # and after every first ask, so it only spends what the caps leave.
    assert [(item.day, item.reason) for item in plan.planned] == [
        (WED, "new"),
        (TUE, "partial_retry"),
    ]
    assert (plan.covered, plan.partial) == (0, 1)  # Monday: all rows passed, not asked again
    report = plan.report()
    assert cast("dict[str, int]", report["reasons"])["partial_retry"] == 1
    assert report["warnings"] == {
        "asks": 2,
        "decisions": {"partial": 1, "partial_retry": 1},
        "held_rows": 1,
        "unjudged": 0,
        "by_reason": [
            {"reason_code": "provider.error", "provider_status_code": "118000", "pages": 1,
             "counts_equal": 1, "counts_differ": 0, "counts_unknown": 0},
            {"reason_code": "provider.error", "provider_status_code": "60310", "pages": 1,
             "counts_equal": 1, "counts_differ": 0, "counts_unknown": 0},
        ],
    }  # fmt: skip


def test_differing_result_counts_ask_again_and_back_off_like_failures(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    # Monday's rows all pass, but the provider counted fewer valid rows than it returned.
    _warned(root, MON, TUE, [_ko("000101", MON)], raw_result_count=3, valid_result_count=2)
    # Tuesday's request warned twice in a row with a held row: it waits two days.
    for observed in (TUE, WED):
        _warned(root, TUE, observed, [_ko("000101", TUE, volume=-1)])
    # Wednesday's ask is unreadable for judging and has no recorded warning: not asked again.
    _ask(root, daily_job("KO", "prices", WED, TODAY), "warned")
    plan = plan_requests(root, KO, today=TODAY)
    assert [(item.day, item.reason) for item in plan.planned] == [(MON, "partial_retry")]
    assert (plan.waiting, plan.partial) == (1, 1)
    assert plan.failing == [{"job_id": "maintain-KO-prices-2026-09-15", "failed_asks": 2,
                             "retry_on": "2026-09-18"}]  # fmt: skip
    summary = cast("dict[str, object]", plan.report()["warnings"])
    assert summary["decisions"] == {"backoff": 1, "partial": 1, "partial_retry": 1}
    assert (summary["held_rows"], summary["unjudged"]) == (1, 1)
    later = plan_requests(root, KO, today=date(2026, 9, 18))
    # Thursday is a new session; both warned requests are due again.
    assert [item.reason for item in later.planned] == ["new", "partial_retry", "partial_retry"]


def test_an_unjudged_warned_ask_is_counted_unclassified_and_not_asked_again(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    _ask(root, daily_job("KO", "prices", MON, TUE), "warned")
    plan = plan_requests(root, KO, today=TODAY)
    assert [item.day for item in plan.planned] == [TUE, WED]
    assert plan.partial == 1
    summary = cast("dict[str, object]", plan.report()["warnings"])
    assert (summary["unjudged"], summary["held_rows"]) == (1, 0)
    assert summary["by_reason"] == [
        {"reason_code": "unclassified", "provider_status_code": "unclassified", "pages": 1,
         "counts_equal": 0, "counts_differ": 0, "counts_unknown": 1}
    ]  # fmt: skip


# --- transient errors: 5xx, no execution ID, no usage event -----------------------------------


def _unsettled(root: Path, job: QverisJob, search: str, status: int | None) -> str:
    page = f"jobs/{job.fingerprint}/0000"
    _write(root, f"{page}.intent.json", {"job": job.document(), "search_id": search})
    if status is not None:
        _write(root, f"{page}.response.json", {"status": status, "execution_id": None})
    return page


def _settlement_read(root: Path, name: str, search: str, total: int) -> None:
    _write(root, f"audits/{name}.json", {
        "path": "/auth/usage/history/v2", "query": {"search_id": search, "page": 1},
        "response": {"status": "success", "data": {"total": total, "items": [{}] * total,
                                                     "page": 1}},
    })  # fmt: skip


def test_a_5xx_page_without_usage_is_reported_as_a_quarantine_candidate(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    maintenance = _unsettled(root, _prices(MON, TUE), "s-502", 502)
    _settlement_read(root, "a", "s-502", 0)
    _settlement_read(root, "b", "s-502", 0)
    _unsettled(root, _prices(TUE, WED), "s-unread", 503)  # no settlement read retained
    _unsettled(root, _prices(WED, TODAY), "s-used", 502)
    _settlement_read(root, "c", "s-used", 1)  # a usage event exists: not transient
    report = plan_requests(root, POLICY, today=TODAY).report()
    held = cast("list[dict[str, object]]", report["held"])
    # Still held: nothing is quarantined.
    assert [item["job_id"] for item in held] == [
        f"maintain-US-prices-2026-09-{day}" for day in (14, 15, 16)
    ]
    assert report["quarantine_candidates"] == [
        {"job_id": "daily-US-prices-2026-09-14", "page": maintenance, "http_status": 502,
         "settlement_reads_without_usage": 2}
    ]  # fmt: skip
    assert not any(path.name.endswith("quarantine.json") for path in root.rglob("*"))
