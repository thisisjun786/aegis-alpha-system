"""``aas collect qveris``: daily jobs from the declared calendar, plans and bounded runs."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from aegis_alpha.application import qveris_daily
from aegis_alpha.application.cli import main
from aegis_alpha.data import qveris_client, qveris_pacing
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_contracts import QverisJob, load_jobs
from aegis_alpha.data.qveris_pacing import RequestAdmission
from tests.data.qveris_support import ScriptedQveris, account_key, bulk_job, complete

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

KO_ROW = {
    "code": "123456",
    "exchange_short_name": "KO",
    "open": 10,
    "high": 12,
    "low": 9,
    "close": 11,
    "adjusted_close": 11,
    "volume": 100,
}


def _daily(raw: Path, output: Path, *extra: str) -> list[str]:
    return [
        "collect",
        "qveris",
        "daily-jobs",
        "--raw-root",
        str(raw),
        "--output",
        str(output),
        *extra,
    ]


def test_daily_jobs_follow_declared_sessions_and_skip_completed_requests(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = tmp_path / "raw"
    # The same request completed under another observation date and job ID is covered.
    done = bulk_job("KO", "prices", "2026-09-21", date(2026, 9, 22))
    complete(raw, done, [{**KO_ROW, "date": "2026-09-21"}])
    window = ["--from", "2026-09-21", "--through", "2026-09-30", "--observation-date", "2026-10-01"]
    args = [*window, "--exchange", "KO", "--dataset", "prices", "--dataset", "splits"]
    assert main(_daily(raw, tmp_path / "jobs.json", *args)) == 0
    result = json.loads(capsys.readouterr().out)
    # Chuseok closes XKRX on 2026-09-24/25; weekends are closed.
    assert (result["planned"], result["covered"], result["held"]) == (11, 1, [])
    jobs = load_jobs((tmp_path / "jobs.json").read_bytes())
    assert [job.job_id for job in jobs][:3] == [
        "daily-KO-splits-2026-09-21",
        "daily-KO-prices-2026-09-22",
        "daily-KO-splits-2026-09-22",
    ]
    assert {job.observation_date for job in jobs} == {date(2026, 10, 1)}
    assert jobs[1].parameters == {"date": "2026-09-22", "exchange": "KO", "fmt": "json"}
    assert jobs[2].parameters["type"] == "splits"
    assert result["calendars"]["XKRX"] == qveris_daily.declaration_for("XKRX").sha256
    assert main(_daily(raw, tmp_path / "jobs.json", *args)) == 1
    assert "never overwritten" in capsys.readouterr().err


def test_an_attempt_without_a_completion_is_held_not_planned(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    job = bulk_job("US", "prices", "2026-09-08", date(2026, 9, 9))
    client = ScriptedQveris({}, account_key(raw))
    client.failure = "timeout"
    client.usage_ready = False
    with pytest.raises(RuntimeError):
        acquire_jobs((job,), raw, client)
    result = qveris_daily.plan_daily_jobs(
        raw,
        exchanges=["US"],
        datasets=["prices"],
        start=date(2026, 9, 7),
        through=date(2026, 9, 8),
        observation_date=date(2026, 9, 9),
    )
    # Labor Day closes XNYS on 2026-09-07.
    assert result["planned"] == 0
    assert result["document"] is None
    assert result["held"] == [
        {"job_id": "daily-US-prices-2026-09-08", "attempted_fingerprints": (job.fingerprint,)}
    ]


@pytest.mark.parametrize(
    ("start", "through", "observed", "match"),
    [
        (date(2026, 9, 2), date(2026, 9, 1), date(2026, 9, 3), "ordered"),
        (date(2026, 9, 1), date(2026, 9, 3), date(2026, 9, 3), "after its date"),
        (date(1989, 12, 1), date(1990, 1, 2), date(1990, 1, 3), "declared"),
        (date(2025, 1, 1), date(2026, 9, 1), date(2026, 9, 3), "at most"),
    ],
)
def test_windows_are_bounded_and_inside_the_declaration(
    start: date, through: date, observed: date, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        qveris_daily.plan_daily_jobs(
            None,
            exchanges=["US"],
            datasets=["prices"],
            start=start,
            through=through,
            observation_date=observed,
        )


def test_lookback_window_ends_the_day_before_observation() -> None:
    assert qveris_daily.default_window(date(2026, 10, 3), 3) == (
        date(2026, 9, 30),
        date(2026, 10, 2),
    )


def _jobs_file(tmp_path: Path, *jobs: QverisJob) -> Path:
    from aegis_alpha.data.serialization import canonical_json_bytes  # noqa: PLC0415

    path = tmp_path / "jobs.json"
    path.write_bytes(
        canonical_json_bytes({"schema_version": 1, "jobs": [j.document() for j in jobs]})
    )
    return path


def test_plan_reads_no_key_and_counts_completed_requests(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = tmp_path / "raw"
    first, second = bulk_job("KO", "prices", "2026-09-01"), bulk_job("KO", "prices", "2026-09-02")
    complete(raw, first, [{**KO_ROW, "date": "2026-09-01"}])
    jobs = _jobs_file(tmp_path, first, second)
    code = main(["collect", "qveris", "plan", "--jobs", str(jobs), "--raw-root", str(raw)])
    assert code == 0
    result = json.loads(capsys.readouterr().out)
    (document,) = result["documents"]
    assert (document["completed"], document["held"], document["new"]) == (1, 0, 1)
    assert result["http_calls"] == 0
    missing = tmp_path / "absent"
    assert main(["collect", "qveris", "plan", "--jobs", str(jobs), "--raw-root", str(missing)]) == 0
    assert not missing.exists()


@pytest.mark.parametrize("workers", ["1", "2"])
def test_run_stops_at_the_paid_call_limit_and_resumes_without_repeating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workers: str,
) -> None:
    raw = tmp_path / "raw"
    days = ("2026-09-01", "2026-09-02", "2026-09-03")
    jobs = [bulk_job("KO", "prices", day) for day in days]
    rows = {job.parameters_json: [{**KO_ROW, "date": job.parameters["date"]}] for job in jobs}
    client = ScriptedQveris(rows, account_key(raw))
    monkeypatch.setattr(qveris_client, "QverisClient", lambda *_a, **_k: client)
    path = _jobs_file(tmp_path, *jobs)
    base = ["collect", "qveris", "run", "--jobs", str(path), "--raw-root", str(raw)]
    limits = ["--key-file", str(tmp_path / "key"), "--workers", workers, "--request-interval", "0"]
    assert main([*base, *limits, "--max-calls", "2", "--max-credits", "10"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert (first["status"], first["paid_executions"], first["reserved_calls"]) == (
        "budget_exhausted",
        2,
        2,
    )
    assert client.execute_count == 2  # noqa: PLR2004
    assert main([*base, *limits, "--max-calls", "5", "--max-credits", "10"]) == 0
    second = json.loads(capsys.readouterr().out)
    assert (second["status"], second["paid_executions"]) == ("succeeded", 1)
    assert client.execute_count == len(days)
    assert Decimal(second["reserved_credits"]) == Decimal("2.81")


def test_run_stops_with_exit_two_when_a_paid_call_is_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = tmp_path / "raw"
    jobs = [bulk_job("KO", "prices", day) for day in ("2026-09-01", "2026-09-02")]
    client = ScriptedQveris({}, account_key(raw))
    client.failure = "timeout"
    client.usage_ready = False
    monkeypatch.setattr(qveris_client, "QverisClient", lambda *_a, **_k: client)
    path = _jobs_file(tmp_path, *jobs)
    code = main(
        [
            *("collect", "qveris", "run", "--jobs", str(path), "--raw-root", str(raw)),
            *("--key-file", str(tmp_path / "key"), "--max-calls", "5", "--max-credits", "10"),
            *("--request-interval", "0"),
        ]
    )
    assert code == 2  # noqa: PLR2004 -- stopped, not partial
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "stopped"
    assert client.execute_count == 1


def _unresolved(raw: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    from aegis_alpha.data.qveris_store import QverisStore  # noqa: PLC0415

    with QverisStore(raw, account_key(raw)) as store:
        return store.pending_pages(), store.pending_batches()


def _clean_stop_then_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    workers: str,
    limit: list[str],
) -> dict[str, object]:
    tmp_path.mkdir()
    raw = tmp_path / "raw"
    jobs = [bulk_job("KO", "prices", day) for day in ("2026-09-01", "2026-09-02")]
    rows = {job.parameters_json: [{**KO_ROW, "date": job.parameters["date"]}] for job in jobs}
    client = ScriptedQveris(rows, account_key(raw))
    monkeypatch.setattr(qveris_client, "QverisClient", lambda *_a, **_k: client)
    path = _jobs_file(tmp_path, *jobs)
    base = [
        *("collect", "qveris", "run", "--jobs", str(path), "--raw-root", str(raw)),
        *("--key-file", str(tmp_path / "key"), "--max-calls", "5", "--max-credits", "10"),
        *("--request-interval", "0", "--workers", workers),
    ]
    code = main([*base, *limit])
    captured = capsys.readouterr()
    assert captured.out, captured.err
    first = json.loads(captured.out)
    # A stop the run reports as success-like never leaves a page or a group unresolved.
    assert (code, first["status"]) in {(0, "budget_exhausted"), (0, "succeeded")}
    assert _unresolved(raw) == ((), ())
    assert client.execute_count == first["paid_executions"]
    assert main(base) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["status"] == "succeeded"
    assert client.execute_count == len(jobs)
    return first


@pytest.mark.parametrize("workers", ["1", "2"])
def test_a_request_limit_at_any_position_never_strands_a_started_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workers: str,
) -> None:
    position = 1
    while True:
        first = _clean_stop_then_resume(
            tmp_path / f"n{position}",
            monkeypatch,
            capsys,
            workers=workers,
            limit=["--max-http-requests", str(position)],
        )
        if first["status"] == "succeeded":
            break
        assert int(str(first["http_requests"])) >= position
        position += 1
    assert position > 2  # noqa: PLR2004 -- the limit fell inside a job before it covered both


def _stepping_admission(max_requests: int, seconds: float) -> RequestAdmission:
    """``RequestAdmission`` on a clock that advances one second per reading."""
    ticks = iter(range(10_000))
    return RequestAdmission(max_requests, seconds, clock=lambda: float(next(ticks)))


@pytest.mark.parametrize("workers", ["1", "2"])
def test_a_time_limit_at_any_position_never_strands_a_started_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workers: str,
) -> None:
    monkeypatch.setattr(qveris_pacing, "RequestAdmission", _stepping_admission)
    seconds = 1
    while True:
        first = _clean_stop_then_resume(
            tmp_path / f"t{seconds}",
            monkeypatch,
            capsys,
            workers=workers,
            limit=["--time-limit-seconds", str(seconds)],
        )
        if first["status"] == "succeeded":
            break
        seconds += 1
    assert seconds > 2  # noqa: PLR2004


@pytest.mark.parametrize("workers", ["1", "2"])
def test_quarantine_releases_the_account_and_keeps_the_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workers: str,
) -> None:
    raw = tmp_path / "raw"
    lost = [bulk_job("KO", "prices", day) for day in ("2026-09-01", "2026-09-02")]
    later = bulk_job("KO", "prices", "2026-09-03")
    rows = {later.parameters_json: [{**KO_ROW, "date": "2026-09-03"}]}
    client = ScriptedQveris(rows, account_key(raw))
    client.failure = "timeout"
    client.usage_ready = False
    monkeypatch.setattr(qveris_client, "QverisClient", lambda *_a, **_k: client)
    limits = [
        *("--key-file", str(tmp_path / "key"), "--max-calls", "5", "--max-credits", "10"),
        *("--request-interval", "0", "--workers", workers),
    ]

    def run(*jobs: QverisJob) -> int:
        (tmp_path / "jobs.json").unlink(missing_ok=True)
        path = _jobs_file(tmp_path, *jobs)
        return main(
            ["collect", "qveris", "run", "--jobs", str(path), "--raw-root", str(raw), *limits]
        )

    assert run(*lost) == 2  # noqa: PLR2004 -- an uncertain paid call
    capsys.readouterr()
    # The lost calls' usage events never appear, so their evidence can never settle.
    client.failure, client.usage_ready, client.usage = None, True, []
    pages, batches = _unresolved(raw)
    target = ["--page", pages[0]] if workers == "1" else ["--batch", batches[0]]
    assert run(later) == 2  # noqa: PLR2004 -- the unresolved evidence blocks the account
    capsys.readouterr()
    base = ["collect", "qveris", "quarantine", "--raw-root", str(raw), "--key-file", "k"]
    assert main([*base, *target, "--reason", "usage event never appeared"]) == 0
    record = json.loads(capsys.readouterr().out)
    assert record["automatic_retry"] is False
    assert Decimal(str(record["reserved_credits"])) == Decimal("2.81") * (
        1 if workers == "1" else 2
    )
    assert _unresolved(raw) == ((), ())
    assert run(later) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "succeeded"
    with pytest.raises(SystemExit):
        main([*base, "--reason", "no target"])
