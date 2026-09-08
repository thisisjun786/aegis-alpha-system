"""Durable SEC execution against disposable PostgreSQL and synthetic responses."""
# ruff: noqa: PLR2004, SLF001
# Exact fixture counts and injected failure points are intentional contract assertions.

from __future__ import annotations

import io
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pytest
from sec_collector_support import (
    AS_OF,
    FIXTURE_ROOT,
    FakeClock,
    fixture_bodies,
    load_named_identity,
    synthetic_user_agent,
)
from sqlalchemy import select

from aegis_alpha.collection.records import CollectionMode, CollectionRunEvent, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_receipts,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.data import sec_collector_cli, sec_runtime
from aegis_alpha.data.sec_collector import DATASET, CollectorConfig, CollectorError
from aegis_alpha.data.sec_rate_limit import RateLimiter
from aegis_alpha.data.sec_runtime import provider_lock, run_sec_collection
from aegis_alpha.data.sec_transport import (
    CollectorRequest,
    CollectorResponse,
    make_fixture_transport,
)
from aegis_alpha.metadata.schema import dataset_versions, source_snapshots

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine, Table

    from aegis_alpha.data.sec_collector import CollectorOutcome


def _config(root: Path, identity: str = "fixture-one") -> CollectorConfig:
    return CollectorConfig(
        root / "raw",
        root / "data",
        root / "receipts" / f"receipt.{identity}.json",
        CollectionMode.BACKFILL,
        4,
        identity,
        AS_OF,
    )


def _run(
    engine: Engine,
    config: CollectorConfig,
    *,
    transport: Callable[[CollectorRequest], CollectorResponse] | None = None,
) -> CollectorOutcome:
    clock = FakeClock()
    return run_sec_collection(
        config,
        engine=engine,
        identity=load_named_identity("identity_admitted.json"),
        transport=transport or make_fixture_transport(fixture_bodies(), clock=clock.now),
        limiter=RateLimiter(max_calls=config.max_calls, clock=clock.time, sleep=clock.sleep),
        clock=clock.now,
        synthetic=True,
    )


def _no_http(_request: CollectorRequest) -> CollectorResponse:
    pytest.fail("verified recovery or preflight refusal must not call transport")


def _rows(engine: Engine, table: Table) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(select(table)).mappings()]


def test_success_registers_response_provenance_and_atomic_receipt(
    clean_postgres: Engine, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    outcome = _run(clean_postgres, config)
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.calls_attempted == 2
    snapshots = {row["snapshot_id"]: row for row in _rows(clean_postgres, source_snapshots)}
    assert len(snapshots) == 3
    encountered: set[str] = set()
    for path in outcome.published_paths:
        if path.suffix != ".parquet":
            continue
        for row in pq.ParquetFile(path).read().to_pylist():
            snapshot = snapshots[row["snapshot_id"]]
            assert row["raw_content_sha256"] == snapshot["content_sha256"]
            assert snapshot["provider"] == "sec"
            assert snapshot["dataset"] in {"submissions", "companyfacts"}
            requested, retrieved = snapshot["requested_at_utc"], snapshot["retrieved_at_utc"]
            assert isinstance(requested, datetime)
            assert isinstance(retrieved, datetime)
            assert requested <= retrieved
            encountered.add(row["snapshot_id"])
    assert len(encountered) == 2
    receipts = _rows(clean_postgres, collection_run_receipts)
    assert len(receipts) == 1
    assert snapshots[receipts[0]["source_snapshot_id"]]["dataset"] == DATASET
    assert [r["event_type"] for r in _rows(clean_postgres, collection_run_events)] == [
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    ]
    assert len(_rows(clean_postgres, collection_watermarks)) == 2
    usage = _rows(clean_postgres, collection_usage_records)
    assert len(usage) == 3
    assert {r["metric"]: int(str(r["quantity"]).split(".")[0]) for r in usage}[
        "calls_attempted"
    ] == 2
    datasets = _rows(clean_postgres, dataset_versions)
    assert len(datasets) == 1
    assert datasets[0]["backtest_eligible"] is False


def test_successive_runs_preserve_first_bytes_and_replay_without_http(
    clean_postgres: Engine, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    first = _run(clean_postgres, config)
    original = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    second = _run(
        clean_postgres,
        replace(
            config,
            run_identity="fixture-two",
            receipt_path=config.receipt_path.with_name("receipt.fixture-two.json"),
        ),
    )
    assert first.run_id != second.run_id
    assert set(first.published_paths).isdisjoint(second.published_paths)
    assert all(path.read_bytes() == payload for path, payload in original.items())
    assert _run(clean_postgres, config, transport=_no_http) == first
    assert len(_rows(clean_postgres, dataset_versions)) == 2
    assert len(_rows(clean_postgres, collection_usage_records)) == 6


@pytest.mark.parametrize("failure", ["http", "timeout", "parse", "publish"])
def test_failures_account_before_terminal_without_watermarks(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    clock = FakeClock()
    calls: list[CollectorRequest] = []
    fixture = make_fixture_transport(fixture_bodies(), clock=clock.now)

    def transport(request: CollectorRequest) -> CollectorResponse:
        calls.append(request)
        if failure == "timeout":
            raise TimeoutError("synthetic timeout")
        if failure == "parse":
            return CollectorResponse(200, {}, b"not json", clock.now(), clock.now())
        if failure == "http":
            return CollectorResponse(400, {}, b"bad request", clock.now(), clock.now())
        return fixture(request)

    if failure == "publish":

        def fail_publish(*_args: object, **_kwargs: object) -> tuple[Path, ...]:
            raise FileExistsError("synthetic publication conflict")

        monkeypatch.setattr(sec_runtime._DurableSecCollector, "_publish", fail_publish)
    original_event = CollectionRegistry.append_event

    def checked_event(self: CollectionRegistry, event: CollectionRunEvent, **kwargs: object) -> int:
        if event.event_type is RunEventType.RUN_FAILED:
            assert len(_rows(clean_postgres, collection_usage_records)) == 3
        return original_event(self, event, **kwargs)  # ty: ignore[invalid-argument-type] # wrapped test boundary

    monkeypatch.setattr(CollectionRegistry, "append_event", checked_event)
    outcome = _run(clean_postgres, _config(tmp_path), transport=transport)
    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.calls_attempted == len(calls)
    assert _rows(clean_postgres, collection_watermarks) == []
    assert _rows(clean_postgres, collection_run_receipts) == []
    with pytest.raises(CollectorError, match="automatic HTTP replay refused"):
        _run(clean_postgres, _config(tmp_path), transport=_no_http)


def test_finalization_interruption_recovers_without_http(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    original = sec_runtime.finish_run

    def stop(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("injected finalization failure")

    monkeypatch.setattr(sec_runtime, "finish_run", stop)
    with pytest.raises(CollectorError, match="runtime failed"):
        _run(clean_postgres, config)
    assert [r["event_type"] for r in _rows(clean_postgres, collection_run_events)] == [
        "attempt_started"
    ]
    assert _rows(clean_postgres, collection_watermarks) == []
    monkeypatch.setattr(sec_runtime, "finish_run", original)
    recovered = _run(clean_postgres, config, transport=_no_http)
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert recovered.calls_attempted == 2
    assert len(_rows(clean_postgres, collection_usage_records)) == 3


def test_watermark_failure_rolls_back_receipt_dataset_and_terminal(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = CollectionRegistry.advance_watermark

    def interrupted(self: CollectionRegistry, advance: object, **kwargs: object) -> int:
        original(self, advance, **kwargs)  # ty: ignore[invalid-argument-type] # injected transaction failure
        raise RuntimeError("injected after watermark insert")

    monkeypatch.setattr(CollectionRegistry, "advance_watermark", interrupted)
    config = _config(tmp_path)
    with pytest.raises(CollectorError, match="runtime failed"):
        _run(clean_postgres, config)
    assert _rows(clean_postgres, collection_watermarks) == []
    assert _rows(clean_postgres, collection_run_receipts) == []
    assert _rows(clean_postgres, dataset_versions) == []
    assert [r["event_type"] for r in _rows(clean_postgres, collection_run_events)] == [
        "attempt_started"
    ]
    monkeypatch.setattr(CollectionRegistry, "advance_watermark", original)
    assert (
        _run(clean_postgres, config, transport=_no_http).terminal_event
        is RunEventType.RUN_SUCCEEDED
    )


@pytest.mark.parametrize("target", ["marker", "raw", "parquet", "request"])
def test_recovery_tampering_fails_closed(
    clean_postgres: Engine, tmp_path: Path, target: str
) -> None:
    config = _config(tmp_path)
    outcome = _run(clean_postgres, config)
    if target == "marker":
        path = config.dataset_root / "runs" / outcome.run_id / "finalization.json"
    elif target == "request":
        path = config.dataset_root / "runs" / outcome.run_id / "request.json"
    elif target == "parquet":
        path = next(path for path in outcome.published_paths if path.suffix == ".parquet")
    else:
        path = next(config.raw_store_root.rglob("*.raw"))
    path.write_bytes(b"tampered")
    with pytest.raises((ValueError, CollectorError)):
        _run(clean_postgres, config, transport=_no_http)


def test_provider_lock_refuses_second_invocation(clean_postgres: Engine, tmp_path: Path) -> None:
    with provider_lock(clean_postgres), pytest.raises(CollectorError, match="already running"):
        _run(clean_postgres, _config(tmp_path), transport=_no_http)
    assert not (tmp_path / "data").exists()


def test_live_policy_is_blocked_without_db_or_transport(tmp_path: Path) -> None:
    clock = FakeClock()
    # None is deliberately unreachable: the frozen policy must refuse before DB construction.
    with pytest.raises(CollectorError, match="scheduled collection"):
        run_sec_collection(
            _config(tmp_path),
            engine=None,  # ty: ignore[invalid-argument-type]
            identity=load_named_identity("identity_admitted.json"),
            transport=_no_http,
            limiter=RateLimiter(max_calls=4, clock=clock.time, sleep=clock.sleep),
            clock=clock.now,
            user_agent=synthetic_user_agent(),
        )
    assert not (tmp_path / "data").exists()


def test_cli_live_dispatch_uses_durable_runtime_with_fixture_transport(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture_clock = FakeClock()
    transport = make_fixture_transport(fixture_bodies(), clock=fixture_clock.now)
    # External authority and network seams are fixtures; DB/publication calls stay real.
    monkeypatch.setattr(sec_collector_cli, "require_live_policy", lambda *_args: None)
    monkeypatch.setattr(sec_runtime, "require_live_policy", lambda *_args: None)
    monkeypatch.setattr(sec_collector_cli, "make_https_transport", lambda **_kwargs: transport)
    monkeypatch.setattr(sec_collector_cli, "limiter_sleeper", lambda **_kwargs: fixture_clock.sleep)
    args = [
        "--live",
        "--mode",
        "backfill",
        "--max-calls",
        "4",
        "--run-identity",
        "cli-fixture",
        "--as-of",
        AS_OF.isoformat(),
        "--identity-snapshot",
        str(FIXTURE_ROOT / "identity_admitted.json"),
        "--raw-store-root",
        str(tmp_path / "raw"),
        "--dataset-root",
        str(tmp_path / "data"),
        "--receipt-path",
        str(tmp_path / "receipts/receipt.json"),
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    result = sec_collector_cli.main(
        args,
        environ={
            "AAS_DATABASE_URL": clean_postgres.url.render_as_string(hide_password=False),
            "SEC_USER_AGENT": synthetic_user_agent(),
        },
        stdout=stdout,
        stderr=stderr,
    )
    assert result == 0, stderr.getvalue()
    assert len(_rows(clean_postgres, collection_run_receipts)) == 1
    assert len(_rows(clean_postgres, source_snapshots)) == 3
    assert synthetic_user_agent() not in stdout.getvalue() + stderr.getvalue()
    # The same explicit run identity, receipt path and fixed cutoff resume without HTTP.
    monkeypatch.setattr(sec_collector_cli, "make_https_transport", lambda **_kwargs: _no_http)
    replay_stdout, replay_stderr = io.StringIO(), io.StringIO()
    assert (
        sec_collector_cli.main(
            args,
            environ={
                "AAS_DATABASE_URL": clean_postgres.url.render_as_string(hide_password=False),
                "SEC_USER_AGENT": synthetic_user_agent(),
            },
            stdout=replay_stdout,
            stderr=replay_stderr,
        )
        == 0
    )
    initial_report = json.loads(stdout.getvalue())
    replay_report = json.loads(replay_stdout.getvalue())
    assert initial_report.pop("provider_calls") == initial_report["calls_attempted"]
    assert replay_report.pop("provider_calls") == 0
    assert replay_report == initial_report
    assert len(_rows(clean_postgres, collection_usage_records)) == 3


def test_policy_revalidated_after_pacing_before_attempt_consumption(
    clean_postgres: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock()
    checks: list[int] = []

    def policy(*_args: object) -> None:
        checks.append(1)
        if len(checks) == 3:
            raise CollectorError("synthetic policy expiry after lock/pacing")

    monkeypatch.setattr(sec_runtime, "require_live_policy", policy)
    result = run_sec_collection(
        _config(tmp_path),
        engine=clean_postgres,
        identity=load_named_identity("identity_admitted.json"),
        transport=_no_http,
        limiter=RateLimiter(max_calls=4, clock=clock.time, sleep=clock.sleep),
        clock=clock.now,
        user_agent=synthetic_user_agent(),
    )
    assert result.terminal_event is RunEventType.RUN_FAILED
    assert result.calls_attempted == 0
    assert len(checks) == 3
    assert _rows(clean_postgres, collection_watermarks) == []


def test_missing_cik_is_blocked_before_db_or_files(tmp_path: Path) -> None:
    clock = FakeClock()
    with pytest.raises(CollectorError, match="pinned CIK"):
        run_sec_collection(
            _config(tmp_path),
            engine=None,  # ty: ignore[invalid-argument-type]
            identity=load_named_identity("identity_no_cik.json"),
            transport=_no_http,
            limiter=RateLimiter(max_calls=4, clock=clock.time, sleep=clock.sleep),
            clock=clock.now,
            synthetic=True,
        )
    assert not (tmp_path / "data").exists()
