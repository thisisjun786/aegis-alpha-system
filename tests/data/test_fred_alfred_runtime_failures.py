"""FRED publication, recovery and authority refusals on disposable state."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from queue import Queue

import pytest
from fred_alfred_collector_support import CREDENTIAL
from sqlalchemy import Connection, Engine, event, select, text

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.collection.schema import collection_runs, collection_watermarks
from aegis_alpha.data import fred_alfred_collector as collector
from aegis_alpha.data.fred_alfred_collector import CollectorError
from aegis_alpha.data.fred_alfred_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fred_alfred_runtime import run_runtime
from aegis_alpha.data.fred_alfred_usage_budget import load_daily_usage_budget
from tests.data.test_fred_alfred_runtime import _setup

_CREDENTIAL = "synthetic-fred-runtime-key"


def test_distinct_run_ids_sharing_suffix_preserve_all_first_run_bytes(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)

    config = replace(config, run_identity="first-identical-tail")
    opener.run_id = config.run_identity
    first = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    original = {path: path.read_bytes() for path in first.published_paths}
    second_config = replace(
        config, run_identity="second-identical-tail", receipt_path=tmp_path / "second.receipt.json"
    )
    opener.run_id = second_config.run_identity
    second = run_runtime(
        config=second_config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert set(first.published_paths).isdisjoint(second.published_paths)
    assert {path: path.read_bytes() for path in original} == original


@pytest.mark.parametrize("tamper", ["receipt", "raw", "parquet"])
def test_recovery_refuses_tampered_evidence_without_http(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch, tamper: str
) -> None:

    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    result = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    if tamper == "receipt":
        target = config.receipt_path
    elif tamper == "raw":
        digest = str(result.rows[0]["raw_content_sha256"])
        target = config.raw_store_root / "blobs" / "sha256" / digest[:2] / f"{digest}.raw"
    else:
        target = next(path for path in result.published_paths if path.suffix == ".parquet")
    calls = len(opener.urls)
    target.chmod(0o600)
    target.write_bytes(b"tampered")
    with pytest.raises((CollectorError, ValueError), match="differ"):
        run_runtime(
            config=config,
            engine=clean_postgres,
            authority=authority,
            credential=CREDENTIAL,
            clock=clock.now,
            monotonic=clock.time,
            sleep=clock.sleep,
        )
    assert len(opener.urls) == calls


def test_publication_failure_accounts_calls_without_advancing(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:

    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    publish = collector._publish_immutable  # noqa: SLF001 -- inject publication boundary failure

    def broken(path: Path, body: bytes) -> None:
        if path.suffix == ".parquet":
            raise OSError("synthetic publication failure")
        publish(path, body)

    monkeypatch.setattr(collector, "_publish_immutable", broken)
    with pytest.raises(OSError, match="synthetic publication failure"):
        run_runtime(
            config=config,
            engine=clean_postgres,
            authority=authority,
            credential=CREDENTIAL,
            clock=clock.now,
            monotonic=clock.time,
            sleep=clock.sleep,
        )
    assert load_daily_usage_budget(
        engine=clean_postgres, authority=authority, now=clock.now()
    ).calls_used == len(opener.urls)
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_watermarks)).all() == []
    assert not config.receipt_path.exists()


def test_authority_is_rechecked_after_actual_provider_lock_wait(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    authority = replace(authority, key_valid_until_utc=clock.now() + timedelta(seconds=1))
    pids: Queue[int] = Queue()

    def trace(connection: Connection, _cursor: object, statement: str, *_args: object) -> None:
        if "pg_advisory_lock(" in statement:
            pids.put(connection.execute(text("SELECT pg_backend_pid()")).scalar_one())

    with clean_postgres.connect().execution_options(isolation_level="AUTOCOMMIT") as holder:
        holder.execute(
            text(
                "SELECT pg_advisory_lock(hashtextextended("
                "'aegis_alpha.fred_alfred.runtime_provider',0))"
            )
        )
        holder_pid = holder.scalar(text("SELECT pg_backend_pid()"))
        event.listen(clean_postgres, "before_cursor_execute", trace)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    run_runtime,
                    config=config,
                    engine=clean_postgres,
                    authority=authority,
                    credential=_CREDENTIAL,
                    clock=clock.now,
                    monotonic=clock.time,
                    sleep=clock.sleep,
                )
                try:
                    pid = pids.get(timeout=5)
                    blocked = False
                    deadline = time.monotonic() + 5
                    with clean_postgres.connect() as observer:
                        while time.monotonic() < deadline:
                            if holder_pid in observer.scalar(
                                text("SELECT pg_blocking_pids(:pid)"), {"pid": pid}
                            ):
                                blocked = True
                                break
                            time.sleep(0.01)
                    assert blocked
                    clock.seconds += 2
                finally:
                    holder.execute(
                        text(
                            "SELECT pg_advisory_unlock(hashtextextended("
                            "'aegis_alpha.fred_alfred.runtime_provider',0))"
                        )
                    )
                with pytest.raises(RecurringAuthorityError, match="expired"):
                    future.result(timeout=5)
        finally:
            event.remove(clean_postgres, "before_cursor_execute", trace)
    assert opener.urls == []
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_runs)).all() == []


def test_durable_call_budget_exhaustion_advances_nothing(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    config = replace(config, max_calls=2)
    result = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert result.terminal_event is RunEventType.RUN_FAILED
    assert len(opener.urls) == config.max_calls
    assert result.calls_attempted == config.max_calls
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_watermarks)).all() == []
