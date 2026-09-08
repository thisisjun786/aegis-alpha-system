"""Shared-budget price reads preserve bytes, bounded scheduling and cancellation."""
# ruff: noqa: SLF001, PLR2004
# Internal phase seams are observed/injected to prove concurrent boundary behavior.

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeCancelledError, compute_lease
from aegis_alpha.data import pinned_prices
from aegis_alpha.data.catalog_access import DatasetView
from aegis_alpha.data.pinned_prices import PriceQuery, read_prices
from tests.data.test_pinned_prices import _artifact, _query, _row, _view

if TYPE_CHECKING:
    from collections.abc import Callable

    from duckdb import DuckDBPyConnection

    from aegis_alpha.data.descriptor_tree import DescriptorTree


def _parts(root: Path, count: int = 8) -> DatasetView:
    artifacts = []
    for index in range(count):
        artifact = _artifact(root, [_row(f"i-{index}")])
        renamed = Path(artifact.relative_path).with_name(f"part-{index + 1:05d}.parquet")
        (root / artifact.relative_path).rename(root / renamed)
        artifacts.append(replace(artifact, relative_path=renamed.as_posix()))
    return _view(*artifacts)


def _budget(cpu: int = 4) -> ComputeBudget:
    return ComputeBudget(Fraction(cpu), 256 * 1024**2)


def test_parallel_and_serial_reports_are_identical(tmp_path: Path) -> None:
    view = _parts(tmp_path)
    query = _query(instruments=("i-0", "i-7"))
    serial = read_prices(view, tmp_path, query)
    assert read_prices(view, tmp_path, query, budget=_budget()) == serial
    assert serial["row_count"] == 2


def test_bounded_hash_queue_drains_before_native_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = _parts(tmp_path, count=12)
    budget = _budget(2)
    release, guard = Event(), Lock()
    submitted = peak = active = peak_active = 0
    original_submit = ThreadPoolExecutor.submit
    original_verify = pinned_prices._verify_file
    original_query = pinned_prices._query_prices
    observed_threads: list[int] = []

    def finished(_future: Future[object]) -> None:
        nonlocal submitted
        with guard:
            submitted -= 1

    def submit(
        self: ThreadPoolExecutor, fn: Callable[..., object], *args: object, **kwargs: object
    ) -> Future[object]:
        nonlocal submitted, peak
        with guard:
            submitted += 1
            peak = max(peak, submitted)
            if submitted == budget.max_in_flight:
                release.set()
        future = original_submit(self, fn, *args, **kwargs)
        future.add_done_callback(finished)
        return future

    def verify(
        tree: DescriptorTree, pin: pinned_prices._PinnedFile, **kwargs: Event | None
    ) -> None:
        nonlocal active, peak_active
        with guard:
            active += 1
            peak_active = max(peak_active, active)
        try:
            assert release.wait(5)
            original_verify(tree, pin, **kwargs)
        finally:
            with guard:
                active -= 1

    def query(
        connection: DuckDBPyConnection, paths: list[str], request: PriceQuery
    ) -> tuple[list[dict[str, object]], bool]:
        assert active == 0
        assert not any(thread.name.startswith("aas-price-hash") for thread in threading.enumerate())
        value = connection.execute("SELECT current_setting('threads')").fetchone()
        assert value is not None
        observed_threads.append(value[0])
        return original_query(connection, paths, request)

    monkeypatch.setattr(ThreadPoolExecutor, "submit", submit)
    monkeypatch.setattr(pinned_prices, "_verify_file", verify)
    monkeypatch.setattr(pinned_prices, "_query_prices", query)
    result = read_prices(view, tmp_path, _query(instruments=("i-0",)), budget=budget)
    assert result["row_count"] == 1
    assert peak == budget.max_in_flight
    assert peak_active <= budget.hash_workers
    assert observed_threads == [budget.duckdb_threads]
    assert submitted == active == 0


def test_hash_cancellation_drains_workers_before_releasing_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = _parts(tmp_path)
    cancel = Event()
    original = pinned_prices._verify_file
    touched: list[bool] = []

    def cancelled(
        tree: DescriptorTree, pin: pinned_prices._PinnedFile, **kwargs: Event | None
    ) -> None:
        touched.append(True)
        cancel.set()
        original(tree, pin, **kwargs)

    monkeypatch.setattr(pinned_prices, "_verify_file", cancelled)
    lock = tmp_path / "compute.lock"
    with pytest.raises(ComputeCancelledError, match="cancelled"), compute_lease(lock):
        read_prices(view, tmp_path, _query(), budget=_budget(), cancel_event=cancel)
    assert touched
    assert not any(thread.name.startswith("aas-price-") for thread in threading.enumerate())
    monkeypatch.setattr(pinned_prices, "_verify_file", original)
    with compute_lease(lock):
        assert (
            read_prices(view, tmp_path, _query(instruments=("i-0",)), budget=_budget())["row_count"]
            == 1
        )


def test_native_query_cancellation_interrupts_and_closes_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = _parts(tmp_path)
    started, cancel = Event(), Event()

    def long_query(
        connection: DuckDBPyConnection, _paths: list[str], _request: PriceQuery
    ) -> tuple[list[dict[str, object]], bool]:
        started.set()
        connection.execute("SELECT sum(i::DOUBLE) FROM range(1000000000000) t(i)").fetchone()
        pytest.fail("native work should be interrupted")

    def cancel_query() -> None:
        if started.wait(5):
            cancel.set()

    canceller = Thread(target=cancel_query)
    monkeypatch.setattr(pinned_prices, "_query_prices", long_query)
    canceller.start()
    try:
        with pytest.raises(ComputeCancelledError, match="cancelled"):
            read_prices(view, tmp_path, _query(), budget=_budget(), cancel_event=cancel)
    finally:
        canceller.join(5)
    assert started.is_set()
    assert not canceller.is_alive()
    assert not any(thread.name.startswith("aas-price-") for thread in threading.enumerate())


@pytest.mark.parametrize("when", ["before", "after"])
def test_parallel_hashing_retains_tamper_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, when: str
) -> None:
    view = _parts(tmp_path)
    target = tmp_path / view.artifacts[-1].relative_path
    if when == "before":
        target.write_bytes(b"tampered")
    else:
        original = pinned_prices._query_prices

        def tamper(
            connection: DuckDBPyConnection, paths: list[str], query: PriceQuery
        ) -> tuple[list[dict[str, object]], bool]:
            result = original(connection, paths, query)
            target.write_bytes(b"tampered")
            return result

        monkeypatch.setattr(pinned_prices, "_query_prices", tamper)
    with pytest.raises(ValueError, match=r"state changed|size mismatch|hash mismatch"):
        read_prices(view, tmp_path, _query(instruments=("i-0",)), budget=_budget())
