"""Midnight interleavings with a clock replacement confined to a disposable DB."""

from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Queue

import pytest
from sqlalchemy import Engine, select, text
from sqlalchemy.exc import IntegrityError

from aegis_alpha.collection.schema import collection_usage_records
from aegis_alpha.data.fred_alfred_usage_budget import _lock_daily_budget, utc_day_window
from tests.data.test_fred_alfred_settlement import _CALL, _SIGNATURE, _arguments, _bound


@pytest.fixture
def controlled_clock(clean_postgres: Engine) -> Iterator[Engine]:
    with clean_postgres.begin() as connection:
        original = connection.scalar(
            text("SELECT pg_get_functiondef(to_regprocedure(:signature))"),
            {"signature": _SIGNATURE},
        )
        connection.execute(
            text("CREATE TABLE engine.fred_fixture_clock (value timestamptz NOT NULL)")
        )
        connection.execute(
            text("INSERT INTO engine.fred_fixture_clock VALUES (:now)"), {"now": datetime.now(UTC)}
        )
        connection.execute(
            text("""CREATE FUNCTION engine.fred_fixture_now() RETURNS timestamptz
            LANGUAGE plpgsql VOLATILE AS $$ BEGIN
                RETURN (SELECT value FROM engine.fred_fixture_clock);
            END; $$""")
        )
        # Production keeps clock_timestamp(). Only this owned test DB uses a controllable clock.
        connection.exec_driver_sql(
            original.replace("clock_timestamp()", "engine.fred_fixture_now()")
        )
    try:
        yield clean_postgres
    finally:
        with clean_postgres.begin() as connection:
            connection.exec_driver_sql(original)
            connection.execute(text("DROP FUNCTION engine.fred_fixture_now()"))
            connection.execute(text("DROP TABLE engine.fred_fixture_clock"))


def _wait_for_block(engine: Engine, pid: int, holder_pid: int) -> None:
    deadline = time.monotonic() + 5
    with engine.connect() as observer:
        while time.monotonic() < deadline:
            if holder_pid in observer.scalar(text("SELECT pg_blocking_pids(:pid)"), {"pid": pid}):
                return
            time.sleep(0.01)
    pytest.fail("settlement did not block on the held lock")


@pytest.mark.parametrize("lock_site", ["advisory", "reservation_row", "unique_insert"])
@pytest.mark.parametrize("cross_midnight", [False, True])
def test_settlement_checks_utc_day_after_lock_waits(
    tmp_path: Path, controlled_clock: Engine, lock_site: str, *, cross_midnight: bool
) -> None:
    engine = controlled_clock
    bound = _bound(engine, tmp_path)
    arguments = _arguments(bound)
    with engine.begin() as clock_writer:
        clock_writer.execute(
            text("UPDATE engine.fred_fixture_clock SET value=:now"), {"now": bound[2]}
        )
    pids: Queue[int] = Queue()

    def settle() -> None:
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL statement_timeout='12s'"))
            pids.put(connection.execute(text("SELECT pg_backend_pid()")).scalar_one())
            connection.execute(_CALL, arguments)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with engine.connect() as holder:
            transaction = holder.begin()
            try:
                if lock_site == "advisory":
                    _lock_daily_budget(holder, day_start=utc_day_window(bound[2])[0])
                elif lock_site == "reservation_row":
                    holder.execute(
                        select(collection_usage_records)
                        .where(collection_usage_records.c.run_id == bound[0].reservation_run_id)
                        .with_for_update()
                    )
                else:
                    # Invisible to the function's prior-row SELECT, but its unique INSERT waits.
                    holder.execute(
                        collection_usage_records.insert().values(
                            run_id=bound[1],
                            usage_seq=1,
                            metric="calls_attempted",
                            quantity=1,
                            unit="call",
                            evidence_json={},
                            recorded_at_utc=bound[2],
                        )
                    )
                holder_pid = holder.execute(text("SELECT pg_backend_pid()")).scalar_one()
                future = executor.submit(settle)
                _wait_for_block(engine, pids.get(timeout=5), holder_pid)
                if cross_midnight:
                    with engine.begin() as clock_writer:
                        clock_writer.execute(
                            text("UPDATE engine.fred_fixture_clock SET value=:now"),
                            {"now": bound[2] + timedelta(days=1)},
                        )
            finally:
                # Releasing an uncommitted unique-key holder allows the waiting INSERT to proceed.
                transaction.rollback()
        if cross_midnight:
            with pytest.raises(IntegrityError, match="UTC day"):
                future.result(timeout=15)
        else:
            future.result(timeout=15)
    with engine.connect() as connection:
        rows = {
            row.run_id: row.quantity for row in connection.execute(select(collection_usage_records))
        }
    assert rows == (
        {bound[0].reservation_run_id: 5}
        if cross_midnight
        else {bound[0].reservation_run_id: 0, bound[1]: 3}
    )
