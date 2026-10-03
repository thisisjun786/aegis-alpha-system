"""``scripts/calendar_compare.py`` over a text-typed export: synthetic rows only."""

from __future__ import annotations

from datetime import date

import duckdb

from scripts.calendar_compare import observed, undated

UNREADABLE = ("08/09/2026", "2026-02-30")


def test_text_dates_and_volumes_are_read_and_undated_rows_counted() -> None:
    connection = duckdb.connect()
    connection.execute('CREATE TABLE "bars" ("date" VARCHAR, volume VARCHAR)')
    connection.executemany(
        'INSERT INTO "bars" VALUES (?, ?)',
        [
            ("2026-09-08", "1000"),
            ("2026-09-08", "1.6357e+06"),
            ("2026-09-08", ""),
            ("2026-09-09", "0"),
            ("2026-09-09", None),
            *((text, "500") for text in UNREADABLE),
        ],
    )
    # An empty or zero volume is an untraded row; an unreadable date is no session at all.
    assert observed(connection, ["bars"], "date", "volume") == {
        date(2026, 9, 8): (3, 2, 2),
        date(2026, 9, 9): (2, 0, 2),
    }
    assert undated(connection, ["bars"], "date") == len(UNREADABLE)
