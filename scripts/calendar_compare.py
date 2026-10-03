"""Compare a packaged calendar declaration with the trading days a daily-bar source shows.

The comparison is review evidence for a declaration, never an input to it. It opens the
market DuckDB file read-only, reads every committed source-library table whose source id
starts with ``--source-prefix`` and whose table name is ``--table``, and reports, inside
the source's observed date range within the declaration (dates outside it are counted
apart, never as closed)::

    uv run --no-sync python -m scripts.calendar_compare --market MARKET.duckdb \\
        --calendar XKRX --source-prefix qveris-kr-history-62d23e53 --table bars

- an observed date has at least one row; a traded date has rows with positive volume for
  at least 5% of the largest such count within 30 calendar days either side, so a date
  with a few stale fills in a thin market is not mistaken for a session;
- declared sessions with no traded rows, split into dates with no rows at all and dates
  with only zero-volume or below-threshold rows (with their years and Saturdays);
- traded dates the declaration closes, each with its row count, traded count and window
  maximum, and the number of other declared-closed dates that carry rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from aegis_alpha.storage.calendar_declaration import packaged_declaration, parse_declaration

_THRESHOLD: Final = 0.05
_WINDOW_DAYS: Final = 30
_SATURDAY: Final = 5


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _targets(connection: duckdb.DuckDBPyConnection, prefix: str, table: str) -> list[str]:
    targets = []
    for (manifest,) in connection.execute(
        "SELECT manifest_json FROM source_library_commits WHERE starts_with(source_id, ?) "
        "ORDER BY source_id",
        [prefix],
    ).fetchall():
        targets.extend(
            str(item["target"])
            for item in json.loads(manifest)["tables"]
            if item["name"] == table and item["rows"]
        )
    if not targets:
        raise SystemExit(f"no committed {table} table under source id prefix {prefix}")
    return targets


def observed(
    connection: duckdb.DuckDBPyConnection,
    targets: list[str],
    date_column: str,
    volume_column: str,
) -> dict[date, tuple[int, int, int]]:
    """Each observed date's (rows, traded rows, largest traded count in its window)."""
    day, volume = _quote(date_column), _quote(volume_column)
    union = " UNION ALL ".join(
        f"SELECT CAST({day} AS DATE) AS d, {volume} AS v FROM {_quote(target)}"  # noqa: S608
        for target in targets
    )
    rows = connection.execute(
        "SELECT d, n, t, max(t) OVER (ORDER BY d RANGE BETWEEN "  # noqa: S608 -- quoted names
        f"INTERVAL {_WINDOW_DAYS} DAYS PRECEDING AND INTERVAL {_WINDOW_DAYS} DAYS FOLLOWING) "
        f"FROM (SELECT d, count(*) AS n, count(*) FILTER (WHERE v > 0) AS t FROM ({union}) "
        "GROUP BY d) ORDER BY d"
    ).fetchall()
    return {row[0]: (int(row[1]), int(row[2]), int(row[3])) for row in rows}


def compare(calendar_id: str, days: dict[date, tuple[int, int, int]]) -> dict[str, object]:
    raw = packaged_declaration(calendar_id)
    declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
    covered = {
        day: value for day, value in days.items() if declaration.start <= day < declaration.end
    }
    outside = len(days) - len(covered)
    days = covered
    first, last = min(days), max(days)
    status = {item.session_date: item.status for item in declaration.days()}
    declared = {day for day, value in status.items() if value == "open" and first <= day <= last}
    traded = {
        day
        for day, (_, count, window) in days.items()
        if count > 0 and count >= _THRESHOLD * window
    }
    untraded = sorted(declared - traded)
    no_rows = [day for day in untraded if day not in days]
    thin = [day for day in untraded if day in days]
    closed_traded = sorted(traded - declared)
    return {
        "calendar_id": calendar_id,
        "declaration_sha256": declaration.sha256,
        "observed_range": [first.isoformat(), last.isoformat()],
        "declared_sessions": len(declared),
        "observed_dates": len(days),
        "observed_dates_outside_declaration": outside,
        "traded_dates": len(traded),
        "both": len(declared & traded),
        "traded_but_declared_closed": [
            {
                "date": day.isoformat(),
                "weekday": day.strftime("%a"),
                "rows": days[day][0],
                "traded": days[day][1],
                "window_max": days[day][2],
            }
            for day in closed_traded
        ],
        "declared_but_not_traded": {
            "total": len(untraded),
            "no_rows": [day.isoformat() for day in no_rows],
            "thin": len(thin),
            "thin_by_year": dict(sorted(Counter(str(day.year) for day in thin).items())),
            "thin_saturdays": sum(1 for day in thin if day.weekday() == _SATURDAY),
        },
        "declared_closed_dates_with_untraded_rows": sum(
            1 for day in days if day not in declared and day not in traded
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--market", type=Path, required=True, help="market DuckDB file")
    parser.add_argument("--calendar", required=True, help="packaged calendar, e.g. XKRX")
    parser.add_argument("--source-prefix", required=True, help="source id prefix")
    parser.add_argument("--table", required=True, help="source table name, e.g. bars")
    parser.add_argument("--date-column", default="date")
    parser.add_argument("--volume-column", default="volume")
    args = parser.parse_args()
    connection = duckdb.connect(str(args.market), read_only=True)
    try:
        targets = _targets(connection, args.source_prefix, args.table)
        days = observed(connection, targets, args.date_column, args.volume_column)
    finally:
        connection.close()
    report = compare(args.calendar.upper(), days)
    document = {"source_prefix": args.source_prefix, "tables": len(targets), **report}
    sys.stdout.write(json.dumps(document) + "\n")


if __name__ == "__main__":
    main()
