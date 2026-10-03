# ruff: noqa: PLR2004 -- synthetic session counts are the expected values
"""``aas calendar refresh`` on synthetic declarations: generations, pins, times and plans.

Every expected instant is computed with Python's zone data from the declared local
hours, independently of the DuckDB mapper that stores them.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime
from fractions import Fraction
from pathlib import Path
from typing import Final, cast
from zoneinfo import ZoneInfo

import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import market
from aegis_alpha.storage.bulk_generation import verify_generation_bulk
from aegis_alpha.storage.calendar_refresh import refresh_calendar, timezone_version
from aegis_alpha.storage.market_inputs import GenerationPin, load_pinned_heads
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin, HeadQuery
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar, register_symbols, spec, us

_ROOT: Final = Path(__file__).resolve().parents[2]
ZONE: Final = ZoneInfo("Asia/Seoul")
BUDGET: Final = ComputeBudget(Fraction(1), 256 * 1024 * 1024)
NOW: Final = us(at("2025-06-01T00:00:00"))
FIRST: Final = "2025-01-10T00:00:00Z"
SECOND: Final = "2025-01-20T00:00:00Z"
PAST: Final = date(2025, 1, 6)
CLOSURE: Final = date(2025, 1, 28)
WEEKDAYS: Final = ("mon", "tue", "wed", "thu", "fri")


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _declaration(
    declared_at: str,
    closed: tuple[str, ...] = (),
    *,
    end: str = "2025-02-03",
    sessions: tuple[tuple[str, str, str], ...] = (),
) -> tuple[bytes, str]:
    document = {
        "schema": "aas-calendar-declaration-v1",
        "calendar_id": "XTST",
        "venue": "XTST",
        "timezone": "Asia/Seoul",
        "declared_at": declared_at,
        "from": "2025-01-06",
        "to": end,
        "sources": ["synthetic"],
        "regimes": [
            {
                "from": "2025-01-06",
                "to": end,
                "hours": {name: ["09:00", "15:30"] for name in WEEKDAYS},
            }
        ],
        "closed": list(closed),
        "sessions": [
            {"date": day, "open": opened, "close": closed_at} for day, opened, closed_at in sessions
        ],
    }
    raw = json.dumps(document, indent=1).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _refresh(
    workspace: Workspace, document: tuple[bytes, str], *, apply: bool = True
) -> dict[str, object]:
    return refresh_calendar(
        workspace, document[0], document[1], apply=apply, now_us=NOW, budget=BUDGET
    )


def _local(day: date, hour: int, minute: int = 0) -> int:
    moment = datetime(day.year, day.month, day.day, hour, minute, tzinfo=ZONE)
    return us(moment.astimezone(UTC))


def _pin(workspace: Workspace, generation_id: str) -> GenerationPin:
    marker = market.marker_for(workspace.market, generation_id)
    return GenerationPin(
        str(marker["dataset_id"]),
        str(marker["version"]),
        generation_id,
        str(marker["chain_hash"]),
        str(marker["request_hash"]),
    )


def _sessions(
    workspace: Workspace,
    pin: GenerationPin,
    query: HeadQuery | None = None,
    grants: tuple[str, ...] = (),
) -> dict[date, dict[str, object]]:
    read = load_pinned_heads(
        workspace,
        HeadBinding("calendar_sessions", (HeadPin(pin),), granted_rules=grants),
        query or HeadQuery(),
        budget=BUDGET,
    )
    return {cast("date", row.values["session_date"]): dict(row.values) for row in read.rows}


def _generation(result: dict[str, object]) -> str:
    promotion = cast("dict[str, object]", result["promotion"])
    return str(promotion["generation_id"])


def test_temporary_closure_is_a_new_generation(ws: Workspace) -> None:
    first = _refresh(ws, _declaration(FIRST))
    promotion = cast("dict[str, object]", first["promotion"])
    assert promotion["published"] is True
    assert promotion["operations"] == {"ASSERT": 28}
    first_pin = _pin(ws, _generation(first))
    before = _sessions(ws, first_pin)
    assert before[CLOSURE]["status"] == "open"
    # A later declaration closes one future session: one SUPERSEDE in a child generation.
    second = _refresh(ws, _declaration(SECOND, ("2025-01-28",)))
    changed = cast("dict[str, object]", second["promotion"])
    assert changed["operations"] == {"SUPERSEDE": 1}
    assert changed["unchanged"] == 27
    assert changed["parent"] == first_pin.generation_id
    second_pin = _pin(ws, _generation(second))
    after = _sessions(ws, second_pin)
    closed = after[CLOSURE]
    assert (closed["status"], closed["open_at_us"], closed["close_at_us"]) == ("closed", None, None)
    # A correction is known from when AAS received the declaration that carries it.
    received = cast("int", closed["ingested_at_us"])
    assert received >= us(at("2025-01-20T00:00:00"))
    assert closed["available_at_us"] == closed["revision_known_at_us"] == received
    assert {day: row for day, row in after.items() if day != CLOSURE} == {
        day: row for day, row in before.items() if day != CLOSURE
    }
    # The earlier pin still verifies and still reads the session as declared then.
    verify_generation_bulk(ws.market, first_pin.generation_id, budget=BUDGET, deep=True)
    assert _sessions(ws, first_pin) == before
    assert verify_workspace(ws)["verified"] is True
    # Refreshing the same declaration again changes nothing.
    again = cast(
        "dict[str, object]", _refresh(ws, _declaration(SECOND, ("2025-01-28",)))["promotion"]
    )
    assert (again["published"], again.get("empty_delta")) == (False, True)


def test_older_declaration_cannot_undo_a_newer_one(ws: Workspace) -> None:
    _refresh(ws, _declaration(FIRST))
    _refresh(ws, _declaration(SECOND, ("2025-01-28",)))
    with pytest.raises(ValueError, match="older than the one"):
        _refresh(ws, _declaration(FIRST))
    with pytest.raises(ValueError, match="other content"):
        _refresh(ws, _declaration(SECOND))
    with pytest.raises(ValueError, match="later than now"):
        _refresh(ws, _declaration("2025-07-01T00:00:00Z"))
    # A newer declaration still states every date the head holds.
    with pytest.raises(ValueError, match="does not cover every date"):
        _refresh(ws, _declaration(THIRD, end="2025-01-27"))


THIRD: Final = "2025-01-25T00:00:00Z"
GRANT: Final = ("declared_session_end@1",)


def test_past_corrections_are_known_from_their_declaration(ws: Workspace) -> None:
    first_pin = _pin(ws, _generation(_refresh(ws, _declaration(FIRST))))
    before = _sessions(ws, first_pin)
    early = date(2025, 1, 7)
    # A later declaration closes a past session and gives another an earlier close.
    corrected = _declaration(SECOND, ("2025-01-06",), sessions=(("2025-01-07", "09:00", "12:00"),))
    planned = cast("dict[str, object]", _refresh(ws, corrected, apply=False)["changes"])
    assert planned["changed"] == 2
    second = _refresh(ws, corrected)
    promotion = cast("dict[str, object]", second["promotion"])
    assert (promotion["published"], promotion["operations"]) == (True, {"SUPERSEDE": 2})
    assert promotion["stale"] == 0
    second_pin = _pin(ws, _generation(second))
    after = _sessions(ws, second_pin)
    assert after[PAST]["status"] == "closed"
    assert after[early]["close_at_us"] == _local(early, 12)
    for day in (PAST, early):
        received = cast("int", after[day]["ingested_at_us"])
        assert received >= us(at("2025-01-20T00:00:00"))
        assert after[day]["available_at_us"] == after[day]["revision_known_at_us"] == received
        assert received > cast("int", before[day]["revision_known_at_us"])
    # A strict read between the two declarations still sees the first one's schedule.
    between = HeadQuery(cutoff_us=us(at("2025-01-15T00:00:00")))
    strict = _sessions(ws, second_pin, between, GRANT)
    assert (strict[PAST]["status"], strict[early]["close_at_us"]) == ("open", _local(early, 15, 30))
    assert strict[PAST]["revision_known_at_us"] == _local(PAST, 15, 30)
    # Without the grant a strict read relies on none of the declared bounds.
    assert _sessions(ws, second_pin, between) == {}
    # Reopening the past date in a third declaration lands as well.
    third = cast("dict[str, object]", _refresh(ws, _declaration(THIRD))["promotion"])
    assert (third["published"], third["operations"]) == (True, {"SUPERSEDE": 2})
    reopened = _sessions(ws, _pin(ws, str(third["generation_id"])))
    assert (reopened[PAST]["status"], reopened[PAST]["close_at_us"]) == (
        "open",
        _local(PAST, 15, 30),
    )
    assert cast("int", reopened[PAST]["revision_known_at_us"]) >= cast(
        "int", after[PAST]["revision_known_at_us"]
    )


def test_refresh_refuses_a_stale_plan(ws: Workspace, monkeypatch: pytest.MonkeyPatch) -> None:
    from aegis_alpha.storage import calendar_refresh  # noqa: PLC0415 -- patched module

    def stale(*_args: object, apply: bool, **_kwargs: object) -> dict[str, object]:
        assert apply is False
        return {"mode": "plan", "stale": 2}

    monkeypatch.setattr(calendar_refresh, "promote", stale)
    with pytest.raises(ValueError, match="2 changed dates"):
        _refresh(ws, _declaration(FIRST))


def test_session_times_are_bounded_by_the_declaration(ws: Workspace) -> None:
    declared = "2025-01-15T00:00:00Z"
    sessions = _sessions(ws, _pin(ws, _generation(_refresh(ws, _declaration(declared)))))
    past, weekend, future = sessions[PAST], sessions[date(2025, 1, 11)], sessions[CLOSURE]
    assert (past["open_at_us"], past["close_at_us"]) == (_local(PAST, 9), _local(PAST, 15, 30))
    # Before the declaration, a session is public by its close and a closed date by its end.
    assert past["available_at_us"] == past["revision_known_at_us"] == _local(PAST, 15, 30)
    end = us(datetime(2025, 1, 12, tzinfo=ZONE).astimezone(UTC)) - 1
    assert (weekend["status"], weekend["available_at_us"]) == ("closed", end)
    # After it, a declared session is public from the declaration only.
    assert future["available_at_us"] == us(at("2025-01-15T00:00:00"))
    assert str(future["timezone_version"]).startswith("duckdb-")


def test_extending_the_year_asserts_only_new_sessions(ws: Workspace) -> None:
    first = _generation(_refresh(ws, _declaration(FIRST)))
    extended = _refresh(ws, _declaration(SECOND, end="2025-02-10"))
    promotion = cast("dict[str, object]", extended["promotion"])
    assert promotion["operations"] == {"ASSERT": 7}
    assert promotion["unchanged"] == 28
    # The later declaration would date 2025-01-10..02-02 later; those rows keep the times
    # they were first promoted with and are only reported.
    assert promotion["time_drift"] == 24
    assert ws.market.execute(
        "SELECT count(*) FROM calendar_sessions WHERE generation_id=?", [first]
    ).fetchone() == (28,)


def test_refresh_plan_writes_nothing(ws: Workspace) -> None:
    _refresh(ws, _declaration(FIRST))
    before = _tree(ws.paths.raw) | {"generations": str(_generations(ws))}
    planned = _refresh(ws, _declaration(SECOND, ("2025-01-28",)), apply=False)
    assert planned["source_committed"] is False
    assert planned["promotion"] is None
    changes = cast("dict[str, object]", planned["changes"])
    assert (changes["added"], changes["changed"], changes["unchanged"]) == (0, 1, 27)
    assert changes["changed_sample"] == [
        {
            "date": "2025-01-28",
            "before": {"status": "open", "open": "09:00", "close": "15:30"},
            "after": {"status": "closed", "open": None, "close": None},
        }
    ]
    assert _tree(ws.paths.raw) | {"generations": str(_generations(ws))} == before
    # Once its source is committed, the plan is the promotion engine's own plan.
    committed = _refresh(ws, _declaration(FIRST), apply=False)
    assert cast("dict[str, object]", committed["promotion"])["mode"] == "plan"


def _tree(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _generations(workspace: Workspace) -> list[tuple[object, ...]]:
    return workspace.market.execute(
        "SELECT generation_id, row_count FROM market_generations ORDER BY 1"
    ).fetchall()


def test_prices_take_session_close_from_the_refreshed_calendar(ws: Workspace) -> None:
    calendar = _pin(ws, _generation(_refresh(ws, _declaration(FIRST))))
    rule = {
        "rule": "session_close_plus_lag@1",
        "basis": "record",
        "input": "session_date",
        "args": {
            "calendar": {
                "dataset_id": calendar.dataset_id,
                "version": calendar.version,
                "generation_id": calendar.generation_id,
                "chain_hash": calendar.chain_hash,
                "manifest_hash": calendar.manifest_hash,
            },
            "calendar_id": "XTST",
            "venue": "XTST",
            "lag_us": 3_600_000_000,
        },
    }
    source = add_source(
        ws, [bar("AAA.KO", PAST, 100.0, retrieved=at("2025-01-20T00:00:00"))], tag="p"
    )
    identity = register_symbols(ws, source["source_id"])
    document = spec(
        [source], identity, rules={"available_at_us": rule, "revision_known_at_us": rule}
    )
    result = promote(ws, document[0], document[1], apply=True, budget=BUDGET)
    (row,) = ws.market.execute(
        "SELECT available_at_us FROM prices WHERE generation_id=?", [result["generation_id"]]
    ).fetchall()
    assert row[0] == _local(PAST, 15, 30) + 3_600_000_000


def test_cli_refreshes_the_packaged_calendars(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)

    def calendar(*args: str) -> dict[str, object]:
        completed = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
            [sys.executable, "-m", "aegis_alpha", "calendar", "refresh", *args],
            env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
            cwd=home.parent,
            text=True,
            capture_output=True,
            check=False,
            timeout=300,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout)

    planned = cast("list[dict[str, object]]", calendar("--plan")["calendars"])
    assert [item["dataset_id"] for item in planned] == ["sessions.xkrx", "sessions.xnys"]
    assert all(item["source_committed"] is False for item in planned)
    applied = cast("list[dict[str, object]]", calendar()["calendars"])
    for item in applied:
        promotion = cast("dict[str, object]", item["promotion"])
        assert promotion["published"] is True
        assert promotion["operations"] == {"ASSERT": (date(2028, 1, 1) - date(1990, 1, 1)).days}
        coverage = cast("dict[str, object]", item["coverage"])
        assert coverage["through"] == "2027-12-31"
    with open_workspace(home) as workspace:
        opened = dict(
            workspace.market.execute(
                "SELECT calendar_id, count(*) FROM calendar_sessions WHERE status='open' "
                "AND session_date >= DATE '2027-01-01' GROUP BY 1"
            ).fetchall()
        )
        versions = workspace.market.execute(
            "SELECT DISTINCT timezone_version FROM calendar_sessions"
        ).fetchall()
    assert opened == {"XKRX": 247, "XNYS": 251}
    # Rows, and so generation IDs, follow the locked DuckDB's zone data label.
    assert versions == [(timezone_version(),)]
    rerun = cast("list[dict[str, object]]", calendar("--calendar", "XNYS")["calendars"])
    assert cast("dict[str, object]", rerun[0]["promotion"])["empty_delta"] is True
