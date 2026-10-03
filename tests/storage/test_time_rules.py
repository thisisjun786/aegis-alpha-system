# ruff: noqa: S608 -- test-owned SQL
"""Versioned time rules: conservative bounds, never ingestion filling an unknown."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import date
from pathlib import Path
from typing import cast

import duckdb
import pytest

from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.promotion import time_rules
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.time_rules import (
    LOCAL_DAY_END,
    SESSION_CLOSE,
    UNKNOWN_NULL,
    bound,
    local_day_end,
    parse_rule,
    revision_time,
    rule_sql,
    session_close_plus_lag,
)
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import (
    SYMBOLS,
    add_source,
    at,
    bar,
    prices,
    publish_calendar,
    register_symbols,
    spec,
    us,
)

AAA = mint_instrument("norgate_assetid", SYMBOLS["AAA.KO"])
D1, D2 = date(2025, 1, 2), date(2025, 1, 3)
END_D1 = us(at("2025-01-02T14:59:59.999999"))
LATE = at("2025-01-10T00:00:00")


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _rules(rule: Mapping[str, object]) -> dict[str, object]:
    return {"available_at_us": rule, "revision_known_at_us": rule}


def _flags(workspace: Workspace, generation: object) -> set[tuple[str, str, str]]:
    return {
        (str(row[0]), str(row[1]), str(row[2]))
        for row in workspace.market.execute(
            "SELECT rule_id, flag, detail FROM quality_flags WHERE generation_id=?", [generation]
        ).fetchall()
    }


def test_rules_never_fill_null_from_ingestion(ws: Workspace) -> None:
    # The reference: an unknown rule value stays unknown whatever the ingestion time.
    assert bound(None, None, 10**15) == time_rules.Bounded(None, clamped=False, held=False)
    for op in ("ASSERT", "SUPERSEDE"):
        assert (
            revision_time(
                op, basis="record", rule=UNKNOWN_NULL, bounded=None, ingested=1, evidence=1
            )
            is None
        )
    assert (
        revision_time(
            "TOMBSTONE", basis="record", rule=UNKNOWN_NULL, bounded=None, ingested=1, evidence=1
        )
        is None
    )
    pin = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)], tag="a")
    identity = register_symbols(ws, pin["source_id"])
    unknown = {"rule": "unknown_null@1", "basis": "record", "input": None, "args": {}}
    document = spec([pin], identity, rules=_rules(unknown))
    applied = promote(ws, document[0], document[1], apply=True)
    (row,) = prices(ws, str(applied["generation_id"]))
    assert row["ingested_at_us"] == us(LATE)
    assert row["available_at_us"] is None
    assert row["revision_known_at_us"] is None


def test_session_close_plus_lag(ws: Workspace) -> None:
    close = us(at("2025-01-02T06:30:00"))
    sessions = {D1: (us(at("2025-01-02T00:00:00")), close), D2: (None, None)}
    assert session_close_plus_lag({D1: close}, D1, 60_000_000) == (close + 60_000_000, close)
    assert session_close_plus_lag({D1: close}, D2, 60_000_000) == (None, None)
    calendar = publish_calendar(ws, sessions)
    pin = add_source(
        ws,
        [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("AAA.KO", D2, 101.0, retrieved=LATE)],
        tag="a",
    )
    identity = register_symbols(ws, pin["source_id"])
    rule = {
        "rule": "session_close_plus_lag@1",
        "basis": "record",
        "input": "session_date",
        "args": {
            "calendar": calendar,
            "calendar_id": "XKRX",
            "venue": "XKRX",
            "lag_us": 60_000_000,
        },
    }
    document = spec([pin], identity, rules=_rules(rule))
    applied = promote(ws, document[0], document[1], apply=True)
    rows = {row["session_date"]: row for row in prices(ws, str(applied["generation_id"]))}
    assert rows[D1]["available_at_us"] == rows[D1]["revision_known_at_us"] == close + 60_000_000
    # A closed session has no close, so the rule has no value: NULL, never the ingestion.
    assert rows[D2]["available_at_us"] is None
    assert rows[D2]["revision_known_at_us"] is None


def test_local_day_end_flags_day_precision(ws: Workspace) -> None:
    assert local_day_end(D1, "Asia/Seoul") == (END_D1, us(at("2025-01-01T15:00:00")))
    # The SQL form agrees with Python's zone data, across a 23-hour DST day too.
    rule = parse_rule(
        "available_at_us",
        {
            "rule": "local_day_end@1",
            "basis": "record",
            "input": "d",
            "args": {"timezone": "America/New_York"},
        },
        {"d": "date"},
    )
    computed = rule_sql(rule, "d", None)
    connection = duckdb.connect()
    for day in (date(2025, 3, 9), date(2025, 11, 2), date(2025, 6, 1)):
        row = connection.execute(
            f"SELECT {computed.value}, {computed.base} FROM (SELECT ?::DATE AS d)", [day]
        ).fetchone()
        assert row == local_day_end(day, "America/New_York")
    pin = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)], tag="a")
    identity = register_symbols(ws, pin["source_id"])
    document = spec([pin], identity)
    applied = promote(ws, document[0], document[1], apply=True)
    (row,) = prices(ws, str(applied["generation_id"]))
    assert row["available_at_us"] == END_D1
    assert (
        "local_day_end",
        "time_precision_day",
        "available_at_us,revision_known_at_us",
    ) in _flags(ws, applied["generation_id"])
    assert LOCAL_DAY_END.flag == "time_precision_day"
    assert SESSION_CLOSE.flag is None


def test_rule_after_ingestion_is_clamped_above_physical_base(ws: Workspace) -> None:
    evening = at("2025-01-02T10:00:00")  # 19:00 in Seoul on D1: after midnight, before day end
    early = at("2025-01-01T10:00:00")  # 19:00 in Seoul the day before D1: before the base
    assert bound(END_D1, us(at("2025-01-01T15:00:00")), us(evening)) == time_rules.Bounded(
        us(evening), clamped=True, held=False
    )
    assert bound(END_D1, us(at("2025-01-01T15:00:00")), us(early)).held is True
    pin = add_source(
        ws,
        [bar("AAA.KO", D1, 100.0, retrieved=evening), bar("BBB.KQ", D1, 50.0, retrieved=early)],
        tag="a",
    )
    identity = register_symbols(ws, pin["source_id"])
    document = spec([pin], identity)
    applied = promote(ws, document[0], document[1], apply=True)
    assert applied["rows"] == {"held": 1, "ok": 1}
    (row,) = prices(ws, str(applied["generation_id"]))
    assert row["instrument_id"] == AAA
    assert row["available_at_us"] == row["revision_known_at_us"] == us(evening)
    assert (
        "local_day_end",
        "time_clamped_to_ingestion",
        "available_at_us,revision_known_at_us",
    ) in _flags(ws, applied["generation_id"])
    timing = cast("dict[str, dict[str, int]]", applied["time_rules"])
    assert timing["available_at_us"]["clamped"] == 1


def test_exdate_open_uses_pinned_session_open(ws: Workspace) -> None:
    opened = us(at("2025-01-02T00:00:00"))
    calendar = publish_calendar(ws, {D1: (opened, us(at("2025-01-02T06:30:00"))), D2: (None, None)})
    pin = add_source(
        ws,
        [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("AAA.KO", D2, 101.0, retrieved=LATE)],
        tag="a",
    )
    identity = register_symbols(ws, pin["source_id"])
    rule = {
        "rule": "exdate_open@1",
        "basis": "record",
        "input": "session_date",
        "args": {"calendar": calendar, "calendar_id": "XKRX", "venue": "XKRX"},
    }
    document = spec([pin], identity, rules=_rules(rule))
    applied = promote(ws, document[0], document[1], apply=True)
    rows = {row["session_date"]: row for row in prices(ws, str(applied["generation_id"]))}
    assert rows[D1]["available_at_us"] == rows[D1]["revision_known_at_us"] == opened
    # No session open, no value: NULL, never the ingestion.
    assert rows[D2]["available_at_us"] is None
    assert rows[D2]["revision_known_at_us"] is None


def test_source_column_is_its_own_value_and_base() -> None:
    rule = parse_rule(
        "revision_known_at_us",
        {"rule": "source_column@1", "basis": "revision", "input": "t", "args": {}},
        {"t": "utc_us"},
    )
    computed = rule_sql(rule, "t", None)
    published = us(at("2025-01-02T09:00:00"))
    connection = duckdb.connect()
    assert connection.execute(
        f"SELECT {computed.value}, {computed.base} FROM (SELECT ?::BIGINT AS t)", [published]
    ).fetchone() == (published, published)
    # Ingested after the source's own time: kept. Before it: held, never clamped.
    assert bound(published, published, published + 1) == time_rules.Bounded(
        published, clamped=False, held=False
    )
    assert bound(published, published, published - 1).held is True
