"""KR prices: history bars, partial bulk days, flags, row-count checks and the backfill.

Every value is synthetic except the one public split the rounding test replays (Samsung
Electronics' 50:1 split of May 2018), whose provider-reconstructed close is the float
residue the plan observed. Expectations are written out independently of the engine.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.kr_prices import kr_prices
from aegis_alpha.storage.promotion.engine import promote, verify_promotion
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import (
    SYMBOLS,
    ZONE,
    add_bulk_source,
    add_held_source,
    add_source,
    at,
    bar,
    bulk_row,
    held_row,
    prices,
    publish_calendar,
    register_symbols,
    spec,
    us,
)

AAA = mint_instrument("norgate_assetid", SYMBOLS["AAA.KO"])
BBB = mint_instrument("norgate_assetid", SYMBOLS["BBB.KQ"])
CCC = mint_instrument("norgate_assetid", SYMBOLS["CCC.KO"])
D0, D1, D2, D3 = date(2024, 12, 30), date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)
LATE = at("2025-01-10T00:00:00")
LATER = at("2025-01-20T00:00:00")
BULK = {
    "name": "eodhd.bulk_quarantine@1",
    "args": {"timezone": ZONE, "currencies": {"KO": "KRW", "KQ": "KRW"}},
}
PARTIAL = ("eodhd.bulk_quarantine", "1", "provider_reported_partial", None)


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _apply(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=True)


def _flags(workspace: Workspace, generation: object) -> list[tuple[object, ...]]:
    return workspace.market.execute(
        "SELECT p.instrument_id, p.session_date, f.rule_id, f.rule_version, f.flag, f.detail "
        "FROM quality_flags f JOIN prices p USING (generation_id, record_id, revision_id) "
        "WHERE f.generation_id = ? ORDER BY 1, 2, 5",
        [generation],
    ).fetchall()


def test_warned_bulk_rows_are_promoted_with_flags_and_counted(ws: Workspace) -> None:
    history = add_source(
        ws,
        [
            bar("AAA.KO", D1, 100.0, retrieved=LATE),
            bar("BBB.KQ", D1, 50.0, retrieved=LATE),
            bar("CCC.KO", D1, 7.0, retrieved=LATE),
        ],
        tag="history",
    )
    identity = register_symbols(ws, history["source_id"])
    first = _apply(ws, spec([history], identity))
    # The partial download repeats AAA's bar, corrects BBB's and lacks CCC on both days.
    bulk = add_bulk_source(
        ws,
        [
            bulk_row("AAA", "KO", D1, 100),
            bulk_row("BBB", "KQ", D1, 51),
            bulk_row("AAA", "KO", D2, 101),
            bulk_row("BBB", "KQ", D2, 52),
        ],
        tag="bulk",
        linked=LATER,
    )
    applied = _apply(ws, spec([bulk], identity, parent=str(first["generation_id"]), mapper=BULK))
    generation = applied["generation_id"]
    assert applied["rows"] == {"ok": 4}
    assert applied["operations"] == {"ASSERT": 2, "SUPERSEDE": 1}
    assert applied["unchanged"] == 1
    # The warned rows are promoted; every revision they produce carries the row flag.
    flagged = [row for row in _flags(ws, generation) if row[4] == PARTIAL[2]]
    assert flagged == [
        (AAA, D2, *PARTIAL),
        (BBB, D1, *PARTIAL),
        (BBB, D2, *PARTIAL),
    ]
    rows = {(row["instrument_id"], row["session_date"]): row for row in prices(ws, str(generation))}
    assert rows[(BBB, D1)]["close"] == Decimal(51)
    # The rows carry no collection instant: ingestion is the source's link time.
    assert {row["ingested_at_us"] for row in rows.values()} == {us(LATER)}
    expected = {
        "rule": "partition_row_count@1",
        "result": "below_reference",
        "dates": [
            {"session_date": "2025-01-02", "rows": 2, "resolved": 2,
             "reference_date": "2025-01-02", "reference_rows": 3},
            {"session_date": "2025-01-03", "rows": 2, "resolved": 2,
             "reference_date": "2025-01-02", "reference_rows": 3},
        ],
    }  # fmt: skip
    assert applied["partition_row_count"] == expected
    check = [
        tuple(row)
        for row in ws.state.execute(
            "SELECT dataset_id, version, rule_id, rule_version, result, reason "
            "FROM quality_checks WHERE rule_id = 'partition_row_count'"
        ).fetchall()
    ]
    assert check == [
        (
            "prices.kr.eodhd",
            "2",
            "partition_row_count",
            "1",
            "below_reference",
            json.dumps(expected["dates"], sort_keys=True, separators=(",", ":")),
        )
    ]
    verify_promotion(ws, str(generation))


def test_kr_bars_refuse_partial_negative_and_unresolved(ws: Workspace) -> None:
    partial = list(bar("AAA.KO", D1, 100.0, retrieved=LATE))
    partial[2] = None  # no open
    pin = add_source(
        ws,
        [
            tuple(partial),
            bar("BBB.KQ", D1, -5.0, retrieved=LATE),
            bar("CCC.KO", D1, 1000.4, retrieved=LATE),
            bar("ZZZ.KO", D1, 9.0, retrieved=LATE),
        ],
        tag="a",
    )
    identity = register_symbols(ws, pin["source_id"])
    applied = _apply(ws, spec([pin], identity))
    # Values that are partial or negative are never stored: the bar is invalid.
    # A symbol the registry does not resolve is reported, not promoted.
    assert applied["rows"] == {"ok": 3, "unresolved": 1}
    assert applied["unresolved_tokens"] == ["ZZZ.KO"]
    stored = {row["instrument_id"]: row for row in prices(ws, str(applied["generation_id"]))}
    assert {key: (row["value_state"], row["close"]) for key, row in stored.items()} == {
        AAA: ("invalid", None),
        BBB: ("invalid", None),
        CCC: ("present", Decimal(1000)),
    }
    assert ("krw_tick", "1", "provider_float_reconstructed", "close,high,low,open") in [
        row[2:] for row in _flags(ws, applied["generation_id"]) if row[0] == CCC
    ]


def test_known_split_rounds_back_to_whole_won(ws: Workspace) -> None:
    # 2018-04-27 is the last close before the 50:1 split; the provider reconstructs it
    # as 2,649,999.947. The first close after the split (2018-05-04) is 51,900.
    before, after = date(2018, 4, 27), date(2018, 5, 4)
    retrieved = at("2026-09-06T00:00:00")
    pin = add_source(
        ws,
        [
            bar("005930.KO", before, 2649999.947, retrieved=retrieved),
            bar("005930.KO", after, 51900.0, retrieved=retrieved),
        ],
        tag="split",
    )
    identity = register_symbols(ws, pin["source_id"], {"005930.KO": "300001"})
    applied = _apply(ws, spec([pin], identity))
    rows = prices(ws, str(applied["generation_id"]))
    closes = {row["session_date"]: row["close"] for row in rows}
    assert closes == {before: Decimal(2650000), after: Decimal(51900)}
    # The rounded pre-split close is exactly the split ratio times the post-split price
    # basis the exchange quoted (53,000), so the rule restores whole won without bias.
    assert closes[before] == Decimal(53000) * 50
    reconstructed = [
        row[1]
        for row in _flags(ws, applied["generation_id"])
        if row[4] == "provider_float_reconstructed"
    ]
    assert reconstructed == [before]


def test_partitioned_plan_refuses_rows_without_partition_date(ws: Workspace) -> None:
    history = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)], tag="history")
    identity = register_symbols(ws, history["source_id"])
    bulk = add_bulk_source(
        ws,
        [bulk_row("AAA", "KO", D2, 101), bulk_row("BBB", "KQ", D2, 5, reason="other")],
        tag="bulk",
        linked=LATER,
    )
    partitioned = spec(
        [bulk], identity, mapper=BULK, partition={"from": "2025-01-03", "to": "2025-01-04"}
    )
    planned = promote(ws, partitioned[0], partitioned[1], apply=False)
    assert planned["refusals"] == [
        "1 source rows have no partition date, so no partition holds them"
    ]
    whole = spec([bulk], identity, mapper=BULK)
    planned = promote(ws, whole[0], whole[1], apply=False)
    # Unpartitioned, the same row has no session date and is refused as incomplete.
    assert planned["rows"] == {"ok": 1, "refused_required": 1}
    with pytest.raises(ValueError, match="required refused"):
        _apply(ws, whole)


def _calendar(workspace: Workspace) -> None:
    days = (D0, D1, D2, D3)
    publish_calendar(
        workspace,
        {
            day: (us(at(f"{day.isoformat()}T00:00:00")), us(at(f"{day.isoformat()}T06:30:00")))
            for day in days
        },
    )


def _backfill(workspace: Workspace, *, apply: bool, reference: bool = False) -> dict[str, object]:
    return kr_prices(
        workspace,
        identity_snapshot="kr",
        lag_us=3_600_000_000,
        history_lineage="synthetic-kr-bars",
        bulk_lineage="synthetic-kr-bulk",
        reference=reference,
        apply=apply,
    )


def test_kr_prices_backfills_years_then_partial_days(ws: Workspace) -> None:
    history = add_source(
        ws,
        [
            bar("AAA.KO", D0, 99.0, retrieved=LATE),
            bar("AAA.KO", D1, 100.0, retrieved=LATE),
            bar("BBB.KQ", D1, 50.0, retrieved=LATE),
        ],
        tag="history",
    )
    register_symbols(ws, history["source_id"])
    _calendar(ws)
    day2 = [bulk_row("AAA", "KO", D2, 101), bulk_row("BBB", "KQ", D2, 51)]
    add_bulk_source(ws, day2, tag="d2", linked=LATER)
    add_bulk_source(ws, day2, tag="d2-again", linked=LATER)  # same table, repeated download
    # The provider divides volumes by split factors too; no exact 12-decimal value holds it.
    held = bulk_row("AAA", "KO", D3, 102, volume=545540.77978275)
    add_bulk_source(ws, [held], tag="d3", linked=LATER)
    add_bulk_source(ws, [bulk_row("SPY", "US", D3, 500)], tag="us", linked=LATER)
    planned = _backfill(ws, apply=False)
    steps = cast("list[dict[str, object]]", planned["steps"])
    assert [(step["kind"], step["from"], step["to"], step["sources"]) for step in steps] == [
        ("history", "2024-01-01", "2025-01-01", 1),
        ("history", "2025-01-01", "2026-01-01", 1),
        ("bulk", "2025-01-03", "2025-01-04", 1),
        ("bulk", "2025-01-06", "2025-01-07", 1),
    ]
    assert (planned["bulk_tables"], planned["bulk_repeated_tables"]) == (2, 1)
    assert planned["totals"] == {
        "source_rows": 6,
        "rows": {"ok": 6},
        "operations": {"ASSERT": 6},
        "flags": cast("dict[str, object]", planned["totals"])["flags"],
        "mapped_flags": cast("dict[str, object]", planned["totals"])["mapped_flags"],
    }
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (1,)
    applied = _backfill(ws, apply=True)
    published = [step["published"] for step in cast("list[dict[str, object]]", applied["steps"])]
    assert published == [True] * 4
    head = str(applied["head"])
    chain = ws.market.execute(
        "SELECT count(*) FROM market_generations WHERE dataset_id = 'prices.kr.eodhd'"
    ).fetchone()
    assert chain == (4,)
    # Each step is the child of the previous one; the bulk days carry the partial flag.
    times = ws.market.execute(
        "SELECT session_date, available_at_us FROM prices p JOIN market_generations g "
        "USING (generation_id) WHERE g.dataset_id = 'prices.kr.eodhd' "
        "AND instrument_id = ? ORDER BY 1",
        [AAA],
    ).fetchall()
    # session_close_plus_lag@1: the pinned XKRX close plus one hour.
    assert times == [
        (D0, us(at("2024-12-30T07:30:00"))),
        (D1, us(at("2025-01-02T07:30:00"))),
        (D2, us(at("2025-01-03T07:30:00"))),
        (D3, us(at("2025-01-06T07:30:00"))),
    ]
    partial = ws.market.execute(
        "SELECT count(*) FROM quality_flags WHERE flag = 'provider_reported_partial'"
    ).fetchone()
    assert partial == (3,)
    volume = ws.market.execute(
        "SELECT p.volume, f.detail FROM quality_flags f JOIN prices p "
        "USING (generation_id, record_id, revision_id) WHERE f.flag = 'provider_float_storage'"
    ).fetchall()
    assert volume == [(Decimal("545540.77978275"), "volume")]
    checks = [
        tuple(row)
        for row in ws.state.execute(
            "SELECT version, result FROM quality_checks WHERE rule_id = 'partition_row_count' "
            "ORDER BY version"
        ).fetchall()
    ]
    # D2 resolves both instruments the history had on D1; D3 resolves one of them.
    assert checks == [("3", "at_least_reference"), ("4", "below_reference")]
    verify_promotion(ws, head)
    again = _backfill(ws, apply=True)
    assert [step["published"] for step in cast("list[dict[str, object]]", again["steps"])] == [
        False
    ] * 4
    assert again["head"] == head
    reference = _backfill(ws, apply=True, reference=True)
    assert reference["dataset_id"] == "prices.kr.eodhd.ref"
    stored = ws.market.execute(
        "SELECT DISTINCT fields, basis, price_role FROM prices p JOIN market_generations g "
        "USING (generation_id) WHERE g.dataset_id = 'prices.kr.eodhd.ref'"
    ).fetchall()
    assert stored == [("close", "total_return", "reference")]


def test_held_history_rows_become_invalid_bars(ws: Workspace) -> None:
    history = add_source(
        ws,
        [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("BBB.KQ", D1, 50.0, retrieved=LATE)],
        tag="history",
    )
    register_symbols(ws, history["source_id"])
    _calendar(ws)
    done = "2025-01-09T09:00:00+09:00"
    jobs = [
        {"fingerprint": "fa", "symbol": "AAA.KO", "completed_at_utc": done},
        {"fingerprint": "fb", "symbol": "BBB.KQ", "completed_at_utc": done},
        {"fingerprint": "fz", "symbol": "ZZZ.KO", "completed_at_utc": done},
    ]
    rows = [held_row("fa", D0), held_row("fb", D2, reason="inconsistent_ohlc"), held_row("fz", D0)]
    lineage = "synthetic-kr-bars"
    add_held_source(ws, rows, jobs, lineage=lineage, tag="held", linked=LATER)
    add_held_source(ws, rows, jobs, lineage=lineage, tag="held-again", linked=LATER)
    add_held_source(ws, [], [], lineage=lineage, tag="empty", linked=LATER)
    planned = _backfill(ws, apply=False)
    steps = cast("list[dict[str, object]]", planned["steps"])
    # The held rows span 2024..2025 and form one step after the history years; the
    # repeated download is pinned once and the empty one not at all.
    assert [(step["kind"], step["from"], step["to"], step["sources"]) for step in steps] == [
        ("history", "2025-01-01", "2026-01-01", 1),
        ("held", "2024-01-01", "2026-01-01", 1),
    ]
    assert (planned["held_tables"], planned["held_repeated_tables"]) == (1, 1)
    assert steps[1]["rows"] == {"ok": 2, "unresolved": 1}
    assert steps[1]["unresolved_tokens"] == ["ZZZ.KO"]
    applied = _backfill(ws, apply=True)
    head = str(applied["head"])
    stored = ws.market.execute(
        "SELECT instrument_id, session_date, value_state, open, high, low, close, volume, "
        "ingested_at_us, available_at_us FROM prices WHERE generation_id = ? ORDER BY 2",
        [head],
    ).fetchall()
    # Held rows are invalid bars that keep none of the provider's values; the job's
    # completion instant is their ingestion and the XKRX close plus the lag their time.
    ingested = us(at("2025-01-09T00:00:00"))
    assert stored == [
        (AAA, D0, "invalid", None, None, None, None, None, ingested,
         us(at("2024-12-30T07:30:00"))),
        (BBB, D2, "invalid", None, None, None, None, None, ingested,
         us(at("2025-01-03T07:30:00"))),
    ]  # fmt: skip
    verify_promotion(ws, head)
    again = _backfill(ws, apply=True)
    assert [step["published"] for step in cast("list[dict[str, object]]", again["steps"])] == [
        False,
        False,
    ]
    reference = _backfill(ws, apply=False, reference=True)
    assert [step["kind"] for step in cast("list[dict[str, object]]", reference["steps"])] == [
        "history"
    ]


def test_held_rows_without_manifest_jobs_are_refused(ws: Workspace) -> None:
    history = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)], tag="history")
    identity = register_symbols(ws, history["source_id"])
    held = add_held_source(
        ws, [held_row("fa", D1)], [], lineage="synthetic-kr-bars", tag="held", linked=LATER
    )
    # A manifest whose jobs list is missing: the commit names no symbols at all.
    ws.market.execute(
        "UPDATE source_library_commits SET manifest_json = json_object('source_id', "
        "source_id, 'store', 'market', 'tables', manifest_json->'tables', 'metadata', "
        "json_object()) WHERE source_id = ?",
        [held["source_id"]],
    )
    document = spec(
        [held],
        identity,
        mapper={
            "name": "eodhd.bars_quarantine@1",
            "args": {"timezone": ZONE, "currencies": {"KO": "KRW", "KQ": "KRW"}},
        },
    )
    planned = promote(ws, document[0], document[1], apply=False)
    # Without its job the row has no symbol and so no currency: it is refused, not dropped.
    assert planned["refusals"] == [
        "1 pinned sources have no manifest metadata list jobs",
        "1 rows required refused",
    ]
    assert planned["rows"] == {"refused_required": 1}
