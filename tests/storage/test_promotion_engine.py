# ruff: noqa: PLR2004 -- synthetic counts and prices are the expected values
"""The promotion engine on synthetic sources: operations, times, flags, CAS and recovery.

Every expected value is spelled independently of the engine: decimal results and rule
times are written out, revision identities come from ``formats.revision_id``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.bulk_generation import ParentChangedError
from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.promotion import engine, formats
from aegis_alpha.storage.promotion.engine import promote, verify_promotion
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import (
    SYMBOLS,
    add_source,
    at,
    bar,
    full_snapshot,
    prices,
    publish_calendar,
    register_symbols,
    spec,
    us,
)

AAA = mint_instrument("norgate_assetid", SYMBOLS["AAA.KO"])
BBB = mint_instrument("norgate_assetid", SYMBOLS["BBB.KQ"])
CCC = mint_instrument("norgate_assetid", SYMBOLS["CCC.KO"])
D1, D2 = date(2025, 1, 2), date(2025, 1, 3)
# local_day_end@1 in Asia/Seoul (UTC+9): the last microsecond of D1 and D2 in UTC.
END_D1 = us(at("2025-01-02T14:59:59.999999"))
END_D2 = us(at("2025-01-03T14:59:59.999999"))
LATE = at("2025-01-10T00:00:00")
LATER = at("2025-01-20T00:00:00")


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _apply(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=True)


def _plan(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=False)


def _setup(
    workspace: Workspace, rows: list[tuple[object, ...]], tag: str = "a"
) -> tuple[dict[str, str], dict[str, str]]:
    pin = add_source(workspace, rows, tag=tag)
    return pin, register_symbols(workspace, pin["source_id"])


def _by_key(rows: list[dict[str, object]]) -> dict[tuple[object, object], dict[str, object]]:
    return {(row["instrument_id"], row["session_date"]): row for row in rows}


def test_same_request_reuses_generation(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    document = spec([pin], identity)
    first = _apply(ws, document)
    second = _apply(ws, document)
    planned = _plan(ws, document)
    assert first["published"] is True
    assert second["reused"] is True
    assert planned["reused"] is True
    assert second["generation_id"] == planned["generation_id"] == first["generation_id"]
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (1,)
    assert engine.list_promotions(ws)[0]["phase"] == "COMPLETED"


def test_repromotion_yields_empty_delta(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    first = _apply(ws, spec([pin], identity))
    again = _apply(ws, spec([pin], identity, parent=str(first["generation_id"])))
    assert again["delta_rows"] == 0
    assert again["published"] is False
    assert again["empty_delta"] is True
    assert again["unchanged"] == 1
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (1,)


def test_head_diff_decides_operation(ws: Workspace) -> None:
    first_pin, identity = _setup(
        ws,
        [
            bar("AAA.KO", D1, 100.0, retrieved=LATE),
            bar("AAA.KO", D2, 101.0, retrieved=LATE),
            bar("BBB.KQ", D1, 50.0, retrieved=LATE),
        ],
    )
    first = _apply(ws, spec([first_pin], identity))
    second_pin = add_source(
        ws,
        [
            bar("AAA.KO", D1, 100.0, retrieved=LATER),
            bar("AAA.KO", D2, 102.0, retrieved=LATER),
            bar("CCC.KO", D1, 7.0, retrieved=LATER),
        ],
        tag="b",
        linked=at("2025-01-21T00:00:00"),
    )
    policy = full_snapshot(second_pin, "2025-01-01", "2025-02-01")
    second = _apply(
        ws, spec([second_pin], identity, parent=str(first["generation_id"]), tombstone=policy)
    )
    assert second["operations"] == {"ASSERT": 1, "SUPERSEDE": 1, "TOMBSTONE": 1}
    assert second["unchanged"] == 1
    rows = _by_key(prices(ws, str(second["generation_id"])))
    assert rows[(AAA, D2)]["op"] == "SUPERSEDE"
    assert rows[(AAA, D2)]["close"] == Decimal(102)
    assert rows[(CCC, D1)]["op"] == "ASSERT"
    assert rows[(BBB, D1)]["op"] == "TOMBSTONE"
    old = _by_key(prices(ws, str(first["generation_id"])))
    assert rows[(AAA, D2)]["supersedes_revision_id"] == old[(AAA, D2)]["revision_id"]
    assert rows[(BBB, D1)]["supersedes_revision_id"] == old[(BBB, D1)]["revision_id"]
    # A tombstoned record that the source carries again comes back as a SUPERSEDE.
    third_pin = add_source(
        ws, [bar("BBB.KQ", D1, 50.0, retrieved=at("2025-02-01T00:00:00"))], tag="c"
    )
    third = _apply(ws, spec([third_pin], identity, parent=str(second["generation_id"])))
    assert third["operations"] == {"SUPERSEDE": 1}
    back = prices(ws, str(third["generation_id"]))[0]
    assert back["supersedes_revision_id"] == rows[(BBB, D1)]["revision_id"]
    assert verify_workspace(ws)["verified"] is True


def test_tombstone_requires_full_snapshot_policy(ws: Workspace) -> None:
    first_pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("BBB.KQ", D1, 50.0, retrieved=LATE)]
    )
    first = _apply(ws, spec([first_pin], identity))
    second_pin = add_source(ws, [bar("AAA.KO", D1, 101.0, retrieved=LATER)], tag="b")
    second = _apply(ws, spec([second_pin], identity, parent=str(first["generation_id"])))
    assert second["operations"] == {"SUPERSEDE": 1}
    assert {row["op"] for row in prices(ws, str(second["generation_id"]))} == {"SUPERSEDE"}


def test_tombstone_stays_within_declared_scope(ws: Workspace) -> None:
    first_pin, identity = _setup(
        ws,
        [
            bar("AAA.KO", D1, 100.0, retrieved=LATE),
            bar("BBB.KQ", D1, 50.0, retrieved=LATE),
            bar("BBB.KQ", D2, 51.0, retrieved=LATE),
        ],
    )
    first = _apply(ws, spec([first_pin], identity))
    second_pin = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATER)], tag="b")
    # BBB is outside the instrument scope, so its absence proves nothing.
    only_aaa = full_snapshot(second_pin, "2025-01-01", "2025-02-01", [AAA])
    planned = _plan(
        ws, spec([second_pin], identity, parent=str(first["generation_id"]), tombstone=only_aaa)
    )
    assert planned["delta_rows"] == 0
    # D2 is outside the date scope; only BBB on D1 is in scope and absent.
    first_day = full_snapshot(second_pin, "2025-01-02", "2025-01-03")
    second = _apply(
        ws, spec([second_pin], identity, parent=str(first["generation_id"]), tombstone=first_day)
    )
    assert second["operations"] == {"TOMBSTONE": 1}
    (row,) = prices(ws, str(second["generation_id"]))
    assert (row["instrument_id"], row["session_date"]) == (BBB, D1)


def test_tombstone_time_comes_from_absence_snapshot(ws: Workspace) -> None:
    first_pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("BBB.KQ", D1, 50.0, retrieved=LATE)]
    )
    first = _apply(ws, spec([first_pin], identity))
    second_pin = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATER)], tag="b")
    policy = full_snapshot(second_pin, "2025-01-01", "2025-02-01")
    second = _apply(
        ws, spec([second_pin], identity, parent=str(first["generation_id"]), tombstone=policy)
    )
    (row,) = prices(ws, str(second["generation_id"]))
    evidence = ws.state.execute(
        "SELECT retrieved_at_us FROM source_snapshots WHERE snapshot_id=?",
        ("sl:" + second_pin["source_id"],),
    ).fetchone()[0]
    assert row["op"] == "TOMBSTONE"
    # Never the record date's rule value (END_D1), always the snapshot's evidence time.
    assert (
        row["available_at_us"] == row["revision_known_at_us"] == row["ingested_at_us"] == evidence
    )
    assert row["available_at_us"] != END_D1
    assert row["source_row_hash"] == formats.tombstone_hash(
        second_pin["source_id"], second_pin["table"], second_pin["digest"]
    )


def test_revision_id_is_unique_across_value_return(ws: Workspace) -> None:
    pins = [
        add_source(ws, [bar("AAA.KO", D1, close, retrieved=moment)], tag=tag)
        for close, moment, tag in (
            (100.0, LATE, "a"),
            (101.0, LATER, "b"),
            (100.0, at("2025-02-01T00:00:00"), "c"),
        )
    ]
    identity = register_symbols(ws, pins[0]["source_id"])
    parent = None
    revisions = []
    for pin in pins:
        result = _apply(ws, spec([pin], identity, parent=parent))
        parent = str(result["generation_id"])
        (row,) = prices(ws, parent)
        revisions.append(row["revision_id"])
        assert row["revision_id"] == formats.revision_id(
            "prices.kr.eodhd",
            str(row["record_id"]),
            str(row["op"]),
            cast("str | None", row["supersedes_revision_id"]),
            str(row["source_row_hash"]),
        )
    assert len(set(revisions)) == 3
    # The first source promoted again into another dataset keeps a distinct revision.
    other = _apply(ws, spec([pins[0]], identity, dataset="prices.kr.eodhdcopy"))
    (copy,) = prices(ws, str(other["generation_id"]))
    assert copy["record_id"] == row["record_id"]
    assert copy["revision_id"] not in revisions


def test_promotion_is_independent_of_wall_clock(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    document = spec([pin], identity)
    today = _plan(ws, document)
    real = time.time_ns
    monkeypatch.setattr(time, "time_ns", lambda: real() + 400 * 86_400 * 10**9)
    later = _plan(ws, document)
    assert today["marker"] == later["marker"]
    applied = _apply(ws, document)
    (row,) = prices(ws, str(applied["generation_id"]))
    assert row["ingested_at_us"] == us(LATE)
    assert row["available_at_us"] == END_D1


def test_interrupted_promotion_resumes_publication_only(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    document = spec([pin], identity)

    def crash(*_: object, **__: object) -> None:
        raise RuntimeError("process killed")

    # Killed after the intent and before the market commit.
    with monkeypatch.context() as patched:
        patched.setattr(engine, "publish_generation_bulk", crash)
        with pytest.raises(RuntimeError):
            _apply(ws, document)
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (0,)
    (pending,) = engine.list_promotions(ws)
    assert pending["phase"] == "PREPARED"
    # A plan of the pending request recomputes its report and recovers nothing.
    planned = _plan(ws, document)
    assert (planned["pending"], planned["market_committed"]) == (True, False)
    assert planned["recomputes_intent"] is True
    assert planned["operations"] == {"ASSERT": 1}
    assert engine.list_promotions(ws)[0]["phase"] == "PREPARED"
    recovered = recover_operations(ws)
    assert recovered["recovered"] == [pending["operation_id"]]
    assert recovered["provider_calls"] == 0
    assert engine.list_promotions(ws)[0]["phase"] == "COMPLETED"
    # Killed after the market commit and before the catalog.
    second_pin = add_source(ws, [bar("AAA.KO", D1, 101.0, retrieved=LATER)], tag="b")
    following = spec([second_pin], identity, parent=str(pending["generation_id"]))
    with monkeypatch.context() as patched:
        patched.setattr(engine, "_complete", crash)
        with pytest.raises(RuntimeError):
            _apply(ws, following)
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (2,)
    assert _plan(ws, following)["market_committed"] is True
    # Repeating the same request finishes the catalog without publishing anything new.
    resumed = _apply(ws, following)
    assert resumed["reused"] is True
    assert resumed["generations"] == 2
    assert verify_workspace(ws)["pending_operations"] == 0


def test_quality_flags_are_hashed_with_generation(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 2649999.947, retrieved=LATE)])
    applied = _apply(ws, spec([pin], identity))
    generation = str(applied["generation_id"])
    (row,) = prices(ws, generation)
    # The flag records the rounding; the value is the rounded one and the source row
    # hash still names the original binary value.
    assert row["close"] == Decimal(2650000)
    flags = ws.market.execute(
        "SELECT rule_id, rule_version, flag, detail FROM quality_flags WHERE generation_id=? "
        "ORDER BY flag",
        [generation],
    ).fetchall()
    assert ("krw_tick", "1", "provider_float_reconstructed", "close,high,low,open") in flags
    verify_promotion(ws, generation)
    ws.market.execute(
        "DELETE FROM quality_flags WHERE generation_id=? AND flag='time_precision_day'",
        [generation],
    )
    with pytest.raises(ValueError, match="quality flags differ"):
        verify_promotion(ws, generation)


def test_superseding_revision_is_not_known_before_its_source(ws: Workspace) -> None:
    first_pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    first = _apply(ws, spec([first_pin], identity))
    second_pin = add_source(ws, [bar("AAA.KO", D1, 101.0, retrieved=LATER)], tag="b")
    second = _apply(ws, spec([second_pin], identity, parent=str(first["generation_id"])))
    (asserted,) = prices(ws, str(first["generation_id"]))
    (corrected,) = prices(ws, str(second["generation_id"]))
    assert asserted["available_at_us"] == END_D1
    # The record-date rule cannot date a correction; its source's ingestion does.
    assert corrected["available_at_us"] == corrected["revision_known_at_us"] == us(LATER)


def test_recollection_at_new_ingestion_time_is_not_a_revision(ws: Workspace) -> None:
    first_pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    first = _apply(ws, spec([first_pin], identity))
    again = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATER)], tag="b")
    planned = _plan(ws, spec([again], identity, parent=str(first["generation_id"])))
    assert planned["delta_rows"] == 0
    assert planned["unchanged"] == 1


def test_older_source_row_does_not_supersede_newer_head(ws: Workspace) -> None:
    newer_pin, identity = _setup(ws, [bar("AAA.KO", D1, 101.0, retrieved=LATE)])
    newer = _apply(ws, spec([newer_pin], identity))
    # Collected during D1's local evening, before the head's day-end time.
    older_pin = add_source(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=at("2025-01-02T10:00:00"))], tag="b"
    )
    planned = _plan(ws, spec([older_pin], identity, parent=str(newer["generation_id"])))
    assert planned["delta_rows"] == 0
    assert planned["stale"] == 1


def test_mapper_rejects_overlapping_natural_keys(ws: Workspace) -> None:
    pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("AAA.KO", D1, 100.0, retrieved=LATER)]
    )
    document = spec([pin], identity)
    planned = _plan(ws, document)
    assert planned["duplicate_keys"] == 1
    assert any("repeat" in reason for reason in cast("list[str]", planned["refusals"]))
    with pytest.raises(ValueError, match="natural keys repeat"):
        _apply(ws, document)
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (0,)


def test_unresolved_identity_rows_are_reported_not_promoted(ws: Workspace) -> None:
    pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("ZZZ.KO", D1, 5.0, retrieved=LATE)]
    )
    applied = _apply(ws, spec([pin], identity))
    assert applied["rows"] == {"ok": 1, "unresolved": 1}
    assert applied["unresolved_tokens"] == ["ZZZ.KO"]
    assert [row["instrument_id"] for row in prices(ws, str(applied["generation_id"]))] == [AAA]


def test_cross_provider_mismatch_uses_spec_tolerance(ws: Workspace) -> None:
    reference_pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 1000.0, retrieved=LATE), bar("BBB.KQ", D1, 1000.0, retrieved=LATE)]
    )
    reference = _apply(ws, spec([reference_pin], identity, dataset="prices.kr.other"))
    marker = cast("dict[str, object]", reference["marker"])
    pin = {
        "dataset_id": "prices.kr.other",
        "version": "1",
        "generation_id": str(marker["generation_id"]),
        "chain_hash": str(marker["chain_hash"]),
        "manifest_hash": str(marker["request_hash"]),
    }
    candidate = add_source(
        ws,
        [bar("AAA.KO", D1, 1005.0, retrieved=LATE), bar("BBB.KQ", D1, 1050.0, retrieved=LATE)],
        tag="b",
    )
    rule = {
        "rule": "cross_provider_mismatch@1",
        "args": {"reference": pin, "column": "close", "tolerance": "0.01"},
    }
    applied = _apply(ws, spec([candidate], identity, quality=[rule]))
    flagged = ws.market.execute(
        "SELECT p.instrument_id, f.detail FROM quality_flags f "
        "JOIN prices p USING (generation_id, record_id, revision_id) "
        "WHERE f.flag='cross_provider_mismatch' AND f.generation_id=?",
        [applied["generation_id"]],
    ).fetchall()
    # 0.5% is inside the 1% tolerance; 5% is not.
    assert flagged == [(BBB, "close")]


def test_competing_request_fails_parent_cas(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    other = add_source(ws, [bar("AAA.KO", D1, 99.0, retrieved=LATE)], tag="b")
    first, competing = spec([pin], identity), spec([other], identity)
    _plan(ws, competing)
    _apply(ws, first)
    with pytest.raises(ParentChangedError):
        _apply(ws, competing)
    # Planning again on the moved head is refused the same way until the parent changes.
    with pytest.raises(ParentChangedError):
        _plan(ws, competing)


def test_time_rule_change_requires_new_chain(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    first = _apply(ws, spec([pin], identity))
    unknown = {"rule": "unknown_null@1", "basis": "record", "input": None, "args": {}}
    changed = {"available_at_us": unknown, "revision_known_at_us": unknown}
    second_pin = add_source(ws, [bar("AAA.KO", D1, 101.0, retrieved=LATER)], tag="b")
    with pytest.raises(ValueError, match=r"\.r<N>"):
        _plan(ws, spec([second_pin], identity, parent=str(first["generation_id"]), rules=changed))
    fresh = _apply(ws, spec([second_pin], identity, dataset="prices.kr.eodhd.r2", rules=changed))
    (row,) = prices(ws, str(fresh["generation_id"]))
    assert row["available_at_us"] is None
    assert row["revision_known_at_us"] is None


def _generation_pin(dataset: str, applied: dict[str, object]) -> dict[str, str]:
    marker = cast("dict[str, object]", applied["marker"])
    return {
        "dataset_id": dataset,
        "version": str(marker["version"]),
        "generation_id": str(marker["generation_id"]),
        "chain_hash": str(marker["chain_hash"]),
        "manifest_hash": str(marker["request_hash"]),
    }


def test_cross_provider_matches_one_reference_per_key(ws: Workspace) -> None:
    # The reference holds two rows per instrument, session and interval: KRW and USD.
    reference_pin, identity = _setup(
        ws,
        [
            bar("AAA.KO", D1, 1000.0, retrieved=LATE),
            bar("AAA.KO", D1, 0.75, retrieved=LATE, currency="USD"),
            bar("BBB.KQ", D1, 1000.0, retrieved=LATE),
            bar("BBB.KQ", D1, 0.75, retrieved=LATE, currency="USD"),
        ],
    )
    shortest = dict.fromkeys(("open", "high", "low", "close"), "float_shortest@1")
    reference = _apply(
        ws,
        spec(
            [reference_pin],
            identity,
            dataset="prices.kr.other",
            decimals={**shortest, "volume": "exact@1"},
        ),
    )
    assert reference["rows"] == {"ok": 4}
    candidate = add_source(
        ws,
        [bar("AAA.KO", D1, 1050.0, retrieved=LATE), bar("BBB.KQ", D1, 1005.0, retrieved=LATE)],
        tag="b",
    )
    rule = {
        "rule": "cross_provider_mismatch@1",
        "args": {
            "reference": _generation_pin("prices.kr.other", reference),
            "column": "close",
            "tolerance": "0.01",
        },
    }
    document = spec([candidate], identity, quality=[rule])
    planned = _plan(ws, document)
    assert planned["refusals"] == []
    assert cast("dict[str, int]", planned["flags"])["cross_provider_mismatch"] == 1
    applied = _apply(ws, document)
    flagged = ws.market.execute(
        "SELECT p.instrument_id, f.detail FROM quality_flags f "
        "JOIN prices p USING (generation_id, record_id, revision_id) "
        "WHERE f.flag='cross_provider_mismatch' AND f.generation_id=?",
        [applied["generation_id"]],
    ).fetchall()
    # Only the KRW reference judges a KRW value: AAA is 5% off, BBB 0.5%.
    assert flagged == [(AAA, "close")]
    assert engine.list_promotions(ws)[-1]["phase"] == "COMPLETED"


def test_krw_tick_refuses_non_krw_rows(ws: Workspace) -> None:
    pin, identity = _setup(
        ws,
        [
            bar("AAA.KO", D1, 1000.4, retrieved=LATE),
            bar("BBB.KQ", D1, 12.34, retrieved=LATE, currency="USD"),
        ],
    )
    document = spec([pin], identity)
    planned = _plan(ws, document)
    assert planned["rows"] == {"ok": 1, "refused_number": 1}
    assert planned["refusals"] == ["1 rows number refused"]
    # The USD bar is never rounded to a whole unit; the promotion is refused instead.
    with pytest.raises(ValueError, match="number refused"):
        _apply(ws, document)
    assert ws.market.execute("SELECT count(*) FROM market_generations").fetchone() == (0,)


def test_calendar_descendant_extends_chain(ws: Workspace) -> None:
    close1, close2 = us(at("2025-01-02T06:30:00")), us(at("2025-01-03T06:30:00"))
    first_calendar = publish_calendar(ws, {D1: (None, close1)})
    # The next calendar generation extends the sessions by one day.
    extended = publish_calendar(ws, {D2: (None, close2)}, sequence=2, parent="cal-1")
    other = publish_calendar(ws, {D1: (None, close1), D2: (None, close2)}, dataset="sessions.alt")

    def rules(calendar: dict[str, str]) -> dict[str, object]:
        rule = {
            "rule": "session_close_plus_lag@1",
            "basis": "record",
            "input": "session_date",
            "args": {"calendar": calendar, "calendar_id": "XKRX", "venue": "XKRX", "lag_us": 0},
        }
        return {"available_at_us": rule, "revision_known_at_us": rule}

    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE)])
    first = _apply(ws, spec([pin], identity, rules=rules(first_calendar)))
    parent = str(first["generation_id"])
    second_pin = add_source(
        ws,
        [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("AAA.KO", D2, 101.0, retrieved=LATE)],
        tag="b",
    )
    with pytest.raises(ValueError, match="descendant"):
        _plan(ws, spec([second_pin], identity, parent=parent, rules=rules(other)))
    second = _apply(ws, spec([second_pin], identity, parent=parent, rules=rules(extended)))
    assert second["operations"] == {"ASSERT": 1}
    assert second["unchanged"] == 1
    assert second["time_drift"] == 0
    (row,) = prices(ws, str(second["generation_id"]))
    assert (row["session_date"], row["available_at_us"]) == (D2, close2)


def test_absence_is_proven_by_the_full_snapshot_only(ws: Workspace) -> None:
    first_pin, identity = _setup(
        ws, [bar("AAA.KO", D1, 100.0, retrieved=LATE), bar("BBB.KQ", D1, 50.0, retrieved=LATE)]
    )
    first = _apply(ws, spec([first_pin], identity))
    parent = str(first["generation_id"])
    snapshot = add_source(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATER)], tag="b")
    history = add_source(ws, [bar("BBB.KQ", D1, 50.0, retrieved=LATE)], tag="c")
    # The full snapshot holds only AAA; a second pinned table still carries BBB in scope.
    policy = full_snapshot(snapshot, "2025-01-01", "2025-02-01")
    planned = _plan(ws, spec([snapshot, history], identity, parent=parent, tombstone=policy))
    (refusal,) = cast("list[str]", planned["refusals"])
    assert refusal.startswith("1 rows of other pinned sources fall in the full snapshot's scope")
    # Outside the scope the other table is no contradiction, and BBB is still absent.
    scoped = full_snapshot(snapshot, "2025-01-01", "2025-02-01", [AAA, BBB])
    outside = add_source(ws, [bar("CCC.KO", D1, 7.0, retrieved=LATE)], tag="d")
    applied = _apply(ws, spec([snapshot, outside], identity, parent=parent, tombstone=scoped))
    assert applied["operations"] == {"ASSERT": 1, "TOMBSTONE": 1}
    rows = _by_key(prices(ws, str(applied["generation_id"])))
    assert rows[(BBB, D1)]["op"] == "TOMBSTONE"
    assert rows[(CCC, D1)]["op"] == "ASSERT"


def test_watermark_version_follows_its_time(ws: Workspace) -> None:
    pin, identity = _setup(ws, [bar("AAA.KO", D1, 100.0, retrieved=LATER)])
    first = _apply(ws, spec([pin], identity))
    # A later generation whose rows were collected earlier does not take the watermark.
    older = add_source(ws, [bar("BBB.KQ", D1, 50.0, retrieved=LATE)], tag="b")
    _apply(ws, spec([older], identity, parent=str(first["generation_id"])))
    watermark = ws.state.execute(
        "SELECT committed_version, through_us FROM watermarks WHERE dataset_id='prices.kr.eodhd'"
    ).fetchone()
    assert tuple(watermark) == ("1", us(LATER))
    newer = add_source(ws, [bar("CCC.KO", D1, 7.0, retrieved=at("2025-01-30T00:00:00"))], tag="c")
    head = engine.list_promotions(ws)[-1]["generation_id"]
    _apply(ws, spec([newer], identity, parent=str(head)))
    watermark = ws.state.execute(
        "SELECT committed_version, through_us FROM watermarks WHERE dataset_id='prices.kr.eodhd'"
    ).fetchone()
    assert tuple(watermark) == ("3", us(at("2025-01-30T00:00:00")))
