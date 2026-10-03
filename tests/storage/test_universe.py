"""Index membership and listing universes: interval compression, resolution and registration."""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Iterator, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.storage import membership_pins
from aegis_alpha.storage.identity import decode_registry, mint_instrument, register_identities
from aegis_alpha.storage.legacy_import.norgate import MEMBERSHIP
from aegis_alpha.storage.membership_pins import (
    UniversePin,
    membership_parts,
    read_membership_pins,
    register_universe_manifest,
)
from aegis_alpha.storage.source_identity import LINK_PREFIX
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.universe import (
    INDEX_UNIVERSE_PREFIX,
    LISTING_UNIVERSE,
    build_index_universes,
    build_listing_universe,
    compress,
    expand,
    interval_us,
    register_universes,
    show_universe,
)
from aegis_alpha.storage.us_identity import build_from_workspace, session_start_us
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.us_identity_support import (
    MASTER_SCHEMA,
    commit,
    master,
    table,
)
from tests.storage.us_identity_support import link_instant as recorded_link

_ALLOWANCE = 2**40
INDEX = "Synthetic 3"


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _days(start: date, count: int) -> list[date]:
    """``count`` weekdays from ``start``: a series' own dates skip weekends."""
    days = []
    day = start
    while len(days) < count:
        if day.weekday() < 5:  # noqa: PLR2004 -- Monday..Friday
            days.append(day)
        day += timedelta(days=1)
    return days


def _rows(
    assetid: int, index: str, series: Sequence[tuple[str, str]], job: str = "job"
) -> list[dict[str, object]]:
    return [
        {
            "job_id": job,
            "assetid": assetid,
            "symbol": f"S{assetid}",
            "indexname": index,
            "file_sha256": hashlib.sha256(f"{assetid}/{index}".encode()).hexdigest(),
            "csv_sha256": hashlib.sha256(f"csv/{assetid}/{index}".encode()).hexdigest(),
            "date": day,
            "index_constituent": value,
        }
        for day, value in series
    ]


def _commit_membership(ws: Workspace, name: str, rows: list[dict[str, object]]) -> str:
    return commit(
        ws,
        f"index-membership-{name}",
        "constituents",
        table(rows, MEMBERSHIP.schema()),
        provider="norgate",
    )


def _register_master(ws: Workspace, rows: list[dict[str, object]]) -> str:
    master_id = commit(ws, "norgate-master", "observations", table(rows, MASTER_SCHEMA))
    registry = build_from_workspace(ws, master=master_id)
    document = decode_registry(registry.raw(), expected_file_sha256=registry.sha256())
    assert register_identities(ws.state, document, apply=True)["conflict_count"] == 0
    return master_id


def _daily(members: Sequence[object], days: Sequence[date]) -> list[int]:
    spans = [
        (cast("int", row["valid_from_us"]), cast("int | None", row["valid_to_us"]))
        for row in cast("Sequence[dict[str, object]]", members)
    ]
    return [sum(expand([span], [day])[0] for span in spans) for day in days]


def _series(days: Sequence[date], values: Sequence[bool]) -> list[tuple[str, str]]:
    return [
        (day.isoformat(), "1" if value else "0") for day, value in zip(days, values, strict=True)
    ]


def test_interval_compression_round_trips_daily_values() -> None:
    """Every run of member rows is one interval, and expanding them restores each day."""
    generator = random.Random(20261003)  # noqa: S311 -- reproducible synthetic series
    for case in range(300):
        days = _days(date(2001, 1, 1) + timedelta(days=case), generator.randint(1, 60))
        values = [generator.random() < 0.6 for _ in days]  # noqa: PLR2004 -- a skew toward members
        intervals = compress(list(zip(days, values, strict=True)))
        assert expand([interval_us(*span) for span in intervals], days) == values
        assert all(first <= last for first, last in intervals)
        # Runs are maximal: two intervals never touch on consecutive series rows.
        assert len(intervals) == sum(
            1
            for position, value in enumerate(values)
            if value and (position == 0 or not values[position - 1])
        )
    # A weekend between two member rows does not split a run, and the interval ends at the
    # New York start of the day after its last member row.
    friday, monday = date(2024, 1, 5), date(2024, 1, 8)
    assert compress([(friday, True), (monday, True)]) == [(friday, monday)]
    assert interval_us(friday, monday) == (
        session_start_us(friday),
        session_start_us(date(2024, 1, 9)),
    )
    with pytest.raises(ValueError, match="strictly increasing"):
        compress([(monday, True), (friday, True)])


def test_index_universe_sql_matches_the_reference_and_round_trips(ws: Workspace) -> None:
    generator = random.Random(7)  # noqa: S311 -- reproducible synthetic series
    days = _days(date(2010, 1, 4), 80)
    series = {
        100 + offset: [generator.random() < 0.5 for _ in days]  # noqa: PLR2004 -- fair coin
        for offset in range(12)
    }
    series[200] = [False] * len(days)  # never a member
    series[201] = [True] * len(days)  # a member throughout
    rows = [
        row
        for assetid, values in series.items()
        for row in _rows(assetid, INDEX, _series(days, values))
    ]
    # The cut falls inside one pair's rows: that pair is in both sources and is refused as
    # a whole, never stitched together.
    cut = len(rows) // 2 + 7
    first = _commit_membership(ws, "a", rows[:cut])
    second = _commit_membership(ws, "b", rows[cut:])
    _register_master(ws, [master(assetid, f"S{assetid}") for assetid in series])
    split = {cast("int", rows[cut]["assetid"])}
    build = build_index_universes(ws, [first, second], version="v1")
    assert build.report.refused == {"pair_repeated": 2}
    (universe,) = build.universes
    assert universe.universe_id == INDEX_UNIVERSE_PREFIX + INDEX
    known = {
        LINK_PREFIX + first: recorded_link(ws, first),
        LINK_PREFIX + second: recorded_link(ws, second),
    }
    by_instrument: dict[str, list[tuple[int, int | None]]] = {}
    for member in universe.members:
        assert member["known_from_us"] == known[cast("str", member["source_snapshot_id"])]
        assert member["known_to_us"] is None
        by_instrument.setdefault(cast("str", member["instrument_id"]), []).append(
            (cast("int", member["valid_from_us"]), cast("int | None", member["valid_to_us"]))
        )
    for assetid, values in series.items():
        instrument = mint_instrument("norgate_assetid", str(assetid))
        if assetid in split:
            assert instrument not in by_instrument
            continue
        spans = by_instrument.get(instrument, [])
        expected = [interval_us(*span) for span in compress(list(zip(days, values, strict=True)))]
        assert sorted(spans) == expected
        assert expand(spans, days) == values
    assert universe.report()["never_member"] == 1
    daily = cast("dict[str, object]", universe.report()["daily_members"])
    assert daily["days"] == len(days)
    counts = [sum(series[a][i] for a in series if a not in split) for i in range(len(days))]
    assert (daily["min"], daily["max"]) == (min(counts), max(counts))


def test_index_pairs_with_unreadable_values_are_refused(ws: Workspace) -> None:
    days = [day.isoformat() for day in _days(date(2015, 3, 2), 4)]
    rows = [
        *_rows(1, INDEX, [(days[0], "1"), (days[1], "1"), (days[2], "0"), (days[3], "1")]),
        *_rows(2, INDEX, [(days[0], "1"), (days[1], "2")]),
        *_rows(3, INDEX, [(days[0], "1"), ("2015-3-4", "1")]),
        *_rows(4, INDEX, [(days[1], "1"), (days[0], "1")]),
        *_rows(5, INDEX, [(days[0], "1"), (days[0], "1")]),
        *_rows(6, " Padded", [(days[0], "1")]),
        *_rows(9, INDEX, [(days[0], "1")]),
    ]
    source = _commit_membership(ws, "bad", rows)
    _register_master(ws, [master(assetid, f"S{assetid}") for assetid in (1, 2, 3, 4, 5, 6)])
    build = build_index_universes(ws, [source], version="v1")
    assert build.report.refused == {
        "constituent_invalid": 1,
        "date_invalid": 1,
        "dates_not_increasing": 2,
        "indexname_invalid": 1,
    }
    (universe,) = build.universes
    instrument = mint_instrument("norgate_assetid", "1")
    assert [(m["instrument_id"], m["valid_from_us"]) for m in universe.members] == [
        (instrument, session_start_us(date(2015, 3, 2))),
        (instrument, session_start_us(date(2015, 3, 5))),
    ]
    # Asset ID 9 is not a registered instrument: it stays out and is reported.
    assert universe.unresolved == ["9"]
    with pytest.raises(ValueError, match="no accepted membership pairs"):
        build_index_universes(ws, [source], version="v1", indexes=["Absent"])
    with pytest.raises(ValueError, match=r"no sl: link|unknown"):
        build_index_universes(ws, ["norgate-index-membership-absent"], version="v1")


def test_a_pair_two_sources_carry_is_refused_when_one_copy_is_refused(ws: Workspace) -> None:
    """A clean copy does not stand in for a pair another source carries and contradicts."""
    days = [day.isoformat() for day in _days(date(2016, 5, 2), 3)]
    first = _commit_membership(ws, "a", _rows(1, INDEX, [(days[0], "1"), (days[1], "2")]))
    rows = [
        *_rows(1, INDEX, [(days[0], "1"), (days[1], "1"), (days[2], "0")]),
        *_rows(2, INDEX, [(days[0], "1")]),
    ]
    second = _commit_membership(ws, "b", rows)
    _register_master(ws, [master(assetid, f"S{assetid}") for assetid in (1, 2)])
    build = build_index_universes(ws, [first, second], version="v1")
    assert build.report.refused == {"constituent_invalid": 1, "pair_repeated": 1}
    assert build.report.accepted == 1
    (universe,) = build.universes
    assert {m["instrument_id"] for m in universe.members} == {
        mint_instrument("norgate_assetid", "2")
    }


def test_index_universes_register_and_read_back(ws: Workspace) -> None:
    days = _days(date(2020, 6, 1), 10)
    rows = [
        *_rows(11, INDEX, _series(days, [True] * 10)),
        *_rows(12, INDEX, _series(days, [False] * 3 + [True] * 7)),
        *_rows(13, INDEX, _series(days, [True] * 6 + [False] * 4)),
        *_rows(11, "Other", _series(days, [False, True] * 5)),
    ]
    source = _commit_membership(ws, "reg", rows)
    _register_master(ws, [master(assetid, f"S{assetid}") for assetid in (11, 12, 13)])
    build = build_index_universes(ws, [source], version="2026-09-08")
    plan = register_universes(ws.state, build, apply=False)
    applied = register_universes(ws.state, build, apply=True)
    assert plan["pins"] == applied["pins"]
    assert register_universes(ws.state, build, apply=True) == applied
    pins = {
        cast("str", row["universe_id"]): UniversePin(
            cast("str", row["universe_id"]),
            cast("str", row["version"]),
            cast("str", row["content_hash"]),
        )
        for row in cast("list[dict[str, object]]", applied["pins"])
    }
    assert set(pins) == {INDEX_UNIVERSE_PREFIX + INDEX, INDEX_UNIVERSE_PREFIX + "Other"}
    read = read_membership_pins(
        ws.state, None, pins[INDEX_UNIVERSE_PREFIX + INDEX], max_materialization_bytes=_ALLOWANCE
    ).universe
    assert read is not None
    assert _daily(read.members, days) == [2, 2, 2, 3, 3, 3, 2, 2, 2, 2]
    shown = show_universe(ws.state, INDEX_UNIVERSE_PREFIX + "Other", "2026-09-08")
    assert shown["members"] == 5  # noqa: PLR2004 -- five alternating runs
    assert verify_workspace(ws)["verified"] is True


def test_listing_universe_spans_each_master_listing(ws: Workspace) -> None:
    rows = [
        master(21, "AAA", first_date="2001-02-05", last_date="2026-07-27"),
        master(22, "OLD-201001", delisted=True, first_date="1995-01-03", last_date="2010-01-29"),
        # Still listed at the export: it holds through the master's last observed session.
        master(25, "NEW", first_date="2020-03-02", last_date=None),
        master(26, "LATE", first_date="2026-07-28", last_date=None),
        master(23, "NODATE", first_date=None, last_date="2026-07-28"),
        master(24, "BACK", first_date="2010-01-05", last_date="2009-01-05"),
        master(27, "GONE-201001", delisted=True, first_date="1999-01-04", last_date=None),
    ]
    rows[-1]["last_date"] = None
    master_id = _register_master(ws, rows)
    build = build_listing_universe(ws, master_id, version="2026-07-28")
    assert build.report.refused == {
        "listing_dates_reversed": 1,
        "listing_end_unknown": 1,
        "listing_start_unknown": 1,
    }
    (universe,) = build.universes
    assert universe.universe_id == LISTING_UNIVERSE
    assert universe.detail["through"] == "2026-07-28"
    spans = {
        cast("str", m["instrument_id"]): (m["valid_from_us"], m["valid_to_us"])
        for m in universe.members
    }
    through = date(2026, 7, 28)
    assert spans == {
        mint_instrument("norgate_assetid", "21"): interval_us(date(2001, 2, 5), date(2026, 7, 27)),
        mint_instrument("norgate_assetid", "22"): interval_us(date(1995, 1, 3), date(2010, 1, 29)),
        mint_instrument("norgate_assetid", "25"): interval_us(date(2020, 3, 2), through),
        mint_instrument("norgate_assetid", "26"): interval_us(through, through),
    }
    assert {m["known_from_us"] for m in universe.members} == {recorded_link(ws, master_id)}
    applied = register_universes(ws.state, build, apply=True)
    assert cast("list[dict[str, object]]", applied["pins"])[0]["universe_id"] == LISTING_UNIVERSE


def _two_source_universe(ws: Workspace, count: int) -> tuple[dict[str, object], list[str]]:
    sources = []
    for name in ("left", "right"):
        rows = _rows(1, INDEX, [("2001-01-02", "1")], job=name)
        sources.append(LINK_PREFIX + _commit_membership(ws, name, rows))
    with atomic(ws.state):
        for index in range(count):
            ws.state.execute(
                "INSERT INTO instruments VALUES (?,?,?,?)", (f"I{index:03d}", None, "equity", "X")
            )
    inventories = []
    for snapshot_id in sources:
        header = ws.state.execute(
            "SELECT snapshot_id,provider,requested_at_us,retrieved_at_us,publication_at_us,status "
            "FROM source_snapshots WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        files = [
            dict(row)
            for row in ws.state.execute(
                "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=?",
                (snapshot_id,),
            )
        ]
        inventories.append({**dict(header), "files": files})
    body: dict[str, object] = {
        "schema": "aas-universe-version-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "universe_id": "u",
        "version": "v",
        "instruments": [
            {
                "instrument_id": f"I{index:03d}",
                "issuer_id": None,
                "asset_type": "equity",
                "venue": "X",
            }
            for index in range(count)
        ],
        "members": [
            {
                "instrument_id": f"I{index:03d}",
                "valid_from_us": 0,
                "valid_to_us": 10,
                "known_from_us": 0,
                "known_to_us": None,
                "source_snapshot_id": sources[index % 2],
            }
            for index in range(count)
        ],
        "sources": inventories,
    }
    return body, sources


def test_universe_parts_are_filled_source_by_source(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A part takes one source's members at a time, so it rarely carries two inventories."""
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    body, sources = _two_source_universe(ws, 40)
    pin = register_universe_manifest(ws.state, body)
    parts = cast("tuple[UniversePin, ...]", membership_parts(ws.state, pin))
    assert len(parts) > 2  # noqa: PLR2004 -- the byte limit forces several parts
    cited = [
        {
            row[0]
            for row in ws.state.execute(
                "SELECT source_snapshot_id FROM universe_members WHERE universe_id=? AND version=?",
                (pin.universe_id, part.version),
            )
        }
        for part in parts
    ]
    # Members alternate sources in canonical order; source-major filling keeps them apart
    # except where one part takes the last members of one source and the first of the next.
    assert sum(len(found) > 1 for found in cited) <= 1
    assert cited[0] == {sources[0]}
    assert cited[-1] == {sources[1]}
    members = read_membership_pins(
        ws.state, None, pin, max_materialization_bytes=_ALLOWANCE
    ).universe
    assert members is not None
    assert [m["instrument_id"] for m in members.members] == [f"I{i:03d}" for i in range(40)]
    assert verify_workspace(ws)["verified"] is True


def test_a_member_key_repeated_across_parts_is_refused(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parts in source order can still repeat one member key; reading and verify refuse it."""
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    body, sources = _two_source_universe(ws, 40)
    members = cast("list[dict[str, object]]", body["members"])
    assert members[0]["source_snapshot_id"] == sources[0]
    members.append({**members[0], "source_snapshot_id": sources[1]})
    canonical = membership_pins._canonical  # noqa: SLF001 -- the whole check is what is bypassed
    with monkeypatch.context() as patched:
        # Admit the repeated key past the whole-document check and the registration read,
        # so each part is a valid v1 document in valid source order and only the repeat is
        # left for the cross-part rule.
        patched.setattr(
            membership_pins,
            "_canonical",
            lambda value, *, identity, bounded=True: (
                dict(cast("dict[str, object]", value))
                if not bounded
                else canonical(value, identity=identity, bounded=bounded)
            ),
        )
        patched.setattr(membership_pins, "verify_membership_pin", lambda *_a, **_k: None)
        pin = register_universe_manifest(ws.state, body)
    parts = cast("tuple[UniversePin, ...]", membership_parts(ws.state, pin))
    holding = [
        part.version
        for part in parts
        if ws.state.execute(
            "SELECT 1 FROM universe_members WHERE universe_id=? AND version=? "
            "AND instrument_id='I000'",
            (pin.universe_id, part.version),
        ).fetchone()
    ]
    # Source A's copy is in the first part and source B's in a later one.
    assert len(holding) == 2  # noqa: PLR2004 -- one copy per source
    assert holding[0] == parts[0].version
    with pytest.raises(ValueError, match="repeat a universe member"):
        read_membership_pins(ws.state, None, pin, max_materialization_bytes=_ALLOWANCE)
    with pytest.raises(ValueError, match="repeat a universe member"):
        verify_workspace(ws)


def test_universe_cli_plans_registers_and_shows(tmp_path: Path) -> None:
    home = tmp_path / "cli"
    initialize(home)
    days = _days(date(2020, 6, 1), 4)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source = _commit_membership(workspace, "cli", _rows(31, INDEX, _series(days, [True] * 4)))
        _register_master(workspace, [master(31, "S31")])
    report = tmp_path / "report.json"
    arguments = ["universe", "index", "--home", str(home), "--source", source, "--version", "v1"]
    assert main([*arguments, "--plan"]) == 0
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM universe_versions").fetchone()[0] == 0
    # A report path that cannot be created is refused before anything is registered.
    assert main([*arguments, "--report", str(tmp_path / "absent" / "report.json")]) != 0
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT count(*) FROM universe_versions").fetchone()[0] == 0
    assert main([*arguments, "--report", str(report)]) == 0
    full = json.loads(report.read_text())
    assert full["universes"][0]["members"] == 1
    universe_id = INDEX_UNIVERSE_PREFIX + INDEX
    show = ["universe", "show", "--home", str(home), "--id", universe_id, "--version", "v1"]
    assert main(show) == 0
    with open_workspace(home) as workspace:
        assert show_universe(workspace.state, universe_id, "v1")["members"] == 1
