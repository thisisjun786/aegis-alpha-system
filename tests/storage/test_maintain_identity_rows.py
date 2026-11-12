# ruff: noqa: S608 -- test-owned SQL
"""A maintenance pass loads each pinned snapshot's identity-key rows once for every source.

Each source is one synthetic US split table committed under the ``qveris-splits-`` shape
with fixed content, so its ID and every hash downstream are the same in every run; the
registry is synthetic. Each scenario runs a pass that starts ``actions.us.eodhd`` from its
declared template and a later pass that continues the chain, and compares what they publish
and record with the values the per-source load published before the rows were shared.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.identity import parse_registry, register_identities, snapshot_identities
from aegis_alpha.storage.maintain_promotion import ROUTES, promote_datasets
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, membership_parts
from aegis_alpha.storage.promotion import engine
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.identity_support import (
    UNBOUNDED,
    assertion,
    document,
    instrument,
    link_source,
)
from tests.storage.promotion_support import register_symbols, us
from tests.storage.test_qveris_actions import opens
from tests.storage.test_us_delisted import ISIN_A, ISIN_B, _bars, _list, _row
from tests.storage.test_us_delisted import ROUTE as STATUS

if TYPE_CHECKING:
    import duckdb

    from aegis_alpha.compute_resources import ComputeBudget

FIRST = date(2026, 8, 31)
ROUTES_US = [route for route in ROUTES if route.dataset_id == "actions.us.eodhd"]
START = frozenset({"actions.us.eodhd"})
# EEE.US is never registered, so every source keeps one unresolved row and records the
# key's members.
REGISTERED = {"AAA.US": "200001", "BBB.US": "200002"}
SPLITS = pa.schema(
    [
        ("provider_symbol", pa.string()),
        ("date", pa.date32()),
        ("split", pa.string()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
    ]
)


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def weekdays(first: date, count: int) -> list[date]:
    found: list[date] = []
    day = first
    while len(found) < count:
        if day.weekday() < 5:  # noqa: PLR2004 -- Monday to Friday
            found.append(day)
        day += timedelta(days=1)
    return found


def split_source(ws: Workspace, day: date) -> str:
    """Commit the day's splits (a registered AAA and the unregistered EEE) as one source,
    linked when they were retrieved; its source ID."""
    retrieved = datetime(day.year, day.month, day.day, 22, tzinfo=UTC)
    _, sha256, size = put_raw(ws.paths.raw, f"synthetic-splits-{day.isoformat()}".encode())
    content = SourceContent("qveris", "splits", 1, (SourceFile(sha256, size),))
    table = pa.table(
        {
            "provider_symbol": ["AAA.US", "EEE.US"],
            "date": [day, day],
            "split": ["2/1", "3/1"],
            "retrieved_at": [retrieved, retrieved],
        },
        schema=SPLITS,
    )
    with patch.object(time, "time_ns", lambda: us(retrieved) * 1000):
        source_library.import_content_arrow(ws, content, "splits", table.to_reader())
    return content.source_id


def collect(ws: Workspace, days: list[date]) -> list[str]:
    return [split_source(ws, day) for day in days]


def scenario(ws: Workspace, *, filler: int, days: list[date]) -> tuple[dict[str, str], list[str]]:
    """Sources for ``days``, the XNYS sessions they need and a snapshot of ``filler`` more
    symbols than the sources name; the snapshot's pin and the sources."""
    sources = collect(ws, days)
    symbols = REGISTERED | {f"F{index:05d}.US": str(300000 + index) for index in range(filler)}
    identity = register_symbols(ws, sources[0], symbols, name="maintain-synthetic")
    opens(ws, "XNYS", tuple(weekdays(days[0], len(days) + 8)))
    return identity, sources


def run(
    ws: Workspace, identity: dict[str, str], now_us: int, *, start: frozenset[str] = START
) -> dict[str, object]:
    return promote_datasets(ws, apply=True, now_us=now_us, routes=ROUTES_US, identity=identity,
                            start=start)  # fmt: skip


def published(ws: Workspace, reports: list[dict[str, object]]) -> dict[str, object]:
    """What the passes published and recorded: their reports (specs, generations, outcomes and
    unresolved counts per step), the catalog with its manifest hashes, the quality checks and
    the rows."""
    versions = ws.state.execute(
        "SELECT * FROM dataset_versions ORDER BY dataset_id, sequence"
    ).fetchall()
    checks = ws.state.execute(
        "SELECT check_id, dataset_id, version, rule_id, result, reason FROM quality_checks "
        "ORDER BY check_id"
    ).fetchall()
    rows = (
        ws.market.execute(
            "SELECT * EXCLUDE (ingested_at_us) FROM corporate_actions ORDER BY ALL"
        ).fetchall()
        if any(row[0] == "actions.us.eodhd" for row in versions)
        else []
    )
    return json.loads(
        json.dumps(
            {
                "reports": reports,
                "versions": [list(row) for row in versions],
                "checks": [list(row) for row in checks],
                "rows": [list(row) for row in rows],
            },
            default=str,
        )
    )


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _steps(report: dict[str, object]) -> list[dict[str, object]]:
    return [
        step
        for item in cast("list[dict[str, object]]", report["datasets"])
        for source in cast("list[dict[str, object]]", item["sources"])
        for step in cast("list[dict[str, object]]", source["steps"])
    ]


def test_a_start_pass_and_a_continuing_pass_publish_what_the_per_source_load_did(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    days = weekdays(FIRST, 5)
    identity, _ = scenario(ws, filler=40, days=days[:3])
    verified: list[str] = []
    verify = engine.verify_membership_pin

    def counted(
        connection: sqlite3.Connection,
        pin: IdentityPin | UniversePin,
        *,
        max_materialization_bytes: int,
    ) -> None:
        verified.append(repr(pin))
        verify(connection, pin, max_materialization_bytes=max_materialization_bytes)

    monkeypatch.setattr(engine, "verify_membership_pin", counted)
    started = run(ws, identity, 1)
    started_checks = len(verified)
    collect(ws, days[3:])
    continued = run(ws, identity, 2, start=frozenset())
    steps = _steps(started)
    assert [step["published"] for step in steps] == [True, True, True]
    assert [step["unresolved_token_count"] for step in steps] == [1, 1, 1]
    assert [step["unresolved_token_count"] for step in _steps(continued)] == [1, 1]
    # Each pass verifies each part of the snapshot once, whatever number of sources it promotes.
    assert len(set(verified)) == started_checks
    assert verified == verified[:started_checks] * 2
    # The values the per-source load published for this input (dev 0efbb9a).
    assert digest(published(ws, [started, continued])) == PUBLISHED


# How every source's promotion read one key's members before the pass shared them.
PER_SOURCE_SQL = (
    "SELECT a.token, a.instrument_id, m.valid_from_us, m.valid_to_us "
    "FROM identity_snapshot_members m JOIN identity_assertions a USING (assertion_id) "
    "WHERE m.snapshot_id IN (SELECT value FROM json_each(?)) AND m.known_to_us IS NULL "
    "AND a.provider=? AND a.namespace=?"
)
TICKER_AND_SYMBOL = IdentityKey("norgate", "ticker", also=(("eodhd", "eodhd_symbol"),))


def _bounded(token: int) -> tuple[int, int]:
    return token * 100, token * 100 + 50


def _registry(ws: Workspace, tokens: range) -> None:
    """A ticker per instrument and, for every other one, an EODHD symbol whose validity is
    bounded for a third of them."""
    names = [str(token) for token in tokens]
    symbols = [
        assertion(name, provider="eodhd", namespace="eodhd_symbol", value=f"S{name}.US",
                  valid=_bounded(int(name)) if int(name) % 3 else (UNBOUNDED, None))
        for name in names[::2]
    ]  # fmt: skip
    document_ = document(instruments=[instrument(name) for name in names],
                         assertions=[*(assertion(name) for name in names), *symbols])  # fmt: skip
    register_identities(ws.state, parse_registry(document_), apply=True)


def _parts(connection: sqlite3.Connection, pin: IdentityPin) -> str:
    parts = membership_parts(connection, pin)
    return json.dumps([cast("IdentityPin", part).snapshot_id for part in parts])


def _snapshot(ws: Workspace) -> IdentityPin:
    """Several parts, and a corrected ticker whose first assertion the snapshot ends."""
    link_source(ws)
    _registry(ws, range(100000, 103000))
    (original,) = ws.state.execute(
        "SELECT assertion_id FROM identity_assertions WHERE provider='norgate' AND token='100000'"
    ).fetchone()
    corrected = assertion("100000", value="CORRECTED", known=9, supersedes=str(original))
    register_identities(ws.state, parse_registry(document(assertions=[corrected])), apply=True)
    report = snapshot_identities(ws.state, "members", created_at_us=5, apply=True)
    pin = IdentityPin("members", str(report["content_hash"]))
    assert len(membership_parts(ws.state, pin)) > 1
    return pin


def test_the_shared_rows_are_the_rows_each_source_loaded(ws: Workspace) -> None:
    pin = _snapshot(ws)
    names = _parts(ws.state, pin)
    ws.market.execute(
        "CREATE TEMP TABLE per_source (token VARCHAR, instrument_id VARCHAR, "
        "valid_from_us BIGINT, valid_to_us BIGINT)"
    )
    for provider, namespace in TICKER_AND_SYMBOL.pairs():
        ws.market.executemany(
            "INSERT INTO per_source VALUES (?, ?, ?, ?)",
            ws.state.execute(PER_SOURCE_SQL, (names, provider, namespace)).fetchall(),
        )
    rows = engine.SharedInputs(ws)
    try:
        shared = rows.table(pin, TICKER_AND_SYMBOL)
        assert rows.table(pin, TICKER_AND_SYMBOL) == shared
        for left, right in (("per_source", shared), (shared, "per_source")):
            difference = f"SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}"
            assert ws.market.execute(difference).fetchall() == []
        counts = ws.market.execute(
            f"SELECT count(*), count(*) FILTER (valid_to_us IS NULL), "
            f"count(*) FILTER (token = 'CORRECTED') FROM {shared}"
        ).fetchone()
    finally:
        rows.close()
    assert counts == (4500, 3500, 1)
    assert ws.market.execute(
        "SELECT count(*) FROM per_source WHERE token = '100000'"
    ).fetchone() == (0,)


def _steps_of(connection: sqlite3.Connection, pin: IdentityPin) -> int:
    """SQLite virtual-machine steps (in hundreds) to read the key's members of ``pin``."""
    names = _parts(connection, pin)
    steps = [0]

    def step() -> int:
        steps[0] += 1
        return 0

    connection.set_progress_handler(step, 100)
    try:
        for provider, namespace in TICKER_AND_SYMBOL.pairs():
            connection.execute(engine.KEY_MEMBERS_SQL, (names, provider, namespace)).fetchall()
    finally:
        connection.set_progress_handler(None, 0)
    return steps[0]


def test_the_key_read_costs_the_pinned_members_not_the_registry(ws: Workspace) -> None:
    pin = _snapshot(ws)
    before = _steps_of(ws.state, pin)
    # Ten times the snapshot's assertions of the same keys, registered after it.
    _registry(ws, range(200000, 230000))
    after = _steps_of(ws.state, pin)
    assert after <= before * 1.05, (before, after)


def _member_reads(root: Path, sources: int) -> list[str]:
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        identity, _ = scenario(workspace, filler=40, days=weekdays(FIRST, sources))
        statements: list[str] = []
        workspace.state.set_trace_callback(statements.append)
        try:
            report = run(workspace, identity, 1)
        finally:
            workspace.state.set_trace_callback(None)
    assert [step["published"] for step in _steps(report)] == [True] * sources
    return [text for text in statements if "identity_snapshot_members" in text]


def test_a_pass_reads_the_snapshot_members_as_often_for_three_sources_as_for_one(
    tmp_path: Path,
) -> None:
    one = _member_reads(tmp_path / "one", 1)
    three = _member_reads(tmp_path / "three", 3)
    assert one
    assert len(three) == len(one)


PUBLISHED = "57bb40907cb0c83fa3978e4b134349c1095eafedd351ddb825ba2a9988b22a56"


def _lists(ws: Workspace, days: list[date]) -> None:
    """Each day a listed list holding AAA and a delisted list holding OLD under ISIN_B."""
    for day in days:
        _list(ws, f"{day.isoformat()}T21:00:00Z", "0", [_row("AAA", ISIN_A)])
        _list(ws, f"{day.isoformat()}T22:00:00Z", "1", [_row("OLD", ISIN_B), _row("GONE", None)])


def _status_rows(ws: Workspace) -> list[list[object]]:
    rows = ws.market.execute(
        "SELECT * EXCLUDE (ingested_at_us) FROM instrument_status ORDER BY ALL"
    ).fetchall()
    return [list(row) for row in rows]


def test_a_status_start_pass_and_a_continuing_pass_publish_what_the_per_source_load_did(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _bars(ws)
    (bars,) = ws.state.execute(
        "SELECT generation_id FROM dataset_versions WHERE dataset_id='prices.us.eodhd'"
    ).fetchone()
    checked: list[str] = []
    verify = engine.verify_generation_bulk

    def counted(
        market: duckdb.DuckDBPyConnection, generation_id: str, *, budget: ComputeBudget
    ) -> dict[str, object]:
        checked.append(generation_id)
        return verify(market, generation_id, budget=budget)

    monkeypatch.setattr(engine, "verify_generation_bulk", counted)
    days = weekdays(date(2026, 9, 14), 5)
    _lists(ws, days[:3])
    start = frozenset({"status.us.eodhd"})
    started = promote_datasets(ws, apply=True, now_us=10, routes=[STATUS], identity=identity,
                               start=start)  # fmt: skip
    started_checks = checked.count(bars)
    _lists(ws, days[3:])
    continued = promote_datasets(ws, apply=True, now_us=20, routes=[STATUS], identity=identity)
    assert [step["published"] for step in _steps(started)] == [True, False, False]
    assert [step["unresolved_token_count"] for step in _steps(continued)] == [1, 1]
    # Each pass verifies the pinned price chain once, whatever number of lists it promotes.
    assert (started_checks, checked.count(bars)) == (1, 2)
    found = published(ws, [started, continued])
    found["rows"] = _status_rows(ws)
    # The values the per-source load published for this input (dev 0efbb9a).
    assert digest(found) == STATUS_PUBLISHED


STATUS_PUBLISHED = "d1f747b8110c28595606046f7d5bc4ec11e92e210f02d7f57be0a47763487bff"
