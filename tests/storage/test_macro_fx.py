# ruff: noqa: PLR2004, S608 -- synthetic counts and values are the expected values; test SQL
"""Macro and FX mappers on synthetic sources: ALFRED vintages, FX series, KR public series.

Every expected value is written out from the fixture, independently of the mapper SQL:
times come from Python's zone data, partitions from a brute-force search, and the
strict reads from which vintage the fixture says was current at each cutoff.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Final, cast

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import market, source_library
from aegis_alpha.storage.legacy_import.engine import apply_import
from aegis_alpha.storage.legacy_import.manifest import parse_manifest
from aegis_alpha.storage.market_inputs import GenerationPin, load_pinned_heads
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers import REGISTRY, mapper
from aegis_alpha.storage.promotion.mappers.fred import vintage_partitions
from aegis_alpha.storage.promotion.spec import parse_spec
from aegis_alpha.storage.promotion.time_rules import local_day_end
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin, HeadQuery
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace

CHICAGO: Final = "America/Chicago"
SEOUL: Final = "Asia/Seoul"
BUDGET: Final = ComputeBudget(Fraction(1), 256 * 1024 * 1024)
RETRIEVED: Final = datetime(2026, 9, 1, tzinfo=UTC)
GRANT: Final = ("local_day_end@1",)
ALFRED: Final = pa.schema(
    [
        ("series_id", pa.string()),
        ("observation_date", pa.date32()),
        ("realtime_start", pa.date32()),
        ("realtime_end", pa.date32()),
        ("value", pa.string()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)
CLOSES: Final = pa.schema(
    [
        ("symbol", pa.string()),
        ("assetid", pa.int64()),
        ("date", pa.date32()),
        ("close", pa.float64()),
        ("raw_row_json", pa.string()),
    ]
)
OPEN_END: Final = date(9999, 12, 31)


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _day_end(day: date, zone: str) -> int:
    return local_day_end(day, zone)[0]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _source(
    workspace: Workspace, schema: pa.Schema, rows: Sequence[tuple[object, ...]], *, tag: str
) -> dict[str, str]:
    """Commit ``rows`` as one linked content source table; return its spec pin."""
    _, digest, size = put_raw(workspace.paths.raw, f"synthetic-{tag}".encode())
    content = SourceContent("synthetic", tag, 1, (SourceFile(digest, size),))
    columns = list(zip(*rows, strict=True))
    table = pa.table(
        {name: list(column) for name, column in zip(schema.names, columns, strict=True)},
        schema=schema,
    )
    result = source_library.import_content_arrow(workspace, content, "rows", table.to_reader())
    tables = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": content.source_id,
        "source_sha256": content.sha256,
        "table": "rows",
        "digest": str(tables[0]["digest"]),
    }


def _rule(name: str, basis: str, source: str | None, zone: str | None) -> dict[str, object]:
    return {
        "rule": name,
        "basis": basis,
        "input": source,
        "args": {} if zone is None else {"timezone": zone},
    }


def _spec(  # noqa: PLR0913, PLR0917 -- every spec field a test varies
    domain: str,
    dataset: str,
    sources: list[dict[str, str]],
    name: str,
    args: Mapping[str, object],
    times: dict[str, object],
    *,
    decimal: Mapping[str, str],
    parent: str | None = None,
    partition: tuple[date, date] | None = None,
    tombstone: Mapping[str, object] | None = None,
) -> tuple[bytes, str]:
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": domain, "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": name, "args": dict(args)},
        "partition": None
        if partition is None
        else {"from": partition[0].isoformat(), "to": partition[1].isoformat()},
        "time_rules": {"available_at_us": times, "revision_known_at_us": times},
        "decimal_rule": dict(decimal),
        "quality_rules": [],
        "tombstone_policy": dict(tombstone or {"mode": "never"}),
        "identity_snapshot": None,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _alfred_spec(
    pin: dict[str, str], *, parent: str | None, partition: tuple[date, date] | None
) -> tuple[bytes, str]:
    return _spec(
        "macro_observations",
        "macro.us.alfred",
        [pin],
        "fred.alfred@1",
        {},
        _rule("local_day_end@1", "revision", "vintage_start", CHICAGO),
        decimal={"value": "decimal_text@1"},
        parent=parent,
        partition=partition,
    )


def _apply(workspace: Workspace, document: tuple[bytes, str]) -> dict[str, object]:
    return promote(workspace, document[0], document[1], apply=True)


def _pin(workspace: Workspace, generation_id: str) -> GenerationPin:
    marker = market.marker_for(workspace.market, generation_id)
    return GenerationPin(
        str(marker["dataset_id"]),
        str(marker["version"]),
        generation_id,
        str(marker["chain_hash"]),
        str(marker["request_hash"]),
    )


def _read(
    workspace: Workspace,
    domain: str,
    generation_id: str,
    cutoff: int | None,
    grants: tuple[str, ...],
) -> list[dict[str, object]]:
    read = load_pinned_heads(
        workspace,
        HeadBinding(domain, (HeadPin(_pin(workspace, generation_id)),), granted_rules=grants),
        HeadQuery(cutoff_us=cutoff),
        budget=BUDGET,
    )
    return [dict(row.values) for row in read.rows]


# --- mappers over in-memory fixtures ---------------------------------------------------------


def _memory(columns: str, rows: Sequence[tuple[object, ...]]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        f"{columns})"
    )
    marks = ", ".join("?" for _ in range(len(rows[0]) + 3))
    connection.executemany(
        f"INSERT INTO src VALUES ({marks})",
        [(0, index, "h", *row) for index, row in enumerate(rows)],
    )
    return connection


def test_fred_alfred_maps_synthetic_fixture() -> None:
    assert {
        "fred.alfred@1",
        "fred.fx_series@1",
        "norgate.fx_closes@1",
        "bok.observations@1",
        "oecd.observations@1",
    } <= set(REGISTRY)
    alfred = mapper("fred.alfred@1")
    alfred.check_args({})
    with pytest.raises(ValueError, match="no arguments"):
        alfred.check_args({"unit": "percent"})
    assert alfred.identity({}) is None
    assert alfred.numeric_columns({}) == {"value": "VARCHAR"}
    connection = _memory(
        "series_id VARCHAR, observation_date DATE, realtime_start DATE, realtime_end DATE, "
        "value VARCHAR, retrieved_at_utc TIMESTAMPTZ",
        [
            ("UNRATE", date(2020, 1, 1), date(2020, 2, 7), date(2020, 3, 5), "3.6", RETRIEVED),
            ("UNRATE", date(2020, 1, 1), date(2020, 3, 6), OPEN_END, "3.5", RETRIEVED),
            ("UNRATE", date(2020, 2, 1), date(2020, 3, 6), OPEN_END, ".", RETRIEVED),
            ("UNRATE", date(2020, 3, 1), date(2020, 4, 3), OPEN_END, None, RETRIEVED),
            ("UNRATE", date(2020, 4, 1), date(2020, 5, 8), OPEN_END, "3,5", RETRIEVED),
            ("INDPRO", date(1919, 1, 1), date(1927, 4, 28), OPEN_END, "-1.25e2", RETRIEVED),
        ],
    )
    rows = connection.execute(
        "SELECT series_id, observation_period, unit, source_vintage_start, source_vintage_end, "
        "value, value_state, _aas_ingested_at_us, _aas_t_vintage_start "
        f"FROM ({alfred.select('src', {})}) ORDER BY _aas_ordinal"
    ).fetchall()
    ingested = _us(RETRIEVED)
    assert rows == [
        ("UNRATE", date(2020, 1, 1), "as_published", date(2020, 2, 7), None, "3.6", "present",
         ingested, date(2020, 2, 7)),
        ("UNRATE", date(2020, 1, 1), "as_published", date(2020, 3, 6), None, "3.5", "present",
         ingested, date(2020, 3, 6)),
        # FRED's "." and no text are missing; any other text is invalid and keeps no value.
        ("UNRATE", date(2020, 2, 1), "as_published", date(2020, 3, 6), None, None, "missing",
         ingested, date(2020, 3, 6)),
        ("UNRATE", date(2020, 3, 1), "as_published", date(2020, 4, 3), None, None, "missing",
         ingested, date(2020, 4, 3)),
        ("UNRATE", date(2020, 4, 1), "as_published", date(2020, 5, 8), None, None, "invalid",
         ingested, date(2020, 5, 8)),
        # A vintage before the epoch keeps its day in the row; its time input is 1970-01-01.
        ("INDPRO", date(1919, 1, 1), "as_published", date(1927, 4, 28), None, "-1.25e2",
         "present", ingested, date(1970, 1, 1)),
    ]  # fmt: skip


def test_fx_mappers_map_synthetic_fixtures() -> None:
    closes = mapper("norgate.fx_closes@1")
    args = {"series": "USDKRW", "base": "USD", "quote": "KRW", "timezone": SEOUL}
    closes.check_args(args)
    for bad, message in (
        ({**args, "base": "usd"}, "currency codes"),
        ({**args, "quote": "USD"}, "differ"),
        ({**args, "timezone": "Mars/Base"}, "IANA zone"),
        ({"series": "USDKRW"}, "exactly"),
    ):
        with pytest.raises(ValueError, match=message):
            closes.check_args(bad)
    assert closes.date_column is None
    day = date(2020, 1, 2)

    def raw(text: str | None, on: date = day) -> str:
        return json.dumps({"Close": text, "Date": on.isoformat()})

    connection = _memory(
        "symbol VARCHAR, assetid BIGINT, date DATE, close DOUBLE, raw_row_json VARCHAR",
        [
            ("USDKRW", 1, day, 1158.1, raw("1158.1")),
            ("USDKRW", 1, day + timedelta(1), 1158.1, raw("1158.2", day + timedelta(1))),
            ("USDKRW", 1, day + timedelta(2), 1158.1, raw("1158.1", day)),
            ("USDKRW", 1, day + timedelta(3), None, raw(None, day + timedelta(3))),
            ("USDKRW", 1, day + timedelta(4), 0.0, raw("0", day + timedelta(4))),
            ("XAUUSD", 2, day, 1500.0, raw("1500")),
        ],
    )
    rows = connection.execute(
        "SELECT _aas_ordinal, base_currency, quote_currency, fixing_at_us, rate, value_state, "
        f"_aas_ingested_at_us, _aas_t_fixing_date FROM ({closes.select('src', args)}) "
        "ORDER BY _aas_ordinal"
    ).fetchall()
    assert rows == [
        # The export's own text is the rate, not the double read from it.
        (0, "USD", "KRW", _day_end(day, SEOUL), "1158.1", "present", None, day),
        # Text and double disagree, or the row's own date is another day: invalid.
        (1, "USD", "KRW", _day_end(day + timedelta(1), SEOUL), None, "invalid", None,
         day + timedelta(1)),
        (2, "USD", "KRW", _day_end(day + timedelta(2), SEOUL), None, "invalid", None,
         day + timedelta(2)),
        (3, "USD", "KRW", _day_end(day + timedelta(3), SEOUL), None, "missing", None,
         day + timedelta(3)),
        (4, "USD", "KRW", _day_end(day + timedelta(4), SEOUL), None, "invalid", None,
         day + timedelta(4)),
    ]  # fmt: skip
    series = mapper("fred.fx_series@1")
    args = {"series": "DEXKOUS", "base": "USD", "quote": "KRW", "timezone": "America/New_York"}
    series.check_args(args)
    connection = _memory(
        "series_id VARCHAR, observation_date VARCHAR, value VARCHAR",
        [
            ("DEXKOUS", "2011-10-03", "1180.00"),
            ("DEXKOUS", "2011-10-04", ""),
            ("DEXKOUS", "2011-10-05", "."),
            ("DEXKOUS", "2011-10-06", "-1"),
            ("DEXKOUS", "2011-02-30", "1"),
            ("DEXKOUS", "2011-10-7", "1"),
            ("DEXJPUS", "2011-10-03", "77.0"),
        ],
    )
    rows = connection.execute(
        "SELECT _aas_ordinal, fixing_at_us, rate, value_state, _aas_t_fixing_date "
        f"FROM ({series.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()
    ny = "America/New_York"
    assert rows == [
        (0, _day_end(date(2011, 10, 3), ny), "1180.00", "present", date(2011, 10, 3)),
        (1, _day_end(date(2011, 10, 4), ny), None, "missing", date(2011, 10, 4)),
        (2, _day_end(date(2011, 10, 5), ny), None, "missing", date(2011, 10, 5)),
        (3, _day_end(date(2011, 10, 6), ny), None, "invalid", date(2011, 10, 6)),
        # An impossible or misspelled date leaves the required fixing time empty.
        (4, None, "1", "present", None),
        (5, None, "1", "present", None),
    ]


def test_korea_observations_map_synthetic_fixture() -> None:
    bok = mapper("bok.observations@1")
    oecd = mapper("oecd.observations@1")
    assert (bok.provider, oecd.provider) == ("bok", "oecd")
    assert bok.time_inputs == {}
    columns = (
        "series_id VARCHAR, period VARCHAR, value VARCHAR, value_raw VARCHAR, units VARCHAR, "
        "unit_multiplier VARCHAR, base_period VARCHAR, regime VARCHAR, status VARCHAR"
    )
    connection = _memory(
        columns,
        [
            ("KOR_POLICY_BOK", "2008-03-07", "5.00", "5.00", "percent", None, None, "base_rate",
             None),
            ("KOR_CPI_OECD", "1965-02", "2.685533", "2.685533", "index", None, "2015", None, "A"),
            ("KOR_GDP", "2020-Q3", "1.5", "1.5", "KRW", "9", None, None, None),
            ("KOR_GDP", "2020", "", "", "KRW", "9", "", None, None),
            ("KOR_X", "2020-13", "1", "1", "index", None, None, None, None),
            ("KOR_X", "2020-01-01", "1", "1", None, None, None, None, None),
        ],
    )  # fmt: skip
    rows = connection.execute(
        "SELECT series_id, observation_period, unit, source_vintage_start, value, value_state, "
        f"_aas_ingested_at_us FROM ({oecd.select('src', {})}) ORDER BY _aas_ordinal"
    ).fetchall()
    assert rows == [
        ("KOR_POLICY_BOK", date(2008, 3, 7), "percent", None, "5.00", "present", None),
        ("KOR_CPI_OECD", date(1965, 2, 1), "index;base=2015", None, "2.685533", "present", None),
        ("KOR_GDP", date(2020, 7, 1), "KRW;multiplier=9", None, "1.5", "present", None),
        ("KOR_GDP", date(2020, 1, 1), "KRW;multiplier=9", None, None, "missing", None),
        # An impossible month and a row without units leave a required column empty.
        ("KOR_X", None, "index", None, "1", "present", None),
        ("KOR_X", date(2020, 1, 1), None, None, "1", "present", None),
    ]


# --- vintage partitions ----------------------------------------------------------------------


def _vintage_table(keys: Mapping[tuple[str, date], Sequence[date]]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE v (series_id VARCHAR, observation_date DATE, realtime_start DATE)"
    )
    rows = [(series, day, start) for (series, day), starts in keys.items() for start in starts]
    if rows:
        connection.executemany("INSERT INTO v VALUES (?, ?, ?)", rows)
    return connection


def _feasible(keys: Mapping[tuple[str, date], Sequence[date]], cuts: Sequence[date]) -> bool:
    """Whether boundaries ``cuts`` separate every two vintages of one observation."""
    for starts in keys.values():
        ordered = sorted(starts)
        for low, high in itertools.pairwise(ordered):
            if not any(low < cut <= high for cut in cuts):
                return False
    return True


def test_vintage_partitions_hold_each_observation_once() -> None:
    base = date(2020, 1, 1)
    days = [base + timedelta(days=offset) for offset in range(7)]
    cases: list[dict[tuple[str, date], list[date]]] = [
        {("A", base): [days[0], days[2], days[5]], ("B", base): [days[1], days[3]]},
        {("A", base): [days[0], days[6]], ("B", base): [days[1], days[2]], ("C", base): [days[4]]},
        {("A", base): [days[i] for i in range(7)]},
        {("A", base): [days[3]]},
    ]
    for keys in cases:
        partitions = vintage_partitions(_vintage_table(keys), "v")
        starts = sorted({start for values in keys.values() for start in values})
        # The partitions tile [first start, last start + 1 day) in order.
        assert partitions[0][0] == starts[0]
        assert partitions[-1][1] == starts[-1] + timedelta(days=1)
        assert all(a[1] == b[0] for a, b in itertools.pairwise(partitions))
        for low, high in partitions:
            held = [key for key, values in keys.items() for v in values if low <= v < high]
            assert len(held) == len(set(held))
        cuts = [low for low, _ in partitions[1:]]
        assert _feasible(keys, cuts)
        # No smaller set of boundaries among the vintage starts separates every pair.
        fewest = next(
            size
            for size in range(len(starts) + 1)
            if any(_feasible(keys, chosen) for chosen in itertools.combinations(starts, size))
        )
        assert len(cuts) == fewest
    assert vintage_partitions(_vintage_table({}), "v") == []
    undated = _vintage_table({("A", base): [days[0]]})
    undated.execute("INSERT INTO v VALUES ('A', DATE '2020-02-01', NULL)")
    with pytest.raises(ValueError, match="no vintage start"):
        vintage_partitions(undated, "v")


# --- promotions --------------------------------------------------------------------------------

# Two observations of one series and one of another, with their vintages (start, value).
VINTAGES: Final = {
    ("UNRATE", date(2020, 1, 1)): [(date(2020, 2, 7), "3.6"), (date(2020, 3, 6), "3.5")],
    ("UNRATE", date(2020, 2, 1)): [
        (date(2020, 3, 6), "3.5"),
        (date(2020, 4, 3), "3.6"),
        (date(2021, 1, 8), "3.5"),
    ],
    ("GDP", date(2019, 10, 1)): [(date(2020, 1, 30), "21.7")],
}


def _alfred_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for (series, day), vintages in VINTAGES.items():
        ends = [start - timedelta(days=1) for start, _ in vintages[1:]] + [OPEN_END]
        rows.extend(
            (series, day, start, end, value, RETRIEVED)
            for (start, value), end in zip(vintages, ends, strict=True)
        )
    return rows


def _current(cutoff: int) -> dict[tuple[str, date], Decimal]:
    """The vintage each observation had at ``cutoff``, from the fixture alone."""
    current = {}
    for key, vintages in VINTAGES.items():
        known = [value for start, value in vintages if _day_end(start, CHICAGO) <= cutoff]
        if known:
            current[key] = Decimal(known[-1])
    return current


def test_alfred_vintages_are_superseding_revisions(ws: Workspace) -> None:
    pin = _source(ws, ALFRED, _alfred_rows(), tag="alfred")
    # Every vintage in one generation repeats the observations' natural keys.
    whole = promote(ws, *_alfred_spec(pin, parent=None, partition=None), apply=False)
    assert whole["duplicate_keys"] == 2
    assert any(
        "natural keys repeat" in str(item) for item in cast("list[object]", whole["refusals"])
    )
    partitions = vintage_partitions(ws.market, market_target(ws, pin["source_id"], pin["table"]))
    assert partitions == [
        (date(2020, 1, 30), date(2020, 3, 6)),
        (date(2020, 3, 6), date(2020, 4, 3)),
        (date(2020, 4, 3), date(2021, 1, 8)),
        (date(2021, 1, 8), date(2021, 1, 9)),
    ]
    parent = None
    operations = []
    for partition in partitions:
        result = _apply(ws, _alfred_spec(pin, parent=parent, partition=partition))
        assert result["refusals"] == []
        operations.append(result["operations"])
        parent = str(result["generation_id"])
    assert operations == [
        {"ASSERT": 2},
        {"ASSERT": 1, "SUPERSEDE": 1},
        {"SUPERSEDE": 1},
        {"SUPERSEDE": 1},
    ]
    assert parent is not None
    stored = ws.market.execute(
        "SELECT series_id, observation_period, op, value, source_vintage_start, "
        "source_vintage_end, available_at_us, revision_known_at_us FROM macro_observations "
        "ORDER BY series_id, observation_period, source_vintage_start"
    ).fetchall()
    expected = []
    for (series, day), vintages in sorted(VINTAGES.items()):
        for index, (start, value) in enumerate(vintages):
            known = _day_end(start, CHICAGO)
            op = "ASSERT" if index == 0 else "SUPERSEDE"
            expected.append((series, day, op, Decimal(value), start, None, known, known))
    assert stored == expected
    # Strict reads under the grant follow the vintage current at each cutoff.
    for moment in ("2020-03-06T12:00:00", "2020-03-07T12:00:00", "2020-12-31T00:00:00"):
        cutoff = _us(datetime.fromisoformat(moment).replace(tzinfo=UTC))
        heads = _read(ws, "macro_observations", parent, cutoff, GRANT)
        assert {
            (row["series_id"], row["observation_period"]): row["value"] for row in heads
        } == _current(cutoff)
    # Without the grant the rule's times are unknown, so strict reads select nothing.
    assert _read(ws, "macro_observations", parent, _us(RETRIEVED), ()) == []
    # Research reads (no cutoff) see the current vintage of every observation.
    latest = _read(ws, "macro_observations", parent, None, ())
    assert {(row["series_id"], row["observation_period"]): row["value"] for row in latest} == {
        key: Decimal(vintages[-1][1]) for key, vintages in VINTAGES.items()
    }
    # Promoting the same partition again changes nothing.
    again = _apply(ws, _alfred_spec(pin, parent=parent, partition=partitions[-1]))
    assert again["delta_rows"] == 0
    assert verify_workspace(ws)["verified"] is True


def market_target(workspace: Workspace, source_id: str, table: str) -> str:
    """The quoted source-library table that holds ``table`` of ``source_id``."""
    tables = source_library.list_tables(workspace, source_id)
    (found,) = [item for item in tables if item["name"] == table]
    return '"' + str(found["target"]) + '"'


def _closes_row(symbol: str, day: date, text: str) -> tuple[object, ...]:
    raw = json.dumps({"Close": text, "Date": day.isoformat()})
    return (symbol, 1, day, float(text), raw)


def test_fx_series_promote_one_pair_each(ws: Workspace) -> None:
    rows = [
        _closes_row("USDKRW", date(1991, 1, 2), "714.5"),
        _closes_row("USDKRW", date(1991, 1, 3), "714.55"),
        _closes_row("XAUUSD", date(1991, 1, 2), "386.2"),
    ]
    pin = _source(ws, CLOSES, rows, tag="closes")
    args = {"series": "USDKRW", "base": "USD", "quote": "KRW", "timezone": SEOUL}
    times = _rule("local_day_end@1", "record", "fixing_date", SEOUL)
    document = _spec(
        "fx_rates",
        "fx.usdkrw.norgate",
        [pin],
        "norgate.fx_closes@1",
        args,
        times,
        decimal={"rate": "decimal_text@1"},
    )
    result = _apply(ws, document)
    assert result["operations"] == {"ASSERT": 2}
    assert result["unselected_rows"] == 1
    # The rate is the export's text, so 714.55 needs no float flag.
    assert result["flags"] == {"time_precision_day": 2}
    stored = ws.market.execute(
        "SELECT base_currency, quote_currency, fixing_at_us, rate, value_state, available_at_us "
        "FROM fx_rates ORDER BY fixing_at_us"
    ).fetchall()
    assert stored == [
        ("USD", "KRW", _day_end(date(1991, 1, 2), SEOUL), Decimal("714.5"), "present",
         _day_end(date(1991, 1, 2), SEOUL)),
        ("USD", "KRW", _day_end(date(1991, 1, 3), SEOUL), Decimal("714.55"), "present",
         _day_end(date(1991, 1, 3), SEOUL)),
    ]  # fmt: skip
    # FX has no domain date column, so absence cannot be scoped and a tombstone is refused.
    tombstone = {
        "mode": "absent_in_full_snapshot",
        "source": {"source_id": pin["source_id"], "table": pin["table"]},
        "scope": {"instruments": None, "from": "1991-01-01", "to": "1992-01-01"},
    }
    raw, sha = _spec(
        "fx_rates",
        "fx.usdkrw.norgate",
        [pin],
        "norgate.fx_closes@1",
        args,
        times,
        decimal={"rate": "decimal_text@1"},
        tombstone=tombstone,
    )
    with pytest.raises(ValueError, match="no date column"):
        parse_spec(raw, sha)


def _private(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _korea_request(root: Path, name: str, source: str, rows: list[dict[str, object]]) -> None:
    body = json.dumps(rows).encode()
    request = {"end": None, "observation_date": "2026-09-06", "source_id": source, "start": None}
    raw_ref = {
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "relative_path": f"{name}/response.raw",
        "size_bytes": len(body),
    }
    _private(root / name / "request.json", json.dumps(request).encode())
    _private(root / name / "response.json", json.dumps({"raw": raw_ref}).encode())
    _private(root / name / "response.raw", body)
    normalized = {
        "raw": raw_ref,
        "request": request,
        "row_count": len(rows),
        "rows": rows,
        "schema_version": 1,
    }
    _private(root / name / "normalized.v1.json", json.dumps(normalized).encode())


def _observation(series: str, period: str, value: str, units: str, base: str | None) -> dict:
    return {
        "base_period": base,
        "period": period,
        "regime": None,
        "series_id": series,
        "status": None,
        "unit_multiplier": None,
        "units": units,
        "value": value,
        "value_raw": value,
    }


def test_legacy_fred_and_kr_public_sources_promote(tmp_path: Path, ws: Workspace) -> None:
    legacy = tmp_path / "legacy"
    fred = _private(
        legacy / "DEXKOUS.csv",
        b"observation_date,DEXKOUS\n2011-10-03,1180.00\n2011-10-04,\n2011-10-05,1197.50\n",
    )
    korea = legacy / "korea"
    _korea_request(
        korea,
        "bok",
        "bok-policy",
        [_observation("KOR_POLICY_BOK", "2008-03-07", "5.00", "percent", None)],
    )
    _korea_request(
        korea,
        "oecd",
        "oecd-cpi",
        [
            _observation("KOR_CPI_OECD", "2026-06", "125.9", "index", "2015"),
            _observation("KOR_CPI_OECD", "2026-07", "126.2585", "index", "2015"),
        ],
    )
    entries = [
        {"name": name, "loader": loader, "path": str(path), "args": {}, "expect": {}}
        for name, loader, path in (
            ("fred", "fred.series_csv@1", fred),
            ("korea", "korea.public_response@1", korea),
        )
    ]
    raw = json.dumps({"schema_version": "aas-legacy-import-v1", "entries": entries}).encode()
    report = apply_import(ws, parse_manifest(raw, hashlib.sha256(raw).hexdigest()))
    assert report["reconciled"] is True
    pins = {
        str(source["source_id"]).rsplit("-", 1)[0]: {
            "source_id": str(source["source_id"]),
            "source_sha256": str(source["source_id"]).rsplit("-", 1)[1],
            "table": str(source["table"]),
            "digest": str(source["digest"]),
        }
        for entry in cast("list[dict[str, object]]", report["entries"])
        for source in cast("list[dict[str, object]]", entry["sources"])
    }
    ny = "America/New_York"
    fx = _spec(
        "fx_rates",
        "fx.usdkrw.fred",
        [pins["fred-series-csv"]],
        "fred.fx_series@1",
        {"series": "DEXKOUS", "base": "USD", "quote": "KRW", "timezone": ny},
        _rule("local_day_end@1", "record", "fixing_date", ny),
        decimal={"rate": "decimal_text@1"},
    )
    result = _apply(ws, fx)
    assert result["operations"] == {"ASSERT": 3}
    rates = ws.market.execute(
        "SELECT fixing_at_us, rate, value_state FROM fx_rates ORDER BY fixing_at_us"
    ).fetchall()
    assert rates == [
        (_day_end(date(2011, 10, 3), ny), Decimal("1180.00"), "present"),
        (_day_end(date(2011, 10, 4), ny), None, "missing"),
        (_day_end(date(2011, 10, 5), ny), Decimal("1197.50"), "present"),
    ]
    # The CSV's dates are text, so a partition is refused instead of cast.
    raw_fx, sha_fx = _spec(
        "fx_rates",
        "fx.usdkrw.fred",
        [pins["fred-series-csv"]],
        "fred.fx_series@1",
        {"series": "DEXKOUS", "base": "USD", "quote": "KRW", "timezone": ny},
        _rule("local_day_end@1", "record", "fixing_date", ny),
        decimal={"rate": "decimal_text@1"},
        parent=str(result["generation_id"]),
        partition=(date(2011, 1, 1), date(2012, 1, 1)),
    )
    with pytest.raises(ValueError, match="without a partition"):
        promote(ws, raw_fx, sha_fx, apply=False)
    unknown = _rule("unknown_null@1", "record", None, None)
    for provider, dataset, count in (("bok", "macro.kr.bok", 1), ("oecd", "macro.kr.oecd", 2)):
        document = _spec(
            "macro_observations",
            dataset,
            [pins[f"{provider}-observations"]],
            f"{provider}.observations@1",
            {},
            unknown,
            decimal={"value": "decimal_text@1"},
        )
        assert _apply(ws, document)["operations"] == {"ASSERT": count}
    stored = ws.market.execute(
        "SELECT series_id, observation_period, unit, value, available_at_us, "
        "revision_known_at_us FROM macro_observations ORDER BY series_id, observation_period"
    ).fetchall()
    assert stored == [
        ("KOR_CPI_OECD", date(2026, 6, 1), "index;base=2015", Decimal("125.9"), None, None),
        ("KOR_CPI_OECD", date(2026, 7, 1), "index;base=2015", Decimal("126.2585"), None, None),
        ("KOR_POLICY_BOK", date(2008, 3, 7), "percent", Decimal("5.00"), None, None),
    ]
    assert verify_workspace(ws)["verified"] is True
