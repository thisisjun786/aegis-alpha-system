# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""US prices: Norgate canonical and reference mappers, EODHD cross-checks and frozen FMP.

Every source row is synthetic. Expected instants are written out in UTC from the New York
offset of their date, independent of the mappers' SQL.
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.storage.identity import (
    decode_registry,
    mint_instrument,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.kr_identity import UNBOUNDED
from aegis_alpha.storage.legacy_import.norgate import HISTORY
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers import REGISTRY, IdentityKey, mapper
from aegis_alpha.storage.us_identity import build_from_workspace, export_sources
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import add_source, at, bar
from tests.storage.us_identity_support import (
    FMP_SCHEMA,
    MASTER_SCHEMA,
    commit,
    cusip,
    master,
    profile,
    table,
)

NEW_YORK = "America/New_York"
HOUR = 3_600 * 10**6
DAY = 24 * HOUR
FIVE = ("open", "high", "low", "close", "volume")


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _edt_start(day: date) -> int:
    """The New York start of a summer ``day``: 04:00 UTC under daylight time (UTC-4)."""
    return _us(datetime(day.year, day.month, day.day, 4, tzinfo=UTC))


def _binary32(value: float) -> float:
    """``value`` stored as an IEEE binary32 and read back."""
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _instrument(assetid: int) -> str:
    return mint_instrument("norgate_assetid", str(assetid))


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


def _source(connection: duckdb.DuckDBPyConnection, columns: str, rows: list[tuple]) -> None:
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        f"{columns})"
    )
    marks = ", ".join("?" * len(rows[0]))
    connection.executemany(
        f"INSERT INTO src VALUES (0, ?, 'h', {marks})",
        [(index, *row) for index, row in enumerate(rows)],
    )


def _mapped(
    connection: duckdb.DuckDBPyConnection, name: str, args: dict[str, object], columns: str
) -> list[tuple[Any, ...]]:
    found = mapper(name)
    found.check_args(args)
    return connection.execute(
        f"SELECT {columns} FROM ({found.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()


def test_us_price_mappers_are_registered() -> None:
    assert {
        "norgate.prices_none@1",
        "norgate.prices_adjusted@1",
        "norgate.reference_closes@1",
        "norgate.reference_history@1",
        "fmp.eod_non_split@1",
    } <= set(REGISTRY)
    norgate = IdentityKey("norgate", "norgate_assetid")
    for name in ("prices_none", "prices_adjusted", "reference_closes", "reference_history"):
        assert mapper(f"norgate.{name}@1").identity({"timezone": NEW_YORK}) == norgate
    fmp = {"timezone": NEW_YORK, "revision": 1}
    assert mapper("fmp.eod_non_split@1").identity(fmp) == IdentityKey("fmp", "fmp_symbol")
    for bad in ({}, {"timezone": "Not/AZone"}, {"timezone": NEW_YORK, "extra": 1}):
        with pytest.raises(ValueError, match=r"norgate\.prices_none@1"):
            mapper("norgate.prices_none@1").check_args(bad)
    for revision in (0, True, "1"):
        with pytest.raises(ValueError, match="revision"):
            mapper("fmp.eod_non_split@1").check_args({"timezone": NEW_YORK, "revision": revision})


def test_norgate_prices_none_maps_export_text() -> None:
    connection = duckdb.connect()
    _source(
        connection,
        "assetid BIGINT, database VARCHAR, date VARCHAR, open VARCHAR, high VARCHAR, "
        "low VARCHAR, close VARCHAR, volume VARCHAR",
        [
            (1, "US Equities", "2026-09-08", "504.05", "505", "498.1", "499.23", "1.6357e+06"),
            (1, "US Equities Delisted", "2026-09-09", "", "", "", "", ""),
            (1, "US Equities", "2026-09-10", "10", "", "9", "10", "100"),
            (1, "US Equities", "2026-09-11", "10", "11", "9", "-10", "100"),
            (1, "US Indices", "2026-09-08", "10", "11", "9", "10", "100"),
            (1, "US Equities", "2026-9-8", "10", "11", "9", "10", "100"),
            (1, "US Equities", "2026-02-30", "10", "11", "9", "10", "100"),
        ],
    )
    rows = _mapped(
        connection,
        "norgate.prices_none@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, _aas_id_at_us, _aas_ingested_at_us, session_date, interval, "
        "bar_end_us, basis, currency, price_role, value_state, open, high, low, close, volume, "
        "_aas_t_session_date",
    )
    start = _edt_start(date(2026, 9, 8))
    # The values stay the export's text for decimal_text@1; nothing is parsed or rounded.
    assert rows[0] == (
        "1",
        start,
        None,
        date(2026, 9, 8),
        "1d",
        start + DAY - 1,
        "unadjusted",
        "USD",
        "canonical",
        "present",
        "504.05",
        "505",
        "498.1",
        "499.23",
        "1.6357e+06",
        date(2026, 9, 8),
    )
    # Empty texts are missing; a partial or signed bar is invalid and keeps nothing.
    assert [(row[9], row[13]) for row in rows[1:4]] == [
        ("missing", None),
        ("invalid", None),
        ("invalid", None),
    ]
    # Another database and a date that is not a real YYYY-MM-DD have no session date.
    assert [row[3] for row in rows[4:]] == [None, None, None]
    partition = mapper("norgate.prices_none@1").partition_sql
    assert connection.execute(
        f"SELECT ({partition}) FROM src ORDER BY _aas_ordinal"
    ).fetchall() == [
        (date(2026, 9, 8),),
        (date(2026, 9, 9),),
        (date(2026, 9, 10),),
        (date(2026, 9, 11),),
        (date(2026, 9, 8),),
        (None,),
        (None,),
    ]


def test_norgate_prices_adjusted_maps_binary32_parts() -> None:
    connection = duckdb.connect()
    midnight = datetime(2020, 8, 28)  # noqa: DTZ001 -- Norgate parts store naive dates
    _source(
        connection,
        "assetid BIGINT, date TIMESTAMP_NS, open FLOAT, high FLOAT, low FLOAT, close FLOAT, "
        "volume FLOAT, adjustment_type VARCHAR",
        [
            (7, midnight, 126.0, 128.8, 123.9, 124.8, 2.0e8, "CAPITAL"),
            (7, midnight, 125.0, 127.0, 122.0, 123.5, 2.0e8, "TOTALRETURN"),
            (7, midnight, 126.0, 128.8, 123.9, 124.8, 2.0e8, "NONE"),
            (7, midnight + timedelta(hours=12), 1.0, 1.0, 1.0, 1.0, 1.0, "CAPITAL"),
            (7, midnight, float("nan"), 1.0, 1.0, 1.0, 1.0, "CAPITAL"),
            (7, midnight, None, None, None, None, None, "CAPITAL"),
        ],
    )
    rows = _mapped(
        connection,
        "norgate.prices_adjusted@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, session_date, basis, price_role, value_state, close, typeof(close)",
    )
    assert rows[:2] == [
        ("7", date(2020, 8, 28), "split_adjusted", "reference", "present", _binary32(124.8),
         "FLOAT"),
        ("7", date(2020, 8, 28), "total_return", "reference", "present", 123.5, "FLOAT"),
    ]  # fmt: skip
    # An unknown adjustment has no basis; a time of day has no session date.
    assert (rows[2][2], rows[3][1]) == (None, None)
    assert [(row[4], row[5]) for row in rows[4:]] == [("invalid", None), ("missing", None)]


def test_norgate_reference_series_are_close_only() -> None:
    connection = duckdb.connect()

    def export(day: str, close: str) -> str:
        return json.dumps({"Date": day, "Close": close})

    _source(
        connection,
        "assetid BIGINT, date DATE, close DOUBLE, raw_row_json VARCHAR",
        [
            (40, date(2026, 9, 8), 6502.08, export("2026-09-08", "6502.08")),
            (40, date(2026, 9, 9), 6502.08, export("2026-09-09", "6502.07")),
            (40, date(2026, 9, 10), 6502.08, export("2026-09-11", "6502.08")),
            (40, date(2026, 9, 11), None, json.dumps({"Date": "2026-09-11"})),
        ],
    )
    rows = _mapped(
        connection,
        "norgate.reference_closes@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, session_date, basis, currency, price_role, value_state, open, high, "
        'low, close, volume, "fields"',
    )
    assert rows[0] == (
        "40",
        date(2026, 9, 8),
        "unadjusted",
        "XXX",
        "reference",
        "present",
        None,
        None,
        None,
        "6502.08",
        None,
        "close",
    )
    # A close text that disagrees with the stored double or its own date is invalid.
    assert [(row[5], row[9]) for row in rows[1:]] == [
        ("invalid", None),
        ("invalid", None),
        ("missing", None),
    ]
    history = duckdb.connect()
    _source(
        history,
        "assetid BIGINT, database VARCHAR, date VARCHAR, close VARCHAR",
        [
            (41, "US Indices", "1885-02-16", "30.5"),
            (41, "US Indices", "2026-09-08", "6502.08"),
            (42, "US Equities", "2026-09-08", "10"),
        ],
    )
    mapped = _mapped(
        history,
        "norgate.reference_history@1",
        {"timezone": NEW_YORK},
        'session_date, value_state, close, "fields", _aas_t_session_date',
    )
    # A series before 1970 keeps its date; its time input is floored at the epoch.
    assert mapped == [
        (date(1885, 2, 16), "present", "30.5", "close", date(1970, 1, 1)),
        (date(2026, 9, 8), "present", "6502.08", "close", date(2026, 9, 8)),
        (None, "present", "10", "close", None),
    ]


def test_fmp_revisions_select_the_first_response_of_each_run() -> None:
    connection = duckdb.connect()
    first, second, third, fourth = (
        datetime(2026, 8, 29, hour, tzinfo=UTC) for hour in (1, 2, 3, 4)
    )
    day = date(2026, 8, 28)
    _source(
        connection,
        "symbol VARCHAR, date DATE, adjOpen DOUBLE, adjHigh DOUBLE, adjLow DOUBLE, "
        "adjClose DOUBLE, volume BIGINT, retrieved_at_utc TIMESTAMPTZ",
        [
            # Stored out of collection order: A, then A again, then B, then A once more.
            ("AAA", day, 10.0, 11.0, 9.0, 10.5, 100, third),
            ("AAA", day, 10.0, 11.0, 9.0, 10.0, 100, first),
            ("AAA", day, 10.0, 11.0, 9.0, 10.0, 100, second),
            ("AAA", day, 10.0, 11.0, 9.0, 10.0, 100, fourth),
            # Two different answers collected at one instant are both revision 1.
            ("TIE", day, 1.0, 1.0, 1.0, 1.0, 1, first),
            ("TIE", day, 2.0, 2.0, 2.0, 2.0, 2, first),
        ],
    )
    columns = "_aas_ordinal, _aas_id_token, _aas_ingested_at_us, basis, price_role, close"
    revisions = [
        _mapped(
            connection,
            "fmp.eod_non_split@1",
            {"timezone": NEW_YORK, "revision": revision},
            columns,
        )
        for revision in (1, 2, 3, 4)
    ]
    assert revisions[0] == [
        (1, "AAA", _us(first), "unadjusted", "reference", 10.0),
        (4, "TIE", _us(first), "unadjusted", "reference", 1.0),
        (5, "TIE", _us(first), "unadjusted", "reference", 2.0),
    ]
    assert revisions[1] == [(0, "AAA", _us(third), "unadjusted", "reference", 10.5)]
    assert revisions[2] == [(3, "AAA", _us(fourth), "unadjusted", "reference", 10.0)]
    assert revisions[3] == []


def test_nanosecond_timestamps_hash_alike_in_sql_and_python() -> None:
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t (date TIMESTAMP_NS, note VARCHAR)")
    connection.execute(
        "INSERT INTO t VALUES (TIMESTAMP_NS '2025-09-08 00:00:00', 'plain'), "
        "(TIMESTAMP_NS '1969-12-31 23:59:59.999999999', 'quote\"d'), (NULL, NULL)"
    )
    columns = [("date", "TIMESTAMP_NS"), ("note", "VARCHAR")]
    expression, plain, _ = formats.source_row_hash_sql(columns)
    found = connection.execute(
        f"SELECT CASE WHEN {plain} THEN {expression} END, "
        f"{', '.join(formats.source_row_fragments_sql(columns))} FROM t ORDER BY note NULLS LAST"
    ).fetchall()
    nanos = [1_757_289_600_000_000_000, -1, None]
    notes = ["plain", 'quote"d', None]
    expected = [
        formats.source_row_hash(
            [("date", None if value is None else formats.LocalNanoseconds(value)), ("note", note)]
        )
        for value, note in zip(nanos, notes, strict=True)
    ]
    # Independent of the codec: the canonical JSON bytes written out by hand.
    assert (
        expected[0]
        == hashlib.sha256(
            b'["aas-source-row-v1",[["date",{"local_ns":1757289600000000000}],["note","plain"]]]'
        ).hexdigest()
    )
    assert found[0][0] == expected[0]
    assert found[2][0] == expected[2]
    # Text JSON escapes is hashed from the SQL fragments in Python.
    assert found[1][0] is None
    assert formats.source_row_hash_from_fragments(columns, found[1][1:]) == expected[1]


# --- end to end -----------------------------------------------------------------------------


def history(  # noqa: PLR0913 -- one export row spells its columns
    assetid: int,
    symbol: str,
    day: str,
    close: str,
    *,
    database: str = "US Equities",
    volume: str = "1000",
) -> dict[str, object]:
    """One ``norgate.history_export@1`` row: every value the CSV text."""
    return {
        "assetid": assetid,
        "symbol": symbol,
        "database": database,
        "security_name": f"Synthetic {symbol}",
        "csv_sha256": hashlib.sha256(symbol.encode()).hexdigest(),
        "date": day,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "turnover": "",
        "unadjusted_close": close,
        "dividend": "0",
        "delivery_month": None,
        "open_interest": None,
    }


def _export(ws: Workspace, rows: list[dict[str, object]]) -> dict[str, str]:
    source_id = commit(ws, "history-csv", "bars", table(rows, HISTORY.schema()), provider="norgate")
    return _pin(ws, source_id, "bars")


def _pin(ws: Workspace, source_id: str, name: str) -> dict[str, str]:
    from aegis_alpha.storage.source_library import list_sources, list_tables  # noqa: PLC0415

    (source,) = [row for row in list_sources(ws) if row["source_id"] == source_id]
    (described,) = [row for row in list_tables(ws, source_id) if row["name"] == name]
    return {
        "source_id": source_id,
        "source_sha256": str(source["sha256"]),
        "table": name,
        "digest": str(described["digest"]),
    }


def _spec(  # noqa: PLR0913 -- every spec field a test varies
    sources: list[dict[str, str]],
    identity: dict[str, str],
    *,
    mapper_name: str,
    dataset: str,
    decimals: dict[str, str],
    args: dict[str, object] | None = None,
    parent: str | None = None,
    quality: Sequence[Mapping[str, object]] | None = None,
) -> tuple[bytes, str]:
    rule = {
        "rule": "local_day_end@1",
        "basis": "record",
        "input": "session_date",
        "args": {"timezone": NEW_YORK},
    }
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "prices", "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": mapper_name, "args": args or {"timezone": NEW_YORK}},
        "partition": None,
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": decimals,
        "quality_rules": list(quality or []),
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _snapshot(ws: Workspace, name: str) -> dict[str, str]:
    report = snapshot_identities(ws.state, name, created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _generation_pin(dataset: str, applied: dict[str, object]) -> dict[str, str]:
    marker = cast("dict[str, object]", applied["marker"])
    return {
        "dataset_id": dataset,
        "version": str(marker["version"]),
        "generation_id": str(marker["generation_id"]),
        "chain_hash": str(marker["chain_hash"]),
        "manifest_hash": str(marker["request_hash"]),
    }


def _flags(ws: Workspace, generation_id: object) -> list[tuple[object, ...]]:
    """A generation's value flags; every row carries the day rule's ``time_precision_day``."""
    return ws.market.execute(
        "SELECT p.instrument_id, p.session_date, f.flag, f.detail FROM quality_flags f "
        "JOIN prices p USING (generation_id, record_id, revision_id) WHERE f.generation_id=? "
        "AND f.flag <> 'time_precision_day' ORDER BY 1, 2, 3",
        [generation_id],
    ).fetchall()


def test_us_prices_promote_through_the_export_registry(ws: Workspace) -> None:
    master_id = commit(
        ws,
        "norgate-master",
        "observations",
        table([master(1, "AAA", last_date="2026-07-28")], MASTER_SCHEMA),
    )
    equities = _export(
        ws,
        [
            history(1, "AAA", "2026-07-28", "10", volume="1.6357e+06"),
            history(1, "AAA", "2026-09-08", "10.5"),
            # A listing the master does not know: minted from its asset ID.
            history(2, "NEW", "2026-08-20", "20"),
            history(2, "NEW", "2026-09-08", "11"),
        ],
    )
    indices = _export(
        ws,
        [
            history(40, "$IDX", "1885-02-16", "30.5", database="US Indices", volume=""),
            history(40, "$IDX", "2026-09-08", "6502.08", database="US Indices", volume=""),
            history(40, "$IDX", "2026-09-09", "6510", database="US Indices", volume=""),
        ],
    )
    assert export_sources(ws) == sorted([equities["source_id"], indices["source_id"]])
    registry = build_from_workspace(ws, master=master_id, exports=export_sources(ws))
    register_identities(
        ws.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    identity = _snapshot(ws, "us")
    text = dict.fromkeys(FIVE, "decimal_text@1")
    norgate = promote(
        ws,
        *_spec(
            [equities],
            identity,
            mapper_name="norgate.prices_none@1",
            dataset="prices.us.norgate",
            decimals=text,
        ),
        apply=True,
    )
    assert norgate["rows"] == {"ok": 4}
    stored = ws.market.execute(
        "SELECT instrument_id, session_date, price_role, value_state, close, volume, "
        "available_at_us FROM prices WHERE generation_id=? ORDER BY 1, 2",
        [norgate["generation_id"]],
    ).fetchall()
    aaa, new = _instrument(1), _instrument(2)
    day_end = _edt_start(date(2026, 9, 9)) - 1
    assert sorted(stored) == sorted(
        [
            (aaa, date(2026, 7, 28), "canonical", "present", 10, 1635700,
             _edt_start(date(2026, 7, 29)) - 1),
            (aaa, date(2026, 9, 8), "canonical", "present", 10.5, 1000, day_end),
            (new, date(2026, 8, 20), "canonical", "present", 20, 1000,
             _edt_start(date(2026, 8, 21)) - 1),
            (new, date(2026, 9, 8), "canonical", "present", 11, 1000, day_end),
        ]
    )  # fmt: skip
    # The truncated export volume keeps its value and says so.
    assert _flags(ws, norgate["generation_id"]) == [
        (aaa, date(2026, 7, 28), "volume_precision_limited", "volume")
    ]
    # EODHD bars after the master's last session resolve through the export's window;
    # each close is judged against Norgate's canonical close of the same bar.
    late = at("2026-09-10T00:00:00")
    eodhd_pin = add_source(
        ws,
        [
            bar("AAA.US", date(2026, 9, 8), 10.5, retrieved=late, currency="USD"),
            bar("NEW.US", date(2026, 9, 8), 12.0, retrieved=late, currency="USD"),
            # After the export's last session nothing says who holds the ticker.
            bar("AAA.US", date(2026, 9, 9), 10.75, retrieved=late, currency="USD"),
        ],
        tag="us",
    )
    rule = {
        "rule": "cross_provider_mismatch@1",
        "args": {
            "reference": _generation_pin("prices.us.norgate", norgate),
            "column": "close",
            "tolerance": "0.001",
        },
    }
    eodhd = promote(
        ws,
        *_spec(
            [eodhd_pin],
            identity,
            mapper_name="eodhd.bars@1",
            dataset="prices.us.eodhd",
            decimals=dict.fromkeys(FIVE, "float_shortest@1"),
            quality=[rule],
        ),
        apply=True,
    )
    assert eodhd["rows"] == {"ok": 2, "unresolved": 1}
    assert eodhd["unresolved_tokens"] == ["AAA.US"]
    assert _flags(ws, eodhd["generation_id"]) == [
        (new, date(2026, 9, 8), "cross_provider_mismatch", "close")
    ]
    # Index levels are close-only reference prices of the instrument the export minted.
    reference = promote(
        ws,
        *_spec(
            [indices],
            identity,
            mapper_name="norgate.reference_history@1",
            dataset="prices.ref.norgate",
            decimals={"close": "decimal_text@1"},
        ),
        apply=True,
    )
    assert reference["rows"] == {"ok": 3}
    assert ws.market.execute(
        'SELECT DISTINCT instrument_id, "fields", price_role, currency FROM prices '
        "WHERE generation_id=?",
        [reference["generation_id"]],
    ).fetchall() == [(_instrument(40), "close", "reference", "XXX")]


def test_norgate_adjusted_parts_promote_with_float_storage_flags(ws: Workspace) -> None:
    schema = pa.schema(
        [
            ("assetid", pa.int64()),
            ("date", pa.timestamp("ns")),
            *((name, pa.float32()) for name in FIVE),
            ("adjustment_type", pa.string()),
        ]
    )
    midnight = datetime(2020, 8, 28)  # noqa: DTZ001 -- Norgate parts store naive dates
    rows = [
        {"assetid": 1, "date": midnight, "open": 126.0, "high": 128.0, "low": 124.0,
         "close": 127.1425, "volume": 2.0e8, "adjustment_type": "CAPITAL"},
        {"assetid": 1, "date": midnight, "open": 1.5, "high": 1.5, "low": 1.5,
         "close": 1.5, "volume": 10.0, "adjustment_type": "TOTALRETURN"},
    ]  # fmt: skip
    part = commit(ws, "norgate-adjusted-part", "observations", pa.Table.from_pylist(rows, schema))
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [
            {
                "anchor_namespace": "norgate_assetid",
                "anchor_token": "1",
                "issuer": None,
                "asset_type": "unclassified",
                "venue": "XNYS",
            }
        ],
        "assertions": [
            {
                "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": "1"},
                "provider": "norgate",
                "namespace": "norgate_assetid",
                "token": "1",
                "valid_from_us": UNBOUNDED,
                "valid_to_us": None,
                "known_from_us": 1,
                "supersedes_assertion_id": None,
                "source_snapshot_id": "sl:" + part,
                "source_hash": hashlib.sha256(b"1").hexdigest(),
            }
        ],
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    applied = promote(
        ws,
        *_spec(
            [_pin(ws, part, "observations")],
            _snapshot(ws, "norgate"),
            mapper_name="norgate.prices_adjusted@1",
            dataset="prices.us.norgate.ref",
            decimals=dict.fromkeys(FIVE, "float_shortest@1"),
        ),
        apply=True,
    )
    assert applied["rows"] == {"ok": 2}
    stored = ws.market.execute(
        "SELECT basis, close FROM prices WHERE generation_id=? ORDER BY basis",
        [applied["generation_id"]],
    ).fetchall()
    # binary32 127.1425 is 127.14250183105469; its shortest round trip is 127.1425.
    assert [(basis, str(close)) for basis, close in stored] == [
        ("split_adjusted", "127.142500000000"),
        ("total_return", "1.500000000000"),
    ]
    assert _flags(ws, applied["generation_id"]) == [
        (_instrument(1), date(2020, 8, 28), "provider_float_storage", "close")
    ]


def test_fmp_corrections_promote_as_later_generations(ws: Workspace) -> None:
    master_id = commit(
        ws,
        "norgate-master",
        "observations",
        table([master(1, "AAA", last_date="2026-07-28")], MASTER_SCHEMA),
    )
    _export(ws, [history(1, "AAA", "2026-07-28", "10"), history(1, "AAA", "2026-09-08", "10")])
    profiles = commit(
        ws,
        "fmp-profiles",
        "observations",
        table([profile("AAA", "0000000101", cusip("03783310"))], FMP_SCHEMA),
    )
    registry = build_from_workspace(
        ws, master=master_id, fmp=[profiles], exports=export_sources(ws)
    )
    register_identities(
        ws.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    identity = _snapshot(ws, "us")
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("date", pa.date32()),
            *((name, pa.float64()) for name in ("adjOpen", "adjHigh", "adjLow", "adjClose")),
            ("volume", pa.int64()),
            ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
        ]
    )
    first, corrected = at("2026-08-29T09:00:00"), at("2026-08-30T09:00:00")

    def response(close: float, retrieved: datetime) -> dict[str, object]:
        return {
            "symbol": "AAA",
            "date": date(2026, 8, 28),
            "adjOpen": close,
            "adjHigh": close,
            "adjLow": close,
            "adjClose": close,
            "volume": 100,
            "retrieved_at_utc": retrieved,
        }

    responses = commit(
        ws,
        "fmp-non-split",
        "bars",
        pa.Table.from_pylist([response(10.0, first), response(10.25, corrected)], schema),
    )
    pin = _pin(ws, responses, "bars")
    decimals = {**dict.fromkeys(FIVE[:4], "float_shortest@1"), "volume": "exact@1"}
    parent = None
    published = []
    for revision in (1, 2):
        applied = promote(
            ws,
            *_spec(
                [pin],
                identity,
                mapper_name="fmp.eod_non_split@1",
                dataset="prices.us.fmp.ref",
                decimals=decimals,
                args={"timezone": NEW_YORK, "revision": revision},
                parent=parent,
            ),
            apply=True,
        )
        parent = str(applied["generation_id"])
        published.extend(
            ws.market.execute(
                "SELECT instrument_id, op, price_role, close, revision_known_at_us, "
                "ingested_at_us FROM prices WHERE generation_id=?",
                [parent],
            ).fetchall()
        )
    end = _edt_start(date(2026, 8, 29)) - 1
    # The first answer is known from its session's end (it was collected later); the
    # correction is known only from when it was collected.
    assert published == [
        (_instrument(1), "ASSERT", "reference", 10, end, _us(first)),
        (_instrument(1), "SUPERSEDE", "reference", 10.25, _us(corrected), _us(corrected)),
    ]


def test_exports_extend_ticker_claims_past_the_master(ws: Workspace) -> None:
    master_id = commit(
        ws,
        "norgate-master",
        "observations",
        table(
            [master(1, "AAA", last_date="2026-07-28"), master(3, "CCC", last_date="2026-07-28")],
            MASTER_SCHEMA,
        ),
    )
    delisted = "US Equities Delisted"
    _export(
        ws,
        [
            history(1, "AAA", "2026-07-27", "10"),
            history(1, "AAA", "2026-09-08", "10"),
            history(10, "NEW", "2026-08-20", "10"),
            history(10, "NEW", "2026-09-08", "10"),
            # The master gave CCC to asset 3, and the export does not show it leaving.
            history(11, "CCC", "2026-08-03", "10"),
            history(12, "DUP", "2026-08-03", "10"),
            history(13, "DUP", "2026-08-04", "10"),
            # HELD named a delisted security until 2026-08-10.
            history(14, "HELD", "2026-08-01", "10"),
            history(15, "HELD-202608", "2026-08-10", "10", database=delisted),
            history(20, "$IDX", "2026-09-08", "6502.08", database="US Indices"),
        ],
    )
    retrieved = datetime(2026, 8, 29, 8, 37, tzinfo=UTC)
    profiles = commit(
        ws,
        "fmp-profiles",
        "observations",
        table([profile("AAA", None, cusip("03783310"), retrieved=retrieved)], FMP_SCHEMA),
    )
    registry = build_from_workspace(
        ws, master=master_id, fmp=[profiles], exports=export_sources(ws)
    )
    assert (registry.through, registry.export_through) == (date(2026, 7, 28), date(2026, 9, 8))
    assert registry.unresolved["exports"] == {
        "export_ticker_ambiguous": ["DUP"],
        "export_ticker_moved": ["CCC"],
    }
    instruments = {
        row["anchor_token"]: (row["asset_type"], row["venue"])
        for row in cast("list[dict[str, Any]]", registry.document["instruments"])
    }
    # Every series the master lacks is minted from its asset ID, never from its ticker.
    assert instruments == {
        "1": ("unclassified", "XNYS"),
        "3": ("unclassified", "XNYS"),
        **dict.fromkeys(("10", "11", "12", "13", "14", "15"), ("unclassified", "XNYS")),
        "20": ("index", "XXXX"),
    }
    claims: dict[tuple[str, str, str], list[tuple[str, int, int | None]]] = {}
    for row in cast("list[dict[str, Any]]", registry.document["assertions"]):
        key = (row["provider"], row["namespace"], row["token"])
        anchor = row["instrument"]["anchor_token"]
        claims.setdefault(key, []).append((anchor, row["valid_from_us"], row["valid_to_us"]))
    window_end = _edt_start(date(2026, 9, 9))
    master_end = _edt_start(date(2026, 7, 29))
    window = ("1", master_end, window_end)
    assert claims["eodhd", "eodhd_symbol", "AAA.US"][-1] == window
    assert claims["eodhd", "eodhd_symbol", "NEW.US"] == [("10", _edt_start(date(2026, 8, 20)),
                                                          window_end)]  # fmt: skip
    assert claims["eodhd", "eodhd_symbol", "HELD.US"] == [("14", _edt_start(date(2026, 8, 11)),
                                                           window_end)]  # fmt: skip
    assert claims["norgate", "norgate_symbol", "HELD-202608"] == [("15", UNBOUNDED, None)]
    assert claims["norgate", "norgate_symbol", "$IDX"] == [("20", UNBOUNDED, None)]
    assert ("eodhd", "eodhd_symbol", "CCC.US") in claims
    assert all(anchor == "3" for anchor, _, _ in claims["eodhd", "eodhd_symbol", "CCC.US"])
    assert ("eodhd", "eodhd_symbol", "DUP.US") not in claims
    # A profile retrieved inside the window is judged against the window's series.
    assert claims["fmp", "fmp_symbol", "AAA"] == [window]
    assert claims["fmp", "cusip", cusip("03783310")] == [("1", _us(retrieved), None)]
    assert registry.unresolved["fmp"] == {}
    resolved = registry.resolve(
        [
            ("AAA.US", date(2026, 9, 8), 1),
            ("AAA.US", date(2026, 9, 9), 2),
            ("NEW.US", date(2026, 8, 19), 3),
            ("CCC.US", date(2026, 9, 8), 4),
        ]
    )
    assert (resolved["resolved_rows"], resolved["unresolved_rows"]) == (
        1,
        {"after_export_through": 2, "after_master_through": 4, "before_ticker_claim": 3},
    )
    registered = register_identities(
        ws.state,
        decode_registry(registry.raw(), expected_file_sha256=registry.sha256()),
        apply=True,
    )
    assert not registered["conflicts"]
    assert not any(cast("dict[str, list[object]]", registered["missing"]).values())
