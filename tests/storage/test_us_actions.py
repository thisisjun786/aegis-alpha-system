# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""US corporate actions and listing status: Norgate parts and master, frozen FMP responses.

Every source row is synthetic. Prices are binary32 values chosen to be exact, so the
expected amounts and ratios are written out by hand rather than recomputed by the SQL.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.adjusted_prices import UNADJUSTABLE, adjust, load_adjusted_prices
from aegis_alpha.storage.identity import mint_instrument, parse_registry, register_identities
from aegis_alpha.storage.kr_identity import UNBOUNDED
from aegis_alpha.storage.market import publish_generation
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.mappers import REGISTRY, IdentityKey, mapper
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin, HeadQuery
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import publish_calendar
from tests.storage.us_identity_support import commit

NEW_YORK = "America/New_York"
HOUR = 3_600 * 10**6
DAY = 24 * HOUR
EXDATE = "exdate_open@1"


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _edt_start(day: date) -> int:
    """The New York start of a summer ``day``: 04:00 UTC under daylight time (UTC-4)."""
    return _us(datetime(day.year, day.month, day.day, 4, tzinfo=UTC))


def _midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day)  # noqa: DTZ001 -- Norgate parts store naive dates


def _part(
    assetid: int, day: date, close: float, unadjusted: float, dividend: float = 0.0
) -> dict[str, object]:
    return {
        "assetid": assetid,
        "date": _midnight(day),
        "close": close,
        "unadjusted_close": unadjusted,
        "dividend": dividend,
        "adjustment_type": "CAPITAL",
    }


PART_SCHEMA = pa.schema(
    [
        ("assetid", pa.int64()),
        ("date", pa.timestamp("ns")),
        ("close", pa.float32()),
        ("unadjusted_close", pa.float32()),
        ("dividend", pa.float32()),
        ("adjustment_type", pa.string()),
    ]
)
STATUS_SCHEMA = pa.schema(
    [
        ("assetid", pa.int64()),
        ("is_delisted", pa.bool_()),
        ("first_date", pa.string()),
        ("last_date", pa.string()),
    ]
)
FMP_DIVIDENDS = pa.schema(
    [
        ("symbol", pa.string()),
        ("date", pa.date32()),
        ("dividend", pa.float64()),
        ("recordDate", pa.date32()),
        ("paymentDate", pa.date32()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)
FMP_SPLITS = pa.schema(
    [
        ("symbol", pa.string()),
        ("date", pa.date32()),
        ("numerator", pa.float64()),
        ("denominator", pa.float64()),
        ("splitType", pa.string()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)
D = [date(2020, 8, day) for day in (5, 6, 7, 10, 11)]


def _arrow(rows: list[dict[str, object]], schema: pa.Schema) -> pa.Table:
    return pa.table({name: [row[name] for row in rows] for name in schema.names}, schema=schema)


def _source(connection: duckdb.DuckDBPyConnection, arrow: pa.Table) -> None:
    connection.execute("SET TimeZone='UTC'")
    connection.register("_arrow", arrow)
    connection.execute(
        "CREATE TABLE src AS SELECT 0::INTEGER AS _aas_pin, "
        "(row_number() OVER () - 1)::BIGINT AS _aas_ordinal, 'h' AS _aas_row_hash, * FROM _arrow"
    )


def _mapped(
    connection: duckdb.DuckDBPyConnection, name: str, args: dict[str, object], columns: str
) -> list[tuple[Any, ...]]:
    found = mapper(name)
    found.check_args(args)
    return connection.execute(
        f"SELECT {columns} FROM ({found.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()


def test_us_action_mappers_are_registered() -> None:
    norgate = IdentityKey("norgate", "norgate_assetid")
    zone = {"timezone": NEW_YORK}
    for name in ("norgate.dividends@1", "norgate.capital_adjustments@1"):
        assert name in REGISTRY
        assert mapper(name).domain == "corporate_actions"
        assert mapper(name).identity(zone) == norgate
        assert mapper(name).time_inputs == {"ex_date": "date"}
    assert mapper("norgate.status@1").identity(zone) == norgate
    for name in ("fmp.dividends@1", "fmp.splits@1"):
        assert mapper(name).identity(zone) == IdentityKey("fmp", "fmp_symbol")
        with pytest.raises(ValueError, match="revision"):
            mapper(name).check_args({"timezone": NEW_YORK, "revision": 0})
    for bad in ({**zone, "event": "halted"}, zone):
        with pytest.raises(ValueError, match=r"norgate\.status@1"):
            mapper("norgate.status@1").check_args(bad)


def test_norgate_dividends_are_paid_cash_on_the_next_session() -> None:
    connection = duckdb.connect()
    _source(
        connection,
        _arrow(
            [
                # Before a later 4:1 split the capital close is a quarter of the unadjusted
                # close, and the capital-basis dividend 0.25 was 1.00 paid per share.
                _part(7, D[0], 100.0, 400.0),
                _part(7, D[1], 105.0, 420.0, dividend=0.25),
                # A total return row never names a dividend of its own.
                {**_part(7, D[1], 99.0, 420.0, dividend=0.5), "adjustment_type": "TOTALRETURN"},
                _part(7, D[2], 104.0, 416.0, dividend=math.nan),
                _part(7, D[3], 103.0, 412.0),
                # The series' last row: no next session, so no ex-date.
                _part(7, D[4], 103.0, 412.0, dividend=0.5),
                _part(8, D[0], 50.0, 50.0, dividend=0.125),
                _part(8, D[2], 51.0, 51.0),
                # A negative dividend is malformed evidence, kept as invalid.
                _part(9, D[0], 10.0, 10.0, dividend=-0.5),
                _part(9, D[1], 10.0, 10.0),
                # A next row that is not at midnight gives no ex-date: the row is refused.
                _part(10, D[0], 10.0, 10.0, dividend=0.5),
                {**_part(10, D[1], 10.0, 10.0), "date": _midnight(D[1]) + timedelta(hours=1)},
            ],
            PART_SCHEMA,
        ),
    )
    rows = _mapped(
        connection,
        "norgate.dividends@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, _aas_id_at_us, _aas_ingested_at_us, action_id, action_type, ex_date, "
        "effective_date, amount, ratio, currency, value_state, _aas_t_ex_date",
    )
    assert rows == [
        ("7", _edt_start(D[2]), None, "dividend:2020-08-07", "dividend", D[2], D[2], 1.0, None,
         "USD", "present", D[2]),
        ("7", _edt_start(D[3]), None, "dividend:2020-08-10", "dividend", D[3], D[3], None, None,
         "USD", "invalid", D[3]),
        # The next session of the series, even across a day it does not trade.
        ("8", _edt_start(D[2]), None, "dividend:2020-08-07", "dividend", D[2], D[2], 0.125,
         None, "USD", "present", D[2]),
        ("9", _edt_start(D[1]), None, "dividend:2020-08-06", "dividend", D[1], D[1], None,
         None, "USD", "invalid", D[1]),
        ("10", None, None, None, "dividend", None, None, 0.5, None, "USD", "present", None),
    ]  # fmt: skip
    # A neighbouring row is part of the mapping, so no partition may cut a series.
    partition = mapper("norgate.dividends@1").partition_sql
    assert connection.execute(f"SELECT DISTINCT ({partition}) FROM src").fetchall() == [(None,)]


def test_norgate_capital_adjustments_step_the_price_factor() -> None:
    connection = duckdb.connect()
    noise = 100.0000076293945  # binary32 100.00001: a factor step of 7.6e-8, storage only
    _source(
        connection,
        _arrow(
            [
                _part(7, D[0], 100.0, 400.0),
                _part(7, D[1], 105.0, 420.0),
                _part(7, D[2], 106.0, 106.0),  # 4:1 split: the factor falls from 4 to 1
                _part(7, D[3], 0.0, 106.0),  # no factor; skipped
                _part(7, D[4], 100.0, noise),
                _part(8, D[0], 60.0, 90.0),
                _part(8, D[1], 61.0, 61.0),  # 3:2 split
                _part(10, D[0], 10.0, 20.0),
                # A step on a row that is not at midnight has no ex-date: the row is refused.
                {**_part(10, D[1], 10.0, 10.0), "date": _midnight(D[1]) + timedelta(hours=1)},
                # A factorless row between two factors hides the step's session.
                _part(11, D[0], 25.0, 100.0),
                _part(11, D[1], 0.0, 100.0),
                _part(11, D[2], 100.0, 100.0),
                {**_part(9, D[0], 10.0, 40.0), "adjustment_type": "TOTALRETURN"},
                {**_part(9, D[1], 10.0, 10.0), "adjustment_type": "TOTALRETURN"},
            ],
            PART_SCHEMA,
        ),
    )
    rows = _mapped(
        connection,
        "norgate.capital_adjustments@1",
        {"timezone": NEW_YORK},
        "_aas_id_token, action_id, action_type, ex_date, effective_date, amount, ratio, "
        "currency, value_state, _aas_t_ex_date",
    )
    assert rows == [
        ("7", "capital_adjustment:2020-08-07", "capital_adjustment", D[2], D[2], None, 4.0, None,
         "present", D[2]),
        ("8", "capital_adjustment:2020-08-06", "capital_adjustment", D[1], D[1], None, 1.5, None,
         "present", D[1]),
        ("10", None, "capital_adjustment", None, None, None, 2.0, None, "present", None),
        ("11", "capital_adjustment:2020-08-07", "capital_adjustment", D[2], D[2], None, None,
         None, "invalid", D[2]),
    ]  # fmt: skip


def test_norgate_status_reads_listing_and_delisting_from_the_master() -> None:
    connection = duckdb.connect()
    _source(
        connection,
        _arrow(
            [
                {"assetid": 1, "is_delisted": False, "first_date": "1990-01-02", "last_date": None},
                {"assetid": 2, "is_delisted": True, "first_date": "2001-02-05",
                 "last_date": "2010-05-28"},
                {"assetid": 3, "is_delisted": True, "first_date": "2001-2-5",
                 "last_date": "2010-5-28"},
                # A series that ends before it starts, and a delisting on the last date.
                {"assetid": 4, "is_delisted": True, "first_date": "2010-05-28",
                 "last_date": "2001-02-05"},
                {"assetid": 5, "is_delisted": True, "first_date": "2001-02-05",
                 "last_date": "9999-12-31"},
            ],
            STATUS_SCHEMA,
        ),
    )  # fmt: skip
    columns = (
        "_aas_id_token, _aas_id_at_us, status_event_id, effective_from_us, effective_to_us, "
        "status, reason, _aas_t_status_date"
    )
    jan = _us(datetime(1990, 1, 2, 5, tzinfo=UTC))  # EST
    feb = _us(datetime(2001, 2, 5, 5, tzinfo=UTC))
    assert _mapped(
        connection, "norgate.status@1", {"timezone": NEW_YORK, "event": "listed"}, columns
    ) == [
        ("1", jan, "norgate:listed", jan, None, "listed", "norgate_first_date", date(1990, 1, 2)),
        ("2", feb, "norgate:listed", feb, None, "listed", "norgate_first_date", date(2001, 2, 5)),
        ("3", None, "norgate:listed", None, None, "listed", "norgate_first_date", None),
        # No start refuses the row.
        ("4", _edt_start(date(2010, 5, 28)), "norgate:listed", None, None, "listed",
         "norgate_first_date", date(2010, 5, 28)),
        ("5", feb, "norgate:listed", feb, None, "listed", "norgate_first_date", date(2001, 2, 5)),
    ]  # fmt: skip
    # A delisting starts the day after the series' last session and is read from that date.
    assert _mapped(
        connection, "norgate.status@1", {"timezone": NEW_YORK, "event": "delisted"}, columns
    ) == [
        ("2", _edt_start(date(2010, 5, 28)), "norgate:delisted", _edt_start(date(2010, 5, 29)),
         None, "delisted", "norgate_last_date", date(2010, 5, 28)),
        ("3", None, "norgate:delisted", None, None, "delisted", "norgate_last_date", None),
        ("4", feb, "norgate:delisted", None, None, "delisted", "norgate_last_date",
         date(2001, 2, 5)),
        ("5", _us(datetime(9999, 12, 31, 5, tzinfo=UTC)), "norgate:delisted", None, None,
         "delisted", "norgate_last_date", date(9999, 12, 31)),
    ]  # fmt: skip


def _fmp(symbol: str, day: date, retrieved: int, **values: object) -> dict[str, object]:
    return {
        "symbol": symbol,
        "date": day,
        "retrieved_at_utc": datetime(2026, 8, retrieved, tzinfo=UTC),
        **values,
    }


def test_fmp_actions_select_each_run_and_never_a_tied_key() -> None:
    connection = duckdb.connect()
    pay = {"recordDate": date(2024, 2, 12), "paymentDate": date(2024, 2, 15)}
    _source(
        connection,
        _arrow(
            [
                _fmp("AAA", date(2024, 2, 9), 1, dividend=0.24, **pay),
                _fmp("AAA", date(2024, 2, 9), 2, dividend=0.24, **pay),
                _fmp("AAA", date(2024, 2, 9), 3, dividend=0.25, **pay),
                # Two dividends on one ex-date in one response: no revision selects the key.
                _fmp("BBB", date(2024, 3, 1), 1, dividend=0.5, **pay),
                _fmp("BBB", date(2024, 3, 1), 1, dividend=1.5, **pay),
                _fmp("BBB", date(2024, 3, 1), 2, dividend=2.0, **pay),
                _fmp("CCC", date(2024, 3, 1), 1, dividend=0.0, **pay),
            ],
            FMP_DIVIDENDS,
        ),
    )
    columns = (
        "_aas_id_token, _aas_ingested_at_us, action_id, action_type, ex_date, record_date, "
        "pay_date, amount, currency, value_state"
    )
    first = _mapped(connection, "fmp.dividends@1", {"timezone": NEW_YORK, "revision": 1}, columns)
    day = _us(datetime(2026, 8, 1, tzinfo=UTC))
    assert first == [
        ("AAA", day, "dividend:2024-02-09", "dividend", date(2024, 2, 9), date(2024, 2, 12),
         date(2024, 2, 15), 0.24, "USD", "present"),
        ("CCC", day, "dividend:2024-03-01", "dividend", date(2024, 3, 1), date(2024, 2, 12),
         date(2024, 2, 15), None, "USD", "invalid"),
    ]  # fmt: skip
    second = _mapped(connection, "fmp.dividends@1", {"timezone": NEW_YORK, "revision": 2}, columns)
    assert [(row[0], row[1], row[7]) for row in second] == [("AAA", day + 2 * DAY, 0.25)]
    connection.execute("DROP TABLE src")
    _source(
        connection,
        _arrow(
            [
                _fmp("AAA", date(2020, 8, 31), 1, numerator=4.0, denominator=1.0,
                     splitType="stock-split"),
                _fmp("BBB", date(2021, 1, 4), 1, numerator=2.0, denominator=3.0,
                     splitType="stock-dividend"),
                _fmp("CCC", date(2021, 1, 4), 1, numerator=1.0, denominator=0.0,
                     splitType="spin-off"),
                _fmp("DDD", date(2021, 1, 4), 1, numerator=1.0, denominator=10.0, splitType=None),
                _fmp("EEE", date(2021, 1, 4), 1, numerator=2.0, denominator=1.0, splitType=""),
                # A correction of the type keeps the action: the same key, superseded.
                _fmp("AAA", date(2020, 8, 31), 2, numerator=4.0, denominator=1.0,
                     splitType="stock-dividend"),
            ],
            FMP_SPLITS,
        ),
    )  # fmt: skip
    splits = _mapped(
        connection,
        "fmp.splits@1",
        {"timezone": NEW_YORK, "revision": 1},
        "_aas_id_token, action_id, action_type, ratio, amount, value_state",
    )
    assert splits == [
        ("AAA", "split:2020-08-31", "split", 4.0, None, "present"),
        ("BBB", "split:2021-01-04", "stock_dividend", 2.0 / 3.0, None, "present"),
        ("CCC", "split:2021-01-04", "spin_off", None, None, "invalid"),
        ("DDD", "split:2021-01-04", "unspecified_split", 0.1, None, "present"),
        ("EEE", "split:2021-01-04", "unspecified_split", 2.0, None, "present"),
    ]
    corrected = _mapped(
        connection, "fmp.splits@1", {"timezone": NEW_YORK, "revision": 2}, "action_id, action_type"
    )
    assert corrected == [("split:2020-08-31", "stock_dividend")]


@pytest.fixture
def ws(tmp_path: Path) -> Iterator[Workspace]:
    root = tmp_path / "aas"
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        yield workspace


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


def _register(
    ws: Workspace,
    source_id: str,
    assets: list[int],
    *,
    provider: str = "norgate",
    symbols: list[str] | None = None,
) -> dict[str, str]:
    """Anchor each asset ID and assert it for ``provider``: its asset ID, or its FMP symbol."""
    from aegis_alpha.storage.identity import snapshot_identities  # noqa: PLC0415

    anchors = [{"anchor_namespace": "norgate_assetid", "anchor_token": str(a)} for a in assets]
    namespace = "norgate_assetid" if symbols is None else "fmp_symbol"
    tokens = symbols or [str(a) for a in assets]
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [
            {**anchor, "issuer": None, "asset_type": "unclassified", "venue": "XNYS"}
            for anchor in anchors
        ],
        "assertions": [
            {
                "instrument": anchor,
                "provider": provider,
                "namespace": namespace,
                "token": token,
                "valid_from_us": UNBOUNDED,
                "valid_to_us": None,
                "known_from_us": 1,
                "supersedes_assertion_id": None,
                "source_snapshot_id": "sl:" + source_id,
                "source_hash": hashlib.sha256(token.encode()).hexdigest(),
            }
            for anchor, token in zip(anchors, tokens, strict=True)
        ],
    }
    register_identities(ws.state, parse_registry(document), apply=True)
    report = snapshot_identities(ws.state, provider, created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _spec(  # noqa: PLR0913 -- every spec field a test varies
    sources: list[dict[str, str]],
    identity: dict[str, str],
    calendar: dict[str, str],
    *,
    mapper_name: str,
    decimals: dict[str, str],
    parent: str | None = None,
    partition: dict[str, str] | None = None,
    dataset: str = "actions.us.norgate",
    args: dict[str, object] | None = None,
) -> tuple[bytes, str]:
    rule = {
        "rule": EXDATE,
        "basis": "record",
        "input": "ex_date",
        "args": {"calendar": calendar, "calendar_id": "XNYS", "venue": "XNYS"},
    }
    document = {
        "schema_version": "aas-promotion-v1",
        "target": {"domain": "corporate_actions", "dataset_id": dataset, "parent": parent},
        "sources": sources,
        "mapper": {"name": mapper_name, "args": args or {"timezone": NEW_YORK}},
        "partition": partition,
        "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
        "decimal_rule": decimals,
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": identity,
    }  # fmt: skip
    raw = json.dumps(document, sort_keys=True).encode()
    return raw, hashlib.sha256(raw).hexdigest()


def _publish_prices(ws: Workspace, assetid: int, closes: dict[date, str]) -> GenerationPin:
    """A catalogued canonical generation of unadjusted bars, known at each session's end."""
    instrument = mint_instrument("norgate_assetid", str(assetid))
    rows = []
    for day, close in closes.items():
        value = Decimal(close)
        end = _edt_start(day) + DAY - 1
        row: dict[str, object] = {
            "revision_id": f"bar-{day}",
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": end,
            "revision_known_at_us": end,
            "ingested_at_us": end,
            "source_snapshot_id": "synthetic-bars",
            "source_row_hash": hashlib.sha256(str(day).encode()).hexdigest(),
            "instrument_id": instrument,
            "session_date": day,
            "interval": "1d",
            "bar_end_us": end,
            "basis": "unadjusted",
            "currency": "USD",
            **dict.fromkeys(("open", "high", "low", "close"), value),
            "volume": Decimal(1000),
            "price_role": "canonical",
            "value_state": "present",
        }
        rows.append(row)
    request = hashlib.sha256(b"synthetic-bars").hexdigest()
    marker = publish_generation(
        ws.market,
        dataset_id="prices.us.synthetic",
        version="1",
        generation_id="bars-1",
        operation_id="op-bars-1",
        request_hash=request,
        parent_id=None,
        domain="prices",
        rows=rows,
    )
    # The generation's time-rule provenance is a retained spec naming recorded times.
    rules = {"rule": "source_column@1", "basis": "revision", "args": {}}
    spec = {
        "schema_version": "aas-promotion-v1",
        "time_rules": {"available_at_us": rules, "revision_known_at_us": rules},
    }
    _, transform, _ = put_raw(ws.paths.raw, json.dumps(spec).encode())
    with atomic(ws.state):
        ws.state.execute(
            "INSERT INTO datasets VALUES ('prices.us.synthetic','prices',?,'test')",
            ("aas-market-rowset-v1",),
        )
        ws.state.execute(
            "INSERT INTO dataset_versions VALUES ('prices.us.synthetic','1','bars-1',NULL,1,?,?,?,"
            "'synthetic',?,NULL,NULL,?,'all','committed')",
            (marker["chain_hash"], request, "aas-market-rowset-v1", transform, marker["row_count"]),
        )
    return GenerationPin("prices.us.synthetic", "1", "bars-1", str(marker["chain_hash"]), request)


def _generation_pin(applied: dict[str, object]) -> GenerationPin:
    marker = applied["marker"]
    assert isinstance(marker, dict)
    return GenerationPin(
        "actions.us.norgate",
        str(marker["version"]),
        str(marker["generation_id"]),
        str(marker["chain_hash"]),
        str(marker["request_hash"]),
    )


def test_norgate_actions_promote_and_adjust_canonical_prices(ws: Workspace) -> None:
    days = [date(2020, 8, 28), date(2020, 8, 31), date(2020, 9, 1)]
    rows = [
        _part(7, days[0], 125.0, 500.0, dividend=0.25),
        _part(7, days[1], 128.0, 128.0),
        _part(7, days[2], 132.0, 132.0),
    ]
    part = commit(ws, "norgate-adjusted-part", "observations", _arrow(rows, PART_SCHEMA))
    identity = _register(ws, part, [7])
    opens = {day: _edt_start(day) + 9 * HOUR + 30 * 60 * 10**6 for day in days}
    calendar = publish_calendar(
        ws,
        {day: (opened, opened + 6 * HOUR + 30 * 60 * 10**6) for day, opened in opens.items()},
        dataset="sessions.xnys",
        calendar_id="XNYS",
    )
    sources = [_pin(ws, part, "observations")]
    # A partition would cut a series from its neighbour, so every row lacks a partition date.
    partitioned = promote(
        ws,
        *_spec(
            sources,
            identity,
            calendar,
            mapper_name="norgate.dividends@1",
            decimals={"amount": "float_shortest@1"},
            partition={"from": "2020-01-01", "to": "2021-01-01"},
        ),
        apply=False,
    )
    refusals = partitioned["refusals"]
    assert isinstance(refusals, list)
    assert any("no partition date" in str(item) for item in refusals)
    dividends = promote(
        ws,
        *_spec(
            sources,
            identity,
            calendar,
            mapper_name="norgate.dividends@1",
            decimals={"amount": "float_shortest@1"},
        ),
        apply=True,
    )
    assert dividends["rows"] == {"ok": 1}
    capital = promote(
        ws,
        *_spec(
            sources,
            identity,
            calendar,
            mapper_name="norgate.capital_adjustments@1",
            decimals={"ratio": "float_shortest@1"},
            parent=str(dividends["generation_id"]),
        ),
        apply=True,
    )
    assert capital["rows"] == {"ok": 1}
    stored = ws.market.execute(
        "SELECT action_id, ex_date, amount, ratio, available_at_us, revision_known_at_us "
        "FROM corporate_actions ORDER BY action_id"
    ).fetchall()
    # Both the dividend (paid 1.00 before the 4:1 split) and the split take effect on
    # 2020-08-31, known from that session's open.
    assert stored == [
        ("capital_adjustment:2020-08-31", days[1], None, Decimal(4), opens[days[1]],
         opens[days[1]]),
        ("dividend:2020-08-31", days[1], Decimal(1), None, opens[days[1]], opens[days[1]]),
    ]  # fmt: skip
    bars = _publish_prices(ws, 7, dict(zip(days, ("500", "128", "132"), strict=True)))
    prices = HeadBinding("prices", (HeadPin(bars),))
    actions = HeadBinding("corporate_actions", (HeadPin(_generation_pin(capital)),), (EXDATE,))
    budget = ComputeBudget(Fraction(1), 256 * 1024 * 1024)

    def closes(cutoff: int, basis: str) -> list[object]:
        read = load_adjusted_prices(
            ws, prices, actions, HeadQuery(cutoff_us=cutoff), basis=basis, budget=budget
        )
        return [row.values["close"] for row in read.rows if row.values["session_date"] == days[0]]

    late = _edt_start(date(2020, 9, 2))
    # The split alone puts 500 in post-split terms; total return also reinvests the
    # dividend at the last close before the ex-date: 125 * (500 - 1) / 500.
    assert closes(late, "split_adjusted") == [Decimal(125)]
    assert closes(late, "total_return") == [Decimal("124.75")]


def test_norgate_status_and_fmp_actions_promote(ws: Workspace) -> None:
    master = commit(
        ws,
        "norgate-master",
        "assets",
        _arrow(
            [
                {"assetid": 1, "is_delisted": False, "first_date": "2020-08-03",
                 "last_date": None},
                {"assetid": 2, "is_delisted": True, "first_date": "2020-08-03",
                 "last_date": "2020-08-28"},
            ],
            STATUS_SCHEMA,
        ),
    )  # fmt: skip
    identity = _register(ws, master, [1, 2])
    rule = {
        "rule": "local_day_end@1",
        "basis": "record",
        "input": "status_date",
        "args": {"timezone": NEW_YORK},
    }

    def status(event: str, parent: str | None) -> dict[str, object]:
        document = {
            "schema_version": "aas-promotion-v1",
            "target": {"domain": "instrument_status", "dataset_id": "status.us.norgate",
                       "parent": parent},
            "sources": [_pin(ws, master, "assets")],
            "mapper": {"name": "norgate.status@1", "args": {"timezone": NEW_YORK, "event": event}},
            "partition": None,
            "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
            "decimal_rule": {},
            "quality_rules": [],
            "tombstone_policy": {"mode": "never"},
            "identity_snapshot": identity,
        }  # fmt: skip
        raw = json.dumps(document, sort_keys=True).encode()
        return promote(ws, raw, hashlib.sha256(raw).hexdigest(), apply=True)

    listed = status("listed", None)
    assert listed["rows"] == {"ok": 2}
    delisted = status("delisted", str(listed["generation_id"]))
    assert delisted["rows"] == {"ok": 1}
    stored = ws.market.execute(
        "SELECT status, effective_from_us, available_at_us FROM instrument_status "
        "ORDER BY status, effective_from_us"
    ).fetchall()
    # Each event is known at the end of the New York day it is read from.
    listing = _edt_start(date(2020, 8, 3))
    assert stored == [
        ("delisted", _edt_start(date(2020, 8, 29)), _edt_start(date(2020, 8, 29)) - 1),
        ("listed", listing, listing + DAY - 1),
        ("listed", listing, listing + DAY - 1),
    ]
    # FMP: a frozen dividend response resolved through (fmp, fmp_symbol), ex-date open on XNYS.
    ex = date(2020, 8, 31)
    response = commit(
        ws,
        "fmp-dividends",
        "dividends",
        _arrow(
            [_fmp("AAA", ex, 1, dividend=0.25, recordDate=ex, paymentDate=date(2020, 9, 15))],
            FMP_DIVIDENDS,
        ),
    )
    fmp = _register(ws, response, [1], provider="fmp", symbols=["AAA"])
    opened = _edt_start(ex) + 9 * HOUR + 30 * 60 * 10**6
    calendar = publish_calendar(
        ws, {ex: (opened, opened + 6 * HOUR + 30 * 60 * 10**6)},
        dataset="sessions.xnys", calendar_id="XNYS",
    )  # fmt: skip
    revision = promote(
        ws,
        *_spec(
            [_pin(ws, response, "dividends")],
            fmp,
            calendar,
            mapper_name="fmp.dividends@1",
            decimals={"amount": "float_shortest@1"},
            dataset="actions.us.fmp.ref",
            args={"timezone": NEW_YORK, "revision": 1},
        ),
        apply=True,
    )
    assert revision["rows"] == {"ok": 1}
    assert ws.market.execute(
        "SELECT action_id, amount, available_at_us, revision_known_at_us, ingested_at_us "
        "FROM corporate_actions"
    ).fetchall() == [
        (
            "dividend:2020-08-31",
            Decimal("0.25"),
            opened,
            opened,
            _us(datetime(2026, 8, 1, tzinfo=UTC)),
        )
    ]


def _bar(day: date, close: str) -> dict[str, object]:
    value = Decimal(close)
    return {
        "session_date": day,
        "basis": "unadjusted",
        "currency": "USD",
        **dict.fromkeys(("open", "high", "low", "close"), value),
        "volume": Decimal(100),
        "value_state": "present",
    }


def _event(kind: str, day: date, value: str | None, **extra: object) -> dict[str, object]:
    cash = kind == "dividend"
    return {
        "action_type": kind,
        "effective_date": day,
        "amount": Decimal(value) if cash and value else None,
        "ratio": Decimal(value) if not cash and value else None,
        "currency": "USD" if cash else None,
        "value_state": "present",
        **extra,
    }


def test_adjustment_marks_bars_before_an_unadjustable_action() -> None:
    bars = [_bar(D[0], "40"), _bar(D[1], "40"), _bar(D[2], "20"), _bar(D[3], "10")]
    split = _event("split", D[2], "2")

    def derived(actions: list[dict[str, object]], basis: str) -> list[tuple[object, ...]]:
        return [
            (values["close"], values["volume"], values["value_state"], reasons)
            for values, _, reasons in adjust(bars, actions, basis)
        ]

    def derived_from(
        series: list[dict[str, object]], actions: list[dict[str, object]]
    ) -> list[tuple[object, ...]]:
        return [
            (values["close"], factor, values["value_state"], reasons)
            for values, factor, reasons in adjust(series, actions, "total_return")
        ]

    # A split scales earlier prices down and volume up; the last bar stays as traded.
    assert derived([split], "split_adjusted") == [
        (Decimal(20), Decimal(200), "present", ()),
        (Decimal(20), Decimal(200), "present", ()),
        (Decimal(20), Decimal(100), "present", ()),
        (Decimal(10), Decimal(100), "present", ()),
    ]
    # An action of a kind the derivation does not apply, an action without a value and a
    # dividend not below the close before it each leave the earlier bars without values.
    for broken in (
        _event("spin_off", D[3], "0.9"),
        _event("split", D[3], None, value_state="invalid"),
        _event("dividend", D[3], "20"),
        _event("dividend", D[3], "1", currency="CAD"),
    ):
        kept = derived([split, broken], "total_return")
        assert kept[-1] == (Decimal(10), Decimal(100), "present", ())
        assert all(item == (None, None, "invalid", (UNADJUSTABLE,)) for item in kept[:-1])
    # Split adjustment never reads a dividend, usable or not.
    assert derived([split, _event("dividend", D[3], "20")], "split_adjusted")[0][0] == Decimal(20)
    # Actions outside the bars read neither scale nor break anything.
    outside = [_event("spin_off", D[0], "0.5"), _event("split", D[4], "3")]
    assert [item[0] for item in derived(outside, "total_return")] == [40, 40, 20, 10]
    # A dividend reinvests at the session before its ex-date only: a bar there that is not
    # present leaves no close, even though an earlier bar has one.
    gap = [*bars[:2], {**bars[2], "close": None, "value_state": "invalid"}, bars[3]]
    assert [item[2] for item in derived_from(gap, [_event("dividend", D[3], "1")])] == [
        *["invalid"] * 3,
        "present",
    ]
    # A held action breaks the earlier bars under either basis and carries its reason.
    held = adjust(bars, [], "split_adjusted", held=[(D[2], ("ungranted_time_rule",))])
    assert [item[2] for item in held] == [
        ("ungranted_time_rule", UNADJUSTABLE),
        ("ungranted_time_rule", UNADJUSTABLE),
        (),
        (),
    ]
    # A dividend is matched against the currency of the close it is reinvested at.
    redenominated = [*bars[:3], {**bars[3], "currency": "CAD"}]
    cad = _event("dividend", D[3], "1", currency="CAD")
    assert [item[0] for item in derived_from(redenominated, [cad])][:3] == [None, None, None]
    assert derived_from(redenominated, [_event("dividend", D[3], "1")])[0][2] == "present"
    with pytest.raises(ValueError, match="adjusted basis"):
        adjust(bars, [], "unadjusted")
    with pytest.raises(ValueError, match="two bars"):
        adjust([*bars, _bar(D[0], "41")], [], "total_return")
    with pytest.raises(ValueError, match="unadjusted"):
        adjust([{**bars[0], "basis": "total_return"}], [], "total_return")
