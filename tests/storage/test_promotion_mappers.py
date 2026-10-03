# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""Each registered mapper against a synthetic source fixture and independent expectations."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pytest

from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.mappers import MANIFEST_ITEMS, REGISTRY, IdentityKey, mapper
from tests.storage import dart_receipt_support as dart
from tests.storage.kr_identity_support import DART_COLUMNS


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def test_eodhd_bars_maps_synthetic_fixture() -> None:
    bars = mapper("eodhd.bars@1")
    assert set(REGISTRY) == {
        "calendar.declared@1",
        "dart.fnltt@1",
        "dart.fnltt_filings@1",
        "eodhd.bars@1",
        "eodhd.bars_adjusted@1",
        "eodhd.bars_quarantine@1",
        "eodhd.bulk_quarantine@1",
        "eodhd.bulk_quarantine_adjusted@1",
        "fmp.eod_non_split@1",
        "norgate.prices_adjusted@1",
        "norgate.prices_none@1",
        "norgate.reference_closes@1",
        "norgate.reference_history@1",
        "fred.alfred@1",
        "fred.fx_series@1",
        "norgate.fx_closes@1",
        "norgate.fx_history@1",
        "bok.observations@1",
        "oecd.observations@1",
        "kind.industry@1",
        "norgate.classification@1",
        "sec.sic@1",
    }
    args = {"timezone": "Asia/Seoul"}
    bars.check_args(args)
    assert bars.identity(args) == IdentityKey("eodhd", "eodhd_symbol")
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "provider_symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
        "volume DOUBLE, currency VARCHAR, retrieved_at TIMESTAMPTZ)"
    )
    collected = datetime(2025, 1, 10, tzinfo=UTC)
    connection.executemany(
        "INSERT INTO src VALUES (0, ?, 'h', ?, ?, ?, ?, ?, ?, ?, 'KRW', ?)",
        [
            (1, "AAA.KO", date(2025, 1, 2), 10.0, 12.0, 9.0, 11.0, 100.0, collected),
            (2, "AAA.KO", date(2025, 1, 3), None, None, None, None, None, collected),
            (3, "AAA.KO", date(2025, 1, 6), 10.0, 12.0, -1.0, 11.0, 100.0, collected),
            (4, "AAA.KO", date(2025, 1, 7), 10.0, None, 9.0, 11.0, 100.0, collected),
        ],
    )
    rows = connection.execute(
        f"SELECT _aas_ordinal, _aas_id_token, _aas_id_at_us, session_date, interval, bar_end_us, "
        f"basis, price_role, value_state, close, _aas_ingested_at_us, _aas_t_session_date "
        f"FROM ({bars.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()
    start = int(datetime(2025, 1, 1, 15, tzinfo=UTC).timestamp()) * 10**6
    day = 86_400 * 10**6
    ingested = int(collected.timestamp()) * 10**6
    assert rows[0] == (
        1,
        "AAA.KO",
        start,
        date(2025, 1, 2),
        "1d",
        start + day - 1,
        "unadjusted",
        "canonical",
        "present",
        11.0,
        ingested,
        date(2025, 1, 2),
    )
    # No values is missing; a negative or partial bar is invalid and keeps none.
    assert [(row[8], row[9]) for row in rows[1:]] == [
        ("missing", None),
        ("invalid", None),
        ("invalid", None),
    ]


def test_calendar_declared_maps_synthetic_fixture() -> None:
    declared = mapper("calendar.declared@1")
    args = {"timezone_version": "zone-data-1"}
    declared.check_args(args)
    assert declared.identity(args) is None
    assert declared.numeric_columns(args) == {}
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "calendar_id VARCHAR, venue VARCHAR, timezone VARCHAR, session_date DATE, status VARCHAR, "
        "open_local TIMESTAMP, close_local TIMESTAMP, declared_at TIMESTAMPTZ)"
    )
    zone = ZoneInfo("America/New_York")
    declared_at = datetime(2026, 3, 9, 12, tzinfo=UTC)
    rows = [
        # Before the March 2026 change to daylight time (UTC-5), then after it (UTC-4).
        (date(2026, 3, 6), "open", (9, 30), (16, 0)),
        (date(2026, 3, 9), "open", (9, 30), (16, 0)),
        (date(2026, 3, 10), "open", (9, 30), (13, 0)),
        (date(2026, 3, 7), "closed", None, None),
        (date(2026, 3, 14), "closed", None, None),
        # Malformed rows keep no status: times on a closed date, a reversed session.
        (date(2026, 3, 11), "closed", (9, 30), (16, 0)),
        (date(2026, 3, 12), "open", (16, 0), (9, 30)),
    ]

    def local(day: date, hours: tuple[int, int] | None) -> datetime | None:
        return None if hours is None else datetime(day.year, day.month, day.day, *hours)  # noqa: DTZ001 -- local wall time

    connection.executemany(
        "INSERT INTO src VALUES (0, ?, 'h', 'XTST', 'XTST', 'America/New_York', ?, ?, ?, ?, ?)",
        [
            (index, day, status, local(day, opened), local(day, closed), declared_at)
            for index, (day, status, opened, closed) in enumerate(rows)
        ],
    )
    mapped = connection.execute(
        f"SELECT _aas_ordinal, calendar_id, venue, session_date, open_at_us, close_at_us, status, "
        f"timezone_version, _aas_ingested_at_us, _aas_t_public_by "
        f"FROM ({declared.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()

    def at(day: date, hour: int, minute: int) -> int:
        return _us(datetime(day.year, day.month, day.day, hour, minute, tzinfo=zone))

    friday, monday, tuesday = date(2026, 3, 6), date(2026, 3, 9), date(2026, 3, 10)
    assert at(friday, 9, 30) == _us(datetime(2026, 3, 6, 14, 30, tzinfo=UTC))
    assert at(monday, 9, 30) == _us(datetime(2026, 3, 9, 13, 30, tzinfo=UTC))
    saturday_end = _us(datetime(2026, 3, 8, tzinfo=zone)) - 1
    # A past session is public by its own close; a future one only by the declaration.
    assert mapped[:5] == [
        (0, "XTST", "XTST", friday, at(friday, 9, 30), at(friday, 16, 0), "open",
         "zone-data-1", None, at(friday, 16, 0)),
        (1, "XTST", "XTST", monday, at(monday, 9, 30), at(monday, 16, 0), "open",
         "zone-data-1", None, _us(declared_at)),
        (2, "XTST", "XTST", tuesday, at(tuesday, 9, 30), at(tuesday, 13, 0), "open",
         "zone-data-1", None, _us(declared_at)),
        (3, "XTST", "XTST", date(2026, 3, 7), None, None, "closed",
         "zone-data-1", None, saturday_end),
        (4, "XTST", "XTST", date(2026, 3, 14), None, None, "closed",
         "zone-data-1", None, _us(declared_at)),
    ]  # fmt: skip
    assert [(row[4], row[5], row[6], row[9]) for row in mapped[5:]] == [
        (None, None, None, None)
    ] * 2


def _bulk(**fields: object) -> str:
    document: dict[str, object] = {
        "code": "005930",
        "exchange_short_name": "KO",
        "date": "2025-01-02",
        "open": 10,
        "high": 12,
        "low": 9,
        "close": 11,
        "adjusted_close": 10.5,
        "volume": 100,
    }
    document.update(fields)
    return json.dumps({key: value for key, value in document.items() if value != "absent"})


def test_eodhd_bulk_quarantine_maps_synthetic_fixture() -> None:
    bulk = mapper("eodhd.bulk_quarantine@1")
    args = {"timezone": "Asia/Seoul", "currencies": {"KO": "KRW", "KQ": "KRW"}}
    bulk.check_args(args)
    for wrong in ({"timezone": "Asia/Seoul"}, {**args, "currencies": {"KO": "krw"}}):
        with pytest.raises(ValueError, match=r"takes exactly|ISO currencies"):
            bulk.check_args(wrong)
    assert bulk.identity(args) == IdentityKey("eodhd", "eodhd_symbol")
    assert bulk.row_flags == {"provider_reported_partial": "_aas_f_provider_reported_partial"}
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "reason VARCHAR, source_row_json VARCHAR)"
    )
    partial = "provider_reported_partial"
    rows = [
        (partial, _bulk()),
        (partial, _bulk(code="035720", exchange_short_name="KQ", open=None, high=None,
                        low=None, close=None, volume=None)),
        (partial, _bulk(low=-1)),
        (partial, _bulk(volume="absent")),
        (partial, _bulk(volume=2**53 + 1)),
        (partial, _bulk(volume=-(2**64) - 1)),
        (partial, _bulk(close="11")),
        (partial, _bulk(exchange_short_name="US")),
        ("invalid_price_or_volume", _bulk()),
        (partial, _bulk(date="2025-1-2")),
        (partial, "{not json"),
    ]  # fmt: skip
    connection.executemany(
        "INSERT INTO src VALUES (0, ?, 'h', ?, ?)",
        [(index, reason, text) for index, (reason, text) in enumerate(rows)],
    )
    mapped = connection.execute(
        f"SELECT _aas_ordinal, _aas_id_token, _aas_id_at_us, session_date, interval, bar_end_us, "
        f"basis, currency, price_role, value_state, open, close, volume, _aas_ingested_at_us, "
        f"_aas_t_session_date, _aas_f_provider_reported_partial "
        f"FROM ({bulk.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()
    start = _us(datetime(2025, 1, 1, 15, tzinfo=UTC))
    day = 86_400 * 10**6
    session = date(2025, 1, 2)
    assert mapped[0] == (
        0, "005930.KO", start, session, "1d", start + day - 1, "unadjusted", "KRW",
        "canonical", "present", 10.0, 11.0, 100.0, None, session, True,
    )  # fmt: skip
    # Every value null is missing; negative, partial, too-wide and mistyped are invalid.
    assert [(row[1], row[9], row[11]) for row in mapped[1:7]] == [
        ("035720.KQ", "missing", None),
        ("005930.KO", "invalid", None),
        ("005930.KO", "invalid", None),
        ("005930.KO", "invalid", None),
        ("005930.KO", "invalid", None),
        ("005930.KO", "invalid", None),
    ]
    # An exchange without a declared currency has none; another hold reason, a date in
    # another spelling and unparsable JSON have no session date.
    assert mapped[7][7] is None
    assert [(row[3], row[15]) for row in mapped[8:]] == [(None, False), (None, True), (None, True)]
    assert mapped[10][1] is None
    partitions = connection.execute(
        f"SELECT ({bulk.partition_sql}) FROM src ORDER BY _aas_ordinal"
    ).fetchall()
    assert [row[0] for row in partitions] == [session] * 8 + [None] * 3


def test_eodhd_adjusted_close_maps_close_only_reference() -> None:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE bars (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "provider_symbol VARCHAR, date DATE, adjusted_close DOUBLE, currency VARCHAR, "
        "retrieved_at TIMESTAMPTZ)"
    )
    collected = datetime(2025, 1, 10, tzinfo=UTC)
    connection.executemany(
        "INSERT INTO bars VALUES (0, ?, 'h', 'AAA.KO', ?, ?, 'KRW', ?)",
        [(1, date(2025, 1, 2), 1234.5678, collected), (2, date(2025, 1, 3), None, collected)],
    )
    connection.execute(
        "CREATE TABLE bulk (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "reason VARCHAR, source_row_json VARCHAR)"
    )
    connection.execute(
        "INSERT INTO bulk VALUES (0, 1, 'h', 'provider_reported_partial', ?)",
        [_bulk(adjusted_close=-3.0)],
    )
    history = mapper("eodhd.bars_adjusted@1")
    held = mapper("eodhd.bulk_quarantine_adjusted@1")
    assert history.numeric_columns({}) == held.numeric_columns({}) == {"close": "DOUBLE"}
    assert "adjusted_close" in history.source_columns()
    assert "close" not in history.source_columns()
    columns = "fields, basis, price_role, value_state, open, high, low, close, volume"
    zone = {"timezone": "Asia/Seoul"}
    assert connection.execute(
        f"SELECT {columns} FROM ({history.select('bars', zone)}) ORDER BY _aas_ordinal"
    ).fetchall() == [
        ("close", "total_return", "reference", "present", None, None, None, 1234.5678, None),
        ("close", "total_return", "reference", "missing", None, None, None, None, None),
    ]
    bulk_args = {**zone, "currencies": {"KO": "KRW"}}
    assert connection.execute(
        f"SELECT {columns}, _aas_f_provider_reported_partial "
        f"FROM ({held.select('bulk', bulk_args)})"
    ).fetchall() == [
        ("close", "total_return", "reference", "invalid", None, None, None, None, None, True)
    ]


def test_eodhd_bars_quarantine_maps_held_history_rows() -> None:
    held = mapper("eodhd.bars_quarantine@1")
    args = {"timezone": "Asia/Seoul", "currencies": {"KO": "KRW", "KQ": "KRW"}}
    held.check_args(args)
    with pytest.raises(ValueError, match="takes exactly"):
        held.check_args({"timezone": "Asia/Seoul"})
    assert held.manifest_items == "jobs"
    assert held.identity(args) == IdentityKey("eodhd", "eodhd_symbol")
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute(
        "CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        "source_fingerprint VARCHAR, reason VARCHAR, source_row_json VARCHAR)"
    )
    zero = json.dumps(
        {
            "date": "2025-01-02",
            "open": 0,
            "high": 0,
            "low": 0,
            "close": 0,
            "adjusted_close": 0,
            "volume": 25607,
        }
    )
    crossed = json.dumps(
        {
            "date": "2025-01-03",
            "open": 1680,
            "high": 1740,
            "low": 1685,
            "close": 1685,
            "adjusted_close": 1000.0038,
            "volume": 104011,
        }
    )
    rows = [
        (0, "fa", "invalid_price_or_volume", zero),
        (0, "fb", "inconsistent_ohlc", crossed),
        (0, "fc", "invalid_price_or_volume", zero),  # listed twice: no symbol
        (0, "fd", "invalid_price_or_volume", zero),  # instant without offset
        (0, "fz", "invalid_price_or_volume", zero),  # not in the manifest
        (0, "fa", "duplicate_row", zero),  # an unmapped reason has no session date
        (1, "fa", "invalid_price_or_volume", zero),  # another pin's jobs
    ]  # fmt: skip
    connection.executemany(
        "INSERT INTO src VALUES (?, ?, 'h', ?, ?, ?)",
        [(pin, index, *row) for index, (pin, *row) in enumerate(rows)],
    )
    connection.execute(f"CREATE TABLE {MANIFEST_ITEMS} (_aas_pin INTEGER, item VARCHAR)")
    done = "2025-01-10T00:00:00.5+09:00"
    jobs = [
        (0, {"fingerprint": "fa", "symbol": "005930.KO", "completed_at_utc": done}),
        (0, {"fingerprint": "fb", "symbol": "035720.KQ", "completed_at_utc": done}),
        (0, {"fingerprint": "fc", "symbol": "000001.KO", "completed_at_utc": done}),
        (0, {"fingerprint": "fc", "symbol": "000002.KO", "completed_at_utc": done}),
        (0, {"fingerprint": "fd", "symbol": "000003.KO", "completed_at_utc": "2025-01-10"}),
        (1, {"fingerprint": "fa", "symbol": "000004.US", "completed_at_utc": done}),
    ]
    connection.executemany(
        f"INSERT INTO {MANIFEST_ITEMS} VALUES (?, ?)",
        [(pin, json.dumps(job)) for pin, job in jobs],
    )
    mapped = connection.execute(
        f"SELECT _aas_ordinal, _aas_id_token, _aas_id_at_us, session_date, bar_end_us, "
        f"currency, basis, price_role, value_state, open, high, low, close, volume, "
        f"_aas_ingested_at_us, _aas_t_session_date "
        f"FROM ({held.select('src', args)}) ORDER BY _aas_ordinal"
    ).fetchall()
    start = _us(datetime(2025, 1, 1, 15, tzinfo=UTC))
    day = 86_400 * 10**6
    completed = _us(datetime(2025, 1, 9, 15, 0, 0, 500_000, tzinfo=UTC))
    session = date(2025, 1, 2)
    # The collector's held rows are invalid bars that keep no values, even all-zero ones.
    assert mapped[0] == (
        0, "005930.KO", start, session, start + day - 1, "KRW", "unadjusted", "canonical",
        "invalid", None, None, None, None, None, completed, session,
    )  # fmt: skip
    assert mapped[1][1:4] == ("035720.KQ", start + day, date(2025, 1, 3))
    assert mapped[1][8:14] == ("invalid", None, None, None, None, None)
    assert [(row[1], row[5], row[14]) for row in mapped[2:5]] == [
        (None, None, None),
        ("000003.KO", "KRW", None),
        (None, None, None),
    ]
    assert mapped[5][3] is None
    # The other pin's jobs name the fingerprint for that pin only; its exchange has no currency.
    assert (mapped[6][1], mapped[6][5]) == ("000004.US", None)
    partitions = connection.execute(
        f"SELECT ({held.partition_sql}) FROM src ORDER BY _aas_ordinal"
    ).fetchall()
    assert [row[0] for row in partitions] == [
        session, date(2025, 1, 3), session, session, session, None, session,
    ]  # fmt: skip


def _dart_source(rows: list[tuple[object, ...]]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    columns = ", ".join(f"{name} VARCHAR" for name in DART_COLUMNS)
    connection.execute(
        f"CREATE TABLE src (_aas_pin INTEGER, _aas_ordinal BIGINT, _aas_row_hash VARCHAR, "
        f"{columns})"
    )
    connection.executemany(
        f"INSERT INTO src VALUES (0, ?, 'h', {', '.join('?' for _ in DART_COLUMNS)})",
        [(index, *row) for index, row in enumerate(rows)],
    )
    return connection


_CORP, _OTHER = "00000101", "00000202"
_ARGS: dict[str, object] = {"december_year_end": [_CORP, _OTHER]}
_NUMBER = "20250814000123"
_RETRIEVED_US = _us(datetime(2026, 9, 12, 8, 33, 43, 268707, tzinfo=UTC))
_LINES = [
    # A quarter amount with its year to date; the second line repeats the account name
    # under another order, as DART statements do.
    dart.Line("ifrs-full_Revenue", "매출액", "1200", cumulative="2300", ord="1"),
    dart.Line("-표준계정코드 미사용-", "기타", "-5", ord="2"),
    dart.Line("-표준계정코드 미사용-", "기타", "", ord="3", sj_div="BS"),
    dart.Line("ifrs-full_Equity", "자본", "12.5", ord="4", sj_div="BS", cumulative="9"),
    dart.Line("ifrs-full_Assets", "자산", "1,000", ord="5", sj_div="BS"),
    dart.Line("ifrs-full_CashFlows", "현금흐름", "40", ord="6", sj_div="CF", cumulative="40"),
]


def _set_line(index: int, name: str, value: str) -> Callable[[dict[str, Any]], None]:
    def edit(document: dict[str, Any]) -> None:
        document["list"][index][name] = value

    return edit


def _dart_rows() -> list[tuple[object, ...]]:
    good = dart.completed(_CORP, "2025", "11012", _NUMBER, _LINES)
    damaged = dart.completed(_OTHER, "2025", "11012", _NUMBER, _LINES[:1], raw=b"not the bytes")
    foreign = dart.completed(_OTHER, "2024", "11011", "20250320000001", _LINES[:1])
    two = dart.completed(_CORP, "2023", "11011", "20240315000009", _LINES[:2])
    return [
        good,
        dart.no_data(_CORP, "2025", "11013"),
        dart.no_data(_CORP, "2025", "11014", outcome="FAILED"),
        dart.corp_codes(),
        damaged,
        # Lines that name another corp than the request.
        (*foreign[:3], dart.request(_CORP, "2024", "11011"), *foreign[4:]),
        # A later line that names another report, then another receipt number.
        dart.edited(good, _set_line(1, "reprt_code", "11014")),
        dart.edited(two, _set_line(1, "rcept_no", "20240401000001")),
        # A receipt number whose first eight digits are no date, and a request whose
        # business year is not a year.
        dart.edited(good, _set_line(1, "rcept_no", "20251399000123")),
        (*good[:3], dart.request(_CORP, "25", "11012"), *good[4:]),
        dart.no_data(_CORP, "2025", "11011", outcome="RETRY"),
    ]


_OUTCOMES = [
    "completed",
    "no_data",
    "failed",
    "other_endpoint",
    "unreadable",
    "mismatched",
    "mismatched",
    "mismatched",
    "unreadable",
    "unreadable",
    "unknown_outcome",
]


def test_dart_receipt_outcomes_and_partition_dates() -> None:
    fnltt = mapper("dart.fnltt@1")
    connection = _dart_source(_dart_rows())
    outcomes = connection.execute(
        f"SELECT {fnltt.outcome(_ARGS)}, {fnltt.partition_sql}, "
        f"{fnltt.outcome({'december_year_end': [_OTHER]})}, "
        f"{mapper('dart.fnltt_filings@1').outcome({})} FROM src ORDER BY _aas_ordinal"
    ).fetchall()
    assert [row[0] for row in outcomes] == _OUTCOMES
    # Filings need no year end; statements of an undeclared year end are not dated.
    assert [row[3] for row in outcomes] == _OUTCOMES
    assert [row[2] for row in outcomes] == ["year_end_unknown", *_OUTCOMES[1:]]
    assert [row[1] for row in outcomes] == [
        date(2025, 1, 1),
        date(2025, 1, 1),
        date(2025, 1, 1),
        None,
        date(2025, 1, 1),
        date(2024, 1, 1),
        date(2025, 1, 1),
        date(2023, 1, 1),
        date(2025, 1, 1),
        None,
        date(2025, 1, 1),
    ]


def test_dart_fnltt_maps_synthetic_fixture() -> None:
    fnltt = mapper("dart.fnltt@1")
    fnltt.check_args(_ARGS)
    assert fnltt.identity({}) is None
    assert fnltt.expands
    assert fnltt.numeric_columns({}) == {"value": "VARCHAR"}
    connection = _dart_source(_dart_rows())
    mapped = connection.execute(
        "SELECT _aas_ordinal, _aas_item, issuer_id, concept, period_start, period_end, "
        "fiscal_period, unit, dimensions_hash, form, accession, accepted_at_us, value, "
        "value_state, _aas_ingested_at_us, _aas_t_filed_date "
        f"FROM ({fnltt.select('src', _ARGS)}) ORDER BY _aas_ordinal, _aas_item"
    ).fetchall()
    issuer = mint_issuer("dart_corp_code", _CORP)
    filed, end = date(2025, 8, 14), date(2025, 6, 30)
    year, quarter = date(2025, 1, 1), date(2025, 4, 1)

    def dims(name: str, sj: str, occurrence: str = "1") -> str:
        return formats.dimensions_hash(
            {
                "account_detail": "-",
                "account_nm": name,
                "fs_div": "CFS",
                "occurrence": occurrence,
                "sj_div": sj,
            }
        )

    def fact(  # noqa: PLR0913, PLR0917 -- one expected fundamentals row
        item: int,
        concept: str,
        start: date | None,
        period: str,
        key: str,
        value: str | None,
        state: str,
    ) -> tuple[object, ...]:
        return (0, item, issuer, concept, start, end, period, "KRW", key, "11012", _NUMBER,
                None, value, state, _RETRIEVED_US, filed)  # fmt: skip

    other = "-표준계정코드 미사용-"
    # An income statement line measures the second quarter and its cumulative amount the
    # half year; balance sheet lines are instants and cash flows the year to date. The
    # cumulative fields of the balance sheet and cash flow lines stay in the source.
    assert mapped[:7] == [
        fact(0, "ifrs-full_Revenue", quarter, "Q2", dims("매출액", "IS"), "1200", "present"),
        fact(1, "ifrs-full_Revenue", year, "H1", dims("매출액", "IS"), "2300", "present"),
        fact(2, other, quarter, "Q2", dims("기타", "IS"), "-5", "present"),
        fact(4, other, None, "H1", dims("기타", "BS"), None, "missing"),
        fact(6, "ifrs-full_Equity", None, "H1", dims("자본", "BS"), "12.5", "present"),
        fact(8, "ifrs-full_Assets", None, "H1", dims("자산", "BS"), None, "invalid"),
        fact(10, "ifrs-full_CashFlows", year, "H1", dims("현금흐름", "CF"), "40", "present"),
    ]  # fmt: skip
    # No data, a failure and the corp code list give no rows. Each refused response
    # gives one row without an issuer.
    refused = [i for i, name in enumerate(_OUTCOMES) if name in {"unreadable", "mismatched",
               "unknown_outcome"}]  # fmt: skip
    assert [(row[0], row[1], row[2]) for row in mapped[7:]] == [(i, 0, None) for i in refused]
    # A spec may grant leaving refused responses out; they still count as outcomes.
    fnltt.check_args({"accept": ["mismatched", "unknown_outcome", "unreadable"]})
    kept = connection.execute(
        "SELECT DISTINCT _aas_ordinal FROM ("
        f"{fnltt.select('src', {**_ARGS, 'accept': ['mismatched', 'unreadable']})})"
    ).fetchall()
    assert sorted(row[0] for row in kept) == [0, 10]
    for bad in ({"other": 1}, {"accept": []}, {"accept": ["unreadable", "mismatched"]},
                {"accept": ["no_data"]}, {"accept": "unreadable"}):  # fmt: skip
        with pytest.raises(ValueError, match="accept"):
            fnltt.check_args(bad)
    for codes in ([], [_OTHER, _CORP], [_CORP, _CORP], ["101"], _CORP):
        with pytest.raises(ValueError, match="december_year_end"):
            fnltt.check_args({"december_year_end": codes})
    # Filings state no period, so they take no year end and cannot accept one unknown.
    filings = mapper("dart.fnltt_filings@1")
    for bad in (_ARGS, {"accept": ["year_end_unknown"]}):
        with pytest.raises(ValueError, match="accept"):
            filings.check_args(bad)


def test_dart_fnltt_reads_each_report_period() -> None:
    fnltt = mapper("dart.fnltt@1")
    lines = [
        dart.Line("is", "is", "1", cumulative="2"),
        dart.Line("cis", "cis", "3", cumulative="4", sj_div="CIS", ord="2"),
        dart.Line("cf", "cf", "5", sj_div="CF", ord="3"),
        dart.Line("sce", "sce", "6", sj_div="SCE", ord="4"),
        dart.Line("bs", "bs", "7", sj_div="BS", ord="5"),
    ]
    reports = {"11013": "20250515000001", "11012": "20250814000001",
               "11014": "20251114000001", "11011": "20260310000001"}  # fmt: skip
    connection = _dart_source(
        [dart.completed(_CORP, "2025", code, number, lines) for code, number in reports.items()]
    )
    mapped = connection.execute(
        "SELECT form, concept, fiscal_period, period_start, period_end, value "
        f"FROM ({fnltt.select('src', _ARGS)}) ORDER BY form, _aas_item"
    ).fetchall()

    def rows(form: str, end: date, quarter: tuple[str, date], ytd: str) -> list[object]:
        start = date(2025, 1, 1)
        # A first-quarter or annual report's cumulative amount measures the same period.
        both = form in {"11012", "11014"}
        return [
            (form, "is", *quarter, end, "1"),
            *([(form, "is", ytd, start, end, "2")] if both else []),
            (form, "cis", *quarter, end, "3"),
            *([(form, "cis", ytd, start, end, "4")] if both else []),
            (form, "cf", ytd, start, end, "5"),
            (form, "sce", ytd, start, end, "6"),
            (form, "bs", ytd, None, end, "7"),
        ]

    assert mapped == [
        *rows("11011", date(2025, 12, 31), ("FY", date(2025, 1, 1)), "FY"),
        *rows("11012", date(2025, 6, 30), ("Q2", date(2025, 4, 1)), "H1"),
        *rows("11013", date(2025, 3, 31), ("Q1", date(2025, 1, 1)), "Q1"),
        *rows("11014", date(2025, 9, 30), ("Q3", date(2025, 7, 1)), "9M"),
    ]


def test_dart_fnltt_maps_one_response_per_request() -> None:
    fnltt = mapper("dart.fnltt@1")
    later = "2026-10-01T00:00:00Z"
    first = dart.completed(_CORP, "2025", "11012", _NUMBER, _LINES[:1])
    again = dart.completed(_CORP, "2025", "11012", _NUMBER, _LINES[:1], retrieved=later)
    # The same filing answered later in other bytes (another amount, extra fields).
    changed = dart.edited(again, _set_line(0, "thstrm_amount", "1300"))
    connection = _dart_source([again, first, changed])
    query = (
        "SELECT _aas_ordinal, value, _aas_ingested_at_us, accession FROM "
        f"({fnltt.select('src', _ARGS)}) WHERE _aas_item = 0 ORDER BY _aas_ordinal"
    )
    # The earliest retrieval of the request's filing speaks for every response of it.
    assert connection.execute(query).fetchall() == [(1, "1200", _RETRIEVED_US, _NUMBER)]
    # An amendment (a larger receipt number) speaks for the request, collected when it may.
    amendment = dart.completed(_CORP, "2025", "11012", "20251002000456", _LINES[:1],
                               retrieved=later)  # fmt: skip
    connection = _dart_source([first, amendment, again])
    mapped = connection.execute(query).fetchall()
    assert [(row[0], row[3]) for row in mapped] == [(1, "20251002000456")]
    # The separate statements are another request and map on their own.
    separate = dart.completed(_CORP, "2025", "11012", _NUMBER, _LINES[:1], fs_div="OFS")
    connection = _dart_source([first, separate])
    assert [row[0] for row in connection.execute(query).fetchall()] == [0, 1]


def test_dart_fnltt_numbers_repeated_lines_by_order() -> None:
    fnltt = mapper("dart.fnltt@1")
    lines = [
        dart.Line("x", "기타", "3", ord="12"),
        dart.Line("x", "기타", "1", ord="2"),
        dart.Line("x", "기타", "2", ord="7"),
        dart.Line("x", "기타", "4", ord="7", sj_div="CIS"),
    ]
    connection = _dart_source([dart.completed(_CORP, "2025", "11013", _NUMBER, lines)])
    mapped = connection.execute(
        f"SELECT value, dimensions_hash FROM ({fnltt.select('src', _ARGS)}) ORDER BY value"
    ).fetchall()

    def dims(sj: str, occurrence: str) -> str:
        return formats.dimensions_hash(
            {"account_detail": "-", "account_nm": "기타", "fs_div": "CFS",
             "occurrence": occurrence, "sj_div": sj}
        )  # fmt: skip

    assert mapped == [("1", dims("IS", "1")), ("2", dims("IS", "2")), ("3", dims("IS", "3")),
                      ("4", dims("CIS", "1"))]  # fmt: skip


def test_dart_fnltt_filings_maps_synthetic_fixture() -> None:
    filings = mapper("dart.fnltt_filings@1")
    filings.check_args({})
    assert not filings.expands
    assert filings.identity({}) is None
    rows = _dart_rows()
    # The separate statements of the same filing, collected later, name the same filing.
    separate = dart.completed(_CORP, "2025", "11012", _NUMBER, _LINES[:1], fs_div="OFS",
                              retrieved="2026-10-01T00:00:00Z")  # fmt: skip
    connection = _dart_source([*rows, separate])
    mapped = connection.execute(
        "SELECT _aas_ordinal, issuer_id, filing_id, form, filed_date, accepted_at_us, "
        "period_end, _aas_ingested_at_us, _aas_t_filed_date "
        f"FROM ({filings.select('src', {})}) ORDER BY _aas_ordinal"
    ).fetchall()
    issuer = mint_issuer("dart_corp_code", _CORP)
    filed = date(2025, 8, 14)
    assert mapped[0] == (0, issuer, _NUMBER, "11012", filed, None, None, _RETRIEVED_US, filed)
    refused = [i for i, name in enumerate(_OUTCOMES) if name not in {"completed", "no_data",
               "failed", "other_endpoint"}]  # fmt: skip
    assert [(row[0], row[1]) for row in mapped[1:]] == [(i, None) for i in refused]
