# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""Each registered mapper against a synthetic source fixture and independent expectations."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pytest

from aegis_alpha.storage.promotion.mappers import MANIFEST_ITEMS, REGISTRY, IdentityKey, mapper


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def test_eodhd_bars_maps_synthetic_fixture() -> None:
    bars = mapper("eodhd.bars@1")
    assert set(REGISTRY) == {
        "calendar.declared@1",
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
