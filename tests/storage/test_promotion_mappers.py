# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""Each registered mapper against a synthetic source fixture and independent expectations."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb

from aegis_alpha.storage.identity import mint_issuer
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.mappers import REGISTRY, IdentityKey, mapper
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
_NUMBER = "20250814000123"
_RETRIEVED_US = _us(datetime(2026, 9, 12, 8, 33, 43, 268707, tzinfo=UTC))


def _dart_rows() -> list[tuple[object, ...]]:
    lines = [
        # A this-term amount and a cumulative one; the second line repeats the account
        # name under another order, as DART statements do.
        dart.Line("ifrs-full_Revenue", "매출액", "1200", cumulative="2300", ord="1"),
        dart.Line("-표준계정코드 미사용-", "기타", "-5", ord="2"),
        dart.Line("-표준계정코드 미사용-", "기타", "", ord="3", sj_div="BS"),
        dart.Line("ifrs-full_Equity", "자본", "12.5", ord="4", sj_div="BS", cumulative=""),
        dart.Line("ifrs-full_Assets", "자산", "1,000", ord="5", sj_div="BS"),
    ]
    damaged = dart.completed(_OTHER, "2025", "11012", _NUMBER, lines[:1], raw=b"not the bytes")
    # A response whose lines name another corp than its request names no issuer.
    foreign = dart.completed(_OTHER, "2024", "11011", "20250320000001", lines[:1])
    return [
        dart.completed(_CORP, "2025", "11012", _NUMBER, lines),
        dart.no_data(_CORP, "2025", "11013"),
        dart.no_data(_CORP, "2025", "11014", outcome="FAILED"),
        dart.corp_codes(),
        damaged,
        (*foreign[:3], dart.request(_CORP, "2024", "11011"), *foreign[4:]),
    ]


def test_dart_fnltt_maps_synthetic_fixture() -> None:
    fnltt = mapper("dart.fnltt@1")
    fnltt.check_args({})
    assert fnltt.identity({}) is None
    assert fnltt.expands
    assert fnltt.numeric_columns({}) == {"value": "VARCHAR"}
    connection = _dart_source(_dart_rows())
    outcomes = connection.execute(
        f"SELECT _aas_ordinal, {fnltt.outcome({})}, {fnltt.partition_date} FROM src "
        "ORDER BY _aas_ordinal"
    ).fetchall()
    year = date(2025, 1, 1)
    assert outcomes == [
        (0, "completed", year),
        (1, "no_data", year),
        (2, "failed", year),
        (3, "other_endpoint", None),
        (4, "unreadable", year),
        (5, "completed", date(2024, 1, 1)),
    ]
    mapped = connection.execute(
        "SELECT _aas_ordinal, _aas_item, issuer_id, concept, period_start, period_end, "
        "fiscal_period, unit, dimensions_hash, form, accession, accepted_at_us, value, "
        "value_state, _aas_ingested_at_us, _aas_t_filed_date "
        f"FROM ({fnltt.select('src', {})}) ORDER BY _aas_ordinal, _aas_item"
    ).fetchall()
    issuer = mint_issuer("dart_corp_code", _CORP)
    filed, end = date(2025, 8, 14), date(2025, 6, 30)

    def dims(name: str, sj: str, order: str, detail: str = "-") -> str:
        return formats.dimensions_hash(
            {
                "account_detail": detail,
                "account_nm": name,
                "fs_div": "CFS",
                "ord": order,
                "rcept_no": _NUMBER,
                "sj_div": sj,
            }
        )

    def fact(  # noqa: PLR0913, PLR0917 -- one expected fundamentals row
        item: int, concept: str, period: str, key: str, value: str | None, state: str
    ) -> tuple[object, ...]:
        return (0, item, issuer, concept, None, end, period, "KRW", key, "11012", _NUMBER,
                None, value, state, _RETRIEVED_US, filed)  # fmt: skip

    other = "-표준계정코드 미사용-"
    assert mapped[:7] == [
        fact(0, "ifrs-full_Revenue", "H1", dims("매출액", "IS", "1"), "1200", "present"),
        fact(1, "ifrs-full_Revenue", "H1-cumulative", dims("매출액", "IS", "1"), "2300",
             "present"),
        fact(2, other, "H1", dims("기타", "IS", "2"), "-5", "present"),
        fact(4, other, "H1", dims("기타", "BS", "3"), None, "missing"),
        fact(6, "ifrs-full_Equity", "H1", dims("자본", "BS", "4"), "12.5", "present"),
        fact(7, "ifrs-full_Equity", "H1-cumulative", dims("자본", "BS", "4"), None, "missing"),
        fact(8, "ifrs-full_Assets", "H1", dims("자산", "BS", "5"), None, "invalid"),
    ]  # fmt: skip
    # No data, a failure and the corp code list give no rows. A damaged response gives
    # one row without an issuer, and a line that disagrees with its request gives its
    # amounts without one.
    assert [(row[0], row[2]) for row in mapped[7:]] == [(4, None), (5, None), (5, None)]
    assert mapped[8][3] == "ifrs-full_Revenue"


def test_dart_fnltt_filings_maps_synthetic_fixture() -> None:
    filings = mapper("dart.fnltt_filings@1")
    filings.check_args({})
    assert not filings.expands
    assert filings.identity({}) is None
    rows = _dart_rows()
    # A response whose lines name two receipt numbers names no filing.
    split = dart.body(_CORP, "2023", "11011", "20240315000009", [dart.Line("a", "a", "1")])
    mixed = json.loads(split)
    mixed["list"].append({**mixed["list"][0], "rcept_no": "20240401000001"})
    raw = json.dumps(mixed).encode()
    receipt = dart.completed(_CORP, "2023", "11011", "20240315000009", [], raw=raw)
    rows.append((*receipt[:6], hashlib.sha256(raw).hexdigest(), *receipt[7:]))
    connection = _dart_source(rows)
    mapped = connection.execute(
        "SELECT _aas_ordinal, issuer_id, filing_id, form, filed_date, accepted_at_us, "
        "period_end, _aas_ingested_at_us, _aas_t_filed_date "
        f"FROM ({filings.select('src', {})}) ORDER BY _aas_ordinal"
    ).fetchall()
    issuer = mint_issuer("dart_corp_code", _CORP)
    filed = date(2025, 8, 14)
    assert mapped[0] == (0, issuer, _NUMBER, "11012", filed, None, None, _RETRIEVED_US, filed)
    assert [(row[0], row[1]) for row in mapped[1:]] == [(4, None), (5, None), (6, None)]
    assert mapped[3][2:5] == ("20240315000009", "11011", date(2024, 3, 15))
