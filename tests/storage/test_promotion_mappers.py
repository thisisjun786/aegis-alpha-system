# ruff: noqa: S608 -- synthetic fixture values and test-owned SQL
"""Each registered mapper against a synthetic source fixture and independent expectations."""

from __future__ import annotations

from datetime import UTC, date, datetime

import duckdb

from aegis_alpha.storage.promotion.mappers import REGISTRY, IdentityKey, mapper


def test_eodhd_bars_maps_synthetic_fixture() -> None:
    bars = mapper("eodhd.bars@1")
    assert set(REGISTRY) == {"eodhd.bars@1"}
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
