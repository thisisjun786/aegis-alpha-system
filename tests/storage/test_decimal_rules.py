# ruff: noqa: PLR2004, S311, S608 -- seeded synthetic values and test-owned SQL
"""Decimal rules: the SQL the promotion runs equals the Python reference, value and flags."""

from __future__ import annotations

import math
import random
import struct
from collections.abc import Sequence
from decimal import Decimal

import duckdb
import pytest

from aegis_alpha.storage.promotion import decimal_rules
from aegis_alpha.storage.promotion.decimal_rules import (
    DECIMAL_TEXT,
    EXACT,
    FLOAT_SHORTEST,
    KRW_TICK,
    Converted,
    DecimalRule,
    convert,
)


def _sql(rule: DecimalRule, kind: str, values: Sequence[object]) -> list[Converted | None]:
    """Run ``rule`` in DuckDB; None marks a refused value."""
    connection = duckdb.connect()
    decimal_rules.install(connection)
    connection.execute(f"CREATE TABLE t (n BIGINT, v {kind})")
    connection.executemany("INSERT INTO t VALUES (?, ?)", list(enumerate(values)))
    layers, conversion = decimal_rules.conversion(rule, "v", kind, "_c_")
    sql = "SELECT * FROM t"
    for layer in layers:
        sql = f"SELECT *, {', '.join(f'{expr} AS {alias}' for alias, expr in layer)} FROM ({sql})"
    flags = "".join(f", coalesce({condition}, false)" for _, condition in conversion.flags)
    results: list[Converted | None] = []
    for row in connection.execute(
        f"SELECT CASE WHEN NOT ({conversion.refused}) THEN {conversion.value} END, "
        f"coalesce({conversion.refused}, false){flags} FROM ({sql}) ORDER BY n"
    ).fetchall():
        if row[1]:
            results.append(None)
            continue
        named = tuple(
            name for (name, _), raised in zip(conversion.flags, row[2:], strict=True) if raised
        )
        results.append(Converted(row[0], named))
    return results


def _reference(rule: DecimalRule, kind: str, value: object) -> Converted | None:
    try:
        return convert(rule, value, kind)
    except (ValueError, TypeError):
        return None


def _same(left: Converted | None, right: Converted | None) -> bool:
    if left is None or right is None:
        return left is right
    return left.value == right.value and set(left.flags) == set(right.flags)


def test_krw_tick_rounds_half_even_and_flags() -> None:
    values = [2649999.947, 1000.5, 1001.5, 51900.0, 0.4999999999999999, -2.5, None]
    expected = [
        Converted(Decimal(2650000), ("provider_float_reconstructed",)),
        Converted(Decimal(1000), ("provider_float_reconstructed", "decimal_rounding_tie")),
        Converted(Decimal(1002), ("provider_float_reconstructed", "decimal_rounding_tie")),
        Converted(Decimal(51900), ()),
        Converted(Decimal(0), ("provider_float_reconstructed",)),
        Converted(Decimal(-2), ("provider_float_reconstructed", "decimal_rounding_tie")),
        Converted(None, ()),
    ]
    for value, want, sql in zip(values, expected, _sql(KRW_TICK, "DOUBLE", values), strict=True):
        assert _same(convert(KRW_TICK, value, "DOUBLE"), want), value
        assert _same(sql, want), value
    for refused in (math.inf, math.nan, 1e26):
        assert _reference(KRW_TICK, "DOUBLE", refused) is None
    assert _sql(KRW_TICK, "DOUBLE", [math.inf, math.nan, 1e26]) == [None, None, None]
    with pytest.raises(ValueError, match="KRW price columns"):
        decimal_rules.check_column("krw_tick@1", "volume", "DOUBLE", domain="prices")


def _doubles(rng: random.Random, count: int) -> list[float]:
    special = [
        0.0,
        -0.0,
        0.5,
        1.5,
        2.5,
        -2.5,
        2649999.947,
        1e25,
        9.999999999999999e25,
        1e26,
        5e-324,
    ]
    values: list[float] = []
    while len(values) < count:
        shape = rng.randrange(5)
        if shape == 0:
            values.append(rng.choice(special))
        elif shape == 1:
            values.append(
                float(
                    rng.randrange(-(10**6), 10**6) + rng.choice([0, 0.5, 0.25, 1 / 4096, 1 / 8192])
                )
            )
        elif shape == 2:
            values.append(rng.uniform(-1e6, 1e6))
        else:
            value = struct.unpack(">d", rng.getrandbits(64).to_bytes(8, "big"))[0]
            if math.isfinite(value):
                values.append(value)
    return values


def test_sql_and_python_rounding_parity() -> None:
    rng = random.Random(20261003)
    doubles = _doubles(rng, 4000)
    singles = [
        struct.unpack(">f", struct.pack(">f", value))[0] if abs(value) < 3e38 else 1.0
        for value in _doubles(rng, 4000)
    ]
    for rule, kind, values in (
        (EXACT, "DOUBLE", doubles),
        (KRW_TICK, "DOUBLE", doubles),
        (FLOAT_SHORTEST, "DOUBLE", doubles),
        (EXACT, "FLOAT", singles),
        (FLOAT_SHORTEST, "FLOAT", singles),
    ):
        for value, sql in zip(values, _sql(rule, kind, values), strict=True):
            assert _same(sql, _reference(rule, kind, value)), (rule.name, kind, value)
    texts = [
        "1.6357e+06",
        "100",
        "0.000000000001",
        "0.0000000000001",
        "1e-13",
        "-12.5",
        "abc",
        "",
        "1.",
        ".",
        ".5",
        "1e99999999999",
        "12345678901234567890123456",
        "123456789012345678901234567",
        "+3.0E2",
        "1.2345678e+06",
        " 1",
        "0e0",
    ]
    for value, sql in zip(texts, _sql(DECIMAL_TEXT, "VARCHAR", texts), strict=True):
        assert _same(sql, _reference(DECIMAL_TEXT, "VARCHAR", value)), value
    assert convert(DECIMAL_TEXT, "1.6357e+06", "VARCHAR") == Converted(
        Decimal("1635700.000000000000"), ("volume_precision_limited",)
    )


def test_float_shortest_rounds_half_even_and_rejects_overflow() -> None:
    stored = struct.unpack(">f", struct.pack(">f", 127.1425))[0]
    # The binary32 value 127.14250183105469 is the provider's 127.1425.
    assert convert(FLOAT_SHORTEST, stored, "FLOAT") == Converted(
        Decimal("127.142500000000"), ("provider_float_storage",)
    )
    # An exactly representable value is unchanged and unflagged.
    assert convert(FLOAT_SHORTEST, 0.5, "FLOAT") == Converted(Decimal("0.500000000000"), ())
    # Beyond 12 decimals the shortest text is rounded half to even, and a tie is flagged.
    assert convert(FLOAT_SHORTEST, 0.1234567890125, "DOUBLE") == Converted(
        Decimal("0.123456789012"), ("provider_float_storage", "decimal_rounding_tie")
    )
    assert convert(FLOAT_SHORTEST, 0.1234567890135, "DOUBLE") == Converted(
        Decimal("0.123456789014"), ("provider_float_storage", "decimal_rounding_tie")
    )
    values = [stored, 0.5, 1e26, math.inf, math.nan]
    assert [_reference(FLOAT_SHORTEST, "FLOAT", value) for value in values] == _sql(
        FLOAT_SHORTEST, "FLOAT", values
    )
    for overflow in (1e26, 1e300, math.inf, math.nan):
        with pytest.raises(ValueError, match="DECIMAL"):
            convert(FLOAT_SHORTEST, overflow, "DOUBLE")
    assert _sql(FLOAT_SHORTEST, "DOUBLE", [0.1234567890125, 1e300]) == [
        Converted(Decimal("0.123456789012"), ("provider_float_storage", "decimal_rounding_tie")),
        None,
    ]
