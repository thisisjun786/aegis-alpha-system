"""DuckDB v2 prerelease read-side validation queries for price Parquet.

Phase 1 scans the ADR 0002 Hive layout without writing Parquet, PostgreSQL, or
artifacts. The pyarrow canonical generation and validation paths remain
authoritative.

These builders deliberately do not port ``validate_partition`` checks for
required-column nulls, adjustment/source identity, date-coverage bounds, or
partition-year consistency. The glob selects only ``part-*.parquet``. Basis
parity is set-based by key, while the pyarrow oracle compares aligned sorted
rows positionally. In some single-file ``read_parquet`` query shapes the pinned
preview mis-evaluates both ``isnan(NaN)`` and direct comparisons involving NaN;
separately, DuckDB's documented PostgreSQL-style NaN ordering differs from the
pyarrow oracle's IEEE behavior. ``isfinite`` and ``isinf`` remain correct on the
affected scan path. The sole comparison helper therefore defines an ordered
operand as ``isfinite(x) OR isinf(x)``: NaN is excluded before either defect or
semantic difference can affect the count, while finite values and ±Inf retain
the oracle's ordering behavior.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Protocol

import duckdb

EXPECTED_DUCKDB_VERSION: Final = "1.6.0.dev365"
_V2_ENGINE_PREFIX: Final = "v2.0.0-"
_GLOB_METACHARACTERS: Final = "*?["
_FLOAT_COLUMNS: Final = (
    "open",
    "high",
    "low",
    "close",
    "unadjusted_close",
    "dividend",
    "volume",
)
_OHLC_COMPARISONS: Final = (
    ("high", "<", "close"),
    ("high", "<", "low"),
    ("high", "<", "open"),
    ("low", ">", "close"),
    ("low", ">", "open"),
)
_PARITY_QUERY_PREFIX: Final = """,
capital_keys AS (
    SELECT year, assetid, date FROM price WHERE adjustment_type = 'CAPITAL'
),
total_return_keys AS (
    SELECT year, assetid, date FROM price WHERE adjustment_type = 'TOTALRETURN'
),
close_by_key AS (
    SELECT
        year,
        assetid,
        date,
        max(unadjusted_close) FILTER (
            WHERE adjustment_type = 'CAPITAL'
        ) AS uc_c,
        max(unadjusted_close) FILTER (
            WHERE adjustment_type = 'TOTALRETURN'
        ) AS uc_t
    FROM price
    GROUP BY year, assetid, date
),
violations AS (
    (SELECT * FROM capital_keys EXCEPT SELECT * FROM total_return_keys)
    UNION ALL
    (SELECT * FROM total_return_keys EXCEPT SELECT * FROM capital_keys)
    UNION ALL
    SELECT year, assetid, date
    FROM close_by_key
    WHERE """

type _Scalar = str | int | float | bool | None
type PartitionCounts = dict[tuple[str, int], int]


class _QueryResult(Protocol):
    def fetchone(self) -> tuple[_Scalar, ...] | None: ...

    def fetchall(self) -> list[tuple[_Scalar, ...]]: ...


class _QueryConnection(Protocol):
    def sql(self, query: str) -> _QueryResult: ...


def connect() -> duckdb.DuckDBPyConnection:
    """Return an in-memory connection for read-only query execution."""

    return duckdb.connect()


def assert_v2_prerelease(connection: _QueryConnection) -> None:
    """Fail closed unless package and engine identities match the exact pin."""

    if duckdb.__version__ != EXPECTED_DUCKDB_VERSION:
        raise RuntimeError(
            f"expected duckdb package {EXPECTED_DUCKDB_VERSION}, got {duckdb.__version__}"
        )
    row = connection.sql("SELECT version()").fetchone()
    if row is None or not str(row[0]).startswith(_V2_ENGINE_PREFIX):
        actual = "no version row" if row is None else str(row[0])
        raise RuntimeError(f"expected a {_V2_ENGINE_PREFIX} prerelease engine, got {actual}")


def price_partition_glob(dataset_dir: str | Path) -> str:
    """Return the fixed full-dataset glob for canonical price parts."""

    return str(Path(dataset_dir) / "adjustment_type=*" / "year=*" / "part-*.parquet")


def price_scan_sql(dataset_dir: str | Path) -> str:
    """Build the quote-hardened read-only Parquet table expression."""

    raw_dataset_dir = str(dataset_dir)
    if "'" in raw_dataset_dir:
        raise ValueError("dataset_dir must not contain a single quote")
    if any(character in raw_dataset_dir for character in _GLOB_METACHARACTERS):
        raise ValueError("dataset_dir must not contain glob metacharacters")
    return f"read_parquet('{price_partition_glob(raw_dataset_dir)}', hive_partitioning=true)"


def nan_guarded_cmp(left_sql: str, op: str, right_sql: str) -> str:
    """Compose an ordering predicate that excludes NaN but preserves ±Inf."""

    if op not in {"<", ">"}:
        raise ValueError(f"unsupported floating-point comparison operator: {op}")
    left_is_ordered = f"(isfinite({left_sql}) OR isinf({left_sql}))"
    right_is_ordered = f"(isfinite({right_sql}) OR isinf({right_sql}))"
    return f"{left_is_ordered} AND {right_is_ordered} AND ({left_sql} {op} {right_sql})"


def _price_cte(dataset_dir: str | Path) -> str:
    # The scan expression rejects quotes and contains no operator-controlled SQL.
    return f"WITH price AS (SELECT * FROM {price_scan_sql(dataset_dir)})"  # noqa: S608


def _ohlc_predicate() -> str:
    return " OR ".join(
        f"({nan_guarded_cmp(left, op, right)})" for left, op, right in _OHLC_COMPARISONS
    )


def _bad_value_predicate() -> str:
    non_finite = " OR ".join(f"NOT isfinite({column})" for column in _FLOAT_COLUMNS)
    return f"{non_finite} OR ({nan_guarded_cmp('volume', '<', '0')})"


def _parity_query_body(*, grouped: bool) -> str:
    delta_violation = nan_guarded_cmp("abs(uc_c - uc_t)", ">", "1e-6")
    select_sql = (
        "SELECT year, count(*) AS violation_count FROM violations GROUP BY year"
        if grouped
        else "SELECT count(*) FROM violations"
    )
    return _PARITY_QUERY_PREFIX + delta_violation + "\n)\n" + select_sql


def ohlc_anomaly_count_sql(dataset_dir: str | Path) -> str:
    return f"{_price_cte(dataset_dir)}\nSELECT count(*) FROM price\nWHERE {_ohlc_predicate()}"


def duplicate_key_count_sql(dataset_dir: str | Path) -> str:
    return (
        _price_cte(dataset_dir)  # noqa: S608 - quote-rejected read-only scan.
        + """
SELECT coalesce(sum(key_count - 1), 0)
FROM (
    SELECT count(*) AS key_count
    FROM price
    GROUP BY adjustment_type, year, assetid, date
    HAVING count(*) > 1
) AS duplicate_groups"""
    )


def bad_value_count_sql(dataset_dir: str | Path) -> str:
    return f"{_price_cte(dataset_dir)}\nSELECT count(*) FROM price\nWHERE {_bad_value_predicate()}"


def basis_parity_violation_count_sql(dataset_dir: str | Path) -> str:
    return _price_cte(dataset_dir) + _parity_query_body(grouped=False)


def ohlc_anomaly_counts_by_partition_sql(dataset_dir: str | Path) -> str:
    return (
        f"{_price_cte(dataset_dir)}\n"
        "SELECT adjustment_type, year, "
        f"count(*) FILTER (WHERE {_ohlc_predicate()}) AS violation_count\n"
        "FROM price GROUP BY adjustment_type, year"
    )


def duplicate_key_counts_by_partition_sql(dataset_dir: str | Path) -> str:
    return (
        _price_cte(dataset_dir)  # noqa: S608 - quote-rejected read-only scan.
        + """
SELECT adjustment_type, year, sum(key_count - 1) AS duplicate_count
FROM (
    SELECT adjustment_type, year, assetid, date, count(*) AS key_count
    FROM price
    GROUP BY adjustment_type, year, assetid, date
) AS keys
GROUP BY adjustment_type, year"""
    )


def bad_value_counts_by_partition_sql(dataset_dir: str | Path) -> str:
    return (
        f"{_price_cte(dataset_dir)}\n"
        "SELECT adjustment_type, year, "
        f"count(*) FILTER (WHERE {_bad_value_predicate()}) AS bad_value_count\n"
        "FROM price GROUP BY adjustment_type, year"
    )


def basis_parity_violation_counts_by_year_sql(dataset_dir: str | Path) -> str:
    return _price_cte(dataset_dir) + _parity_query_body(grouped=True)


def _count(connection: _QueryConnection, sql: str) -> int:
    row = connection.sql(sql).fetchone()
    if row is None or not isinstance(row[0], int):
        raise RuntimeError("DuckDB count query did not return an integer")
    return row[0]


def _partition_counts(connection: _QueryConnection, sql: str) -> PartitionCounts:
    counts: PartitionCounts = {}
    for adjustment, year, count in connection.sql(sql).fetchall():
        if not isinstance(adjustment, str) or not isinstance(year, int):
            raise TypeError("DuckDB partition count query returned an invalid key")
        if not isinstance(count, int):
            raise TypeError("DuckDB partition count query returned a non-integer count")
        counts[(adjustment, year)] = count
    return counts


def _year_counts(connection: _QueryConnection, sql: str) -> dict[int, int]:
    counts: dict[int, int] = {}
    for year, count in connection.sql(sql).fetchall():
        if not isinstance(year, int) or not isinstance(count, int):
            raise TypeError("DuckDB year count query returned invalid values")
        counts[year] = count
    return counts


def count_ohlc_anomalies(connection: _QueryConnection, dataset_dir: str | Path) -> int:
    return _count(connection, ohlc_anomaly_count_sql(dataset_dir))


def count_duplicate_keys(connection: _QueryConnection, dataset_dir: str | Path) -> int:
    return _count(connection, duplicate_key_count_sql(dataset_dir))


def count_bad_values(connection: _QueryConnection, dataset_dir: str | Path) -> int:
    return _count(connection, bad_value_count_sql(dataset_dir))


def count_basis_parity_violations(
    connection: _QueryConnection,
    dataset_dir: str | Path,
) -> int:
    return _count(connection, basis_parity_violation_count_sql(dataset_dir))


def count_ohlc_anomalies_by_partition(
    connection: _QueryConnection,
    dataset_dir: str | Path,
) -> PartitionCounts:
    return _partition_counts(
        connection,
        ohlc_anomaly_counts_by_partition_sql(dataset_dir),
    )


def count_duplicate_keys_by_partition(
    connection: _QueryConnection,
    dataset_dir: str | Path,
) -> PartitionCounts:
    return _partition_counts(
        connection,
        duplicate_key_counts_by_partition_sql(dataset_dir),
    )


def count_bad_values_by_partition(
    connection: _QueryConnection,
    dataset_dir: str | Path,
) -> PartitionCounts:
    return _partition_counts(
        connection,
        bad_value_counts_by_partition_sql(dataset_dir),
    )


def count_basis_parity_violations_by_year(
    connection: _QueryConnection,
    dataset_dir: str | Path,
) -> dict[int, int]:
    return _year_counts(
        connection,
        basis_parity_violation_counts_by_year_sql(dataset_dir),
    )
