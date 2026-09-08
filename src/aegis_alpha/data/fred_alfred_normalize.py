"""Provider-normalized FRED/ALFRED observations with a mandatory vintage axis."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from io import BytesIO
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq

from aegis_alpha.data.fred_alfred_series import PROVIDER

SCHEMA_ID: Final = "fred_alfred_observations"
SCHEMA_VERSION: Final = 1
DATASET_VERSION: Final = f"{SCHEMA_ID}.v{SCHEMA_VERSION}"
CURRENT_VINTAGE_END: Final = date(9999, 12, 31)
MISSING_VALUE_TOKEN: Final = "."  # noqa: S105 - FRED missing-observation token, not a secret

OBSERVATION_KEY: Final[tuple[str, ...]] = (
    "series_id",
    "observation_date",
    "realtime_start",
    "realtime_end",
)

NORMALIZED_COLUMNS: Final[tuple[str, ...]] = (
    *OBSERVATION_KEY,
    "value",
    "availability_time_utc",
    "source_snapshot_id",
    "raw_content_sha256",
    "retrieved_at_utc",
    "provider",
)

SERIES_DIMENSION_COLUMNS: Final[tuple[str, ...]] = (
    "series_id",
    "title",
    "units",
    "frequency",
    "seasonal_adjustment",
    "source_snapshot_id",
)

NORMALIZED_SCHEMA: Final = pa.schema(
    [
        ("series_id", pa.string()),
        ("observation_date", pa.date32()),
        ("realtime_start", pa.date32()),
        ("realtime_end", pa.date32()),
        ("value", pa.string()),
        ("availability_time_utc", pa.timestamp("us", tz="UTC")),
        ("source_snapshot_id", pa.string()),
        ("raw_content_sha256", pa.string()),
        ("retrieved_at_utc", pa.timestamp("us", tz="UTC")),
        ("provider", pa.string()),
    ]
)

SERIES_DIMENSION_SCHEMA: Final = pa.schema(
    [
        ("series_id", pa.string()),
        ("title", pa.string()),
        ("units", pa.string()),
        ("frequency", pa.string()),
        ("seasonal_adjustment", pa.string()),
        ("source_snapshot_id", pa.string()),
    ]
)


class NormalizeError(ValueError):
    """Provider payload violated the frozen observation contract."""


@dataclass(frozen=True, slots=True)
class ObservationKey:
    series_id: str
    observation_date: date
    realtime_start: date
    realtime_end: date

    def as_tuple(self) -> tuple[str, date, date, date]:
        return (
            self.series_id,
            self.observation_date,
            self.realtime_start,
            self.realtime_end,
        )


def parse_fred_date(label: str, value: object) -> date:
    if not isinstance(value, str):
        raise NormalizeError(f"{label} must be an ISO calendar date string")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise NormalizeError(f"{label} must be an ISO calendar date string") from None


def observation_key(row: Mapping[str, object]) -> ObservationKey:
    series_id = row.get("series_id")
    if not isinstance(series_id, str) or not series_id:
        raise NormalizeError("series_id is required")
    observation_date = row.get("observation_date")
    realtime_start = row.get("realtime_start")
    realtime_end = row.get("realtime_end")
    if not isinstance(observation_date, date):
        raise NormalizeError("observation_date must be a date")
    if not isinstance(realtime_start, date) or not isinstance(realtime_end, date):
        raise NormalizeError("vintage dates must be dates")
    return ObservationKey(
        series_id=series_id,
        observation_date=observation_date,
        realtime_start=realtime_start,
        realtime_end=realtime_end,
    )


def assert_unique_keys(rows: Sequence[Mapping[str, object]]) -> None:
    seen: set[tuple[str, date, date, date]] = set()
    for row in rows:
        key = observation_key(row).as_tuple()
        if key in seen:
            raise NormalizeError(
                "duplicate natural key (series_id, observation_date, realtime_start, realtime_end)"
            )
        seen.add(key)


def normalize_value(raw: object) -> str | None:
    """Keep the provider string; map FRED's ``.`` missing token to null."""

    if not isinstance(raw, str):
        raise NormalizeError("observation value must be a string")
    if raw == MISSING_VALUE_TOKEN:
        return None
    return raw


def normalize_observations(  # noqa: PLR0913 - each argument is one frozen observation field
    records: Sequence[Mapping[str, object]],
    *,
    series_id: str,
    source_snapshot_id: str,
    raw_content_sha256: str,
    retrieved_at_utc: datetime,
    availability_time_utc: datetime,
) -> tuple[dict[str, object], ...]:
    if retrieved_at_utc.tzinfo is None or availability_time_utc.tzinfo is None:
        raise NormalizeError("collector timestamps must be timezone-aware")
    rows: list[dict[str, object]] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise NormalizeError("observation records must be JSON objects")
        rows.append(
            {
                "series_id": series_id,
                "observation_date": parse_fred_date("date", record.get("date")),
                "realtime_start": parse_fred_date("realtime_start", record.get("realtime_start")),
                "realtime_end": parse_fred_date("realtime_end", record.get("realtime_end")),
                "value": normalize_value(record.get("value")),
                "availability_time_utc": availability_time_utc.astimezone(UTC),
                "source_snapshot_id": source_snapshot_id,
                "raw_content_sha256": raw_content_sha256,
                "retrieved_at_utc": retrieved_at_utc.astimezone(UTC),
                "provider": PROVIDER,
            }
        )
    assert_unique_keys(rows)
    return tuple(rows)


def normalize_series_dimension(
    record: Mapping[str, object],
    *,
    series_id: str,
    source_snapshot_id: str,
) -> dict[str, object]:
    if not isinstance(record, Mapping):
        raise NormalizeError("series metadata must be a JSON object")
    returned = record.get("id")
    if returned != series_id:
        raise NormalizeError(f"series metadata id {returned!r} does not match {series_id}")
    return {
        "series_id": series_id,
        "title": _optional_string(record.get("title")),
        "units": _optional_string(record.get("units")),
        "frequency": _optional_string(record.get("frequency")),
        "seasonal_adjustment": _optional_string(record.get("seasonal_adjustment")),
        "source_snapshot_id": source_snapshot_id,
    }


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise NormalizeError("series dimension fields must be strings")
    return value


def observations_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    assert_unique_keys(rows)
    return pa.Table.from_pylist([dict(row) for row in rows], schema=NORMALIZED_SCHEMA)


def series_dimension_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    return pa.Table.from_pylist([dict(row) for row in rows], schema=SERIES_DIMENSION_SCHEMA)


def parquet_bytes(table: pa.Table) -> bytes:
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def partition_relative_path(series_id: str, year: int, *, part_name: str) -> str:
    return f"series_id={series_id}/year={year}/{part_name}"


__all__ = [
    "CURRENT_VINTAGE_END",
    "DATASET_VERSION",
    "MISSING_VALUE_TOKEN",
    "NORMALIZED_COLUMNS",
    "NORMALIZED_SCHEMA",
    "OBSERVATION_KEY",
    "SCHEMA_ID",
    "SCHEMA_VERSION",
    "SERIES_DIMENSION_COLUMNS",
    "SERIES_DIMENSION_SCHEMA",
    "NormalizeError",
    "ObservationKey",
    "assert_unique_keys",
    "normalize_observations",
    "normalize_series_dimension",
    "normalize_value",
    "observation_key",
    "observations_table",
    "parquet_bytes",
    "parse_fred_date",
    "partition_relative_path",
    "series_dimension_table",
]
