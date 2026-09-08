"""Exact Arrow price schema and parquet media type for catalog-pinned reads."""

from __future__ import annotations

from typing import Final

import pyarrow as pa

PARQUET_MEDIA_TYPE: Final = "application/vnd.apache.parquet"

PRICE_SCHEMA: Final = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("observation_id", pa.string()),
        ("instrument_id", pa.string()),
        ("issuer_id", pa.string()),
        ("observation_date", pa.date32()),
        ("adjustment_basis", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("unadjusted_close", pa.float64()),
        ("dividend", pa.float64()),
        ("currency", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("available_at", pa.timestamp("us", tz="UTC")),
        ("source_provider", pa.string()),
        ("source_dataset_id", pa.string()),
        ("source_dataset_version", pa.string()),
        ("source_snapshot_id", pa.string()),
        ("source_artifact_path", pa.string()),
        ("source_artifact_sha256", pa.string()),
        ("source_row_ordinal", pa.int64()),
        ("quality_flags", pa.string()),
    ]
)
