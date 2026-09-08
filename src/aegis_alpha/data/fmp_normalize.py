from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from aegis_alpha.data.fmp_windows import CollectorContractError

PROVIDER: Final = "fmp"


class ProviderRecordSkippedError(Exception):
    """A single provider row cannot be typed; the rest of the response is kept."""


def canonical_symbol(symbol: str) -> str:
    """Canonical form used only to *compare* provider labels with manifest symbols.

    Section 8.1 stores manifest symbols casefolded while the provider returns
    its own label casing. Comparison therefore happens on this canonical form,
    but the provider label itself is always persisted verbatim (004B 1.12): a
    symbol is a provider label, never an identity, and this function creates no
    join key.
    """

    return symbol.strip().casefold()


class FieldType(StrEnum):
    STRING = "str"
    DATE = "date"
    TIMESTAMP = "timestamp"
    FLOAT = "float64"
    INTEGER = "int64"
    BOOLEAN = "bool"


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    field_type: FieldType
    required: bool


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """One provider-normalized dataset; bases are never mixed or derived."""

    dataset: str
    endpoint: str
    fields: tuple[FieldSpec, ...]

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.fields)


#: Section 6.2: provenance columns carried by every row of every dataset.
PROVENANCE_FIELDS: Final = (
    FieldSpec("provider", FieldType.STRING, required=True),
    FieldSpec("source_receipt_id", FieldType.STRING, required=True),
    FieldSpec("raw_content_sha256", FieldType.STRING, required=True),
    FieldSpec("retrieved_at_utc", FieldType.TIMESTAMP, required=True),
)

_PRICE_EOD_FULL = DatasetSpec(
    dataset="fmp_price_eod_full",
    endpoint="/stable/historical-price-eod/full",
    fields=(
        FieldSpec("symbol", FieldType.STRING, required=True),
        FieldSpec("date", FieldType.DATE, required=True),
        FieldSpec("open", FieldType.FLOAT, required=True),
        FieldSpec("high", FieldType.FLOAT, required=True),
        FieldSpec("low", FieldType.FLOAT, required=True),
        FieldSpec("close", FieldType.FLOAT, required=True),
        FieldSpec("volume", FieldType.INTEGER, required=True),
        FieldSpec("change", FieldType.FLOAT, required=False),
        FieldSpec("changePercent", FieldType.FLOAT, required=False),
        FieldSpec("vwap", FieldType.FLOAT, required=False),
    ),
)


def _adjusted_spec(dataset: str, endpoint: str) -> DatasetSpec:
    return DatasetSpec(
        dataset=dataset,
        endpoint=endpoint,
        fields=(
            FieldSpec("symbol", FieldType.STRING, required=True),
            FieldSpec("date", FieldType.DATE, required=True),
            FieldSpec("adjOpen", FieldType.FLOAT, required=True),
            FieldSpec("adjHigh", FieldType.FLOAT, required=True),
            FieldSpec("adjLow", FieldType.FLOAT, required=True),
            FieldSpec("adjClose", FieldType.FLOAT, required=True),
            FieldSpec("volume", FieldType.INTEGER, required=True),
        ),
    )


_PRICE_EOD_NON_SPLIT_ADJUSTED = _adjusted_spec(
    "fmp_price_eod_non_split_adjusted",
    "/stable/historical-price-eod/non-split-adjusted",
)
_PRICE_EOD_DIVIDEND_ADJUSTED = _adjusted_spec(
    "fmp_price_eod_dividend_adjusted",
    "/stable/historical-price-eod/dividend-adjusted",
)

_SPLITS = DatasetSpec(
    dataset="fmp_splits",
    endpoint="/stable/splits",
    fields=(
        FieldSpec("symbol", FieldType.STRING, required=True),
        FieldSpec("date", FieldType.DATE, required=True),
        FieldSpec("numerator", FieldType.FLOAT, required=True),
        FieldSpec("denominator", FieldType.FLOAT, required=True),
        FieldSpec("splitType", FieldType.STRING, required=False),
    ),
)

_DIVIDENDS = DatasetSpec(
    dataset="fmp_dividends",
    endpoint="/stable/dividends",
    fields=(
        FieldSpec("symbol", FieldType.STRING, required=True),
        FieldSpec("date", FieldType.DATE, required=True),
        FieldSpec("dividend", FieldType.FLOAT, required=True),
        FieldSpec("adjDividend", FieldType.FLOAT, required=True),
        FieldSpec("declarationDate", FieldType.DATE, required=False),
        FieldSpec("recordDate", FieldType.DATE, required=False),
        FieldSpec("paymentDate", FieldType.DATE, required=False),
        FieldSpec("frequency", FieldType.STRING, required=False),
        FieldSpec("yield", FieldType.FLOAT, required=False),
    ),
)

_PROFILE = DatasetSpec(
    dataset="fmp_profile",
    endpoint="/stable/profile",
    fields=(
        FieldSpec("symbol", FieldType.STRING, required=True),
        FieldSpec("companyName", FieldType.STRING, required=False),
        FieldSpec("exchange", FieldType.STRING, required=False),
        FieldSpec("exchangeFullName", FieldType.STRING, required=False),
        FieldSpec("currency", FieldType.STRING, required=False),
        FieldSpec("country", FieldType.STRING, required=False),
        FieldSpec("sector", FieldType.STRING, required=False),
        FieldSpec("industry", FieldType.STRING, required=False),
        FieldSpec("cik", FieldType.STRING, required=False),
        FieldSpec("cusip", FieldType.STRING, required=False),
        FieldSpec("isin", FieldType.STRING, required=False),
        FieldSpec("isActivelyTrading", FieldType.BOOLEAN, required=False),
        FieldSpec("isAdr", FieldType.BOOLEAN, required=False),
        FieldSpec("isEtf", FieldType.BOOLEAN, required=False),
        FieldSpec("isFund", FieldType.BOOLEAN, required=False),
        FieldSpec("ipoDate", FieldType.DATE, required=False),
        FieldSpec("price", FieldType.FLOAT, required=False),
        FieldSpec("beta", FieldType.FLOAT, required=False),
        FieldSpec("marketCap", FieldType.FLOAT, required=False),
        FieldSpec("lastDividend", FieldType.FLOAT, required=False),
    ),
)

DATASET_SPECS: Final[Mapping[str, DatasetSpec]] = MappingProxyType(
    {
        spec.dataset: spec
        for spec in (
            _PRICE_EOD_FULL,
            _PRICE_EOD_NON_SPLIT_ADJUSTED,
            _PRICE_EOD_DIVIDEND_ADJUSTED,
            _SPLITS,
            _DIVIDENDS,
            _PROFILE,
        )
    }
)

#: The three price bases stay separate and are never derived from one another.
PRICE_BASIS_DATASETS: Final = (
    _PRICE_EOD_FULL.dataset,
    _PRICE_EOD_NON_SPLIT_ADJUSTED.dataset,
    _PRICE_EOD_DIVIDEND_ADJUSTED.dataset,
)

ENDPOINT_DATASETS: Final[Mapping[str, str]] = MappingProxyType(
    {spec.endpoint: spec.dataset for spec in DATASET_SPECS.values()}
)


@dataclass(frozen=True, slots=True)
class Provenance:
    source_receipt_id: str
    raw_content_sha256: str
    retrieved_at_utc: datetime

    def __post_init__(self) -> None:
        if not self.source_receipt_id.strip():
            raise ValueError("source_receipt_id cannot be empty")
        if not self.raw_content_sha256.strip():
            raise ValueError("raw_content_sha256 cannot be empty")
        if self.retrieved_at_utc.tzinfo is None or self.retrieved_at_utc.utcoffset() is None:
            raise ValueError("retrieved_at_utc must be timezone-aware UTC")


@dataclass(frozen=True, slots=True)
class NormalizationResult:
    dataset: str
    rows: tuple[Mapping[str, object], ...]
    unknown_fields: tuple[str, ...] = field(default=())
    skipped_records: int = 0


def _coerce_string(dataset: str, name: str, value: object) -> str:
    if not isinstance(value, str):
        raise CollectorContractError(f"{dataset}.{name} must be a string")
    return value


def _coerce_date(dataset: str, name: str, value: object) -> date:
    if not isinstance(value, str):
        raise CollectorContractError(f"{dataset}.{name} must be an ISO calendar date string")
    text = value.split(" ")[0]
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise CollectorContractError(
            f"{dataset}.{name} must be an ISO calendar date string"
        ) from None


def _coerce_timestamp(dataset: str, name: str, value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise CollectorContractError(
                f"{dataset}.{name} must be an RFC 3339 timestamp"
            ) from None
    else:
        raise CollectorContractError(f"{dataset}.{name} must be an RFC 3339 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CollectorContractError(f"{dataset}.{name} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _coerce_float(dataset: str, name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CollectorContractError(f"{dataset}.{name} must be a float64 number")
    return float(value)


def _coerce_integer(dataset: str, name: str, value: object) -> int:
    if isinstance(value, bool):
        raise CollectorContractError(f"{dataset}.{name} must be an int64 value")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, float):
        raise ProviderRecordSkippedError(f"{dataset}.{name} is a non-integral float")
    raise CollectorContractError(f"{dataset}.{name} must be an int64 value")


def _coerce_boolean(dataset: str, name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise CollectorContractError(f"{dataset}.{name} must be a boolean")
    return value


_COERCIONS: Final = {
    FieldType.STRING: _coerce_string,
    FieldType.DATE: _coerce_date,
    FieldType.TIMESTAMP: _coerce_timestamp,
    FieldType.FLOAT: _coerce_float,
    FieldType.INTEGER: _coerce_integer,
    FieldType.BOOLEAN: _coerce_boolean,
}


def _typed_value(spec: DatasetSpec, field_spec: FieldSpec, record: Mapping[str, object]) -> object:
    if field_spec.name not in record:
        if field_spec.required:
            raise CollectorContractError(
                f"{spec.dataset} is missing required field {field_spec.name!r}"
            )
        return None
    value = record[field_spec.name]
    if value is None:
        if field_spec.required:
            raise CollectorContractError(
                f"{spec.dataset} required field {field_spec.name!r} cannot be null"
            )
        return None
    if (
        not field_spec.required
        and field_spec.field_type is FieldType.DATE
        and isinstance(value, str)
        and not value.strip()
    ):
        return None
    # Nullability governs presence of a value, never its type: a wrong-typed
    # value in any typed field, required or nullable, blocks the run (G3).
    return _COERCIONS[field_spec.field_type](spec.dataset, field_spec.name, value)


def normalize_records(
    *,
    dataset: str,
    records: Sequence[Mapping[str, object]],
    provenance: Provenance,
    symbol: str | None = None,
) -> NormalizationResult:
    """Turn validated raw records into typed provider-normalized rows."""

    spec = DATASET_SPECS.get(dataset)
    if spec is None:
        raise CollectorContractError(f"unknown provider-normalized dataset: {dataset!r}")

    known = set(spec.field_names)
    unknown: set[str] = set()
    rows: list[Mapping[str, object]] = []
    skipped_records = 0
    for record in records:
        if not isinstance(record, Mapping):
            raise CollectorContractError(f"{dataset} records must be JSON objects")
        unknown.update(name for name in record if name not in known)
        try:
            row: dict[str, object] = {
                field_spec.name: _typed_value(spec, field_spec, record)
                for field_spec in spec.fields
            }
        except ProviderRecordSkippedError:
            skipped_records += 1
            continue
        if symbol is not None:
            returned = row.get("symbol")
            if not isinstance(returned, str) or canonical_symbol(returned) != canonical_symbol(
                symbol
            ):
                raise CollectorContractError(
                    f"{dataset} returned a record for another symbol than {symbol!r}"
                )
        row["provider"] = PROVIDER
        row["source_receipt_id"] = provenance.source_receipt_id
        row["raw_content_sha256"] = provenance.raw_content_sha256
        # Section 6.2: ``retrieved_at_utc`` is also the profile observation
        # date used for partitioning, so it is required in every dataset.
        row["retrieved_at_utc"] = provenance.retrieved_at_utc
        rows.append(MappingProxyType(row))

    return NormalizationResult(
        dataset=dataset,
        rows=tuple(rows),
        unknown_fields=tuple(sorted(unknown)),
        skipped_records=skipped_records,
    )


def normalized_columns(dataset: str) -> tuple[str, ...]:
    spec = DATASET_SPECS.get(dataset)
    if spec is None:
        raise CollectorContractError(f"unknown provider-normalized dataset: {dataset!r}")
    return spec.field_names + tuple(item.name for item in PROVENANCE_FIELDS)


def collected_dates(rows: Sequence[Mapping[str, object]]) -> tuple[date, ...]:
    """Ordered trade dates present in normalized price rows."""

    values = {row["date"] for row in rows if isinstance(row.get("date"), date)}
    return tuple(sorted(value for value in values if isinstance(value, date)))
