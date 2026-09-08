"""Read-only SEC vs FMP disagreement sidecar (ADR 0004).

Disagreements are diagnostic evidence. This module never updates FMP, Norgate,
or canonical rows. The 009 build engine must not consume this sidecar as a
price-repair input; that change is outside AAS-DATA-013.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from aegis_alpha.data.sec_identity import pad_cik
from aegis_alpha.data.serialization import canonical_json_bytes

DISAGREEMENT_DATASET: Final = "sec_disagreements"
REVENUE_TAG: Final = "Revenues"
DIVIDEND_TAGS: Final = frozenset(
    {
        "CommonStockDividendsPerShareDeclared",
        "PaymentsOfDividends",
        "Dividends",
    }
)
SPLIT_TAGS: Final = frozenset(
    {
        "StockholdersEquityNoteStockSplitConversionRatio",
        "ShareIssued",
    }
)


class DisagreementError(ValueError):
    """The optional FMP comparison view is unusable."""


@dataclass(frozen=True, slots=True)
class ComparableFact:
    cik: str
    field: str
    period: str | None
    value: object
    snapshot_id: str
    source: str


@dataclass(frozen=True, slots=True)
class DisagreementRow:
    cik: str
    field: str
    period: str | None
    sec_value: object
    fmp_value: object
    sec_snapshot_id: str
    fmp_snapshot_id: str
    observed_at_utc: datetime

    def projection(self) -> dict[str, object]:
        return {
            "cik": self.cik,
            "field": self.field,
            "fmp_snapshot_id": self.fmp_snapshot_id,
            "fmp_value": self.fmp_value,
            "observed_at_utc": self.observed_at_utc,
            "period": self.period,
            "sec_snapshot_id": self.sec_snapshot_id,
            "sec_value": self.sec_value,
        }


def _require_mapping(label: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DisagreementError(f"{label} must be a JSON object")
    return cast("Mapping[str, object]", value)


def load_fmp_comparison(path: Path) -> tuple[ComparableFact, ...]:
    document = json.loads(path.read_bytes())
    payload = _require_mapping("FMP comparison", document)
    snapshot_id = payload.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise DisagreementError("FMP comparison snapshot_id must be a nonempty string")
    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
        raise DisagreementError("FMP comparison rows must be an array")
    facts: list[ComparableFact] = []
    for raw in raw_rows:
        row = _require_mapping("FMP comparison row", raw)
        cik_source = row.get("cik")
        if not isinstance(cik_source, str) or not cik_source.strip():
            continue
        field = row.get("field")
        if not isinstance(field, str) or not field.strip():
            raise DisagreementError("FMP comparison field must be a nonempty string")
        period = row.get("period")
        facts.append(
            ComparableFact(
                cik=pad_cik(cik_source),
                field=field,
                period=None if period is None else str(period),
                value=row.get("value"),
                snapshot_id=snapshot_id,
                source="fmp",
            )
        )
    return tuple(facts)


def sec_comparable_facts(
    fact_rows: Sequence[Mapping[str, object]],
    *,
    snapshot_id: str,
) -> tuple[ComparableFact, ...]:
    facts: list[ComparableFact] = []
    for row in fact_rows:
        tag = str(row.get("tag"))
        field = _field_for_tag(tag)
        if field is None:
            continue
        period = row.get("period_end")
        facts.append(
            ComparableFact(
                cik=str(row["cik"]),
                field=field,
                period=None if period is None else str(period),
                value=row.get("value"),
                snapshot_id=snapshot_id,
                source="sec",
            )
        )
    return tuple(facts)


def _field_for_tag(tag: str) -> str | None:
    if tag == REVENUE_TAG:
        return "revenue"
    if tag in DIVIDEND_TAGS:
        return "dividend"
    if tag in SPLIT_TAGS:
        return "split"
    return None


def compare_disagreements(
    sec_facts: Sequence[ComparableFact],
    fmp_facts: Sequence[ComparableFact],
    *,
    observed_at_utc: datetime,
) -> tuple[DisagreementRow, ...]:
    """Emit one row per (cik, field, period) whose values differ. No source mutation."""

    if observed_at_utc.tzinfo is None or observed_at_utc.utcoffset() is None:
        raise DisagreementError("observed_at_utc must be timezone-aware")
    sec_index = _index(sec_facts)
    fmp_index = _index(fmp_facts)
    keys = sorted(set(sec_index) & set(fmp_index))
    rows: list[DisagreementRow] = []
    for key in keys:
        sec_fact = sec_index[key]
        fmp_fact = fmp_index[key]
        if _canonical(sec_fact.value) == _canonical(fmp_fact.value):
            continue
        rows.append(
            DisagreementRow(
                cik=key[0],
                field=key[1],
                period=key[2],
                sec_value=sec_fact.value,
                fmp_value=fmp_fact.value,
                sec_snapshot_id=sec_fact.snapshot_id,
                fmp_snapshot_id=fmp_fact.snapshot_id,
                observed_at_utc=observed_at_utc.astimezone(UTC),
            )
        )
    return tuple(rows)


def _index(
    facts: Sequence[ComparableFact],
) -> dict[tuple[str, str, str | None], ComparableFact]:
    indexed: dict[tuple[str, str, str | None], ComparableFact] = {}
    for fact in facts:
        indexed[(fact.cik, fact.field, fact.period)] = fact
    return indexed


def _canonical(value: object) -> bytes:
    return canonical_json_bytes(value)


def sidecar_relative_path(year: int) -> Path:
    return Path("disagreements") / f"year={year:04d}" / "part-000.parquet"
