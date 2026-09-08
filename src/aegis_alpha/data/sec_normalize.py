"""First-wave SEC normalization: submissions, company facts, and 13F index.

Normalized rows pad CIK to 10 digits and keep the unpadded source beside them.
Values are preserved, never computed. Full Archives document mirroring is out
of scope.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Final, cast

from aegis_alpha.data.sec_identity import pad_cik

PROVIDER: Final = "sec"
NORMALIZED_VERSION: Final = "sec-normalized-v1"
THIRTEEN_F_FORMS: Final = frozenset({"13F-HR", "13F-HR/A"})
SUBMISSIONS_DATASET: Final = "sec_submissions"
FACTS_DATASET: Final = "sec_facts"
THIRTEEN_F_DATASET: Final = "sec_13f_index"

SUBMISSIONS_COLUMNS: Final = (
    "cik",
    "cik_source",
    "accession_number",
    "form",
    "filed",
    "accepted_at",
    "primary_document",
    "instrument_id",
    "issuer_id",
    "snapshot_id",
    "raw_content_sha256",
)
FACTS_COLUMNS: Final = (
    "cik",
    "cik_source",
    "taxonomy",
    "tag",
    "unit",
    "period_end",
    "filed",
    "fy",
    "fp",
    "frame",
    "value",
    "accession_number",
    "instrument_id",
    "issuer_id",
    "snapshot_id",
    "raw_content_sha256",
)
THIRTEEN_F_COLUMNS: Final = SUBMISSIONS_COLUMNS


class ContractError(ValueError):
    """A first-wave SEC payload violated the frozen G-A contract."""


@dataclass(frozen=True, slots=True)
class NormalizationContext:
    cik_source: str
    instrument_id: str
    issuer_id: str
    snapshot_id: str
    raw_content_sha256: str


@dataclass(frozen=True, slots=True)
class FilingCursor:
    filing_date: date
    accession_number: str
    accepted_at: datetime

    @property
    def watermark_value(self) -> str:
        return f"{self.filing_date.isoformat()}|{self.accession_number}"


def _require_mapping(label: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{label} must be a JSON object")
    return cast("Mapping[str, object]", value)


def parse_json_object(body: bytes, *, label: str) -> Mapping[str, object]:
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"{label} body is not valid JSON") from error
    return _require_mapping(label, parsed)


def extract_cik_source(payload: Mapping[str, object]) -> str:
    raw = payload.get("cik")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
        return str(raw)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    raise ContractError("payload is missing a CIK")


def cik_prefix(cik: str) -> str:
    return cik[:3]


def _parse_date(label: str, value: object) -> date:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{label} must be an ISO calendar date")
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError as error:
        raise ContractError(f"{label} must be an ISO calendar date") from error


def _parse_accepted(label: str, value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{label} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ContractError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractError(f"{label} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _string_list(container: Mapping[str, object], name: str) -> tuple[str, ...]:
    raw = container.get(name)
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ContractError(f"filings.recent.{name} must be an array")
    values: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            raise ContractError(f"filings.recent.{name} entries must be strings")
        values.append(item)
    return tuple(values)


def _parallel_filings(recent: Mapping[str, object]) -> tuple[dict[str, str], ...]:
    names = (
        "accessionNumber",
        "filingDate",
        "acceptanceDateTime",
        "form",
        "primaryDocument",
    )
    columns = {name: _string_list(recent, name) for name in names}
    lengths = {len(values) for values in columns.values()}
    if len(lengths) != 1:
        raise ContractError("filings.recent parallel arrays have unequal lengths")
    count = next(iter(lengths))
    return tuple({name: columns[name][index] for name in names} for index in range(count))


def normalize_submissions(
    payload: Mapping[str, object],
    context: NormalizationContext,
) -> tuple[dict[str, object], ...]:
    cik = pad_cik(context.cik_source)
    filings = _require_mapping("filings", payload.get("filings"))
    recent = _require_mapping("filings.recent", filings.get("recent"))
    rows: list[dict[str, object]] = []
    for filing in _parallel_filings(recent):
        accepted_at = _parse_accepted("acceptanceDateTime", filing["acceptanceDateTime"])
        rows.append(
            {
                "cik": cik,
                "cik_source": context.cik_source,
                "accession_number": filing["accessionNumber"],
                "form": filing["form"],
                "filed": _parse_date("filingDate", filing["filingDate"]).isoformat(),
                "accepted_at": accepted_at,
                "primary_document": filing["primaryDocument"],
                "instrument_id": context.instrument_id,
                "issuer_id": context.issuer_id,
                "snapshot_id": context.snapshot_id,
                "raw_content_sha256": context.raw_content_sha256,
            }
        )
    return tuple(rows)


def extract_13f_index(rows: Sequence[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    return tuple(dict(row) for row in rows if str(row.get("form")) in THIRTEEN_F_FORMS)


def latest_filing_cursor(rows: Sequence[Mapping[str, object]]) -> FilingCursor | None:
    cursors: list[FilingCursor] = []
    for row in rows:
        accepted = row["accepted_at"]
        if not isinstance(accepted, datetime):
            raise ContractError("accepted_at must remain a datetime on normalized rows")
        cursors.append(
            FilingCursor(
                filing_date=date.fromisoformat(str(row["filed"])),
                accession_number=str(row["accession_number"]),
                accepted_at=accepted,
            )
        )
    if not cursors:
        return None
    return max(cursors, key=lambda item: (item.filing_date, item.accession_number))


def cursor_is_newer(cursor: FilingCursor, watermark_value: str | None) -> bool:
    if watermark_value is None or not watermark_value.strip():
        return True
    return cursor.watermark_value > watermark_value


def fact_sort_key(row: Mapping[str, object]) -> tuple[str, ...]:
    """Stable first-wave fact identity. Hash or JSON key order must not drop tags."""

    return tuple(
        "" if row.get(name) is None else str(row.get(name))
        for name in (
            "cik",
            "taxonomy",
            "tag",
            "unit",
            "period_end",
            "filed",
            "fy",
            "fp",
            "accession_number",
        )
    )


def sort_fact_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [dict(row) for row in sorted(rows, key=fact_sort_key)]


def normalize_companyfacts(
    payload: Mapping[str, object],
    context: NormalizationContext,
) -> tuple[dict[str, object], ...]:
    cik = pad_cik(context.cik_source)
    facts = _require_mapping("facts", payload.get("facts"))
    rows: list[dict[str, object]] = []
    for taxonomy, taxonomy_value in sorted(facts.items(), key=lambda item: item[0]):
        tags = _require_mapping(f"facts.{taxonomy}", taxonomy_value)
        for tag, tag_value in sorted(tags.items(), key=lambda item: item[0]):
            tag_object = _require_mapping(f"facts.{taxonomy}.{tag}", tag_value)
            units = _require_mapping(f"facts.{taxonomy}.{tag}.units", tag_object.get("units"))
            for unit, observations in sorted(units.items(), key=lambda item: item[0]):
                if not isinstance(observations, Sequence) or isinstance(observations, (str, bytes)):
                    raise ContractError(f"facts.{taxonomy}.{tag}.units.{unit} must be an array")
                for observation in observations:
                    item = _require_mapping(f"facts.{taxonomy}.{tag}.units.{unit}[]", observation)
                    period_end = item.get("end")
                    if period_end is None:
                        period_end = item.get("instant")
                    rows.append(
                        {
                            "cik": cik,
                            "cik_source": context.cik_source,
                            "taxonomy": taxonomy,
                            "tag": tag,
                            "unit": unit,
                            "period_end": None
                            if period_end is None
                            else _parse_date("period_end", period_end).isoformat(),
                            "filed": _parse_date("filed", item.get("filed")).isoformat(),
                            "fy": item.get("fy"),
                            "fp": item.get("fp"),
                            "frame": item.get("frame"),
                            "value": item.get("val"),
                            "accession_number": item.get("accn"),
                            "instrument_id": context.instrument_id,
                            "issuer_id": context.issuer_id,
                            "snapshot_id": context.snapshot_id,
                            "raw_content_sha256": context.raw_content_sha256,
                        }
                    )
    return tuple(sort_fact_rows(rows))


def partition_values(cik: str, moment: datetime | date) -> dict[str, str]:
    return {"cik_prefix": cik_prefix(cik), "year": f"{moment.year:04d}"}


def partition_key(cik: str, moment: datetime | date) -> tuple[str, str]:
    values = partition_values(cik, moment)
    return (values["cik_prefix"], values["year"])


def row_partition_moment(row: Mapping[str, object], dataset: str) -> datetime | date:
    if dataset in {SUBMISSIONS_DATASET, THIRTEEN_F_DATASET}:
        accepted = row.get("accepted_at")
        if isinstance(accepted, datetime):
            return accepted
        filed = row.get("filed")
        if isinstance(filed, str):
            return date.fromisoformat(filed)
        raise ContractError("submission rows need accepted_at or filed")
    filed = row.get("filed")
    if isinstance(filed, str):
        return date.fromisoformat(filed)
    raise ContractError("fact rows need filed")
