"""Section 8.1 universe-manifest composition from terminated list walks.

The builder combines the active-symbol and delisted-company list walks
(``FmpCollector.walk_list_endpoint`` results) into the typed
``UniverseManifest`` that ``parse_universe_manifest`` validates. Symbols stay
provider labels: no identity resolution, no joins, no fuzzy matching, and no
network access. A source's ``raw_content_sha256`` is the canonical-content
hash of the ordered per-page SHA-256 digests of every raw page body the walk
consumed, including the empty page whose retry confirmed exhaustion
(section 8.5); its ``retrieved_at_utc`` is the latest page retrieval
timestamp (the maximum) - the first moment the composed source bytes were
fully present, regardless of arrival-order clock skew. The raw pages must
reproduce the walk exactly: one page per recorded observation, in order,
each carrying its recorded record count.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

from aegis_alpha.data.fmp_windows import (
    ListPageOutcome,
    ListWalk,
    ManifestSource,
    UniverseEntry,
    UniverseManifest,
)
from aegis_alpha.data.serialization import content_sha256

#: Section 6.1: both universe list endpoints paginate with this identity key.
SYMBOL_IDENTITY_KEY: Final = "symbol"


class UniverseCompositionError(ValueError):
    """The list-walk outputs cannot compose a valid section 8.1 manifest."""


@dataclass(frozen=True, slots=True)
class RawListPage:
    """One raw list-endpoint page body with its retrieval timestamp."""

    body: bytes
    retrieved_at_utc: datetime

    def __post_init__(self) -> None:
        if self.retrieved_at_utc.tzinfo is None or self.retrieved_at_utc.utcoffset() is None:
            raise UniverseCompositionError("page retrieval timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ListWalkSource:
    """A list walk plus the raw pages it consumed, in page order."""

    endpoint: str
    walk: ListWalk
    pages: tuple[RawListPage, ...]

    def __post_init__(self) -> None:
        if not self.endpoint.strip():
            raise UniverseCompositionError("endpoint identity cannot be empty")


def build_universe_manifest(
    *,
    generated_at_utc: datetime,
    active: ListWalkSource,
    delisted: ListWalkSource,
) -> UniverseManifest:
    """Compose the active and delisted list walks into a universe manifest.

    Symbols are composed verbatim as provider labels. Cross-list overlap,
    mixed-case labels, in-source duplicates, and future delisted dates are
    provider facts preserved here and classified later. An active-list record
    carrying a ``delistedDate`` is still preserved as returned.
    """

    if generated_at_utc.tzinfo is None or generated_at_utc.utcoffset() is None:
        raise UniverseCompositionError("generated_at_utc must be timezone-aware")
    active_entries = _source_entries(active, active=True)
    delisted_entries = _source_entries(delisted, active=False)
    return UniverseManifest(
        generated_at_utc=generated_at_utc,
        sources=(_source_provenance(active), _source_provenance(delisted)),
        entries=(*active_entries, *delisted_entries),
    )


def universe_manifest_document(manifest: UniverseManifest) -> dict[str, object]:
    """Project a typed manifest onto its section 8.1 JSON document shape."""

    return {
        "generated_at_utc": manifest.generated_at_utc,
        "sources": [
            {
                "endpoint": source.endpoint,
                "retrieved_at_utc": source.retrieved_at_utc,
                "raw_content_sha256": source.raw_content_sha256,
            }
            for source in manifest.sources
        ],
        "entries": [
            {
                "symbol": entry.symbol,
                "ipoDate": entry.ipo_date,
                "delistedDate": entry.delisted_date,
                "active": entry.active,
            }
            for entry in manifest.entries
        ],
    }


def _source_entries(source: ListWalkSource, *, active: bool) -> tuple[UniverseEntry, ...]:
    entries: list[UniverseEntry] = []
    seen: set[str] = set()
    for record in _source_records(source):
        symbol = _record_symbol(source.endpoint, record)
        folded = symbol.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        delisted_date = _record_date(source.endpoint, record, "delistedDate")
        entries.append(
            UniverseEntry(
                symbol=symbol,
                ipo_date=_record_date(source.endpoint, record, "ipoDate"),
                delisted_date=delisted_date,
                active=active,
            )
        )
    return tuple(entries)


def _source_records(source: ListWalkSource) -> tuple[Mapping[str, object], ...]:
    if source.walk.identity_key != SYMBOL_IDENTITY_KEY:
        raise UniverseCompositionError(
            f"universe sources must paginate with the {SYMBOL_IDENTITY_KEY!r} identity key: "
            f"{source.endpoint} uses {source.walk.identity_key!r}"
        )
    observations = source.walk.observations
    if not observations or observations[-1].outcome is not ListPageOutcome.COMPLETE:
        raise UniverseCompositionError(
            f"list walk for {source.endpoint} did not terminate complete"
        )
    if len(source.pages) != len(observations):
        raise UniverseCompositionError(
            f"raw page count for {source.endpoint} does not match the walk's recorded observations"
        )
    records: list[Mapping[str, object]] = []
    for observation, page in zip(observations, source.pages, strict=True):
        page_records = _page_records(source.endpoint, page.body)
        if len(page_records) != observation.record_count:
            raise UniverseCompositionError(
                f"raw page {observation.page} for {source.endpoint} does not match "
                "the walk's recorded record count"
            )
        records.extend(page_records)
    identities = tuple(_record_symbol(source.endpoint, record) for record in records)
    if identities != source.walk.identities:
        raise UniverseCompositionError(
            f"raw pages for {source.endpoint} do not match the walk's recorded identities"
        )
    return tuple(records)


def _page_records(endpoint: str, body: bytes) -> tuple[Mapping[str, object], ...]:
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise UniverseCompositionError(f"raw page for {endpoint} is not valid JSON") from None
    if not isinstance(parsed, list):
        raise UniverseCompositionError(f"raw page for {endpoint} must be a JSON array")
    records: list[Mapping[str, object]] = []
    for record in parsed:
        if not isinstance(record, Mapping):
            raise UniverseCompositionError(f"list records for {endpoint} must be JSON objects")
        records.append(record)
    return tuple(records)


def _record_symbol(endpoint: str, record: Mapping[str, object]) -> str:
    value = record.get(SYMBOL_IDENTITY_KEY)
    if not isinstance(value, str) or not value.strip():
        raise UniverseCompositionError(
            f"list record for {endpoint} is missing its {SYMBOL_IDENTITY_KEY!r} identity"
        )
    return value


def _record_date(endpoint: str, record: Mapping[str, object], key: str) -> date | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise UniverseCompositionError(f"{endpoint} record {key!r} must be an ISO date or null")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise UniverseCompositionError(
            f"{endpoint} record {key!r} must be an ISO calendar date"
        ) from None


def _source_provenance(source: ListWalkSource) -> ManifestSource:
    # ``_source_records`` ran first: a complete walk consumed at least one page.
    digests = tuple(hashlib.sha256(page.body).hexdigest() for page in source.pages)
    return ManifestSource(
        endpoint=source.endpoint,
        retrieved_at_utc=max(page.retrieved_at_utc for page in source.pages),
        raw_content_sha256=content_sha256(digests),
    )
