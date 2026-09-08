from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Final, cast

#: Section 8.4: exactly 1,500 calendar days per backfill chunk.
BACKFILL_CHUNK_DAYS: Final = 1500
#: Section 8.4: the provider record cap that signals truncation.
MAX_RECORDS_PER_WINDOW: Final = 5000
#: Section 6.1/8.5: list endpoints page with ``limit=1000`` starting at page 0.
LIST_PAGE_LIMIT: Final = 1000
#: A first page larger than this limit is an unpaged dump and completes the walk.
#: Later pages still fail closed at the limit.
#: Fail-closed ceiling on paginated list pages once an effective page size is known.
MAX_LIST_PAGES: Final = 10_000
#: Page 0 only teaches a paging size when it is at least this large (FMP delisted).
MIN_LEARNED_LIST_PAGE_SIZE: Final = 100

#: Section 8.3: revalidation reaches 5 collected trading days behind the watermark.
REVALIDATION_TRADING_DAYS: Final = 5

_SHA256_HEX_LENGTH: Final = 64
_SHA256_HEX_DIGITS: Final = frozenset("0123456789abcdef")


class CollectorContractError(RuntimeError):
    """Guardrail G3: contract drift that blocks the whole run."""


@dataclass(frozen=True, slots=True)
class DateWindow:
    """An inclusive naive calendar-date window (section 6.1 ``from``/``to``)."""

    start: date
    end: date

    def __post_init__(self) -> None:
        for label, value in (("start", self.start), ("end", self.end)):
            if isinstance(value, datetime) or not isinstance(value, date):
                raise TypeError(f"window {label} must be a naive calendar date")
        if self.end < self.start:
            raise ValueError("window end cannot precede its start")

    @property
    def day_count(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def is_single_day(self) -> bool:
        return self.start == self.end

    def contains(self, value: date) -> bool:
        return self.start <= value <= self.end

    def as_parameters(self) -> dict[str, str]:
        return {"from": self.start.isoformat(), "to": self.end.isoformat()}


def split_window(window: DateWindow) -> tuple[DateWindow, DateWindow]:
    """Split ``[a, b]`` into ``[a, m]`` and ``[m + 1, b]`` with ``m = a + (b - a) / 2``."""

    if window.is_single_day:
        raise ValueError("a single-day window is irreducible and cannot be split")
    midpoint = window.start + timedelta(days=(window.end - window.start).days // 2)
    return (
        DateWindow(window.start, midpoint),
        DateWindow(midpoint + timedelta(days=1), window.end),
    )


class WindowOutcome(StrEnum):
    COMPLETE = "complete"
    COVERAGE_GAP = "coverage_gap"
    SPLIT_REQUIRED = "split_required"
    IRREDUCIBLE_TRUNCATION = "irreducible_truncation"


def classify_window_response(window: DateWindow, record_count: int) -> WindowOutcome:
    """Map a validated response size onto the section 8.4 truncation rules."""

    if record_count < 0:
        raise ValueError("record_count cannot be negative")
    if record_count > MAX_RECORDS_PER_WINDOW:
        raise CollectorContractError(
            f"window returned more than the {MAX_RECORDS_PER_WINDOW}-record provider cap"
        )
    if record_count == MAX_RECORDS_PER_WINDOW:
        if window.is_single_day:
            return WindowOutcome.IRREDUCIBLE_TRUNCATION
        return WindowOutcome.SPLIT_REQUIRED
    if record_count == 0:
        return WindowOutcome.COVERAGE_GAP
    return WindowOutcome.COMPLETE


def validate_window_dates(window: DateWindow, observed: Iterable[date]) -> None:
    """Section 8.4: returned dates outside ``[from, to]`` block the run (G3)."""

    outside = sorted({value for value in observed if not window.contains(value)})
    if outside:
        raise CollectorContractError(
            "response returned dates outside the requested window "
            f"[{window.start.isoformat()}, {window.end.isoformat()}]: "
            f"{outside[0].isoformat()}"
        )


@dataclass(frozen=True, slots=True)
class ManifestSource:
    endpoint: str
    retrieved_at_utc: datetime
    raw_content_sha256: str


@dataclass(frozen=True, slots=True)
class UniverseEntry:
    symbol: str
    ipo_date: date | None
    delisted_date: date | None
    active: bool


@dataclass(frozen=True, slots=True)
class UniverseManifest:
    generated_at_utc: datetime
    sources: tuple[ManifestSource, ...]
    entries: tuple[UniverseEntry, ...]

    def symbols(self) -> tuple[str, ...]:
        return tuple(entry.symbol for entry in self.entries)


def partition_universe_manifest(
    manifest: UniverseManifest,
    *,
    shard_index: int,
    shard_total: int,
) -> UniverseManifest:
    """Deterministically assign symbols to one shard of a parallel collection.

    Assignment is stable across processes (sha256 of the uppercase symbol), the
    shards are disjoint, and their union is the full manifest. Raises when a
    shard would be empty so an operator never silently collects nothing.
    """

    if shard_total < 1 or not 1 <= shard_index <= shard_total:
        raise ValueError("universe manifest shard coordinates are out of range")
    kept = tuple(
        entry
        for entry in manifest.entries
        if int(
            hashlib.sha256(entry.symbol.upper().encode()).hexdigest()[:16],
            16,
        )
        % shard_total
        == shard_index - 1
    )
    if not kept:
        raise ValueError(f"universe manifest shard {shard_index}/{shard_total} selected no symbols")
    return replace(manifest, entries=kept)


def _require_mapping(label: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object with string keys")
    return cast("Mapping[str, object]", value)


def _require_str(label: str, container: Mapping[str, object], key: str) -> str:
    value = container.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} requires a nonempty string {key!r}")
    return value


def _require_bool(label: str, container: Mapping[str, object], key: str) -> bool:
    value = container.get(key)
    if not isinstance(value, bool):
        raise ValueError(  # noqa: TRY004 - manifest validation, not a call-site type bug
            f"{label} requires a boolean {key!r}"
        )
    return value


def _require_sha256(label: str, container: Mapping[str, object], key: str) -> str:
    value = _require_str(label, container, key)
    if len(value) != _SHA256_HEX_LENGTH or any(
        character not in _SHA256_HEX_DIGITS for character in value
    ):
        raise ValueError(f"{label} {key!r} must be lowercase SHA-256 hexadecimal")
    return value


def _require_utc_timestamp(label: str, container: Mapping[str, object], key: str) -> datetime:
    raw = _require_str(label, container, key)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"{label} {key!r} must be an RFC 3339 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} {key!r} must be timezone-aware")
    return parsed


def _optional_date(label: str, container: Mapping[str, object], key: str) -> date | None:
    if key not in container:
        raise ValueError(f"{label} requires {key!r} (use null when unknown)")
    value = container[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(  # noqa: TRY004 - manifest validation, not a call-site type bug
            f"{label} {key!r} must be an ISO date or null"
        )
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{label} {key!r} must be an ISO calendar date") from None


def _parse_manifest_sources(document: Mapping[str, object]) -> tuple[ManifestSource, ...]:
    raw_sources = document.get("sources")
    if not isinstance(raw_sources, Sequence) or isinstance(raw_sources, (str, bytes)):
        raise ValueError(  # noqa: TRY004 - manifest validation, not a call-site type bug
            "universe manifest requires a 'sources' array"
        )
    if not raw_sources:
        raise ValueError("universe manifest requires at least one provenance source")
    sources: list[ManifestSource] = []
    for raw_source in raw_sources:
        source = _require_mapping("universe manifest source", raw_source)
        sources.append(
            ManifestSource(
                endpoint=_require_str("universe manifest source", source, "endpoint"),
                retrieved_at_utc=_require_utc_timestamp(
                    "universe manifest source", source, "retrieved_at_utc"
                ),
                raw_content_sha256=_require_sha256(
                    "universe manifest source", source, "raw_content_sha256"
                ),
            )
        )
    return tuple(sources)


def _parse_manifest_entries(document: Mapping[str, object]) -> tuple[UniverseEntry, ...]:
    raw_entries = document.get("entries")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, (str, bytes)):
        raise ValueError(  # noqa: TRY004 - manifest validation, not a call-site type bug
            "universe manifest requires an 'entries' array"
        )
    entries: list[UniverseEntry] = []
    seen: set[tuple[str, bool]] = set()
    for raw_entry in raw_entries:
        entry = _require_mapping("universe manifest entry", raw_entry)
        symbol = _require_str("universe manifest entry", entry, "symbol")
        active = _require_bool("universe manifest entry", entry, "active")
        key = (symbol.casefold(), active)
        if key in seen:
            raise ValueError(f"universe manifest contains a duplicate symbol: {symbol!r}")
        seen.add(key)
        entries.append(
            UniverseEntry(
                symbol=symbol,
                ipo_date=_optional_date("universe manifest entry", entry, "ipoDate"),
                delisted_date=_optional_date("universe manifest entry", entry, "delistedDate"),
                active=active,
            )
        )
    if not entries:
        raise ValueError("universe manifest requires at least one symbol entry")
    return tuple(entries)


def parse_universe_manifest(document: object) -> UniverseManifest:
    """Section 8.1: strict manifest validation before any window is planned."""

    manifest = _require_mapping("universe manifest", document)
    return UniverseManifest(
        generated_at_utc=_require_utc_timestamp("universe manifest", manifest, "generated_at_utc"),
        sources=_parse_manifest_sources(manifest),
        entries=_parse_manifest_entries(manifest),
    )


@dataclass(frozen=True, slots=True)
class IncrementalPlan:
    backfill_required: bool
    window: DateWindow | None


def plan_incremental_window(
    *,
    as_of: date,
    watermark: date | None,
    collected_dates: Sequence[date],
) -> IncrementalPlan:
    """Section 8.3: revalidate 5 collected trading days behind the watermark."""

    if watermark is None:
        return IncrementalPlan(backfill_required=True, window=None)
    if watermark > as_of:
        raise ValueError("watermark cannot exceed the frozen as-of date")
    eligible = sorted({value for value in collected_dates if value <= watermark})
    if not eligible:
        # The collected data is its own calendar; with no collected dates the
        # watermark itself is the only defensible lower bound.
        start = watermark
    elif len(eligible) > REVALIDATION_TRADING_DAYS:
        start = eligible[-(REVALIDATION_TRADING_DAYS + 1)]
    else:
        start = eligible[0]
    return IncrementalPlan(backfill_required=False, window=DateWindow(start, as_of))


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    windows: tuple[DateWindow, ...]
    bounded_range: DateWindow | None
    skipped_reason: str | None


def plan_backfill_windows(
    *,
    entry: UniverseEntry,
    operator_from: date,
    as_of: date,
) -> BackfillPlan:
    """Section 8.4: manifest-bounded 1,500-day chunks laid out newest-first."""

    range_start = operator_from if entry.ipo_date is None else max(operator_from, entry.ipo_date)
    range_end = as_of if entry.delisted_date is None else min(as_of, entry.delisted_date)
    if range_end < range_start:
        return BackfillPlan(windows=(), bounded_range=None, skipped_reason="empty_range")

    bounded_range = DateWindow(range_start, range_end)
    windows: list[DateWindow] = []
    chunk_end = range_end
    while chunk_end >= range_start:
        chunk_start = max(range_start, chunk_end - timedelta(days=BACKFILL_CHUNK_DAYS - 1))
        windows.append(DateWindow(chunk_start, chunk_end))
        if chunk_start == range_start:
            break
        chunk_end = chunk_start - timedelta(days=1)
    return BackfillPlan(windows=tuple(windows), bounded_range=bounded_range, skipped_reason=None)


@dataclass(frozen=True, slots=True)
class WindowSplitRecord:
    parent: DateWindow
    first: DateWindow
    second: DateWindow

    def as_receipt_entry(self) -> dict[str, str]:
        return {
            "parent_from": self.parent.start.isoformat(),
            "parent_to": self.parent.end.isoformat(),
            "first_from": self.first.start.isoformat(),
            "first_to": self.first.end.isoformat(),
            "second_from": self.second.start.isoformat(),
            "second_to": self.second.end.isoformat(),
        }


class WindowWalk:
    """Newest-first window queue that records every truncation split."""

    def __init__(self, windows: Sequence[DateWindow]) -> None:
        self._pending: list[DateWindow] = list(windows)
        self._splits: list[WindowSplitRecord] = []

    def next_window(self) -> DateWindow | None:
        if not self._pending:
            return None
        return self._pending.pop(0)

    def record_split(self, window: DateWindow) -> tuple[DateWindow, DateWindow]:
        first, second = split_window(window)
        # Newest chunk first, matching the backward layout of section 8.4.
        self._pending.insert(0, first)
        self._pending.insert(0, second)
        self._splits.append(WindowSplitRecord(parent=window, first=first, second=second))
        return (first, second)

    @property
    def pending(self) -> tuple[DateWindow, ...]:
        return tuple(self._pending)

    @property
    def splits(self) -> tuple[WindowSplitRecord, ...]:
        return tuple(self._splits)


class ListPageOutcome(StrEnum):
    CONTINUE = "continue"
    COMPLETE = "complete"
    RETRY_REQUIRED = "retry_required"
    ENDPOINT_BLOCKED = "endpoint_blocked"


@dataclass(frozen=True, slots=True)
class ListPageObservation:
    page: int
    record_count: int
    retried: bool
    outcome: ListPageOutcome


class ListWalk:
    """Section 8.5: paginated list walk with retry-confirmed termination.

    Page 0 may exceed ``LIST_PAGE_LIMIT`` when the provider returns the full
    list unpaged; that dump completes the walk. A later page over the limit
    remains contract drift.

    When page 0 returns at least ``MIN_LEARNED_LIST_PAGE_SIZE`` and at most
    ``LIST_PAGE_LIMIT`` records, that count is the provider's effective page
    size. The walk continues until a shorter page or a retried empty page after
    a full page. A smaller first page still completes. A page matching the
    learned size is never treated as the last page merely because it is smaller
    than the requested ``limit``.

    Walk-level checks are transport and pagination only. Duplicate identities
    are preserved as observed unless a later page adds none while the walk is
    still incomplete; missing identity keys still fail closed.
    """

    def __init__(self, identity_key: str) -> None:
        if not identity_key.strip():
            raise ValueError("identity_key cannot be empty")
        self._identity_key = identity_key
        self._identities: list[str] = []
        self._seen: set[str] = set()
        self._observations: list[ListPageObservation] = []
        self._last_page_was_full = False
        self._effective_page_size: int | None = None

    @property
    def identity_key(self) -> str:
        return self._identity_key

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(self._identities)

    @property
    def observations(self) -> tuple[ListPageObservation, ...]:
        return tuple(self._observations)

    def as_receipt_pages(self) -> list[dict[str, object]]:
        return [
            {
                "page": observation.page,
                "record_count": observation.record_count,
                "retried": observation.retried,
                "outcome": observation.outcome.value,
            }
            for observation in self._observations
        ]

    def observe(
        self,
        *,
        page: int,
        records: Sequence[Mapping[str, object]],
        retried: bool = False,
    ) -> ListPageOutcome:
        self._require_expected_page(page)
        if len(records) > LIST_PAGE_LIMIT and (page != 0 or retried or self._observations):
            raise CollectorContractError(
                f"list page returned more than the {LIST_PAGE_LIMIT}-record page limit"
            )

        outcome = self._page_outcome(page=page, record_count=len(records), retried=retried)
        if outcome in {ListPageOutcome.CONTINUE, ListPageOutcome.COMPLETE} and records:
            self._absorb_identities(records, completing=outcome is ListPageOutcome.COMPLETE)
        self._observations.append(
            ListPageObservation(
                page=page,
                record_count=len(records),
                retried=retried,
                outcome=outcome,
            )
        )
        if records:
            # Only a page that actually returned records can establish the
            # "empty page following a full page" exhaustion evidence; the
            # first empty attempt must not erase it before its retry.
            self._last_page_was_full = len(records) == self._page_size
        return outcome

    def _require_expected_page(self, page: int) -> None:
        expected = 0 if not self._observations else self._next_expected_page()
        if page != expected:
            raise ValueError(f"list walk expected page {expected}, received {page}")

    def _next_expected_page(self) -> int:
        last = self._observations[-1]
        if last.outcome is ListPageOutcome.RETRY_REQUIRED:
            return last.page
        return last.page + 1

    @property
    def _page_size(self) -> int:
        return LIST_PAGE_LIMIT if self._effective_page_size is None else self._effective_page_size

    def _page_outcome(self, *, page: int, record_count: int, retried: bool) -> ListPageOutcome:  # noqa: PLR0911 - page outcomes stay adjacent
        if record_count == 0:
            if not retried:
                return ListPageOutcome.RETRY_REQUIRED
            if page == 0 or not self._last_page_was_full:
                return ListPageOutcome.ENDPOINT_BLOCKED
            return ListPageOutcome.COMPLETE
        if page == 0 and record_count > LIST_PAGE_LIMIT:
            return ListPageOutcome.COMPLETE
        if page == 0:
            if record_count < MIN_LEARNED_LIST_PAGE_SIZE:
                return ListPageOutcome.COMPLETE
            self._effective_page_size = record_count
        elif record_count > self._page_size:
            raise CollectorContractError(
                f"list page returned more than the learned {self._page_size}-record page size"
            )
        if record_count < self._page_size:
            return ListPageOutcome.COMPLETE
        if page + 1 >= MAX_LIST_PAGES:
            raise CollectorContractError(
                f"list walk exceeded the {MAX_LIST_PAGES}-page runaway ceiling"
            )
        return ListPageOutcome.CONTINUE

    def _absorb_identities(
        self,
        records: Sequence[Mapping[str, object]],
        *,
        completing: bool,
    ) -> None:
        page_identities: list[str] = []
        new_identities = 0
        for record in records:
            value = record.get(self._identity_key)
            if not isinstance(value, str) or not value.strip():
                raise CollectorContractError(
                    f"list record is missing its {self._identity_key!r} identity key"
                )
            if value not in self._seen:
                new_identities += 1
            page_identities.append(value)
        if not completing and records and new_identities == 0:
            raise CollectorContractError("list page made no identity progress")
        self._seen.update(page_identities)
        self._identities.extend(page_identities)
