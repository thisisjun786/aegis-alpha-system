"""Maintenance promotion: each dataset chain continued with the sources collected since.

A route names one dataset of the catalog, the registered mapper that builds it from one
collected source shape (a source ID prefix and table), and, for a shape that several
datasets share, the predicate every row of a routed table satisfies (an exchange's
symbols, partial-response rows of a KR exchange). A table whose rows do not all satisfy
it is left to the other routes of the shape; an empty table is skipped. A table that
lacks a column the route's mapper reads (its ``source_columns``) is another shape under
the same prefix and table name (the ``korea.public_response@1`` legacy import of
``kind-listings``): it is skipped and counted as ``shape``. A route may also name a value
``check`` every row of a table it claims must pass: the KR daily bulk routes check each
warned row as the clean response path does (``qveris_native._bar``: valid JSON, finite
JSON numbers, prices above zero, volume not negative, consistent OHLC, an ISO date). A
table with one failing row is not promoted and is counted as ``value_check`` (before
``mixed``), so a warned response never passes unchecked.

A route continues a chain; it never starts one. Its template is the spec of the latest
committed generation of the dataset that used one of the route's mappers: its own, or
an earlier major it ``continues`` (an operator's backfill or an earlier maintenance
generation). That generation's mapper is the one the chain continues with, so
``classifications.kr.kind`` continues a chain whose latest generation used
``kind.industry@1`` under ``@1`` and one an operator published under ``kind.industry@2``
under ``@2``. A route that names ``derive_from`` (the KR daily bulk routes) and finds no
such generation in a chain that has a head takes its first template from the latest
committed spec of the ``derive_from`` mapper (the history backfill's ``eodhd.bars@1`` or
``eodhd.bars_adjusted@1``): only the mapper's name becomes the route's and
``derive_args`` (the KR ``currencies``) join its arguments, so the template is the spec
``aas data kr-prices`` writes for a bulk day. The report's ``template_spec_sha256`` is
then the spec it came from and ``derived_from`` names its mapper. Once the chain holds a
generation of the route's mapper, that spec is the template. A chain without a head is
never derived.

A route may also declare a ``start``: the mapper arguments, time rule and numeric rules of the
first generation of a dataset collected before any chain existed (``actions.*.eodhd``,
``fx.usdkrw.eodhd``, ``status.kr.kind``). It is used only for a dataset the caller names in
``start``, and only while the chain holds no generation of the route's mappers (with or
without a head: the dividends route of a chain its splits route started starts as well). The
declared calendar dataset's committed head is the time rule's calendar pin (none:
``no_calendar``) and a template that resolves identities pins the run's snapshot (none:
``identity_unavailable``); the report says ``started``. Once a started generation exists,
its spec is the template like any other. Without ``start`` such a dataset reports ``no_head``
with its waiting sources. A maintenance spec is that spec with only these fields advanced:

- ``target.parent``: the dataset head;
- ``sources``: the one new source table;
- ``partition``: null when the template's is null; the source's vintage partitions in
  order (``fred.alfred@1``, one generation each); otherwise ``[first, last + 1 day)`` of the
  mapper's partition dates in the table;
- every generation pin in the mapper, time-rule and quality-rule arguments: the committed
  head of the pinned dataset now (the calendar a refresh advanced, the SEC filings a
  promotion before this one published);
- ``identity_snapshot``: the maintenance identity snapshot when the run has one and the
  template pins a snapshot. When the run's identity stage failed, a route whose template
  pins a snapshot promotes nothing (``identity_unavailable``) and its sources wait for
  the next run;
- ``tombstone_policy``: ``never``. One collected source is no full snapshot, so a
  maintenance generation never removes a record.

The mapper and its arguments, the time, numeric and quality rules stay those of the
template, so a chain's rules never change under maintenance. Dataset generations of a
later time-rule generation (``<dataset>.r<N>``) are continued the same way, each from its
own template.

New sources are taken in order of their ``sl:`` link time, then source ID. A source is
done for a dataset once all its steps were promoted or found unchanged: maintenance then
records a ``maintain_source@1`` quality check on the dataset's head version naming the
pin and each step's request and generation (``promoted`` or ``unchanged``). A source
whose rows the pinned snapshot left ``unresolved`` or ``ambiguous`` is recorded
``unresolved`` with the snapshot and the SHA-256 of the rows that snapshot resolves the
mapper's identity key with; it is planned again once the snapshot a run would pin
resolves that key with other rows, so a listing registered later reaches its rows. When
every step of the source has a partition, the record names that scope (``key_scope``,
the first ``from`` and last ``to``) and the SHA-256 covers the key's long-lived rows and
only the short ones (valid at most ``SHORT_DAYS`` days, such as a symbol list's per-day
claims) that can hold at any instant of those dates in any time zone, because a mapper may
resolve at a row's own instant rather than at the start of its date
(``aas-maintain-key-scope-v1``). A claim registered for a later day then leaves the
source settled, so daily identity increments do not plan every waiting source again; a
record without a scope keeps the whole key's SHA-256. A refused or
blocked step records nothing, so the source is planned again on the next run; a run that
stopped between the steps of one source plans its done steps again as empty deltas.
Sources an operator's promotion pinned are planned once more on the first maintenance
run and recorded as unchanged (an older download than the head is ``stale`` and never
replaces it). A source whose outcome-declaring mapper recorded ``promotion_coverage@1`` is
done as well.

Every pass also reports ``standing_unresolved``: per dataset, the sources whose latest
``maintain_source@1`` record is ``unresolved``, whatever this pass planned, read from those
persisted records alone (``promote.unresolved`` stays the datasets this pass recorded).
"""

from __future__ import annotations

import hashlib
import json
import re
from bisect import bisect_left, bisect_right
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from functools import cache
from itertools import pairwise
from typing import TYPE_CHECKING, Final, Literal, cast
from zoneinfo import ZoneInfo

import duckdb

from aegis_alpha.storage.calendar_declaration import packaged_declaration, parse_declaration
from aegis_alpha.storage.dataset_catalog import CATALOG, catalog_entry
from aegis_alpha.storage.kr_identity import KIND_REQUIRED
from aegis_alpha.storage.kr_prices import CURRENCIES as KR_CURRENCIES
from aegis_alpha.storage.market import generation_chain
from aegis_alpha.storage.membership_pins import IdentityPin, membership_parts
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers import mapper
from aegis_alpha.storage.promotion.mappers.fred import vintage_partitions
from aegis_alpha.storage.promotion.spec import SPEC_SCHEMA
from aegis_alpha.storage.promotion.time_rules import TIME_COLUMNS
from aegis_alpha.storage.provider_collection import committed_tables
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_library import list_sources, list_tables
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

CHECK_RULE: Final = ("maintain_source", "1")
UNRESOLVED: Final = "unresolved"
_LINK: Final = "sl:"
SCOPE_FORMAT: Final = "aas-maintain-key-scope-v1"
SHORT_DAYS: Final = 8
_DAY_US: Final = 86_400_000_000
_ZONE_US: Final = 14 * 3_600_000_000
"""The widest UTC offset: every local instant of a date lies within it of the UTC date."""
_PIN_KEYS: Final = frozenset({"dataset_id", "version", "generation_id", "chain_hash",
                              "manifest_hash"})  # fmt: skip
_EXCHANGE: Final = (
    "CASE WHEN json_valid(source_row_json) THEN "
    "json_extract_string(source_row_json, '$.exchange_short_name') END"
)
_US_SYMBOL: Final = "provider_symbol LIKE '%.US'"
_KR_SYMBOL: Final = "(provider_symbol LIKE '%.KO' OR provider_symbol LIKE '%.KQ')"
_KR_PARTIAL: Final = f"reason = 'provider_reported_partial' AND {_EXCHANGE} IN ('KO', 'KQ')"
_KR_BULK_ARGS: Final = {"currencies": dict(KR_CURRENCIES)}
_ROW: Final = "(CASE WHEN json_valid(source_row_json) THEN source_row_json END)"


def _row_number(name: str) -> str:
    """A provider row's field as DOUBLE when it is a JSON number; NULL otherwise."""
    path = f"'$.{name}'"
    return (
        f"(CASE WHEN json_type({_ROW}, {path}) IN ('UBIGINT', 'BIGINT', 'DOUBLE') "
        f"THEN TRY_CAST(json_extract_string({_ROW}, {path}) AS DOUBLE) END)"
    )


def _bar_values() -> str:
    """A held bulk row's values pass the clean response path's bar checks.

    Those of ``qveris_native._bar``: valid JSON; open, high, low, close, adjusted close and
    volume finite JSON numbers (no text, null or boolean); prices above zero, volume not
    negative; low at most min(open, close), high at least max(open, close), low at most
    high; an ISO ``YYYY-MM-DD`` date.
    """
    o, h, low, c, a, v = map(
        _row_number, ("open", "high", "low", "close", "adjusted_close", "volume")
    )
    day = f"json_extract_string({_ROW}, '$.date')"
    return " AND ".join(
        [
            *(f"isfinite({value})" for value in (o, h, low, c, a, v)),
            *(f"{value} > 0" for value in (o, h, low, c, a)),
            f"{v} >= 0",
            f"{low} <= least({o}, {c})",
            f"{h} >= greatest({o}, {c})",
            f"{low} <= {h}",
            f"json_type({_ROW}, '$.date') = 'VARCHAR'",
            f"regexp_full_match({day}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')",
            f"TRY_CAST({day} AS DATE) IS NOT NULL",
        ]
    )


_KR_VALUES: Final = _bar_values()
_SUMMARY: Final = (
    "generation_id",
    "published",
    "empty_delta",
    "reused",
    "source_rows",
    "rows",
    "operations",
    "unchanged",
    "stale",
    "time_drift",
    "flags",
    "unresolved_token_count",
    "partition_row_count",
    "coverage_check",
    "blocking",
    "refusals",
)


@dataclass(frozen=True, slots=True)
class Start:
    """The first template of a route's dataset: what an operator's first spec would declare.

    ``time_rule`` is the rule document both time columns use; a ``calendar`` argument names
    the calendar dataset whose committed head it pins. ``identity`` says whether the template
    pins the run's identity snapshot; ``partitioned`` whether each step has a partition.
    """

    args: Mapping[str, object]
    time_rule: Mapping[str, object]
    decimal_rule: Mapping[str, str]
    identity: bool = True
    partitioned: bool = True


@dataclass(frozen=True, slots=True)
class Route:
    """One dataset built by one mapper from one collected source shape."""

    dataset_id: str
    mapper: str
    prefix: str
    table: str
    accept: str | None = None
    continues: tuple[str, ...] = ()
    derive_from: str | None = None
    """The mapper whose latest spec is the first template when the chain has none of its own."""
    derive_args: Mapping[str, object] | None = None
    """Arguments a derived template adds to the mapper's."""
    check: str | None = None
    """The predicate every row of a table the route claims must satisfy (``value_check``)."""
    start: Start | None = None
    """The first template of a dataset no chain holds yet, used when the caller names it."""

    @property
    def mappers(self) -> tuple[str, ...]:
        """The route's mapper and the earlier majors whose chains it continues."""
        return (self.mapper, *self.continues)


def _actions(calendar: str, zone: str, numbers: Mapping[str, str]) -> Start:
    """An EODHD action chain's first template: known at the ex-date's open on ``calendar``."""
    venue = calendar.rsplit(".", 1)[1].upper()
    rule = {"rule": "exdate_open@1", "basis": "record", "input": "ex_date",
            "args": {"calendar": calendar, "calendar_id": venue, "venue": venue}}  # fmt: skip
    return Start({"timezone": zone}, rule, numbers)


# EODHD's daily FOREX bars follow the UTC day; a close is known at the XNYS close of its date
# plus four hours, the end of the UTC day under New York daylight time (later in winter).
FX_LAG_US: Final = 4 * 3_600_000_000
_FX_START: Final = Start(
    {"base": "USD", "quote": "KRW", "timezone": "UTC"},
    {"rule": "session_close_plus_lag@1", "basis": "record", "input": "fixing_date",
     "args": {"calendar": "sessions.xnys", "calendar_id": "XNYS", "venue": "XNYS",
              "lag_us": FX_LAG_US}},
    {"rate": "float_shortest@1"},
    identity=False,
)  # fmt: skip
# A KIND listing is known by the end of its Seoul listing day, as a Norgate first date is.
_STATUS_START: Final = Start(
    {},
    {"rule": "local_day_end@1", "basis": "record", "input": "status_date",
     "args": {"timezone": "Asia/Seoul"}},
    {},
)  # fmt: skip

# In dependency order: SEC facts take each filing's acceptance from the filings head.
ROUTES: Final = (
    Route("filings.us.sec", "sec.submissions@1", "sec-submissions-filings-", "filings"),
    Route("fundamentals.us.sec", "sec.companyfacts@1", "sec-companyfacts-facts-", "facts"),
    Route("filings.kr.dart", "dart.fnltt_filings@1", "opendart-receipts-", "receipts"),
    Route("fundamentals.kr.dart", "dart.fnltt@1", "opendart-receipts-", "receipts"),
    Route("macro.us.alfred", "fred.alfred@1", "fred-alfred-observations-", "observations"),
    Route("fx.usdkrw.fred", "fred.fx_series@1", "fred-series-csv-", "observations"),
    Route("classifications.kr.kind", "kind.industry@2", "kind-listings-", "listings",
          continues=("kind.industry@1",)),
    Route("prices.us.eodhd", "eodhd.bars@1", "qveris-bulk-bars-", "bars", _US_SYMBOL),
    Route("prices.kr.eodhd", "eodhd.bars@1", "qveris-bulk-bars-", "bars", _KR_SYMBOL),
    Route("prices.kr.eodhd", "eodhd.bulk_quarantine@1", "qveris-bulk-quarantine-",
          "quarantine", _KR_PARTIAL, derive_from="eodhd.bars@1", derive_args=_KR_BULK_ARGS,
          check=_KR_VALUES),
    Route("prices.kr.eodhd.ref", "eodhd.bars_adjusted@1", "qveris-bulk-bars-", "bars",
          _KR_SYMBOL),
    Route("prices.kr.eodhd.ref", "eodhd.bulk_quarantine_adjusted@1", "qveris-bulk-quarantine-",
          "quarantine", _KR_PARTIAL, derive_from="eodhd.bars_adjusted@1",
          derive_args=_KR_BULK_ARGS, check=_KR_VALUES),
    Route("actions.us.eodhd", "eodhd.splits@1", "qveris-splits-", "splits", _US_SYMBOL,
          start=_actions("sessions.xnys", "America/New_York", {"ratio": "float_shortest@1"})),
    Route("actions.us.eodhd", "eodhd.dividends@1", "qveris-dividends-", "dividends", _US_SYMBOL,
          start=_actions("sessions.xnys", "America/New_York", {"amount": "decimal_text@1"})),
    Route("actions.kr.eodhd", "eodhd.splits@1", "qveris-splits-", "splits", _KR_SYMBOL,
          start=_actions("sessions.xkrx", "Asia/Seoul", {"ratio": "float_shortest@1"})),
    Route("actions.kr.eodhd", "eodhd.dividends@1", "qveris-dividends-", "dividends", _KR_SYMBOL,
          start=_actions("sessions.xkrx", "Asia/Seoul", {"amount": "decimal_text@1"})),
    Route("fx.usdkrw.eodhd", "eodhd.fx_history@1", "qveris-fx-history-bars-", "bars",
          "base_currency = 'USD' AND quote_currency = 'KRW'", start=_FX_START),
    Route("status.kr.kind", "kind.status@1", "kind-listings-", "listings", start=_STATUS_START),
)  # fmt: skip

# Collected shapes whose dataset has no registered mapper yet: counted, never promoted.
UNMAPPED: Final = (
    ("prices.us.eodhd", "qveris-bulk-quarantine-", "quarantine", f"{_EXCHANGE} = 'US'"),
)  # fmt: skip


@dataclass(frozen=True, slots=True)
class Candidate:
    """A committed source table a route may promote."""

    pin: dict[str, str]
    target: str
    rows: int
    linked_at_us: int | None


@dataclass(slots=True)
class Step:
    partition: tuple[date, date] | None


@dataclass(slots=True)
class DatasetRun:
    dataset_id: str
    mapper: str
    status: str = "current"
    head: str | None = None
    template: str | None = None
    derived_from: str | None = None
    started: bool = False
    sources: list[dict[str, object]] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)

    def report(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "mapper": self.mapper,
            "status": self.status,
            "head": self.head,
            "template_spec_sha256": self.template,
            "derived_from": self.derived_from,
            "started": self.started,
            "sources": self.sources,
            "skipped": self.skipped,
        }


def _quote(name: str) -> str:
    return formats.quote_identifier(name)


def _datasets(workspace: Workspace, base: str) -> list[str]:
    """``base`` and its committed later time-rule generations (``base.r<N>``)."""
    pattern = re.compile(re.escape(base) + r"\.r(?:[2-9]|[1-9][0-9]+)")
    later = sorted(
        (
            str(row[0])
            for row in workspace.state.execute(
                "SELECT DISTINCT dataset_id FROM dataset_versions WHERE status='committed'"
            )
            if pattern.fullmatch(str(row[0]))
        ),
        key=lambda name: int(name.rsplit(".r", 1)[1]),
    )
    return [base, *later]


def _linked(workspace: Workspace) -> dict[str, int]:
    return {
        str(snapshot)[len(_LINK) :]: int(at)
        for snapshot, at in workspace.state.execute(
            "SELECT snapshot_id, retrieved_at_us FROM source_snapshots "
            "WHERE snapshot_id LIKE 'sl:%'"
        )
    }


def candidates(  # noqa: PLR0913 -- the route and the pass's lookups
    workspace: Workspace,
    route: Route,
    linked: Mapping[str, int],
    shas: Mapping[str, str],
    skipped: dict[str, int],
    *,
    done: set[tuple[str, str]] | None = None,
) -> list[Candidate]:
    """The route's committed, nonempty tables not yet done whose every row it accepts.

    A table without every column the route's mappers read is another shape: it is
    skipped as ``shape``. They are in ``sl:`` link order, then source ID order.
    """
    needed = {name for each in route.mappers for name in mapper(each).source_columns()}
    found: list[Candidate] = []
    for table in committed_tables(workspace, route.prefix):
        entry = table.entry
        if table.store != "market" or entry["name"] != route.table or entry["format"] != "arrow":
            continue
        if not needed <= set(table.columns):
            skipped["shape"] = skipped.get("shape", 0) + 1
            continue
        if done is not None and (table.source_id, route.table) in done:
            skipped["done"] = skipped.get("done", 0) + 1
            continue
        rows = int(cast("int", entry["rows"]))
        if not rows:
            skipped["empty"] = skipped.get("empty", 0) + 1
            continue
        target = str(entry["target"])
        reason = _left(workspace, route, target, rows)
        if reason is not None:
            if reason:
                skipped[reason] = skipped.get(reason, 0) + 1
            continue
        pin = {
            "source_id": table.source_id,
            "source_sha256": shas[table.source_id],
            "table": route.table,
            "digest": str(entry["digest"]),
        }
        found.append(Candidate(pin, target, rows, linked.get(table.source_id)))
    return sorted(
        found,
        key=lambda item: (
            item.linked_at_us is None,
            item.linked_at_us or 0,
            item.pin["source_id"],
        ),
    )


def _count(workspace: Workspace, predicate: str, target: str) -> int:
    found = workspace.market.execute(
        f"SELECT count(*) FILTER (WHERE coalesce(({predicate}), false)) "  # noqa: S608 -- fixed route predicates over a quoted table
        f"FROM {_quote(target)}"
    ).fetchone()
    return 0 if found is None else int(found[0])


def _left(workspace: Workspace, route: Route, target: str, rows: int) -> str | None:
    """Why the route leaves a nonempty table: ``""`` (another route's), the skip, or None."""
    if route.accept is None:
        return None
    matched = _count(workspace, route.accept, target)
    if matched == 0:
        return ""
    if route.check is not None and _count(workspace, f"NOT coalesce(({route.check}), false)",
                                          target):  # fmt: skip
        return "value_check"
    return "mixed" if matched != rows else None


_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


def _utc_start_us(day: date) -> int:
    return (datetime(day.year, day.month, day.day, tzinfo=UTC) - _EPOCH) // timedelta(
        microseconds=1
    )


type Member = tuple[str, str, int, int | None]


@dataclass(slots=True)
class KeyMembers:
    """The rows one snapshot resolves one identity key with, in the engine's order."""

    snapshot_id: str
    rows: list[Member]
    _long: str | None = None
    _short: list[Member] | None = None
    _starts: list[int] = field(default_factory=list)

    def sha256(self) -> str:
        """SHA-256 of every row (``key_members_sha256``)."""
        return hashlib.sha256(formats.canonical([list(row) for row in self.rows])).hexdigest()

    def _split(self) -> tuple[str, list[Member]]:
        if self._long is None or self._short is None:
            limit = SHORT_DAYS * _DAY_US
            short = [row for row in self.rows if row[3] is not None and row[3] - row[2] <= limit]
            kept = set(short)
            # Sorted here, not trusted from the caller, so the hash never follows input order.
            long = [
                list(row)
                for row in sorted(
                    (row for row in self.rows if row not in kept),
                    key=lambda row: (row[0], row[1], row[2], row[3] is None, row[3] or 0),
                )
            ]
            self._long = hashlib.sha256(formats.canonical(long)).hexdigest()
            self._short = sorted(short, key=lambda row: row[2])
            self._starts = [row[2] for row in self._short]
        return self._long, self._short

    def scoped_sha256(self, first: date, stop: date) -> str:
        """SHA-256 of the long rows and the short rows that can hold at any instant of the
        dates ``[first, stop)`` in any time zone."""
        long, short = self._split()
        low = _utc_start_us(first) - _ZONE_US
        # Any instant of the dates in any zone: a mapper may resolve at a row's retrieval
        # instant (KIND lists late in the evening), not at the start of its date.
        high = _utc_start_us(stop) + _ZONE_US
        begin = bisect_left(self._starts, low - SHORT_DAYS * _DAY_US)
        end = bisect_right(self._starts, high)
        inside = sorted(list(row) for row in short[begin:end] if cast("int", row[3]) > low)
        return hashlib.sha256(formats.canonical([SCOPE_FORMAT, long, inside])).hexdigest()

    def recorded(self, scope: Mapping[str, str] | None) -> dict[str, object]:
        """The ``identity`` a record names for a source of ``scope`` (None: unpartitioned)."""
        if scope is None:
            return {"snapshot_id": self.snapshot_id, "key_sha256": self.sha256()}
        digest = self.scoped_sha256(date.fromisoformat(scope["from"]),
                                    date.fromisoformat(scope["to"]))  # fmt: skip
        return {"snapshot_id": self.snapshot_id, "key_sha256": digest,
                "key_scope": dict(scope)}  # fmt: skip


def _done(
    workspace: Workspace, dataset_id: str, members: KeyMembers | None
) -> set[tuple[str, str]]:
    """(source ID, table) pins a maintenance record or a coverage check already settles.

    An ``unresolved`` record settles its source only while the identity members the run
    would resolve with give the SHA-256 it recorded, over its scope when it names one.
    """
    done: set[tuple[str, str]] = set()
    for rule, result, reason in workspace.state.execute(
        "SELECT rule_id, result, reason FROM quality_checks WHERE dataset_id=? "
        "AND rule_id IN ('maintain_source', 'promotion_coverage')",
        (dataset_id,),
    ):
        body = json.loads(str(reason))
        if rule == CHECK_RULE[0] and result == UNRESOLVED:
            recorded = cast("dict[str, object]", body.get("identity") or {})
            scope = cast("dict[str, str] | None", recorded.get("key_scope"))
            if (
                members is None
                or recorded.get("key_sha256") != members.recorded(scope)["key_sha256"]
            ):
                continue
        pins = [body["source"]] if rule == CHECK_RULE[0] else body.get("sources", [])
        done.update((str(pin["source_id"]), str(pin["table"])) for pin in pins)
    return done


def key_members_sha256(workspace: Workspace, pin: Mapping[str, str], provider: str,
                       namespace: str) -> str:  # fmt: skip
    """SHA-256 of the rows a snapshot resolves one identity key with (the engine's view)."""
    return key_members(workspace, pin, provider, namespace).sha256()


def key_members(workspace: Workspace, pin: Mapping[str, str], provider: str,
                namespace: str) -> KeyMembers:  # fmt: skip
    """The rows a snapshot resolves one identity key with (the engine's view)."""
    snapshot = IdentityPin(pin["snapshot_id"], pin["content_hash"])
    parts = membership_parts(workspace.state, snapshot)
    names = [cast("IdentityPin", part).snapshot_id for part in parts] or [snapshot.snapshot_id]
    rows = workspace.state.execute(
        "SELECT a.token, a.instrument_id, m.valid_from_us, m.valid_to_us "
        "FROM identity_snapshot_members m JOIN identity_assertions a USING (assertion_id) "
        "WHERE m.snapshot_id IN (SELECT value FROM json_each(?)) AND m.known_to_us IS NULL "
        "AND a.provider=? AND a.namespace=? ORDER BY 1, 2, 3, 4",
        (json.dumps(names), provider, namespace),
    ).fetchall()
    return KeyMembers(
        snapshot.snapshot_id,
        [(str(token), str(instrument), int(start), None if end is None else int(end))
         for token, instrument, start, end in rows],
    )  # fmt: skip


def _resolution(
    workspace: Workspace,
    template: Mapping[str, object],
    mapper_name: str,
    identity: Mapping[str, str] | None,
) -> KeyMembers | None:
    """The identity-key members of the snapshot a maintenance spec of ``template`` pins."""
    pinned = cast("dict[str, str] | None", template.get("identity_snapshot"))
    if pinned is None:
        return None
    snapshot = dict(identity) if identity is not None else dict(pinned)
    args = cast("dict[str, object]", cast("dict[str, object]", template["mapper"]).get("args", {}))
    key = mapper(mapper_name).identity(args)
    if key is None:
        return None
    return key_members(workspace, snapshot, key.provider, key.namespace)


def _template(
    workspace: Workspace, dataset_id: str, mappers: Sequence[str]
) -> tuple[str, str, dict] | None:
    """The latest committed spec of ``dataset_id`` under one of ``mappers``: hash, mapper, spec."""
    row = workspace.state.execute(
        "SELECT transform_hash, normalizer_version FROM dataset_versions WHERE dataset_id=? "
        "AND normalizer_version IN (SELECT value FROM json_each(?)) AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (dataset_id, json.dumps(list(mappers))),
    ).fetchone()
    if row is None:
        return None
    digest, name = str(row[0]), str(row[1])
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(f"{digest[:2]}/{digest}", max_bytes=64 * 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"the retained spec of {dataset_id} differs from its transform hash")
    return digest, name, cast("dict", json.loads(raw))


def _derived(workspace: Workspace, dataset_id: str, route: Route) -> tuple[str, str, dict] | None:
    """The route's first template: the latest ``derive_from`` spec under the route's mapper.

    Only the mapper's name changes and ``derive_args`` join its arguments; the hash is the
    spec it was derived from.
    """
    if route.derive_from is None:
        return None
    found = _template(workspace, dataset_id, (route.derive_from,))
    if found is None:
        return None
    digest, _, spec = found
    named = cast("dict[str, object]", spec["mapper"])
    args = {
        **cast("dict[str, object]", named.get("args", {})),
        **json.loads(json.dumps(dict(route.derive_args or {}))),
    }
    return digest, route.mapper, {**spec, "mapper": {**named, "name": route.mapper, "args": args}}


def _chain_template(
    workspace: Workspace, run: DatasetRun, route: Route
) -> tuple[str, str, dict] | None:
    """The template a chain with a head continues from, derived when it has none."""
    if run.head is None:
        return None
    found = _template(workspace, run.dataset_id, route.mappers)
    if found is None:
        # The chain's first generation of this mapper starts from a sibling's spec.
        found = _derived(workspace, run.dataset_id, route)
        run.derived_from = None if found is None else route.derive_from
    return found


def _started(
    workspace: Workspace, dataset_id: str, route: Route, identity: Mapping[str, str] | None
) -> tuple[str, str, dict] | str:
    """The route's declared first template, or why it cannot start (the run's skip reason).

    The hash is that of the declared template; it is retained only as part of the specs
    built from it.
    """
    declared = cast("Start", route.start)
    args = dict(cast("Mapping[str, object]", declared.time_rule["args"]))
    if "calendar" in args:
        calendar = head_pin(workspace, str(args["calendar"]))
        if calendar is None:
            return "no_calendar"
        args["calendar"] = calendar
    if declared.identity and identity is None:
        return "identity_unavailable"
    rule = {**declared.time_rule, "args": args}
    # The partition only says the template is partitioned; each step sets its own.
    partition = {"from": "1970-01-01", "to": "1970-01-02"} if declared.partitioned else None
    template = {
        "schema_version": SPEC_SCHEMA,
        "target": {"domain": mapper(route.mapper).domain, "dataset_id": dataset_id,
                   "parent": None},
        "sources": [],
        "mapper": {"name": route.mapper, "args": dict(declared.args)},
        "partition": partition,
        "time_rules": dict.fromkeys(TIME_COLUMNS, rule),
        "decimal_rule": dict(declared.decimal_rule),
        "quality_rules": [],
        "tombstone_policy": {"mode": "never"},
        "identity_snapshot": dict(identity) if declared.identity and identity else None,
    }  # fmt: skip
    raw = formats.canonical(template)
    return hashlib.sha256(raw).hexdigest(), route.mapper, json.loads(raw)


def _run_template(
    workspace: Workspace,
    run: DatasetRun,
    route: Route,
    identity: Mapping[str, str] | None,
    start: Collection[str],
) -> tuple[tuple[str, str, dict] | None, str | None]:
    """The run's template (continued, derived or started) and why a named start is blocked."""
    found = _chain_template(workspace, run, route)
    if found is not None or route.start is None or run.dataset_id not in start:
        return found, None
    declared = _started(workspace, run.dataset_id, route, identity)
    if isinstance(declared, str):
        return None, declared
    run.started = True
    return declared, None


def head_pin(workspace: Workspace, dataset_id: str) -> dict[str, str] | None:
    row = workspace.state.execute(
        "SELECT version, generation_id, chain_hash, manifest_hash FROM dataset_versions "
        "WHERE dataset_id=? AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (dataset_id,),
    ).fetchone()
    if row is None:
        return None
    names = ("version", "generation_id", "chain_hash", "manifest_hash")
    return {"dataset_id": dataset_id, **dict(zip(names, map(str, row), strict=True))}


def _refreshed(workspace: Workspace, value: object) -> object:
    """``value`` with every generation pin advanced to its dataset's committed head."""
    if isinstance(value, dict):
        if set(value) == _PIN_KEYS:
            return head_pin(workspace, str(value["dataset_id"])) or value
        return {key: _refreshed(workspace, item) for key, item in value.items()}
    if isinstance(value, list):
        return [_refreshed(workspace, item) for item in value]
    return value


def _steps(
    workspace: Workspace, template: Mapping[str, object], mapper_name: str, item: Candidate
) -> list[Step]:
    if template.get("partition") is None:
        return [Step(None)]
    if mapper_name == "fred.alfred@1":
        return [
            Step(bounds) for bounds in vintage_partitions(workspace.market, _quote(item.target))
        ]
    day = mapper(mapper_name).partition_sql
    bounds = workspace.market.execute(
        f"SELECT min(TRY_CAST(({day}) AS DATE)), max(TRY_CAST(({day}) AS DATE)) "  # noqa: S608 -- registered mapper SQL over a quoted table
        f"FROM {_quote(item.target)}"
    ).fetchone()
    if bounds is None or bounds[0] is None:
        return [Step(None)]
    return [Step((bounds[0], bounds[1] + timedelta(days=1)))]


def maintenance_spec(  # noqa: PLR0913 -- the template and every field maintenance advances
    workspace: Workspace,
    template: Mapping[str, object],
    *,
    parent: str | None,
    pin: Mapping[str, str],
    step: Step,
    identity: Mapping[str, str] | None,
) -> bytes:
    """The template's canonical spec with only the maintenance fields advanced."""
    spec = cast("dict[str, object]", json.loads(json.dumps(template)))
    target = cast("dict[str, object]", spec["target"])
    target["parent"] = parent
    spec["sources"] = [dict(pin)]
    spec["partition"] = (
        None
        if step.partition is None
        else {"from": step.partition[0].isoformat(), "to": step.partition[1].isoformat()}
    )
    for key in ("mapper", "time_rules", "quality_rules"):
        spec[key] = _refreshed(workspace, spec[key])
    if identity is not None and spec.get("identity_snapshot") is not None:
        spec["identity_snapshot"] = dict(identity)
    spec["tombstone_policy"] = {"mode": "never"}
    return formats.canonical(spec)


def _record(  # noqa: PLR0913 -- the source, its steps and the identity it resolved with
    workspace: Workspace,
    dataset_id: str,
    pin: Mapping[str, str],
    steps: Sequence[Mapping[str, object]],
    *,
    resolution: Mapping[str, object] | None,
    now_us: int,
) -> str | None:
    version = workspace.state.execute(
        "SELECT version FROM dataset_versions WHERE dataset_id=? AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (dataset_id,),
    ).fetchone()
    if version is None:
        return None
    published = any(step.get("published") for step in steps)
    unresolved = sum(int(cast("int", step.get("unresolved_token_count") or 0)) for step in steps)
    reason: dict[str, object] = {
        "source": dict(pin),
        "steps": [
            {
                key: step.get(key)
                for key in ("partition", "spec_sha256", "generation_id", "published",
                            "unresolved_token_count")
            }
            for step in steps
        ],
    }  # fmt: skip
    key = f"maintain/{dataset_id}/{pin['source_id']}/{pin['table']}"
    result = "promoted" if published else "unchanged"
    if unresolved:
        # Planned again under other identity-key members, so each set records once.
        reason["identity"] = None if resolution is None else dict(resolution)
        key += "/unresolved/" + ("" if resolution is None else str(resolution["key_sha256"]))
        result = UNRESOLVED
    check_id = "qc-" + hashlib.sha256(key.encode()).hexdigest()
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO quality_checks(check_id, dataset_id, version, rule_id, rule_version, "
            "result, reason, checked_at_us) VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(check_id) DO NOTHING",
            (
                check_id,
                dataset_id,
                str(version[0]),
                *CHECK_RULE,
                result,
                formats.canonical(reason).decode(),
                now_us,
            ),
        )
    return check_id


def _promote_source(  # noqa: PLR0913 -- one source's steps and the run's inputs
    workspace: Workspace,
    run: DatasetRun,
    template: Mapping[str, object],
    item: Candidate,
    *,
    apply: bool,
    identity: Mapping[str, str] | None,
    resolution: KeyMembers | None,
    budget: ComputeBudget | None,
    now_us: int,
    evaluate: bool = True,
) -> dict[str, object]:
    results: list[dict[str, object]] = []
    outcome: dict[str, object] = {"source_id": item.pin["source_id"], "rows": item.rows}
    for step in _steps(workspace, template, run.mapper, item):
        parent = dataset_head(workspace, run.dataset_id)
        raw = maintenance_spec(
            workspace, template, parent=parent, pin=item.pin, step=step, identity=identity
        )
        sha256 = hashlib.sha256(raw).hexdigest()
        partition = (
            None
            if step.partition is None
            else {"from": step.partition[0].isoformat(), "to": step.partition[1].isoformat()}
        )
        summary: dict[str, object] = {"partition": partition, "spec_sha256": sha256}
        if not evaluate:
            results.append(summary)
            continue
        try:
            result = promote(workspace, raw, sha256, apply=apply, budget=budget)
        except (ValueError, duckdb.Error) as error:
            # A database error (out of memory under the compute budget) refuses this
            # source; the other datasets of the pass still run.
            if apply:
                put_raw(workspace.paths.raw, raw)
            results.append({**summary, "error": str(error)})
            return {**outcome, "status": "refused", "steps": results}
        summary.update({key: result[key] for key in _SUMMARY if key in result})
        results.append(summary)
        if result.get("blocking") or result.get("refusals"):
            return {**outcome, "status": "refused", "steps": results}
    published = any(step.get("published") for step in results)
    status = "promoted" if published else "unchanged" if apply else "planned"
    partitions = [cast("dict[str, str] | None", step.get("partition")) for step in results]
    scope = (
        None
        if not partitions or any(part is None for part in partitions)
        else {"from": min(cast("dict[str, str]", part)["from"] for part in partitions),
              "to": max(cast("dict[str, str]", part)["to"] for part in partitions)}
    )  # fmt: skip
    recorded = None if resolution is None else resolution.recorded(scope)
    if any(step.get("unresolved_token_count") for step in results):
        status = UNRESOLVED if apply else status
        outcome["identity"] = recorded
    if apply:
        outcome["check_id"] = _record(workspace, run.dataset_id, item.pin, results,
                                      resolution=recorded, now_us=now_us)  # fmt: skip
    return {**outcome, "status": status, "steps": results}


def promote_datasets(  # noqa: PLR0913 -- the pass's explicit inputs
    workspace: Workspace,
    *,
    apply: bool,
    identity: Mapping[str, str] | None = None,
    identity_failed: bool = False,
    budget: ComputeBudget | None = None,
    now_us: int,
    routes: Sequence[Route] = ROUTES,
    evaluate: bool = True,
    start: Collection[str] = (),
) -> dict[str, object]:
    """Plan, or apply in order, every route's new sources as the next generations.

    A plan with ``evaluate`` false lists each new source's steps and specs without
    planning their promotions. With ``identity_failed`` (the run's identity stage did not
    complete), a route whose template pins an identity snapshot promotes nothing. A
    dataset named in ``start`` whose chain holds no generation of a route's mappers starts
    from the route's declared ``start`` template.
    """
    if apply and not evaluate:
        raise ValueError("an applied maintenance promotion evaluates every step")
    linked = _linked(workspace)
    shas = {str(row["source_id"]): str(row["sha256"]) for row in list_sources(workspace)}
    runs: list[DatasetRun] = []
    for route in routes:
        for dataset_id in _datasets(workspace, route.dataset_id):
            run = DatasetRun(dataset_id, route.mapper)
            runs.append(run)
            run.skipped["done"] = 0
            run.head = dataset_head(workspace, dataset_id)
            found, blocked = _run_template(workspace, run, route, identity, start)
            # The chain continues with the mapper its latest generation used.
            run.mapper = route.mapper if found is None else found[1]
            resolution = (
                None if found is None else _resolution(workspace, found[2], run.mapper, identity)
            )
            done = _done(workspace, dataset_id, resolution)
            pending = candidates(workspace, route, linked, shas, run.skipped, done=done)
            if found is None:
                reason = blocked or ("no_head" if run.head is None else "no_template")
                run.status = reason if pending else "current"
                run.skipped[reason] = len(pending)
                continue
            digest, _, template = found
            run.template = None if run.started else digest
            if identity_failed and template.get("identity_snapshot") is not None:
                # Without this run's snapshot, new listings would resolve against an older one.
                run.status = "identity_unavailable" if pending else "current"
                run.skipped["identity_unavailable"] = len(pending)
                continue
            for item in pending:
                result = _promote_source(
                    workspace,
                    run,
                    template,
                    item,
                    apply=apply,
                    identity=identity,
                    resolution=resolution,
                    budget=budget,
                    now_us=now_us,
                    evaluate=evaluate,
                )
                run.sources.append(result)
                if result["status"] == "refused":
                    run.status = "refused"
            if run.status != "refused" and run.sources:
                run.status = "advanced" if apply else "planned"
            run.head = dataset_head(workspace, dataset_id)
    refused = [run.dataset_id for run in runs if run.status == "refused"]
    unresolved = sorted({run.dataset_id for run in runs for item in run.sources
                         if item["status"] == UNRESOLVED})  # fmt: skip
    return {
        "mode": "apply" if apply else "plan",
        "datasets": [run.report() for run in runs],
        "refused": refused,
        "unresolved": unresolved,
        "standing_unresolved": standing_unresolved(workspace),
        "unmapped": unmapped(workspace),
        "provider_calls": 0,
    }


def standing_unresolved(workspace: Workspace) -> dict[str, dict[str, object]]:
    """Per dataset, the sources whose latest ``maintain_source@1`` record is ``unresolved``.

    Read from the persisted records alone, so a dataset that brought no new source this
    pass is listed as long as its sources wait. ``sources`` counts them, ``rows`` sums
    their source tables' rows (a source whose table can no longer be listed, a retired one,
    is counted in ``rows_unavailable`` instead), ``unresolved_tokens`` sums the identity
    tokens their latest records left unresolved, ``unpublished_sources`` counts those
    whose latest record published nothing, and ``oldest_date`` is the earliest partition
    date they cover (the UTC date of the first such record for an unpartitioned source).
    A dataset without one is not listed.
    """
    latest: dict[tuple[str, str, str], tuple[int, bool, dict[str, object]]] = {}
    first: dict[tuple[str, str, str], int] = {}
    for dataset_id, result, reason, checked in workspace.state.execute(
        "SELECT dataset_id, result, reason, checked_at_us FROM quality_checks "
        "WHERE rule_id=? ORDER BY checked_at_us, check_id",
        (CHECK_RULE[0],),
    ):
        body = cast("dict[str, object]", json.loads(str(reason)))
        pin = cast("dict[str, str]", body["source"])
        key = (str(dataset_id), pin["source_id"], pin["table"])
        waiting = result == UNRESOLVED
        prior = latest.get(key)
        # A settled record of the same instant outranks an unresolved one.
        if prior is None or int(checked) > prior[0] or (int(checked) == prior[0] and not waiting):
            latest[key] = (int(checked), waiting, body)
        if waiting:
            first.setdefault(key, int(checked))
    summary: dict[str, dict[str, object]] = {}
    for (dataset_id, source_id, table), (_, waiting, body) in sorted(latest.items()):
        if not waiting:
            continue
        entry = summary.setdefault(dataset_id, {
            "sources": 0, "rows": 0, "rows_unavailable": 0, "unresolved_tokens": 0,
            "unpublished_sources": 0, "oldest_date": None})  # fmt: skip
        steps = cast("list[dict[str, object]]", body.get("steps") or [])
        entry["sources"] = cast("int", entry["sources"]) + 1
        try:
            rows = next(int(cast("int", item["rows"])) for item in list_tables(workspace, source_id)
                        if item["name"] == table)  # fmt: skip
        except (ValueError, StopIteration):
            entry["rows_unavailable"] = cast("int", entry["rows_unavailable"]) + 1
        else:
            entry["rows"] = cast("int", entry["rows"]) + rows
        entry["unresolved_tokens"] = cast("int", entry["unresolved_tokens"]) + sum(
            int(cast("int", step.get("unresolved_token_count") or 0)) for step in steps
        )
        if not any(step.get("published") for step in steps):
            entry["unpublished_sources"] = cast("int", entry["unpublished_sources"]) + 1
        days = [
            cast("dict[str, str]", step["partition"])["from"]
            for step in steps
            if step.get("partition") is not None
        ]
        recorded_at = _EPOCH + timedelta(microseconds=first[dataset_id, source_id, table])
        oldest = min(days) if days else recorded_at.date().isoformat()
        if entry["oldest_date"] is None or oldest < cast("str", entry["oldest_date"]):
            entry["oldest_date"] = oldest
    return summary


def unmapped(workspace: Workspace) -> dict[str, int]:
    """Nonempty collected tables of each dataset that no registered mapper promotes yet."""
    counts: dict[str, int] = {}
    for dataset_id, prefix, name, accept in UNMAPPED:
        for table in committed_tables(workspace, prefix):
            entry = table.entry
            if table.store != "market" or entry["name"] != name or not entry["rows"]:
                continue
            if accept is not None:
                target = _quote(str(entry["target"]))
                row = workspace.market.execute(
                    f"SELECT bool_and(coalesce(({accept}), false)) FROM {target}"  # noqa: S608 -- fixed predicates over a quoted table
                ).fetchone()
                if row is None or row[0] is not True:
                    continue
            counts[dataset_id] = counts.get(dataset_id, 0) + 1
    return dict(sorted(counts.items()))


# --- data freshness ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Expectation:
    """How far a dataset's data should reach at a run, and when it is stale.

    ``session``: the venue's last session closed by the run; ``previous_day``: the day
    before the run's date at the venue (OpenDART answers a day's filings the next day).
    Both count the lag in the venue's sessions. ``series``: each series' own period,
    inferred from its latest observation periods, is expected once one further period has
    passed after it ends; the lag counts periods and the dataset's is its worst series.
    ``none``: no date the data must reach (declared calendars, event domains).
    ``stale_after`` is the lag above which the dataset is stale; ``collector`` is the
    maintenance stage that collects its sources.
    """

    kind: Literal["session", "previous_day", "series", "none"]
    calendar: str | None = None
    stale_after: int | None = None
    collector: str | None = None


# The one place freshness thresholds live. A catalog dataset that is not frozen has an entry.
FRESHNESS: Final[Mapping[str, Expectation]] = {
    "prices.kr.eodhd": Expectation("session", "XKRX", 2, "qveris"),
    "prices.kr.eodhd.ref": Expectation("session", "XKRX", 2, "qveris"),
    "prices.us.eodhd": Expectation("session", "XNYS", 2, "qveris"),
    "actions.us.eodhd": Expectation("session", "XNYS", 5, "qveris"),
    "actions.kr.eodhd": Expectation("session", "XKRX", 20, "qveris"),
    "filings.us.sec": Expectation("session", "XNYS", 2, "sec"),
    "fundamentals.us.sec": Expectation("session", "XNYS", 5, "sec"),
    "filings.kr.dart": Expectation("previous_day", "XKRX", 2, "dart"),
    "fundamentals.kr.dart": Expectation("previous_day", "XKRX", 5, "dart"),
    "macro.us.alfred": Expectation("series", stale_after=2, collector="fred"),
    "fx.usdkrw.fred": Expectation("series", stale_after=5, collector="fred"),
    "macro.kr.bok": Expectation("series", stale_after=2),
    "macro.kr.oecd": Expectation("series", stale_after=2),
    "sessions.xnys": Expectation("none"),
    "sessions.xkrx": Expectation("none"),
    "status.kr.kind": Expectation("none", collector="kind"),
    "classifications.us.sec": Expectation("none"),
    "classifications.kr.kind": Expectation("none", collector="kind"),
}
# Consecutive runs whose collector succeeded while the head did not advance and the data
# was behind (or had no expected date) before a dataset is stale.
STALE_AFTER_ZERO_PROMOTION_RUNS: Final = 5
# Providers whose subscription ended: every dataset of theirs is frozen. The catalog's own
# ``frozen`` flag marks only the datasets it promotes from retained exports.
FROZEN_PROVIDERS: Final = frozenset({"norgate", "fmp"})
_SERIES_PERIODS: Final = 12
_UTC_DAY: Final = "(DATE '1970-01-01' + CAST(floor({} / 86400000000.0) AS INTEGER))"
# The column (or UTC date of an instant) that says how far each domain's data reaches.
DATA_DATES: Final[Mapping[str, str]] = {
    "prices": '"session_date"',
    "corporate_actions": '"ex_date"',
    "instrument_status": _UTC_DAY.format('"effective_from_us"'),
    "fundamentals": _UTC_DAY.format('"available_at_us"'),
    "macro_observations": '"observation_period"',
    "fx_rates": _UTC_DAY.format('"fixing_at_us"'),
    "filings": '"filed_date"',
    "classifications": '"effective_from"',
    "calendar_sessions": '"session_date"',
    "estimates": '"target_period"',
    "feature_values": _UTC_DAY.format('"feature_at_us"'),
}
_SERIES_KEYS: Final = {"macro_observations": '"series_id"',
                       "fx_rates": '("base_currency" || \'/\' || "quote_currency")'}  # fmt: skip


def _base(dataset_id: str) -> str:
    entry = catalog_entry(dataset_id)
    return dataset_id if entry is None else entry.dataset_id


def frozen(dataset_id: str) -> bool:
    """Whether the dataset's provider stopped: the catalog's flag or a frozen provider."""
    entry = catalog_entry(dataset_id)
    return (entry is not None and entry.frozen) or bool(
        FROZEN_PROVIDERS & set(_base(dataset_id).split("."))
    )


def collector(dataset_id: str) -> str | None:
    """The maintenance stage that collects the dataset's sources, if any."""
    found = FRESHNESS.get(_base(dataset_id))
    return None if found is None else found.collector


@cache
def _sessions(calendar: str) -> tuple[ZoneInfo, list[date], list[datetime]]:
    """The packaged declaration's open sessions with their close instants, in order."""
    raw = packaged_declaration(calendar)
    declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
    zone = ZoneInfo(declaration.timezone)
    days, closes = [], []
    for item in declaration.days():
        if item.close_local is not None:
            days.append(item.session_date)
            closes.append(item.close_local.replace(tzinfo=zone))
    return zone, days, closes


def _expected_day(expectation: Expectation, now: datetime) -> date | None:
    """The date the data should reach at ``now``; None past the packaged declaration."""
    zone, days, closes = _sessions(cast("str", expectation.calendar))
    if expectation.kind == "previous_day":
        day = now.astimezone(zone).date() - timedelta(days=1)
        return day if days and days[0] <= day < days[-1] else None
    closed = bisect_right(closes, now)
    if closed == 0 or closed == len(closes):
        return None
    return days[closed - 1]


def _session_lag(calendar: str, data: date, expected: date) -> int:
    _, days, _ = _sessions(calendar)
    return max(0, bisect_right(days, expected) - bisect_right(days, data))


# Month counts by the day gaps they span: monthly, quarterly, half-yearly, yearly.
_MONTHLY: Final = {1: (28, 31), 3: (89, 92), 6: (181, 184), 12: (365, 366)}


def _shift(day: date, cadence: int, steps: int) -> date:
    """``day`` moved by ``steps`` periods: weekdays, or calendar months for monthly cadences."""
    if cadence == 1:
        moved = day
        for _ in range(abs(steps)):
            moved += timedelta(days=1 if steps > 0 else -1)
            while moved.weekday() >= 5:  # noqa: PLR2004 -- Saturday and Sunday
                moved += timedelta(days=1 if steps > 0 else -1)
        return moved
    months = next((m for m, (low, high) in _MONTHLY.items() if low <= cadence <= high), None)
    if months is None:
        return day + timedelta(days=cadence * steps)
    index = day.year * 12 + day.month - 1 + months * steps
    year, month = divmod(index, 12)
    return date(year, month + 1, min(day.day, monthrange(year, month + 1)[1]))


def _series_lag(periods: Sequence[date], today: date) -> tuple[date, int] | None:
    """The latest expected period and the periods the series is behind it.

    A period is expected once one further period has passed after it ends, so a daily
    series' Monday is due on Wednesday and a month's on the first of the month after next.
    A series with one period has no cadence and no lag.
    """
    ordered = sorted(set(periods))
    gaps = [(later - earlier).days for earlier, later in pairwise(ordered)]
    if not gaps:
        return None
    cadence = 1 if min(gaps) <= 3 else min(gaps)  # noqa: PLR2004 -- a weekend between weekdays
    expected, lag = ordered[-1], 0
    # Data ahead of the expectation: step back to the latest period already due.
    while _shift(expected, cadence, 2) > today:
        expected = _shift(expected, cadence, -1)
    # Data behind it: every further period already due is one period of lag.
    while _shift(_shift(expected, cadence, 1), cadence, 2) <= today:
        expected = _shift(expected, cadence, 1)
        lag += 1
    return expected, lag


def _chain(workspace: Workspace, generation_id: str) -> tuple[str, list[str]]:
    chain = generation_chain(workspace.market, generation_id)
    return str(chain[-1]["domain"]), [str(marker["generation_id"]) for marker in chain]


def _data_through(workspace: Workspace, domain: str, generations: list[str]) -> date | None:
    column = DATA_DATES.get(domain)
    if column is None:
        return None
    row = workspace.market.execute(
        f"SELECT max({column}) FROM {_quote(domain)} WHERE op<>'TOMBSTONE' "  # noqa: S608 -- code-owned columns and tables
        "AND generation_id IN (SELECT unnest($g::VARCHAR[]))",
        {"g": generations},
    ).fetchone()
    return None if row is None else cast("date | None", row[0])


def _series_periods(
    workspace: Workspace, domain: str, generations: list[str]
) -> dict[str, list[date]]:
    key, column = _SERIES_KEYS[domain], DATA_DATES[domain]
    found: dict[str, list[date]] = {}
    for series, period in workspace.market.execute(
        f"SELECT s, d FROM (SELECT DISTINCT {key} AS s, {column} AS d FROM {_quote(domain)} "  # noqa: S608 -- code-owned columns and tables
        "WHERE op<>'TOMBSTONE' AND generation_id IN (SELECT unnest($g::VARCHAR[]))) "
        f"QUALIFY row_number() OVER (PARTITION BY s ORDER BY d DESC) <= {_SERIES_PERIODS}",
        {"g": generations},
    ).fetchall():
        found.setdefault(str(series), []).append(cast("date", period))
    return found


def freshness(
    workspace: Workspace, dataset_id: str, generation_id: str | None, now: datetime
) -> dict[str, object]:
    """How far the dataset's data reaches, how far it should, and its freshness state.

    Read-only: the market rows of the head's chain and the packaged session calendars.
    """
    result: dict[str, object] = {
        "data_through": None, "expected_through": None, "lag_sessions": None,
        "lag_unit": None, "stale_after": None, "freshness": "no_head",
        "stale_reasons": [], "stale_series": [], "zero_promotion_runs": None,
    }  # fmt: skip
    if generation_id is None:
        return result
    domain, generations = _chain(workspace, generation_id)
    data = _data_through(workspace, domain, generations)
    result["data_through"] = None if data is None else data.isoformat()
    expectation = FRESHNESS.get(_base(dataset_id), Expectation("none"))
    if frozen(dataset_id):
        result["freshness"] = "frozen"
        return result
    if data is None:
        result["freshness"] = "no_data"
        return result
    result["freshness"] = "untracked"
    expected: date | None = None
    lag: int | None = None
    if expectation.kind in {"session", "previous_day"}:
        expected = _expected_day(expectation, now)
        if expected is not None:
            lag = _session_lag(cast("str", expectation.calendar), data, expected)
            result["lag_unit"] = "sessions"
    elif expectation.kind == "series" and domain in _SERIES_KEYS:
        today = now.astimezone(UTC).date()
        lags = {series: found for series, periods in
                _series_periods(workspace, domain, generations).items()
                if (found := _series_lag(periods, today)) is not None}  # fmt: skip
        if lags:
            expected = max(found[0] for found in lags.values())
            lag = max(found[1] for found in lags.values())
            result["lag_unit"] = "periods"
            result["stale_series"] = sorted(
                series for series, found in lags.items()
                if found[1] > cast("int", expectation.stale_after)
            )  # fmt: skip
    if lag is None:
        return result
    result.update(expected_through=cast("date", expected).isoformat(), lag_sessions=lag,
                  stale_after=expectation.stale_after)  # fmt: skip
    stale = lag > cast("int", expectation.stale_after)
    result["freshness"] = "stale" if stale else "current"
    result["stale_reasons"] = ["lag"] if stale else []
    return result


def dataset_heads(workspace: Workspace, *, now: datetime) -> list[dict[str, object]]:
    """Every committed dataset's head, watermarks and data freshness at ``now``.

    A watermark is the latest ingestion time, not how far the data reaches; ``freshness``
    reports that. A catalog dataset without a committed version is listed as ``no_head``.
    """
    heads: list[dict[str, object]] = []
    for dataset_id, version, sequence, rows, generation_id in workspace.state.execute(
        "SELECT dataset_id, version, sequence, row_count, generation_id FROM dataset_versions v "
        "WHERE status='committed' AND sequence=(SELECT max(sequence) FROM dataset_versions w "
        "WHERE w.dataset_id=v.dataset_id AND w.status='committed') ORDER BY dataset_id"
    ).fetchall():
        watermarks = {
            f"{provider}/{partition}": {"through_us": int(through), "version": str(committed)}
            for provider, partition, through, committed in workspace.state.execute(
                "SELECT provider, partition_id, through_us, committed_version FROM watermarks "
                "WHERE dataset_id=? ORDER BY provider, partition_id",
                (dataset_id,),
            )
        }
        heads.append(
            {
                "dataset_id": str(dataset_id),
                "version": str(version),
                "sequence": int(sequence),
                "rows": int(rows),
                "watermarks": watermarks,
                **freshness(workspace, str(dataset_id), str(generation_id), now),
            }
        )
    listed = {str(item["dataset_id"]) for item in heads}
    heads.extend(
        {"dataset_id": entry.dataset_id, "version": None, "sequence": None, "rows": 0,
         "watermarks": {}, **freshness(workspace, entry.dataset_id, None, now)}
        for entry in CATALOG if entry.dataset_id not in listed
    )  # fmt: skip
    heads.sort(key=lambda item: str(item["dataset_id"]))
    return heads
