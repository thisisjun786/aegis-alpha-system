"""``eodhd.us_symbol_list@1``: dated EODHD US symbol lists continue US ticker claims by day.

Norgate is a frozen source, so the US registry's ticker claims (``us_identity``) end at the
New York start of the day after the last session its master or history export shows.
This mapper continues them from collection point to collection point (#99: tickers can
change between collection points, so a link that cannot be verified at both ends stays
unresolved):

- An observation is one row of a committed ``qveris-eodhd-exchange-symbols-*`` list asked
  for ``EXCHANGE_CODE=US`` without delisted symbols, retrieved at the instant its page
  receipt records. It covers the New York trading day it follows: the New York date of
  its retrieval, or the day before when it was retrieved before 09:30 New York time.
- A row names an instrument only through its ISIN: the ISIN must carry a valid check
  digit and be the registered, not superseded FMP ``isin`` of exactly one instrument with
  a Norgate asset ID, and the row's currency must be USD. A ticker, a name, an exchange or
  a price never names an instrument, and a list never mints one.
- A weekday ``d`` whose observations all name instrument ``I`` for ``<Code>.US`` (and no
  other symbol of them names ``I``) is claimed when the latest earlier claim of the symbol
  (``before``: a registered claim of the master, an export or an earlier list, or a claim
  this build accepted for an earlier day) shows ``I`` holding it, and ``I`` holds no other
  US symbol from the day after ``before`` through ``d``. Today's list is never the earlier
  end, and neither is any observation the build rejected (or a Saturday or Sunday one):
  lists alone never carry a ticker to an instrument no registered claim shows holding it,
  so a reused, renamed or new ticker waits for independent registered evidence.
- With only Saturdays and Sundays between the two, the claim is the one New York day ``d``.
  Otherwise one claim bridges the gap: from the New York start of the day after ``before``
  to that of the day after ``d``, at most ``MAX_BRIDGE_DAYS`` calendar days. Bridged days
  are verified at their two ends, not observed one by one; the report lists them
  (``bridges``, ``bridged_symbols``, ``bridged_days``, ``bridge_unresolved``).
- A claim is known from the retrieval instant of the earliest observation covering ``d``
  (the end: no one could know it earlier) and cites that row; the earlier end is named in
  the report. A day a registered claim of the symbol already covers is left to it, so
  claims never overlap.

Every day that cannot be claimed stays unresolved with its reason (data-vertical risk 7):
``list_isin_missing``, ``list_isin_invalid``, ``list_isin_unregistered`` (a new listing or
a security the registry never tied to an ISIN), ``list_isin_ambiguous``,
``list_currency_not_usd``, ``list_symbol_repeated``, ``list_isin_repeated``,
``list_observations_disagree``, ``list_not_bracketed`` (no registered or accepted claim
shows the symbol earlier: a new or renamed listing), ``list_symbol_moved`` (the symbol
named another instrument before, or the instrument held another symbol meanwhile: a
reused or renamed ticker), ``list_registered_conflict`` (a registered claim of another
instrument covers the day, or sits between the instrument's earlier claim and the day) and
``list_gap_too_long``. The report names the weekdays between the registry's last ticker
claim and the first observation (``history_gap``, with the registry symbols bridged over it
and those left unbridged) and the unobserved weekdays between observations
(``observation_gaps``).

Every unresolved day also gets a symbol class and a disposition, for the report only (they
never reach a claim). The class is the list row's provider ``Type`` label
(``symbol_class``); the disposition (``disposition``) is ``out_of_scope`` when the class has
no instrument in the Norgate-anchored registry (preferred shares, warrants, units, rights,
debt, mutual funds) and no row of the day names an instrument, ``register`` when the ISIN
is missing or unregistered for a symbol a registered or accepted claim held before (an
ISIN-bearing source for that instrument would resolve it), and ``unresolved`` otherwise
(``unresolved_classes``, ``unresolved_dispositions``). ``unresolved_breakdown`` counts the
rows of waiting US bar sources the same way, judged against the current registry, for the
maintenance report's ``standing_unresolved``.

The registered claims and ISIN owners come from the registry itself, so the result is a
registry document of new assertions only, which registration validates like any other.
The contract is in dev-notes/design/data-vertical.md.
"""

from __future__ import annotations

import re
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage.identity import REGISTRY_SCHEMA, mint_instrument, parse_registry
from aegis_alpha.storage.kr_identity import (
    EODHD_PROVIDER,
    EODHD_SHAPE,
    EODHD_TABLE,
    Evidence,
    MapperReport,
    SourceRows,
    instant_us,
    pinned_rows,
)
from aegis_alpha.storage.source_identity import LINK_PREFIX
from aegis_alpha.storage.us_identity import LIST_SOURCE, NEW_YORK, session_start_us

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from aegis_alpha.storage.workspace import Workspace

MAPPER: Final = "eodhd.us_symbol_list@1"
EXCHANGE: Final = "US"
SOURCE_PREFIX: Final = f"{EODHD_PROVIDER}-{EODHD_SHAPE}-"
SESSION_OPEN: Final = time(9, 30)
MAX_BRIDGE_DAYS: Final = 120
"""The longest gap, in calendar days through the end observation, a claim may bridge."""
_BRIDGE: Final = "bridge:"
_COLUMNS: Final = ("exchange_code", "delisted", "code", "currency", "isin", "retrieved_at_utc")
_WEEKEND: Final = frozenset({5, 6})
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_SAMPLE: Final = 100
BAR_DATASET: Final = "prices.us.eodhd"
"""The dataset whose waiting rows ``unresolved_breakdown`` counts (and its ``.r<N>``)."""
CLASSES: Final[Mapping[str, str]] = {
    "Common Stock": "common_stock",
    "ETF": "etf",
    "FUND": "fund",
    "Mutual Fund": "mutual_fund",
    "Preferred Stock": "preferred",
    "Warrant": "warrant",
    "Unit": "unit",
    "Right": "right",
    "BOND": "debt",
    "Notes": "debt",
}
"""EODHD list ``Type`` labels by symbol class; any other label is ``other``."""
OUT_OF_SCOPE_CLASSES: Final = frozenset(
    {"mutual_fund", "preferred", "warrant", "unit", "right", "debt"}
)
"""Classes the Norgate-anchored US registry holds no instrument kind for."""
REGISTRABLE: Final = frozenset({"list_isin_missing", "list_isin_unregistered"})
"""Reasons an ISIN-bearing source for an already registered instrument would resolve."""
NOT_OBSERVED: Final = "list_not_observed"
CLAIMABLE: Final = "list_claimable"
RESOLVED: Final = "resolved"

_HELD: Final = (
    "SELECT token, instrument_id, valid_from_us, valid_to_us, source_snapshot_id "
    "FROM identity_assertions WHERE provider='eodhd' AND namespace='eodhd_symbol' "
    "AND substr(token, -3) = '.US' AND assertion_id NOT IN (SELECT supersedes_assertion_id "
    "FROM identity_assertions WHERE supersedes_assertion_id IS NOT NULL) "
    "ORDER BY token, valid_from_us"
)
_OWNERS: Final = (
    "SELECT token, instrument_id FROM identity_assertions WHERE provider='fmp' "
    "AND namespace='isin' AND assertion_id NOT IN (SELECT supersedes_assertion_id "
    "FROM identity_assertions WHERE supersedes_assertion_id IS NOT NULL)"
)


def covered_day(retrieved_us: int) -> date:
    """The New York trading day an observation retrieved at ``retrieved_us`` follows."""
    moment = (_EPOCH + timedelta(microseconds=retrieved_us)).astimezone(NEW_YORK)
    day = moment.date()
    return day if moment.time() >= SESSION_OPEN else day - timedelta(days=1)


def _new_york_day(instant_us: int) -> date:
    return (_EPOCH + timedelta(microseconds=instant_us)).astimezone(NEW_YORK).date()


def _weekend(day: date) -> bool:
    return day.weekday() in _WEEKEND


@dataclass(frozen=True, slots=True)
class Held:
    """A registered, not superseded ``eodhd_symbol`` claim of a US symbol."""

    token: str
    instrument_id: str
    start: int
    end: int | None
    listed: bool
    """Whether a symbol list supports it (otherwise the master or an export)."""

    def holds(self, day: date) -> bool:
        at = session_start_us(day)
        return self.start <= at and (self.end is None or at < self.end)


@dataclass(frozen=True, slots=True)
class Registry:
    """What the registry holds that a symbol-list build reads."""

    held: tuple[Held, ...]
    owners: Mapping[str, frozenset[str]]
    """ISIN -> instruments a registered FMP ``isin`` assertion names."""
    anchors: Mapping[str, str]
    """Instrument ID -> its Norgate asset ID."""


def read_registry(state: sqlite3.Connection) -> Registry:
    """The registered US symbol claims, FMP ISIN owners and Norgate anchors (SELECT only)."""
    held = tuple(
        Held(str(token), str(instrument), int(start), None if end is None else int(end),
             str(source).startswith(LIST_SOURCE))
        for token, instrument, start, end, source in state.execute(_HELD)
    )  # fmt: skip
    owners: dict[str, set[str]] = defaultdict(set)
    for token, instrument in state.execute(_OWNERS):
        owners[str(token)].add(str(instrument))
    anchors = {
        str(instrument): str(token)
        for instrument, token in state.execute(
            "SELECT DISTINCT instrument_id, token FROM identity_assertions "
            "WHERE provider='norgate' AND namespace='norgate_assetid'"
        )
        if mint_instrument("norgate_assetid", str(token)) == instrument
    }
    return Registry(held, {key: frozenset(value) for key, value in owners.items()}, anchors)


# --- observations -------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Observation:
    """What one list row says about its symbol on the day it covers."""

    symbol: str
    day: date
    instrument_id: str | None
    reason: str | None
    isin: str | None
    evidence: Evidence
    names: str | None = None
    """The instrument the row's ISIN names (a valid, registered, unambiguous ISIN in USD),
    even when the row is refused for repeating a symbol or an ISIN: such a row contradicts a
    claim of another instrument. A row naming no instrument contradicts nothing."""
    kind: str = "unknown"
    """The row's symbol class (``symbol_class``), for the report only."""

    def outcome(self) -> str:
        return self.instrument_id or f"unresolved:{self.reason}"


def symbol_class(label: object) -> str:
    """The class of a list row's ``Type`` label: ``unknown`` when absent, ``other`` when new."""
    if not isinstance(label, str) or not label:
        return "unknown"
    return CLASSES.get(label, "other")


def disposition(reason: str, kind: str, *, names_instrument: bool, held_before: bool) -> str:
    """How an unresolved symbol-day is handled: ``register``, ``unresolved`` or
    ``out_of_scope``.

    A day no row of which names an instrument is out of scope when its class has no
    instrument kind in the registry. A missing or unregistered ISIN of a symbol some claim
    held before can be registered by an ISIN-bearing source for that instrument. Everything
    else (a new listing, an invalid or repeated ISIN, a moved or unbracketed symbol) stays
    unresolved with its reason: nothing here infers an instrument.
    """
    if kind in OUT_OF_SCOPE_CLASSES and not names_instrument:
        return "out_of_scope"
    if reason in REGISTRABLE and held_before:
        return "register"
    return "unresolved"


def _text(value: object) -> str | None:
    if not isinstance(value, str) or not value or value.strip() != value or " " in value:
        return None
    return value if value.isprintable() else None


def _isin(value: object) -> tuple[str | None, str | None]:
    if value is None or value == "":
        return None, "list_isin_missing"
    try:
        canonical = normalize_identifier(IdentifierType.ISIN, cast("str", value))
    except (IdentifierValueError, TypeError):
        return None, "list_isin_invalid"
    return (canonical, None) if canonical == value else (None, "list_isin_invalid")


def _owner(
    row: Mapping[str, object], registry: Registry
) -> tuple[str | None, str | None, str | None]:
    """(ISIN, the one instrument it names, or why the row names none)."""
    isin, reason = _isin(row["isin"])
    if reason is not None:
        return isin, None, reason
    if row["currency"] != "USD":
        return isin, None, "list_currency_not_usd"
    owners = {
        item for item in registry.owners.get(cast("str", isin), ()) if item in registry.anchors
    }
    if not owners:
        return isin, None, "list_isin_unregistered"
    if len(owners) > 1:
        return isin, None, "list_isin_ambiguous"
    return isin, next(iter(owners)), None


def _observations(
    source: SourceRows, registry: Registry, report: MapperReport
) -> list[Observation]:
    missing = sorted(set(_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"{MAPPER} source {source.snapshot_id} lacks columns {missing}")
    listed: list[Observation] = []
    for row, row_hash in source.records():
        report.rows += 1
        if row["exchange_code"] != EXCHANGE or row["delisted"] != "0":
            report.skip("not_a_listed_us_row")
            continue
        code = _text(row["code"])
        if code is None:
            report.refuse("symbol_invalid")
            continue
        retrieved = instant_us(row["retrieved_at_utc"])
        isin, instrument, reason = _owner(row, registry)
        evidence = Evidence(source.snapshot_id, row_hash, retrieved)
        listed.append(Observation(f"{code}.{EXCHANGE}", covered_day(retrieved), instrument,
                                  reason, isin, evidence, instrument,
                                  symbol_class(row.get("type"))))  # fmt: skip
    return listed


def map_us_symbol_lists(
    sources: Sequence[SourceRows], registry: Registry
) -> tuple[list[Observation], MapperReport]:
    """``eodhd.us_symbol_list@1``: each listed US row as an observation of its day.

    Rows of another exchange or of a delisted list are skipped. Within one list, a symbol
    listed twice and an ISIN two symbols carry name no instrument.
    """
    report = MapperReport()
    found: list[Observation] = []
    for source in sources:
        listed = _observations(source, registry, report)
        symbols: dict[str, int] = defaultdict(int)
        isins: dict[str, set[str]] = defaultdict(set)
        for item in listed:
            symbols[item.symbol] += 1
            if item.isin is not None:
                isins[item.isin].add(item.symbol)
        for item in listed:
            reason = item.reason
            if symbols[item.symbol] > 1:
                reason = "list_symbol_repeated"
            elif item.isin is not None and len(isins[item.isin]) > 1:
                reason = "list_isin_repeated"
            if reason is None:
                report.accepted += 1
                found.append(item)
            else:
                report.refuse(reason)
                found.append(Observation(item.symbol, item.day, None, reason, item.isin,
                                         item.evidence, item.names, item.kind))  # fmt: skip
    return found, report


# --- claims -------------------------------------------------------------------------------


@dataclass(slots=True)
class SymbolClaims:
    """New claims and why every other observed symbol-day has none."""

    assertions: list[dict[str, object]] = field(default_factory=list)
    mapper: MapperReport = field(default_factory=MapperReport)
    unresolved: dict[str, dict[str, set[date]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    bridges: list[dict[str, object]] = field(default_factory=list)
    """Each gap claim: symbol, first and last day, weekdays, and the evidence of both ends."""
    bridge_unresolved: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    """Symbols whose gap could not be bridged, by reason."""
    already_claimed: int = 0
    observed_days: set[date] = field(default_factory=set)
    claimed_days: set[date] = field(default_factory=set)
    registry_through: date | None = None
    first_listed: date | None = None
    registry_symbols: set[str] = field(default_factory=set)
    """Symbols the registry's last master or export claims end with, after its ``through``."""
    continued: set[str] = field(default_factory=set)
    """Those of them a claim covers on the first weekday after ``through``."""
    judged: dict[tuple[str, date], tuple[str, str, str]] = field(default_factory=dict)
    """Each unresolved symbol-day's reason, symbol class and disposition."""
    claimed_kinds: list[str] = field(default_factory=list)
    """The symbol class of each assertion's end observation, in ``assertions`` order."""

    def document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "schema": REGISTRY_SCHEMA,
            "issuers": [],
            "instruments": [],
            "assertions": sorted(
                self.assertions,
                key=lambda row: (str(row["token"]), cast("int", row["valid_from_us"])),
            ),
        }  # fmt: skip
        parse_registry(document)
        return document

    def report(self, *, sample: int | None = _SAMPLE) -> dict[str, object]:
        def cut(values: Iterable[str]) -> list[str]:
            ordered = sorted(values)
            return ordered if sample is None else ordered[:sample]

        observed = sorted(self.observed_days)
        bridges = sorted(self.bridges, key=lambda item: str(item["symbol"]))
        return {
            "mapper": MAPPER,
            "rows": self.mapper.json(),
            "claims": len(self.assertions),
            "claimed_days": [day.isoformat() for day in sorted(self.claimed_days)],
            "already_claimed": self.already_claimed,
            "observed_days": None if not observed else
            {"from": observed[0].isoformat(), "through": observed[-1].isoformat()},
            "registry_through": None if self.registry_through is None
            else self.registry_through.isoformat(),
            "history_gap": self._history_gap(sample),
            "observation_gaps": self._observation_gaps(),
            "bridged_symbols": len({item["symbol"] for item in bridges}),
            "bridged_days": sum(cast("int", item["weekdays"]) for item in bridges),
            "bridge_bounds": None if not bridges else {
                "from": min(str(item["from"]) for item in bridges),
                "through": max(str(item["through"]) for item in bridges)},
            "bridges": bridges if sample is None else bridges[:sample],
            "bridge_unresolved": {reason: len(symbols)
                                  for reason, symbols in sorted(self.bridge_unresolved.items())},
            "unresolved_days": {reason: sum(len(days) for days in symbols.values())
                                for reason, symbols in sorted(self.unresolved.items())},
            "unresolved_symbols": {reason: len(symbols)
                                   for reason, symbols in sorted(self.unresolved.items())},
            "unresolved": {reason: cut(symbols)
                           for reason, symbols in sorted(self.unresolved.items())},
            "unresolved_classes": _tally((reason, kind, 1)
                                         for reason, kind, _ in self.judged.values()),
            "unresolved_dispositions": _tally((handled, reason, 1)
                                              for reason, _, handled in self.judged.values()),
        }  # fmt: skip

    def _history_gap(self, sample: int | None) -> dict[str, object] | None:
        """Weekdays after the registry's last ticker claim before the first symbol list.

        The registry's symbols a claim continues over them are ``bridged_symbols``; the
        rest stay a gap (``unbridged_symbols``, with a sample).
        """
        if self.registry_through is None or self.first_listed is None:
            return None
        gap = _span(self.registry_through + timedelta(days=1), self.first_listed)
        if gap is None:
            return None
        waiting = sorted(self.registry_symbols - self.continued)
        return {**gap, "bridged_symbols": len(self.registry_symbols & self.continued),
                "unbridged_symbols": len(waiting),
                "unbridged": waiting if sample is None else waiting[:sample]}  # fmt: skip

    def _observation_gaps(self) -> list[dict[str, object]]:
        observed = sorted(self.observed_days)
        gaps = []
        for before, after in pairwise(observed):
            gap = _span(before + timedelta(days=1), after)
            if gap is not None:
                gaps.append(gap)
        return gaps


def _tally(items: Iterable[tuple[str, str, int]]) -> dict[str, dict[str, int]]:
    """``{outer: {inner: sum of counts}}`` of ``(outer, inner, count)`` items, sorted."""
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for outer, inner, count in items:
        counts[outer][inner] += count
    return {outer: dict(sorted(inner.items())) for outer, inner in sorted(counts.items())}


def _weekdays(first: date, stop: date) -> list[date]:
    """The weekdays of ``[first, stop)``."""
    days = (first + timedelta(days=n) for n in range((stop - first).days))
    return [day for day in days if not _weekend(day)]


def _span(first: date, stop: date) -> dict[str, object] | None:
    """The weekdays of ``[first, stop)`` as ``{from, through, weekdays}``, or None."""
    weekdays = _weekdays(first, stop)
    if not weekdays:
        return None
    return {"from": weekdays[0].isoformat(), "through": weekdays[-1].isoformat(),
            "weekdays": len(weekdays)}  # fmt: skip


def _assertion(
    assetid: str, symbol: str, first: date, last: date, evidence: Evidence
) -> dict[str, object]:
    return {
        "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": assetid},
        "provider": "eodhd",
        "namespace": "eodhd_symbol",
        "token": symbol,
        "valid_from_us": session_start_us(first),
        "valid_to_us": session_start_us(last + timedelta(days=1)),
        "known_from_us": evidence.known_from_us,
        "supersedes_assertion_id": None,
        "source_snapshot_id": evidence.source,
        "source_hash": evidence.source_hash,
    }


@dataclass(frozen=True, slots=True)
class _Before:
    """The latest day before a symbol-day on which something is shown about the symbol."""

    day: date
    outcome: str
    """The instrument shown holding the symbol, or ``unresolved:<reason>``."""
    evidence: str
    """``registered``, or the ``sl:`` source of the accepted claim's end observation."""


@dataclass(slots=True)
class _Index:
    """What a build judges each symbol-day against.

    Only registered claims and the claims this build accepts anchor a later day: an
    observation the build rejected (a moved, unbracketed or unverifiable row) or a
    Saturday or Sunday one is never the earlier end of a claim, so lists alone can never
    carry a ticker to an instrument that no registered claim shows holding it. Every
    observation that names an instrument, judged or veto-only, rejected or not, is a veto:
    a claim never covers a day on which its symbol is observed naming another instrument or
    its instrument is observed under another symbol. Days are judged in day order, so a
    claim depends only on claims of earlier days, as when the lists arrive day by day.
    """

    held: dict[str, list[Held]]
    """Registered claims by symbol."""
    holdings: dict[str, list[Held]]
    """Registered claims by instrument."""
    accepted: dict[str, list[tuple[date, date, str, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    """Each symbol's claims this build accepted: first day, last day, instrument, source."""
    accepted_by: dict[str, list[tuple[str, date, date]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    """Each instrument's claims this build accepted: symbol, first day, last day."""
    named: dict[str, list[tuple[date, str]]] = field(default_factory=lambda: defaultdict(list))
    """Each instrument's observed days and the symbol naming it, sorted by (day, symbol)."""
    seen: dict[str, list[tuple[date, str]]] = field(default_factory=lambda: defaultdict(list))
    """Each symbol's observed days and the instrument named, sorted by (day, instrument)."""

    def observe(self, observations: Iterable[Observation]) -> None:
        """Index every observation that names an instrument, each list in day order."""
        for item in observations:
            if item.names is not None:
                self.named[item.names].append((item.day, item.symbol))
                self.seen[item.symbol].append((item.day, item.names))
        for found in (*self.named.values(), *self.seen.values()):
            found.sort()

    def accept(self, symbol: str, first: date, last: date, instrument: str, source: str) -> None:
        self.accepted[symbol].append((first, last, instrument, source))
        self.accepted_by[instrument].append((symbol, first, last))

    def before(self, symbol: str, day: date) -> _Before | None:
        """The latest earlier day a registered or accepted claim shows the symbol's holder."""
        found: _Before | None = None
        for _, last, instrument, source in self.accepted.get(symbol, []):
            if last < day and (found is None or last >= found.day):
                found = _Before(last, instrument, source)
        start = session_start_us(day)
        for item in self.held.get(symbol, []):
            if item.start >= start:
                continue
            last = day - timedelta(days=1)
            if item.end is not None and item.end <= start:
                last = _new_york_day(item.end - 1)
            if found is None or last >= found.day:
                found = _Before(last, item.instrument_id, "registered")
        return found

    def held_before(self, symbol: str, instrument: str, day: date) -> bool:
        """Whether a registered or accepted claim shows ``instrument`` holding ``symbol``
        before ``day``."""
        start = session_start_us(day)
        if any(item.instrument_id == instrument and item.start < start
               for item in self.held.get(symbol, [])):  # fmt: skip
            return True
        return any(first < day and held == instrument
                   for first, _, held, _ in self.accepted.get(symbol, []))  # fmt: skip

    def contradicted(self, symbol: str, instrument: str, first: date, last: date) -> bool:
        """Whether ``symbol`` is observed naming another instrument on a day of the claim."""
        days = self.seen.get(symbol, [])
        position = bisect_left(days, (first, ""))
        return any(named != instrument for seen, named in days[position:] if seen <= last)

    def other_symbol(self, instrument: str, symbol: str, first: date, last: date) -> bool:
        """Whether ``instrument`` holds or is observed under another US symbol on a day of
        ``[first, last]``."""
        low, high = session_start_us(first), session_start_us(last + timedelta(days=1))
        for item in self.holdings.get(instrument, []):
            if item.token != symbol and item.start < high and (item.end is None or item.end > low):
                return True
        if any(other != symbol and start <= last and first <= end
               for other, start, end in self.accepted_by.get(instrument, [])):  # fmt: skip
            return True
        days = self.named.get(instrument, [])
        position = bisect_left(days, (first, ""))
        return any(other != symbol for seen, other in days[position:] if seen <= last)


def build_symbol_claims(
    sources: Sequence[SourceRows],
    registry: Registry,
    context: Sequence[SourceRows] = (),
) -> SymbolClaims:
    """The ``eodhd_symbol`` claims the lists show beyond the registered ones.

    ``context`` lists (earlier lists a maintenance run reads again) are veto evidence
    only: their rows can stop a claim, never anchor or make one.
    """
    claims = SymbolClaims()
    observations, claims.mapper = map_us_symbol_lists(sources, registry)
    vetoes, _ = map_us_symbol_lists(context, registry)
    index = _Index(defaultdict(list), defaultdict(list))
    for item in registry.held:
        index.held[item.token].append(item)
        index.holdings[item.instrument_id].append(item)
    index.observe([*observations, *vetoes])
    _registry_edge(claims, registry)
    grouped: dict[tuple[str, date], list[Observation]] = defaultdict(list)
    for item in observations:
        grouped[item.symbol, item.day].append(item)
        claims.observed_days.add(item.day)
    listed_days = [_new_york_day(item.start) for item in registry.held if item.listed]
    claims.first_listed = min([*listed_days, *claims.observed_days], default=None)
    days = _outcomes(grouped)
    for (symbol, day), outcome in sorted(days.items(), key=lambda item: (item[0][1], item[0][0])):
        if _weekend(day):
            continue
        judged = _judge(symbol, day, outcome, index)
        if judged is None:
            claims.already_claimed += 1
            continue
        if isinstance(judged, str):
            if judged.startswith(_BRIDGE):
                judged = judged.removeprefix(_BRIDGE)
                claims.bridge_unresolved[judged].add(symbol)
            claims.unresolved[judged][symbol].add(day)
            claims.judged[symbol, day] = _classified(
                judged, grouped[symbol, day], held_before=index.before(symbol, day) is not None
            )
            continue
        first, start = judged
        evidence = min((item.evidence for item in grouped[symbol, day]),
                       key=lambda item: item.order())  # fmt: skip
        claims.assertions.append(
            _assertion(registry.anchors[outcome], symbol, first, day, evidence)
        )
        claims.claimed_kinds.append(_classified("", grouped[symbol, day], held_before=True)[1])
        claims.claimed_days.add(day)
        index.accept(symbol, first, day, outcome, evidence.source)
        if first < day:
            claims.bridges.append({
                "symbol": symbol, "from": first.isoformat(), "through": day.isoformat(),
                "weekdays": len(_weekdays(first, day + timedelta(days=1))),
                "start": {"day": start.day.isoformat(), "evidence": start.evidence},
                "end": {"day": day.isoformat(), "evidence": evidence.source},
            })  # fmt: skip
    _continued(claims, registry)
    return claims


def _classified(
    reason: str, items: Sequence[Observation], *, held_before: bool
) -> tuple[str, str, str]:
    """(reason, symbol class, disposition) of an unresolved symbol-day's observations."""
    kinds = {item.kind for item in items}
    kind = kinds.pop() if len(kinds) == 1 else "mixed"
    names = any(item.names is not None for item in items)
    return reason, kind, disposition(reason, kind, names_instrument=names,
                                     held_before=held_before)  # fmt: skip


def _registry_edge(claims: SymbolClaims, registry: Registry) -> None:
    """The registry's ``through`` and the symbols its last master or export claims end with."""
    ends = [item.end for item in registry.held if not item.listed and item.end is not None]
    if not ends:
        return
    edge = max(ends)
    claims.registry_through = _new_york_day(edge - 1)
    claims.registry_symbols = {item.token for item in registry.held
                               if not item.listed and item.end == edge}  # fmt: skip


def _continued(claims: SymbolClaims, registry: Registry) -> None:
    """The registry's symbols a list claim, registered or new, covers on the first weekday
    after ``through``."""
    if claims.registry_through is None:
        return
    first = claims.registry_through + timedelta(days=1)
    while _weekend(first):
        first += timedelta(days=1)
    claims.continued = {item.token for item in registry.held if item.listed and item.holds(first)}
    at = session_start_us(first)
    for row in claims.assertions:
        if cast("int", row["valid_from_us"]) <= at < cast("int", row["valid_to_us"]):
            claims.continued.add(str(row["token"]))


def _judge(
    symbol: str, day: date, outcome: str, index: _Index
) -> str | tuple[date, _Before] | None:
    """Why a symbol-day gets no claim, None when a registered claim holds it already, or
    the first day of its claim and the earlier evidence the claim continues from."""
    if outcome.startswith("unresolved:"):
        return outcome.removeprefix("unresolved:")
    covering = {item.instrument_id for item in index.held.get(symbol, []) if item.holds(day)}
    if covering == {outcome}:
        return None
    if covering:
        return "list_registered_conflict"
    before = index.before(symbol, day)
    if before is None or before.outcome.startswith("unresolved:"):
        return "list_not_bracketed"
    if before.outcome != outcome:
        # The instrument held the symbol earlier and a registered claim of another one sits
        # in between: the registry contradicts a continuation, not only today's list.
        earlier = before.evidence == "registered" and index.held_before(symbol, outcome, day)
        return "list_registered_conflict" if earlier else "list_symbol_moved"
    return _continue(symbol, day, outcome, before, index)


def _continue(
    symbol: str, day: date, outcome: str, before: _Before, index: _Index
) -> str | tuple[date, _Before]:
    """The first day a claim continuing ``before`` covers through ``day``, or why none.

    Weekend days alone between the two need no claim; a gap with a weekday is bridged by
    one claim from the day after ``before`` when it is at most ``MAX_BRIDGE_DAYS`` long.
    Either claim is refused when an observation of the symbol on one of its days names
    another instrument (``list_contradicted``), or the instrument holds or is observed
    under another symbol on one of them (``list_symbol_moved``).
    """
    first = before.day + timedelta(days=1)
    bridged = bool(_weekdays(first, day))
    reason = None
    if not bridged:
        first = day
    elif (day - first).days + 1 > MAX_BRIDGE_DAYS:
        reason = "list_gap_too_long"
    if reason is None and index.contradicted(symbol, outcome, first, day):
        reason = "list_contradicted"
    if reason is None and index.other_symbol(outcome, symbol, first, day):
        reason = "list_symbol_moved"
    if reason is not None:
        return f"{_BRIDGE}{reason}" if bridged else reason
    return first, before


def _outcomes(grouped: Mapping[tuple[str, date], list[Observation]]) -> dict[tuple[str, date], str]:
    """One outcome per symbol-day: the instrument every observation of it names, or why not.

    An instrument two symbols name on one day is named by neither.
    """
    days: dict[tuple[str, date], str] = {}
    for key, items in grouped.items():
        outcomes = {item.outcome() for item in items}
        days[key] = (
            outcomes.pop() if len(outcomes) == 1 else "unresolved:list_observations_disagree"
        )
    named: dict[tuple[str, date], set[str]] = defaultdict(set)
    for (symbol, day), outcome in days.items():
        if not outcome.startswith("unresolved:"):
            named[outcome, day].add(symbol)
    for (_, day), symbols in named.items():
        if len(symbols) > 1:
            for symbol in symbols:
                days[symbol, day] = "unresolved:list_isin_repeated"
    return days


# --- workspace ----------------------------------------------------------------------------


def list_sources(workspace: Workspace) -> dict[str, int]:
    """Committed, linked US symbol lists by source ID, with their ``sl:`` link instants.

    A list is a US list when one of its rows was asked for ``EXCHANGE_CODE=US``.
    """
    from aegis_alpha.storage.provider_collection import committed_tables  # noqa: PLC0415

    linked = {
        str(snapshot)[len(LINK_PREFIX) :]: int(at)
        for snapshot, at in workspace.state.execute(
            "SELECT snapshot_id, retrieved_at_us FROM source_snapshots WHERE snapshot_id LIKE ?",
            (LIST_SOURCE + "%",),
        )
    }
    found: dict[str, int] = {}
    for table in committed_tables(workspace, SOURCE_PREFIX):
        entry = table.entry
        if table.store != "market" or entry["name"] != EODHD_TABLE or table.source_id not in linked:
            continue
        target = '"' + str(entry["target"]).replace('"', '""') + '"'
        row = workspace.market.execute(
            f"SELECT count(*) FROM {target} WHERE exchange_code = ?",  # noqa: S608 -- quoted manifest identifier
            [EXCHANGE],
        ).fetchone()
        if row is not None and int(row[0]):
            found[table.source_id] = linked[table.source_id]
    return found


@dataclass(frozen=True, slots=True)
class Earlier:
    """A list linked before a run's new lists: veto evidence its build may load."""

    linked_at_us: int
    load: Callable[[Callable[[Mapping[str, object]], bool]], SourceRows]
    """Read the list, keeping only the rows the given predicate keeps."""


def build_increment(
    fresh: Sequence[SourceRows], registry: Registry, earlier: Sequence[Earlier], floor: int
) -> tuple[SymbolClaims, int]:
    """Build the new lists' claims with the earlier lists their claims' days need as vetoes.

    A claim's days reach back to the day after its earlier end; any list that covers one of
    them was linked at or after that day's New York start. So the build runs on the new
    lists (``floor``: the earliest of their link instants), then reads the earlier lists
    linked from the earliest such start up to ``floor`` (keeping only
    rows of the claims' symbols or their instruments' ISINs) and builds again, until no
    claim needs an earlier list it has not read. A daily run whose claims start today
    reads none. Returns the claims and the number of earlier lists read.
    """
    if not fresh:
        return build_symbol_claims(fresh, registry), 0
    isins: dict[str, set[str]] = defaultdict(set)
    for value, owners in registry.owners.items():
        for owner in owners:
            isins[owner].add(value)
    need: int | None = None
    codes: set[str] = set()
    wanted: set[str] = set()
    context: list[SourceRows] = []
    while True:
        built = build_symbol_claims(fresh, registry, context)
        reaching = [row for row in built.assertions if cast("int", row["valid_from_us"]) < floor]
        if not reaching:
            return built, len(context)
        start = min(cast("int", row["valid_from_us"]) for row in reaching)
        more_codes = {str(row["token"]).removesuffix(f".{EXCHANGE}") for row in reaching}
        more_isins = {value for row in reaching for value in isins[_owner_of(row, registry)]}
        if need is not None and start >= need and more_codes <= codes and more_isins <= wanted:
            return built, len(context)
        need = start if need is None else min(need, start)
        codes |= more_codes
        wanted |= more_isins

        keep = _keeper(frozenset(codes), frozenset(wanted))
        context = [item.load(keep) for item in earlier if need <= item.linked_at_us < floor]


def _keeper(codes: frozenset[str], isins: frozenset[str]) -> Callable[[Mapping[str, object]], bool]:
    """Keep a list row of one of ``codes`` or naming one of ``isins``."""
    return lambda row: row.get("code") in codes or row.get("isin") in isins


def _owner_of(row: Mapping[str, object], registry: Registry) -> str:
    anchor = cast("dict[str, str]", row["instrument"])["anchor_token"]
    return next(key for key, value in registry.anchors.items() if value == anchor)


def build_from_workspace(
    workspace: Workspace, fresh: Mapping[str, int], earlier: Mapping[str, int] | None = None
) -> tuple[SymbolClaims, int]:
    """Read the new US lists (source ID -> link instant) and the registry and build their
    claims; ``earlier`` are the lists linked before them, read again only as vetoes."""
    sources = [pinned_rows(workspace, source, EODHD_TABLE) for source in sorted(fresh)]

    def loader(source: str) -> Callable[[Callable[[Mapping[str, object]], bool]], SourceRows]:
        return lambda keep: pinned_rows(workspace, source, EODHD_TABLE, keep)

    older = [Earlier(at, loader(source)) for source, at in sorted((earlier or {}).items())]
    floor = min(fresh.values(), default=0)
    return build_increment(sources, read_registry(workspace.state), older, floor)


# --- the waiting rows of the maintenance report ------------------------------------------

_JSON_ROW: Final = "(CASE WHEN json_valid(source_row_json) THEN source_row_json END)"
_UNREADABLE: Final = "unreadable_row"


def covers(dataset_id: str) -> bool:
    """Whether ``unresolved_breakdown`` counts a dataset's waiting rows."""
    return (
        dataset_id == BAR_DATASET
        or re.fullmatch(re.escape(BAR_DATASET) + r"\.r[0-9]+", dataset_id) is not None
    )


def _waiting_rows(
    workspace: Workspace, tables: Iterable[tuple[str, str]]
) -> dict[tuple[str | None, date | None], int]:
    """Rows by (symbol, session date) of each (source ID, table) still listed.

    A bar table names them in ``provider_symbol`` and ``date``; a held bulk table in its
    provider JSON (``code`` and ``exchange_short_name``, ``date``). A table of another
    shape, or a retired one, is not read.
    """
    from aegis_alpha.storage.promotion.formats import quote_identifier  # noqa: PLC0415
    from aegis_alpha.storage.source_library import list_tables  # noqa: PLC0415

    counts: dict[tuple[str | None, date | None], int] = defaultdict(int)
    for source_id, name in tables:
        try:
            described = next(item for item in list_tables(workspace, source_id)
                             if item["name"] == name)  # fmt: skip
        except (ValueError, StopIteration):
            continue
        columns = set(cast("list[str]", described["columns"]))
        target = quote_identifier(str(described["target"]))
        if {"provider_symbol", "date"} <= columns:
            query = f'SELECT provider_symbol, "date", count(*) FROM {target} GROUP BY ALL'  # noqa: S608 -- quoted manifest identifier
        elif {"reason", "source_row_json"} <= columns:
            text = "json_extract_string(j, '$.{}')".format
            query = (
                f"SELECT {text('code')} || '.' || {text('exchange_short_name')}, "  # noqa: S608 -- quoted manifest identifier
                f"TRY_CAST({text('date')} AS DATE), count(*) "
                f"FROM (SELECT {_JSON_ROW} AS j FROM {target}) GROUP BY ALL"
            )
        else:
            continue
        for symbol, day, count in workspace.market.execute(query).fetchall():
            counts[symbol, day] += int(count)
    return counts


def _judge_now(
    workspace: Workspace, registry: Registry, waiting: Iterable[tuple[str, date]]
) -> SymbolClaims:
    """One build of the US lists over the waiting symbol-days, against ``registry``.

    Only the rows of the waiting symbols and of the ISINs their rows' instruments carry are
    read, from the lists linked since ``MAX_BRIDGE_DAYS`` before the earliest waiting day.
    """
    pairs = list(waiting)
    codes = frozenset(symbol.removesuffix(f".{EXCHANGE}") for symbol, _ in pairs)
    floor = session_start_us(min(day for _, day in pairs) - timedelta(days=MAX_BRIDGE_DAYS))
    lists = sorted(source for source, at in list_sources(workspace).items() if at >= floor)

    def read(keep: Callable[[Mapping[str, object]], bool]) -> list[SourceRows]:
        return [pinned_rows(workspace, source, EODHD_TABLE, keep) for source in lists]

    named = read(_keeper(codes, frozenset()))
    carried: dict[str, set[str]] = defaultdict(set)
    for value, owners in registry.owners.items():
        for owner in owners:
            carried[owner].add(value)
    isins: set[str] = set()
    for source in named:
        position = source.columns.index("isin")
        for row in source.rows:
            for owner in registry.owners.get(cast("str", row[position]), ()):
                isins |= carried[owner]
    sources = read(_keeper(codes, frozenset(isins))) if isins else named
    return build_symbol_claims(sources, registry)


def _judged_rows(
    workspace: Workspace, registry: Registry, waiting: Mapping[tuple[str, date], int]
) -> list[tuple[str, str, str, int]]:
    """(reason, symbol class, disposition, rows) of each waiting symbol-day, judged now."""
    built = _judge_now(workspace, registry, waiting)
    claimed: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    for row, kind in zip(built.assertions, built.claimed_kinds, strict=True):
        claimed[str(row["token"])].append(
            (cast("int", row["valid_from_us"]), cast("int", row["valid_to_us"]), kind)
        )
    found: list[tuple[str, str, str, int]] = []
    for (symbol, day), count in waiting.items():
        at = session_start_us(day)
        judged = built.judged.get((symbol, day))
        kinds = [kind for start, end, kind in claimed.get(symbol, ()) if start <= at < end]
        if judged is not None:
            found.append((*judged, count))
        elif kinds:
            found.append((CLAIMABLE, kinds[0], "register", count))
        else:
            found.append((NOT_OBSERVED, "unknown", "unresolved", count))
    return found


def add_breakdowns(
    workspace: Workspace,
    summary: Mapping[str, dict[str, object]],
    tables: Mapping[str, Sequence[tuple[str, str]]],
) -> None:
    """Add ``unresolved_breakdown`` to each standing entry of a dataset it ``covers``."""
    for dataset_id, entry in summary.items():
        if covers(dataset_id):
            entry.update(unresolved_breakdown(workspace, tables[dataset_id]))


def unresolved_breakdown(
    workspace: Workspace, tables: Iterable[tuple[str, str]]
) -> dict[str, object]:
    """The rows of waiting US bar sources by reason, symbol class and disposition.

    Judged now, not when the rows were recorded: a row whose symbol a registered claim
    holds at the New York start of its date is ``resolved`` (its source waits for the next
    promotion). Every other row takes the reason one build of the US lists gives its
    symbol-day today (``list_claimable`` when that build would claim it, so the identity
    stage registers it next; ``list_not_observed`` when no list row covers it), with the
    symbol class and disposition of ``disposition``. SELECT only.
    """
    rows = _waiting_rows(workspace, tables)
    registry = read_registry(workspace.state)
    held: dict[str, list[Held]] = defaultdict(list)
    for item in registry.held:
        held[item.token].append(item)
    reasons: dict[str, int] = defaultdict(int)
    found: list[tuple[str, str, str, int]] = []
    waiting: dict[tuple[str, date], int] = {}
    for (symbol, day), count in rows.items():
        if symbol is None or day is None:
            found.append((_UNREADABLE, "unknown", "unresolved", count))
        elif any(item.holds(day) for item in held.get(symbol, ())):
            reasons[RESOLVED] += count
        else:
            waiting[symbol, day] = count
    if waiting:
        found.extend(_judged_rows(workspace, registry, waiting))
    dispositions: dict[str, int] = defaultdict(int)
    for reason, _, handled, count in found:
        reasons[reason] += count
        dispositions[handled] += count
    return {
        "rows_by_reason": dict(sorted(reasons.items())),
        "rows_by_class": _tally((reason, kind, count) for reason, kind, _, count in found),
        "rows_by_disposition": dict(sorted(dispositions.items())),
    }
