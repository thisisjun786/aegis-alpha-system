"""``eodhd.us_symbol_list@1``: dated EODHD US symbol lists continue US ticker claims by day.

Norgate is a frozen source, so the US registry's ticker claims (``us_identity``) end at the
New York start of the day after the last session its master or history export shows.
This mapper adds claims only for days a dated observation shows, never across a weekday
no observation shows:

- An observation is one row of a committed ``qveris-eodhd-exchange-symbols-*`` list asked
  for ``EXCHANGE_CODE=US`` without delisted symbols, retrieved at the instant its page
  receipt records. It covers the New York trading day it follows: the New York date of
  its retrieval, or the day before when it was retrieved before 09:30 New York time.
- A row names an instrument only through its ISIN: the ISIN must carry a valid check
  digit and be the registered, not superseded FMP ``isin`` of exactly one instrument with
  a Norgate asset ID, and the row's currency must be USD. A ticker, a name or an exchange
  never names an instrument, and a list never mints one.
- A weekday ``d`` is claimed for ``<Code>.US`` and instrument ``I`` only when every
  observation covering ``d`` names ``I`` for the symbol, no other symbol of those
  observations names ``I``, and ``I`` held the symbol on the latest earlier day that is
  not a Saturday or Sunday, shown by an observation covering that day or by a registered
  claim (the master's, an export's or an earlier list day's). Saturdays and Sundays
  between the two need no observation. Today's list is no evidence for an earlier day.
- The claim is valid for exactly that day, from the New York start of ``d`` to that of
  the next day, the instants ``eodhd.bars@1`` resolves, so a claim is never extended
  over a day it was not observed; it is known from the retrieval instant of the earliest
  observation covering ``d`` and cites that row. A day a registered claim of the symbol
  already covers is left to it, so claims never overlap.

Every day that cannot be claimed stays unresolved with its reason (data-vertical risk 7):
``list_isin_missing``, ``list_isin_invalid``, ``list_isin_unregistered`` (a new listing or
a security the registry never tied to an ISIN), ``list_isin_ambiguous``,
``list_currency_not_usd``, ``list_symbol_repeated``, ``list_isin_repeated``,
``list_observations_disagree``, ``list_not_bracketed`` (the latest earlier weekday is not
observed, or its observation names no instrument), ``list_symbol_moved`` (the symbol
named another instrument that day: a reused or renamed ticker) and
``list_registered_conflict`` (a registered claim of another instrument covers the day).
The report names the weekdays between the registry's last ticker claim and the first
observation (``history_gap``) and the unobserved weekdays between observations
(``observation_gaps``); no source in the registry covers them.

The registered claims and ISIN owners come from the registry itself, so the result is a
registry document of new assertions only, which registration validates like any other.
The contract is in dev-notes/design/data-vertical.md.
"""

from __future__ import annotations

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
    from collections.abc import Iterable, Mapping, Sequence

    from aegis_alpha.storage.workspace import Workspace

MAPPER: Final = "eodhd.us_symbol_list@1"
EXCHANGE: Final = "US"
SOURCE_PREFIX: Final = f"{EODHD_PROVIDER}-{EODHD_SHAPE}-"
SESSION_OPEN: Final = time(9, 30)
_COLUMNS: Final = ("exchange_code", "delisted", "code", "currency", "isin", "retrieved_at_utc")
_WEEKEND: Final = frozenset({5, 6})
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_SAMPLE: Final = 100

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

    def outcome(self) -> str:
        return self.instrument_id or f"unresolved:{self.reason}"


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
                                  reason, isin, evidence))  # fmt: skip
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
                                         item.evidence))  # fmt: skip
    return found, report


# --- claims -------------------------------------------------------------------------------


@dataclass(slots=True)
class SymbolClaims:
    """New per-day claims and why every other observed symbol-day has none."""

    assertions: list[dict[str, object]] = field(default_factory=list)
    mapper: MapperReport = field(default_factory=MapperReport)
    unresolved: dict[str, dict[str, set[date]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    already_claimed: int = 0
    observed_days: set[date] = field(default_factory=set)
    claimed_days: set[date] = field(default_factory=set)
    registry_through: date | None = None
    first_listed: date | None = None

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
            "history_gap": self._history_gap(),
            "observation_gaps": self._observation_gaps(),
            "unresolved_days": {reason: sum(len(days) for days in symbols.values())
                                for reason, symbols in sorted(self.unresolved.items())},
            "unresolved_symbols": {reason: len(symbols)
                                   for reason, symbols in sorted(self.unresolved.items())},
            "unresolved": {reason: cut(symbols)
                           for reason, symbols in sorted(self.unresolved.items())},
        }  # fmt: skip

    def _history_gap(self) -> dict[str, object] | None:
        """Weekdays after the registry's last ticker claim that no symbol list observed."""
        if self.registry_through is None or self.first_listed is None:
            return None
        return _span(self.registry_through + timedelta(days=1), self.first_listed)

    def _observation_gaps(self) -> list[dict[str, object]]:
        observed = sorted(self.observed_days)
        gaps = []
        for before, after in pairwise(observed):
            gap = _span(before + timedelta(days=1), after)
            if gap is not None:
                gaps.append(gap)
        return gaps


def _span(first: date, stop: date) -> dict[str, object] | None:
    """The weekdays of ``[first, stop)`` as ``{from, through, weekdays}``, or None."""
    days = [first + timedelta(days=n) for n in range((stop - first).days)]
    weekdays = [day for day in days if not _weekend(day)]
    if not weekdays:
        return None
    return {"from": weekdays[0].isoformat(), "through": weekdays[-1].isoformat(),
            "weekdays": len(weekdays)}  # fmt: skip


def _assertion(assetid: str, symbol: str, day: date, evidence: Evidence) -> dict[str, object]:
    return {
        "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": assetid},
        "provider": "eodhd",
        "namespace": "eodhd_symbol",
        "token": symbol,
        "valid_from_us": session_start_us(day),
        "valid_to_us": session_start_us(day + timedelta(days=1)),
        "known_from_us": evidence.known_from_us,
        "supersedes_assertion_id": None,
        "source_snapshot_id": evidence.source,
        "source_hash": evidence.source_hash,
    }


def _bracket(
    symbol: str,
    day: date,
    instrument: str,
    days: Mapping[tuple[str, date], str],
    held: Sequence[Held],
) -> str | None:
    """None when ``instrument`` held ``symbol`` on the latest earlier weekday, else why not.

    Saturdays and Sundays between the two days need no observation; the first other day
    without one leaves ``day`` unbracketed.
    """
    previous = day - timedelta(days=1)
    for _ in range(3):
        named = days.get((symbol, previous))
        if named is None:
            holders = {item.instrument_id for item in held if item.holds(previous)}
            named = next(iter(holders)) if len(holders) == 1 else None
        if named == instrument:
            return None
        if named is not None and not named.startswith("unresolved:"):
            return "list_symbol_moved"
        if named is not None or not _weekend(previous):
            return "list_not_bracketed"
        previous -= timedelta(days=1)
    return "list_not_bracketed"


def build_symbol_claims(sources: Sequence[SourceRows], registry: Registry) -> SymbolClaims:
    """The per-day ``eodhd_symbol`` claims the lists show beyond the registered ones."""
    claims = SymbolClaims()
    observations, claims.mapper = map_us_symbol_lists(sources, registry)
    by_symbol: dict[str, list[Held]] = defaultdict(list)
    for item in registry.held:
        by_symbol[item.token].append(item)
    master_ends = [item.end for item in registry.held if not item.listed and item.end is not None]
    if master_ends:
        claims.registry_through = _new_york_day(max(master_ends) - 1)
    listed_days = [_new_york_day(item.start) for item in registry.held if item.listed]
    grouped: dict[tuple[str, date], list[Observation]] = defaultdict(list)
    for item in observations:
        grouped[item.symbol, item.day].append(item)
        claims.observed_days.add(item.day)
    claims.first_listed = min([*listed_days, *claims.observed_days], default=None)
    days = _outcomes(grouped)
    for (symbol, day), outcome in sorted(days.items()):
        if _weekend(day):
            continue
        held = by_symbol.get(symbol, [])
        covering = {item.instrument_id for item in held if item.holds(day)}
        if covering == {outcome}:
            claims.already_claimed += 1
            continue
        reason = _judge(symbol, day, outcome, days, held, covering=covering)
        if reason is not None:
            claims.unresolved[reason][symbol].add(day)
            continue
        evidence = min((item.evidence for item in grouped[symbol, day]),
                       key=lambda item: item.order())  # fmt: skip
        claims.assertions.append(_assertion(registry.anchors[outcome], symbol, day, evidence))
        claims.claimed_days.add(day)
    return claims


def _judge(  # noqa: PLR0913 -- one symbol-day and what it is judged against
    symbol: str,
    day: date,
    outcome: str,
    days: Mapping[tuple[str, date], str],
    held: Sequence[Held],
    *,
    covering: set[str],
) -> str | None:
    """Why a symbol-day gets no claim, or None when it gets one."""
    if outcome.startswith("unresolved:"):
        return outcome.removeprefix("unresolved:")
    if covering:
        return "list_registered_conflict"
    return _bracket(symbol, day, outcome, days, held)


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


def build_from_workspace(workspace: Workspace, source_ids: Sequence[str]) -> SymbolClaims:
    """Read the named US lists and the registry, and build their new per-day claims."""
    sources = [pinned_rows(workspace, source, EODHD_TABLE) for source in sorted(source_ids)]
    return build_symbol_claims(sources, read_registry(workspace.state))
