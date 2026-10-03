"""Universe documents: Norgate index membership and the US listing universe.

Two universe mappers turn committed, ``sl:``-linked source rows into whole
``aas-universe-version-v1`` documents, registered as chunked manifests
(``membership_pins.register_universe_manifest``).

- ``norgate.index_membership@1`` reads Norgate ``index_constituent_timeseries`` tables
  (``norgate.index_membership@1`` legacy import, table ``constituents``: one row per
  (asset ID, index, date) with ``index_constituent`` ``0`` or ``1``). Each index is one
  universe ``index.us.norgate/<index name>``. A pair's daily values are compressed into
  intervals: every run of consecutive ``1`` rows is one member valid from the New York
  start of its first date until the New York start of the day after its last date. Expanding
  the intervals over the pair's own dates gives back exactly its daily values. A pair whose
  values are not ``0``/``1``, whose dates are not ``YYYY-MM-DD`` or not strictly increasing,
  whose asset ID or index name is not canonical, or that two sources both carry is refused
  as a whole, never repaired.
- ``norgate.listings@1`` reads the Norgate security master (``norgate.master@1`` rows) and
  makes the universe ``listing.us.norgate``: each listing is a member from the New York
  start of its ``first_date`` until the New York start of the day after its ``last_date``.
  A listing without both dates, with reversed dates, or whose asset ID two rows share is
  refused.

Members are the instruments ``mint_instrument('norgate_assetid', assetid)`` the identity
registry already holds; a member's instrument row is copied from the registry, and an
asset ID the registry lacks stays out of the document and is reported as unresolved. The
source rows record no retrieval instant, so every member is known from its source's
``sl:`` link instant and cites that source. Norgate is a frozen source, so its coverage ends
at its last exported date. The contract is in dev-notes/design/data-vertical.md.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.identity import mint_instrument
from aegis_alpha.storage.kr_identity import MapperReport
from aegis_alpha.storage.membership_pins import (
    UniversePin,
    plan_membership_manifest,
    register_universe_manifest,
)
from aegis_alpha.storage.source_identity import LINK_PREFIX
from aegis_alpha.storage.us_identity import (
    MASTER_TABLE,
    LinkedRows,
    Listing,
    link_instant,
    map_norgate_master,
    read_rows,
    session_start_us,
)

if TYPE_CHECKING:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.workspace import Workspace

INDEX_MAPPER: Final = "norgate.index_membership@1"
LISTING_MAPPER: Final = "norgate.listings@1"
MEMBERSHIP_TABLE: Final = "constituents"
INDEX_UNIVERSE_PREFIX: Final = "index.us.norgate/"
LISTING_UNIVERSE: Final = "listing.us.norgate"
_UNIVERSE_SCHEMA: Final = "aas-universe-version-v1"
_HASH_FORMAT: Final = "aas-canonical-json-sha256-v1"
_MEMBERSHIP_COLUMNS: Final = ("assetid", "indexname", "date", "index_constituent")
_SAMPLE: Final = 100

type Record = dict[str, object]
type Interval = tuple[date, date]


# --- interval compression -----------------------------------------------------------------


def compress(series: Sequence[tuple[date, bool]]) -> list[Interval]:
    """The ``(first, last)`` member dates of each run of consecutive member rows.

    ``series`` is one pair's rows in strictly increasing date order. This is the reference
    form of the SQL in ``index_intervals``.
    """
    intervals: list[Interval] = []
    start: date | None = None
    previous: date | None = None
    for day, member in series:
        if previous is not None and day <= previous:
            raise ValueError("membership dates must be strictly increasing")
        if member and start is None:
            start = day
        if not member and start is not None and previous is not None:
            intervals.append((start, previous))
            start = None
        previous = day
    if start is not None and previous is not None:
        intervals.append((start, previous))
    return intervals


def interval_us(first: date, last: date) -> tuple[int, int]:
    """``[valid_from_us, valid_to_us)``: New York start of ``first`` to that of ``last + 1``."""
    return session_start_us(first), session_start_us(last + timedelta(days=1))


def expand(intervals: Iterable[tuple[int, int | None]], days: Sequence[date]) -> list[bool]:
    """Whether each of ``days`` (at its New York start) falls in any ``[from, to)`` interval."""
    spans = list(intervals)
    result = []
    for day in days:
        moment = session_start_us(day)
        result.append(
            any(start <= moment and (end is None or moment < end) for start, end in spans)
        )
    return result


# --- norgate.index_membership@1 -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Pair:
    """One (asset ID, index) series of one source and its member intervals."""

    assetid: str
    index: str
    source: str
    rows: int
    first: date | None
    last: date | None
    intervals: tuple[Interval, ...]


# Per pair: row count, refusal counts and date span; per run of member rows: its first and
# last date. Runs are islands: within a pair, the row number minus the row number among the
# pair's rows with the same value is constant over a run of equal values.
_INDEX_SQL: Final = """
WITH r AS (
  SELECT _aas_ordinal AS o, assetid, indexname, "date" AS d, index_constituent AS v,
         try_strptime("date", '%Y-%m-%d')::DATE AS day
  FROM {table}
), c AS (
  SELECT *, lag(day) OVER w AS prev, row_number() OVER w AS n,
         row_number() OVER (PARTITION BY assetid, indexname, v ORDER BY o) AS nv
  FROM r WINDOW w AS (PARTITION BY assetid, indexname ORDER BY o)
)
SELECT assetid, indexname, 'pair' AS kind, count(*) AS rows,
       count(*) FILTER (WHERE v IS NULL OR v NOT IN ('0', '1')) AS bad_value,
       count(*) FILTER (WHERE day IS NULL OR strftime(day, '%Y-%m-%d') <> d) AS bad_date,
       count(*) FILTER (WHERE prev IS NOT NULL AND day <= prev) AS unordered,
       min(day) AS first_day, max(day) AS last_day
FROM c GROUP BY assetid, indexname
UNION ALL
SELECT assetid, indexname, 'run', count(*), 0, 0, 0, min(day), max(day)
FROM c WHERE v = '1' GROUP BY assetid, indexname, n - nv
ORDER BY assetid, indexname, kind, first_day
"""


def _pinned_table(workspace: Workspace, source_id: str, table: str) -> str:
    """Verify one committed DuckDB source table against its marker; return its target name."""
    from aegis_alpha.storage.source_library import list_sources, list_tables  # noqa: PLC0415
    from aegis_alpha.storage.source_reader import SourcePin, resolve_source  # noqa: PLC0415

    source = next((row for row in list_sources(workspace) if row["source_id"] == source_id), None)
    if source is None:
        raise ValueError(f"unknown or incomplete source {source_id}")
    described = next(
        (row for row in list_tables(workspace, source_id) if row["name"] == table), None
    )
    if described is None:
        raise ValueError(f"source {source_id} has no table {table}")
    missing = sorted(set(_MEMBERSHIP_COLUMNS) - set(cast("list[str]", described["columns"])))
    if missing:
        raise ValueError(f"{INDEX_MAPPER} source {source_id} lacks columns {missing}")
    if source["store"] != "market" or described["format"] == "sqlite":
        raise ValueError(f"{INDEX_MAPPER} reads Arrow source tables in the market store")
    pin = SourcePin(source_id, str(source["sha256"]), table, str(described["digest"]))
    try:
        resolved = resolve_source(workspace, pin)
    except ImportError:
        raise ValueError(
            "reading an Arrow source table needs pyarrow; install the legacy extra"
        ) from None
    return str(resolved["target"])


def _canonical_text(value: object) -> str | None:
    if not isinstance(value, str) or not value or value.strip() != value:
        return None
    return value if value.isprintable() else None


_DAYS_SQL: Final = """
SELECT DISTINCT indexname, try_strptime("date", '%Y-%m-%d')::DATE AS day FROM {table}
WHERE try_strptime("date", '%Y-%m-%d') IS NOT NULL
"""


def index_pairs(  # noqa: C901 -- one pass classifies every pair beside its refusal
    connection: duckdb.DuckDBPyConnection,
    target: str,
    source: str,
    report: MapperReport,
    days: dict[str, set[date]] | None = None,
) -> list[Pair]:
    """``norgate.index_membership@1`` over one staged table: its accepted pairs.

    Every pair counts one mapper row; refusals are counted by reason in ``report``.
    ``days`` collects every date each index's rows carry, for the daily member report.
    """
    from aegis_alpha.storage.source_library_schema import quoted  # noqa: PLC0415

    query = _INDEX_SQL.format(table=quoted(target))
    if days is not None:
        for index, day in connection.execute(_DAYS_SQL.format(table=quoted(target))).fetchall():
            days.setdefault(str(index), set()).add(cast("date", day))
    pairs: list[Pair] = []
    runs: dict[tuple[object, object], list[Interval]] = defaultdict(list)
    heads: list[tuple[object, ...]] = []
    for row in connection.execute(query).fetchall():
        assetid, index, kind, *_rest = row
        if kind == "run":
            runs[assetid, index].append((row[7], row[8]))
        else:
            heads.append(tuple(row))
    for assetid, index, _, rows, bad_value, bad_date, unordered, first, last in heads:
        report.rows += 1
        name = _canonical_text(index)
        if type(assetid) is not int or assetid <= 0:
            report.refuse("assetid_invalid")
        elif name is None:
            report.refuse("indexname_invalid")
        elif bad_value:
            report.refuse("constituent_invalid")
        elif bad_date:
            report.refuse("date_invalid")
        elif unordered:
            report.refuse("dates_not_increasing")
        else:
            report.accepted += 1
            pairs.append(
                Pair(
                    str(assetid),
                    name,
                    source,
                    cast("int", rows),
                    cast("date | None", first),
                    cast("date | None", last),
                    tuple(sorted(runs[assetid, index])),
                )
            )
    return pairs


# --- documents ------------------------------------------------------------------------------


def _registered_instruments(
    state: sqlite3.Connection, instrument_ids: Iterable[str]
) -> dict[str, Record]:
    wanted = sorted(set(instrument_ids))
    return {
        str(row["instrument_id"]): dict(row)
        for row in state.execute(
            "SELECT instrument_id,issuer_id,asset_type,venue FROM instruments "
            "WHERE instrument_id IN (SELECT value FROM json_each(?))",
            (canonical_json_bytes(wanted).decode(),),
        )
    }


def _sources(state: sqlite3.Connection, snapshot_ids: Iterable[str]) -> list[Record]:
    """Each source snapshot's header and complete file inventory, as a universe cites them."""
    sources = []
    for snapshot_id in sorted(set(snapshot_ids)):
        header = state.execute(
            "SELECT snapshot_id,provider,requested_at_us,retrieved_at_us,publication_at_us,status "
            "FROM source_snapshots WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        if header is None:
            raise ValueError(f"source {snapshot_id} has no source snapshot")
        files = [
            dict(row)
            for row in state.execute(
                "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=? "
                "ORDER BY relative_path",
                (snapshot_id,),
            )
        ]
        sources.append({**dict(header), "files": files})
    return sources


@dataclass(slots=True)
class Universe:
    """One whole universe document and what its build left out."""

    universe_id: str
    document: Record
    unresolved: list[str] = field(default_factory=list)
    days: tuple[date, ...] = ()
    detail: dict[str, object] = field(default_factory=dict)

    @property
    def members(self) -> list[Record]:
        return cast("list[Record]", self.document["members"])

    def daily_members(self) -> dict[str, object] | None:
        """How many members each source date holds, over the dates the sources carry."""
        if not self.days:
            return None
        starts: dict[int, int] = defaultdict(int)
        for member in self.members:
            starts[cast("int", member["valid_from_us"])] += 1
            end = member["valid_to_us"]
            if end is not None:
                starts[cast("int", end)] -= 1
        edges = sorted(starts.items())
        counts = []
        active = 0
        position = 0
        for day in self.days:
            moment = session_start_us(day)
            while position < len(edges) and edges[position][0] <= moment:
                active += edges[position][1]
                position += 1
            counts.append(active)
        return {
            "days": len(counts),
            "first": self.days[0].isoformat(),
            "last": self.days[-1].isoformat(),
            "min": min(counts),
            "median": statistics.median(counts),
            "max": max(counts),
        }

    def report(self, *, sample: int | None = _SAMPLE) -> dict[str, object]:
        unresolved = self.unresolved if sample is None else self.unresolved[:sample]
        return {
            "universe_id": self.universe_id,
            "members": len(self.members),
            "instruments": len(cast("list[object]", self.document["instruments"])),
            "sources": len(cast("list[object]", self.document["sources"])),
            "unresolved_count": len(self.unresolved),
            "unresolved": unresolved,
            "daily_members": self.daily_members(),
            **self.detail,
        }


def _document(
    state: sqlite3.Connection,
    universe_id: str,
    version: str,
    candidates: Sequence[tuple[str, int, int | None, int, str]],
) -> tuple[Record, list[str]]:
    """The universe of ``(assetid, valid_from, valid_to, known_from, source)`` candidates.

    Asset IDs the registry holds no instrument for are left out and returned.
    """
    minted = {assetid: mint_instrument("norgate_assetid", assetid) for assetid, *_ in candidates}
    registered = _registered_instruments(state, minted.values())
    unresolved = sorted(
        {assetid for assetid, instrument in minted.items() if instrument not in registered},
        key=int,
    )
    members = [
        {
            "instrument_id": minted[assetid],
            "valid_from_us": valid_from,
            "valid_to_us": valid_to,
            "known_from_us": known_from,
            "known_to_us": None,
            "source_snapshot_id": source,
        }
        for assetid, valid_from, valid_to, known_from, source in candidates
        if minted[assetid] in registered
    ]
    members.sort(
        key=lambda row: (
            cast("str", row["instrument_id"]),
            cast("int", row["valid_from_us"]),
            cast("int", row["known_from_us"]),
        )
    )
    used = {cast("str", row["instrument_id"]) for row in members}
    document: Record = {
        "schema": _UNIVERSE_SCHEMA,
        "hash_format": _HASH_FORMAT,
        "universe_id": universe_id,
        "version": version,
        "instruments": [registered[key] for key in sorted(used)],
        "members": members,
        "sources": _sources(state, {cast("str", row["source_snapshot_id"]) for row in members}),
    }
    return document, unresolved


@dataclass(slots=True)
class UniverseBuild:
    """The universes one mapper built, with its mapper report."""

    mapper: str
    universes: list[Universe]
    report: MapperReport
    detail: dict[str, object] = field(default_factory=dict)

    def summary(self, *, sample: int | None = _SAMPLE) -> dict[str, object]:
        return {
            "mapper": self.mapper,
            "rows": self.report.json(),
            **self.detail,
            "universes": [universe.report(sample=sample) for universe in self.universes],
        }


def build_index_universes(
    workspace: Workspace, sources: Sequence[str], *, version: str, indexes: Sequence[str] = ()
) -> UniverseBuild:
    """``norgate.index_membership@1``: one universe per index over committed, linked sources.

    ``indexes`` limits the universes built to those index names; every pair is still read
    and checked, so a pair repeated across sources is refused whichever index it is in.
    """
    if not sources or len(set(sources)) != len(sources):
        raise ValueError("index universes need distinct membership source IDs")
    state = workspace.state
    report = MapperReport()
    pairs: list[Pair] = []
    known: dict[str, int] = {}
    index_days: dict[str, set[date]] = {}
    for source in sorted(sources):
        known[LINK_PREFIX + source] = link_instant(state, source)
        target = _pinned_table(workspace, source, MEMBERSHIP_TABLE)
        pairs.extend(
            index_pairs(workspace.market, target, LINK_PREFIX + source, report, index_days)
        )
    seen: dict[tuple[str, str], int] = defaultdict(int)
    for pair in pairs:
        seen[pair.assetid, pair.index] += 1
    repeated = {key for key, count in seen.items() if count > 1}
    for key in repeated:
        report.accepted -= seen[key]
        report.refused["pair_repeated"] = report.refused.get("pair_repeated", 0) + seen[key]
    by_index: dict[str, list[Pair]] = defaultdict(list)
    for pair in pairs:
        if (pair.assetid, pair.index) not in repeated:
            by_index[pair.index].append(pair)
    selected = sorted(by_index) if not indexes else sorted(set(indexes))
    missing = sorted(set(selected) - set(by_index))
    if missing:
        raise ValueError(f"no accepted membership pairs for index {missing}")
    universes = []
    for index in selected:
        members = by_index[index]
        candidates = [
            (pair.assetid, *interval_us(first, last), known[pair.source], pair.source)
            for pair in members
            for first, last in pair.intervals
        ]
        document, unresolved = _document(state, INDEX_UNIVERSE_PREFIX + index, version, candidates)
        days = tuple(sorted(index_days.get(index, ())))
        never = sum(1 for pair in members if not pair.intervals)
        firsts = [pair.first for pair in members if pair.first is not None]
        lasts = [pair.last for pair in members if pair.last is not None]
        universes.append(
            Universe(
                INDEX_UNIVERSE_PREFIX + index,
                document,
                unresolved,
                days,
                {
                    "index": index,
                    "pairs": len(members),
                    "rows": sum(pair.rows for pair in members),
                    "never_member": never,
                    "first": min(firsts).isoformat() if firsts else None,
                    "last": max(lasts).isoformat() if lasts else None,
                },
            )
        )
    return UniverseBuild(INDEX_MAPPER, universes, report, {"version": version})


# --- norgate.listings@1 ---------------------------------------------------------------------


def _listing_span(listing: Listing, through: date | None) -> Interval | str:
    """A listing's first and last member date, or the reason it has none."""
    last = listing.last_date
    if last is None and listing.listed:
        last = through
    if listing.first_date is None:
        return "listing_start_unknown"
    if last is None:
        return "listing_end_unknown"
    if last < listing.first_date:
        return "listing_dates_reversed"
    return listing.first_date, last


def build_listing_universe(workspace: Workspace, master: str, *, version: str) -> UniverseBuild:
    """``norgate.listings@1``: every master listing from its first to its last observed date.

    A listed row without ``last_date`` is still listed when the master was exported, so it
    holds through the master's last observed session (``through``), the latest
    ``first_date``/``last_date`` any accepted row shows.
    """
    state = workspace.state
    rows = LinkedRows(read_rows(workspace, master, MASTER_TABLE), link_instant(state, master))
    listings, report = map_norgate_master(rows)
    counts: dict[str, int] = defaultdict(int)
    for listing in listings:
        counts[listing.assetid] += 1
    days = [day for row in listings for day in (row.first_date, row.last_date) if day]
    through = max(days, default=None)
    candidates = []
    for listing in listings:
        span = (
            "assetid_repeated" if counts[listing.assetid] > 1 else _listing_span(listing, through)
        )
        if isinstance(span, str):
            report.accepted -= 1
            report.refuse(span)
            continue
        candidates.append(
            (listing.assetid, *interval_us(*span), rows.linked_at_us, rows.rows.snapshot_id)
        )
    document, unresolved = _document(state, LISTING_UNIVERSE, version, candidates)
    detail: dict[str, object] = {
        "listed": sum(1 for listing in listings if listing.listed),
        "delisted": sum(1 for listing in listings if not listing.listed),
        "through": None if through is None else through.isoformat(),
    }
    universe = Universe(LISTING_UNIVERSE, document, unresolved, (), detail)
    return UniverseBuild(LISTING_MAPPER, [universe], report, {"version": version})


# --- registration ---------------------------------------------------------------------------


def register_universes(
    state: sqlite3.Connection, build: UniverseBuild, *, apply: bool
) -> dict[str, object]:
    """Plan, or register, each built universe as a chunked manifest; report pins and parts."""
    registered = []
    for universe in build.universes:
        plan = plan_membership_manifest(universe.document, identity=False)
        pin = plan.pin
        if apply:
            pin = register_universe_manifest(state, universe.document)
            if pin != plan.pin:
                raise ValueError("universe changed while it was being registered")
        registered.append(
            {
                "universe_id": universe.universe_id,
                "version": cast("UniversePin", pin).version,
                "content_hash": pin.content_hash,
                "parts": len(plan.part_pins),
            }
        )
    return {"mode": "apply" if apply else "plan", "pins": registered}


def show_universe(state: sqlite3.Connection, universe_id: str, version: str) -> dict[str, object]:
    """SELECT only: a universe header and, for a manifest, its parts and member counts."""
    from aegis_alpha.storage.membership_pins import membership_parts  # noqa: PLC0415

    header = state.execute(
        "SELECT content_hash FROM universe_versions WHERE universe_id=? AND version=?",
        (universe_id, version),
    ).fetchone()
    if header is None:
        raise ValueError("unknown universe version")
    pin = UniversePin(universe_id, version, str(header[0]))
    parts = cast("tuple[UniversePin, ...]", membership_parts(state, pin))
    counted = [
        {
            "version": part.version,
            "content_hash": part.content_hash,
            "members": state.execute(
                "SELECT count(*) FROM universe_members WHERE universe_id=? AND version=?",
                (universe_id, part.version),
            ).fetchone()[0],
        }
        for part in parts or (pin,)
    ]
    return {
        "universe_id": universe_id,
        "version": version,
        "content_hash": pin.content_hash,
        "kind": "manifest" if parts else "document",
        "members": sum(cast("int", part["members"]) for part in counted),
        "parts": counted if parts else [],
    }
