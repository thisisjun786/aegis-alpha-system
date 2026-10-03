"""US identity registry: Norgate master, SEC submissions, FMP profiles and EODHD US symbols.

Four identity mappers turn retained source rows into one ``aas-identity-registry-v1``
document; ``aas identity register`` then appends it like any other registry document.

- ``norgate.master@1`` reads the one Norgate security master. Every row mints the
  instrument ``mint_instrument('norgate_assetid', assetid)`` (venue ``XNYS``, the session
  calendar every US listing trades on) and asserts the asset ID and Norgate's own symbol
  (provider ``norgate``). A listed (not delisted) row's symbol, spelled with ``-`` for its
  class separator ``.``, is its US ticker; a ticker two listed rows share is ambiguous.
- ``eodhd.us_symbol@1`` asserts the EODHD symbol ``<ticker>.US`` (namespace
  ``eodhd_symbol``, the token ``eodhd.bars@1`` resolves) of each unambiguous listed
  ticker. Its evidence is the Norgate row.
- ``fmp.profile@1`` reads FMP company profiles. A symbol whose rows agree with each other
  and with the Norgate listing's currency and ETF type asserts ``fmp_symbol``, and the
  ``cusip`` and ``isin`` it names from the instant they were retrieved.
- ``sec.tickers@1`` reads the ``CIK##########.json`` members of a retained SEC
  submissions archive through its member index. A ticker listed under exactly one CIK
  whose FMP profile names the same CIK links that listing to the issuer
  ``mint_issuer('sec_cik', cik)`` by a ``sec`` issuer assertion.

Nothing is minted from a ticker, and a cross-provider ticker match needs every provider to
agree: a ticker two listings share, two CIKs list or FMP rows disagree on stays unresolved
with its reason (data-vertical risk 7). Delisted listings carry Norgate's claims only.

A ticker names a listing only over what the master shows: its claims (Norgate's own
listed symbol, the EODHD and FMP symbols) are valid from the New York start of the
listing's ``first_date``, or of the day after the last ``last_date`` of a delisted row
whose ``<ticker>-YYYYMM`` symbol shows an earlier holder, whichever is later, until the
New York start of the day after the master's last observed session (its ``through``
date). Extending a claim past ``through`` needs newer evidence registered as a later
interval. An FMP profile or SEC filer matched by ticker names the listing only when it was
retrieved inside that interval (``fmp_after_master_through``, ``sec_after_master_through``
and the ``before_ticker_claim`` pair otherwise).

Every claim cites its source as ``sl:<source_id>`` with the ``aas-source-row-v1`` hash of
the row it came from. A Norgate or SEC claim is known from its source's ``sl:`` link
instant (the source rows record no retrieval instant), an FMP claim from the instant its
row was retrieved, and an issuer link from the latest of the three instants it rests on.
An issuer link is valid from the later of its SEC and FMP instants: both state today's
ticker-to-CIK mapping, not since when, so an earlier CIK can be added as its own link.
The build input is cumulative (``check_registered``). The contract is in
dev-notes/design/data-vertical.md.
"""

from __future__ import annotations

import hashlib
import json
import re
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, BinaryIO, Final, cast
from zoneinfo import ZoneInfo

from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage.identity import (
    REGISTRY_SCHEMA,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    parse_registry,
)
from aegis_alpha.storage.kr_identity import UNBOUNDED, Evidence, MapperReport, SourceRows
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.source_identity import LINK_PREFIX, SourceFile

if TYPE_CHECKING:
    import sqlite3

    import duckdb

    from aegis_alpha.storage.workspace import Workspace

MASTER_MAPPER: Final = "norgate.master@1"
EODHD_MAPPER: Final = "eodhd.us_symbol@1"
FMP_MAPPER: Final = "fmp.profile@1"
SEC_MAPPER: Final = "sec.tickers@1"
MASTER_TABLE: Final = "observations"
FMP_TABLE: Final = "observations"
SEC_TABLE: Final = "members"
BINDINGS_TABLE: Final = "observations"
SEC_PREFIX: Final = "sec-submissions-zip-"
VENUE: Final = "XNYS"
ASSETID_SET_FORMAT: Final = "aas-norgate-assetids-v1"
NEW_YORK: Final = ZoneInfo("America/New_York")
_DELISTED_SYMBOL: Final = re.compile(r"(.+)-[0-9]{6}")
_DAY: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_SEC_MEMBER: Final = re.compile(r"CIK([0-9]{10})\.json")
_MAX_MEMBER_BYTES: Final = 64 * 1024 * 1024
_CHUNK: Final = 1024 * 1024
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)

type Record = dict[str, object]


def _text(value: object) -> str | None:
    """Trimmed printable text, or None for anything a claim cannot stand on."""
    if not isinstance(value, str) or not value or value.strip() != value:
        return None
    return value if value.isprintable() else None


def _identifier(kind: IdentifierType, value: object) -> str | None:
    """``value`` when it is already the canonical spelling of ``kind``, otherwise None."""
    if not isinstance(value, str):
        return None
    try:
        canonical = normalize_identifier(kind, value)
    except IdentifierValueError:
        return None
    return canonical if canonical == value else None


def us_ticker(symbol: str) -> str:
    """A US ticker as SEC, FMP and EODHD spell it: the class separator ``.`` becomes ``-``."""
    return symbol.replace(".", "-")


def _instant_us(value: object) -> int | None:
    """UTC microseconds of a time-zone-aware timestamp, or None for anything else."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    return (value - _EPOCH) // _MICROSECOND


def _day(value: object) -> date | None:
    """A master date column (``YYYY-MM-DD`` text or a date), or None."""
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and _DAY.fullmatch(value):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def session_start_us(day: date) -> int:
    """UTC microseconds of ``day``'s start in New York, the instant ``eodhd.bars@1`` resolves."""
    moment = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
    return (moment - _EPOCH) // _MICROSECOND


# --- pinned source rows -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LinkedRows:
    """A pinned source table and the ``retrieved_at_us`` of its ``sl:`` link."""

    rows: SourceRows
    linked_at_us: int

    def evidence(self, row_hash: str) -> Evidence:
        return Evidence(self.rows.snapshot_id, row_hash, self.linked_at_us)


def read_rows(
    workspace: Workspace,
    source_id: str,
    table: str,
    keep: Callable[[Mapping[str, object]], bool] | None = None,
) -> SourceRows:
    """Verify a committed source table against its marker and read its rows through Arrow.

    Arrow keeps a timestamp column's instant without a time zone database, so tables
    with ``TIMESTAMP WITH TIME ZONE`` columns read like any other.
    """
    from aegis_alpha.storage.source_library import list_sources, list_tables  # noqa: PLC0415
    from aegis_alpha.storage.source_library_schema import connections, quoted  # noqa: PLC0415
    from aegis_alpha.storage.source_reader import SourcePin, resolve_source  # noqa: PLC0415

    source = next((row for row in list_sources(workspace) if row["source_id"] == source_id), None)
    if source is None:
        raise ValueError(f"unknown or incomplete source {source_id}")
    described = next(
        (row for row in list_tables(workspace, source_id) if row["name"] == table), None
    )
    if described is None:
        raise ValueError(f"source {source_id} has no table {table}")
    pin = SourcePin(source_id, str(source["sha256"]), table, str(described["digest"]))
    try:
        resolved = resolve_source(workspace, pin)
    except ImportError:
        raise ValueError(
            "reading an Arrow source table needs pyarrow; install the legacy extra"
        ) from None
    columns = tuple(cast("list[str]", resolved["columns"]))
    query = (
        "SELECT "  # noqa: S608 -- manifest-owned names, quoted
        + ",".join(quoted(column) for column in columns)
        + " FROM "
        + quoted(str(resolved["target"]))
        + " ORDER BY _aas_ordinal"
    )
    connection = connections(workspace)[str(resolved["store"])]
    if resolved["format"] == "sqlite":
        items: Iterator[Mapping[str, object]] = (
            dict(zip(columns, row, strict=True))
            for row in cast("sqlite3.Connection", connection).execute(query)
        )
    else:
        reader = cast("duckdb.DuckDBPyConnection", connection).execute(query).to_arrow_reader(65536)
        items = (item for batch in reader for item in batch.to_pylist())
    rows = [tuple(item[name] for name in columns) for item in items if keep is None or keep(item)]
    return SourceRows(LINK_PREFIX + source_id, columns, tuple(rows))


def link_instant(state: sqlite3.Connection, source_id: str) -> int:
    """The ``retrieved_at_us`` of a source's ``sl:`` link; an unlinked source is refused."""
    row = state.execute(
        "SELECT retrieved_at_us FROM source_snapshots WHERE snapshot_id=?",
        (LINK_PREFIX + source_id,),
    ).fetchone()
    if row is None or row[0] is None:
        raise ValueError(
            f"source {source_id} has no sl: link; run aas db source-link --apply first"
        )
    return int(row[0])


def _require(rows: SourceRows, columns: Sequence[str], mapper: str) -> None:
    missing = sorted(set(columns) - set(rows.columns))
    if missing:
        raise ValueError(f"{mapper} source {rows.snapshot_id} lacks columns {missing}")


# --- norgate.master@1 ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Listing:
    """One Norgate master row: an instrument and what Norgate says about it."""

    assetid: str
    symbol: str
    listed: bool
    asset_type: str
    evidence: Evidence
    first_date: date | None = None
    last_date: date | None = None

    @property
    def instrument_id(self) -> str:
        return mint_instrument("norgate_assetid", self.assetid)


_MASTER_COLUMNS: Final = (
    "assetid",
    "symbol",
    "is_delisted",
    "currency",
    "is_etf",
    "first_date",
    "last_date",
)


def map_norgate_master(source: LinkedRows) -> tuple[list[Listing], MapperReport]:
    """``norgate.master@1``: every master row with a canonical asset ID is an instrument.

    ``is_etf`` true is ``etf``; everything else is ``unclassified`` and the share or fund
    class is left to the classifications dataset. A row that is not USD, whose symbol is
    not trimmed printable text or whose listing state is unknown is refused, not repaired.
    ``first_date`` and ``last_date`` are kept when they are dates; they bound ticker claims.
    """
    _require(source.rows, _MASTER_COLUMNS, MASTER_MAPPER)
    report = MapperReport()
    listings: list[Listing] = []
    for row, row_hash in source.rows.records():
        report.rows += 1
        raw_id = row["assetid"]
        assetid = _identifier(
            IdentifierType.NORGATE_ASSETID,
            str(raw_id) if type(raw_id) is int and raw_id > 0 else None,
        )
        symbol = _text(row["symbol"])
        if assetid is None:
            report.refuse("assetid_invalid")
        elif symbol is None:
            report.refuse("symbol_invalid")
        elif row["currency"] != "USD":
            report.refuse("currency_not_usd")
        elif type(row["is_delisted"]) is not bool:
            report.refuse("listing_state_unknown")
        else:
            report.accepted += 1
            listings.append(
                Listing(
                    assetid,
                    symbol,
                    not row["is_delisted"],
                    "etf" if row["is_etf"] is True else "unclassified",
                    source.evidence(row_hash),
                    _day(row["first_date"]),
                    _day(row["last_date"]),
                )
            )
    return listings, report


# --- fmp.profile@1 ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Profile:
    """What one FMP profile row states about its symbol."""

    symbol: str
    cik: str | None
    cusip: str | None
    isin: str | None
    etf: bool
    currency: object
    evidence: Evidence

    def statement(self) -> tuple[object, ...]:
        return (self.cik, self.cusip, self.isin, self.etf, self.currency)


_FMP_COLUMNS: Final = ("symbol", "cik", "cusip", "isin", "isEtf", "currency", "retrieved_at_utc")


def _optional(kind: IdentifierType, value: object) -> tuple[str | None, bool]:
    """A blank identifier is absent; a present one must already be canonical."""
    if value is None or value == "":
        return None, True
    canonical = _identifier(kind, value)
    return canonical, canonical is not None


def map_fmp_profiles(
    sources: Sequence[SourceRows],
) -> tuple[dict[str, list[Profile]], dict[str, str], MapperReport]:
    """``fmp.profile@1``: profile rows grouped by symbol, and the symbols that cannot be used.

    A row whose CIK, CUSIP or ISIN is present but not canonical (check digits included),
    or whose ETF flag is not a boolean, makes its symbol ``fmp_identifier_invalid``; a row
    whose ``retrieved_at_utc`` is not a time-zone-aware timestamp makes it
    ``fmp_retrieved_invalid``.
    """
    report = MapperReport()
    profiles: dict[str, list[Profile]] = defaultdict(list)
    refused: dict[str, str] = {}
    for source in sources:
        _require(source, _FMP_COLUMNS, FMP_MAPPER)
        for row, row_hash in source.records():
            report.rows += 1
            symbol = _text(row["symbol"])
            if symbol is None:
                report.refuse("symbol_invalid")
                continue
            cik, cik_ok = _optional(IdentifierType.CIK, row["cik"])
            cusip, cusip_ok = _optional(IdentifierType.CUSIP, row["cusip"])
            isin, isin_ok = _optional(IdentifierType.ISIN, row["isin"])
            if not (cik_ok and cusip_ok and isin_ok) or type(row["isEtf"]) is not bool:
                report.refuse("fmp_identifier_invalid")
                refused[symbol] = "fmp_identifier_invalid"
                continue
            retrieved = _instant_us(row["retrieved_at_utc"])
            if retrieved is None:
                report.refuse("fmp_retrieved_invalid")
                refused[symbol] = "fmp_retrieved_invalid"
                continue
            report.accepted += 1
            evidence = Evidence(source.snapshot_id, row_hash, retrieved)
            profiles[symbol].append(
                Profile(symbol, cik, cusip, isin, bool(row["isEtf"]), row["currency"], evidence)
            )
    return profiles, refused, report


# --- sec.tickers@1 ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Filer:
    """One SEC submissions member: a CIK, its name and the tickers it lists."""

    cik: str
    name: str
    tickers: tuple[str, ...]
    evidence: Evidence


type ArchiveOpener = Callable[[], AbstractContextManager[BinaryIO]]


def _filer(raw: bytes, cik: str) -> tuple[str, tuple[str, ...]] | str:
    """The name and tickers of one ``CIK##########.json`` document, or why it is refused."""
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "member_not_json"
    if not isinstance(body, dict):
        return "member_not_json"
    stated = body.get("cik")
    if not isinstance(stated, str) or not stated.isdigit() or stated.zfill(10) != cik:
        return "cik_differs"
    tickers = body.get("tickers")
    if not isinstance(tickers, list) or not all(_text(item) for item in tickers):
        return "tickers_invalid"
    name = _text(body.get("name"))
    if tickers and name is None:
        return "name_invalid"
    return name or "", tuple(cast("list[str]", tickers))


def _check_archive(handle: BinaryIO, archive: SourceFile) -> zipfile.ZipFile:
    """Open the archive after checking it has the size and SHA-256 its source records."""
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: handle.read(_CHUNK), b""):
        digest.update(chunk)
        size += len(chunk)
    if (digest.hexdigest(), size) != (archive.sha256, archive.size_bytes):
        raise ValueError("SEC archive bytes differ from the content source that names them")
    handle.seek(0)
    try:
        return zipfile.ZipFile(handle)
    except zipfile.BadZipFile:
        raise ValueError("SEC submissions source is not a zip archive") from None


def _member(bundle: zipfile.ZipFile, row: Mapping[str, object]) -> bytes:
    """One member's bytes, which must have the size and SHA-256 its index row records."""
    name, expected = str(row["member"]), row["size"]
    if type(expected) is not int or expected > _MAX_MEMBER_BYTES:
        raise ValueError(f"SEC member {name} exceeds its bound")
    try:
        raw = bundle.read(name)
    except KeyError:
        raise ValueError(f"SEC archive lacks indexed member {name}") from None
    if len(raw) != expected or hashlib.sha256(raw).hexdigest() != row["sha256"]:
        raise ValueError(f"SEC member {name} differs from its index row")
    return raw


def map_sec_tickers(
    members: LinkedRows, archive: SourceFile, opener: ArchiveOpener
) -> tuple[list[Filer], MapperReport]:
    """``sec.tickers@1``: the CIK documents of one archive, read through its member index.

    The archive must have the size and SHA-256 its content source records, and each member
    the size and SHA-256 its index row records. Members other than ``CIK##########.json``
    (paginated filing histories, placeholders) are skipped. A CIK that lists no ticker
    cannot link a listing and is skipped.
    """
    _require(members.rows, ("member", "size", "sha256"), SEC_MAPPER)
    report = MapperReport()
    filers: list[Filer] = []
    with opener() as handle, _check_archive(handle, archive) as bundle:
        for row, row_hash in members.rows.records():
            report.rows += 1
            match = _SEC_MEMBER.fullmatch(str(row["member"]))
            if match is None:
                report.skip("not_a_cik_document")
                continue
            stated = _filer(_member(bundle, row), match[1])
            if isinstance(stated, str):
                report.refuse(stated)
            elif not stated[1]:
                report.skip("no_ticker")
            else:
                report.accepted += 1
                filers.append(Filer(match[1], *stated, members.evidence(row_hash)))
    return filers, report


# --- registry -----------------------------------------------------------------------------


def _anchor(namespace: str, token: str) -> Record:
    return {"anchor_namespace": namespace, "anchor_token": token}


type Interval = tuple[int, int | None]
_ALWAYS: Final[Interval] = (UNBOUNDED, None)


def _assertion(
    assetid: str, key: tuple[str, str, str], valid: Interval, evidence: Evidence
) -> Record:
    provider, namespace, token = key
    return {
        "instrument": _anchor("norgate_assetid", assetid),
        "provider": provider,
        "namespace": namespace,
        "token": token,
        "valid_from_us": valid[0],
        "valid_to_us": valid[1],
        "known_from_us": evidence.known_from_us,
        "supersedes_assertion_id": None,
        "source_snapshot_id": evidence.source,
        "source_hash": evidence.source_hash,
    }


def assetid_set_sha256(assetids: Sequence[str] | set[str]) -> str:
    """SHA-256 of ``["aas-norgate-assetids-v1", ascending asset IDs]`` as canonical JSON."""
    ordered = sorted({int(value) for value in assetids})
    return hashlib.sha256(formats.canonical([ASSETID_SET_FORMAT, ordered])).hexdigest()


@dataclass(slots=True)
class UsRegistry:
    """A built registry document and why every unresolved key stayed unresolved."""

    document: dict[str, object]
    mappers: dict[str, MapperReport]
    symbols: dict[str, str]
    """Each EODHD US symbol of a listed ticker: its instrument ID or ``unresolved:<reason>``."""
    unresolved: dict[str, dict[str, list[str]]]
    sources: frozenset[str] = frozenset()
    """The ``sl:`` IDs of every source the build read."""
    withdrawn: list[dict[str, object]] = field(default_factory=list)
    """Registered US assertions, not yet corrected, that these sources no longer give."""
    bindings: dict[str, object] | None = None
    """How the minted asset IDs compare with a legacy identity-bindings table."""
    intervals: dict[str, tuple[int, int]] = field(default_factory=dict)
    """The valid interval of each resolved EODHD US symbol's claim."""
    through: date | None = None
    """The master's last observed session; ticker claims end the day after it."""

    def raw(self) -> bytes:
        return formats.canonical(self.document)

    def sha256(self) -> str:
        return hashlib.sha256(self.raw()).hexdigest()

    def assetids(self) -> list[str]:
        instruments = cast("list[dict[str, object]]", self.document["instruments"])
        return [str(row["anchor_token"]) for row in instruments]

    def resolve(
        self, bars: Iterable[tuple[str, date, int]], *, sample: int | None = None
    ) -> dict[str, object]:
        """Classify EODHD US bar keys ``(symbol, session date, rows)`` by this registry.

        A key resolves when its symbol's claim is valid at the session's New York start,
        the instant ``eodhd.bars@1`` resolves; rows and symbols are counted by reason.
        """
        rows: dict[str, int] = defaultdict(int)
        symbols: dict[str, set[str]] = defaultdict(set)
        for symbol, day, count in bars:
            status = self.symbols.get(symbol, "unresolved:not_a_listed_norgate_ticker")
            if status.startswith("ins-"):
                start, end = self.intervals[symbol]
                at = session_start_us(day)
                reason = (
                    "resolved"
                    if start <= at < end
                    else "before_ticker_claim"
                    if at < start
                    else "after_master_through"
                )
            else:
                reason = status.removeprefix("unresolved:")
            rows[reason] += count
            symbols[reason].add(symbol)
        total = sum(rows.values())
        resolved = rows.pop("resolved", 0)
        resolved_symbols = symbols.pop("resolved", set())
        return {
            "rows": total,
            "resolved_rows": resolved,
            "resolved_ratio": None if not total else round(resolved / total, 6),
            "symbols": len(set().union(resolved_symbols, *symbols.values())),
            "resolved_symbols": len(resolved_symbols),
            "unresolved_rows": dict(sorted(rows.items())),
            "unresolved_symbols": {reason: len(found) for reason, found in sorted(symbols.items())},
            "unresolved": {
                reason: sorted(found) if sample is None else sorted(found)[:sample]
                for reason, found in sorted(symbols.items())
            },
        }

    def report(self, *, sample: int | None = 100) -> dict[str, object]:
        def cut(values: list[str]) -> list[str]:
            return values if sample is None else values[:sample]

        assertions = cast("list[dict[str, object]]", self.document["assertions"])
        instruments = cast("list[dict[str, object]]", self.document["instruments"])
        kinds: dict[str, int] = defaultdict(int)
        for row in assertions:
            kinds[f"{row['provider']}/{row['namespace']}"] += 1
        return {
            "sha256": self.sha256(),
            "issuers": len(cast("list[object]", self.document["issuers"])),
            "instruments": len(instruments),
            "instruments_with_issuer": sum(row["issuer"] is not None for row in instruments),
            "assetids_sha256": assetid_set_sha256(self.assetids()),
            "through": None if self.through is None else self.through.isoformat(),
            "assertions": dict(sorted(kinds.items())),
            "mappers": {name: report.json() for name, report in sorted(self.mappers.items())},
            "unresolved_count": {
                key: {reason: len(values) for reason, values in sorted(groups.items())}
                for key, groups in sorted(self.unresolved.items())
            },
            "unresolved": {
                key: {reason: cut(values) for reason, values in sorted(groups.items())}
                for key, groups in sorted(self.unresolved.items())
            },
            "bindings": self.bindings,
            "withdrawn_count": len(self.withdrawn),
            "withdrawn": self.withdrawn if sample is None else self.withdrawn[:sample],
        }


def _tickers(listings: Sequence[Listing], unresolved: dict[str, list[str]]) -> dict[str, Listing]:
    """Listed rows by US ticker; a ticker two listed rows spell alike is ambiguous."""
    spelled: dict[str, list[Listing]] = defaultdict(list)
    for listing in listings:
        if listing.listed:
            spelled[us_ticker(listing.symbol)].append(listing)
    tickers: dict[str, Listing] = {}
    for ticker, group in sorted(spelled.items()):
        if len(group) > 1:
            unresolved["ticker_ambiguous"].append(ticker)
        else:
            tickers[ticker] = group[0]
    return tickers


@dataclass(frozen=True, slots=True)
class _Bounds:
    """What the master shows about when each listed row's ticker named it."""

    through: date | None
    intervals: dict[str, tuple[int, int]]
    """Listed asset ID -> valid interval of its ticker claims."""
    reasons: dict[str, str]
    """Listed asset ID -> why its ticker claims have no interval."""


def _bounds(accepted: Sequence[Listing]) -> _Bounds:
    """Bound each listed row's ticker claims by the master's own dates.

    A claim starts at the listing's ``first_date``, or the day after the latest
    ``last_date`` of a delisted row whose ``<ticker>-YYYYMM`` symbol shows an earlier
    holder, whichever is later, and ends the day after the master's last observed session.
    """
    days = [day for row in accepted for day in (row.first_date, row.last_date) if day]
    through = max(days, default=None)
    held: dict[str, date] = {}
    unbounded: set[str] = set()
    for listing in accepted:
        match = None if listing.listed else _DELISTED_SYMBOL.fullmatch(listing.symbol)
        if match is None:
            continue
        base = us_ticker(match[1])
        if listing.last_date is None:
            unbounded.add(base)
        else:
            held[base] = max(listing.last_date, held.get(base, listing.last_date))
    intervals: dict[str, tuple[int, int]] = {}
    reasons: dict[str, str] = {}
    for listing in accepted:
        if not listing.listed:
            continue
        ticker = us_ticker(listing.symbol)
        if through is None or listing.first_date is None:
            reasons[listing.assetid] = "listing_start_unknown"
        elif ticker in unbounded:
            reasons[listing.assetid] = "ticker_reuse_unbounded"
        else:
            start = listing.first_date
            if ticker in held:
                start = max(start, held[ticker] + timedelta(days=1))
            if start > through:
                reasons[listing.assetid] = "ticker_reused"
            else:
                end = session_start_us(through + timedelta(days=1))
                intervals[listing.assetid] = (session_start_us(start), end)
    return _Bounds(through, intervals, reasons)


def _bounded(
    tickers: Mapping[str, Listing], bounds: _Bounds, unresolved: dict[str, list[str]]
) -> dict[str, Listing]:
    """The unambiguous tickers whose claims have an interval; the rest stay unresolved."""
    kept: dict[str, Listing] = {}
    for ticker, listing in tickers.items():
        reason = bounds.reasons.get(listing.assetid)
        if reason is None:
            kept[ticker] = listing
        else:
            unresolved[reason].append(ticker)
    return kept


def _outside(at: int, interval: tuple[int, int]) -> str | None:
    """Why a ticker-matched row retrieved at ``at`` cannot name the claim's listing."""
    start, end = interval
    if at < start:
        return "before_ticker_claim"
    if at >= end:
        return "after_master_through"
    return None


def _profiles(
    profiles: Mapping[str, list[Profile]],
    refused: Mapping[str, str],
    tickers: Mapping[str, Listing],
    bounds: _Bounds,
    unresolved: dict[str, list[str]],
) -> dict[str, Profile]:
    """The one agreed profile of each listed ticker, judged against its Norgate listing.

    Only profiles retrieved while the listing's ticker claim holds are judged; a profile
    retrieved outside it may describe another holder of the ticker.
    """
    agreed: dict[str, Profile] = {}
    for symbol in sorted({*profiles, *refused}):
        listing = tickers.get(symbol)
        if listing is None:
            unresolved["not_a_listed_norgate_ticker"].append(symbol)
            continue
        group = profiles.get(symbol, [])
        interval = bounds.intervals[listing.assetid]
        inside = [row for row in group if _outside(row.evidence.known_from_us, interval) is None]
        if symbol in refused:
            reason = refused[symbol]
        elif not inside:
            earliest = min(group, key=lambda profile: profile.evidence.order())
            reason = f"fmp_{_outside(earliest.evidence.known_from_us, interval)}"
        elif len({profile.statement() for profile in inside}) > 1:
            reason = "fmp_profile_ambiguous"
        elif inside[0].currency != "USD":
            reason = "fmp_currency_not_usd"
        elif inside[0].etf != (listing.asset_type == "etf"):
            reason = "fmp_type_differs"
        else:
            agreed[symbol] = min(inside, key=lambda profile: profile.evidence.order())
            continue
        unresolved[reason].append(symbol)
    return agreed


def _shared(claims: Mapping[str, str], unresolved: dict[str, list[str]], reason: str) -> set[str]:
    """Identifier values (CUSIP, ISIN) two tickers claim: neither claim is kept."""
    owners: dict[str, set[str]] = defaultdict(set)
    for ticker, value in claims.items():
        owners[value].add(ticker)
    shared = {value for value, found in owners.items() if len(found) > 1}
    unresolved[reason].extend(sorted(shared))
    return shared


def _fmp_assertions(
    agreed: Mapping[str, Profile],
    tickers: Mapping[str, Listing],
    bounds: _Bounds,
    unresolved: dict[str, list[str]],
) -> list[Record]:
    cusips = {ticker: profile.cusip for ticker, profile in agreed.items() if profile.cusip}
    isins = {ticker: profile.isin for ticker, profile in agreed.items() if profile.isin}
    shared_cusips = _shared(cusips, unresolved, "cusip_ambiguous")
    shared_isins = _shared(isins, unresolved, "isin_ambiguous")
    assertions: list[Record] = []
    for ticker, profile in agreed.items():
        assetid, evidence = tickers[ticker].assetid, profile.evidence
        symbol = ("fmp", "fmp_symbol", ticker)
        assertions.append(_assertion(assetid, symbol, bounds.intervals[assetid], evidence))
        # A profile states the identifiers current when it was retrieved, not since when.
        retrieved: Interval = (evidence.known_from_us, None)
        if profile.cusip and profile.cusip not in shared_cusips:
            key = ("fmp", "cusip", profile.cusip)
            assertions.append(_assertion(assetid, key, retrieved, evidence))
        if profile.isin and profile.isin not in shared_isins:
            key = ("fmp", "isin", profile.isin)
            assertions.append(_assertion(assetid, key, retrieved, evidence))
    return assertions


def _inside(listed: Sequence[Filer], interval: tuple[int, int]) -> list[Filer] | str:
    """The SEC filers of a ticker retrieved inside its claim, or why there are none."""
    if not listed:
        return "sec_ticker_missing"
    found = [filer for filer in listed if _outside(filer.evidence.known_from_us, interval) is None]
    if found:
        return found
    earliest = min(listed, key=lambda item: item.evidence.order())
    return f"sec_{_outside(earliest.evidence.known_from_us, interval)}"


def _issuers(
    filers: Sequence[Filer],
    agreed: Mapping[str, Profile],
    tickers: Mapping[str, Listing],
    bounds: _Bounds,
    unresolved: dict[str, list[str]],
) -> tuple[list[Record], dict[str, tuple[str, Evidence, int]]]:
    """Issuer links of listed tickers that SEC and FMP tie to the same CIK.

    Only SEC filers retrieved while the listing's ticker claim holds count. Both sources
    state the current ticker-to-CIK mapping, not since when, so a link is valid from the
    later of the SEC and FMP instants and known from the latest of all three.
    """
    by_ticker: dict[str, list[Filer]] = defaultdict(list)
    for filer in filers:
        for ticker in set(filer.tickers):
            by_ticker[ticker].append(filer)
    names: dict[str, Filer] = {}
    links: dict[str, tuple[str, Evidence, int]] = {}
    for ticker, listing in sorted(tickers.items()):
        found = _inside(by_ticker.get(ticker, []), bounds.intervals[listing.assetid])
        if isinstance(found, str):
            unresolved[found].append(ticker)
            continue
        ciks = {filer.cik for filer in found}
        profile = agreed.get(ticker)
        if len(ciks) > 1:
            unresolved["sec_ticker_ambiguous"].append(ticker)
            continue
        filer = min(found, key=lambda item: item.evidence.order())
        if profile is None:
            unresolved["fmp_profile_missing"].append(ticker)
            continue
        if profile.cik is None:
            unresolved["fmp_cik_missing"].append(ticker)
            continue
        if profile.cik != filer.cik:
            unresolved["fmp_cik_differs"].append(ticker)
            continue
        valid = max(filer.evidence.known_from_us, profile.evidence.known_from_us)
        known = max(valid, listing.evidence.known_from_us)
        links[listing.assetid] = (filer.cik, replace(filer.evidence, known_from_us=known), valid)
        earliest = names.get(filer.cik)
        if earliest is None or filer.evidence.order() < earliest.evidence.order():
            names[filer.cik] = filer
    issuers = [
        {**_anchor("sec_cik", cik), "name": filer.name} for cik, filer in sorted(names.items())
    ]
    return issuers, links


def _accepted(
    listings: Sequence[Listing], unresolved: dict[str, list[str]]
) -> tuple[list[Listing], set[str]]:
    """Listings in asset ID order, and the symbols more than one of them carries.

    Two rows of one asset ID register neither. Norgate keeps its symbols unique, so two
    rows with one symbol keep only their asset IDs.
    """
    by_assetid: dict[str, list[Listing]] = defaultdict(list)
    for listing in listings:
        by_assetid[listing.assetid].append(listing)
    accepted: list[Listing] = []
    for assetid, group in sorted(by_assetid.items(), key=lambda item: int(item[0])):
        if len(group) > 1:
            unresolved["assetid_repeated"].append(assetid)
        else:
            accepted.append(group[0])
    rows: dict[str, int] = defaultdict(int)
    for listing in accepted:
        rows[listing.symbol] += 1
    repeated = {symbol for symbol, count in rows.items() if count > 1}
    unresolved["symbol_repeated"].extend(sorted(repeated))
    return accepted, repeated


def _listing_assertions(
    accepted: Sequence[Listing],
    repeated: set[str],
    links: Mapping[str, tuple[str, Evidence, int]],
    bounds: _Bounds,
) -> list[Record]:
    """Norgate's own claims and the issuer link of every accepted listing.

    A delisted row's suffixed symbol names it for good; a listed row's symbol is its
    ticker and is bounded like the ticker's other claims.
    """
    assertions: list[Record] = []
    for listing in accepted:
        claims: list[tuple[str, str, Interval]] = [("norgate_assetid", listing.assetid, _ALWAYS)]
        if listing.symbol not in repeated:
            valid = bounds.intervals.get(listing.assetid) if listing.listed else _ALWAYS
            if valid is not None:
                claims.append(("norgate_symbol", listing.symbol, valid))
        for namespace, token, interval in claims:
            key = ("norgate", namespace, token)
            assertions.append(_assertion(listing.assetid, key, interval, listing.evidence))
        if listing.assetid in links:
            cik, link, since = links[listing.assetid]
            token = issuer_link_token(mint_issuer("sec_cik", cik), listing.instrument_id)
            key = ("sec", "issuer", token)
            assertions.append(_assertion(listing.assetid, key, (since, None), link))
    return assertions


def _eodhd(
    tickers: Mapping[str, Listing], bounds: _Bounds, unresolved: Mapping[str, Sequence[str]]
) -> tuple[list[Record], dict[str, str], dict[str, tuple[int, int]], MapperReport]:
    """``eodhd.us_symbol@1``: ``<ticker>.US`` of every unambiguous, bounded listed ticker."""
    report = MapperReport()
    symbols = {
        f"{ticker}.US": f"unresolved:{reason}"
        for reason, found in unresolved.items()
        for ticker in found
    }
    intervals: dict[str, tuple[int, int]] = {}
    assertions: list[Record] = []
    for ticker, listing in tickers.items():
        report.rows += 1
        report.accepted += 1
        symbol = f"{ticker}.US"
        symbols[symbol] = listing.instrument_id
        intervals[symbol] = bounds.intervals[listing.assetid]
        key = ("eodhd", "eodhd_symbol", symbol)
        assertions.append(_assertion(listing.assetid, key, intervals[symbol], listing.evidence))
    return assertions, dict(sorted(symbols.items())), intervals, report


def _sec_filers(
    sec: Sequence[tuple[LinkedRows, SourceFile, ArchiveOpener]],
) -> tuple[list[Filer], MapperReport]:
    filers: list[Filer] = []
    report = MapperReport()
    for members, archive, opener in sec:
        found, part = map_sec_tickers(members, archive, opener)
        filers.extend(found)
        report.merge(part)
    return filers, report


def build_us_registry(
    master: LinkedRows,
    *,
    fmp: Sequence[SourceRows] = (),
    sec: Sequence[tuple[LinkedRows, SourceFile, ArchiveOpener]] = (),
) -> UsRegistry:
    """Build the US ``aas-identity-registry-v1`` document from pinned source rows.

    Norgate is a frozen source, so one master names every instrument. The document lists
    issuers by CIK, instruments by asset ID and assertions by provider, namespace and
    token, so the same sources always give the same bytes.
    """
    unresolved: dict[str, dict[str, list[str]]] = {
        key: defaultdict(list) for key in ("listings", "tickers", "fmp", "identifiers", "issuers")
    }
    mappers: dict[str, MapperReport] = {}
    listings, mappers[MASTER_MAPPER] = map_norgate_master(master)
    accepted, repeated = _accepted(listings, unresolved["listings"])
    bounds = _bounds(accepted)
    tickers = _bounded(
        _tickers(
            [listing for listing in accepted if listing.symbol not in repeated],
            unresolved["tickers"],
        ),
        bounds,
        unresolved["tickers"],
    )
    profiles, refused, mappers[FMP_MAPPER] = map_fmp_profiles(fmp)
    agreed = _profiles(profiles, refused, tickers, bounds, unresolved["fmp"])
    filers, mappers[SEC_MAPPER] = _sec_filers(sec)
    issuers, links = (
        _issuers(filers, agreed, tickers, bounds, unresolved["issuers"]) if sec else ([], {})
    )
    eodhd, symbols, intervals, mappers[EODHD_MAPPER] = _eodhd(
        tickers, bounds, unresolved["tickers"]
    )
    assertions = [
        *_listing_assertions(accepted, repeated, links, bounds),
        *eodhd,
        *_fmp_assertions(agreed, tickers, bounds, unresolved["identifiers"]),
    ]
    instruments = [
        {
            **_anchor("norgate_assetid", listing.assetid),
            "issuer": None
            if listing.assetid not in links
            else _anchor("sec_cik", links[listing.assetid][0]),
            "asset_type": listing.asset_type,
            "venue": VENUE,
        }
        for listing in accepted
    ]
    document: dict[str, object] = {
        "schema": REGISTRY_SCHEMA,
        "issuers": issuers,
        "instruments": instruments,
        "assertions": sorted(
            assertions,
            key=lambda row: (str(row["provider"]), str(row["namespace"]), str(row["token"])),
        ),
    }
    parse_registry(document)
    return UsRegistry(
        document,
        mappers,
        symbols,
        {
            key: {reason: sorted(values) for reason, values in groups.items() if values}
            for key, groups in unresolved.items()
        },
        frozenset(
            [master.rows.snapshot_id, *(rows.snapshot_id for rows in fmp)]
            + [members.rows.snapshot_id for members, _, _ in sec]
        ),
        intervals=intervals,
        through=bounds.through,
    )


def compare_bindings(registry: UsRegistry, bindings: SourceRows) -> dict[str, object]:
    """Compare the minted asset IDs with a legacy identity-bindings table.

    The legacy registry bound each Norgate asset ID (``provider_identifier`` of its
    ``norgate``/``norgate_assetid`` rows) to one placeholder instrument. Equal sets mean
    the registry carries every instrument the legacy one did and no other.
    """
    _require(bindings, ("provider", "namespace", "provider_identifier", "state"), "bindings")
    bound: set[str] = set()
    rows = 0
    for row, _ in bindings.records():
        rows += 1
        if (row["provider"], row["namespace"], row["state"]) == (
            "norgate",
            "norgate_assetid",
            "resolved",
        ):
            bound.add(str(row["provider_identifier"]))
    minted = set(registry.assetids())
    missing, extra = sorted(bound - minted, key=int), sorted(minted - bound, key=int)
    return {
        "source": bindings.snapshot_id,
        "rows": rows,
        "bound": len(bound),
        "instruments": len(minted),
        "equal": bound == minted,
        "bound_sha256": assetid_set_sha256(bound),
        "instruments_sha256": assetid_set_sha256(minted),
        "missing_count": len(missing),
        "missing": missing[:100],
        "extra_count": len(extra),
        "extra": extra[:100],
    }


# Registered assertions the US registry owns: Norgate, SEC and FMP claims and EODHD US
# symbols.
_US_ASSERTIONS: Final = (
    "SELECT a.assertion_id,a.provider,a.namespace,a.token,a.source_snapshot_id,"
    # A non-correlated IN is evaluated once; a correlated EXISTS would scan the table per
    # row, and no index covers supersedes_assertion_id.
    "a.assertion_id IN (SELECT supersedes_assertion_id FROM identity_assertions"
    " WHERE supersedes_assertion_id IS NOT NULL)"
    " FROM identity_assertions a WHERE a.provider IN ('norgate','sec','fmp')"
    " OR (a.provider='eodhd' AND a.namespace='eodhd_symbol' AND a.token LIKE '%.US')"
    " ORDER BY a.provider,a.namespace,a.token,a.assertion_id"
)


def check_registered(registry: UsRegistry, state: sqlite3.Connection) -> UsRegistry:
    """Hold a build to the US assertions already registered.

    Every source a registered US assertion cites must be among the build's sources, so a
    claim registered before is rebuilt with the same assertion ID instead of conflicting
    with itself; a build missing one is refused, naming the sources to add. A registered
    claim not corrected that the sources no longer give is reported in ``withdrawn``; the
    builder never closes it, because no source says when it stopped holding.
    """
    built = {
        cast("str", row["assertion_id"]) for row in parse_registry(registry.document).assertions
    }
    missing: set[str] = set()
    withdrawn: list[dict[str, object]] = []
    for assertion_id, provider, namespace, token, source, corrected in state.execute(
        _US_ASSERTIONS
    ):
        if source not in registry.sources:
            missing.add(str(source))
        elif not corrected and assertion_id not in built:
            withdrawn.append(
                {
                    "assertion_id": assertion_id,
                    "provider": provider,
                    "namespace": namespace,
                    "token": token,
                }
            )
    if missing:
        raise ValueError(
            "the US registry build must include every source its registered assertions "
            f"cite; add {sorted(missing)}"
        )
    registry.withdrawn = withdrawn
    return registry


def _sec_archive(workspace: Workspace, source_id: str) -> tuple[SourceFile, ArchiveOpener]:
    """The retained archive of an SEC submissions content source and how to open it."""
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415
    from aegis_alpha.storage.source_identity import content_of  # noqa: PLC0415
    from aegis_alpha.storage.source_library import _marker  # noqa: PLC0415

    if not source_id.startswith(SEC_PREFIX):
        raise ValueError(f"{SEC_MAPPER} reads an {SEC_PREFIX}* content source, not {source_id}")
    marker = _marker(workspace, source_id)
    if marker is None:
        raise ValueError(f"unknown source {source_id}")
    metadata = json.loads(str(marker[4])).get("metadata")
    content = content_of(source_id, str(marker[2]), metadata)
    if content is None:
        raise ValueError(f"{source_id} is not a content source")
    root = workspace.paths.raw

    def opener_for(item: SourceFile) -> ArchiveOpener:
        @contextmanager
        def opener() -> Iterator[BinaryIO]:
            with (
                DescriptorTree.open_path(root) as tree,
                tree.binary_reader(item.relative_path, require_single_link=True) as handle,
            ):
                yield handle

        return opener

    archives = []
    for item in content.files:
        with opener_for(item)() as handle:
            if handle.read(4) == b"PK\x03\x04":
                archives.append(item)
    if len(archives) != 1:
        raise ValueError(f"{source_id} must retain exactly one zip archive")
    return archives[0], opener_for(archives[0])


def build_from_workspace(
    workspace: Workspace,
    *,
    master: str,
    fmp: Sequence[str] = (),
    sec: Sequence[str] = (),
    bindings: str | None = None,
) -> UsRegistry:
    """Build the registry from committed, linked sources named by their source IDs.

    The sources must include every source the workspace's registered US assertions cite
    (``check_registered``). ``bindings`` names a legacy identity-bindings table to compare
    the minted asset IDs with; it adds no claim.
    """
    state = workspace.state
    # Every claim cites its source's sl: link, so an unlinked FMP source is refused here
    # rather than by registration.
    for source in fmp:
        link_instant(state, source)
    master_rows = LinkedRows(
        read_rows(workspace, master, MASTER_TABLE), link_instant(state, master)
    )
    sec_inputs = []
    for source in sec:
        archive, opener = _sec_archive(workspace, source)
        members = LinkedRows(read_rows(workspace, source, SEC_TABLE), link_instant(state, source))
        sec_inputs.append((members, archive, opener))
    registry = build_us_registry(
        master_rows,
        fmp=[read_rows(workspace, source, FMP_TABLE) for source in fmp],
        sec=sec_inputs,
    )
    if bindings is not None:
        registry.bindings = compare_bindings(
            registry, read_rows(workspace, bindings, BINDINGS_TABLE)
        )
    return check_registered(registry, state)
