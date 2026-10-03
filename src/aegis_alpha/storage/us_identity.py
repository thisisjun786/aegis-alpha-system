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
EXPORT_MAPPER: Final = "norgate.export_listing@1"
EXPORT_TABLE: Final = "bars"
EXPORT_PREFIX: Final = "norgate-history-csv-"
MASTER_TABLE: Final = "observations"
FMP_TABLE: Final = "observations"
SEC_TABLE: Final = "members"
BINDINGS_TABLE: Final = "observations"
SEC_PREFIX: Final = "sec-submissions-zip-"
VENUE: Final = "XNYS"
REFERENCE_VENUE: Final = "XXXX"
"""ISO 10383's "no market": a reference series (an index, a rate) trades on no venue."""
LISTED_DATABASE: Final = "US Equities"
DELISTED_DATABASE: Final = "US Equities Delisted"
REFERENCE_TYPES: Final = {
    "US Indices": "index",
    "World Indices": "index",
    "Economic": "economic_series",
    "Forex Spot": "fx_spot",
    "Cash Commodities": "commodity",
    "Continuous Futures": "continuous_future",
}
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


def read_export_rows(workspace: Workspace, source_id: str) -> ExportRows:
    """One linked export source's series: each group's first row and its date span.

    The table is verified against its marker like ``read_rows``; the grouping runs in
    DuckDB, so only one row per series reaches Python.
    """
    from aegis_alpha.storage.source_library import list_sources, list_tables  # noqa: PLC0415
    from aegis_alpha.storage.source_library_schema import connections, quoted  # noqa: PLC0415
    from aegis_alpha.storage.source_reader import SourcePin, resolve_source  # noqa: PLC0415

    if not source_id.startswith(EXPORT_PREFIX):
        raise ValueError(f"{EXPORT_MAPPER} reads {EXPORT_PREFIX}* sources, not {source_id}")
    linked = link_instant(workspace.state, source_id)
    source = next((row for row in list_sources(workspace) if row["source_id"] == source_id), None)
    described = next(
        (row for row in list_tables(workspace, source_id) if row["name"] == EXPORT_TABLE), None
    )
    if source is None or described is None:
        raise ValueError(f"{source_id} is not a committed source with a {EXPORT_TABLE} table")
    pin = SourcePin(source_id, str(source["sha256"]), EXPORT_TABLE, str(described["digest"]))
    resolved = resolve_source(workspace, pin)
    columns = tuple(cast("list[str]", resolved["columns"]))
    target = quoted(str(resolved["target"]))
    names = ",".join(quoted(column) for column in columns)
    connection = cast("duckdb.DuckDBPyConnection", connections(workspace)[str(resolved["store"])])
    query = (
        f"SELECT {names}, s.first_day, s.last_day FROM {target} t JOIN ("  # noqa: S608
        'SELECT min(_aas_ordinal) AS first_ordinal, min("date") AS first_day, '
        f'max("date") AS last_day FROM {target} GROUP BY "assetid", "symbol", "database") s '
        "ON t._aas_ordinal = s.first_ordinal ORDER BY t._aas_ordinal"
    )
    reader = connection.execute(query).to_arrow_reader(65536)
    rows: list[tuple[object, ...]] = []
    spans: list[tuple[object, object]] = []
    for batch in reader:
        for item in batch.to_pylist():
            rows.append(tuple(item[name] for name in columns))
            spans.append((item["first_day"], item["last_day"]))
    return ExportRows(
        SourceRows(LINK_PREFIX + source_id, columns, tuple(rows)), tuple(spans), linked
    )


def export_sources(workspace: Workspace) -> list[str]:
    """Every committed ``norgate-history-csv-*`` source with a ``bars`` table, in ID order."""
    from aegis_alpha.storage.source_library import list_sources, list_tables  # noqa: PLC0415

    return sorted(
        source_id
        for source in list_sources(workspace)
        if (source_id := str(source["source_id"])).startswith(EXPORT_PREFIX)
        and any(table["name"] == EXPORT_TABLE for table in list_tables(workspace, source_id))
    )


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


# --- norgate.export_listing@1 ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExportSeries:
    """One series of a Norgate history export: what its export record says, and its dates."""

    assetid: str
    symbol: str
    database: str
    first_date: date
    last_date: date
    evidence: Evidence

    @property
    def equity(self) -> bool:
        return self.database in {LISTED_DATABASE, DELISTED_DATABASE}

    @property
    def listed(self) -> bool:
        return self.database == LISTED_DATABASE

    @property
    def instrument_id(self) -> str:
        return mint_instrument("norgate_assetid", self.assetid)


@dataclass(frozen=True, slots=True)
class ExportRows:
    """The first row of each series of one export source, with the series' date span.

    ``rows`` holds one source row per (asset ID, symbol, database) group, the one with the
    smallest ordinal, which the series' claims cite; ``spans`` holds that group's smallest
    and largest ``date`` text in the same order.
    """

    rows: SourceRows
    spans: tuple[tuple[object, object], ...]
    linked_at_us: int


_EXPORT_COLUMNS: Final = ("assetid", "symbol", "database", "date")


def map_norgate_exports(
    sources: Sequence[ExportRows],
) -> tuple[list[ExportSeries], list[str], MapperReport]:
    """``norgate.export_listing@1``: each exported series, refusing what it cannot stand on.

    A series needs a positive asset ID, a trimmed symbol, a known database (the two US
    equity databases or a reference database of ``REFERENCE_TYPES``) and ``YYYY-MM-DD``
    first and last dates. Series of one asset ID from several sources merge when they agree
    on symbol and database, keeping the earliest evidence; an asset ID whose series disagree
    is refused whole (``export_assetid_repeated``).
    """
    report = MapperReport()
    found: dict[str, list[ExportSeries]] = defaultdict(list)
    for source in sources:
        _require(source.rows, _EXPORT_COLUMNS, EXPORT_MAPPER)
        for (row, row_hash), (first, last) in zip(source.rows.records(), source.spans, strict=True):
            report.rows += 1
            raw_id = row["assetid"]
            assetid = _identifier(
                IdentifierType.NORGATE_ASSETID,
                str(raw_id) if type(raw_id) is int and raw_id > 0 else None,
            )
            symbol = _text(row["symbol"])
            database = row["database"]
            start, end = _day(first), _day(last)
            if assetid is None:
                report.refuse("assetid_invalid")
            elif symbol is None:
                report.refuse("symbol_invalid")
            elif database not in REFERENCE_TYPES and database not in {
                LISTED_DATABASE,
                DELISTED_DATABASE,
            }:
                report.refuse("database_unknown")
            elif start is None or end is None or end < start:
                report.refuse("dates_invalid")
            else:
                report.accepted += 1
                evidence = Evidence(source.rows.snapshot_id, row_hash, source.linked_at_us)
                found[assetid].append(
                    ExportSeries(assetid, symbol, str(database), start, end, evidence)
                )
    series: list[ExportSeries] = []
    repeated: list[str] = []
    for assetid, group in sorted(found.items(), key=lambda item: int(item[0])):
        if len({(item.symbol, item.database) for item in group}) > 1:
            report.refuse("export_assetid_repeated")
            repeated.append(assetid)
            continue
        first = min(group, key=lambda item: item.evidence.order())
        series.append(
            replace(
                first,
                first_date=min(item.first_date for item in group),
                last_date=max(item.last_date for item in group),
            )
        )
    return series, repeated, report


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
    windows: dict[str, tuple[str, int, int]] = field(default_factory=dict)
    """Each EODHD US symbol an export lists after ``through``: (instrument, start, end)."""
    export_through: date | None = None
    """The exports' last observed equity session; export ticker claims end the day after it."""

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
            window = self.windows.get(symbol)
            at = session_start_us(day)
            if window is not None and window[1] <= at < window[2]:
                reason = "resolved"
            elif status.startswith("ins-"):
                start, end = self.intervals[symbol]
                reason = (
                    "resolved"
                    if start <= at < end
                    else "before_ticker_claim"
                    if at < start
                    else "after_export_through"
                    if window is not None
                    else "after_master_through"
                )
            elif window is not None:
                reason = "before_ticker_claim" if at < window[1] else "after_export_through"
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
            "export_through": None
            if self.export_through is None
            else self.export_through.isoformat(),
            "export_symbols": len(self.windows),
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


@dataclass(frozen=True, slots=True)
class _Claim:
    """One interval in which a US ticker names one Norgate series.

    The master gives a bounded listed ticker one claim that ends the day after its last
    observed session; a history export can give the ticker a later claim. ``after`` names
    the bound that an instant at or past ``end`` lies beyond.
    """

    assetid: str
    start: int
    end: int
    etf: bool
    evidence: Evidence
    after: str

    @property
    def interval(self) -> tuple[int, int]:
        return (self.start, self.end)


def _master_claims(tickers: Mapping[str, Listing], bounds: _Bounds) -> dict[str, list[_Claim]]:
    return {
        ticker: [
            _Claim(
                listing.assetid,
                *bounds.intervals[listing.assetid],
                etf=listing.asset_type == "etf",
                evidence=listing.evidence,
                after="after_master_through",
            )
        ]
        for ticker, listing in tickers.items()
    }


def _outside(at: int, claims: Sequence[_Claim]) -> str:
    """Why a ticker-matched row retrieved at ``at`` names none of the ticker's claims."""
    if at < claims[0].start:
        return "before_ticker_claim"
    if at >= claims[-1].end:
        return claims[-1].after
    return "between_ticker_claims"


def _by_claim[T](
    rows: Sequence[T], instant: Callable[[T], int], claims: Sequence[_Claim], prefix: str
) -> dict[_Claim, list[T]] | str:
    """The rows each claim holds by their retrieval instant, or why no claim holds any.

    The reason is ``<prefix>_ticker_missing`` when there are no rows, otherwise
    ``<prefix>_<position>`` placing the earliest row against the claims.
    """
    if not rows:
        return f"{prefix}_ticker_missing"
    found: dict[_Claim, list[T]] = {}
    for claim in claims:
        inside = [row for row in rows if claim.start <= instant(row) < claim.end]
        if inside:
            found[claim] = inside
    if found:
        return found
    return f"{prefix}_{_outside(min(instant(row) for row in rows), claims)}"


def _profiles(
    profiles: Mapping[str, list[Profile]],
    refused: Mapping[str, str],
    claims: Mapping[str, list[_Claim]],
    unresolved: dict[str, list[str]],
) -> dict[tuple[str, _Claim], Profile]:
    """The one agreed profile of each ticker claim, judged against the claim's series.

    A profile is judged only against the claim that holds when it was retrieved; a
    profile retrieved outside every claim may describe another holder of the ticker.
    """
    agreed: dict[tuple[str, _Claim], Profile] = {}
    for symbol in sorted({*profiles, *refused}):
        held = claims.get(symbol)
        if held is None:
            unresolved["not_a_listed_norgate_ticker"].append(symbol)
            continue
        if symbol in refused:
            unresolved[refused[symbol]].append(symbol)
            continue
        found = _by_claim(
            profiles[symbol], lambda profile: profile.evidence.known_from_us, held, "fmp"
        )
        if isinstance(found, str):
            unresolved[found].append(symbol)
            continue
        reasons: set[str] = set()
        for claim, inside in found.items():
            if len({profile.statement() for profile in inside}) > 1:
                reasons.add("fmp_profile_ambiguous")
            elif inside[0].currency != "USD":
                reasons.add("fmp_currency_not_usd")
            elif inside[0].etf != claim.etf:
                reasons.add("fmp_type_differs")
            else:
                agreed[symbol, claim] = min(inside, key=lambda profile: profile.evidence.order())
        for reason in sorted(reasons):
            unresolved[reason].append(symbol)
    return agreed


def _shared(
    values: Iterable[tuple[str, str]], unresolved: dict[str, list[str]], reason: str
) -> set[str]:
    """Identifier values (CUSIP, ISIN) two series claim: neither claim is kept."""
    owners: dict[str, set[str]] = defaultdict(set)
    for assetid, value in values:
        owners[value].add(assetid)
    shared = {value for value, found in owners.items() if len(found) > 1}
    unresolved[reason].extend(sorted(shared))
    return shared


def _fmp_assertions(
    agreed: Mapping[tuple[str, _Claim], Profile], unresolved: dict[str, list[str]]
) -> list[Record]:
    """The FMP symbol of every agreed claim and the CUSIP and ISIN its profile names.

    A profile states the identifiers current when it was retrieved, not since when, so
    an identifier holds from that instant; one series' identifier stated by several
    claims' profiles is claimed once, from the earliest of them.
    """
    shared = {
        namespace: _shared(
            (
                (claim.assetid, value)
                for (_, claim), profile in agreed.items()
                if (value := getattr(profile, namespace))
            ),
            unresolved,
            f"{namespace}_ambiguous",
        )
        for namespace in ("cusip", "isin")
    }
    assertions: list[Record] = []
    identifiers: dict[tuple[str, str, str], tuple[str, Evidence]] = {}
    for (ticker, claim), profile in agreed.items():
        key = ("fmp", "fmp_symbol", ticker)
        assertions.append(_assertion(claim.assetid, key, claim.interval, profile.evidence))
        for namespace, values in shared.items():
            value = getattr(profile, namespace)
            if not value or value in values:
                continue
            key = ("fmp", namespace, value)
            earlier = identifiers.get(key)
            if earlier is None or profile.evidence.order() < earlier[1].order():
                identifiers[key] = (claim.assetid, profile.evidence)
    for key, (assetid, evidence) in identifiers.items():
        assertions.append(_assertion(assetid, key, (evidence.known_from_us, None), evidence))
    return assertions


def _link(
    inside: Sequence[Filer],
    profile: Profile | None,
    claim: _Claim,
) -> tuple[Filer, int, int] | str:
    """One claim's SEC filer and the link's valid and known instants, or why it has none."""
    if len({filer.cik for filer in inside}) > 1:
        return "sec_ticker_ambiguous"
    filer = min(inside, key=lambda item: item.evidence.order())
    if profile is None:
        return "fmp_profile_missing"
    if profile.cik is None:
        return "fmp_cik_missing"
    if profile.cik != filer.cik:
        return "fmp_cik_differs"
    valid = max(filer.evidence.known_from_us, profile.evidence.known_from_us)
    return filer, valid, max(valid, claim.evidence.known_from_us)


def _issuers(
    filers: Sequence[Filer],
    agreed: Mapping[tuple[str, _Claim], Profile],
    claims: Mapping[str, list[_Claim]],
    unresolved: dict[str, list[str]],
) -> tuple[list[Record], dict[str, tuple[str, Evidence, int]]]:
    """Issuer links of ticker claims that SEC and FMP tie to the same CIK.

    Only SEC filers retrieved while a claim holds count for it. Both sources state the
    current ticker-to-CIK mapping, not since when, so a link is valid from the later of
    the SEC and FMP instants and known from the latest of those and the series' own
    evidence. A series links once, by its earliest-valid link over all its claims; a claim
    naming another CIK for it stays unresolved.
    """
    by_ticker: dict[str, list[Filer]] = defaultdict(list)
    for filer in filers:
        for ticker in set(filer.tickers):
            by_ticker[ticker].append(filer)
    candidates: list[tuple[str, str, Filer, int, int]] = []
    for ticker, held in sorted(claims.items()):
        found = _by_claim(
            by_ticker.get(ticker, []), lambda filer: filer.evidence.known_from_us, held, "sec"
        )
        if isinstance(found, str):
            unresolved[found].append(ticker)
            continue
        reasons: set[str] = set()
        for claim, inside in found.items():
            judged = _link(inside, agreed.get((ticker, claim)), claim)
            if isinstance(judged, str):
                reasons.add(judged)
                continue
            filer, valid, known = judged
            candidates.append((claim.assetid, ticker, filer, valid, known))
        for reason in sorted(reasons):
            unresolved[reason].append(ticker)
    return _chosen(candidates, unresolved)


def _chosen(
    candidates: Sequence[tuple[str, str, Filer, int, int]], unresolved: dict[str, list[str]]
) -> tuple[list[Record], dict[str, tuple[str, Evidence, int]]]:
    """Each series' earliest-valid link among its (assetid, ticker, filer, valid, known)."""
    links: dict[str, tuple[str, Evidence, int]] = {}
    for assetid, _, filer, valid, known in sorted(
        candidates, key=lambda item: (item[3], item[2].evidence.order(), item[1])
    ):
        if assetid not in links:
            links[assetid] = (filer.cik, replace(filer.evidence, known_from_us=known), valid)
    names: dict[str, Filer] = {}
    differing: set[str] = set()
    for assetid, ticker, filer, _, _ in candidates:
        if links[assetid][0] != filer.cik:
            differing.add(ticker)
            continue
        named = names.get(filer.cik)
        if named is None or filer.evidence.order() < named.evidence.order():
            names[filer.cik] = filer
    if differing:
        unresolved["sec_cik_differs_across_claims"].extend(sorted(differing))
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
    accepted: Sequence[Listing], repeated: set[str], bounds: _Bounds
) -> list[Record]:
    """Norgate's own claims of every accepted listing.

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
    return assertions


def _issuer_assertions(links: Mapping[str, tuple[str, Evidence, int]]) -> list[Record]:
    """The ``sec``/``issuer`` link of every linked series, valid from its later instant."""
    assertions: list[Record] = []
    for assetid, (cik, link, since) in sorted(links.items()):
        instrument = mint_instrument("norgate_assetid", assetid)
        key = ("sec", "issuer", issuer_link_token(mint_issuer("sec_cik", cik), instrument))
        assertions.append(_assertion(assetid, key, (since, None), link))
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


@dataclass(slots=True)
class _Exported:
    """What the exports add: instruments, claims and the EODHD symbols of their window."""

    instruments: list[Record] = field(default_factory=list)
    assertions: list[Record] = field(default_factory=list)
    windows: dict[str, tuple[str, int, int]] = field(default_factory=dict)
    through: date | None = None


def _export_instruments(
    series: Sequence[ExportSeries], listings: Sequence[Listing], exported: _Exported
) -> None:
    """Instruments the master does not know, and the permanent names exports give them.

    An asset ID the master lists (even one it lists twice) is never minted again. A new
    equity is ``unclassified`` on ``XNYS``; a reference series takes its database's type on
    the ``XXXX`` venue. A reference or delisted symbol (``<ticker>-YYYYMM``) names its series
    for good, so it holds for all time when no master row and no other series has it.
    """
    known = {listing.assetid for listing in listings}
    master_symbols = {listing.symbol for listing in listings}
    counts: dict[str, int] = defaultdict(int)
    for item in series:
        counts[item.symbol] += 1
    for item in series:
        if item.assetid not in known:
            exported.instruments.append(
                {
                    **_anchor("norgate_assetid", item.assetid),
                    "asset_type": "unclassified" if item.equity else REFERENCE_TYPES[item.database],
                    "venue": VENUE if item.equity else REFERENCE_VENUE,
                }
            )
            key = ("norgate", "norgate_assetid", item.assetid)
            exported.assertions.append(_assertion(item.assetid, key, _ALWAYS, item.evidence))
        if (
            not item.listed
            and counts[item.symbol] == 1
            and item.symbol not in master_symbols
            and (not item.equity or _DELISTED_SYMBOL.fullmatch(item.symbol) is not None)
        ):
            key = ("norgate", "norgate_symbol", item.symbol)
            exported.assertions.append(_assertion(item.assetid, key, _ALWAYS, item.evidence))


def _export_tickers(
    series: Sequence[ExportSeries],
) -> tuple[dict[str, list[ExportSeries]], dict[str, date], dict[str, set[str]]]:
    """Listed series by ticker, and each ticker's delisted holders' last date and IDs."""
    held: dict[str, date] = {}
    held_by: dict[str, set[str]] = defaultdict(set)
    spelled: dict[str, list[ExportSeries]] = defaultdict(list)
    for item in series:
        if item.listed:
            spelled[us_ticker(item.symbol)].append(item)
            continue
        match = _DELISTED_SYMBOL.fullmatch(item.symbol) if item.equity else None
        if match is not None:
            base = us_ticker(match[1])
            held[base] = max(item.last_date, held.get(base, item.last_date))
            held_by[base].add(item.assetid)
    return spelled, held, held_by


def _export_window(  # noqa: PLR0913 -- the window reads the master's view of every ticker
    series: Sequence[ExportSeries],
    *,
    holders: Mapping[str, set[str]],
    master: Mapping[str, Listing],
    through: date | None,
    exported: _Exported,
    unresolved: dict[str, list[str]],
) -> dict[str, _Claim]:
    """Ticker claims of the export's listed series after the master's ``through``.

    The window runs from the New York start of the day after ``through`` to that of the day
    after the exports' last equity session. A listed series' ticker holds in it from the
    later of the window start, the series' first date and the day after the last date of a
    delisted ``<ticker>-YYYYMM`` series. A ticker two listed series share is ambiguous, and
    a ticker the master gave to another listing is held over only when the export shows
    that listing as the ticker's delisted earlier holder; otherwise when it moved is unknown.
    Norgate's symbol and the EODHD symbol are claimed here; FMP and SEC rows retrieved
    inside the window are judged against the returned claim like the master's.
    """
    days = [item.last_date for item in series if item.equity]
    exported.through = max(days, default=None)
    claims: dict[str, _Claim] = {}
    if through is None or exported.through is None or exported.through <= through:
        return claims
    opens = through + timedelta(days=1)
    closes = exported.through + timedelta(days=1)
    spelled, held, held_by = _export_tickers(series)
    for ticker, group in sorted(spelled.items()):
        if len(group) > 1:
            unresolved["export_ticker_ambiguous"].append(ticker)
            continue
        item = group[0]
        moved = holders.get(ticker, set()) - {item.assetid}
        if not moved <= held_by.get(ticker, set()):
            unresolved["export_ticker_moved"].append(ticker)
            continue
        start = max(opens, item.first_date)
        if ticker in held:
            start = max(start, held[ticker] + timedelta(days=1))
        if start >= closes:
            unresolved["export_ticker_reused"].append(ticker)
            continue
        listing = master.get(item.assetid)
        claim = _Claim(
            item.assetid,
            session_start_us(start),
            session_start_us(closes),
            etf=listing is not None and listing.asset_type == "etf",
            evidence=item.evidence,
            after="after_export_through",
        )
        for key in (
            ("norgate", "norgate_symbol", item.symbol),
            ("eodhd", "eodhd_symbol", f"{ticker}.US"),
        ):
            exported.assertions.append(_assertion(item.assetid, key, claim.interval, item.evidence))
        exported.windows[f"{ticker}.US"] = (item.instrument_id, *claim.interval)
        claims[ticker] = claim
    return claims


def build_us_registry(
    master: LinkedRows,
    *,
    fmp: Sequence[SourceRows] = (),
    sec: Sequence[tuple[LinkedRows, SourceFile, ArchiveOpener]] = (),
    exports: Sequence[ExportRows] = (),
) -> UsRegistry:
    """Build the US ``aas-identity-registry-v1`` document from pinned source rows.

    Norgate is a frozen source: one master names the listed and delisted instruments, and
    its later history exports add the series it lacks and the ticker claims of the sessions
    they show after the master's last one. The document lists issuers by CIK, instruments by
    asset ID and assertions by provider, namespace and token, so the same sources always
    give the same bytes.
    """
    unresolved: dict[str, dict[str, list[str]]] = {
        key: defaultdict(list)
        for key in ("listings", "tickers", "fmp", "identifiers", "issuers", "exports")
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
    claims = _master_claims(tickers, bounds)
    exported = _Exported()
    if exports:
        series, conflicting, mappers[EXPORT_MAPPER] = map_norgate_exports(exports)
        unresolved["exports"]["export_assetid_repeated"].extend(conflicting)
        _export_instruments(series, listings, exported)
        holders: dict[str, set[str]] = defaultdict(set)
        for listing in accepted:
            if listing.listed and listing.symbol not in repeated:
                holders[us_ticker(listing.symbol)].add(listing.assetid)
        windows = _export_window(
            series,
            holders=holders,
            master={listing.assetid: listing for listing in accepted},
            through=bounds.through,
            exported=exported,
            unresolved=unresolved["exports"],
        )
        for ticker, claim in windows.items():
            claims.setdefault(ticker, []).append(claim)
    profiles, refused, mappers[FMP_MAPPER] = map_fmp_profiles(fmp)
    agreed = _profiles(profiles, refused, claims, unresolved["fmp"])
    filers, mappers[SEC_MAPPER] = _sec_filers(sec)
    issuers, links = _issuers(filers, agreed, claims, unresolved["issuers"]) if sec else ([], {})
    eodhd, symbols, intervals, mappers[EODHD_MAPPER] = _eodhd(
        tickers, bounds, unresolved["tickers"]
    )
    assertions = [
        *_listing_assertions(accepted, repeated, bounds),
        *_issuer_assertions(links),
        *eodhd,
        *_fmp_assertions(agreed, unresolved["identifiers"]),
        *exported.assertions,
    ]
    instruments: list[Record] = [
        {**_anchor("norgate_assetid", listing.assetid), "asset_type": listing.asset_type}
        | {"venue": VENUE}
        for listing in accepted
    ]
    instruments.extend(exported.instruments)
    for row in instruments:
        link = links.get(str(row["anchor_token"]))
        row["issuer"] = None if link is None else _anchor("sec_cik", link[0])
    instruments.sort(key=lambda row: int(str(row["anchor_token"])))
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
            + [source.rows.snapshot_id for source in exports]
        ),
        intervals=intervals,
        through=bounds.through,
        windows=exported.windows,
        export_through=exported.through,
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


def build_from_workspace(  # noqa: PLR0913 -- one keyword per source kind
    workspace: Workspace,
    *,
    master: str,
    fmp: Sequence[str] = (),
    sec: Sequence[str] = (),
    exports: Sequence[str] = (),
    bindings: str | None = None,
) -> UsRegistry:
    """Build the registry from committed, linked sources named by their source IDs.

    The sources must include every source the workspace's registered US assertions cite
    (``check_registered``). ``exports`` names Norgate history export sources
    (``norgate.export_listing@1``). ``bindings`` names a legacy identity-bindings table to
    compare the minted asset IDs with; it adds no claim.
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
        exports=[read_export_rows(workspace, source) for source in exports],
    )
    if bindings is not None:
        registry.bindings = compare_bindings(
            registry, read_rows(workspace, bindings, BINDINGS_TABLE)
        )
    return check_registered(registry, state)
