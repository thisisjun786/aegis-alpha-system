"""KR identity registry: EODHD KR symbols, KIND listings and DART corp codes.

Three identity mappers turn retained source rows into one ``aas-identity-registry-v1``
document; ``aas identity register`` then appends it like any other registry document.

- ``eodhd.kr_symbol@1`` reads the EODHD exchange symbol lists of ``KO`` (KOSPI) and
  ``KQ`` (KOSDAQ). A KRW row whose ``Isin`` has a valid check digit mints the instrument
  ``mint_instrument('krx_isin', Isin)`` (venue ``XKRX``, the operator of both markets;
  a foreign company listed on KRX keeps its own country prefix) and asserts the provider
  symbol ``<Code>.<Exchange>`` (namespace ``eodhd_symbol``, the token ``eodhd.bars@1``
  resolves) and the KRX short code ``Code`` (namespace ``krx_short_code``). The list
  carries no dates, so both claims are valid over the provider's whole series. EODHD types
  preferred shares "Common Stock", so a stock's asset type is ``unclassified``.
- ``kind.listings@1`` reads the KIND listed-company lists and asserts the short code
  (provider ``kind``) from its listing date, at local midnight in Asia/Seoul.
- ``dart.corp_codes@1`` reads DART ``corpCode.xml`` receipts. A corp with a stock code
  is an issuer ``mint_issuer('dart_corp_code', corp_code)`` and the instrument with that
  short code is linked to it by a ``dart`` issuer assertion.

KIND and DART name no ISIN, so they reach an instrument only through the short code that
EODHD binds to exactly one ISIN. Nothing is derived from a ticker, a short code or a
name, and no list is chosen over another: lists that disagree on a symbol's ISIN, type or
currency (only a blank ISIN defers), an ISIN claimed by two short codes or two asset
types, a short code claimed by two ISINs and a stock code claimed by two corps all stay
unresolved with their reason, never guessed (data-vertical risk 7).

Every assertion is known from the retrieval instant its source receipt records, the
earliest time AAS can show the claim was public, and cites its source as
``sl:<source_id>`` with the ``aas-source-row-v1`` hash of the row it came from. The build
input is cumulative (``check_registered``). The contract is in
dev-notes/design/data-vertical.md."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import zipfile
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Final, cast
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage.identity import (
    REGISTRY_SCHEMA,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    parse_registry,
)
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.source_identity import LINK_PREFIX, SourceContent, SourceFile

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace

SOURCE_MAJOR: Final = 1
KIND_PROVIDER: Final = "kind"
KIND_SHAPE: Final = "listings"
KIND_TABLE: Final = "listings"
EODHD_PROVIDER: Final = "qveris"
EODHD_SHAPE: Final = "eodhd-exchange-symbols"
EODHD_TABLE: Final = "symbols"
DART_TABLE: Final = "receipts"

KIND_MAPPER: Final = "kind.listings@1"
EODHD_MAPPER: Final = "eodhd.kr_symbol@1"
DART_MAPPER: Final = "dart.corp_codes@1"

KIND_COLUMNS: Final = (
    "list_id",
    "company_name",
    "market",
    "short_code",
    "industry",
    "products",
    "listed_on",
    "fiscal_month",
    "representative",
    "homepage",
    "region",
    "retrieved_at_utc",
)
# The columns ``kind.listings@1`` reads; a ``kind-listings`` table without them (the
# ``korea.public_response@1`` legacy import) is no KR identity source.
KIND_REQUIRED: Final = ("short_code", "listed_on", "retrieved_at_utc")
EODHD_COLUMNS: Final = (
    "exchange_code",
    "delisted",
    "job_status",
    "code",
    "name",
    "country",
    "exchange",
    "currency",
    "type",
    "isin",
    "retrieved_at_utc",
)
_KIND_HEADER: Final = (
    "회사명",
    "시장구분",
    "종목코드",
    "업종",
    "주요제품",
    "상장일",
    "결산월",
    "대표자명",
    "홈페이지",
    "지역",
)
_KIND_LISTS: Final = frozenset({"kind-kospi", "kind-kosdaq"})
_EODHD_ORDER: Final = ("Code", "Name", "Country", "Exchange", "Currency", "Type", "Isin")
_EODHD_KEYS: Final = frozenset(_EODHD_ORDER)
_HTTP_OK: Final = 200
_EODHD_TOOL: Final = "eodhd.exchange_symbols.list."
KR_EXCHANGES: Final = frozenset({"KO", "KQ"})
VENUE: Final = "XKRX"
# EODHD types KRX preferred shares "Common Stock", so a share's class is not asserted here:
# it stays ``unclassified`` for the classifications dataset to state.
ASSET_TYPES: Final[Mapping[str, str]] = {
    "Common Stock": "unclassified",
    "Preferred Stock": "unclassified",
    "ETF": "etf",
}
SEOUL: Final = ZoneInfo("Asia/Seoul")
UNBOUNDED: Final = -(2**63)
MAX_FILE_BYTES: Final = 64 * 1024 * 1024
MAX_XML_BYTES: Final = 256 * 1024 * 1024
_SHORT_CODE: Final = re.compile(r"[0-9A-Z]{6}")
_INSTANT: Final = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z")
_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_DAY: Final = re.compile(r"\d{4}-\d{2}-\d{2}")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)
_READ_BATCH: Final = 1024

type Row = tuple[object, ...]
type Record = dict[str, object]


def instant_us(text: object) -> int:
    """UTC microseconds of a receipt instant spelled ``YYYY-MM-DDTHH:MM:SS[.ffffff]Z``."""
    if not isinstance(text, str) or _INSTANT.fullmatch(text) is None:
        raise ValueError("receipt instant must be UTC ISO-8601 ending in Z")
    moment = datetime.fromisoformat(text[:-1]).replace(tzinfo=UTC)
    return (moment - _EPOCH) // _MICROSECOND


def local_midnight_us(day: date) -> int:
    """UTC microseconds of ``day``'s local start in Asia/Seoul."""
    moment = datetime(day.year, day.month, day.day, tzinfo=SEOUL)
    return (moment - _EPOCH) // _MICROSECOND


def _json(raw: bytes, name: str) -> Mapping[str, object]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError(f"{name} is not UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")  # noqa: TRY004 -- malformed content
    return cast("Mapping[str, object]", value)


def _field(body: Mapping[str, object], key: str, name: str) -> object:
    if key not in body:
        raise ValueError(f"{name} lacks {key}")
    return body[key]


def _check_bytes(raw: bytes, sha256: object, size: object, name: str) -> None:
    if hashlib.sha256(raw).hexdigest() != sha256 or len(raw) != size:
        raise ValueError(f"{name} bytes differ from the size and SHA-256 its receipt records")


# --- retained source units ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceUnit:
    """One complete original unit: its files, content identity and the source table rows."""

    content: SourceContent
    files: tuple[bytes, ...]
    table: str
    columns: tuple[str, ...]
    rows: tuple[tuple[str | None, ...], ...]

    def source_rows(self) -> SourceRows:
        """The rows as the source library will hold them, cited by the content ID."""
        return SourceRows(LINK_PREFIX + self.content.source_id, self.columns, self.rows)

    def arrow(self) -> pa.Table:
        import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

        return pa.table(
            {name: [row[index] for row in self.rows] for index, name in enumerate(self.columns)},
            schema=pa.schema([(name, pa.string()) for name in self.columns]),
        )


@dataclass(frozen=True, slots=True)
class _Shape:
    provider: str
    shape: str
    table: str
    columns: tuple[str, ...]

    def unit(self, files: Sequence[bytes], rows: Sequence[tuple[str | None, ...]]) -> SourceUnit:
        content = SourceContent(
            self.provider,
            self.shape,
            SOURCE_MAJOR,
            tuple(SourceFile(hashlib.sha256(raw).hexdigest(), len(raw)) for raw in files),
        )
        return SourceUnit(content, tuple(files), self.table, self.columns, tuple(rows))


KIND_SOURCE: Final = _Shape(KIND_PROVIDER, KIND_SHAPE, KIND_TABLE, KIND_COLUMNS)
EODHD_SOURCE: Final = _Shape(EODHD_PROVIDER, EODHD_SHAPE, EODHD_TABLE, EODHD_COLUMNS)


class _TableParser(HTMLParser):
    """Collect the text of every ``th``/``td`` cell, row by row."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[tuple[str, list[str]]] = []
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag == "tr":
            self.rows.append(("", []))
        elif tag in {"th", "td"}:
            if not self.rows:
                raise ValueError("KIND listing cell outside a table row")
            kind, cells = self.rows[-1]
            if kind not in {"", tag}:
                raise ValueError("KIND listing row mixes header and data cells")
            self.rows[-1] = (tag, cells)
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None:
            self.rows[-1][1].append(" ".join("".join(self._cell).split()))
            self._cell = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
    return cast("Mapping[str, object]", value)


def kind_unit(receipt: bytes, raw: bytes) -> SourceUnit:
    """A KIND listed-company download (``corpList.do``) and the receipt that names it.

    The receipt records the request (``kind-kospi`` or ``kind-kosdaq``), the retrieval
    instant and the response bytes' size and SHA-256. The response is the HTML table KIND
    serves as its Excel download, declared EUC-KR and decoded as its superset CP949; each
    cell keeps its text with whitespace runs collapsed.
    """
    body = _json(receipt, "KIND receipt")
    request = _object(_field(body, "request", "KIND receipt"), "KIND receipt request")
    reference = _object(_field(body, "raw", "KIND receipt"), "KIND receipt raw")
    list_id = request.get("source_id")
    if list_id not in _KIND_LISTS:
        raise ValueError("KIND receipt is not a kind-kospi or kind-kosdaq listing")
    if _field(body, "status", "KIND receipt") != _HTTP_OK:
        raise ValueError("KIND receipt does not record an HTTP 200 response")
    _check_bytes(raw, reference.get("content_sha256"), reference.get("size_bytes"), "KIND response")
    retrieved = _field(body, "retrieved_at_utc", "KIND receipt")
    instant_us(retrieved)
    try:
        text = raw.decode("cp949")
    except UnicodeDecodeError:
        raise ValueError("KIND response is not EUC-KR text") from None
    parser = _TableParser()
    parser.feed(text)
    parser.close()
    headers = [cells for kind, cells in parser.rows if kind == "th"]
    if headers != [list(_KIND_HEADER)]:
        raise ValueError("KIND response does not carry the listed-company header")
    rows = []
    for kind, cells in parser.rows:
        if kind != "td":
            continue
        if len(cells) != len(_KIND_HEADER):
            raise ValueError("KIND response row does not have one cell per header")
        rows.append((cast("str", list_id), *cells, cast("str", retrieved)))
    return KIND_SOURCE.unit((receipt, raw), rows)


def _job_request(body: Mapping[str, object]) -> tuple[str, str, str]:
    """The listed exchange, the ``delisted`` request flag and the completion status."""
    job = _object(_field(body, "job", "EODHD symbol job completion"), "EODHD symbol job")
    if not str(job.get("tool_id", "")).startswith(_EODHD_TOOL):
        raise ValueError("job is not an EODHD exchange symbol list")
    try:
        parameters = json.loads(cast("str", job.get("parameters_json")))
    except (TypeError, json.JSONDecodeError):
        raise ValueError("EODHD symbol job parameters are not JSON") from None
    parameters = _object(parameters, "EODHD symbol job parameters")
    exchange, delisted = parameters.get("EXCHANGE_CODE"), parameters.get("delisted", "0")
    status = _field(body, "status", "EODHD symbol job completion")
    if not isinstance(exchange, str) or delisted not in {"0", "1"} or not isinstance(status, str):
        raise ValueError("EODHD symbol job names no exchange, delisted flag or status")
    return exchange, cast("str", delisted), status


def _job_files(body: Mapping[str, object], files: Mapping[str, bytes]) -> dict[str, bytes]:
    """Exactly the files the completion lists, each with its recorded size and SHA-256."""
    listed = _field(body, "files", "EODHD symbol job completion")
    if not isinstance(listed, list):
        raise ValueError("EODHD symbol job completion files must be a list")  # noqa: TRY004 -- malformed-content ValueError contract
    contents: dict[str, bytes] = {}
    for item in cast("list[object]", listed):
        entry = _object(item, "EODHD symbol job file")
        path = entry.get("path")
        if not isinstance(path, str) or path not in files or path in contents:
            raise ValueError(f"EODHD symbol job file {path} is missing or repeated")
        _check_bytes(files[path], entry.get("sha256"), entry.get("size"), path)
        contents[path] = files[path]
    if set(files) != set(contents):
        raise ValueError("EODHD symbol job files differ from the files its completion lists")
    return contents


def _page(contents: Mapping[str, bytes], page: str) -> tuple[str, list[object]]:
    """One response page's retrieval instant (from its receipt) and its symbol rows."""
    receipt = page.removesuffix(".raw") + ".response.json"
    if receipt not in contents:
        raise ValueError(f"EODHD symbol page {page} has no response receipt")
    retrieved = _field(_json(contents[receipt], receipt), "retrieved_at_utc", receipt)
    instant_us(retrieved)
    result = _object(_json(contents[page], page).get("result"), f"{page} result")
    data = result.get("data")
    if result.get("status_code") != _HTTP_OK or not isinstance(data, list):
        raise ValueError(f"EODHD symbol page {page} is not a successful symbol list")
    return cast("str", retrieved), cast("list[object]", data)


def eodhd_unit(complete: bytes, files: Mapping[str, bytes]) -> SourceUnit:
    """One collected EODHD exchange-symbol-list job: ``complete.json`` and every file it lists.

    ``files`` maps each path ``complete.json`` lists to its bytes. Each page's response
    receipt (``NNNN.response.json``) gives the retrieval instant of its ``NNNN.raw`` page,
    whose ``result.data`` rows are kept with the job's exchange, ``delisted`` request flag
    and completion status (a provider warning is recorded, not hidden).
    """
    body = _json(complete, "EODHD symbol job completion")
    exchange, delisted, status = _job_request(body)
    contents = _job_files(body, files)
    pages = sorted(path for path in contents if path.endswith(".raw"))
    if not pages:
        raise ValueError("EODHD symbol job lists no response page")
    rows: list[tuple[str | None, ...]] = []
    for page in pages:
        retrieved, data = _page(contents, page)
        for item in data:
            entry = _object(item, f"{page} row")
            if set(entry) != _EODHD_KEYS:
                raise ValueError(f"EODHD symbol page {page} holds a row of another shape")
            values = [entry[key] for key in _EODHD_ORDER]
            if any(value is not None and not isinstance(value, str) for value in values):
                raise ValueError(f"EODHD symbol page {page} holds a non-text value")
            rows.append((exchange, delisted, status, *cast("list[str | None]", values), retrieved))
    return EODHD_SOURCE.unit((complete, *(contents[path] for path in sorted(contents))), rows)


def _read(path: Path) -> bytes:
    from aegis_alpha.data.descriptor_tree import (  # noqa: PLC0415 -- lazy alias-safe reads
        DescriptorTree,
        DescriptorTreeError,
    )

    path = path.absolute()
    try:
        with DescriptorTree.open_path(path.parent) as tree:
            return tree.read_bytes(path.name, max_bytes=MAX_FILE_BYTES)
    except (OSError, DescriptorTreeError) as error:
        raise ValueError(f"cannot read bounded regular file {path.name}") from error


def read_kind_receipt(path: Path) -> SourceUnit:
    """Read a KIND receipt and the response file beside it that the receipt names."""
    receipt = _read(path)
    reference = _json(receipt, "KIND receipt").get("raw")
    relative = reference.get("relative_path") if isinstance(reference, dict) else None
    if not isinstance(relative, str) or not relative:
        raise ValueError("KIND receipt names no response file")
    return kind_unit(receipt, _read(path.parent / relative.rsplit("/", 1)[-1]))


def read_eodhd_job(directory: Path) -> SourceUnit:
    """Read one collected job directory: its ``complete.json`` and the files it lists."""
    complete = _read(directory / "complete.json")
    listed = _json(complete, "EODHD symbol job completion").get("files")
    if not isinstance(listed, list):
        raise ValueError("EODHD symbol job completion lists no files")  # noqa: TRY004 -- malformed-content ValueError contract
    files: dict[str, bytes] = {}
    for item in cast("list[object]", listed):
        path = item.get("path") if isinstance(item, dict) else None
        if not isinstance(path, str) or not path:
            raise ValueError("EODHD symbol job completion lists a malformed file")
        files[path] = _read(directory / path.rsplit("/", 1)[-1])
    return eodhd_unit(complete, files)


def import_unit(workspace: Workspace, unit: SourceUnit) -> dict[str, object]:
    """Retain a unit's files in ``raw/`` and commit its table as a content source."""
    from aegis_alpha.storage.raw import put_raw  # noqa: PLC0415 -- lazy storage imports
    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    for raw in unit.files:
        put_raw(workspace.paths.raw, raw)
    try:
        table = unit.arrow()
    except ImportError:
        raise ValueError(
            "aas identity kr-import commits source tables through pyarrow; install the legacy extra"
        ) from None
    result = import_content_arrow(
        workspace,
        unit.content,
        unit.table,
        table.to_reader(),
        lineage={"loader": "aas identity kr-import", "parser": "kr-identity-sources-v1"},
    )
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    return {
        "source_id": unit.content.source_id,
        "table": unit.table,
        "rows": committed["rows"],
        "digest": committed["digest"],
        "reused": bool(result.get("reused", False)),
    }


# --- source rows --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceRows:
    """The rows of one pinned source table, cited by its ``sl:`` snapshot ID."""

    snapshot_id: str
    columns: tuple[str, ...]
    rows: tuple[Row, ...]

    def records(self) -> list[tuple[Mapping[str, object], str]]:
        """Each row by column name with its ``aas-source-row-v1`` hash."""
        return [
            (
                dict(zip(self.columns, row, strict=True)),
                formats.source_row_hash(list(zip(self.columns, row, strict=True))),
            )
            for row in self.rows
        ]


def pinned_rows(
    workspace: Workspace,
    source_id: str,
    table: str,
    keep: Callable[[Mapping[str, object]], bool] | None = None,
) -> SourceRows:
    """Verify a committed source table against its marker and read its rows.

    ``keep`` selects rows batch by batch, so a wide table (the DART receipts that hold one
    corp-code archive among many financial statements) is never held whole.
    """
    from aegis_alpha.storage.source_library import list_tables, source_entry  # noqa: PLC0415
    from aegis_alpha.storage.source_reader import SourcePin, iter_source_rows  # noqa: PLC0415

    source = source_entry(workspace, source_id)
    if source is None:
        raise ValueError(f"unknown or incomplete source {source_id}")
    described = next(
        (row for row in list_tables(workspace, source_id) if row["name"] == table), None
    )
    if described is None:
        raise ValueError(f"source {source_id} has no table {table}")
    pin = SourcePin(source_id, str(source["sha256"]), table, str(described["digest"]))
    columns = tuple(cast("list[str]", described["columns"]))
    rows: list[Row] = []
    try:
        for batch in iter_source_rows(workspace, pin, batch_size=_READ_BATCH):
            rows.extend(
                tuple(item[name] for name in columns)
                for item in batch
                if keep is None or keep(item)
            )
    except ImportError:
        raise ValueError(
            "reading an Arrow source table needs pyarrow; install the legacy extra"
        ) from None
    return SourceRows(LINK_PREFIX + source_id, columns, tuple(rows))


# --- mappers ------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Evidence:
    """One accepted source claim, with the row and time that support it."""

    source: str
    source_hash: str
    known_from_us: int

    def order(self) -> tuple[int, str, str]:
        return (self.known_from_us, self.source, self.source_hash)


@dataclass(slots=True)
class MapperReport:
    rows: int = 0
    accepted: int = 0
    skipped: dict[str, int] = field(default_factory=dict)
    refused: dict[str, int] = field(default_factory=dict)

    def refuse(self, reason: str) -> None:
        self.refused[reason] = self.refused.get(reason, 0) + 1

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1

    def merge(self, other: MapperReport) -> None:
        self.rows += other.rows
        self.accepted += other.accepted
        for mine, theirs in ((self.skipped, other.skipped), (self.refused, other.refused)):
            for reason, count in theirs.items():
                mine[reason] = mine.get(reason, 0) + count

    def json(self) -> dict[str, object]:
        return {
            "rows": self.rows,
            "accepted": self.accepted,
            "skipped": dict(sorted(self.skipped.items())),
            "refused": dict(sorted(self.refused.items())),
        }


def _require(rows: SourceRows, columns: Sequence[str], mapper: str) -> None:
    missing = sorted(set(columns) - set(rows.columns))
    if missing:
        raise ValueError(f"{mapper} source {rows.snapshot_id} lacks columns {missing}")


@dataclass(frozen=True, slots=True)
class SymbolClaim:
    symbol: str
    code: str
    isin: str
    asset_type: str
    evidence: Evidence


@dataclass(frozen=True, slots=True)
class SymbolRow:
    """One KR symbol-list row as the list states it, with the reason it cannot mint."""

    symbol: str
    code: str
    isin: str | None
    """The ``Isin`` cell, or None when the list leaves it blank."""
    asset_type: str | None
    currency: object
    type_label: object
    evidence: Evidence
    reason: str | None

    def statement(self) -> tuple[object, ...]:
        """What the row says about its symbol; two lists that differ here disagree."""
        return (self.isin, self.asset_type or f"unknown:{self.type_label}", self.currency)

    def claim(self) -> SymbolClaim:
        return SymbolClaim(
            self.symbol, self.code, cast("str", self.isin), cast("str", self.asset_type),
            self.evidence,
        )  # fmt: skip


def _isin(value: object) -> tuple[str | None, str | None]:
    if value is None or value == "":
        return None, "isin_missing"
    try:
        canonical = normalize_identifier(IdentifierType.ISIN, cast("str", value))
    except IdentifierValueError:
        return None, "isin_invalid"
    if canonical != value:
        return None, "isin_invalid"
    return canonical, None


def map_eodhd_symbols(sources: Sequence[SourceRows]) -> tuple[list[SymbolRow], MapperReport]:
    """``eodhd.kr_symbol@1``: KR symbol-list rows, each with the reason it cannot mint.

    A row mints when its ``Isin`` is an ISIN with a valid check digit, its ``Type`` is
    known and its currency is KRW. Whether its symbol resolves is decided over every list
    together (``_resolve_symbols``). Rows of an exchange other than ``KO``/``KQ`` are
    skipped, not refused.
    """
    report = MapperReport()
    rows: list[SymbolRow] = []
    for source in sources:
        _require(source, ("code", "exchange", "currency", "type", "isin", "retrieved_at_utc"),
                 EODHD_MAPPER)  # fmt: skip
        for row, row_hash in source.records():
            report.rows += 1
            code, exchange = row["code"], row["exchange"]
            if not isinstance(code, str) or not code or exchange not in KR_EXCHANGES:
                report.skip("exchange_not_kr")
                continue
            _, reason = _isin(row["isin"])
            asset_type = ASSET_TYPES.get(cast("str", row["type"]))
            if reason is None and asset_type is None:
                reason = "type_unknown"
            if reason is None and row["currency"] != "KRW":
                reason = "currency_not_krw"
            if reason is None:
                report.accepted += 1
            else:
                report.refuse(reason)
            stated = row["isin"]
            rows.append(
                SymbolRow(
                    f"{code}.{exchange}",
                    code,
                    None if stated is None or stated == "" else cast("str", stated),
                    asset_type,
                    row["currency"],
                    row["type"],
                    Evidence(source.snapshot_id, row_hash, instant_us(row["retrieved_at_utc"])),
                    reason,
                )
            )
    return rows, report


@dataclass(frozen=True, slots=True)
class ListingClaim:
    code: str
    listed_on: date
    evidence: Evidence


def map_kind_listings(sources: Sequence[SourceRows]) -> tuple[list[ListingClaim], MapperReport]:
    """``kind.listings@1``: listed-company rows to short codes with their listing dates."""
    report = MapperReport()
    claims: list[ListingClaim] = []
    for source in sources:
        _require(source, KIND_REQUIRED, KIND_MAPPER)
        for row, row_hash in source.records():
            report.rows += 1
            code, listed = row["short_code"], row["listed_on"]
            if not isinstance(code, str) or _SHORT_CODE.fullmatch(code) is None:
                report.refuse("short_code_invalid")
                continue
            if not isinstance(listed, str) or _DAY.fullmatch(listed) is None:
                report.refuse("listed_on_invalid")
                continue
            try:
                day = date.fromisoformat(listed)
            except ValueError:
                report.refuse("listed_on_invalid")
                continue
            report.accepted += 1
            evidence = Evidence(source.snapshot_id, row_hash, instant_us(row["retrieved_at_utc"]))
            claims.append(ListingClaim(code, day, evidence))
    return claims, report


@dataclass(frozen=True, slots=True)
class CorpClaim:
    corp_code: str
    name: str
    stock_code: str
    evidence: Evidence


def _corp_xml(row: Mapping[str, object]) -> bytes:
    encoded = row["raw_base64"]
    if not isinstance(encoded, str):
        raise ValueError("DART corp code receipt holds no response bytes")  # noqa: TRY004 -- malformed-content ValueError contract
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError:
        raise ValueError("DART corp code receipt bytes are not base64") from None
    if not isinstance(row["raw_sha256"], str) or _SHA256.fullmatch(row["raw_sha256"]) is None:
        raise ValueError("DART corp code receipt records no SHA-256")
    if hashlib.sha256(raw).hexdigest() != row["raw_sha256"]:
        raise ValueError("DART corp code response bytes differ from their recorded SHA-256")
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
        (member,) = archive.infolist()
    except (zipfile.BadZipFile, ValueError):
        raise ValueError("DART corp code response is not a one-file zip archive") from None
    if member.filename != "CORPCODE.xml" or member.file_size > MAX_XML_BYTES:
        raise ValueError("DART corp code archive must hold one bounded CORPCODE.xml")
    with archive.open(member) as handle:
        xml = handle.read(MAX_XML_BYTES + 1)
    if len(xml) > MAX_XML_BYTES:
        raise ValueError("DART corp code archive exceeds its bound")
    return xml


def _issuer_name(value: str | None) -> str | None:
    if value is None or not value or value.strip() != value or not value.isprintable():
        return None
    return value


def map_dart_corp_codes(source: SourceRows) -> tuple[list[CorpClaim], MapperReport]:
    """``dart.corp_codes@1``: the one completed ``corp_codes`` receipt to listed corps.

    A corp with a blank stock code is not listed and is skipped. A listed corp whose name
    is not trimmed printable text is refused rather than renamed.
    """
    _require(source, ("endpoint", "outcome", "raw_base64", "raw_sha256", "retrieved_at_utc"),
             DART_MAPPER)  # fmt: skip
    receipts = [
        (row, row_hash) for row, row_hash in source.records() if row["endpoint"] == "corp_codes"
    ]
    if len(receipts) != 1:
        raise ValueError(f"{DART_MAPPER} needs exactly one corp_codes receipt in its source")
    ((row, row_hash),) = receipts
    if row["outcome"] != "COMPLETED":
        raise ValueError("DART corp code receipt did not complete")
    evidence = Evidence(source.snapshot_id, row_hash, instant_us(row["retrieved_at_utc"]))
    try:
        root = ET.fromstring(_corp_xml(row))  # noqa: S314 -- size-bounded provider document
    except ET.ParseError:
        raise ValueError("DART CORPCODE.xml is not well-formed XML") from None
    report = MapperReport()
    claims: list[CorpClaim] = []
    for item in root.findall("list"):
        report.rows += 1
        corp_code = item.findtext("corp_code")
        stock_code = (item.findtext("stock_code") or "").strip()
        if not stock_code:
            report.skip("not_listed")
            continue
        if corp_code is None or re.fullmatch(r"[0-9]{8}", corp_code) is None:
            report.refuse("corp_code_invalid")
            continue
        if _SHORT_CODE.fullmatch(stock_code) is None:
            report.refuse("stock_code_invalid")
            continue
        name = _issuer_name(item.findtext("corp_name"))
        if name is None:
            report.refuse("corp_name_invalid")
            continue
        report.accepted += 1
        claims.append(CorpClaim(corp_code, name, stock_code, evidence))
    return claims, report


# --- registry -----------------------------------------------------------------------------


def _anchor(namespace: str, token: str) -> dict[str, object]:
    return {"anchor_namespace": namespace, "anchor_token": token}


def _assertion(
    isin: str, key: tuple[str, str, str], valid_from_us: int, evidence: Evidence
) -> dict[str, object]:
    """One open-ended assertion of the provider key ``(provider, namespace, token)``."""
    provider, namespace, token = key
    return {
        "instrument": _anchor("krx_isin", isin),
        "provider": provider,
        "namespace": namespace,
        "token": token,
        "valid_from_us": valid_from_us,
        "valid_to_us": None,
        "known_from_us": evidence.known_from_us,
        "supersedes_assertion_id": None,
        "source_snapshot_id": evidence.source,
        "source_hash": evidence.source_hash,
    }


def _first(items: Sequence[Evidence]) -> Evidence:
    return min(items, key=Evidence.order)


@dataclass(slots=True)
class KrRegistry:
    """A built registry document and why every unresolved key stayed unresolved."""

    document: dict[str, object]
    mappers: dict[str, MapperReport]
    symbols: dict[str, str]
    """Each KR EODHD symbol seen: its minted instrument ID, or ``unresolved:<reason>``."""
    unresolved: dict[str, dict[str, list[str]]]
    sources: frozenset[str] = frozenset()
    """The ``sl:`` IDs of every source the build read."""
    withdrawn: list[dict[str, object]] = field(default_factory=list)
    """Registered KR assertions, not yet corrected, that these sources no longer give."""

    def raw(self) -> bytes:
        return formats.canonical(self.document)

    def sha256(self) -> str:
        return hashlib.sha256(self.raw()).hexdigest()

    def resolve(self, symbols: Sequence[str], *, sample: int | None = None) -> dict[str, object]:
        """Classify provider symbols (for example a price history's) by this registry."""
        resolved = 0
        reasons: dict[str, list[str]] = defaultdict(list)
        for symbol in sorted(set(symbols)):
            status = self.symbols.get(symbol, "unresolved:not_in_symbol_list")
            if status.startswith("ins-"):
                resolved += 1
            else:
                reasons[status.removeprefix("unresolved:")].append(symbol)
        total = len(set(symbols))
        return {
            "symbols": total,
            "resolved": resolved,
            "resolved_ratio": None if total == 0 else round(resolved / total, 6),
            "unresolved_count": {reason: len(found) for reason, found in sorted(reasons.items())},
            "unresolved": {
                reason: found if sample is None else found[:sample]
                for reason, found in sorted(reasons.items())
            },
        }

    def report(self, *, sample: int | None = 100) -> dict[str, object]:
        def cut(values: list[str]) -> list[str]:
            return values if sample is None else values[:sample]

        assertions = cast("list[dict[str, object]]", self.document["assertions"])
        kinds: dict[str, int] = defaultdict(int)
        for row in assertions:
            kinds[f"{row['provider']}/{row['namespace']}"] += 1
        return {
            "sha256": self.sha256(),
            "issuers": len(cast("list[object]", self.document["issuers"])),
            "instruments": len(cast("list[object]", self.document["instruments"])),
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
            "withdrawn_count": len(self.withdrawn),
            "withdrawn": self.withdrawn if sample is None else self.withdrawn[:sample],
        }


@dataclass(frozen=True, slots=True)
class _Symbols:
    """Symbol claims grouped into accepted ISINs and unambiguous short codes."""

    accepted: dict[str, list[SymbolClaim]]
    by_code: dict[str, str]
    status: dict[str, str]


def _accepted(
    by_isin: Mapping[str, list[SymbolClaim]], unresolved: dict[str, list[str]]
) -> dict[str, list[SymbolClaim]]:
    """ISINs whose claims agree on one short code and one asset type."""
    accepted: dict[str, list[SymbolClaim]] = {}
    for isin, group in sorted(by_isin.items()):
        codes = {claim.code for claim in group if _SHORT_CODE.fullmatch(claim.code)}
        if len(codes) > 1 or len({claim.asset_type for claim in group}) > 1:
            unresolved["isin_ambiguous"].append(isin)
        else:
            accepted[isin] = group
    return accepted


def _codes(
    stated: Mapping[str, list[SymbolClaim]],
    accepted: Mapping[str, object],
    unresolved: dict[str, list[str]],
) -> dict[str, str]:
    """Short codes every mintable row ties to one ISIN, when that ISIN is accepted."""
    isins_of: dict[str, set[str]] = defaultdict(set)
    for isin, group in stated.items():
        for claim in group:
            if _SHORT_CODE.fullmatch(claim.code):
                isins_of[claim.code].add(isin)
    by_code: dict[str, str] = {}
    for code, isins in sorted(isins_of.items()):
        if len(isins) > 1:
            unresolved["short_code_ambiguous"].append(code)
        elif (isin := next(iter(isins))) in accepted:
            by_code[code] = isin
    return by_code


def _symbol_claims(group: Sequence[SymbolRow]) -> tuple[list[SymbolClaim], str | None]:
    """One symbol's rows across every list: its claims, or why it stays unresolved.

    A blank ``Isin`` defers to the lists that name one. Lists that name an ISIN must
    agree on it, on the asset type and on the currency; any disagreement leaves the
    symbol ``symbol_ambiguous`` rather than choosing a side. An agreed statement that
    cannot mint keeps its own reason.
    """
    named = [row for row in group if row.isin is not None]
    if not named:
        return [], "isin_missing"
    if len({row.statement() for row in named}) > 1:
        return [], "symbol_ambiguous"
    if named[0].reason is not None:
        return [], named[0].reason
    return [row.claim() for row in named], None


def _group_symbols(
    rows: Sequence[SymbolRow], unresolved: Mapping[str, dict[str, list[str]]]
) -> _Symbols:
    """A symbol the lists disagree on, an ISIN with two short codes or asset types, and a
    short code with two ISINs are each ambiguous; the claims that remain mint instruments."""
    per_symbol: dict[str, list[SymbolRow]] = defaultdict(list)
    for row in rows:
        per_symbol[row.symbol].append(row)
    reasons: dict[str, str] = {}
    resolved: dict[str, list[SymbolClaim]] = defaultdict(list)
    for symbol, group in per_symbol.items():
        claims, reason = _symbol_claims(group)
        if reason is not None:
            reasons[symbol] = reason
        for claim in claims:
            resolved[claim.isin].append(claim)
    # Every mintable row speaks to its ISIN, even one whose symbol stays unresolved.
    stated: dict[str, list[SymbolClaim]] = defaultdict(list)
    for row in rows:
        if row.reason is None:
            stated[cast("str", row.isin)].append(row.claim())
    agreed = _accepted(stated, unresolved["isins"])
    accepted = {isin: resolved[isin] for isin in sorted(agreed) if resolved.get(isin)}
    by_code = _codes(stated, accepted, unresolved["short_codes"])
    status: dict[str, str] = {}
    for symbol, group in sorted(per_symbol.items()):
        reason = reasons.get(symbol)
        isin = next((row.isin for row in group if row.isin is not None), None)
        if reason is None and isin not in accepted:
            reason = "isin_ambiguous"
        if reason is None:
            status[symbol] = mint_instrument("krx_isin", cast("str", isin))
        else:
            status[symbol] = "unresolved:" + reason
            unresolved["symbols"][reason].append(symbol)
    return _Symbols(accepted, by_code, status)


def _dart(
    claims: Sequence[CorpClaim], symbols: _Symbols, unresolved: dict[str, list[str]]
) -> tuple[list[Record], dict[str, tuple[str, Evidence]]]:
    """Issuers and the instrument each listed corp's stock code reaches, by ISIN.

    Receipts collected at different times are read together: a stock code two corps
    claim, or a corp that names two stock codes, stays unresolved. A corp's name and
    evidence are its earliest receipt's.
    """
    per_stock: dict[str, list[CorpClaim]] = defaultdict(list)
    stocks_of: dict[str, set[str]] = defaultdict(set)
    for claim in claims:
        per_stock[claim.stock_code].append(claim)
        stocks_of[claim.corp_code].add(claim.stock_code)
    issuers: list[Record] = []
    links: dict[str, tuple[str, Evidence]] = {}
    for stock, group in sorted(per_stock.items()):
        if len({claim.corp_code for claim in group}) > 1:
            unresolved["stock_code_ambiguous"].append(stock)
            continue
        claim = min(group, key=lambda item: item.evidence.order())
        if len(stocks_of[claim.corp_code]) > 1:
            unresolved["corp_code_ambiguous"].append(stock)
            continue
        isin = symbols.by_code.get(stock)
        if isin is None:
            unresolved["short_code_unresolved"].append(stock)
            continue
        if symbols.accepted[isin][0].asset_type == "etf":
            unresolved["stock_code_is_etf"].append(stock)
            continue
        issuers.append({**_anchor("dart_corp_code", claim.corp_code), "name": claim.name})
        links[isin] = (claim.corp_code, claim.evidence)
    return issuers, links


def _kind(
    claims: Sequence[ListingClaim], symbols: _Symbols, unresolved: dict[str, list[str]]
) -> list[Record]:
    """One ``kind`` short-code assertion per listed code that reaches one instrument."""
    per_code: dict[str, list[ListingClaim]] = defaultdict(list)
    for claim in claims:
        per_code[claim.code].append(claim)
    assertions = []
    for code, group in sorted(per_code.items()):
        if len({claim.listed_on for claim in group}) > 1:
            unresolved["listing_ambiguous"].append(code)
            continue
        isin = symbols.by_code.get(code)
        if isin is None:
            unresolved["short_code_unresolved"].append(code)
            continue
        first = _first([claim.evidence for claim in group])
        listed = local_midnight_us(group[0].listed_on)
        assertions.append(_assertion(isin, ("kind", "krx_short_code", code), listed, first))
    return assertions


def _eodhd_assertions(symbols: _Symbols, links: Mapping[str, tuple[str, Evidence]]) -> list[Record]:
    assertions: list[Record] = []
    for isin, group in symbols.accepted.items():
        per_symbol: dict[str, list[Evidence]] = defaultdict(list)
        for claim in group:
            per_symbol[claim.symbol].append(claim.evidence)
        for symbol, evidence in sorted(per_symbol.items()):
            assertions.append(
                _assertion(isin, ("eodhd", "eodhd_symbol", symbol), UNBOUNDED, _first(evidence))
            )
        coded = [claim for claim in group if symbols.by_code.get(claim.code) == isin]
        if coded:
            first = _first([claim.evidence for claim in coded])
            assertions.append(
                _assertion(isin, ("eodhd", "krx_short_code", coded[0].code), UNBOUNDED, first)
            )
        if isin in links:
            corp_code, evidence = links[isin]
            token = issuer_link_token(
                mint_issuer("dart_corp_code", corp_code), mint_instrument("krx_isin", isin)
            )
            assertions.append(_assertion(isin, ("dart", "issuer", token), UNBOUNDED, evidence))
    return assertions


def build_kr_registry(
    eodhd: Sequence[SourceRows],
    kind: Sequence[SourceRows] = (),
    dart: Sequence[SourceRows] = (),
) -> KrRegistry:
    """Build the KR ``aas-identity-registry-v1`` document from pinned source rows.

    The document lists issuers by corp code, instruments by ISIN and assertions by
    provider, namespace and token, so the same sources always give the same bytes.
    """
    unresolved: dict[str, dict[str, list[str]]] = {
        key: defaultdict(list) for key in ("symbols", "isins", "short_codes", "kind", "dart")
    }
    symbol_rows, eodhd_report = map_eodhd_symbols(eodhd)
    symbols = _group_symbols(symbol_rows, unresolved)
    mappers = {EODHD_MAPPER: eodhd_report}
    issuers: list[Record] = []
    links: dict[str, tuple[str, Evidence]] = {}
    if dart:
        corps: list[CorpClaim] = []
        dart_report = MapperReport()
        for source in dart:
            found, report = map_dart_corp_codes(source)
            corps.extend(found)
            dart_report.merge(report)
        mappers[DART_MAPPER] = dart_report
        issuers, links = _dart(corps, symbols, unresolved["dart"])
    listings, mappers[KIND_MAPPER] = map_kind_listings(kind)
    instruments = [
        {
            **_anchor("krx_isin", isin),
            "issuer": None if isin not in links else _anchor("dart_corp_code", links[isin][0]),
            "asset_type": group[0].asset_type,
            "venue": VENUE,
        }
        for isin, group in symbols.accepted.items()
    ]
    assertions = [
        *_eodhd_assertions(symbols, links),
        *_kind(listings, symbols, unresolved["kind"]),
    ]
    document: dict[str, object] = {
        "schema": REGISTRY_SCHEMA,
        "issuers": sorted(issuers, key=lambda row: cast("str", row["anchor_token"])),
        "instruments": instruments,
        "assertions": sorted(
            assertions,
            key=lambda row: (str(row["provider"]), str(row["namespace"]), str(row["token"])),
        ),
    }
    parse_registry(document)
    return KrRegistry(
        document,
        mappers,
        symbols.status,
        {
            key: {reason: sorted(values) for reason, values in groups.items() if values}
            for key, groups in unresolved.items()
        },
        frozenset(source.snapshot_id for source in (*eodhd, *kind, *dart)),
    )


# Registered assertions the KR registry owns: KIND short codes, DART issuer links, EODHD
# short codes and EODHD symbols of the two KRX markets.
_KR_ASSERTIONS: Final = (
    "SELECT a.assertion_id,a.provider,a.namespace,a.token,a.source_snapshot_id,"
    "EXISTS(SELECT 1 FROM identity_assertions c WHERE c.supersedes_assertion_id=a.assertion_id)"
    " FROM identity_assertions a WHERE a.provider='kind'"
    " OR (a.provider='dart' AND a.namespace='issuer')"
    " OR (a.provider='eodhd' AND (a.namespace='krx_short_code'"
    " OR (a.namespace='eodhd_symbol' AND (a.token LIKE '%.KO' OR a.token LIKE '%.KQ'))))"
    " ORDER BY a.provider,a.namespace,a.token,a.assertion_id"
)


def check_registered(registry: KrRegistry, state: sqlite3.Connection) -> KrRegistry:
    """Hold a build to the KR assertions already registered.

    The build input is cumulative: every source a registered KR assertion cites must be
    among the build's sources, so a claim registered before is rebuilt from the same
    earliest evidence with the same assertion ID instead of conflicting with itself. A
    build missing one is refused, naming the sources to add. A registered claim that is
    not corrected and that the sources no longer give (a later list made its key
    ambiguous) is reported in ``withdrawn``; the builder never closes it, because no
    source says when it stopped holding, so a correction is registered separately.
    """
    sources = registry.sources
    built = {
        cast("str", row["assertion_id"]) for row in parse_registry(registry.document).assertions
    }
    missing: set[str] = set()
    withdrawn: list[dict[str, object]] = []
    for assertion_id, provider, namespace, token, source, corrected in state.execute(
        _KR_ASSERTIONS
    ):
        if source not in sources:
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
            "the KR registry build must include every source its registered assertions "
            f"cite; add {sorted(missing)}"
        )
    registry.withdrawn = withdrawn
    return registry


def build_from_workspace(
    workspace: Workspace,
    *,
    eodhd: Sequence[str],
    kind: Sequence[str] = (),
    dart: Sequence[str] = (),
) -> KrRegistry:
    """Build the registry from committed sources named by their source IDs.

    The sources must include every source the workspace's registered KR assertions cite
    (``check_registered``).
    """
    if not eodhd:
        raise ValueError("the KR registry needs at least one EODHD symbol source")
    registry = build_kr_registry(
        [pinned_rows(workspace, source, EODHD_TABLE) for source in eodhd],
        [pinned_rows(workspace, source, KIND_TABLE) for source in kind],
        [
            pinned_rows(workspace, source, DART_TABLE, lambda row: row["endpoint"] == "corp_codes")
            for source in dart
        ],
    )
    return check_registered(registry, workspace.state)
