"""OpenDART requests, response outcomes and the HTTP client the KR collector calls.

A request is an endpoint and its exact parameters, nothing else. Its fingerprint hashes
``aas-opendart-request-v1`` with the endpoint and the canonical parameter JSON, so asking
the same question on another day is another attempt of the same request rather than a new
request (the legacy cohort hashed its observation date in, which froze its job list).

Endpoints:

- ``corp_codes`` (``corpCode.xml``): the zipped list of every corp code; no parameters.
- ``financials`` (``fnlttSinglAcntAll.json``): one company's full statements for a
  business year, report (``11013`` first quarter, ``11012`` half year, ``11014`` third
  quarter, ``11011`` annual) and ``fs_div`` (``CFS`` consolidated, ``OFS`` separate).
- ``list`` (``list.json``): one page of the periodic-report (``pblntf_ty=A``) disclosure
  list for a date window, every filing (``last_reprt_at=N``) including amendments.

A response's outcome is ``COMPLETED`` (the provider answered with content), ``NO_DATA``
(provider status ``013``) or ``FAILED`` (any other answer). The outcome only routes the
collector; the response bytes are always retained and the promotion mappers judge them.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from typing import Final, Protocol, cast
from xml.etree import ElementTree as ET

REQUEST_FORMAT: Final = "aas-opendart-request-v1"
CORP_CODES: Final = "corp_codes"
FINANCIALS: Final = "financials"
LIST: Final = "list"
ENDPOINTS: Final[Mapping[str, str]] = {
    CORP_CODES: "https://opendart.fss.or.kr/api/corpCode.xml",
    FINANCIALS: "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
    LIST: "https://opendart.fss.or.kr/api/list.json",
}
# Report code -> the month its period ends in a December fiscal year.
REPORT_END_MONTH: Final[Mapping[str, int]] = {
    "11013": 3,
    "11012": 6,
    "11014": 9,
    "11011": 12,
}
FS_DIVS: Final = ("CFS", "OFS")
FIRST_YEAR: Final = 2015
COMPLETED: Final = "COMPLETED"
NO_DATA: Final = "NO_DATA"
FAILED: Final = "FAILED"
OUTCOMES: Final = frozenset({COMPLETED, NO_DATA, FAILED})
# Provider statuses after which no further call in the run can succeed: an unknown,
# unauthorised, blocked-IP or expired key, and the daily request or company limits.
STOP_STATUSES: Final = frozenset({"010", "011", "012", "020", "021", "901"})
STOP_HTTP: Final = frozenset({401, 403, 429})
LIST_PAGE_COUNT: Final = "100"
# OpenDART refuses a list query without a corp code over a window longer than three months.
MAX_LIST_WINDOW_DAYS: Final = 92
MAX_RESPONSE_BYTES: Final = 64 * 1024 * 1024
MAX_XML_BYTES: Final = 256 * 1024 * 1024
_RETAINED_HEADERS: Final = frozenset({"content-type", "date", "retry-after"})
_HTTP_OK: Final = 200
_CORP: Final = re.compile(r"[0-9]{8}")
_YEAR: Final = re.compile(r"[0-9]{4}")
_DAY: Final = re.compile(r"[0-9]{8}")
_PAGE: Final = re.compile(r"[1-9][0-9]{0,5}")
_STATUS: Final = re.compile(r"[0-9]{3}")
_RECEIPT: Final = re.compile(r"[0-9]{14}")
_STOCK: Final = re.compile(r"[0-9A-Z]{6}")
_LIST_KEYS: Final = frozenset(
    {"bgn_de", "end_de", "last_reprt_at", "page_count", "page_no", "pblntf_ty"}
)
_FINANCIAL_KEYS: Final = frozenset({"bsns_year", "corp_code", "fs_div", "reprt_code"})
# A periodic report's name: optional bracketed amendment prefixes, the report kind and
# the period it covers as ``(YYYY.MM)``.
_REPORT_NAME: Final = re.compile(
    r"(?:\[[^\]]+\]\s*)*(사업보고서|반기보고서|분기보고서)\s*\((\d{4})\.(\d{2})\)"
)
_REPORT_KINDS: Final[Mapping[tuple[str, int], str]] = {
    ("분기보고서", 3): "11013",
    ("반기보고서", 6): "11012",
    ("분기보고서", 9): "11014",
    ("사업보고서", 12): "11011",
}


def canonical(value: object) -> str:
    """Sorted-key, compact, ASCII-escaped JSON: the one spelling every hash reads."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _day(text: str, name: str) -> date:
    if _DAY.fullmatch(text) is None:
        raise ValueError(f"{name} must be YYYYMMDD")
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    except ValueError:
        raise ValueError(f"{name} must be a calendar date") from None


def _check_financials(parameters: Mapping[str, str]) -> None:
    if set(parameters) != _FINANCIAL_KEYS:
        raise ValueError("financials takes corp_code, bsns_year, reprt_code and fs_div")
    if _CORP.fullmatch(parameters["corp_code"]) is None:
        raise ValueError("corp_code must be eight digits")
    year = parameters["bsns_year"]
    if _YEAR.fullmatch(year) is None or int(year) < FIRST_YEAR:
        raise ValueError(f"bsns_year must be a year from {FIRST_YEAR}")
    if parameters["reprt_code"] not in REPORT_END_MONTH:
        raise ValueError("reprt_code must be 11011, 11012, 11013 or 11014")
    if parameters["fs_div"] not in FS_DIVS:
        raise ValueError("fs_div must be CFS or OFS")


def _check_list(parameters: Mapping[str, str]) -> None:
    if set(parameters) != _LIST_KEYS:
        raise ValueError(f"list takes exactly {sorted(_LIST_KEYS)}")
    begin = _day(parameters["bgn_de"], "bgn_de")
    end = _day(parameters["end_de"], "end_de")
    if not begin <= end <= begin + timedelta(days=MAX_LIST_WINDOW_DAYS - 1):
        raise ValueError(f"list window must run forward at most {MAX_LIST_WINDOW_DAYS} days")
    if (parameters["pblntf_ty"], parameters["last_reprt_at"]) != ("A", "N"):
        raise ValueError("list collects every periodic report filing (pblntf_ty A, last N)")
    if parameters["page_count"] != LIST_PAGE_COUNT:
        raise ValueError("list pages hold 100 filings")
    if _PAGE.fullmatch(parameters["page_no"]) is None:
        raise ValueError("list pages are numbered from 1")


def _check(endpoint: str, parameters: Mapping[str, str]) -> None:
    if endpoint not in ENDPOINTS:
        raise ValueError(f"unsupported OpenDART endpoint {endpoint!r}")
    if any(not isinstance(key, str) or not isinstance(v, str) for key, v in parameters.items()):
        raise TypeError("OpenDART parameters must be text")
    if endpoint == FINANCIALS:
        _check_financials(parameters)
    elif endpoint == LIST:
        _check_list(parameters)
    elif parameters:
        raise ValueError("corp_codes takes no parameters")


@dataclass(frozen=True, slots=True)
class DartRequest:
    """One OpenDART question: an endpoint and its exact parameters."""

    endpoint: str
    parameters_json: str

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint, str) or not isinstance(self.parameters_json, str):
            raise TypeError("an OpenDART request is an endpoint and its parameter JSON")
        try:
            parameters = json.loads(self.parameters_json)
        except json.JSONDecodeError:
            raise ValueError("OpenDART parameters must be JSON") from None
        if not isinstance(parameters, dict):
            raise ValueError("OpenDART parameters must be a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
        if canonical(parameters) != self.parameters_json:
            raise ValueError("OpenDART parameters must be canonical JSON")
        _check(self.endpoint, cast("dict[str, str]", parameters))

    @classmethod
    def of(cls, endpoint: str, parameters: Mapping[str, str] | None = None) -> DartRequest:
        return cls(endpoint, canonical(dict(parameters or {})))

    @classmethod
    def financials(cls, corp_code: str, year: int, report: str, fs_div: str) -> DartRequest:
        return cls.of(
            FINANCIALS,
            {"bsns_year": f"{year:04d}", "corp_code": corp_code, "fs_div": fs_div,
             "reprt_code": report},
        )  # fmt: skip

    @classmethod
    def list_page(cls, day: date, page: int) -> DartRequest:
        stamp = day.strftime("%Y%m%d")
        return cls.of(
            LIST,
            {"bgn_de": stamp, "end_de": stamp, "last_reprt_at": "N",
             "page_count": LIST_PAGE_COUNT, "page_no": str(page), "pblntf_ty": "A"},
        )  # fmt: skip

    @property
    def parameters(self) -> dict[str, str]:
        return cast("dict[str, str]", json.loads(self.parameters_json))

    @property
    def document(self) -> dict[str, str]:
        """The request as the receipt and the source row record it."""
        return {"endpoint": self.endpoint, "parameters_json": self.parameters_json}

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            canonical([REQUEST_FORMAT, self.endpoint, self.parameters_json]).encode()
        ).hexdigest()

    @classmethod
    def from_document(cls, document: object) -> DartRequest:
        """A receipt's request, ignoring the observation date a legacy request carried."""
        if not isinstance(document, Mapping):
            raise ValueError("an OpenDART request document is a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
        body = cast("Mapping[str, object]", document)
        if not set(body) <= {"endpoint", "parameters_json", "observation_date"}:
            raise ValueError("an OpenDART request document holds unknown fields")
        endpoint, parameters = body.get("endpoint"), body.get("parameters_json")
        if not isinstance(endpoint, str) or not isinstance(parameters, str):
            raise ValueError("an OpenDART request document names its endpoint and parameters")  # noqa: TRY004 -- malformed-content ValueError contract
        return cls(endpoint, parameters)


@dataclass(frozen=True, slots=True)
class DartResponse:
    """One provider answer: HTTP status, retained headers, bytes and the call's instants."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    requested_at: datetime
    retrieved_at: datetime

    def __post_init__(self) -> None:
        if type(self.status) is not int or not 100 <= self.status <= 599:  # noqa: PLR2004 -- HTTP status range
            raise ValueError("invalid HTTP status")
        if type(self.body) is not bytes or len(self.body) > MAX_RESPONSE_BYTES:
            raise ValueError("response body must be bounded bytes")
        for moment in (self.requested_at, self.retrieved_at):
            if moment.tzinfo is None or moment.utcoffset() is None:
                raise ValueError("response instants must be timezone-aware")
        if self.retrieved_at < self.requested_at:
            raise ValueError("a response is retrieved after it is requested")


def _json_status(body: bytes) -> tuple[str | None, Mapping[str, object] | None]:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None
    if not isinstance(value, dict):
        return None, None
    status = value.get("status")
    if not isinstance(status, str) or _STATUS.fullmatch(status) is None:
        return None, None
    return status, cast("Mapping[str, object]", value)


def _xml_status(body: bytes) -> str | None:
    upper = body[:4096].upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        return None
    try:
        status = ET.fromstring(body).findtext("status")  # noqa: S314 -- bounded, DTD refused
    except ET.ParseError:
        return None
    return status if status is not None and _STATUS.fullmatch(status) else None


def provider_status(body: bytes) -> str | None:
    """The three-digit OpenDART status a JSON or XML answer carries, else None."""
    status, _ = _json_status(body)
    return status if status is not None else _xml_status(body)


def corp_code_xml(body: bytes) -> bytes:
    """The ``CORPCODE.xml`` bytes of a corp code archive, refused unless exactly that file."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
        (member,) = archive.infolist()
    except (zipfile.BadZipFile, ValueError):
        raise ValueError("corp code response is not a one-file zip archive") from None
    if member.filename != "CORPCODE.xml" or member.file_size > MAX_XML_BYTES:
        raise ValueError("corp code archive must hold one bounded CORPCODE.xml")
    try:
        with archive.open(member) as handle:
            xml = handle.read(MAX_XML_BYTES + 1)
    except (zipfile.BadZipFile, RuntimeError, OSError):
        raise ValueError("corp code archive fails its CRC or decompression") from None
    if len(xml) != member.file_size:
        raise ValueError("corp code archive member size differs from its header")
    if b"<!DOCTYPE" in xml[:4096].upper() or b"<!ENTITY" in xml[:4096].upper():
        raise ValueError("corp code XML may not declare a DTD or entities")
    return xml


def listed_corp_codes(body: bytes) -> dict[str, str]:
    """Corp code -> stock code of every company the archive lists with a stock code."""
    try:
        root = ET.fromstring(corp_code_xml(body))  # noqa: S314 -- bounded, DTD refused
    except ET.ParseError:
        raise ValueError("CORPCODE.xml is not well-formed XML") from None
    codes: dict[str, str] = {}
    for item in root.findall("list"):
        corp = (item.findtext("corp_code") or "").strip()
        stock = (item.findtext("stock_code") or "").strip()
        if _CORP.fullmatch(corp) is not None and _STOCK.fullmatch(stock) is not None:
            codes[corp] = stock
    if not codes:
        raise ValueError("corp code archive lists no listed company")
    return dict(sorted(codes.items()))


@dataclass(frozen=True, slots=True)
class Filing:
    """One periodic report filing a list page names, mapped to the request it answers."""

    corp_code: str
    bsns_year: int
    reprt_code: str
    rcept_no: str
    filed_on: date
    market: str


@dataclass(frozen=True, slots=True)
class ListPage:
    """A completed list page: its filings, the page count and filings it could not map."""

    total_pages: int
    filings: tuple[Filing, ...]
    unmapped: int


def report_of(name: str) -> tuple[str, int] | None:
    """``(reprt_code, bsns_year)`` of a December-year periodic report name, else None.

    ``분기보고서 (2026.09)`` is the third quarter of 2026 and ``[기재정정]사업보고서
    (2025.12)`` amends the 2025 annual report. A period ending in another month belongs to
    a fiscal year that does not end in December and maps to None.
    """
    match = _REPORT_NAME.fullmatch(" ".join(name.split()))
    if match is None:
        return None
    kind, year, month = match[1], int(match[2]), int(match[3])
    report = _REPORT_KINDS.get((kind, month))
    return None if report is None else (report, year)


def parse_list(body: bytes) -> ListPage:
    """A completed ``list.json`` page (status ``000``); refused when it is anything else."""
    status, document = _json_status(body)
    if status != "000" or document is None:
        raise ValueError("list page is not a completed provider answer")
    total = document.get("total_page")
    items = document.get("list")
    if type(total) is not int or total < 1 or not isinstance(items, list):
        raise ValueError("list page names no page count or filing list")
    filings: list[Filing] = []
    unmapped = 0
    for item in cast("list[object]", items):
        if not isinstance(item, dict):
            raise ValueError("list page holds a filing that is not an object")  # noqa: TRY004 -- malformed-content ValueError contract
        entry = cast("Mapping[str, object]", item)
        corp, number, name = entry.get("corp_code"), entry.get("rcept_no"), entry.get("report_nm")
        market, filed = entry.get("corp_cls"), entry.get("rcept_dt")
        if not all(isinstance(value, str) for value in (corp, number, name, market, filed)):
            raise ValueError("list page filing lacks a text corp code, number, name or date")
        corp, number, name = cast("str", corp), cast("str", number), cast("str", name)
        if _CORP.fullmatch(corp) is None or _RECEIPT.fullmatch(number) is None:
            raise ValueError("list page filing has a malformed corp code or receipt number")
        filed_on = _day(cast("str", filed), "rcept_dt")
        report = report_of(name)
        if report is None or report[1] < FIRST_YEAR:
            unmapped += 1
            continue
        filings.append(Filing(corp, report[1], report[0], number, filed_on, cast("str", market)))
    return ListPage(total, tuple(filings), unmapped)


def _answered(request: DartRequest, body: bytes) -> bool:
    """Whether a status ``000`` answer carries the content its endpoint promises."""
    _, document = _json_status(body)
    rows = None if document is None else document.get("list")
    if not isinstance(rows, list):
        return False
    if request.endpoint == FINANCIALS:
        return bool(rows)
    try:
        parse_list(body)
    except ValueError:
        return False
    return True


def classify(request: DartRequest, response: DartResponse) -> tuple[str, str | None]:
    """The response's outcome and the provider status it carries."""
    status = provider_status(response.body)
    if response.status != _HTTP_OK:
        return FAILED, status
    if request.endpoint == CORP_CODES:
        try:
            listed_corp_codes(response.body)
        except ValueError:
            return FAILED, status
        return COMPLETED, None
    if status == "013":
        return NO_DATA, status
    answered = status == "000" and _answered(request, response.body)
    return (COMPLETED if answered else FAILED), status


def stops_run(response: DartResponse) -> bool:
    """Whether no later call in this run can be answered (key, IP or quota refusals)."""
    return response.status in STOP_HTTP or provider_status(response.body) in STOP_STATUSES


# --- transport ----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HttpAnswer:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class Transport(Protocol):
    def __call__(
        self, method: str, url: str, body: bytes | None, headers: Mapping[str, str]
    ) -> HttpAnswer: ...


class TransportError(RuntimeError):
    """The call may or may not have reached the provider; no answer was retained."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        raise urllib.error.HTTPError("", 310, "redirect refused", Message(), None)


def urllib_transport(timeout_seconds: float = 30, max_bytes: int = MAX_RESPONSE_BYTES) -> Transport:
    """A urllib transport: no redirects, identity encoding, bounded bodies."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or max_bytes <= 0:
        raise ValueError("transport bounds must be positive")

    def call(method: str, url: str, body: bytes | None, headers: Mapping[str, str]) -> HttpAnswer:
        if not url.startswith("https://"):
            raise ValueError("collector calls are HTTPS only")
        query = urllib.request.Request(url, data=body, headers=dict(headers), method=method)  # noqa: S310 -- closed endpoint map, HTTPS checked
        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(query, timeout=timeout_seconds) as response:
                status = int(response.status)
                pairs = response.headers.items()
                payload = response.read(max_bytes + 1)
                encoding = response.headers.get("Content-Encoding", "identity")
        except urllib.error.HTTPError as error:
            status, pairs = int(error.code), list(error.headers.items()) if error.headers else []
            try:
                payload, encoding = error.read(max_bytes + 1), "identity"
            finally:
                error.close()
        except (urllib.error.URLError, OSError, TimeoutError):
            raise TransportError("provider transport failed") from None
        if len(payload) > max_bytes or encoding != "identity":
            raise TransportError("provider answer exceeds its bound or is encoded")
        kept = tuple(
            (str(name).lower(), str(value))
            for name, value in pairs
            if str(name).lower() in _RETAINED_HEADERS
        )
        return HttpAnswer(status, kept, payload)

    return call


type Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC)


class OpenDartClient:
    """Calls OpenDART with the key it holds; the key never reaches a receipt."""

    def __init__(self, key: str, transport: Transport, clock: Clock = utc_now) -> None:
        if not isinstance(key, str) or len(key) != 40 or not key.isalnum() or not key.isascii():  # noqa: PLR2004 -- OpenDART key length
            raise ValueError("OpenDART key must be 40 ASCII letters and digits")
        self._key = key
        self._transport = transport
        self._clock = clock

    def request(self, request: DartRequest) -> DartResponse:
        query = {**request.parameters, "crtfc_key": self._key}
        url = ENDPOINTS[request.endpoint] + "?" + urllib.parse.urlencode(sorted(query.items()))
        started = self._clock()
        answer = self._transport("GET", url, None, {"Accept": "*/*"})
        finished = self._clock()
        key = self._key.encode()
        echoed = b"\n".join(value.encode() for _, value in answer.headers)
        if key in answer.body or key in echoed:
            raise TransportError("provider answer echoed the credential; it is not retained")
        return DartResponse(answer.status, answer.headers, answer.body, started, finished)


def instant(moment: datetime) -> str:
    """UTC ``YYYY-MM-DDTHH:MM:SS.ffffffZ``, the receipt instant spelling."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("instants must be timezone-aware")
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
