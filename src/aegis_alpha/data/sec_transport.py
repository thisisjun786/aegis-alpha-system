"""SEC EDGAR transport: host allowlist, User-Agent gate, synthetic-only I/O.

G-A never opens a socket. The HTTPS helper exists for a later owner-gated G-B
probe and still refuses every host other than ``data.sec.gov``. Archives hosts
are forbidden until a later owner list.
"""

from __future__ import annotations

import hashlib
import re
import socket
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final, Protocol, Self
from urllib.parse import urlparse

from aegis_alpha.data.sec_identity import CIK_DIGITS

ALLOWED_HOST: Final = "data.sec.gov"
BASE_URL: Final = f"https://{ALLOWED_HOST}"
USER_AGENT_ENV: Final = "SEC_USER_AGENT"
REQUEST_TIMEOUT_SECONDS: Final = 30.0
_MAX_RAW_RESPONSE_BYTES: Final = 32 * 1024 * 1024
_CONTACT_ADDRESS = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_ARCHIVES_HINTS: Final = ("archives",)
_FORBIDDEN_PATH_PREFIXES: Final = ("/archives/", "/edgar/data/")


class TransportError(RuntimeError):
    """A request was rejected before or during transport without leaking headers."""


class UserAgentError(TransportError):
    """``SEC_USER_AGENT`` is missing or has no contact address. Zero calls."""


class HostForbiddenError(TransportError):
    """The URL is outside the ``data.sec.gov`` first-wave allowlist."""


class SocketAccessError(TransportError):
    """A synthetic transport attempted to open a network resource."""


class DatasetKind(StrEnum):
    SUBMISSIONS = "submissions"
    COMPANYFACTS = "companyfacts"


@dataclass(frozen=True, slots=True)
class CollectorRequest:
    dataset: DatasetKind
    cik: str

    def __post_init__(self) -> None:
        if len(self.cik) != CIK_DIGITS or not self.cik.isdigit():
            raise ValueError("request CIK must be the 10-digit padded form")

    @property
    def path(self) -> str:
        if self.dataset is DatasetKind.SUBMISSIONS:
            return f"/submissions/CIK{self.cik}.json"
        return f"/api/xbrl/companyfacts/CIK{self.cik}.json"

    @property
    def source_uri(self) -> str:
        return f"{BASE_URL}{self.path}"

    @property
    def request_fingerprint(self) -> str:
        payload = f"GET\n{self.path}\n".encode()
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True, slots=True)
class CollectorResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    requested_at_utc: datetime
    retrieved_at_utc: datetime

    def __post_init__(self) -> None:
        if self.requested_at_utc.tzinfo is None or self.retrieved_at_utc.tzinfo is None:
            raise ValueError("collector timestamps must be timezone-aware")
        if len(self.body) > _MAX_RAW_RESPONSE_BYTES:
            raise ValueError("provider response exceeds the collector byte limit")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


Transport = Callable[[CollectorRequest], CollectorResponse]


class RetrievedHttpResponse(Protocol):
    """urllib response surface used after open, without assuming a socket."""

    status: int

    @property
    def headers(self) -> Mapping[str, str]: ...

    def read(self) -> bytes: ...

    def geturl(self) -> str: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> bool | None: ...


class UrlOpener(Protocol):
    """Minimal opener used by the live transport and socket-free tests."""

    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> RetrievedHttpResponse: ...


def validate_user_agent(value: str | None) -> str:
    """Accept a User-Agent that includes a contact address. Never echo the value."""

    if value is None or not value.strip():
        raise UserAgentError("SEC_USER_AGENT is missing; zero calls will be made")
    if _CONTACT_ADDRESS.search(value) is None:
        raise UserAgentError("SEC_USER_AGENT has no contact address; zero calls will be made")
    return value


def configured_user_agent_needles(user_agent: str) -> tuple[bytes, ...]:
    """Scan only the configured User-Agent and its extracted contact.

    SEC 403 HTML includes ``webmaster@sec.gov``. A generic RFC-like email scan
    would treat that provider page as a leak and block retry.
    """

    needles = [user_agent.encode()]
    match = _CONTACT_ADDRESS.search(user_agent)
    if match is not None:
        contact = match.group(0).encode()
        if contact not in needles:
            needles.append(contact)
    return tuple(needles)


def assert_user_agent_absent(user_agent: str, *payloads: bytes) -> None:
    """Fail closed if the configured User-Agent leaked into evidence bytes."""

    if not user_agent:
        return
    needles = configured_user_agent_needles(user_agent)
    for payload in payloads:
        if any(needle in payload for needle in needles):
            raise TransportError("User-Agent material detected in provider or evidence bytes")


def redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Drop User-Agent and other request-identity headers from evidence."""

    redacted: dict[str, str] = {}
    for name, value in headers.items():
        folded = name.casefold()
        if folded in {"user-agent", "authorization", "cookie", "set-cookie"}:
            redacted[folded] = "[REDACTED]"
            continue
        redacted[folded] = value
    return redacted


class RefuseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse 3xx. ``urlopen`` otherwise follows to hosts outside the allowlist."""

    def redirect_request(  # noqa: PLR0913,PLR0917 - urllib handler signature
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> urllib.request.Request | None:
        del req, fp, code, msg, headers, newurl
        raise HostForbiddenError("SEC collector refuses HTTP redirects")


def make_https_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(RefuseRedirectHandler)


def require_final_url(url: str) -> str:
    """Re-check the URL urllib actually retrieved against the allowlist."""

    return require_allowed_url(url)


def require_allowed_url(url: str) -> str:
    """Admit only ``https://data.sec.gov`` first-wave JSON paths."""

    parsed = urlparse(url)
    host = (parsed.hostname or "").casefold()
    if parsed.scheme != "https" or host != ALLOWED_HOST:
        raise HostForbiddenError("SEC collector host is not on the first-wave allowlist")
    if any(hint in host for hint in _ARCHIVES_HINTS):
        raise HostForbiddenError("Archives hosts are forbidden until a later owner list")
    path = parsed.path.casefold()
    if any(path.startswith(prefix) for prefix in _FORBIDDEN_PATH_PREFIXES):
        raise HostForbiddenError("Archives document paths are forbidden in G-A")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise HostForbiddenError("SEC first-wave URLs must be credential-free path-only GET")
    return url


def submissions_url(cik: str) -> str:
    return require_allowed_url(CollectorRequest(DatasetKind.SUBMISSIONS, cik).source_uri)


def companyfacts_url(cik: str) -> str:
    return require_allowed_url(CollectorRequest(DatasetKind.COMPANYFACTS, cik).source_uri)


_SYNTHETIC_EXECUTION_ACTIVE: ContextVar[bool] = ContextVar(
    "sec_synthetic_execution_active",
    default=False,
)
_SYNTHETIC_AUDIT_HOOK_INSTALLED = False


def _synthetic_audit_hook(event: str, _args: tuple[object, ...]) -> None:
    if not _SYNTHETIC_EXECUTION_ACTIVE.get():
        return
    if event.startswith(("socket.", "subprocess.", "ctypes.")) or event in {
        "os.system",
        "os.posix_spawn",
        "os.spawn",
    }:
        raise SocketAccessError(f"synthetic transport external I/O is blocked: {event}")


@contextmanager
def no_socket_access() -> Iterator[None]:
    """Disable direct and subprocess network escape paths during synthetic I/O."""

    global _SYNTHETIC_AUDIT_HOOK_INSTALLED  # noqa: PLW0603 - process-wide hook is append-only
    if not _SYNTHETIC_AUDIT_HOOK_INSTALLED:
        sys.addaudithook(_synthetic_audit_hook)
        _SYNTHETIC_AUDIT_HOOK_INSTALLED = True

    def denied(*_args: object, **_kwargs: object) -> None:
        raise SocketAccessError("a synthetic transport must not open a socket")

    blocked = ("socket", "create_connection", "getaddrinfo")
    originals = {name: getattr(socket, name) for name in blocked}
    for name in blocked:
        setattr(socket, name, denied)
    token = _SYNTHETIC_EXECUTION_ACTIVE.set(True)
    try:
        yield
    finally:
        _SYNTHETIC_EXECUTION_ACTIVE.reset(token)
        for name, original in originals.items():
            setattr(socket, name, original)


def make_fixture_transport(
    bodies: Mapping[tuple[DatasetKind, str], bytes],
    *,
    clock: Callable[[], datetime],
    status_code: int = 200,
) -> Transport:
    """Return an offline transport that cannot open a socket."""

    def transport(request: CollectorRequest) -> CollectorResponse:
        require_allowed_url(request.source_uri)
        with no_socket_access():
            body = bodies.get((request.dataset, request.cik))
            if body is None:
                raise TransportError("synthetic fixture is missing for the requested CIK")
            now = clock()
            return CollectorResponse(
                status_code=status_code,
                headers={"content-type": "application/json"},
                body=body,
                requested_at_utc=now,
                retrieved_at_utc=now,
            )

    return transport


def make_https_transport(
    *,
    user_agent: str,
    timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
    clock: Callable[[], datetime],
    opener: UrlOpener | None = None,
) -> Transport:
    """Build the live transport. G-A tests never open a socket."""

    validated = validate_user_agent(user_agent)
    resolved_opener = make_https_opener() if opener is None else opener

    def transport(request: CollectorRequest) -> CollectorResponse:
        url = require_allowed_url(request.source_uri)
        requested_at = clock()
        http_request = urllib.request.Request(  # noqa: S310 - host is allowlisted above
            url,
            headers={"User-Agent": validated, "Accept": "application/json"},
            method="GET",
        )
        try:
            with resolved_opener.open(http_request, timeout=timeout_seconds) as response:
                require_final_url(response.geturl())
                payload = response.read()
                status = int(response.status)
                headers = {name.casefold(): value for name, value in response.headers.items()}
        except urllib.error.HTTPError as error:
            if error.url:
                require_final_url(error.url)
            payload = error.read()
            status = int(error.code)
            headers = {
                name.casefold(): value
                for name, value in (error.headers.items() if error.headers is not None else ())
            }
        except urllib.error.URLError as error:
            raise TransportError(
                f"provider request failed: {type(error.reason).__name__}"
            ) from None
        assert_user_agent_absent(validated, payload)
        return CollectorResponse(
            status_code=status,
            headers=redact_headers(headers),
            body=payload,
            requested_at_utc=requested_at,
            retrieved_at_utc=clock(),
        )

    return transport
