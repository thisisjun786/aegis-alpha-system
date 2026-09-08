"""Real HTTPS transport for the FMP collector (AAS-DATA-004C live path).

Mirrors the probe and SEC transport guardrails: the exact host allowlist
``financialmodelingprep.com``, the ``apikey`` credential in a request header
(never in the URL), cross-host redirects refused before they can be followed,
response headers redacted at the evidence boundary, and the collector byte cap
enforced by ``CollectorResponse``. Credential-body rejection belongs to the
engine so accounting and durable terminal state happen first. Tests inject a
fake opener; no socket exists in tests or CI.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Final, Protocol, Self
from urllib.parse import parse_qsl, quote, quote_plus, unquote, unquote_plus, urlparse

from aegis_alpha.data.fmp_collector import (
    ALLOWED_HOST,
    CollectorRequest,
    CollectorResponse,
    Transport,
)
from aegis_alpha.data.fmp_rate_limit import REQUEST_TIMEOUT_SECONDS
from aegis_alpha.data.fmp_response_security import redact_raw_headers

#: Read bound mirroring fmp_collector._MAX_RAW_RESPONSE_BYTES; CollectorResponse
#: enforces the authoritative cap itself when the response is constructed.
_READ_LIMIT_BYTES: Final = 32 * 1024 * 1024 + 1
_USER_AGENT: Final = "aegis-alpha-system/AAS-DATA-004"
_STABLE_PATH_PREFIX: Final = "/stable/"
_HTTPS_SCHEME: Final = "https"
_HTTPS_PORT: Final = 443


class HostForbiddenError(RuntimeError):
    """The URL or redirect target is outside the financialmodelingprep.com allowlist."""


class RefuseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse 3xx: urllib would otherwise follow redirects off the allowlist."""

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
        raise HostForbiddenError("FMP collector refuses HTTP redirects")


class RawHeaders(Protocol):
    """Header collection whose items preserve wire order and duplicates."""

    def items(self) -> Iterable[tuple[str, str]]: ...


class RetrievedHttpResponse(Protocol):
    """urllib response surface used after open, without assuming a socket."""

    status: int

    @property
    def headers(self) -> RawHeaders: ...

    def read(self, amount: int = -1) -> bytes: ...

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


def require_allowed_url(url: str) -> str:
    """Admit only credential-free ``https://financialmodelingprep.com/stable/`` GETs."""

    parsed = urlparse(url)
    try:
        port = parsed.port
    except ValueError as error:
        raise HostForbiddenError("URL contains an invalid port") from error
    if (
        parsed.scheme != _HTTPS_SCHEME
        or (parsed.hostname or "").casefold() != ALLOWED_HOST
        or parsed.username is not None
        or parsed.password is not None
        or (port is not None and port != _HTTPS_PORT)
        or parsed.fragment
        or not parsed.path.startswith(_STABLE_PATH_PREFIX)
    ):
        raise HostForbiddenError("URL is outside the financialmodelingprep.com /stable/ allowlist")
    if any(
        "apikey" in name.casefold() or "token" in name.casefold()
        for name, _value in parse_qsl(parsed.query)
    ):
        raise HostForbiddenError("credential parameters are forbidden in URLs")
    return url


def require_final_url(url: str) -> str:
    """Re-check the URL urllib actually retrieved against the allowlist."""

    return require_allowed_url(url)


def url_contains_credential(url: str, credential: str) -> bool:
    """True when the actual credential appears in query values or encoded URL forms."""

    if not credential:
        return False
    parsed = urlparse(url)
    query_values = [value for _name, value in parse_qsl(parsed.query, keep_blank_values=True)]
    if any(credential == value or credential in value for value in query_values):
        return True
    encoded_forms = {credential, quote(credential, safe=""), quote_plus(credential, safe="")}
    haystacks = (url, parsed.query, unquote(url), unquote_plus(url))
    return any(
        form in haystack for form in encoded_forms if form for haystack in haystacks if haystack
    )


def refuse_credential_in_url(url: str, credential: str) -> None:
    """Reject a URL that would place the live credential on the wire before open."""

    if url_contains_credential(url, credential):
        raise HostForbiddenError("credential values are forbidden in URLs")


def make_https_opener() -> UrlOpener | urllib.request.OpenerDirector:
    proxies = urllib.request.getproxies()
    if proxies.get("https") or proxies.get("all"):
        # Leave proxy and NO_PROXY policy to urllib; never route around configured proxies.
        return urllib.request.build_opener(RefuseRedirectHandler)
    from aegis_alpha.data.fmp_http_session import (  # noqa: PLC0415 -- shared URL admission
        FmpHTTPSOpener,
    )

    return FmpHTTPSOpener(body_limit=_READ_LIMIT_BYTES - 1)


def make_fmp_https_transport(
    *,
    timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
    clock: Callable[[], datetime],
    opener: UrlOpener | None = None,
) -> Transport:
    """Build the live collector transport; tests inject ``opener``, never a socket."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    resolved_opener = make_https_opener() if opener is None else opener

    def transport(request: CollectorRequest, credential: str) -> CollectorResponse:
        if not credential or "\r" in credential or "\n" in credential:
            raise ValueError("credential must be non-empty and header-safe")
        url = require_allowed_url(request.source_uri)
        refuse_credential_in_url(url, credential)
        http_request = urllib.request.Request(  # noqa: S310 - host is allowlisted above
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": _USER_AGENT,
                "apikey": credential,
            },
            method="GET",
        )
        refuse_credential_in_url(http_request.full_url, credential)
        requested_at = clock()
        try:
            with resolved_opener.open(http_request, timeout=timeout_seconds) as response:
                require_final_url(response.geturl())
                payload = response.read(_READ_LIMIT_BYTES)
                status = int(response.status)
                raw_headers = tuple(response.headers.items())
        except urllib.error.HTTPError as error:
            if error.url:
                require_final_url(error.url)
            payload = error.read(_READ_LIMIT_BYTES)
            status = int(error.code)
            raw_headers = tuple(error.headers.items() if error.headers is not None else ())
            error.close()
        except urllib.error.URLError as error:
            if isinstance(error.reason, TimeoutError):
                raise error.reason from None
            raise
        return CollectorResponse(
            status_code=status,
            headers=redact_raw_headers(raw_headers),
            body=payload,
            requested_at_utc=requested_at,
            retrieved_at_utc=clock(),
            raw_headers=raw_headers,
        )

    return transport
