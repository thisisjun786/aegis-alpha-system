"""One direct, sequential FMP HTTPS connection; all retries belong to the collector."""

from __future__ import annotations

import http.client
import math
import urllib.request
import weakref
from typing import Self
from urllib.parse import urlsplit

from aegis_alpha.data.fmp_collector import ALLOWED_HOST
from aegis_alpha.data.fmp_collector_http import (
    HostForbiddenError,
    RawHeaders,
    refuse_credential_in_url,
    require_allowed_url,
)
from aegis_alpha.data.fmp_rate_limit import REQUEST_TIMEOUT_SECONDS


def _request_inputs(
    request: urllib.request.Request,
    timeout: float | None,
) -> tuple[str, dict[str, str], float]:
    url = require_allowed_url(request.full_url)
    if request.get_method() != "GET" or request.data is not None:
        raise HostForbiddenError("FMP HTTPS session accepts body-free GET only")
    credential = request.get_header("Apikey")
    if not credential or "\r" in credential or "\n" in credential:
        raise ValueError("credential must be non-empty and header-safe")
    refuse_credential_in_url(url, credential)
    host = request.get_header("Host")
    if host is not None and host.casefold() not in {ALLOWED_HOST, ALLOWED_HOST + ":443"}:
        raise HostForbiddenError("FMP HTTPS session refuses a foreign Host header")
    seconds = REQUEST_TIMEOUT_SECONDS if timeout is None else timeout
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("timeout must be positive and finite")
    return url, dict(request.header_items()), seconds


class FmpHTTPSOpener:
    """A UrlOpener-compatible direct session, selected only when no proxy is configured."""

    def __init__(self, *, body_limit: int) -> None:
        if type(body_limit) is not int or body_limit < 1:
            raise ValueError("FMP session body limit must be positive")
        self._body_limit = body_limit
        self._connection: http.client.HTTPSConnection | None = None
        self._cleanup: weakref.finalize | None = None
        self._active: _SessionResponse | None = None

    def close(self) -> None:
        if self._active is not None:
            self._active.finish(reusable=False)
        self._discard_connection()

    def _discard_connection(self) -> None:
        self._connection = None
        if self._cleanup is not None:
            self._cleanup()
            self._cleanup = None

    def release(self, response: _SessionResponse, *, reusable: bool) -> None:
        if self._active is response:
            self._active = None
            if not reusable:
                self._discard_connection()

    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> _SessionResponse:
        url, headers, seconds = _request_inputs(request, timeout)
        # An abandoned response cannot be pipelined or reused for the next request.
        if self._active is not None:
            self.close()
        try:
            if self._connection is None:
                self._connection = http.client.HTTPSConnection(ALLOWED_HOST, timeout=seconds)
                self._cleanup = weakref.finalize(self, self._connection.close)
            connection = self._connection
            connection.timeout = seconds
            if connection.sock is not None:
                connection.sock.settimeout(seconds)
            parsed = urlsplit(url)
            target = parsed.path + ("?" + parsed.query if parsed.query else "")
            connection.request("GET", target, headers=headers)
            response = connection.getresponse()
        except OSError:
            self.close()
            raise
        except http.client.HTTPException:
            self.close()
            raise OSError("FMP HTTPS session received an invalid HTTP response") from None
        if 300 <= response.status < 400:  # noqa: PLR2004 -- HTTP redirect class
            response.close()
            self.close()
            raise HostForbiddenError("FMP collector refuses HTTP redirects")
        if response.status < 200:  # noqa: PLR2004 -- reject protocol switching
            response.close()
            self.close()
            raise OSError("FMP HTTPS session refuses protocol switching")
        self._active = _SessionResponse(self, response, url, self._body_limit)
        return self._active


class _SessionResponse:
    """Release a connection only after a complete, bounded response was consumed."""

    def __init__(
        self,
        owner: FmpHTTPSOpener,
        response: http.client.HTTPResponse,
        url: str,
        body_limit: int,
    ) -> None:
        self._owner = owner
        self.status = response.status
        self._response = response
        self._url = url
        self._body_limit = body_limit
        self._bytes_read = 0
        self._consumed = False
        self._closed = False

    @property
    def headers(self) -> RawHeaders:
        return self._response.headers

    def geturl(self) -> str:
        return self._url

    def read(self, amount: int = -1) -> bytes:
        if self._closed:
            return b""
        remaining = self._body_limit + 1 - self._bytes_read
        requested = remaining if amount < 0 else min(amount, remaining)
        before = self._response.length
        try:
            payload = self._response.read(requested)
        except OSError:
            self.finish(reusable=False)
            raise
        except http.client.HTTPException:
            self.finish(reusable=False)
            raise OSError("FMP HTTPS session received an incomplete HTTP body") from None
        # http.client.read(n) can silently return an incomplete fixed-length body.
        if before is not None and len(payload) < min(requested, before):
            self.finish(reusable=False)
            raise OSError("FMP HTTPS response ended before its Content-Length")
        self._bytes_read += len(payload)
        self._consumed = self._response.isclosed()
        if self._bytes_read > self._body_limit:
            # Return the sentinel byte so the existing CollectorResponse cap owns refusal.
            self.finish(reusable=False)
        return payload

    def finish(self, *, reusable: bool) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._response.close()
        finally:
            self._owner.release(self, reusable=reusable)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.finish(
            reusable=(not args or args[0] is None)
            and self._consumed
            and not self._response.will_close
        )
