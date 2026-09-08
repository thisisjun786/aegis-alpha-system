"""Real stdlib HTTP parsing over byte streams; no network or TLS handshake."""

from __future__ import annotations

import gc
import http.client
import io
import socket
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast

import pytest

from aegis_alpha.data import fmp_collector_http, fmp_http_session
from aegis_alpha.data.fmp_collector import CollectorRequest
from aegis_alpha.data.fmp_collector_http import HostForbiddenError, make_fmp_https_transport
from aegis_alpha.data.fmp_http_session import FmpHTTPSOpener
from tests.data.test_fmp_collector_http import _API_KEY, _fixed_clock


def wire(body: bytes = b"[]", *, status: int = 200, headers: bytes = b"") -> bytes:
    return (
        f"HTTP/1.1 {status} Fixture\r\nContent-Length: {len(body)}\r\n".encode()
        + headers
        + b"\r\n"
        + body
    )


class StreamSocket:
    def __init__(self, content: bytes = b"") -> None:
        self.content = content
        self.timeouts: list[float | None] = []

    def makefile(self, _mode: str) -> io.BytesIO:
        return io.BytesIO(self.content)

    def settimeout(self, timeout: float | None) -> None:
        self.timeouts.append(timeout)


@dataclass
class ConnectionDouble:
    outcomes: list[bytes | Exception]
    timeout: float | None
    sock: StreamSocket | None = field(default_factory=StreamSocket)
    requests: list[tuple[str, str, dict[str, str]]] = field(default_factory=list)
    closed: bool = False

    def request(self, method: str, url: str, *, headers: Mapping[str, str]) -> None:
        assert not self.closed
        self.requests.append((method, url, dict(headers)))

    def getresponse(self) -> http.client.HTTPResponse:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        # Only socket.makefile is needed; the parser reads these exact wire bytes.
        response = http.client.HTTPResponse(cast("socket.socket", StreamSocket(outcome)))
        response.begin()
        return response

    def close(self) -> None:
        self.closed = True
        self.sock = None


@dataclass
class ConnectionFactory:
    outcomes: list[bytes | Exception]
    connections: list[ConnectionDouble] = field(default_factory=list)

    def __call__(self, host: str, *, timeout: float | None) -> ConnectionDouble:
        assert host == "financialmodelingprep.com"
        connection = ConnectionDouble(self.outcomes, timeout)
        self.connections.append(connection)
        return connection


def install_factory(
    monkeypatch: pytest.MonkeyPatch, outcomes: list[bytes | Exception]
) -> ConnectionFactory:
    factory = ConnectionFactory(outcomes)
    monkeypatch.setattr(fmp_http_session.http.client, "HTTPSConnection", factory)
    monkeypatch.setattr(fmp_collector_http.urllib.request, "getproxies", dict)
    return factory


def request(
    url: str = "https://financialmodelingprep.com/stable/profile?symbol=AAPL",
) -> urllib.request.Request:
    return urllib.request.Request(url, headers={"apikey": _API_KEY}, method="GET")  # noqa: S310 -- fixture allowlisted URL


def test_fully_consumed_responses_reuse_connection_and_keep_header_only_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = install_factory(monkeypatch, [wire(), wire()])
    transport = make_fmp_https_transport(clock=_fixed_clock)
    for symbol in ("AAPL", "MSFT"):
        assert (
            transport(CollectorRequest("/stable/profile", {"symbol": symbol}), _API_KEY).body
            == b"[]"
        )
    assert len(factory.connections) == 1
    connection = factory.connections[0]
    assert len(connection.requests) == 2  # noqa: PLR2004 -- two calls, one connection
    for method, target, headers in connection.requests:
        assert method == "GET"
        assert target.startswith("/stable/profile?")
        assert _API_KEY not in target
        assert headers["Apikey"] == _API_KEY


def test_raw_duplicate_headers_and_error_status_reach_existing_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = install_factory(
        monkeypatch,
        [
            wire(
                status=429, headers=b"X-Trace: first\r\nX-Trace: second\r\nSet-Cookie: private\r\n"
            ),
            wire(),
        ],
    )
    transport = make_fmp_https_transport(clock=_fixed_clock)
    result = transport(CollectorRequest("/stable/profile", {}), _API_KEY)
    assert result.status_code == 429  # noqa: PLR2004 -- counted rate-limit response
    assert [(key, value) for key, value in result.raw_headers if key == "X-Trace"] == [
        ("X-Trace", "first"),
        ("X-Trace", "second"),
    ]
    assert result.headers["set-cookie"] == "[REDACTED]"
    transport(CollectorRequest("/stable/profile", {}), _API_KEY)
    assert len(factory.connections) == 1


@pytest.mark.parametrize("consume", [False, True])
def test_partial_or_abandoned_response_closes_before_next_request(
    monkeypatch: pytest.MonkeyPatch, *, consume: bool
) -> None:
    factory = install_factory(monkeypatch, [wire(b"abcdef"), wire()])
    opener = FmpHTTPSOpener(body_limit=100)
    with opener.open(request()) as response:
        if consume:
            assert response.read(1) == b"a"
    assert factory.connections[0].closed
    with opener.open(request()) as response:
        assert response.read() == b"[]"
    assert len(factory.connections) == 2  # noqa: PLR2004 -- incomplete first connection discarded
    opener.close()
    assert factory.connections[-1].closed


def test_chunked_complete_response_is_reusable(monkeypatch: pytest.MonkeyPatch) -> None:
    chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n[]\r\n0\r\n\r\n"
    factory = install_factory(monkeypatch, [chunked, wire()])
    opener = FmpHTTPSOpener(body_limit=100)
    for _ in range(2):
        with opener.open(request()) as response:
            assert response.read(101) == b"[]"
    assert len(factory.connections) == 1
    opener.close()


@pytest.mark.parametrize(
    "broken",
    [
        http.client.RemoteDisconnected("closed"),
        http.client.BadStatusLine("bad"),
        TimeoutError("synthetic timeout"),
        b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nshort",
    ],
)
def test_protocol_failure_closes_without_an_internal_retry(
    monkeypatch: pytest.MonkeyPatch, broken: bytes | Exception
) -> None:
    factory = install_factory(monkeypatch, [broken, wire()])
    opener = FmpHTTPSOpener(body_limit=100)
    with (
        pytest.raises(OSError, match=r"closed|FMP HTTPS|timeout"),
        opener.open(request()) as response,
    ):
        response.read(101)
    assert len(factory.connections) == 1
    assert len(factory.connections[0].requests) == 1
    assert factory.connections[0].closed
    with opener.open(request()) as response:
        assert response.read() == b"[]"
    assert len(factory.connections) == 2  # noqa: PLR2004 -- caller explicitly made the next attempt
    opener.close()


def test_oversize_complete_body_discards_connection_and_preserves_collector_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maximum = 32 * 1024 * 1024
    factory = install_factory(monkeypatch, [wire(b"x" * (maximum + 1))])
    transport = make_fmp_https_transport(clock=_fixed_clock)
    with pytest.raises(ValueError, match="byte limit"):
        transport(CollectorRequest("/stable/profile", {}), _API_KEY)
    assert factory.connections[0].closed


@pytest.mark.parametrize("location", ["/stable/profile", "https://foreign.invalid/collect"])
def test_redirect_closes_without_following(monkeypatch: pytest.MonkeyPatch, location: str) -> None:
    factory = install_factory(
        monkeypatch, [wire(status=302, headers=f"Location: {location}\r\n".encode())]
    )
    opener = FmpHTTPSOpener(body_limit=100)
    with pytest.raises(HostForbiddenError, match="redirect"):
        opener.open(request())
    assert len(factory.connections[0].requests) == 1
    assert factory.connections[0].closed


@pytest.mark.parametrize(
    "url",
    [
        "https://foreign.invalid/stable/profile",
        "http://financialmodelingprep.com/stable/profile",
        "https://financialmodelingprep.com:8443/stable/profile",
    ],
)
def test_session_refuses_unsafe_url_before_connection(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    factory = install_factory(monkeypatch, [])
    with pytest.raises(HostForbiddenError):
        FmpHTTPSOpener(body_limit=100).open(request(url))
    assert factory.connections == []


def test_non_get_refused_before_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = install_factory(monkeypatch, [])
    prepared = request()
    prepared.method = "POST"
    with pytest.raises(HostForbiddenError, match="GET"):
        FmpHTTPSOpener(body_limit=100).open(prepared)
    assert factory.connections == []


@pytest.mark.parametrize(
    "proxy",
    [
        {"https": "http://proxy.invalid:3128"},
        {"https": "http://proxy.invalid:3128", "no": "financialmodelingprep.com"},
        {"all": "http://proxy.invalid:3128"},
    ],
)
def test_proxy_configuration_keeps_urllib_policy(
    monkeypatch: pytest.MonkeyPatch, proxy: dict[str, str]
) -> None:
    monkeypatch.setattr(fmp_collector_http.urllib.request, "getproxies", lambda: proxy)
    assert isinstance(fmp_collector_http.make_https_opener(), urllib.request.OpenerDirector)


def test_connection_close_response_is_not_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = install_factory(monkeypatch, [wire(headers=b"Connection: close\r\n"), wire()])
    opener = FmpHTTPSOpener(body_limit=100)
    with opener.open(request()) as response:
        assert response.read() == b"[]"
    assert factory.connections[0].closed
    with opener.open(request()) as response:
        assert response.read() == b"[]"
    assert len(factory.connections) == 2  # noqa: PLR2004 -- peer closed the first connection
    opener.close()


def test_default_transport_releases_idle_connection_when_discarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = install_factory(monkeypatch, [wire()])
    transport = make_fmp_https_transport(clock=_fixed_clock)
    transport(CollectorRequest("/stable/profile", {}), _API_KEY)
    assert factory.connections[0].closed is False
    del transport
    gc.collect()
    assert factory.connections[0].closed is True


def test_next_open_discards_unclosed_response_without_closing_new_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = install_factory(monkeypatch, [wire(b"abcdef"), wire()])
    opener = FmpHTTPSOpener(body_limit=100)
    abandoned = opener.open(request())
    assert abandoned.read(1) == b"a"
    with opener.open(request()) as response:
        assert factory.connections[0].closed is True
        abandoned.__exit__(None, None, None)
        assert factory.connections[1].closed is False
        assert response.read() == b"[]"
    opener.close()
