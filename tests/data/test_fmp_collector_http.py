"""FMP collector HTTPS transport: allowlist, header apikey, leak scan. Zero network."""

from __future__ import annotations

import email
import io
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.message import Message
from pathlib import Path
from typing import Literal, Self, assert_never
from urllib.parse import quote, quote_plus, urlencode

import pytest

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionRunState,
    CollectionUsageRecord,
    CurrentWatermark,
    WatermarkAdvance,
)
from aegis_alpha.data.fmp_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
    CredentialLeakError,
    FmpCollector,
)
from aegis_alpha.data.fmp_collector_http import (
    HostForbiddenError,
    RefuseRedirectHandler,
    make_fmp_https_transport,
    refuse_credential_in_url,
    require_allowed_url,
    require_final_url,
    url_contains_credential,
)
from aegis_alpha.data.fmp_rate_limit import (
    REQUEST_TIMEOUT_SECONDS,
    RateLimiter,
    TierArtifact,
)

_API_KEY = "test-only-fmp-key"
_BASE_URL = "https://financialmodelingprep.com"
_HTTP_OK = 200
_HTTP_FOUND = 302
_HTTP_NOT_FOUND = 404
_HTTP_TOO_MANY_REQUESTS = 429
_MAX_BODY_BYTES = 32 * 1024 * 1024
_REQUESTED_AT = datetime(2026, 8, 18, 1, 2, 3, tzinfo=UTC)


def _fixed_clock() -> datetime:
    return _REQUESTED_AT


def _collector_request() -> CollectorRequest:
    return CollectorRequest(endpoint="/stable/profile", parameters={"symbol": "AAPL"})


class _FakeResponse:
    """Minimal opener-response double; records reads and closure, no socket."""

    def __init__(
        self,
        body: bytes,
        *,
        status: int = _HTTP_OK,
        url: str = f"{_BASE_URL}/stable/profile?symbol=AAPL",
        headers: dict[str, str] | Message | None = None,
    ) -> None:
        self.status = status
        self._body = body
        self._url = url
        self._headers = headers if headers is not None else {"Content-Type": "application/json"}
        self.closed = False
        self.read_amounts: list[int] = []

    @property
    def headers(self) -> dict[str, str] | Message:
        return self._headers

    def read(self, amount: int = -1) -> bytes:
        self.read_amounts.append(amount)
        if amount >= 0:
            return self._body[:amount]
        return self._body

    def geturl(self) -> str:
        return self._url

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.closed = True


@dataclass(frozen=True, slots=True)
class _ObservedCall:
    request: urllib.request.Request
    timeout: float | None


class _UnusedControlPlane:
    """Structurally complete control plane for transport-only engine tests."""

    def register_plan(self, plan: CollectionRunPlan) -> None:
        raise AssertionError(plan)

    def start_run(self, run: CollectionRun) -> None:
        raise AssertionError(run)

    def append_event(self, event: CollectionRunEvent) -> int:
        raise AssertionError(event)

    def current_run_state(self, run_id: str) -> CollectionRunState | None:
        raise AssertionError(run_id)

    def run_plan_dataset(self, run_id: str) -> str | None:
        raise AssertionError(run_id)

    def advance_watermark(self, advance: WatermarkAdvance) -> int:
        raise AssertionError(advance)

    def latest_watermark(self, provider: str, dataset: str, stream: str) -> CurrentWatermark | None:
        raise AssertionError(provider, dataset, stream)

    def record_usage(
        self,
        record: CollectionUsageRecord,
        *,
        recorded_at_clock: object | None = None,
    ) -> None:
        del recorded_at_clock
        raise AssertionError(record)


class _FakeOpener:
    """Opener double: records the exact call, then returns or raises canned state."""

    def __init__(
        self,
        *,
        response: _FakeResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._response = response if response is not None else _FakeResponse(b"[]")
        self._error = error
        self.calls: list[_ObservedCall] = []

    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> _FakeResponse:
        self.calls.append(_ObservedCall(request=request, timeout=timeout))
        if self._error is not None:
            raise self._error
        return self._response


def _single_call(opener: _FakeOpener) -> _ObservedCall:
    (call,) = opener.calls
    return call


def test_happy_path_returns_collector_response_with_header_only_apikey() -> None:
    # Given an opener answering a synthetic JSON body with echo/cookie headers
    body = b'[{"symbol":"AAPL","companyName":"Synthetic"}]'
    response = _FakeResponse(
        body,
        headers={
            "Content-Type": "application/json",
            "apikey": _API_KEY,
            "Set-Cookie": "provider-cookie",
            "X-RateLimit-Remaining": "749",
        },
    )
    opener = _FakeOpener(response=response)
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    # When the transport executes a collector request
    result = transport(_collector_request(), _API_KEY)
    # Then the apikey travelled in a header only, never in the URL
    observed = _single_call(opener)
    assert observed.request.full_url == f"{_BASE_URL}/stable/profile?symbol=AAPL"
    assert _API_KEY not in observed.request.full_url
    assert observed.request.get_header("Apikey") == _API_KEY
    assert observed.request.get_method() == "GET"
    assert observed.timeout == REQUEST_TIMEOUT_SECONDS
    # And the response is a well-formed, redacted, closed CollectorResponse
    assert isinstance(result, CollectorResponse)
    assert result.status_code == _HTTP_OK
    assert result.body == body
    assert result.headers == {
        "content-type": "application/json",
        "apikey": "[REDACTED]",
        "set-cookie": "[REDACTED]",
        "x-ratelimit-remaining": "749",
    }
    assert result.requested_at_utc == _REQUESTED_AT
    assert result.retrieved_at_utc == _REQUESTED_AT
    assert response.closed is True


def test_evil_host_is_rejected_before_any_socket() -> None:
    # Given/When/Then: URL admission refuses foreign hosts, userinfo smuggling,
    # non-HTTPS schemes, and credential query parameters without any opener
    for url in (
        "https://evil.example.com/stable/profile",
        "https://financialmodelingprep.com@evil.example.com/stable/profile",
        "http://financialmodelingprep.com/stable/profile",
        "https://financialmodelingprep.com/other/profile",
        f"{_BASE_URL}/stable/profile?apikey=smuggled",
    ):
        with pytest.raises(HostForbiddenError):
            require_allowed_url(url)
        with pytest.raises(HostForbiddenError):
            require_final_url(url)


def test_explicit_non_443_ports_are_rejected_before_any_socket() -> None:
    # Given FMP-shaped URLs with unsafe, malformed, or out-of-range ports
    for url in (
        f"{_BASE_URL}:8443/stable/profile",
        f"{_BASE_URL}:80/stable/profile",
        f"{_BASE_URL}:444/stable/profile",
        f"{_BASE_URL}:bad/stable/profile",
        f"{_BASE_URL}:99999/stable/profile",
        "https://user:pass@financialmodelingprep.com:443/stable/profile",
    ):
        # When the URL crosses the transport boundary, Then it is rejected
        with pytest.raises(HostForbiddenError):
            require_allowed_url(url)


def test_absent_and_explicit_443_ports_remain_allowed() -> None:
    # Given otherwise-valid stable HTTPS URLs with no port or port 443
    for url in (
        f"{_BASE_URL}/stable/profile",
        f"{_BASE_URL}:443/stable/profile",
    ):
        # When the URL crosses the transport boundary, Then it remains allowed
        assert require_allowed_url(url) == url


def test_cross_host_redirect_is_refused_before_any_socket() -> None:
    # Given a redirect response pointing at a foreign host
    handler = RefuseRedirectHandler()
    request = urllib.request.Request(f"{_BASE_URL}/stable/profile")  # noqa: S310
    headers = email.message_from_string("Location: https://evil.example.com/x\n")
    # When urllib consults the handler, Then the redirect fails closed pre-socket
    with pytest.raises(HostForbiddenError, match="refuses HTTP redirects"):
        handler.redirect_request(
            request,
            fp=None,
            code=_HTTP_FOUND,
            msg="Found",
            headers=headers,
            newurl="https://evil.example.com/x",
        )
    assert isinstance(handler, urllib.request.HTTPRedirectHandler)


def test_followed_cross_host_redirect_fails_closed_without_a_socket() -> None:
    # Given an opener whose response claims it landed on a foreign host
    response = _FakeResponse(b"[]", url="https://evil.example.com/stable/profile")
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=_FakeOpener(response=response))
    # When the transport runs, Then the final-URL recheck raises post-open
    with pytest.raises(HostForbiddenError):
        transport(_collector_request(), _API_KEY)
    assert response.closed is True


def test_real_adapter_credential_body_reaches_engine_sanitized_accounting(
    tmp_path: Path,
) -> None:
    body = f'["{_API_KEY}"]'.encode()
    opener = _FakeOpener(response=_FakeResponse(body))
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=5,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
    )
    collector = FmpCollector(
        config=CollectorConfig(
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "datasets",
            receipt_path=tmp_path / "receipt.json",
            as_of=date(2026, 8, 18),
            mode=CollectionMode.INCREMENTAL,
            max_calls=5,
            run_identity="http-adapter-credential-rejection",
        ),
        transport=transport,
        control_plane=_UnusedControlPlane(),
        limiter=limiter,
        credential=_API_KEY,
        clock=_fixed_clock,
    )

    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol="AAPL")

    assert len(opener.calls) == 1
    assert limiter.calls_attempted == 1
    assert limiter.bytes_received == len(body)
    persisted = (path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert all(_API_KEY.encode() not in path.read_bytes() for path in persisted)


@pytest.mark.parametrize("status", [_HTTP_OK, 429])
@pytest.mark.parametrize("credential_location", ["name", "value", "duplicate"])
def test_real_adapter_credential_header_is_exactly_accounted_and_sanitized(
    tmp_path: Path,
    status: int,
    credential_location: Literal["name", "value", "duplicate"],
) -> None:
    headers = Message()
    headers.add_header("Content-Type", "application/json")
    match credential_location:
        case "name":
            headers.add_header(f"X-{_API_KEY}", "safe")
        case "value":
            headers.add_header("X-Provider", f"echo:{_API_KEY}")
        case "duplicate":
            headers.add_header("X-Provider", "safe")
            headers.add_header("X-Provider", _API_KEY)
        case unreachable:
            assert_never(unreachable)
    body = b"[]"
    opener = _FakeOpener(response=_FakeResponse(body, status=status, headers=headers))
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=5,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
    )
    collector = FmpCollector(
        config=CollectorConfig(
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "datasets",
            receipt_path=tmp_path / "receipt.json",
            as_of=date(2026, 8, 18),
            mode=CollectionMode.INCREMENTAL,
            max_calls=5,
            run_identity=f"http-header-{status}-{credential_location}",
        ),
        transport=transport,
        control_plane=_UnusedControlPlane(),
        limiter=limiter,
        credential=_API_KEY,
        clock=_fixed_clock,
    )

    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol="AAPL")

    assert len(opener.calls) == 1
    assert limiter.calls_attempted == 1
    assert limiter.bytes_received == len(body)
    assert limiter.ledger().rate_limited_attempts == (status == _HTTP_TOO_MANY_REQUESTS)
    persisted = (path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert all(_API_KEY.encode() not in path.read_bytes() for path in persisted)
    resumed_opener = _FakeOpener()
    resumed_limiter = RateLimiter(
        tier=TierArtifact(calls_per_minute=300, calls_per_day=None, bandwidth_gb_30d=None),
        max_calls=5,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
    )
    resumed = FmpCollector(
        config=collector.config,
        transport=make_fmp_https_transport(clock=_fixed_clock, opener=resumed_opener),
        control_plane=_UnusedControlPlane(),
        limiter=resumed_limiter,
        credential=_API_KEY,
        clock=_fixed_clock,
    )
    with pytest.raises(CredentialLeakError):
        resumed.collect_profile(symbol="AAPL")
    assert resumed_opener.calls == []
    assert resumed_limiter.calls_attempted == 1
    assert resumed_limiter.bytes_received == len(body)
    assert resumed_limiter.ledger().rate_limited_attempts == (status == _HTTP_TOO_MANY_REQUESTS)


def test_transport_returns_credential_body_for_engine_disposition() -> None:
    body = f'["{_API_KEY}"]'.encode()
    response = _FakeResponse(body)
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=_FakeOpener(response=response))

    result = transport(_collector_request(), _API_KEY)

    assert result.body == body


def test_oversized_body_raises_value_error_after_a_bounded_read() -> None:
    # Given a provider body one byte over the collector cap
    response = _FakeResponse(b"x" * (_MAX_BODY_BYTES + 1))
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=_FakeOpener(response=response))
    # When/Then the CollectorResponse cap raises and the read stayed bounded
    with pytest.raises(ValueError, match="byte limit"):
        transport(_collector_request(), _API_KEY)
    assert response.read_amounts == [_MAX_BODY_BYTES + 1]


def test_wrapped_timeout_stays_timeout_error_for_engine_retry() -> None:
    # Given urllib's timeout surface: URLError wrapping a TimeoutError reason
    opener = _FakeOpener(error=urllib.error.URLError(TimeoutError("timed out")))
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    # When/Then the engine retry path still sees a plain TimeoutError
    with pytest.raises(TimeoutError):
        transport(_collector_request(), _API_KEY)


def test_direct_timeout_propagates_for_engine_retry() -> None:
    # Given an opener raising a bare TimeoutError
    transport = make_fmp_https_transport(
        clock=_fixed_clock,
        opener=_FakeOpener(error=TimeoutError("timed out")),
    )
    # When/Then it propagates unwrapped
    with pytest.raises(TimeoutError):
        transport(_collector_request(), _API_KEY)


def test_http_error_is_preserved_as_collector_response_for_classification() -> None:
    # Given an opener failing with a 404 whose urllib error carries the URL
    error = urllib.error.HTTPError(
        url=f"{_BASE_URL}/stable/profile?symbol=AAPL",
        code=_HTTP_NOT_FOUND,
        msg="Not Found",
        hdrs=email.message_from_string("X-RateLimit-Remaining: 0\nSet-Cookie: x\n"),
        fp=io.BytesIO(b'{"Error Message":"unknown symbol"}'),
    )
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=_FakeOpener(error=error))
    # When the transport runs, Then the status/body reach classify_failure intact
    result = transport(_collector_request(), _API_KEY)
    assert result.status_code == _HTTP_NOT_FOUND
    assert result.body == b'{"Error Message":"unknown symbol"}'
    assert result.headers == {"x-ratelimit-remaining": "0", "set-cookie": "[REDACTED]"}
    assert result.requested_at_utc == _REQUESTED_AT
    assert error.fp is not None
    assert error.fp.closed


def test_transport_returns_credential_error_body_for_engine_disposition() -> None:
    body = f'{{"key":"{_API_KEY}"}}'.encode()
    error = urllib.error.HTTPError(
        url=f"{_BASE_URL}/stable/profile?symbol=AAPL",
        code=_HTTP_NOT_FOUND,
        msg="Not Found",
        hdrs=email.message_from_string(""),
        fp=io.BytesIO(body),
    )
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=_FakeOpener(error=error))

    result = transport(_collector_request(), _API_KEY)

    assert result.body == body


def test_non_timeout_url_error_propagates_unswallowed() -> None:
    # Given a DNS-class failure with no timeout inside
    opener = _FakeOpener(error=urllib.error.URLError(OSError("name or service not known")))
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    # When/Then the failure surfaces loudly instead of a fake success
    with pytest.raises(urllib.error.URLError):
        transport(_collector_request(), _API_KEY)


def test_unsafe_credential_is_rejected_before_any_open() -> None:
    # Given a recording opener
    opener = _FakeOpener()
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    # When the credential is empty or header-unsafe, Then nothing is opened
    for credential in ("", "bad\r\nkey"):
        with pytest.raises(ValueError, match="credential"):
            transport(_collector_request(), credential)
    assert opener.calls == []


def test_alphanumeric_credential_query_value_is_rejected_before_open() -> None:
    credential = "abc123SecretKey"
    opener = _FakeOpener()
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    request = CollectorRequest(endpoint="/stable/profile", parameters={"symbol": credential})

    with pytest.raises(HostForbiddenError, match="credential values are forbidden") as caught:
        transport(request, credential)

    assert opener.calls == []
    assert credential not in str(caught.value)
    assert credential not in repr(caught.value)


def test_urlencode_and_quote_credential_query_value_is_rejected_before_open() -> None:
    credential = "sec/ret +key=&?%#"
    opener = _FakeOpener()
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)
    request = CollectorRequest(endpoint="/stable/profile", parameters={"symbol": credential})
    encoded = urlencode({"symbol": credential})
    quoted = quote(credential, safe="")
    plus_quoted = quote_plus(credential, safe="")
    assert encoded != credential
    assert quoted != credential or plus_quoted != credential

    with pytest.raises(HostForbiddenError, match="credential values are forbidden") as caught:
        transport(request, credential)

    assert opener.calls == []
    message = f"{caught.value!s}{caught.value!r}"
    assert credential not in message
    assert quoted not in message
    assert plus_quoted not in message


def test_percent_encoded_credential_forms_are_detected_in_exact_url() -> None:
    credential = "sec/ret +key=&?%#"
    encoded_url = f"{_BASE_URL}/stable/profile?symbol={quote(credential, safe='')}"
    plus_url = f"{_BASE_URL}/stable/profile?symbol={quote_plus(credential, safe='')}"
    assert url_contains_credential(encoded_url, credential)
    assert url_contains_credential(plus_url, credential)
    with pytest.raises(HostForbiddenError, match="credential values are forbidden") as caught:
        refuse_credential_in_url(encoded_url, credential)
    assert credential not in str(caught.value)


def test_header_only_credential_still_opens_a_normal_request() -> None:
    opener = _FakeOpener()
    transport = make_fmp_https_transport(clock=_fixed_clock, opener=opener)

    result = transport(_collector_request(), _API_KEY)

    observed = _single_call(opener)
    assert observed.request.full_url == f"{_BASE_URL}/stable/profile?symbol=AAPL"
    assert _API_KEY not in observed.request.full_url
    assert observed.request.get_header("Apikey") == _API_KEY
    assert result.status_code == _HTTP_OK
