"""Qveris client transport and credential guardrails. Zero network, zero real keys."""

from __future__ import annotations

import hashlib
import io
import json
import os
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from typing import Any, Self, cast

import pytest

from aegis_alpha.data.qveris_client import (
    ALLOWED_PATHS,
    MAX_RESPONSE_BYTES_CEILING,
    QVERIS_BASE_URL,
    QverisClient,
    QverisClientError,
    QverisCredentialEchoError,
    QverisDocumentError,
    QverisEncodingError,
    QverisKeyFileError,
    QverisRedirectError,
    QverisRequestError,
    QverisResponse,
    QverisResponseTooLargeError,
    QverisTransportError,
    RefuseRedirectHandler,
    make_https_opener,
)

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
SYNTHETIC_CREDENTIAL = "unit-test-credential"
HTTP_OK = 200
HTTP_FOUND = 302
HTTP_PAYMENT_REQUIRED = 402
HTTP_SERVICE_UNAVAILABLE = 503


class FakeClock:
    def __init__(self, start: datetime = NOW) -> None:
        self.current = start

    def __call__(self) -> datetime:
        value = self.current
        self.current = value + timedelta(seconds=1)
        return value


def _message(headers: list[tuple[str, str]]) -> Message:
    message = Message()
    for name, value in headers:
        message[name] = value
    return message


@dataclass
class FakeResponse:
    status: int = HTTP_OK
    body: bytes = b"{}"
    header_pairs: list[tuple[str, str]] = field(default_factory=list)
    url: str | None = None
    reads: list[int] = field(default_factory=list)
    closed: bool = False

    @property
    def headers(self) -> Message:
        return _message(self.header_pairs)

    def read(self, amount: int = -1) -> bytes:
        self.reads.append(amount)
        return self.body if amount < 0 else self.body[:amount]

    def geturl(self) -> str:
        return self.url if self.url is not None else ""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True


@dataclass
class FakeOpener:
    """Records every request; returns scripted responses or raises scripted errors."""

    script: list[FakeResponse | BaseException] = field(default_factory=list)
    calls: list[tuple[urllib.request.Request, float | None]] = field(default_factory=list)

    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> FakeResponse:
        self.calls.append((request, timeout))
        if not self.script:
            pytest.fail("unexpected HTTP call")
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if item.url is None:
            item.url = request.full_url
        return item


def _write_key(directory: Path, key: str = SYNTHETIC_CREDENTIAL, *, name: str = "key") -> Path:
    path = directory / name
    path.write_bytes(f"{key}\n".encode("ascii"))
    path.chmod(0o600)
    return path


@pytest.fixture
def key_file(tmp_path: Path) -> Path:
    return _write_key(tmp_path)


@pytest.fixture
def opener() -> FakeOpener:
    return FakeOpener()


@pytest.fixture
def client(key_file: Path, opener: FakeOpener) -> QverisClient:
    return QverisClient(key_file, opener=opener, clock=FakeClock())


def _assert_no_credential(error: BaseException) -> None:
    assert SYNTHETIC_CREDENTIAL not in str(error)
    assert SYNTHETIC_CREDENTIAL not in repr(error)
    assert SYNTHETIC_CREDENTIAL not in repr(error.args)


# --- construction and key file -------------------------------------------------


def test_constructor_reads_key_and_makes_no_http_call(key_file: Path, opener: FakeOpener) -> None:
    client = QverisClient(key_file, opener=opener)
    assert opener.calls == []
    assert client.account_key == hashlib.sha256(SYNTHETIC_CREDENTIAL.encode()).hexdigest()
    assert SYNTHETIC_CREDENTIAL not in repr(client)
    assert client.account_key in repr(client)
    assert not hasattr(client, "__dict__")


def test_default_opener_is_created_lazily(key_file: Path) -> None:
    client = QverisClient(key_file)
    assert client._opener is None  # noqa: SLF001 - no urllib opener until request()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_response_bytes": 0}, "max_response_bytes"),
        ({"max_response_bytes": MAX_RESPONSE_BYTES_CEILING + 1}, "max_response_bytes"),
        ({"max_response_bytes": True}, "max_response_bytes"),
        ({"max_response_bytes": 1.0}, "max_response_bytes"),
        ({"timeout_seconds": 0}, "timeout_seconds"),
        ({"timeout_seconds": float("inf")}, "timeout_seconds"),
        ({"timeout_seconds": float("nan")}, "timeout_seconds"),
        ({"timeout_seconds": 601}, "timeout_seconds"),
        ({"timeout_seconds": True}, "timeout_seconds"),
    ],
)
def test_bounds_are_validated_before_the_key_is_read(
    tmp_path: Path, kwargs: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        QverisClient(tmp_path / "absent", **cast("dict[str, Any]", kwargs))


def test_key_file_symlink_is_refused(tmp_path: Path) -> None:
    real = _write_key(tmp_path, name="real")
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(QverisKeyFileError) as info:
        QverisClient(link)
    _assert_no_credential(info.value)


def test_key_file_symlinked_parent_is_refused(tmp_path: Path) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    _write_key(real_dir)
    link_dir = tmp_path / "alias"
    link_dir.symlink_to(real_dir, target_is_directory=True)
    with pytest.raises(QverisKeyFileError):
        QverisClient(link_dir / "key")


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o644, 0o660, 0o666])
def test_key_file_shared_permissions_are_refused(tmp_path: Path, mode: int) -> None:
    path = _write_key(tmp_path)
    path.chmod(mode)
    with pytest.raises(QverisKeyFileError, match="group or others"):
        QverisClient(path)


def test_key_file_owner_only_modes_are_accepted(tmp_path: Path) -> None:
    path = _write_key(tmp_path)
    path.chmod(0o400)
    assert QverisClient(path).account_key


def test_key_file_hard_link_is_refused(tmp_path: Path) -> None:
    path = _write_key(tmp_path)
    os.link(path, tmp_path / "twin")
    with pytest.raises(QverisKeyFileError):
        QverisClient(path)


def test_key_file_inside_git_is_refused(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    nested = tmp_path / "secrets"
    nested.mkdir()
    path = _write_key(nested)
    with pytest.raises(QverisKeyFileError, match="Git"):
        QverisClient(path)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\n\n",
        b"first\nsecond\n",
        b"has space\n",
        b"Bearer abc\n",
        "\u00e9\n".encode(),
        b"x" * 5000,
    ],
)
def test_malformed_key_files_are_refused(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "key"
    path.write_bytes(payload)
    path.chmod(0o600)
    with pytest.raises(QverisKeyFileError):
        QverisClient(path)


def test_missing_relative_and_directory_key_paths_are_refused(tmp_path: Path) -> None:
    with pytest.raises(QverisKeyFileError):
        QverisClient(tmp_path / "absent")
    with pytest.raises(QverisKeyFileError):
        QverisClient(Path("relative/key"))
    with pytest.raises(QverisKeyFileError):
        QverisClient(tmp_path)


# --- request construction ----------------------------------------------------


def test_get_request_shape(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse(body=b'{"remaining_credits": 3}'))
    response = client.request("/auth/credits")
    ((request, timeout),) = opener.calls
    assert request.full_url == f"{QVERIS_BASE_URL}/auth/credits"
    assert request.get_method() == "GET"
    assert request.data is None
    assert request.get_header("Authorization") == f"Bearer {SYNTHETIC_CREDENTIAL}"
    assert request.get_header("X-qveris-cache-mode") == "default"
    assert request.get_header("Accept-encoding") == "identity"
    assert request.get_header("Accept") == "application/json"
    assert timeout == 55.0  # noqa: PLR2004 - contract default
    assert response.status == HTTP_OK
    assert response.document() == {"remaining_credits": 3}


def test_post_request_shape_and_execute_query(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse(body=b'{"ok": true}'))
    client.request(
        "/tools/execute",
        body={"parameters": {"z": 1, "a": "b"}, "unicode": "\u00e9"},
        query={"tool_id": "eodhd/bulk daily", "page": 2},
    )
    ((request, _timeout),) = opener.calls
    assert request.get_method() == "POST"
    assert (
        request.full_url == f"{QVERIS_BASE_URL}/tools/execute?tool_id=eodhd%2Fbulk%20daily&page=2"
    )
    assert request.get_header("Content-type") == "application/json"
    assert request.data == '{"parameters":{"a":"b","z":1},"unicode":"\u00e9"}'.encode()


def test_empty_body_object_is_a_post(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse())
    client.request("/search", body={})
    assert opener.calls[0][0].get_method() == "POST"
    assert opener.calls[0][0].data == b"{}"


@pytest.mark.parametrize(
    "path",
    [
        "/providers",
        "/auth/login",
        "/auth/credits/",
        "/auth/credits/ledger/export",
        "auth/credits",
        "/Search",
        "/search?x=1",
        "https://qveris.ai/api/v1/search",
        "/../search",
    ],
)
def test_paths_outside_allowlist_make_zero_calls(
    client: QverisClient, opener: FakeOpener, path: str
) -> None:
    with pytest.raises(QverisRequestError, match="allowlist"):
        client.request(path)
    assert opener.calls == []


def test_allowlist_matches_contract() -> None:
    expected = {
        "/search",
        "/tools/by-ids",
        "/tools/probe",
        "/tools/execute",
        "/auth/credits",
        "/auth/usage/history/v2",
        "/auth/credits/ledger",
    }
    assert expected == ALLOWED_PATHS


@pytest.mark.parametrize("path", ["/tools/execute", "/tools/probe"])
def test_tool_paths_require_tool_id(client: QverisClient, opener: FakeOpener, path: str) -> None:
    with pytest.raises(QverisRequestError, match="tool_id"):
        client.request(path, body={})
    with pytest.raises(QverisRequestError, match="tool_id"):
        client.request(path, body={}, query={"other": "x"})
    assert opener.calls == []


@pytest.mark.parametrize(
    "query",
    [
        {"api_key": "x"},
        {"API_KEY": "x"},
        {"token": "x"},
        {"Authorization": "x"},
        {"tool_id": SYNTHETIC_CREDENTIAL},
        {"tool_id": f"prefix-{SYNTHETIC_CREDENTIAL}-suffix"},
        {"tool id": "x"},
        {"tool_id=1&x": "y"},
        {"": "x"},
        {"tool_id": 1.5},
        {"tool_id": True},
        {"tool_id": None},
        {"tool_id": "line\nbreak"},
        {"tool_id": "x" * 5000},
        {"\u00e9": "x"},
    ],
)
def test_unsafe_queries_make_zero_calls(
    client: QverisClient, opener: FakeOpener, query: dict[str, object]
) -> None:
    with pytest.raises(QverisRequestError) as info:
        client.request("/tools/probe", body={}, query=cast("dict[str, str]", query))
    _assert_no_credential(info.value)
    assert opener.calls == []


def test_query_values_are_fully_percent_encoded(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse())
    client.request("/auth/credits/ledger", query={"scope": "a&b=c#d/e?f", "page": 0})
    assert (
        opener.calls[0][0].full_url
        == f"{QVERIS_BASE_URL}/auth/credits/ledger?scope=a%26b%3Dc%23d%2Fe%3Ff&page=0"
    )


@pytest.mark.parametrize(
    "body",
    [
        {"query": SYNTHETIC_CREDENTIAL},
        {"nested": {"deep": [SYNTHETIC_CREDENTIAL]}},
        {SYNTHETIC_CREDENTIAL: "value"},
        {"x": float("nan")},
        {"x": float("inf")},
        {"x": object()},
        {"x": datetime(2026, 1, 1, tzinfo=UTC)},
        {1: "x"},
        {"blob": "y" * (4 * 1024 * 1024 + 1)},
    ],
)
def test_unsafe_bodies_make_zero_calls(
    client: QverisClient, opener: FakeOpener, body: dict[object, object]
) -> None:
    with pytest.raises(QverisRequestError) as info:
        client.request("/search", body=cast("dict[str, object]", body))
    _assert_no_credential(info.value)
    assert opener.calls == []


def test_list_body_is_rejected(client: QverisClient, opener: FakeOpener) -> None:
    with pytest.raises(QverisRequestError):
        client.request("/search", body=cast("dict[str, object]", [1, 2]))
    assert opener.calls == []


# --- response handling -------------------------------------------------------


def test_non_2xx_is_preserved_raw_not_raised(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(
        FakeResponse(
            status=HTTP_PAYMENT_REQUIRED,
            body=b'{"detail": "insufficient credits"}',
            header_pairs=[
                ("Content-Type", "application/json"),
                ("X-QVeris-API-Version", "2026-09"),
                ("Retry-After", "30"),
                ("Set-Cookie", "session=abc"),
                ("X-Request-Id", "r-1"),
                ("Date", "Sun, 06 Sep 2026 12:00:00 GMT"),
            ],
        )
    )
    response = client.request("/tools/execute", body={}, query={"tool_id": "t"})
    assert response.status == HTTP_PAYMENT_REQUIRED
    assert response.body == b'{"detail": "insufficient credits"}'
    assert response.headers == (
        ("content-type", "application/json"),
        ("x-qveris-api-version", "2026-09"),
        ("retry-after", "30"),
        ("date", "Sun, 06 Sep 2026 12:00:00 GMT"),
    )
    assert response.header("Retry-After") == "30"
    assert response.header("set-cookie") is None
    assert response.requested_at_utc == NOW
    assert response.retrieved_at_utc == NOW + timedelta(seconds=1)


def test_urllib_http_error_is_consumed_raw(client: QverisClient, opener: FakeOpener) -> None:
    error = urllib.error.HTTPError(
        f"{QVERIS_BASE_URL}/auth/credits",
        HTTP_SERVICE_UNAVAILABLE,
        "unavailable",
        _message([("Retry-After", "5"), ("Content-Type", "text/plain")]),
        io.BytesIO(b"down"),
    )
    opener.script.append(error)
    response = client.request("/auth/credits")
    assert response.status == HTTP_SERVICE_UNAVAILABLE
    assert response.body == b"down"
    assert response.header("retry-after") == "5"
    assert len(opener.calls) == 1


def test_response_body_echoing_key_is_discarded(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse(body=f'{{"echo": "{SYNTHETIC_CREDENTIAL}"}}'.encode()))
    with pytest.raises(QverisCredentialEchoError) as info:
        client.request("/auth/credits")
    _assert_no_credential(info.value)


def test_percent_encoded_key_echo_is_discarded(client: QverisClient, opener: FakeOpener) -> None:
    from urllib.parse import quote  # noqa: PLC0415 - test-local encoding helper

    body = json.dumps({"url": "https://x/?k=" + quote(SYNTHETIC_CREDENTIAL + "/x", safe="")})
    opener.script.append(FakeResponse(body=body.encode()))
    with pytest.raises(QverisCredentialEchoError):
        client.request("/auth/credits")


def test_response_header_echoing_key_is_discarded(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse(header_pairs=[("Date", SYNTHETIC_CREDENTIAL)]))
    with pytest.raises(QverisCredentialEchoError) as info:
        client.request("/auth/credits")
    _assert_no_credential(info.value)


@pytest.mark.parametrize("status", [301, HTTP_FOUND, 303, 307, 308])
def test_redirect_statuses_are_refused(
    client: QverisClient, opener: FakeOpener, status: int
) -> None:
    opener.script.append(
        FakeResponse(status=status, header_pairs=[("Location", "https://evil.example/")])
    )
    with pytest.raises(QverisRedirectError):
        client.request("/auth/credits")
    assert len(opener.calls) == 1


def test_urllib_redirect_handler_refuses_before_follow() -> None:
    handler = RefuseRedirectHandler()
    request = urllib.request.Request(f"{QVERIS_BASE_URL}/auth/credits")  # noqa: S310
    with pytest.raises(QverisRedirectError):
        handler.redirect_request(request, None, HTTP_FOUND, "Found", None, "https://evil/")


@pytest.mark.parametrize(
    "final_url",
    [
        "https://evil.example/api/v1/auth/credits",
        "http://qveris.ai/api/v1/auth/credits",
        QVERIS_BASE_URL.replace("://", "://user:pw@") + "/auth/credits",
        "https://qveris.ai:8443/api/v1/auth/credits",
        f"{QVERIS_BASE_URL}/auth/credits/ledger",
    ],
)
def test_final_url_drift_is_refused(
    client: QverisClient, opener: FakeOpener, final_url: str
) -> None:
    opener.script.append(FakeResponse(url=final_url))
    with pytest.raises(QverisRedirectError) as info:
        client.request("/auth/credits")
    assert "pw" not in str(info.value)


def test_declared_oversize_is_refused_without_reading(key_file: Path, opener: FakeOpener) -> None:
    client = QverisClient(key_file, opener=opener, max_response_bytes=16)
    response = FakeResponse(body=b"x" * 8, header_pairs=[("Content-Length", "17")])
    opener.script.append(response)
    with pytest.raises(QverisResponseTooLargeError):
        client.request("/auth/credits")
    assert response.reads == []


def test_undeclared_oversize_is_bounded_by_read(key_file: Path, opener: FakeOpener) -> None:
    client = QverisClient(key_file, opener=opener, max_response_bytes=16)
    response = FakeResponse(body=b"x" * 1000)
    opener.script.append(response)
    with pytest.raises(QverisResponseTooLargeError):
        client.request("/auth/credits")
    assert response.reads == [17]


def test_exact_max_size_is_accepted(key_file: Path, opener: FakeOpener) -> None:
    client = QverisClient(key_file, opener=opener, max_response_bytes=16)
    opener.script.append(FakeResponse(body=b"x" * 16, header_pairs=[("Content-Length", "16")]))
    assert client.request("/auth/credits").body == b"x" * 16


@pytest.mark.parametrize("encoding", ["gzip", "br", "deflate", "GZIP", "identity, gzip"])
def test_compressed_responses_are_refused(
    client: QverisClient, opener: FakeOpener, encoding: str
) -> None:
    opener.script.append(FakeResponse(header_pairs=[("Content-Encoding", encoding)]))
    with pytest.raises(QverisEncodingError):
        client.request("/auth/credits")


def test_identity_encoding_is_accepted(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(FakeResponse(header_pairs=[("Content-Encoding", "identity")]))
    assert client.request("/auth/credits").status == HTTP_OK


@pytest.mark.parametrize(
    "failure",
    [
        urllib.error.URLError(TimeoutError("timed out")),
        TimeoutError("timed out"),
        ConnectionResetError("reset"),
        urllib.error.URLError(f"secret {SYNTHETIC_CREDENTIAL}"),
    ],
)
def test_transport_failures_are_sanitized_and_never_retried(
    client: QverisClient, opener: FakeOpener, failure: BaseException
) -> None:
    opener.script.append(failure)
    with pytest.raises(QverisTransportError) as info:
        client.request("/auth/credits")
    _assert_no_credential(info.value)
    assert "timed out" not in str(info.value)
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True
    assert len(opener.calls) == 1
    assert opener.script == []


def test_timeout_failure_names_its_class(client: QverisClient, opener: FakeOpener) -> None:
    opener.script.append(urllib.error.URLError(TimeoutError("t")))
    with pytest.raises(QverisTransportError, match="URLError/TimeoutError"):
        client.request("/auth/credits")


def test_every_client_error_is_one_family() -> None:
    for kind in (
        QverisKeyFileError,
        QverisRequestError,
        QverisRedirectError,
        QverisResponseTooLargeError,
        QverisEncodingError,
        QverisCredentialEchoError,
        QverisTransportError,
    ):
        assert issubclass(kind, QverisClientError)


def test_default_opener_refuses_redirects_and_verifies_tls() -> None:
    opener = make_https_opener()
    assert isinstance(opener, urllib.request.OpenerDirector)
    handlers = cast("list[object]", getattr(opener, "handlers", []))
    kinds = {type(handler) for handler in handlers}
    assert RefuseRedirectHandler in kinds
    assert urllib.request.HTTPRedirectHandler not in kinds
    https = next(h for h in handlers if isinstance(h, urllib.request.HTTPSHandler))
    context = cast("ssl.SSLContext", getattr(https, "_context"))  # noqa: B009 - private stdlib attr
    assert context.check_hostname is True
    assert context.verify_mode.name == "CERT_REQUIRED"


# --- QverisResponse and document() -------------------------------------------


def _response(body: bytes) -> QverisResponse:
    return QverisResponse(
        status=HTTP_OK,
        headers=(("content-type", "application/json"),),
        body=body,
        requested_at_utc=NOW,
        retrieved_at_utc=NOW,
    )


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"not json",
        b"[1, 2]",
        b'"string"null',
        b'{"a": 1, "a": 2}',
        b'{"a": NaN}',
        b'{"a": Infinity}',
        b'{"a": -Infinity}',
        b'{"nested": {"k": 1, "k": 1}}',
        b'{"a": 1} trailing',
        b"\xff\xfe",
    ],
)
def test_document_rejects_non_strict_json(body: bytes) -> None:
    with pytest.raises(QverisDocumentError):
        _response(body).document()


def test_document_accepts_strict_object() -> None:
    assert _response(b'{"b": [1, 2.5, "\\u00e9"], "a": {"n": null}}').document() == {
        "b": [1, 2.5, "\u00e9"],
        "a": {"n": None},
    }


def test_response_is_frozen_and_slotted() -> None:
    response = _response(b"{}")
    with pytest.raises(AttributeError):
        setattr(response, "status", 500)  # noqa: B010 - frozen dataclass mutation attempt
    assert not hasattr(response, "__dict__")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": 99},
        {"status": 600},
        {"status": "200"},
        {"body": "text"},
        {"headers": [("content-type", "x")]},
        {"headers": (("set-cookie", "x"),)},
        {"headers": (("Content-Type", "x"),)},
        {"requested_at_utc": datetime(2026, 9, 6, 12, 0)},  # noqa: DTZ001 - naive on purpose
        {"retrieved_at_utc": NOW - timedelta(seconds=1)},
    ],
)
def test_response_validates_fields(kwargs: dict[str, object]) -> None:
    fields: dict[str, object] = {
        "status": HTTP_OK,
        "headers": (),
        "body": b"{}",
        "requested_at_utc": NOW,
        "retrieved_at_utc": NOW,
    }
    fields.update(kwargs)
    with pytest.raises((ValueError, TypeError)):
        QverisResponse(**fields)  # type: ignore[arg-type]
