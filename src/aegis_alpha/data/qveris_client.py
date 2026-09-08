"""Bounded Qveris HTTPS client: fixed host, allowlisted paths, private key file.

The client performs exactly one HTTP exchange per ``request`` call. It never
follows redirects, never retries, never decompresses, and preserves non-2xx
responses as raw ``QverisResponse`` values so the caller can record them.
The API key is read descriptor-relative from a private ``0600`` regular file
outside Git at construction time; it is sent only as a bearer header and is
rejected if it reappears in a URL, a request body, or any response byte.
Every error raised by this module is credential-free by construction.

Tests inject a fake opener and a fake clock; no socket is opened in tests.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import ssl
import stat
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Protocol, Self
from urllib.parse import quote, urlencode, urlsplit

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError

QVERIS_BASE_URL: Final = "https://qveris.ai/api/v1"
ALLOWED_HOST: Final = "qveris.ai"
QVERIS_CACHE_MODE: Final = "default"
ALLOWED_PATHS: Final = frozenset(
    {
        "/search",
        "/tools/by-ids",
        "/tools/probe",
        "/tools/execute",
        "/auth/credits",
        "/auth/usage/history/v2",
        "/auth/credits/ledger",
    }
)
#: Paths whose query string must carry ``tool_id`` (the OpenAPI query parameter).
_TOOL_ID_PATHS: Final = frozenset({"/tools/execute", "/tools/probe"})
#: Response headers exposed to callers, lowercase, in wire order.
OUTPUT_HEADERS: Final = (
    "content-type",
    "date",
    "content-length",
    "x-qveris-api-version",
    "retry-after",
)
MAX_RESPONSE_BYTES_CEILING: Final = 128 * 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES: Final = MAX_RESPONSE_BYTES_CEILING
DEFAULT_TIMEOUT_SECONDS: Final = 55.0
MAX_TIMEOUT_SECONDS: Final = 600.0
MAX_KEY_FILE_BYTES: Final = 4096
MAX_QUERY_VALUE_LENGTH: Final = 4096
MAX_REQUEST_BODY_BYTES: Final = 4 * 1024 * 1024
_USER_AGENT: Final = "aegis-alpha-system/qveris-client"
_HTTPS_PORT: Final = 443
_STATUS_REDIRECT_LOW: Final = 300
_STATUS_REDIRECT_HIGH: Final = 400
_STATUS_INFORMATIONAL_HIGH: Final = 200
_STATUS_MIN: Final = 100
_STATUS_MAX: Final = 599
_HEADER_PAIR_LENGTH: Final = 2
_GROUP_OTHER_MASK: Final = 0o077
#: Query names that would carry a credential; refused regardless of value.
_CREDENTIAL_QUERY_NAMES: Final = frozenset(
    {"api_key", "apikey", "api-key", "token", "access_token", "authorization", "bearer", "key"}
)


class QverisClientError(RuntimeError):
    """Base class; messages never contain the key, a URL with userinfo, or a body."""


class QverisKeyFileError(QverisClientError):
    """The key file is missing, aliased, shared, in Git, or malformed."""


class QverisRequestError(QverisClientError):
    """A request was rejected before any network activity."""


class QverisRedirectError(QverisClientError):
    """The server answered with a redirect, which this client never follows."""


class QverisResponseTooLargeError(QverisClientError):
    """The response body exceeds ``max_response_bytes``."""


class QverisEncodingError(QverisClientError):
    """The response used a content encoding this client does not decode."""


class QverisCredentialEchoError(QverisClientError):
    """The API key reappeared in a response; the response is discarded."""


class QverisTransportError(QverisClientError):
    """The exchange failed below HTTP; carries only the failure class name."""

    def __init__(self, cause: BaseException) -> None:
        reason = getattr(cause, "reason", None)
        label = type(cause).__name__
        if isinstance(reason, BaseException):
            label = f"{label}/{type(reason).__name__}"
        super().__init__(f"Qveris transport failure: {label}")
        self.failure_class = label


class QverisDocumentError(ValueError):
    """The response body is not a strict JSON object."""


class RawHeaders(Protocol):
    """Header collection whose ``items`` preserves wire order and duplicates."""

    def items(self) -> Iterable[tuple[str, str]]: ...


class RetrievedHttpResponse(Protocol):
    """The urllib response surface used after open, without assuming a socket."""

    status: int

    @property
    def headers(self) -> RawHeaders: ...

    def read(self, amount: int = -1) -> bytes: ...

    @property
    def url(self) -> str | None: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> bool | None: ...


class UrlOpener(Protocol):
    """Minimal opener used by the live client and by socket-free tests."""

    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> RetrievedHttpResponse: ...


class RefuseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse 3xx before urllib can follow it anywhere."""

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
        raise QverisRedirectError("Qveris client refuses HTTP redirects")


def make_https_opener() -> UrlOpener | urllib.request.OpenerDirector:
    """Build a redirect-refusing urllib opener with stdlib certificate verification."""

    context = ssl.create_default_context()
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return urllib.request.build_opener(
        RefuseRedirectHandler,
        urllib.request.HTTPSHandler(context=context),
    )


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _require_aware(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TypeError(f"{label} must be a timezone-aware datetime")
    return value


def _reject_nonfinite(token: str) -> object:
    raise QverisDocumentError(f"Qveris document contains a non-finite number: {token}")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise QverisDocumentError("Qveris document contains a duplicate object key")
        document[key] = value
    return document


@dataclass(frozen=True, slots=True)
class QverisResponse:
    """One raw HTTP exchange; ``headers`` holds only the allowlisted names."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    requested_at_utc: datetime
    retrieved_at_utc: datetime

    def __post_init__(self) -> None:
        if type(self.status) is not int or not _STATUS_MIN <= self.status <= _STATUS_MAX:
            raise ValueError("Qveris response status must be an HTTP status code")
        if type(self.body) is not bytes:
            raise TypeError("Qveris response body must be bytes")
        if type(self.headers) is not tuple:
            raise TypeError("Qveris response headers must be a tuple of pairs")
        for pair in self.headers:
            if (
                type(pair) is not tuple
                or len(pair) != _HEADER_PAIR_LENGTH
                or type(pair[0]) is not str
                or type(pair[1]) is not str
                or pair[0] not in OUTPUT_HEADERS
            ):
                raise ValueError("Qveris response headers must be allowlisted lowercase pairs")
        _require_aware(self.requested_at_utc, label="requested_at_utc")
        _require_aware(self.retrieved_at_utc, label="retrieved_at_utc")
        if self.retrieved_at_utc < self.requested_at_utc:
            raise ValueError("Qveris response cannot be retrieved before it was requested")

    def header(self, name: str) -> str | None:
        """First value of an allowlisted header, or ``None``."""

        wanted = name.casefold()
        for header_name, value in self.headers:
            if header_name == wanted:
                return value
        return None

    def document(self) -> dict[str, object]:
        """Decode the body as one strict UTF-8 JSON object."""

        try:
            text = self.body.decode("utf-8")
        except UnicodeDecodeError as error:
            raise QverisDocumentError("Qveris document is not valid UTF-8") from error
        try:
            parsed = json.loads(
                text,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite,
            )
        except json.JSONDecodeError as error:
            raise QverisDocumentError("Qveris document is not valid JSON") from error
        if not isinstance(parsed, dict):
            raise QverisDocumentError("Qveris document must be a JSON object")
        return parsed


def _read_key_file(key_file: Path) -> str:
    if not isinstance(key_file, Path) or not key_file.is_absolute() or ".." in key_file.parts:
        raise QverisKeyFileError("Qveris key file must be an absolute path without traversal")
    if key_file.name in {"", ".", ".."}:
        raise QverisKeyFileError("Qveris key file must name a regular file")
    if any((parent / ".git").exists() for parent in (key_file.parent, *key_file.parents)):
        raise QverisKeyFileError("Qveris key file must be outside a Git repository")
    try:
        with (
            DescriptorTree.open_path(key_file.parent) as tree,
            tree.binary_reader(key_file.name, require_single_link=True) as handle,
        ):
            _require_private(os.fstat(handle.fileno()))
            payload = handle.read(MAX_KEY_FILE_BYTES + 1)
    except DescriptorTreeError as error:
        raise QverisKeyFileError(
            "Qveris key file must be a plain, unaliased regular file"
        ) from error
    except OSError as error:
        raise QverisKeyFileError(
            f"Qveris key file cannot be read: {type(error).__name__}"
        ) from error
    if len(payload) > MAX_KEY_FILE_BYTES:
        raise QverisKeyFileError("Qveris key file is too large")
    return _parse_key(payload)


def _require_private(metadata: os.stat_result) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise QverisKeyFileError("Qveris key file must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & _GROUP_OTHER_MASK:
        raise QverisKeyFileError("Qveris key file must not be readable by group or others")
    if metadata.st_uid != os.geteuid():
        raise QverisKeyFileError("Qveris key file must be owned by the current user")
    if metadata.st_nlink != 1:
        raise QverisKeyFileError("Qveris key file must not be hard-linked")


def _parse_key(payload: bytes) -> str:
    try:
        text = payload.decode("ascii")
    except UnicodeDecodeError as error:
        raise QverisKeyFileError("Qveris key file must be ASCII") from error
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise QverisKeyFileError("Qveris key file must contain exactly one non-empty line")
    key = lines[0].strip()
    if not key.isprintable() or any(character.isspace() for character in key):
        raise QverisKeyFileError("Qveris key must be printable without whitespace")
    if key.casefold().startswith("bearer "):
        raise QverisKeyFileError("Qveris key file must hold the bare key")
    return key


def _credential_forms(key: str) -> tuple[bytes, ...]:
    forms = {key, quote(key, safe=""), quote(key, safe="/")}
    return tuple(form.encode("utf-8") for form in forms if form)


def _contains_credential(haystack: bytes, forms: Iterable[bytes]) -> bool:
    return any(form in haystack for form in forms)


def _query_name_ok(name: object) -> bool:
    return (
        type(name) is str
        and bool(name)
        and name.isascii()
        and all(character.isalnum() or character in "_-." for character in name)
    )


def _validate_query(query: Mapping[str, str | int] | None, forms: tuple[bytes, ...]) -> str:
    if query is None:
        return ""
    if not isinstance(query, Mapping):
        raise QverisRequestError("Qveris query must be a mapping")
    pairs: list[tuple[str, str]] = []
    for name, value in query.items():
        if not _query_name_ok(name):
            raise QverisRequestError("Qveris query names must be simple ASCII identifiers")
        if name.casefold() in _CREDENTIAL_QUERY_NAMES:
            raise QverisRequestError("Qveris query must not carry a credential parameter")
        if type(value) is int:
            rendered = str(value)
        elif type(value) is str:
            rendered = value
        else:
            raise QverisRequestError("Qveris query values must be str or int")
        if not rendered.isprintable() or len(rendered) > MAX_QUERY_VALUE_LENGTH:
            raise QverisRequestError("Qveris query values must be short printable text")
        if _contains_credential(rendered.encode("utf-8"), forms):
            raise QverisRequestError("Qveris query must not contain the API key")
        pairs.append((name, rendered))
    return urlencode(pairs, quote_via=quote, safe="")


def _validate_path(path: str) -> str:
    if type(path) is not str or path not in ALLOWED_PATHS:
        raise QverisRequestError("Qveris path is outside the allowlist")
    return path


def _encode_body(body: Mapping[str, object] | None, forms: tuple[bytes, ...]) -> bytes | None:
    if body is None:
        return None
    if not isinstance(body, Mapping) or any(type(name) is not str for name in body):
        raise QverisRequestError("Qveris request body must be a JSON object with string keys")
    try:
        payload = json.dumps(
            body,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise QverisRequestError("Qveris request body must be strict JSON") from error
    if len(payload) > MAX_REQUEST_BODY_BYTES:
        raise QverisRequestError("Qveris request body is too large")
    if _contains_credential(payload, forms):
        raise QverisRequestError("Qveris request body must not contain the API key")
    return payload


def _require_expected_url(url: str, expected: str) -> None:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as error:
        raise QverisRedirectError("Qveris response URL has an invalid port") from error
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").casefold() != ALLOWED_HOST
        or parsed.username is not None
        or parsed.password is not None
        or (port is not None and port != _HTTPS_PORT)
        or url != expected
    ):
        raise QverisRedirectError("Qveris response URL differs from the requested URL")


def _select_headers(raw: Iterable[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    selected: list[tuple[str, str]] = []
    for name, value in raw:
        lowered = str(name).casefold()
        if lowered in OUTPUT_HEADERS:
            selected.append((lowered, str(value).replace("\r", " ").replace("\n", " ").strip()))
    return tuple(selected)


def _content_length(raw: Iterable[tuple[str, str]]) -> int | None:
    for name, value in raw:
        if str(name).casefold() == "content-length":
            try:
                return int(str(value).strip())
            except ValueError:
                return None
    return None


def _require_identity_encoding(raw: Iterable[tuple[str, str]]) -> None:
    for name, value in raw:
        if str(name).casefold() == "content-encoding":
            encoding = str(value).strip().casefold()
            if encoding not in {"", "identity"}:
                raise QverisEncodingError("Qveris client does not decode compressed responses")


class QverisClient:
    """Sequential, bounded HTTPS access to ``https://qveris.ai/api/v1``."""

    __slots__ = (
        "_account_key",
        "_clock",
        "_forms",
        "_key",
        "_max_response_bytes",
        "_opener",
        "_timeout",
    )

    def __init__(
        self,
        key_file: Path,
        *,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        opener: UrlOpener | urllib.request.OpenerDirector | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if (
            type(max_response_bytes) is not int
            or not 1 <= max_response_bytes <= MAX_RESPONSE_BYTES_CEILING
        ):
            raise ValueError("max_response_bytes must be an int within the 128 MiB ceiling")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= MAX_TIMEOUT_SECONDS
        ):
            raise ValueError("timeout_seconds must be finite, positive, and at most 600")
        key = _read_key_file(key_file)
        self._key = key
        self._forms = _credential_forms(key)
        self._account_key = hashlib.sha256(key.encode("utf-8")).hexdigest()
        self._max_response_bytes = max_response_bytes
        self._timeout = float(timeout_seconds)
        self._opener = opener
        self._clock = _utc_now if clock is None else clock

    @property
    def account_key(self) -> str:
        """Non-secret SHA-256 of the key; identifies the cooperating account lock."""

        return self._account_key

    @property
    def max_response_bytes(self) -> int:
        return self._max_response_bytes

    @property
    def timeout_seconds(self) -> float:
        return self._timeout

    def __repr__(self) -> str:
        return (
            f"QverisClient(account_key={self._account_key!r}, "
            f"max_response_bytes={self._max_response_bytes}, "
            f"timeout_seconds={self._timeout})"
        )

    def _build_request(
        self,
        path: str,
        body: Mapping[str, object] | None,
        query: Mapping[str, str | int] | None,
    ) -> tuple[urllib.request.Request, str]:
        resolved = _validate_path(path)
        encoded_query = _validate_query(query, self._forms)
        if resolved in _TOOL_ID_PATHS and (query is None or "tool_id" not in query):
            raise QverisRequestError("Qveris tool paths require a tool_id query parameter")
        payload = _encode_body(body, self._forms)
        url = QVERIS_BASE_URL + resolved + ("?" + encoded_query if encoded_query else "")
        if _contains_credential(url.encode("utf-8"), self._forms):
            raise QverisRequestError("Qveris URL must not contain the API key")
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {self._key}",
            "User-Agent": _USER_AGENT,
            "X-QVeris-Cache-Mode": QVERIS_CACHE_MODE,
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(  # noqa: S310 - fixed https host above
            url,
            data=payload,
            headers=headers,
            method="GET" if payload is None else "POST",
        )
        return request, url

    def request(
        self,
        path: str,
        *,
        body: Mapping[str, object] | None = None,
        query: Mapping[str, str | int] | None = None,
    ) -> QverisResponse:
        """Perform one bounded exchange; non-2xx bodies are returned, not raised."""

        request, url = self._build_request(path, body, query)
        if self._opener is None:
            self._opener = make_https_opener()
        requested_at = self._clock()
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                status, raw_headers, payload = self._consume(response, url)
        except urllib.error.HTTPError as error:
            try:
                status, raw_headers, payload = self._consume(error, url)
            finally:
                error.close()
        except QverisClientError:
            raise
        except (urllib.error.URLError, OSError, ssl.SSLError) as error:
            raise QverisTransportError(error) from None
        retrieved_at = self._clock()
        if _contains_credential(payload, self._forms):
            raise QverisCredentialEchoError("Qveris response echoed the API key; discarded")
        headers = _select_headers(raw_headers)
        for _name, value in headers:
            if _contains_credential(value.encode("utf-8"), self._forms):
                raise QverisCredentialEchoError("Qveris response header echoed the API key")
        return QverisResponse(
            status=status,
            headers=headers,
            body=payload,
            requested_at_utc=requested_at,
            retrieved_at_utc=retrieved_at,
        )

    def _consume(
        self,
        response: RetrievedHttpResponse | urllib.error.HTTPError,
        url: str,
    ) -> tuple[int, tuple[tuple[str, str], ...], bytes]:
        status = int(getattr(response, "status", None) or getattr(response, "code", 0))
        if _STATUS_REDIRECT_LOW <= status < _STATUS_REDIRECT_HIGH:
            raise QverisRedirectError("Qveris client refuses HTTP redirects")
        if status < _STATUS_INFORMATIONAL_HIGH:
            raise QverisTransportError(OSError("informational status"))
        final_url = response.url
        if final_url:
            _require_expected_url(final_url, url)
        header_source = response.headers
        raw_headers = tuple(header_source.items()) if header_source is not None else ()
        _require_identity_encoding(raw_headers)
        declared = _content_length(raw_headers)
        if declared is not None and declared > self._max_response_bytes:
            raise QverisResponseTooLargeError("Qveris response exceeds max_response_bytes")
        payload = response.read(self._max_response_bytes + 1)
        if type(payload) is not bytes:
            raise QverisTransportError(TypeError("non-bytes body"))
        if len(payload) > self._max_response_bytes:
            raise QverisResponseTooLargeError("Qveris response exceeds max_response_bytes")
        return status, raw_headers, payload
