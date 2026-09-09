"""Loopback browser transport. Never a public or multi-user API."""

from __future__ import annotations

# ruff: noqa: C901, PLR0912, TRY301 -- one HTTP exception boundary maps validation failures.
import json
import re
from contextlib import suppress
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib.resources import files
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, urlsplit

from aegis_alpha.console.catalog import Catalog
from aegis_alpha.console.registry import ConflictError, Registry

_MAX_IDENTIFIER = 240
_BODY_LIMIT = 32768
_RESPONSE_LIMIT = 4 * 1024 * 1024
_ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
}
_CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self'; object-src 'none'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
)


def tailscale_origin(value: str) -> str:
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if not re.fullmatch(rf"https://{label}\.{label}\.ts\.net(?::[1-9][0-9]{{0,4}})?", value):
        raise ValueError("expected an exact Tailscale HTTPS origin without a path")
    if urlsplit(value).port in {0, 443}:
        raise ValueError("invalid port")
    return value


class ConsoleServer(HTTPServer):
    def __init__(
        self, home: Path, registry: Registry, port: int = 0, *, remote_origin: str | None = None
    ) -> None:
        self.remote_origin = tailscale_origin(remote_origin) if remote_origin else None
        self.catalog = Catalog(home)
        self.registry = registry
        super().__init__(("127.0.0.1", port), Handler)


class Handler(BaseHTTPRequestHandler):
    server: ConsoleServer
    timeout = 5

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 -- inherited keyword
        """Do not log private query identifiers or browser request content."""

    def _send(self, status: HTTPStatus, payload: bytes, mime: str) -> None:
        self.send_response(status)
        for key, value in {
            "Content-Type": mime,
            "Content-Length": str(len(payload)),
            "Cache-Control": "no-store",
            "Content-Security-Policy": _CSP,
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "X-Frame-Options": "DENY",
            "Cross-Origin-Resource-Policy": "same-origin",
        }.items():
            self.send_header(key, value)
        with suppress(BrokenPipeError, ConnectionResetError):
            self.end_headers()
            self.wfile.write(payload)

    def _json(self, status: HTTPStatus, value: object) -> None:
        payload = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        if len(payload) > _RESPONSE_LIMIT:
            self._error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "response_limit",
                "표가 너무 큽니다. 더 작은 원본 표를 선택해주세요.",
            )
            return
        self._send(status, payload, "application/json; charset=utf-8")

    def _error(self, status: HTTPStatus, code: str, message: str) -> None:
        self._json(status, {"error": {"code": code, "message": message}})

    def _admit(self, *, mutation: bool = False) -> bool:
        hosts = {f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}"}
        origins = {host: f"http://{host}" for host in hosts}
        if self.server.remote_origin:
            origins[urlsplit(self.server.remote_origin).netloc] = self.server.remote_origin
        host = self.headers.get("Host", "")
        origin = self.headers.get("Origin")
        expected = origins.get(host)
        valid = (
            len(self.headers.get_all("Host", [])) == 1
            and host in origins
            and self.headers.get("Sec-Fetch-Site", "none") in {"none", "same-origin"}
            and (origin is None or origin == expected)
        )
        if mutation:
            valid = valid and origin == expected and self.headers.get("X-AAS-Console") == "1"
        if not valid:
            self._error(
                HTTPStatus.FORBIDDEN,
                "origin_denied",
                "이 주소에서 접근할 수 없습니다. 로컬 콘솔 주소로 다시 열어주세요.",
            )
        return valid

    def do_GET(self) -> None:
        if not self._admit():
            return
        try:
            route = urlsplit(self.path)
            if route.path in _ASSETS and not route.query:
                name, mime = _ASSETS[route.path]
                self._send(
                    HTTPStatus.OK,
                    files("aegis_alpha.console").joinpath("static", name).read_bytes(),
                    mime,
                )
                return
            if route.path == "/api/overview" and not route.query:
                value = self.server.catalog.overview()
            elif route.path == "/api/resources" and not route.query:
                value = {"resources": self.server.registry.resources()}
            elif route.path == "/api/table":
                query = parse_qs(route.query, strict_parsing=True)
                if set(query) != {"source", "table"} or any(
                    len(v) != 1 or len(v[0]) > _MAX_IDENTIFIER for v in query.values()
                ):
                    raise ValueError("조회할 원본과 표를 확인해주세요.")
                value = self.server.catalog.sample(query["source"][0], query["table"][0])
            elif route.path in {"/api/strategies", "/api/coverage"}:
                query = parse_qs(route.query, strict_parsing=True, keep_blank_values=True)
                allowed = (
                    {"collection", "q", "country", "offset"}
                    if route.path.endswith("strategies")
                    else {"q", "category", "offset"}
                )
                if set(query) - allowed or any(
                    len(values) != 1 or len(values[0]) > _MAX_IDENTIFIER
                    for values in query.values()
                ):
                    raise ValueError("조회 매개변수를 확인해주세요.")
                offset = int(query.get("offset", ["0"])[0])
                if offset < 0:
                    raise ValueError("조회 매개변수를 확인해주세요.")
                if route.path.endswith("strategies"):
                    value = self.server.catalog.strategies(
                        collection=query.get("collection", [None])[0],
                        query=query.get("q", [""])[0],
                        country=query.get("country", [""])[0],
                        offset=offset,
                    )
                else:
                    value = self.server.catalog.coverage(
                        query=query.get("q", [""])[0],
                        category=query.get("category", [""])[0],
                        offset=offset,
                    )
            else:
                self._error(
                    HTTPStatus.NOT_FOUND, "not_found", "요청한 화면이나 항목을 찾을 수 없습니다."
                )
                return
            self._json(HTTPStatus.OK, {"data": value})
        except (ValueError, TypeError, OSError, RuntimeError, ImportError, LookupError) as error:
            self._failure(error)

    def _body(self) -> dict[str, object]:
        if self.headers.get("Content-Type") != "application/json" or self.headers.get(
            "Transfer-Encoding"
        ):
            raise ValueError("JSON 형식으로 요청해주세요.")
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not lengths[0].isdigit():
            raise ValueError("요청 길이가 잘못됐습니다.")
        length = int(lengths[0])
        if not 0 < length <= _BODY_LIMIT:
            raise ValueError("요청 크기가 너무 큽니다.")
        body = json.loads(self.rfile.read(length))
        if not isinstance(body, dict):
            raise TypeError("입력은 JSON 객체여야 합니다.")
        return cast("dict[str, object]", body)

    def _mutate(self, *, create: bool) -> None:
        if not self._admit(mutation=True):
            return
        try:
            route = urlsplit(self.path)
            if route.query:
                raise ValueError("요청 주소가 잘못됐습니다.")
            if create and route.path == "/api/resources":
                value = self.server.registry.create(self._body())
            elif not create and route.path.startswith("/api/resources/"):
                rid = route.path.removeprefix("/api/resources/")
                value = self.server.registry.update(rid, self._body())
            else:
                raise LookupError("리소스를 찾을 수 없습니다.")
            self._json(HTTPStatus.CREATED if create else HTTPStatus.OK, {"data": value})
        except (ValueError, TypeError, OSError, RuntimeError, ImportError, LookupError) as error:
            self._failure(error)

    def _failure(self, error: Exception) -> None:
        if isinstance(error, ConflictError):
            self._error(HTTPStatus.CONFLICT, "revision_conflict", str(error))
        elif isinstance(error, ImportError):
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "arrow_unavailable",
                "이 표의 미리보기에는 선택 의존성 pyarrow가 필요합니다.",
            )
        elif isinstance(error, RuntimeError):
            self._error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "busy",
                "다른 작업이 사용 중입니다. 잠시 후 다시 시도해주세요.",
            )
        elif isinstance(error, KeyError):
            self._error(
                HTTPStatus.CONFLICT,
                "invalid_storage",
                "저장된 카탈로그 형식이 잘못됐습니다. aas doctor로 확인해주세요.",
            )
        elif isinstance(error, LookupError):
            self._error(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "항목을 찾을 수 없습니다. 목록을 새로고침해주세요.",
            )
        else:
            self._error(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "입력이나 저장 경로를 확인해주세요. 내용은 저장되지 않았습니다.",
            )

    def do_POST(self) -> None:
        self._mutate(create=True)

    def do_PUT(self) -> None:
        self._mutate(create=False)
