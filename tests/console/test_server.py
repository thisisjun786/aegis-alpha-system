# ruff: noqa: PLR2004 -- explicit expected protocol values.
from __future__ import annotations

import io
import json
import socket
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from aegis_alpha.console.catalog import Catalog
from aegis_alpha.console.registry import Registry
from aegis_alpha.console.server import ConsoleServer, Handler


def request(  # noqa: PLR0913 -- explicit HTTP boundary fixture
    tmp_path: Path,
    target: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: object = None,
    remote_origin: str | None = None,
) -> tuple[int, bytes]:
    payload = b"" if body is None else json.dumps(body).encode()
    lines = {"Host": "127.0.0.1:8765", **(headers or {})}
    if body is not None:
        lines.setdefault("Content-Length", str(len(payload)))
        lines.setdefault("Content-Type", "application/json")
    raw = (
        f"{method} {target} HTTP/1.0\r\n"
        + "".join(f"{k}: {v}\r\n" for k, v in lines.items())
        + "\r\n"
    ).encode() + payload
    incoming, outgoing = io.BytesIO(raw), io.BytesIO()

    class Connection:
        def makefile(self, mode: str, *_args: object) -> io.BytesIO:
            return incoming if mode == "rb" else outgoing

        def sendall(self, value: bytes) -> None:
            outgoing.write(value)

        def settimeout(self, _timeout: int) -> None:
            pass

    server = SimpleNamespace(
        server_port=8765,
        remote_origin=remote_origin,
        catalog=Catalog(tmp_path / "absent"),
        registry=Registry(tmp_path / "registry"),
    )
    Handler(cast("socket.socket", Connection()), ("127.0.0.1", 1), cast("ConsoleServer", server))
    result = outgoing.getvalue()
    head, data = result.split(b"\r\n\r\n", 1)
    assert b"Cache-Control: no-store" in head
    return int(head.split()[1]), data


@pytest.mark.parametrize(
    "headers",
    [{"Host": "evil.test:8765"}, {"Origin": "https://evil.test"}, {"Sec-Fetch-Site": "cross-site"}],
)
def test_host_and_cross_site_denied(tmp_path: Path, headers: dict[str, str]) -> None:
    status, _ = request(tmp_path, "/api/resources", headers=headers)
    assert status == 403


def test_mutation_requires_same_origin_and_header(tmp_path: Path) -> None:
    status, _ = request(tmp_path, "/api/resources", method="POST", body={})
    assert status == 403
    status, _ = request(
        tmp_path,
        "/api/resources",
        method="POST",
        headers={"Origin": "http://127.0.0.1:8765", "X-AAS-Console": "1"},
        body={"bad": "field"},
    )
    assert status == 400


def test_resource_roundtrip_and_conflict(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    headers = {"Origin": "http://127.0.0.1:8765", "X-AAS-Console": "1"}
    status, data = request(
        tmp_path,
        "/api/resources",
        method="POST",
        headers=headers,
        body={"name": "test", "path": str(source), "kind": "directory", "note": ""},
    )
    assert status == 201
    rid = json.loads(data)["data"]["id"]
    path = "/api/resources/" + rid
    update = {"name": "test", "note": "saved", "revision": 1}
    assert request(tmp_path, path, method="PUT", headers=headers, body=update)[0] == 200
    assert request(tmp_path, path, method="PUT", headers=headers, body=update)[0] == 409
    assert (
        json.loads(request(tmp_path, "/api/resources")[1])["data"]["resources"][0]["note"]
        == "saved"
    )


def test_unknown_route_and_bad_query(tmp_path: Path) -> None:
    assert request(tmp_path, "/../runtime.json")[0] == 404
    assert request(tmp_path, "/api/table?source=a&table=x&sql=DELETE")[0] == 400
    assert request(tmp_path, "/api/overview")[0] == 200
    assert request(tmp_path, "/api/strategies?offset=-1")[0] == 400
    assert request(tmp_path, "/api/coverage?category=us_equity&extra=1")[0] == 400


def test_oversized_body_and_wrong_media_type(tmp_path: Path) -> None:
    headers = {"Origin": "http://127.0.0.1:8765", "X-AAS-Console": "1"}
    assert (
        request(
            tmp_path,
            "/api/resources",
            method="POST",
            headers={**headers, "Content-Length": "999999"},
            body={},
        )[0]
        == 400
    )
    assert (
        request(
            tmp_path,
            "/api/resources",
            method="POST",
            headers={**headers, "Content-Type": "text/plain"},
            body={},
        )[0]
        == 400
    )


def test_missing_resource_and_wrong_method(tmp_path: Path) -> None:
    headers = {"Origin": "http://127.0.0.1:8765", "X-AAS-Console": "1"}
    assert (
        request(
            tmp_path,
            "/api/resources/absent",
            method="PUT",
            headers=headers,
            body={"name": "missing", "note": "", "revision": 1},
        )[0]
        == 404
    )


@pytest.mark.parametrize(
    "origin",
    [
        "http://node.tailtest.ts.net",
        "https://evil.test",
        "https://node.tailtest.ts.net/",
        "https://node.tailtest.ts.net:99999",
        "https://node.tailtest.ts.net:443",
        "https://node-.tailtest.ts.net:8765",
        "https://node.tailtest.ts.net@evil.test",
    ],
)
def test_invalid_tailnet_origin(origin: str) -> None:
    from aegis_alpha.console.server import tailscale_origin  # noqa: PLC0415

    with pytest.raises(ValueError, match=r".+"):
        tailscale_origin(origin)


def test_exact_tailnet_origin_and_mutation(tmp_path: Path) -> None:
    origin = "https://node.tailtest.ts.net:8765"
    headers = {"Host": "node.tailtest.ts.net:8765", "Origin": origin, "X-AAS-Console": "1"}
    assert request(tmp_path, "/api/resources", headers=headers)[0] == 403
    assert request(tmp_path, "/api/resources", headers=headers, remote_origin=origin)[0] == 200
    source = tmp_path / "source"
    source.mkdir()
    body = {"name": "remote", "path": str(source), "kind": "directory", "note": "test"}
    assert (
        request(
            tmp_path,
            "/api/resources",
            method="POST",
            headers=headers,
            body=body,
            remote_origin=origin,
        )[0]
        == 201
    )
    assert (
        request(
            tmp_path,
            "/api/resources",
            headers={**headers, "Origin": "https://other.tailtest.ts.net:8765"},
            remote_origin=origin,
        )[0]
        == 403
    )
