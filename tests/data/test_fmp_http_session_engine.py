"""Real HTTPConnection state and collector retries over a socket-free byte stream."""

from __future__ import annotations

import http.client
import io
import json
import socket
import ssl
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data import fmp_collector_http
from aegis_alpha.data.fmp_collector import CollectorConfig, CredentialLeakError, FmpCollector
from aegis_alpha.data.fmp_collector_http import make_fmp_https_transport
from aegis_alpha.data.fmp_rate_limit import RateLimiter, TierArtifact
from tests.data.test_fmp_collector_http import _API_KEY, _fixed_clock, _UnusedControlPlane
from tests.data.test_fmp_http_session import wire


@dataclass
class WireSocket:
    responses: list[bytes]
    fail_second_send: bool = False
    sends: list[bytes] = field(default_factory=list)
    closed: bool = False

    def sendall(self, data: bytes) -> None:
        self.sends.append(data)
        if self.fail_second_send and len(self.sends) == 2:  # noqa: PLR2004 -- stale second request
            raise BrokenPipeError("synthetic stale connection")

    def makefile(self, _mode: str) -> io.BytesIO:
        return io.BytesIO(self.responses.pop(0))

    def settimeout(self, _timeout: float | None) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def _collector(tmp_path: Path) -> tuple[FmpCollector, RateLimiter]:
    limiter = RateLimiter(
        tier=TierArtifact(3000, None, None),
        max_calls=3,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=1,
    )
    collector = FmpCollector(
        config=CollectorConfig(
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "data",
            receipt_path=tmp_path / "receipt.json",
            as_of=date(2026, 8, 18),
            mode=CollectionMode.INCREMENTAL,
            max_calls=3,
            run_identity="fmp-session-engine",
        ),
        transport=make_fmp_https_transport(clock=_fixed_clock),
        control_plane=_UnusedControlPlane(),
        limiter=limiter,
        credential=_API_KEY,
        clock=_fixed_clock,
    )
    return collector, limiter


@pytest.mark.parametrize("stale", [False, True])
def test_real_http_state_reuses_connection_and_stale_retry_is_counted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stale: bool,
) -> None:
    responses = [
        wire(b'[{"symbol":"AAPL"}]', headers=b"Content-Type: application/json\r\n"),
        wire(b'[{"symbol":"MSFT"}]', headers=b"Content-Type: application/json\r\n"),
    ]
    sockets: list[WireSocket] = []

    def connect(connection: http.client.HTTPSConnection) -> None:
        # The stdlib's verified TLS context is unchanged; only network establishment is replaced.
        context = cast("ssl.SSLContext", vars(connection)["_context"])
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True
        stream = WireSocket(responses, fail_second_send=stale and not sockets)
        sockets.append(stream)
        connection.sock = cast("socket.socket", stream)

    monkeypatch.setattr(http.client.HTTPSConnection, "connect", connect)
    monkeypatch.setattr(fmp_collector_http.urllib.request, "getproxies", dict)
    collector, limiter = _collector(tmp_path)
    for symbol in ("AAPL", "MSFT"):
        rows, _bodies = collector.collect_profile(symbol=symbol)
        assert len(rows) == 1
    expected = 3 if stale else 2
    assert limiter.calls_attempted == expected
    assert sum(len(stream.sends) for stream in sockets) == expected
    assert len(sockets) == (2 if stale else 1)
    attempts = sorted((tmp_path / "raw/fmp/runs/fmp-session-engine/attempts").glob("*.json"))
    assert len(attempts) == expected
    if stale:
        assert sockets[0].closed is True
        assert json.loads(attempts[1].read_bytes())["outcome"] == "network_error"
    for stream in sockets:
        for sent in stream.sends:
            assert _API_KEY.encode() not in sent.split(b"\r\n", 1)[0]
            assert b"Apikey: " + _API_KEY.encode() in sent


def test_session_duplicate_credential_header_reaches_existing_engine_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b"[]"
    headers = f"Content-Type: application/json\r\nX-Echo: safe\r\nX-Echo: {_API_KEY}\r\n".encode()
    stream = WireSocket([wire(body, status=429, headers=headers)])

    def connect(connection: http.client.HTTPSConnection) -> None:
        connection.sock = cast("socket.socket", stream)

    monkeypatch.setattr(http.client.HTTPSConnection, "connect", connect)
    monkeypatch.setattr(fmp_collector_http.urllib.request, "getproxies", dict)
    collector, limiter = _collector(tmp_path)
    with pytest.raises(CredentialLeakError):
        collector.collect_profile(symbol="AAPL")
    assert limiter.calls_attempted == 1
    assert limiter.bytes_received == len(body)
    assert limiter.ledger().rate_limited_attempts == 1
    assert len(stream.sends) == 1
    for path in (tmp_path / "raw").rglob("*"):
        if path.is_file():
            assert _API_KEY.encode() not in path.read_bytes()
