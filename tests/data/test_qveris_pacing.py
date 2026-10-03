from __future__ import annotations

from datetime import UTC, datetime

import pytest

from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_pacing import PacedQverisClient, RequestAdmission, RequestPacer


class Clock:
    now = 10.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Transport:
    account_key = "synthetic"

    def __init__(self, clock: Clock, starts: list[tuple[str, float]]) -> None:
        self.clock = clock
        self.starts = starts

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        del body, query
        self.starts.append((path, self.clock.now))
        now = datetime(2026, 9, 6, tzinfo=UTC)
        return QverisResponse(200, (), b"{}", now, now)


def test_workers_and_coordinator_share_request_start_spacing() -> None:
    clock = Clock()
    starts: list[tuple[str, float]] = []
    pacer = RequestPacer(0.75, clock=clock.monotonic, sleep=clock.sleep)
    clients = [PacedQverisClient(Transport(clock, starts), pacer) for _ in range(3)]
    clients[0].request("/tools/execute")
    clients[1].request("/auth/credits")
    clients[2].request("/tools/probe")
    assert starts == [
        ("/tools/execute", 10.0),
        ("/auth/credits", 10.75),
        ("/tools/probe", 11.5),
    ]
    clock.now = 20
    clients[0].request("/tools/execute")
    assert starts[-1] == ("/tools/execute", 20)
    assert clients[0].account_key == "synthetic"


@pytest.mark.parametrize("interval", [True, -1, float("nan"), float("inf")])
def test_invalid_pacing_is_rejected(interval: float) -> None:
    with pytest.raises(ValueError, match="interval"):
        RequestPacer(interval)


def test_admission_bounds_requests_and_time_across_shared_clients() -> None:
    clock = Clock()
    starts: list[tuple[str, float]] = []
    pacer = RequestPacer(0, clock=clock.monotonic, sleep=clock.sleep)
    admission = RequestAdmission(3, 60, clock=clock.monotonic)
    clients = [PacedQverisClient(Transport(clock, starts), pacer, admission) for _ in range(2)]
    clients[0].request("/tools/probe")
    clients[1].request("/tools/execute")
    clients[0].request("/tools/execute")
    with pytest.raises(RuntimeError, match="INVOCATION_HTTP_LIMIT"):
        clients[1].request("/auth/credits")
    assert (admission.http_requests, admission.paid_executions) == (3, 2)
    assert [path for path, _ in starts] == ["/tools/probe", "/tools/execute", "/tools/execute"]
    late = RequestAdmission(10, 5, clock=clock.monotonic)
    clock.now += 5
    with pytest.raises(RuntimeError, match="INVOCATION_HTTP_LIMIT"):
        PacedQverisClient(Transport(clock, starts), pacer, late).request("/tools/probe")
    assert late.http_requests == 0


@pytest.mark.parametrize(("requests", "seconds"), [(0, 1), (True, 1), (1, 0), (1, float("inf"))])
def test_invalid_admission_is_rejected(requests: int, seconds: float) -> None:
    with pytest.raises(ValueError, match="limit"):
        RequestAdmission(requests, seconds)
