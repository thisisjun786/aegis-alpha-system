"""One request-start pacer and one HTTP admission shared by a coordinator and its workers."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.qveris_client import QverisResponse

DEFAULT_REQUEST_INTERVAL = 0.75


def _seconds(value: float, name: str, *, positive: bool) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        or (positive and value == 0)
    ):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be finite and {qualifier}")
    return float(value)


class RequestPacer:
    """Space request starts by ``interval`` seconds across every thread that shares it."""

    def __init__(
        self,
        interval: float = DEFAULT_REQUEST_INTERVAL,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = _seconds(interval, "request interval", positive=False)
        self._clock = clock
        self._sleep = sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            remaining = self._next - self._clock()
            if remaining > 0:
                self._sleep(remaining)
            self._next = self._clock() + self.interval


class RequestAdmission:
    """A thread-safe bound on HTTP attempts and wall time for one invocation.

    Paid executions are counted separately; the paid-call and credit limits are the
    ``InvocationBudget`` reservations made before any intent exists.
    """

    def __init__(
        self,
        max_requests: int,
        seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("HTTP request limit must be positive")
        self.max_requests = max_requests
        self.seconds = _seconds(seconds, "request time limit", positive=True)
        self._clock = clock
        self._deadline = clock() + self.seconds
        self._lock = threading.Lock()
        self.http_requests = 0
        self.paid_executions = 0

    def admit(self, path: str) -> None:
        with self._lock:
            if self.http_requests >= self.max_requests or self._clock() >= self._deadline:
                raise RuntimeError("INVOCATION_HTTP_LIMIT: request was not attempted")
            self.http_requests += 1
            if path == "/tools/execute":
                self.paid_executions += 1


class PacedQverisClient:
    """A port that admits, then paces, every request before the wrapped client sends it."""

    def __init__(
        self,
        client: QverisPort,
        pacer: RequestPacer,
        admission: RequestAdmission | None = None,
    ) -> None:
        self._client = client
        self._pacer = pacer
        self._admission = admission

    @property
    def account_key(self) -> str:
        return self._client.account_key

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if self._admission is not None:
            self._admission.admit(path)
        self._pacer.wait()
        return self._client.request(path, body=body, query=query)
