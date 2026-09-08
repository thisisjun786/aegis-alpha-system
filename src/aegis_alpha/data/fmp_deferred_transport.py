"""Defer production HTTPS adapter construction until an approved request."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from aegis_alpha.data.fmp_collector import (
    CollectorRequest,
    CollectorResponse,
    Transport,
)
from aegis_alpha.data.fmp_collector_http import make_fmp_https_transport


def deferred_live_transport(
    injected: Transport | None,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Transport:
    """Return injected replay directly or lazily construct the live HTTPS port."""

    if injected is not None:
        return injected
    live: Transport | None = None

    def request(request: CollectorRequest, credential: str) -> CollectorResponse:
        nonlocal live
        if live is None:
            live = make_fmp_https_transport(clock=clock)
        return live(request, credential)

    return request
