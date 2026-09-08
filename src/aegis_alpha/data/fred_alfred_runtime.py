"""Durable FRED execution over signed scope, immutable evidence and PostgreSQL."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy import Engine, text

from aegis_alpha.data.fred_alfred_collector import (
    CollectorConfig,
    CollectorError,
    CollectorOutcome,
    FredAlfredCollector,
    Transport,
    make_https_transport,
    validate_destination,
)
from aegis_alpha.data.fred_alfred_rate_limit import RateLimiter
from aegis_alpha.data.fred_alfred_recovery import recover_runtime
from aegis_alpha.data.fred_alfred_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fred_alfred_registration import FredRuntime

_PROVIDER_LOCK = "aegis_alpha.fred_alfred.runtime_provider"


@contextmanager
def _provider_lock(engine: Engine) -> Iterator[None]:
    # Session lock, autocommit: no transaction spans pacing or HTTP.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(
            text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"), {"key": _PROVIDER_LOCK}
        )
        try:
            yield
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                {"key": _PROVIDER_LOCK},
            )


def run_runtime(  # noqa: PLR0913 -- explicit runtime dependency ports
    *,
    config: CollectorConfig,
    engine: Engine,
    authority: VerifiedRecurringAuthority,
    credential: str,
    transport: Transport | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> CollectorOutcome:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", config.run_identity):
        raise CollectorError("FRED run identity must be a safe stable identifier")
    if not credential:
        raise CollectorError("FRED credential is required")
    for label, path in (
        ("raw root", config.raw_store_root),
        ("dataset root", config.dataset_root),
        ("receipt", config.receipt_path),
    ):
        if validate_destination(label, path) != path:
            raise CollectorError("FRED runtime paths must be canonical absolute paths")
    if (
        config.raw_store_root != authority.raw_store_root
        or config.dataset_root != authority.dataset_root
        or config.max_calls > authority.calls_per_day
    ):
        raise CollectorError("FRED runtime scope exceeds signed authority")
    authority.require_request(clock())
    with _provider_lock(engine):
        authority.require_request(clock())
        runtime = FredRuntime(config, engine, authority, clock)
        if runtime.admission_path.exists():
            return recover_runtime(runtime)
        if config.receipt_path.exists() or runtime.ready_path.exists():
            raise CollectorError("FRED output exists without its admission evidence")
        # Bound the first request across separate invocations as well as within a run.
        sleep(60 / authority.calls_per_minute)
        authority.require_request(clock())
        limiter = RateLimiter(
            max_calls=config.max_calls,
            calls_per_minute=authority.calls_per_minute,
            clock=monotonic,
            sleep=sleep,
            revalidate=runtime.require_request,
        )
        collector = FredAlfredCollector(
            config=config,
            transport=transport or make_https_transport(),
            control_plane=runtime.registry,
            limiter=limiter,
            credential=credential,
            clock=clock,
            run_id=config.run_identity,
            plan=runtime.plan(clock()),
            startup=runtime.start,
            capture=runtime.capture,
            finalize=runtime.finalize,
        )
        return collector.collect()
