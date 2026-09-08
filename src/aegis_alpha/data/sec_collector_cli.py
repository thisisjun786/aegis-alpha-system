"""SEC CLI with explicit synthetic and durable runtime modes.

Live execution requires the pinned registry, contact, exact invocation bounds,
CIK identity input and configured application database. The retained registry
currently refuses live collection. Stable-ID recovery requires the same explicit
timezone-aware --as-of, --run-identity and receipt path on every invocation.
Synthetic mode remains visibly in-process.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionRunState,
    CollectionUsageRecord,
    CurrentWatermark,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.data.sec_collector import (
    CollectorConfig,
    CollectorError,
    DestinationError,
    SecCollector,
    validate_destination,
)
from aegis_alpha.data.sec_disagreement import load_fmp_comparison
from aegis_alpha.data.sec_identity import IdentityError, load_identity_snapshot
from aegis_alpha.data.sec_policy import require_live_policy, trusted_registry_path
from aegis_alpha.data.sec_rate_limit import RateLimiter
from aegis_alpha.data.sec_transport import (
    USER_AGENT_ENV,
    DatasetKind,
    UserAgentError,
    make_fixture_transport,
    make_https_transport,
    validate_user_agent,
)
from aegis_alpha.data.serialization import canonical_json_bytes

PRECONDITION_EXIT = 2
DEFAULT_FIXTURE_SUBMISSIONS = "submissions.json"
DEFAULT_FIXTURE_FACTS = "companyfacts.json"
SEC_POLICY_ID = "sec-official-verifier-v1"
TRUSTED_SEC_REGISTRY = trusted_registry_path()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one bounded AAS-DATA-013 SEC collection")
    parser.add_argument("--mode", required=True, choices=[item.value for item in CollectionMode])
    parser.add_argument("--max-calls", required=True, type=int)
    parser.add_argument(
        "--registry",
        type=Path,
        default=TRUSTED_SEC_REGISTRY,
    )
    parser.add_argument("--identity-snapshot", required=True, type=Path)
    parser.add_argument("--raw-store-root", required=True, type=Path)
    parser.add_argument("--dataset-root", required=True, type=Path)
    parser.add_argument("--receipt-path", required=True, type=Path)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--instrument-id", action="append", default=None)
    parser.add_argument("--fmp-comparison", type=Path, default=None)
    parser.add_argument("--user-agent-env", default=USER_AGENT_ENV)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--fixture-root", type=Path, default=None)
    parser.add_argument("--run-identity", default="sec-ga-synthetic")
    return parser


def discard_limiter_delay(_seconds: float) -> None:
    """Synthetic runs must not wait on wall time."""


def limiter_sleeper(*, live: bool) -> Callable[[float], None]:
    """``--live`` paces with ``time.sleep``; ``--synthetic`` discards the delay."""

    return time.sleep if live else discard_limiter_delay


def build_rate_limiter(max_calls: int, *, live: bool) -> RateLimiter:
    return RateLimiter(
        max_calls=max_calls,
        clock=lambda: datetime.now(UTC).timestamp(),
        sleep=limiter_sleeper(live=live),
    )


def _require_scheduled_collection(registry_path: Path) -> None:
    require_live_policy(registry_path)


def _require_trusted_registry_path(registry_path: Path) -> Path:
    if registry_path != TRUSTED_SEC_REGISTRY:
        raise CollectorError("live SEC requires the trusted SEC registry")
    return registry_path


def _parse_as_of(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as error:
        raise CollectorError("--as-of must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CollectorError("--as-of must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _load_fixture_bodies(root: Path, ciks: Sequence[str]) -> dict[tuple[DatasetKind, str], bytes]:
    submissions = root / DEFAULT_FIXTURE_SUBMISSIONS
    facts = root / DEFAULT_FIXTURE_FACTS
    if not submissions.is_file() or not facts.is_file():
        raise CollectorError(
            "synthetic fixture root must contain submissions and companyfacts JSON"
        )
    bodies: dict[tuple[DatasetKind, str], bytes] = {}
    for cik in ciks:
        bodies[(DatasetKind.SUBMISSIONS, cik)] = submissions.read_bytes()
        bodies[(DatasetKind.COMPANYFACTS, cik)] = facts.read_bytes()
    per_cik_dir = root / "by_cik"
    if per_cik_dir.is_dir():
        for cik_dir in per_cik_dir.iterdir():
            if not cik_dir.is_dir():
                continue
            sub = cik_dir / DEFAULT_FIXTURE_SUBMISSIONS
            fac = cik_dir / DEFAULT_FIXTURE_FACTS
            if sub.is_file():
                bodies[(DatasetKind.SUBMISSIONS, cik_dir.name)] = sub.read_bytes()
            if fac.is_file():
                bodies[(DatasetKind.COMPANYFACTS, cik_dir.name)] = fac.read_bytes()
    return bodies


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    try:
        arguments = parser.parse_args(list(argv) if argv is not None else None)
    except SystemExit as error:
        return int(error.code or 0)
    env = os.environ if environ is None else environ
    try:
        return _run(arguments, environ=env, stderr=stderr, stdout=stdout)
    except (CollectorError, DestinationError, IdentityError, UserAgentError, ValueError) as error:
        stderr.write(f"{error}\n")
        return PRECONDITION_EXIT
    except Exception:  # noqa: BLE001 - DB/driver errors must not echo credentials or contact
        stderr.write("SEC invocation failed; inspect durable run evidence\n")
        return PRECONDITION_EXIT


def _run(
    arguments: argparse.Namespace, *, environ: Mapping[str, str], stderr: TextIO, stdout: TextIO
) -> int:
    del stderr
    if arguments.synthetic == arguments.live:
        raise CollectorError("exactly one of --synthetic or --live is required")
    if arguments.max_calls < 1:
        raise CollectorError("--max-calls must be a positive integer")
    as_of = _parse_as_of(arguments.as_of)
    identity = load_identity_snapshot(arguments.identity_snapshot)
    raw_store = arguments.raw_store_root
    dataset_root = arguments.dataset_root
    receipt_path = arguments.receipt_path
    user_agent: str | None = None
    if arguments.live:
        raw_store = validate_destination("raw store", raw_store)
        dataset_root = validate_destination("dataset root", dataset_root)
        receipt_path = validate_destination("receipt path", receipt_path)
        user_agent = validate_user_agent(environ.get(arguments.user_agent_env))
        _require_scheduled_collection(_require_trusted_registry_path(arguments.registry))
        transport = make_https_transport(
            user_agent=user_agent,
            clock=lambda: datetime.now(UTC),
        )
    else:
        if arguments.fixture_root is None:
            raise CollectorError("--fixture-root is required for --synthetic")
        admitted = [
            item
            for item in (
                identity.admit(instrument_id, identity.as_of)
                for instrument_id in (arguments.instrument_id or identity.requested_instruments())
            )
            if item.cik is not None
        ]
        transport = make_fixture_transport(
            _load_fixture_bodies(
                arguments.fixture_root, [item.cik for item in admitted if item.cik]
            ),
            clock=lambda: datetime.now(UTC),
        )
    config = CollectorConfig(
        raw_store_root=raw_store,
        dataset_root=dataset_root,
        receipt_path=receipt_path,
        mode=CollectionMode(arguments.mode),
        max_calls=arguments.max_calls,
        run_identity=arguments.run_identity,
        as_of=as_of,
    )
    fmp_facts = (
        () if arguments.fmp_comparison is None else load_fmp_comparison(arguments.fmp_comparison)
    )
    fmp_bytes = None
    if arguments.fmp_comparison is not None:
        fmp_bytes = {arguments.fmp_comparison: arguments.fmp_comparison.read_bytes()}
    limiter = build_rate_limiter(arguments.max_calls, live=arguments.live)
    if arguments.live:
        from aegis_alpha.data.sec_runtime import run_sec_collection  # noqa: PLC0415
        from aegis_alpha.metadata.database import create_metadata_engine  # noqa: PLC0415

        database_url = environ.get("AAS_DATABASE_URL")
        if not database_url:
            raise CollectorError("SEC live runtime requires the configured AAS_DATABASE_URL")
        if arguments.run_identity == "sec-ga-synthetic" or arguments.as_of is None:
            raise CollectorError("SEC live runtime requires explicit --run-identity and --as-of")
        engine = create_metadata_engine(database_url)
        try:
            outcome = run_sec_collection(
                config,
                engine=engine,
                identity=identity,
                transport=transport,
                limiter=limiter,
                clock=lambda: datetime.now(UTC),
                instrument_ids=arguments.instrument_id,
                user_agent=user_agent,
                registry_path=arguments.registry,
                fmp_facts=fmp_facts,
                fmp_source_bytes=fmp_bytes,
            )
        finally:
            engine.dispose()
    else:
        collector = SecCollector(
            config=config,
            transport=transport,
            control_plane=_NullControlPlane(),
            limiter=limiter,
            clock=lambda: datetime.now(UTC),
        )
        outcome = collector.collect(
            identity=identity,
            instrument_ids=arguments.instrument_id,
            fmp_facts=fmp_facts,
            fmp_source_bytes=fmp_bytes,
        )
    stdout.write(
        canonical_json_bytes(
            {
                "run_id": outcome.run_id,
                "state": outcome.terminal_event.value,
                "calls_attempted": outcome.calls_attempted,
                "provider_calls": limiter.calls_attempted,
                "receipt_path": None if outcome.receipt_path is None else str(outcome.receipt_path),
                "synthetic": arguments.synthetic,
            }
        ).decode()
        + "\n"
    )
    return 0 if outcome.terminal_event.value == "run_succeeded" else 1


class _NullControlPlane:
    """In-process 005 stand-in for G-A CLI. No production tables are opened."""

    def __init__(self) -> None:
        self.plans: dict[str, CollectionRunPlan] = {}
        self.runs: dict[str, CollectionRun] = {}
        self.events: list[CollectionRunEvent] = []
        self.watermarks: dict[tuple[str, str, str], CurrentWatermark] = {}
        self.usage: list[CollectionUsageRecord] = []

    def register_plan(self, plan: CollectionRunPlan) -> None:
        self.plans[plan.plan_id] = plan

    def start_run(self, run: CollectionRun) -> None:
        self.runs[run.run_id] = run

    def append_event(self, event: CollectionRunEvent) -> int:
        self.events.append(event)
        return len(self.events)

    def current_run_state(self, run_id: str) -> CollectionRunState | None:
        run = self.runs.get(run_id)
        if run is None:
            return None
        matching = [event for event in self.events if event.run_id == run_id]
        last = matching[-1] if matching else None
        return CollectionRunState(
            run_id=run_id,
            plan_id=run.plan_id,
            state=None if last is None else last.event_type,
            terminal=last is not None
            and last.event_type
            in {RunEventType.RUN_SUCCEEDED, RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED},
            attempt_count=sum(
                1 for event in matching if event.event_type is RunEventType.ATTEMPT_STARTED
            ),
            last_event_seq=None if not matching else len(matching),
            last_occurred_at_utc=None if last is None else last.occurred_at_utc,
        )

    def advance_watermark(self, advance: WatermarkAdvance) -> int:
        key = (advance.provider, advance.dataset, advance.stream)
        current = self.watermarks.get(key)
        seq = 1 if current is None else current.watermark_seq + 1
        self.watermarks[key] = CurrentWatermark(
            provider=advance.provider,
            dataset=advance.dataset,
            stream=advance.stream,
            watermark_seq=seq,
            run_id=advance.run_id,
            watermark_value=advance.watermark_value,
            watermark_position=advance.watermark_position,
            recorded_at_utc=advance.watermark_position,
        )
        return seq

    def latest_watermark(self, provider: str, dataset: str, stream: str) -> CurrentWatermark | None:
        return self.watermarks.get((provider, dataset, stream))

    def record_usage(self, record: CollectionUsageRecord) -> None:
        self.usage.append(record)
