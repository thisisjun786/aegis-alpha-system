"""Constant-memory usage projection from durable FMP attempt ledgers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

from aegis_alpha.data.fmp_attempt_state import load_latest_attempt_record
from aegis_alpha.data.fmp_windows import CollectorContractError
from aegis_alpha.data.serialization import canonical_json_bytes

_ZERO_DIGEST = "0" * 64
_ROOT_DOMAIN = b"aegis-alpha/fmp-orphan-usage-root/v1\x00"


class _Hash(Protocol):
    def update(self, data: bytes, /) -> None: ...


@dataclass(frozen=True, slots=True)
class DurableRunUsage:
    run_id: str
    calls_used_today: int
    bytes_used_30d: int


@dataclass(frozen=True, slots=True)
class DurableUsageEvidence:
    runs: tuple[DurableRunUsage, ...]
    attempt_count: int
    records_root_sha256: str


@dataclass(frozen=True, slots=True)
class DurableUsageReconciliation:
    recorded_calls_by_run: Mapping[str, int] = field(default_factory=dict)
    recorded_bytes_by_run: Mapping[str, int] = field(default_factory=dict)
    calls_start_utc: datetime | None = None
    bytes_start_utc: datetime | None = None
    exclude_start_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class _UsageWindow:
    calls_start: datetime
    bytes_start: datetime
    excluded_start: datetime | None
    end: datetime


def load_durable_attempt_usage(
    raw_store_root: Path,
    *,
    now: datetime,
    exclude_run_ids: Collection[str] = (),
    reconciliation: DurableUsageReconciliation | None = None,
) -> DurableUsageEvidence:
    """Stream and validate orphan-candidate ledgers without retaining attempts."""

    moment = now.astimezone(UTC)
    state = DurableUsageReconciliation() if reconciliation is None else reconciliation
    calls_start = (
        moment.replace(hour=0, minute=0, second=0, microsecond=0)
        if state.calls_start_utc is None
        else state.calls_start_utc.astimezone(UTC)
    )
    bytes_start = (
        moment - timedelta(days=30)
        if state.bytes_start_utc is None
        else state.bytes_start_utc.astimezone(UTC)
    )
    window = _UsageWindow(
        calls_start=calls_start,
        bytes_start=bytes_start,
        excluded_start=(
            None if state.exclude_start_utc is None else state.exclude_start_utc.astimezone(UTC)
        ),
        end=moment,
    )
    digest = hashlib.sha256(_ROOT_DOMAIN)
    attempt_count = 0
    usages = []
    runs_root = raw_store_root / "fmp" / "runs"
    run_roots = sorted(runs_root.iterdir()) if runs_root.is_dir() else ()
    for run_root in run_roots:
        if run_root.name in exclude_run_ids:
            continue
        if run_root.is_symlink():
            raise CollectorContractError("durable FMP run root cannot be a symlink")
        if not run_root.is_dir():
            continue
        latest_attempt = _latest_attempt_time(run_root)
        if latest_attempt is None or latest_attempt < min(calls_start, bytes_start):
            continue
        calls, received_bytes, count = _run_usage(
            run_root,
            window=window,
            digest=digest,
        )
        attempt_count += count
        if calls or received_bytes:
            usages.append(DurableRunUsage(run_root.name, calls, received_bytes))
    return DurableUsageEvidence(tuple(usages), attempt_count, digest.hexdigest())


def _latest_attempt_time(run_root: Path) -> datetime | None:
    record = load_latest_attempt_record(run_root)
    if record is None:
        return None
    try:
        attempted_at = datetime.fromisoformat(str(record["attempted_at_utc"]))
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        raise CollectorContractError("durable attempt usage is invalid") from None
    if attempted_at.tzinfo is None:
        raise CollectorContractError("durable attempt usage is invalid")
    return attempted_at.astimezone(UTC)


def _run_usage(
    run_root: Path,
    *,
    window: _UsageWindow,
    digest: _Hash,
) -> tuple[int, int, int]:
    attempts_root = run_root / "attempts"
    if attempts_root.is_symlink():
        raise CollectorContractError("durable attempt root cannot be a symlink")
    latest = load_latest_attempt_record(run_root)
    count = 0 if latest is None else int(str(latest["attempt_seq"]))
    paths = (attempts_root / f"{sequence:08d}.json" for sequence in range(1, count + 1))
    previous = _ZERO_DIGEST
    calls = 0
    received_bytes = 0
    for expected, path in enumerate(paths, start=1):
        payload, attempted_at, byte_count = _validated_attempt(
            path,
            run_id=run_root.name,
            expected=expected,
            previous=previous,
        )
        previous = hashlib.sha256(payload).hexdigest()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        after_excluded_start = (
            window.excluded_start is None or attempted_at >= window.excluded_start
        )
        if after_excluded_start and window.calls_start <= attempted_at < window.end:
            calls += 1
        if after_excluded_start and window.bytes_start <= attempted_at < window.end:
            received_bytes += byte_count
    return calls, received_bytes, count


def _validated_attempt(
    path: Path,
    *,
    run_id: str,
    expected: int,
    previous: str,
) -> tuple[bytes, datetime, int]:
    if path.is_symlink():
        raise CollectorContractError("durable attempt record cannot be a symlink")
    try:
        payload = path.read_bytes()
        record = cast("dict[str, object]", json.loads(payload))
        attempted_at = datetime.fromisoformat(str(record["attempted_at_utc"]))
        byte_count = int(str(record.get("raw_byte_length", 0)))
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        raise CollectorContractError("durable attempt usage is invalid") from None
    if (
        payload != canonical_json_bytes(record)
        or attempted_at.tzinfo is None
        or byte_count < 0
        or record.get("attempt_seq") != expected
        or record.get("run_identity") != run_id
        or record.get("previous_attempt_sha256") != previous
        or path.name != f"{expected:08d}.json"
    ):
        raise CollectorContractError("durable attempt usage is invalid")
    return payload, attempted_at.astimezone(UTC), byte_count
