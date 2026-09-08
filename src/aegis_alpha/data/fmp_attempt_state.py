# ruff: noqa: SIM905 - compact immutable key declaration keeps this module focused
"""Durable request-attempt state and exact response reconstruction."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import assert_never, cast

from aegis_alpha.data.fmp_attempt_provenance import load_attempt_provenance
from aegis_alpha.data.fmp_rate_types import RateResumeState, reconcile_request_states
from aegis_alpha.data.fmp_symbol_observation import (
    AttemptOutcome,
    AttemptRequest,
    DurableResponseEvidence,
    ReconciledAttempts,
    RejectedResponseAttempt,
    ResponseAttempt,
    TransportFailureAttempt,
    reconcile_durable_response,
)
from aegis_alpha.data.fmp_windows import CollectorContractError
from aegis_alpha.data.serialization import canonical_json_bytes

_ZERO_DIGEST = "0" * 64
_HTTP_TOO_MANY_REQUESTS = 429
_RETRY_KEYS = frozenset("retry_after_wait retry_delay_seconds retry_not_before_utc".split())
_PACING_KEY = "next_request_not_before_utc"


class AttemptState:
    """One run's canonical attempt chain and exact response projection."""

    def __init__(
        self,
        *,
        raw_root: Path,
        run_identity: str,
        clock: Callable[[], datetime],
        publish: Callable[[Sequence[tuple[Path, bytes]]], tuple[Path, ...]],
    ) -> None:
        self._raw_root = raw_root
        self._run_identity = run_identity
        self._clock = clock
        self._publish = publish
        self._sequence = 0
        self._digest = _ZERO_DIGEST

    @property
    def digest(self) -> str:
        return self._digest

    def record(
        self,
        request: AttemptRequest,
        outcome: AttemptOutcome,
        *,
        next_request_not_before_utc: datetime,
    ) -> tuple[int, str]:
        sequence = self._sequence + 1
        record: dict[str, object] = {
            "attempt_seq": sequence,
            "attempted_at_utc": self._clock(),
            "previous_attempt_sha256": self._digest,
            "request_fingerprint": request.request_fingerprint,
            "run_identity": self._run_identity,
            _PACING_KEY: next_request_not_before_utc,
        }
        match outcome:
            case TransportFailureAttempt(kind=kind, request_attempt_index=index, retry=retry):
                record.update({"outcome": kind.value, "request_attempt_index": index})
            case ResponseAttempt(response=response, request_attempt_index=index, retry=retry):
                record.update(
                    {
                        "content_sha256": response.content_sha256,
                        "outcome": "response",
                        "raw_byte_length": len(response.body),
                        "request_attempt_index": index,
                        "status_code": response.status_code,
                    }
                )
            case RejectedResponseAttempt(
                status_code=status_code,
                raw_byte_length=raw_byte_length,
                request_attempt_index=index,
            ):
                retry = None
                record.update(
                    {
                        "outcome": "credential_rejected_response",
                        "raw_byte_length": raw_byte_length,
                        "request_attempt_index": index,
                        "status_code": status_code,
                    }
                )
            case unreachable:
                assert_never(unreachable)
        if retry is not None:
            record.update(
                {
                    "retry_after_wait": retry.from_retry_after,
                    "retry_delay_seconds": retry.delay_seconds,
                    "retry_not_before_utc": next_request_not_before_utc,
                }
            )
        payload = canonical_json_bytes(record)
        root = self._raw_root / "fmp" / "runs" / self._run_identity / "attempts"
        self._publish([(root / f"{sequence:08d}.json", payload)])
        _replace_latest_attempt(root.parent / "latest-attempt.json", payload)
        self._sequence = sequence
        self._digest = hashlib.sha256(payload).hexdigest()
        return sequence, self._digest

    def load(self) -> ReconciledAttempts:
        attempts = self._load_ledger()
        provenance = load_attempt_provenance(self._raw_root, self._run_identity)
        response_attempts = {
            sequence for sequence, record in attempts.items() if record["outcome"] == "response"
        }
        if set(provenance) != response_attempts:
            raise CollectorContractError(
                "durable response provenance does not exactly match response attempts"
            )
        responses = tuple(
            reconcile_durable_response(
                self._raw_root,
                DurableResponseEvidence(attempts[sequence], *provenance[sequence]),
            )
            for sequence in sorted(provenance)
        )
        return ReconciledAttempts(
            rate_state=RateResumeState(
                calls_attempted=len(attempts),
                bytes_received=sum(
                    int(str(record.get("raw_byte_length", 0))) for record in attempts.values()
                ),
                retry_after_waits=sum(
                    record.get("retry_after_wait") is True for record in attempts.values()
                ),
                rate_limited_attempts=sum(
                    record.get("status_code") == _HTTP_TOO_MANY_REQUESTS
                    for record in attempts.values()
                ),
                jitter_draws=sum(
                    "retry_delay_seconds" in record and record.get("retry_after_wait") is False
                    for record in attempts.values()
                ),
                bandwidth_bytes_received=sum(
                    int(str(record.get("raw_byte_length", 0)))
                    for record in attempts.values()
                    if datetime.fromisoformat(str(record["attempted_at_utc"]))
                    >= self._clock() - timedelta(days=30)
                ),
            ),
            responses=responses,
            request_states=reconcile_request_states(attempts),
            next_request_not_before_utc=self._next_request_not_before(attempts),
        )

    @staticmethod
    def _next_request_not_before(
        attempts: dict[int, dict[str, object]],
    ) -> datetime | None:
        if not attempts:
            return None
        latest = attempts[max(attempts)]
        raw = latest.get(_PACING_KEY, latest.get("retry_not_before_utc"))
        return None if raw is None else datetime.fromisoformat(str(raw))

    def _load_ledger(self) -> dict[int, dict[str, object]]:
        return dict(self._iter_ledger())

    def _iter_ledger(self) -> Iterator[tuple[int, dict[str, object]]]:
        previous = _ZERO_DIGEST
        root = self._raw_root / "fmp" / "runs" / self._run_identity / "attempts"
        latest = load_latest_attempt_record(root.parent)
        count = 0 if latest is None else int(str(latest["attempt_seq"]))
        paths = (root / f"{sequence:08d}.json" for sequence in range(1, count + 1))
        common = {
            "attempt_seq",
            "attempted_at_utc",
            "outcome",
            "previous_attempt_sha256",
            "request_attempt_index",
            "request_fingerprint",
            "run_identity",
        }
        for expected, path in enumerate(paths, start=1):
            try:
                payload = path.read_bytes()
                record = cast("dict[str, object]", json.loads(payload))
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                raise CollectorContractError("durable attempt ledger is invalid") from None
            try:
                expected_keys = (
                    common
                    | {
                        "credential_rejected_response": {"raw_byte_length", "status_code"},
                        "network_error": set(),
                        "response": {"content_sha256", "raw_byte_length", "status_code"},
                        "timeout": set(),
                    }[str(record.get("outcome"))]
                )
            except KeyError:
                raise CollectorContractError("durable attempt ledger is invalid") from None
            retry_keys = set(record).intersection(_RETRY_KEYS)
            pacing_keys = set(record).intersection({_PACING_KEY})
            expected_keys |= retry_keys | pacing_keys
            try:
                retry_deadline = (
                    datetime.fromisoformat(str(record.get("retry_not_before_utc")))
                    if retry_keys
                    else None
                )
                pacing_deadline = (
                    datetime.fromisoformat(str(record.get(_PACING_KEY))) if pacing_keys else None
                )
                retry_delay = float(str(record.get("retry_delay_seconds", 0.0)))
                request_attempt_index = int(str(record["request_attempt_index"]))
            except (KeyError, TypeError, ValueError):
                raise CollectorContractError("durable attempt ledger is invalid") from None
            if (
                payload != canonical_json_bytes(record)
                or set(record) != expected_keys
                or (bool(retry_keys) and retry_keys != set(_RETRY_KEYS))
                or (pacing_deadline is not None and pacing_deadline.tzinfo is None)
                or record.get("attempt_seq") != expected
                or record.get("run_identity") != self._run_identity
                or record.get("previous_attempt_sha256") != previous
                or path.name != f"{expected:08d}.json"
                or not str(record.get("request_fingerprint", "")).startswith("sha256:")
                or not isinstance(record.get("request_attempt_index"), int)
                or request_attempt_index < 1
                or (
                    retry_deadline is not None
                    and (
                        retry_deadline.tzinfo is None
                        or not isinstance(record.get("retry_after_wait"), bool)
                        or retry_delay < 0
                    )
                )
            ):
                raise CollectorContractError("durable attempt ledger is invalid")
            previous = hashlib.sha256(payload).hexdigest()
            yield expected, record
        marker = root.parent / "publication.json"
        if marker.is_file():
            marker_payload = marker.read_bytes()
            marker_document = json.loads(marker_payload)
            if (
                marker_payload != canonical_json_bytes(marker_document)
                or marker_document.get("attempt_ledger_sha256") != previous
            ):
                raise CollectorContractError(
                    "durable attempt ledger does not match its publication marker"
                )
        self._sequence = count
        self._digest = previous


def latest_durable_pacing_deadline(raw_root: Path) -> datetime | None:
    """Return the latest validated next-request boundary across FMP runs."""

    runs_root = raw_root / "fmp" / "runs"
    if not runs_root.is_dir():
        return None
    deadlines = []
    for run_root in sorted(runs_root.iterdir()):
        if run_root.is_symlink():
            raise CollectorContractError("durable FMP run root cannot be a symlink")
        if not run_root.is_dir():
            continue
        deadline = _latest_run_pacing_deadline(run_root)
        if deadline is not None:
            deadlines.append(deadline)
    return max(deadlines, default=None)


def _latest_run_pacing_deadline(run_root: Path) -> datetime | None:
    record = load_latest_attempt_record(run_root)
    if record is None:
        return None
    raw_deadline = record.get(_PACING_KEY)
    if raw_deadline is None:
        return None
    try:
        deadline = datetime.fromisoformat(str(raw_deadline))
    except ValueError:
        raise CollectorContractError("durable pacing boundary is invalid") from None
    if deadline.tzinfo is None:
        raise CollectorContractError("durable pacing boundary is invalid")
    return deadline


def load_latest_attempt_record(run_root: Path) -> dict[str, object] | None:
    pointer = run_root / "latest-attempt.json"
    attempts = run_root / "attempts"
    if attempts.is_symlink() or pointer.is_symlink():
        raise CollectorContractError("durable latest attempt pointer is invalid")
    if not pointer.exists():
        if (attempts / "00000001.json").exists():
            raise CollectorContractError("durable latest attempt pointer is missing")
        return None
    try:
        payload = pointer.read_bytes()
        record = cast("dict[str, object]", json.loads(payload))
        sequence = int(str(record["attempt_seq"]))
        immutable = attempts / f"{sequence:08d}.json"
        immutable_payload = immutable.read_bytes()
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        raise CollectorContractError("durable latest attempt pointer is invalid") from None
    if (
        sequence < 1
        or payload != canonical_json_bytes(record)
        or immutable.is_symlink()
        or immutable_payload != payload
        or record.get("run_identity") != run_root.name
        or (attempts / f"{sequence + 1:08d}.json").exists()
    ):
        raise CollectorContractError("durable latest attempt pointer is invalid")
    return record


def _replace_latest_attempt(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
