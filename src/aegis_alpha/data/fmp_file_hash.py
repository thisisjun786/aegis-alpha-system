"""Memory-bounded validation and replay of durable FMP raw blobs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from aegis_alpha.data.fmp_attempt_provenance import (
    count_attempt_provenance_references,
    load_attempt_provenance_record,
)
from aegis_alpha.data.fmp_attempt_state import AttemptState
from aegis_alpha.data.fmp_rate_types import (
    DurableRequestState,
    RateResumeState,
    update_request_states,
)
from aegis_alpha.data.fmp_response_security import CollectorResponse
from aegis_alpha.data.fmp_windows import CollectorContractError
from aegis_alpha.data.serialization import canonical_json_bytes

_HASH_CHUNK_BYTES = 1024 * 1024
_HTTP_SUCCESS_MIN = 200
_HTTP_REDIRECT_MIN = 300
_HTTP_TOO_MANY_REQUESTS = 429


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(_HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


class RawCaptureIndex:
    def __init__(self) -> None:
        self._paths: dict[str, Path] = {}
        self._ordered_paths: list[Path] = []

    def clear(self) -> None:
        self._paths.clear()
        self._ordered_paths.clear()

    def remember(self, digest: str, path: Path) -> None:
        if digest not in self._paths:
            self._paths[digest] = path
            self._ordered_paths.append(path)

    @property
    def digests(self) -> frozenset[str]:
        return frozenset(self._paths)

    @property
    def paths(self) -> tuple[Path, ...]:
        return tuple(self._ordered_paths)

    def cursor(self) -> int:
        return len(self._ordered_paths)

    def bodies(self) -> tuple[bytes, ...]:
        return tuple(path.read_bytes() for path in self._paths.values())

    def bodies_since(self, cursor: int) -> tuple[bytes, ...]:
        return tuple(path.read_bytes() for path in self._ordered_paths[cursor:])


@dataclass(frozen=True, slots=True)
class DurableReplay:
    attempt_sequence: int
    request_fingerprint: str
    status_code: int
    response_headers: dict[str, str]
    blob_path: Path
    content_sha256: str
    raw_byte_length: int
    requested_at_utc: datetime
    retrieved_at_utc: datetime
    provenance: bytes
    receipt_record: dict[str, object]

    def materialize(self) -> CollectorResponse:
        body = self.blob_path.read_bytes()
        if len(body) != self.raw_byte_length or hashlib.sha256(body).hexdigest() != (
            self.content_sha256
        ):
            raise CollectorContractError("durable raw body changed after replay validation")
        return CollectorResponse(
            status_code=self.status_code,
            headers=self.response_headers,
            body=body,
            requested_at_utc=self.requested_at_utc,
            retrieved_at_utc=self.retrieved_at_utc,
            raw_headers=(),
        )


@dataclass(frozen=True, slots=True)
class BoundedAttemptResume:
    rate_state: RateResumeState
    responses: Iterator[DurableReplay]
    request_states: tuple[DurableRequestState, ...]
    next_request_not_before_utc: datetime | None


def _reconcile_reference(
    raw_root: Path,
    attempt: dict[str, object],
    provenance: bytes,
    record: dict[str, object],
) -> DurableReplay:
    digest = str(record["content_sha256"])
    blob = raw_root / "fmp" / "blobs" / "sha256" / digest[:2] / f"{digest}.raw"
    raw_byte_length = int(str(record["raw_byte_length"]))
    if (
        blob.is_symlink()
        or record["request_fingerprint"] != attempt["request_fingerprint"]
        or record["attempt_record_sha256"]
        != hashlib.sha256(canonical_json_bytes(attempt)).hexdigest()
        or record["status_code"] != attempt["status_code"]
        or digest != attempt["content_sha256"]
        or raw_byte_length != int(str(attempt["raw_byte_length"]))
        or blob.stat().st_size != raw_byte_length
        or sha256_file(blob) != digest
    ):
        raise CollectorContractError(
            "durable response provenance conflicts with its unique attempt"
        )
    requested_at_utc = datetime.fromisoformat(str(record["requested_at_utc"]))
    retrieved_at_utc = datetime.fromisoformat(str(record["retrieved_at_utc"]))
    status_code = int(str(record["status_code"]))
    return DurableReplay(
        attempt_sequence=int(str(attempt["attempt_seq"])),
        request_fingerprint=str(record["request_fingerprint"]),
        status_code=status_code,
        response_headers=cast("dict[str, str]", record["response_headers"]),
        blob_path=blob,
        content_sha256=digest,
        raw_byte_length=raw_byte_length,
        requested_at_utc=requested_at_utc,
        retrieved_at_utc=retrieved_at_utc,
        provenance=provenance,
        receipt_record={
            "attempts": int(str(attempt["request_attempt_index"])),
            "disposition": (
                "succeeded" if _HTTP_SUCCESS_MIN <= status_code < _HTTP_REDIRECT_MIN else "failed"
            ),
            "page": record.get("page"),
            "raw_byte_length": raw_byte_length,
            "raw_content_address": f"sha256:{digest}",
            "request_fingerprint": str(record["request_fingerprint"]),
            "requested_at_utc": requested_at_utc,
            "response_headers": cast("dict[str, str]", record["response_headers"]),
            "retrieved_at_utc": retrieved_at_utc,
            "source_uri": str(record["source_uri"]),
            "status_code": status_code,
            "symbol": record.get("symbol"),
        },
    )


def load_bounded_attempt_resume(attempt_state: AttemptState) -> BoundedAttemptResume:
    calls = bytes_received = retry_waits = rate_limited = jitter_draws = bandwidth = 0
    request_states: dict[str, DurableRequestState] = {}
    next_request_not_before_utc = None
    cutoff = attempt_state._clock() - timedelta(days=30)  # noqa: SLF001
    for _sequence, record in attempt_state._iter_ledger():  # noqa: SLF001
        calls += 1
        byte_count = int(str(record.get("raw_byte_length", 0)))
        bytes_received += byte_count
        retry_waits += record.get("retry_after_wait") is True
        rate_limited += record.get("status_code") == _HTTP_TOO_MANY_REQUESTS
        jitter_draws += "retry_delay_seconds" in record and record.get("retry_after_wait") is False
        if datetime.fromisoformat(str(record["attempted_at_utc"])) >= cutoff:
            bandwidth += byte_count
        update_request_states(request_states, record)
        raw_deadline = record.get("next_request_not_before_utc", record.get("retry_not_before_utc"))
        next_request_not_before_utc = (
            None if raw_deadline is None else datetime.fromisoformat(str(raw_deadline))
        )
    return BoundedAttemptResume(
        rate_state=RateResumeState(
            calls_attempted=calls,
            bytes_received=bytes_received,
            retry_after_waits=retry_waits,
            rate_limited_attempts=rate_limited,
            jitter_draws=jitter_draws,
            bandwidth_bytes_received=bandwidth,
        ),
        responses=_iter_bounded_replays(attempt_state),
        request_states=tuple(request_states.values()),
        next_request_not_before_utc=next_request_not_before_utc,
    )


def _iter_bounded_replays(attempt_state: AttemptState) -> Iterator[DurableReplay]:
    raw_root = attempt_state._raw_root  # noqa: SLF001
    run_identity = attempt_state._run_identity  # noqa: SLF001
    response_count = 0
    for sequence, attempt in attempt_state._iter_ledger():  # noqa: SLF001
        if attempt["outcome"] != "response":
            continue
        provenance, record = load_attempt_provenance_record(raw_root, run_identity, sequence)
        response_count += 1
        yield _reconcile_reference(raw_root, attempt, provenance, record)
    if count_attempt_provenance_references(raw_root, run_identity) != response_count:
        raise CollectorContractError(
            "durable response provenance does not exactly match response attempts"
        )


def load_durable_replay(attempt_state: AttemptState, sequence: int) -> DurableReplay:
    raw_root = attempt_state._raw_root  # noqa: SLF001
    run_identity = attempt_state._run_identity  # noqa: SLF001
    attempt_path = raw_root / "fmp" / "runs" / run_identity / "attempts" / f"{sequence:08d}.json"
    try:
        attempt = cast("dict[str, object]", json.loads(attempt_path.read_bytes()))
        provenance, record = load_attempt_provenance_record(raw_root, run_identity, sequence)
        return _reconcile_reference(raw_root, attempt, provenance, record)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        raise CollectorContractError("durable replay index is invalid") from None
