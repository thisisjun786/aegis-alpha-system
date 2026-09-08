from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol, Self, assert_never
from urllib.parse import urlencode

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
    collection_plan_digest,
)
from aegis_alpha.data import fmp_rate_types as rate_types
from aegis_alpha.data.fmp_attempt_provenance import provenance_reference
from aegis_alpha.data.fmp_attempt_state import AttemptState
from aegis_alpha.data.fmp_file_hash import (
    RawCaptureIndex,
    load_bounded_attempt_resume,
    load_durable_replay,
)
from aegis_alpha.data.fmp_normalize import (
    DATASET_SPECS,
    ENDPOINT_DATASETS,
    PROVIDER,
    Provenance,
    collected_dates,
    normalize_records,
)
from aegis_alpha.data.fmp_rate_limit import (
    REQUEST_TIMEOUT_SECONDS,
    BudgetExhaustedError,
    EntitlementError,
    FailureClass,
    RateLimiter,
    RetryCeilingError,
    UnexpectedStatusError,
    UsageLedger,
)
from aegis_alpha.data.fmp_receipt_metadata import ReceiptMetadataStream
from aegis_alpha.data.fmp_request_pacing import RequestPacing
from aegis_alpha.data.fmp_response_security import (
    CollectorResponse,
    assert_credential_absent,
    redact_headers,
)
from aegis_alpha.data.fmp_symbol_observation import (
    CollectedSymbolRows,
    CredentialLeakError,
    ResponseAttempt,
    SymbolObservationRequest,
    SymbolObservationResult,
    TransportFailureAttempt,
    TransportFailureKind,
    classify_and_account_response,
    classify_response,
    normalize_symbol_observation,
    parse_provider_records,
    resume_request_state,
    sanitized_credential_rejection,
)
from aegis_alpha.data.fmp_windows import (
    LIST_PAGE_LIMIT,
    CollectorContractError,
    DateWindow,
    ListPageOutcome,
    ListWalk,
    UniverseEntry,
    WindowOutcome,
    WindowWalk,
    classify_window_response,
    plan_backfill_windows,
    plan_incremental_window,
)
from aegis_alpha.data.serialization import canonical_json_bytes

#: Guardrail G2: the only host this collector may ever contact.
ALLOWED_HOST: Final = "financialmodelingprep.com"
BASE_URL: Final = f"https://{ALLOWED_HOST}"
#: Section 6.3: the 004C-owned concurrency lock, relative to the raw store root.
LOCK_RELATIVE_PATH: Final = Path("fmp") / "collector.lock"


def _lock_relative_path(shard: tuple[int, int] | None) -> Path:
    if shard is None:
        return LOCK_RELATIVE_PATH
    index, total = shard
    return LOCK_RELATIVE_PATH.with_name(
        f"{LOCK_RELATIVE_PATH.stem}-shard{index}of{total}{LOCK_RELATIVE_PATH.suffix}"
    )


SCHEMA_VERSION: Final = 1


_HTTP_SUCCESS_MIN: Final = 200
_HTTP_REDIRECT_MIN: Final = 300
_ATTEMPT_FAILED_EVENT_SEQUENCE: Final = 2
_RUN_FAILED_EVENT_SEQUENCE: Final = 3


class CollectorLockError(RuntimeError):
    """Section 6.3: another FMP run already holds the collector lock."""


class CollectorLockLostError(RuntimeError):
    """Section 6.3: this run no longer owns the collector lock, so it fails closed."""


class DestinationError(RuntimeError):
    """Guardrail G1: a live destination resolves inside a Git repository."""


@dataclass(frozen=True, slots=True)
class CollectorRequest:
    endpoint: str
    parameters: Mapping[str, str]
    symbol: str | None = None
    page: int | None = None

    def __post_init__(self) -> None:
        if not self.endpoint.startswith("/stable/") or "?" in self.endpoint:
            raise ValueError("endpoint must be a credential-free /stable/ path")
        for name, value in self.parameters.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("request parameters must be strings")
            if "apikey" in name.casefold() or "token" in name.casefold():
                raise ValueError("credential parameters are forbidden")

    @property
    def query(self) -> str:
        return urlencode(sorted(self.parameters.items()))

    @property
    def source_uri(self) -> str:
        return f"{BASE_URL}{self.endpoint}" + (f"?{self.query}" if self.query else "")

    @property
    def request_fingerprint(self) -> str:
        payload = f"GET\n{self.endpoint}\n{self.query}".encode()
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"


#: Offline-replayable transport; no socket exists in tests or CI.
Transport = Callable[[CollectorRequest, str], CollectorResponse]


class ControlPlanePort(Protocol):
    """The bounded AAS-DATA-005 operations 004C is allowed to call."""

    def register_plan(self, plan: CollectionRunPlan) -> None: ...

    def start_run(self, run: CollectionRun) -> None: ...

    def append_event(self, event: CollectionRunEvent) -> int: ...

    def current_run_state(self, run_id: str) -> CollectionRunState | None: ...

    def run_plan_dataset(self, run_id: str) -> str | None: ...

    def advance_watermark(self, advance: WatermarkAdvance) -> int: ...

    def latest_watermark(
        self, provider: str, dataset: str, stream: str
    ) -> CurrentWatermark | None: ...

    def record_usage(
        self,
        record: CollectionUsageRecord,
        *,
        recorded_at_clock: Callable[[], datetime] | None = None,
    ) -> None: ...


class QualityResultKind(StrEnum):
    COVERAGE_GAP = "coverage_gap"
    IRREDUCIBLE_TRUNCATION = "irreducible_truncation"
    EMPTY_RANGE_SKIPPED = "empty_range_skipped"
    DELISTED_SHORTFALL = "delisted_shortfall"
    RECYCLED_TICKER_AMBIGUITY = "recycled_ticker_ambiguity"
    OBSERVED_COVERAGE_START = "observed_coverage_start"
    ENDPOINT_BLOCKED = "endpoint_blocked"
    SCHEMA_OBSERVATION = "schema_observation"
    OUT_OF_WINDOW_DATES = "out_of_window_dates"
    INFORMATIONAL_EMPTY = "informational_empty"
    PROVIDER_ROW_SKIPPED = "provider_row_skipped"


@dataclass(frozen=True, slots=True)
class QualityResult:
    kind: QualityResultKind
    symbol: str | None
    dataset: str | None
    detail: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class CollectorOutcome:
    run_id: str
    plan_id: str
    terminal_event: RunEventType
    published_paths: tuple[Path, ...]
    receipt_path: Path | None
    quality_results: tuple[QualityResult, ...]
    watermarks_advanced: tuple[tuple[str, str, str], ...]
    blocked_symbols: tuple[str, ...] = field(default=())
    cancel_reason: str | None = None
    artifact_path: Path | None = None
    error_class: str | None = None


def containing_git_repository(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def validate_destination(label: str, destination: Path) -> Path:
    """Guardrail G1: canonicalize and reject any Git-contained destination."""

    resolved = destination.resolve(strict=False)
    if repository_root := containing_git_repository(resolved):
        raise DestinationError(
            f"{label} must be outside a Git repository (resolved repository: {repository_root})"
        )
    return resolved


class CollectorLock:
    """Stable advisory lock that never follows links or unlinks successors."""

    def __init__(
        self,
        raw_store_root: Path,
        *,
        run_identity: str,
        shard: tuple[int, int] | None = None,
    ) -> None:
        self._path = raw_store_root / _lock_relative_path(shard)
        self._run_identity = run_identity
        self._acquired = False
        self._owned_payload: bytes | None = None
        self._descriptor: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def owned_payload(self) -> bytes | None:
        return self._owned_payload

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json_bytes(
            {
                "created_at_utc": datetime.now(UTC),
                "pid": os.getpid(),
                "run_identity": self._run_identity,
            }
        )
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            descriptor = os.open(self._path, flags, 0o600)
        except OSError:
            raise CollectorLockError(f"unsafe lock target rejected at {self._path}") from None
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise CollectorLockError(f"unsafe lock target rejected at {self._path}")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            existing = self._describe_existing(descriptor)
            os.close(descriptor)
            raise CollectorLockError(
                f"an FMP collector run already holds {self._path}: {existing}"
            ) from None
        os.fchmod(descriptor, 0o600)
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, payload)
        os.fsync(descriptor)
        self._descriptor = descriptor
        self._owned_payload = payload
        self._acquired = True

    def _describe_existing(self, descriptor: int) -> str:
        try:
            existing = json.loads(os.pread(descriptor, 4096, 0))
        except (OSError, json.JSONDecodeError):
            return "existing lock is unreadable; an operator must clear it deliberately"
        return (
            f"run_identity={existing.get('run_identity')!r} pid={existing.get('pid')!r} "
            f"created_at_utc={existing.get('created_at_utc')!r}; "
            "an operator must clear a stale lock deliberately"
        )

    def release(self) -> None:
        """Drop this run's kernel lock without unlinking its path."""

        if not self._acquired:
            return
        self._acquired = False
        self._owned_payload = None
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is None:
            return
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def require_ownership(self) -> None:
        """Fail closed before a protected request when ownership was lost."""

        if not self._acquired or self._descriptor is None:
            raise CollectorLockLostError(
                "the FMP collector lock is not held by this run; no request may be attempted"
            )
        try:
            path_status = self._path.stat()
        except OSError:
            raise CollectorLockLostError(
                f"the FMP collector lock at {self._path} is missing or unreadable; "
                "the run fails closed"
            ) from None
        held_status = os.fstat(self._descriptor)
        if (path_status.st_dev, path_status.st_ino) != (
            held_status.st_dev,
            held_status.st_ino,
        ):
            raise CollectorLockLostError(
                f"the FMP collector lock at {self._path} is now held by another run; "
                "the run fails closed"
            )

    def rebind_run_identity(self, run_identity: str) -> None:
        """Update forensic lock ownership after under-lock attempt selection."""

        self.require_ownership()
        descriptor = self._descriptor
        if descriptor is None:
            raise CollectorLockLostError("the FMP collector lock descriptor is unavailable")
        payload = canonical_json_bytes(
            {
                "created_at_utc": datetime.now(UTC),
                "pid": os.getpid(),
                "run_identity": run_identity,
            }
        )
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, payload)
        os.fsync(descriptor)
        self._run_identity = run_identity
        self._owned_payload = payload

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *_exception: object) -> None:
        self.release()


def publish_bundle(publications: Sequence[tuple[Path, bytes]]) -> tuple[Path, ...]:
    """Reuse the 004 immutable no-clobber bundle semantics verbatim."""

    for destination, content in publications:
        if destination.exists() and destination.read_bytes() != content:
            raise FileExistsError(
                f"immutable evidence already exists with different bytes: {destination}"
            )
    created: list[Path] = []
    try:
        for destination, content in publications:
            if _publish_immutable(destination, content):
                created.append(destination)
    except Exception:
        for destination in reversed(created):
            destination.unlink(missing_ok=True)
        raise
    return tuple(destination for destination, _content in publications)


def _publish_immutable(destination: Path, content: bytes) -> bool:
    if destination.exists():
        if destination.read_bytes() != content:
            raise FileExistsError(
                f"immutable evidence already exists with different bytes: {destination}"
            )
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        try:
            os.link(temporary_path, destination)
        except FileExistsError:
            if destination.read_bytes() != content:
                raise FileExistsError(
                    f"immutable evidence already exists with different bytes: {destination}"
                ) from None
            return False
        return True
    finally:
        temporary_path.unlink(missing_ok=True)


def raw_blob_path(raw_store_root: Path, content: bytes) -> Path:
    digest = hashlib.sha256(content).hexdigest()
    return raw_store_root / "fmp" / "blobs" / "sha256" / digest[:2] / f"{digest}.raw"


def raw_provenance_path(raw_store_root: Path, provenance: bytes) -> Path:
    """Address one request-attempt provenance record by its own digest.

    Addressing by the provenance bytes rather than by the body digest keeps a
    separate immutable record for every received response, so two distinct
    requests that return identical bytes each retain their own provenance.
    """

    digest = hashlib.sha256(provenance).hexdigest()
    return raw_store_root / "fmp" / "provenance" / "sha256" / digest[:2] / f"{digest}.json"


def build_run_plan(  # noqa: PLR0913 - each field is one immutable 005 plan attribute
    *,
    mode: CollectionMode,
    dataset: str,
    parameters: Mapping[str, object],
    created_at_utc: datetime,
    requested_window_start: datetime | None = None,
    requested_window_end: datetime | None = None,
) -> CollectionRunPlan:
    """Section 9.1: derive ``plan_id`` from 005's own plan digest."""

    provisional = CollectionRunPlan(
        plan_id="fmp-plan-provisional",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=dataset,
        mode=mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )
    digest = collection_plan_digest(provisional)
    return CollectionRunPlan(
        plan_id=f"fmp-plan-{digest}",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=dataset,
        mode=mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )


@dataclass(frozen=True, slots=True)
class PlanResumeRecord:
    """The canonical plan-resume record written outside Git (section 9.1)."""

    plan_id: str
    plan_sha256: str
    schema_version: int
    provider: str
    dataset: str
    mode: str
    parameters: Mapping[str, object]
    requested_window_start: str | None
    requested_window_end: str | None
    created_at_utc: str
    run_id: str | None = None

    def with_run_id(self, run_id: str) -> PlanResumeRecord:
        return PlanResumeRecord(
            plan_id=self.plan_id,
            plan_sha256=self.plan_sha256,
            schema_version=self.schema_version,
            provider=self.provider,
            dataset=self.dataset,
            mode=self.mode,
            parameters=self.parameters,
            requested_window_start=self.requested_window_start,
            requested_window_end=self.requested_window_end,
            created_at_utc=self.created_at_utc,
            run_id=run_id,
        )


def plan_resume_record(plan: CollectionRunPlan, *, run_id: str | None = None) -> PlanResumeRecord:
    return PlanResumeRecord(
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_sha256,
        schema_version=plan.schema_version,
        provider=plan.provider,
        dataset=plan.dataset,
        mode=plan.mode.value,
        parameters=dict(plan.parameters),
        requested_window_start=(
            None
            if plan.requested_window_start is None
            else plan.requested_window_start.astimezone(UTC).isoformat()
        ),
        requested_window_end=(
            None
            if plan.requested_window_end is None
            else plan.requested_window_end.astimezone(UTC).isoformat()
        ),
        created_at_utc=plan.created_at_utc.astimezone(UTC).isoformat(),
        run_id=run_id,
    )


def read_plan_resume_record(path: Path) -> PlanResumeRecord | None:
    """Reread the exact immutable record so a retry never regenerates it."""

    if not path.exists():
        return None
    document = json.loads(path.read_bytes())
    return PlanResumeRecord(
        plan_id=document["plan_id"],
        plan_sha256=document["plan_sha256"],
        schema_version=document["schema_version"],
        provider=document["provider"],
        dataset=document["dataset"],
        mode=document["mode"],
        parameters=document["parameters"],
        requested_window_start=document.get("requested_window_start"),
        requested_window_end=document.get("requested_window_end"),
        created_at_utc=document["created_at_utc"],
        run_id=document.get("run_id"),
    )


def _failure_lifecycle_state(state: CollectionRunState | None) -> RunEventType:
    """Validate the only durable states that may finish a failed run."""

    if state is None:
        raise CollectorContractError("failure requires a registered run state")
    if state.attempt_count != 1 or state.last_occurred_at_utc is None:
        raise CollectorContractError("failure lifecycle state has an invalid attempt projection")
    match state.state:
        case RunEventType.ATTEMPT_STARTED:
            if state.terminal or state.last_event_seq != 1:
                raise CollectorContractError("failure lifecycle state is not an initial attempt")
        case RunEventType.ATTEMPT_FAILED:
            if state.terminal or state.last_event_seq != _ATTEMPT_FAILED_EVENT_SEQUENCE:
                raise CollectorContractError(
                    "failure lifecycle state is not a resumable attempt failure"
                )
        case RunEventType.RUN_FAILED:
            if not state.terminal or state.last_event_seq != _RUN_FAILED_EVENT_SEQUENCE:
                raise CollectorContractError("failure lifecycle state is not terminally failed")
        case _:
            raise CollectorContractError(
                f"cannot record failure from lifecycle state {state.state}"
            )
    return state.state


@dataclass(frozen=True, slots=True)
class SymbolTask:
    symbol: str
    dataset: str
    endpoint: str
    windows: tuple[DateWindow, ...]


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    raw_store_root: Path
    dataset_root: Path
    receipt_path: Path
    as_of: date
    mode: CollectionMode
    max_calls: int | None
    run_identity: str
    operator_from: date | None = None
    artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    manifest_path: Path | None = None
    shard: tuple[int, int] | None = None
    receipt_evidence_path: Path | None = None


class FmpCollector:
    """Bounded, fail-closed FMP collection engine (sections 4, 5, and 9)."""

    def __init__(  # noqa: PLR0913 - every dependency is an explicit injected port
        self,
        *,
        config: CollectorConfig,
        transport: Transport,
        control_plane: ControlPlanePort,
        limiter: RateLimiter,
        credential: str,
        clock: Callable[[], datetime],
        lock: CollectorLock | None = None,
        approval_check: Callable[[], None] | None = None,
        shared_pacing_deadline: datetime | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._control_plane = control_plane
        self._limiter = limiter
        self._credential = credential
        self._clock = clock
        self._lock = lock
        self._approval_check = approval_check
        self._quality: list[QualityResult] = []
        self._blocked_symbols: list[str] = []
        self._requests: list[Mapping[str, object]] = []
        self._unknown_fields: set[str] = set()
        self._splits: list[dict[str, str]] = []
        self._captured = RawCaptureIndex()
        self._provenance_records: list[bytes] = []
        self._receipt_metadata = self._new_receipt_metadata()
        self._historical_index_cache: dict[str, dict[str, tuple[Mapping[str, object], ...]]] = {}
        self._historical_run_validation_cache: dict[str, tuple[str, dict[str, str]]] = {}
        self._historical_validated_runs: set[str] = set()
        self._request_states: dict[str, rate_types.DurableRequestState] = {}
        self._pacing = RequestPacing(clock, limiter)
        self._attempt_state = AttemptState(
            raw_root=config.raw_store_root,
            run_identity=config.run_identity,
            clock=clock,
            publish=publish_bundle,
        )
        self._durable_replays = self._load_durable_replays()
        self._pacing.restore(shared_pacing_deadline)

    def _new_receipt_metadata(self) -> ReceiptMetadataStream | None:
        if self._config.max_calls is not None:
            return None
        return ReceiptMetadataStream(
            raw_store_root=self._config.raw_store_root,
            run_identity=self._config.run_identity,
            publish=publish_bundle,
            validate=lambda payload: assert_credential_absent(self._credential, payload),
        )

    def _load_durable_replays(self) -> dict[str, list[int]]:
        """Load responses structurally reconciled by the focused attempt-state module."""

        reconciled = load_bounded_attempt_resume(self._attempt_state)
        self._limiter.restore_run_usage(reconciled.rate_state)
        self._request_states = {
            state.request_fingerprint: state for state in reconciled.request_states
        }
        self._pacing.restore(reconciled.next_request_not_before_utc)
        replays: dict[str, list[int]] = {}
        for durable in reconciled.responses:
            if _HTTP_SUCCESS_MIN <= durable.status_code < _HTTP_REDIRECT_MIN:
                replays.setdefault(durable.request_fingerprint, []).append(durable.attempt_sequence)
            self._captured.remember(durable.content_sha256, durable.blob_path)
            self._record_provenance(durable.provenance)
            self._record_request_receipt(durable.receipt_record)
        return replays

    def resume_as(self, run_identity: str) -> None:
        """Adopt a structurally validated resume identity and load its durable responses."""

        if run_identity == self._config.run_identity:
            return
        self._config = replace(self._config, run_identity=run_identity)
        self._captured.clear()
        self._provenance_records.clear()
        self._receipt_metadata = self._new_receipt_metadata()
        self._attempt_state = AttemptState(
            raw_root=self._config.raw_store_root,
            run_identity=run_identity,
            clock=self._clock,
            publish=publish_bundle,
        )
        self._durable_replays = self._load_durable_replays()

    @property
    def config(self) -> CollectorConfig:
        """Frozen run configuration used by the top-level driver."""

        return self._config

    @property
    def attempt_ledger_sha256(self) -> str:
        return self._attempt_state.digest

    @property
    def calls_attempted(self) -> int:
        """Actual transport attempts consumed, including retries."""

        return self._limiter.calls_attempted

    def require_bound_approval(self) -> None:
        """Recheck the bound grant before replay or an approval-bound durable write."""

        if self._approval_check is not None:
            self._approval_check()

    def _execute(self, request: CollectorRequest) -> CollectorResponse:  # noqa: C901
        """Run one request under pacing, retry, and guardrail policy."""

        replay = self._durable_replays.get(request.request_fingerprint, [])
        if replay:
            self.require_bound_approval()
            return load_durable_replay(self._attempt_state, replay.pop(0)).materialize()
        attempt_index = 1
        state = self._request_states.pop(request.request_fingerprint, None)
        if state is not None:
            attempt_index = resume_request_state(state)
        while True:
            response, attempt_index, request_started_at = self._receive_response(
                request, attempt_index
            )
            rejection = sanitized_credential_rejection(response, self._credential, attempt_index)
            if rejection is not None:
                self._attempt_state.record(
                    request,
                    rejection,
                    next_request_not_before_utc=self._pacing.next_deadline(
                        request_started_at, None
                    ),
                )
                self._limiter.account_response(
                    byte_count=rejection.raw_byte_length, failure=rejection.failure
                )
                self._request_states[request.request_fingerprint] = (
                    rate_types.DurableTerminalRequest(
                        request.request_fingerprint,
                        attempt_index,
                        rate_types.TerminalRequestDisposition.CREDENTIAL_REJECTED,
                    )
                )
                raise CredentialLeakError
            retry = None
            blocked_error: EntitlementError | RetryCeilingError | UnexpectedStatusError | None = (
                None
            )
            try:
                failure = classify_response(response)
            except UnexpectedStatusError as error:
                failure = None
                blocked_error = error
            if failure is not None:
                try:
                    retry = self._limiter.retry_obligation(
                        attempt_index=attempt_index,
                        failure=failure,
                        headers=response.headers,
                    )
                except (EntitlementError, RetryCeilingError) as error:
                    blocked_error = error
            next_request_not_before = self._pacing.next_deadline(request_started_at, retry)
            sequence, attempt_digest = self._attempt_state.record(
                request,
                ResponseAttempt(response, attempt_index, retry),
                next_request_not_before_utc=next_request_not_before,
            )
            self._capture(
                request,
                response,
                attempt_seq=sequence,
                attempt_record_sha256=attempt_digest,
            )
            self._record_request(request, response, attempts=attempt_index)
            if blocked_error is not None:
                self._request_states[request.request_fingerprint] = (
                    rate_types.DurableTerminalRequest(
                        request.request_fingerprint,
                        attempt_index,
                        rate_types.terminal_disposition(blocked_error),
                    )
                )
            classify_and_account_response(response, self._limiter)
            if blocked_error is not None:
                raise blocked_error
            if retry is None:
                return response
            self._limiter.apply_retry_obligation(retry, before_wait=self._approval_check)
            attempt_index += 1

    def _receive_response(
        self, request: CollectorRequest, attempt_index: int
    ) -> tuple[CollectorResponse, int, datetime]:
        while True:
            if self._lock is not None:
                self._lock.require_ownership()
            self._pacing.honor_restored_deadline(before_wait=self._approval_check)
            self._limiter.before_request(
                before_wait=self._approval_check,
                before_consume=self._approval_check,
            )
            request_started_at = self._clock()
            try:
                return self._transport(request, self._credential), attempt_index, request_started_at
            except (TimeoutError, OSError) as transport_error:
                match transport_error:
                    case TimeoutError():
                        kind = TransportFailureKind.TIMEOUT
                    case OSError():
                        kind = TransportFailureKind.NETWORK_ERROR
                    case unreachable:
                        assert_never(unreachable)
                try:
                    retry = self._limiter.retry_obligation(
                        attempt_index=attempt_index, failure=FailureClass.TIMEOUT
                    )
                except RetryCeilingError:
                    self._attempt_state.record(
                        request,
                        TransportFailureAttempt(kind, attempt_index, None),
                        next_request_not_before_utc=self._pacing.next_deadline(
                            request_started_at, None
                        ),
                    )
                    self._request_states[request.request_fingerprint] = (
                        rate_types.DurableTerminalRequest(
                            request.request_fingerprint,
                            attempt_index,
                            rate_types.TerminalRequestDisposition.RETRY_CEILING,
                        )
                    )
                    raise
                self._attempt_state.record(
                    request,
                    TransportFailureAttempt(kind, attempt_index, retry),
                    next_request_not_before_utc=self._pacing.next_deadline(
                        request_started_at, retry
                    ),
                )
                self._limiter.apply_retry_obligation(retry, before_wait=self._approval_check)
                attempt_index += 1

    def _capture(
        self,
        request: CollectorRequest,
        response: CollectorResponse,
        *,
        attempt_seq: int,
        attempt_record_sha256: str,
    ) -> None:
        """Publish one body and its credential-free per-attempt provenance."""

        provenance = canonical_json_bytes(
            {
                "attempt_record_sha256": attempt_record_sha256,
                "attempt_seq": attempt_seq,
                "content_sha256": response.content_sha256,
                "page": request.page,
                "raw_byte_length": len(response.body),
                "request_fingerprint": request.request_fingerprint,
                "requested_at_utc": response.requested_at_utc,
                "response_headers": redact_headers(response.headers),
                "retrieved_at_utc": response.retrieved_at_utc,
                "run_identity": self._config.run_identity,
                "source_uri": request.source_uri,
                "status_code": response.status_code,
                "symbol": request.symbol,
            }
        )
        assert_credential_absent(self._credential, response.body, provenance)
        root = self._config.raw_store_root
        blob_path = raw_blob_path(root, response.body)
        publish_bundle(
            [
                (blob_path, response.body),
                (raw_provenance_path(root, provenance), provenance),
                provenance_reference(
                    root,
                    self._config.run_identity,
                    attempt_seq,
                    request.request_fingerprint,
                    provenance,
                ),
            ]
        )
        self._captured.remember(response.content_sha256, blob_path)
        self._record_provenance(provenance)

    def _record_provenance(self, provenance: bytes) -> None:
        if self._receipt_metadata is None:
            self._provenance_records.append(provenance)
        else:
            self._receipt_metadata.record_provenance(provenance)

    @property
    def captured_bodies(self) -> tuple[bytes, ...]:
        """Every received body, including error, empty, and invalid payloads."""

        return self._captured.bodies()

    @property
    def provenance_records(self) -> tuple[bytes, ...]:
        """One record per received response, in arrival order."""

        return tuple(self.iter_provenance_records())

    def iter_provenance_records(self) -> Iterator[bytes]:
        if self._receipt_metadata is None:
            yield from self._provenance_records
        else:
            yield from self._receipt_metadata.provenance_records()

    def _record_request(
        self,
        request: CollectorRequest,
        response: CollectorResponse,
        *,
        attempts: int,
    ) -> None:
        record: dict[str, object] = {
            "source_uri": request.source_uri,
            "request_fingerprint": request.request_fingerprint,
            "raw_content_address": f"sha256:{response.content_sha256}",
            "raw_byte_length": len(response.body),
            "status_code": response.status_code,
            "attempts": attempts,
            "requested_at_utc": response.requested_at_utc,
            "retrieved_at_utc": response.retrieved_at_utc,
            "response_headers": redact_headers(response.headers),
            "symbol": request.symbol,
            "page": request.page,
            "disposition": (
                "succeeded"
                if _HTTP_SUCCESS_MIN <= response.status_code < _HTTP_REDIRECT_MIN
                else "failed"
            ),
        }
        self._record_request_receipt(record)

    def _record_request_receipt(self, record: Mapping[str, object]) -> None:
        if self._receipt_metadata is None:
            self._requests.append(record)
        else:
            self._receipt_metadata.record_request(record)

    # -- collection --------------------------------------------------------

    def collect_window(
        self,
        *,
        dataset: str,
        symbol: str,
        window: DateWindow,
    ) -> tuple[tuple[Mapping[str, object], ...], tuple[bytes, ...]]:
        """Collect one dataset/symbol window, splitting on truncation.

        The returned bodies are every body received while collecting this
        window — including truncated split originals, empty responses, and
        error payloads — so raw publication never silently drops evidence.
        Only validated records become normalized rows.
        """

        spec = DATASET_SPECS[dataset]
        walk = WindowWalk([window])
        rows: list[Mapping[str, object]] = []
        captured_before = self._captured.cursor()
        while (current := walk.next_window()) is not None:
            request = CollectorRequest(
                endpoint=spec.endpoint,
                parameters={"symbol": symbol, **current.as_parameters()},
                symbol=symbol,
            )
            response = self._execute(request)
            records = parse_provider_records(response)
            outcome = classify_window_response(current, len(records))
            if outcome is WindowOutcome.SPLIT_REQUIRED:
                walk.record_split(current)
                continue
            if outcome is WindowOutcome.IRREDUCIBLE_TRUNCATION:
                self._block_symbol(
                    symbol,
                    QualityResultKind.IRREDUCIBLE_TRUNCATION,
                    dataset=dataset,
                    detail={"date": current.start.isoformat()},
                )
                continue
            if outcome is WindowOutcome.COVERAGE_GAP:
                self._quality.append(
                    QualityResult(
                        kind=QualityResultKind.COVERAGE_GAP,
                        symbol=symbol,
                        dataset=dataset,
                        detail={
                            "from": current.start.isoformat(),
                            "to": current.end.isoformat(),
                        },
                    )
                )
                continue
            normalized = normalize_records(
                dataset=dataset,
                records=records,
                provenance=Provenance(
                    source_receipt_id=request.request_fingerprint,
                    raw_content_sha256=response.content_sha256,
                    retrieved_at_utc=response.retrieved_at_utc,
                ),
                symbol=symbol,
            )
            observed = collected_dates(normalized.rows)
            outside = tuple(sorted({value for value in observed if not current.contains(value)}))
            if outside:
                self._quality.append(
                    QualityResult(
                        kind=QualityResultKind.OUT_OF_WINDOW_DATES,
                        symbol=symbol,
                        dataset=dataset,
                        detail={
                            "from": current.start.isoformat(),
                            "to": current.end.isoformat(),
                            "first_outside": outside[0].isoformat(),
                            "interpretation": "provider_dates_outside_requested_window_kept",
                        },
                    )
                )
            if normalized.skipped_records:
                self._quality.append(
                    QualityResult(
                        kind=QualityResultKind.PROVIDER_ROW_SKIPPED,
                        symbol=symbol,
                        dataset=dataset,
                        detail={
                            "skipped_records": normalized.skipped_records,
                            "kept_records": len(normalized.rows),
                            "interpretation": "non_integral_provider_numeric_row_skipped",
                        },
                    )
                )
            self._unknown_fields.update(normalized.unknown_fields)
            rows.extend(normalized.rows)
        self._splits.extend(record.as_receipt_entry() for record in walk.splits)
        bodies = self._captured.bodies_since(captured_before)
        return (tuple(rows), bodies)

    def _collect_symbol_observation(
        self, *, dataset: str, symbol: str
    ) -> tuple[SymbolObservationResult, tuple[bytes, ...]]:
        captured_before = self._captured.cursor()
        request = CollectorRequest(
            endpoint=DATASET_SPECS[dataset].endpoint,
            parameters={"symbol": symbol},
            symbol=symbol,
        )
        observation = normalize_symbol_observation(
            SymbolObservationRequest(dataset, symbol, request.request_fingerprint),
            self._execute(request),
        )
        bodies = self._captured.bodies_since(captured_before)
        return observation, bodies

    def collect_profile(self, *, symbol: str) -> CollectedSymbolRows:
        """Collect one profile without inventing unsupported date parameters."""

        observation, bodies = self._collect_symbol_observation(dataset="fmp_profile", symbol=symbol)
        self._unknown_fields.update(observation.unknown_fields)
        return observation.rows, bodies

    def collect_symbol_observations(self, *, dataset: str, symbol: str) -> CollectedSymbolRows:
        """Collect one bounded symbol call for an endpoint without date parameters."""

        observation, bodies = self._collect_symbol_observation(dataset=dataset, symbol=symbol)
        if observation.is_empty:
            informational = any(body.lstrip().startswith(b"{") for body in bodies)
            self._quality.append(
                QualityResult(
                    kind=(
                        QualityResultKind.INFORMATIONAL_EMPTY
                        if informational
                        else QualityResultKind.COVERAGE_GAP
                    ),
                    symbol=symbol,
                    dataset=dataset,
                    detail={
                        "interpretation": (
                            "provider_informational_empty"
                            if informational
                            else "validated_empty_response"
                        )
                    },
                )
            )
        self._unknown_fields.update(observation.unknown_fields)
        return observation.rows, bodies

    def walk_list_endpoint(self, *, endpoint: str, identity_key: str) -> ListWalk:
        """Section 8.5: paginate a list endpoint with retry-confirmed exits."""

        walk = ListWalk(identity_key)
        page = 0
        retried = False
        while True:
            request = CollectorRequest(
                endpoint=endpoint,
                parameters={"limit": str(LIST_PAGE_LIMIT), "page": str(page)},
                page=page,
            )
            response = self._execute(request)
            records = parse_provider_records(response)
            outcome = walk.observe(page=page, records=records, retried=retried)
            if outcome is ListPageOutcome.RETRY_REQUIRED:
                retried = True
                continue
            if outcome is ListPageOutcome.ENDPOINT_BLOCKED:
                self._quality.append(
                    QualityResult(
                        kind=QualityResultKind.ENDPOINT_BLOCKED,
                        symbol=None,
                        dataset=ENDPOINT_DATASETS.get(endpoint),
                        detail={"endpoint": endpoint, "page": page},
                    )
                )
                return walk
            if outcome is ListPageOutcome.COMPLETE:
                return walk
            retried = False
            page += 1

    def check_recycled_ticker(
        self,
        *,
        symbol: str,
        collected_cik: str | None,
        prior_cik: str | None,
    ) -> bool:
        """A ``cik`` disagreement blocks the symbol instead of joining histories."""

        if prior_cik is None or collected_cik is None or collected_cik == prior_cik:
            return False
        self._block_symbol(
            symbol,
            QualityResultKind.RECYCLED_TICKER_AMBIGUITY,
            dataset="fmp_profile",
            detail={"prior_cik": prior_cik, "collected_cik": collected_cik},
        )
        return True

    def record_delisted_shortfall(self, *, symbol: str, dataset: str, window: DateWindow) -> None:
        """An empty validated response is never proof of absence."""

        self._quality.append(
            QualityResult(
                kind=QualityResultKind.DELISTED_SHORTFALL,
                symbol=symbol,
                dataset=dataset,
                detail={
                    "from": window.start.isoformat(),
                    "to": window.end.isoformat(),
                    "interpretation": "recorded_shortfall_not_proof_of_absence",
                },
            )
        )

    def record_observed_coverage_start(
        self,
        *,
        symbol: str,
        dataset: str,
        observed_start: date,
        requested_start: date,
    ) -> None:
        """Coverage evidence never asserts an IPO or listing date."""

        self._quality.append(
            QualityResult(
                kind=QualityResultKind.OBSERVED_COVERAGE_START,
                symbol=symbol,
                dataset=dataset,
                detail={
                    "requested_start": requested_start.isoformat(),
                    "observed_start": observed_start.isoformat(),
                    "interpretation": "observed_coverage_only_not_a_listing_date",
                },
            )
        )

    def _block_symbol(
        self,
        symbol: str,
        kind: QualityResultKind,
        *,
        dataset: str | None,
        detail: Mapping[str, object],
    ) -> None:
        if symbol not in self._blocked_symbols:
            self._blocked_symbols.append(symbol)
        self._quality.append(
            QualityResult(kind=kind, symbol=symbol, dataset=dataset, detail=detail)
        )

    # -- planning ----------------------------------------------------------

    def plan_symbol_windows(
        self,
        *,
        dataset: str,
        entry: UniverseEntry,
        watermark: date | None,
        known_dates: Sequence[date],
    ) -> tuple[DateWindow, ...]:
        """Choose incremental or backfill windows per sections 8.3 and 8.4."""

        incremental = plan_incremental_window(
            as_of=self._config.as_of,
            watermark=watermark,
            collected_dates=known_dates,
        )
        if not incremental.backfill_required and incremental.window is not None:
            return (incremental.window,)
        if self._config.operator_from is None:
            raise ValueError("backfill mode requires an explicit operator --from date")
        plan = plan_backfill_windows(
            entry=entry,
            operator_from=self._config.operator_from,
            as_of=self._config.as_of,
        )
        if plan.skipped_reason is not None:
            self._quality.append(
                QualityResult(
                    kind=QualityResultKind.EMPTY_RANGE_SKIPPED,
                    symbol=entry.symbol,
                    dataset=dataset,
                    detail={"reason": plan.skipped_reason},
                )
            )
        return plan.windows

    # -- receipts and publication -----------------------------------------

    def build_receipt(
        self,
        *,
        run_id: str,
        plan: CollectionRunPlan,
        ledger: UsageLedger | None = None,
        publication_marker_sha256: str | None = None,
    ) -> bytes:
        if self._unknown_fields:
            self._quality.append(
                QualityResult(
                    kind=QualityResultKind.SCHEMA_OBSERVATION,
                    symbol=None,
                    dataset=None,
                    detail={"unknown_fields": sorted(self._unknown_fields)},
                )
            )
            self._unknown_fields.clear()
        resolved_ledger = self._limiter.ledger() if ledger is None else ledger
        receipt = {
            "run_id": run_id,
            "plan_id": plan.plan_id,
            "plan_sha256": plan.plan_sha256,
            "as_of": self._config.as_of.isoformat(),
            "mode": self._config.mode.value,
            "run_identity": self._config.run_identity,
            "run_seed": self._limiter.run_seed,
            "artifact_hashes": dict(sorted(self._config.artifact_hashes.items())),
            "requests": self._requests,
            "raw_content_addresses": [
                f"sha256:{digest}" for digest in sorted(self._captured.digests)
            ],
            "provenance_addresses": [
                f"sha256:{hashlib.sha256(record).hexdigest()}"
                for record in self._provenance_records
            ],
            "request_metadata_chunk_addresses": (
                [] if self._receipt_metadata is None else list(self._receipt_metadata.addresses())
            ),
            "window_splits": self._splits,
            "quality_results": [
                {
                    "kind": result.kind.value,
                    "symbol": result.symbol,
                    "dataset": result.dataset,
                    "detail": dict(result.detail),
                }
                for result in self._quality
            ],
            "blocked_symbols": list(self._blocked_symbols),
            "usage": {
                "calls_attempted": resolved_ledger.calls_attempted,
                "bytes_received": resolved_ledger.bytes_received,
                "rate_limited_attempts": resolved_ledger.rate_limited_attempts,
                "retry_after_waits": resolved_ledger.retry_after_waits,
            },
        }
        if publication_marker_sha256 is not None:
            receipt["publication_marker_sha256"] = publication_marker_sha256
        receipt_bytes = canonical_json_bytes(receipt)
        assert_credential_absent(self._credential, receipt_bytes)
        return receipt_bytes

    def receipt_destination(self) -> Path:
        """Sharded runs publish receipts under per-shard evidence names."""

        evidence = self._config.receipt_evidence_path
        return self._config.receipt_path if evidence is None else evidence

    def publish(
        self,
        *,
        raw_bodies: Sequence[bytes] | None = None,
        dataset_publications: Sequence[tuple[Path, bytes]],
        receipt_bytes: bytes,
    ) -> tuple[Path, ...]:
        """Publish the captured raw union, datasets, and receipt atomically."""

        published_bodies: dict[str, bytes] = {}
        for body in raw_bodies or ():
            published_bodies.setdefault(hashlib.sha256(body).hexdigest(), body)
        bodies = tuple(published_bodies.values())
        assert_credential_absent(self._credential, *bodies, receipt_bytes)
        publications: list[tuple[Path, bytes]] = [
            (raw_blob_path(self._config.raw_store_root, body), body) for body in bodies
        ]
        publications.extend(
            (raw_provenance_path(self._config.raw_store_root, record), record)
            for record in self._provenance_records
        )
        publications.extend(dataset_publications)
        publications.append((self.receipt_destination(), receipt_bytes))
        self.require_bound_approval()
        published = publish_bundle(publications)
        metadata_paths = () if self._receipt_metadata is None else self._receipt_metadata.paths
        return tuple(dict.fromkeys((*self._captured.paths, *metadata_paths, *published)))

    # -- lifecycle ---------------------------------------------------------

    def register(self, plan: CollectionRunPlan, *, run_id: str) -> None:
        self.require_bound_approval()
        self._control_plane.register_plan(plan)
        state = self._control_plane.current_run_state(run_id)
        if state is not None and state.plan_id != plan.plan_id:
            self.require_bound_approval()
            self._control_plane.start_run(
                CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=self._clock())
            )
            raise CollectorContractError("run ID is never reused with another plan")
        if state is None:
            self.require_bound_approval()
            self._control_plane.start_run(
                CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=self._clock())
            )
            state = self._control_plane.current_run_state(run_id)
        if state is None:
            raise CollectorContractError("registered run state projection is missing")
        # A run row without an event is a durable partial registration. Its
        # immutable creation time must not be regenerated on resume; append
        # the required first event below instead.
        if state.state is None and (
            state.terminal
            or state.attempt_count != 0
            or state.last_event_seq is not None
            or state.last_occurred_at_utc is not None
        ):
            raise CollectorContractError("run without an initial event has an invalid projection")
        if state.state is not None:
            return
        self.require_bound_approval()
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_STARTED,
                occurred_at_utc=self._clock(),
                attempt_number=1,
            )
        )

    def succeed(self, *, run_id: str, occurred_at_utc: datetime | None = None) -> None:
        occurred = self._clock() if occurred_at_utc is None else occurred_at_utc
        state = self._control_plane.current_run_state(run_id)
        if state is not None and state.state is RunEventType.RUN_SUCCEEDED:
            return
        if state is None or state.state is not RunEventType.ATTEMPT_SUCCEEDED:
            self.require_bound_approval()
            self._control_plane.append_event(
                CollectionRunEvent(
                    run_id=run_id,
                    event_type=RunEventType.ATTEMPT_SUCCEEDED,
                    occurred_at_utc=occurred,
                    attempt_number=1,
                )
            )
        self.require_bound_approval()
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=occurred,
            )
        )

    def fail(
        self,
        *,
        run_id: str,
        error_class: str,
        error_message: str,
        require_bound_approval: bool = False,
    ) -> None:
        state = _failure_lifecycle_state(self._control_plane.current_run_state(run_id))
        if state is RunEventType.RUN_FAILED:
            return
        if state is RunEventType.ATTEMPT_STARTED:
            if require_bound_approval:
                self.require_bound_approval()
            self._control_plane.append_event(
                CollectionRunEvent(
                    run_id=run_id,
                    event_type=RunEventType.ATTEMPT_FAILED,
                    occurred_at_utc=self._clock(),
                    attempt_number=1,
                    error_class=error_class,
                    error_message=error_message,
                )
            )
        if require_bound_approval:
            self.require_bound_approval()
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_FAILED,
                occurred_at_utc=self._clock(),
                error_class=error_class,
                error_message=error_message,
            )
        )

    def cancel(self, *, run_id: str, reason: str, require_bound_approval: bool = False) -> None:
        if require_bound_approval:
            self.require_bound_approval()
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_CANCELLED,
                occurred_at_utc=self._clock(),
                details={"reason": reason},
            )
        )

    def record_usage(
        self,
        *,
        run_id: str,
        recorded_at_utc: datetime | None = None,
        ledger: UsageLedger | None = None,
        require_bound_approval: bool = False,
    ) -> None:
        resolved_ledger = self._limiter.ledger() if ledger is None else ledger
        loader = getattr(self._control_plane, "run_usage_records", None)
        existing = () if loader is None else loader(run_id)
        totals: dict[str, Decimal] = {}
        for prior in existing:
            totals[prior.metric] = totals.get(prior.metric, Decimal(0)) + prior.quantity
        sequence = max((prior.usage_seq for prior in existing), default=0)
        for metric, quantity, unit in resolved_ledger.as_usage_records():
            delta = Decimal(quantity) - totals.get(metric, Decimal(0))
            if delta < 0:
                raise CollectorContractError("persisted usage exceeds durable ledger")
            if delta == 0 and metric in totals:
                continue
            sequence += 1
            if require_bound_approval:
                self.require_bound_approval()
            insert_boundary = self._clock()
            recorded = (
                insert_boundary
                if recorded_at_utc is None
                else max(recorded_at_utc, insert_boundary)
            )
            record = CollectionUsageRecord(
                run_id=run_id,
                usage_seq=sequence,
                metric=metric,
                quantity=delta,
                unit=unit,
                recorded_at_utc=recorded,
            )

            def _commit_boundary(
                lower: datetime = recorded,
                clock: Callable[[], datetime] = self._clock,
            ) -> datetime:
                return max(lower, clock())

            self._control_plane.record_usage(record, recorded_at_clock=_commit_boundary)

    def advance_watermarks(
        self,
        *,
        run_id: str,
        advances: Sequence[tuple[str, str, date]],
    ) -> tuple[tuple[str, str, str], ...]:
        """Only a terminally successful run may advance a watermark."""

        state = self._control_plane.current_run_state(run_id)
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            raise CollectorContractError(
                "watermarks advance only after a terminal run_succeeded event"
            )
        advanced: list[tuple[str, str, str]] = []
        for dataset, stream, value in advances:
            if stream in self._blocked_symbols:
                continue
            self.require_bound_approval()
            self._control_plane.advance_watermark(
                WatermarkAdvance(
                    provider=PROVIDER,
                    dataset=dataset,
                    stream=stream,
                    run_id=run_id,
                    watermark_value=value.isoformat(),
                    watermark_position=datetime.combine(value, datetime.min.time(), tzinfo=UTC),
                )
            )
            advanced.append((dataset, stream, value.isoformat()))
        return tuple(advanced)

    def latest_watermark_date(self, *, dataset: str, symbol: str) -> date | None:
        current = self._control_plane.latest_watermark(PROVIDER, dataset, symbol)
        if current is None:
            return None
        return date.fromisoformat(current.watermark_value)

    @property
    def quality_results(self) -> tuple[QualityResult, ...]:
        return tuple(self._quality)

    @property
    def blocked_symbols(self) -> tuple[str, ...]:
        return tuple(self._blocked_symbols)


def new_run_id() -> str:
    return f"fmp-run-{uuid.uuid4().hex}"


__all__ = [
    "ALLOWED_HOST",
    "BASE_URL",
    "LOCK_RELATIVE_PATH",
    "REQUEST_TIMEOUT_SECONDS",
    "BudgetExhaustedError",
    "CollectorConfig",
    "CollectorContractError",
    "CollectorLock",
    "CollectorLockError",
    "CollectorLockLostError",
    "CollectorOutcome",
    "CollectorRequest",
    "CollectorResponse",
    "ControlPlanePort",
    "CredentialLeakError",
    "DestinationError",
    "EntitlementError",
    "FmpCollector",
    "PlanResumeRecord",
    "QualityResult",
    "QualityResultKind",
    "RetryCeilingError",
    "SymbolTask",
    "Transport",
    "assert_credential_absent",
    "build_run_plan",
    "new_run_id",
    "plan_resume_record",
    "publish_bundle",
    "raw_blob_path",
    "read_plan_resume_record",
    "redact_headers",
    "validate_destination",
]
