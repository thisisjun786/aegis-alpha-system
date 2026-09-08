"""AAS-DATA-013 G-A SEC EDGAR verifier collector.

Synthetic-first: tests inject a fixture transport that cannot open a socket.
The collector reads a frozen 007 identity double, never writes ``identity_*``,
and never updates FMP, Norgate, or canonical rows. Disagreements become a
diagnostic sidecar only (ADR 0004).
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

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
from aegis_alpha.data.sec_disagreement import (
    DISAGREEMENT_DATASET,
    ComparableFact,
    DisagreementRow,
    compare_disagreements,
    sec_comparable_facts,
    sidecar_relative_path,
)
from aegis_alpha.data.sec_evidence import open_directory, publish_bytes, read_bytes
from aegis_alpha.data.sec_identity import (
    Admission,
    AdmissionState,
    IdentityError,
    IdentityPort,
    admit_universe,
    pad_cik,
)
from aegis_alpha.data.sec_normalize import (
    FACTS_COLUMNS,
    FACTS_DATASET,
    NORMALIZED_VERSION,
    PROVIDER,
    SUBMISSIONS_COLUMNS,
    SUBMISSIONS_DATASET,
    THIRTEEN_F_COLUMNS,
    THIRTEEN_F_DATASET,
    ContractError,
    NormalizationContext,
    cursor_is_newer,
    extract_13f_index,
    extract_cik_source,
    latest_filing_cursor,
    normalize_companyfacts,
    normalize_submissions,
    parse_json_object,
    partition_key,
    row_partition_moment,
    sort_fact_rows,
)
from aegis_alpha.data.sec_rate_limit import (
    HTTP_FORBIDDEN,
    HTTP_TOO_MANY_REQUESTS,
    BudgetExhaustedError,
    RateLimiter,
    RetryCeilingError,
    classify_failure,
)
from aegis_alpha.data.sec_transport import (
    ALLOWED_HOST,
    CollectorRequest,
    CollectorResponse,
    DatasetKind,
    Transport,
    TransportError,
    assert_user_agent_absent,
    redact_headers,
    require_allowed_url,
)
from aegis_alpha.data.serialization import canonical_json_bytes

TASK_ID: Final = "AAS-DATA-013"
SCHEMA_VERSION: Final = 1
DATASET: Final = "sec:submissions+companyfacts+13f_index"
HTTP_SUCCESS_MIN: Final = 200
HTTP_REDIRECT_MIN: Final = 300


def submissions_stream(cik: str) -> str:
    """Per-CIK 005 stream. The plan dataset stays ``DATASET``."""

    return f"submissions:{cik}"


def companyfacts_stream(cik: str) -> str:
    """Per-CIK company-facts stream on the same plan dataset."""

    return f"companyfacts:{cik}"


HTTP_SERVER_ERROR_FLOOR: Final = 500


class DestinationError(RuntimeError):
    """A live destination resolves inside a Git repository."""


class CollectorError(RuntimeError):
    """Fail-closed collector error that is safe to report without User-Agent."""


class ControlPlanePort(Protocol):
    """Exactly the seven AAS-DATA-005 operations 013 is allowed to call."""

    def register_plan(self, plan: CollectionRunPlan) -> None: ...

    def start_run(self, run: CollectionRun) -> None: ...

    def append_event(self, event: CollectionRunEvent) -> int: ...

    def current_run_state(self, run_id: str) -> CollectionRunState | None: ...

    def advance_watermark(self, advance: WatermarkAdvance) -> int: ...

    def latest_watermark(
        self, provider: str, dataset: str, stream: str
    ) -> CurrentWatermark | None: ...

    def record_usage(self, record: CollectionUsageRecord) -> None: ...


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    raw_store_root: Path
    dataset_root: Path
    receipt_path: Path
    mode: CollectionMode
    max_calls: int
    run_identity: str
    as_of: datetime
    normalized_version: str = NORMALIZED_VERSION


@dataclass(frozen=True, slots=True)
class CapturedResponse:
    response: CollectorResponse
    snapshot_id: str


@dataclass(frozen=True, slots=True)
class PublicationInputs:
    run_id: str
    plan: CollectionRunPlan
    admissions: tuple[Admission, ...]
    skips: tuple[SkipReceipt, ...]
    submissions: tuple[Mapping[str, object], ...]
    facts: tuple[Mapping[str, object], ...]
    thirteen_f: tuple[Mapping[str, object], ...]
    disagreements: tuple[DisagreementRow, ...]


@dataclass(frozen=True, slots=True)
class SkipReceipt:
    instrument_id: str
    state: AdmissionState
    reason: str | None
    cik: str | None


@dataclass(frozen=True, slots=True)
class CollectorOutcome:
    run_id: str
    plan_id: str
    terminal_event: RunEventType
    published_paths: tuple[Path, ...]
    receipt_path: Path | None
    admissions: tuple[Admission, ...]
    skips: tuple[SkipReceipt, ...]
    watermarks_advanced: tuple[tuple[str, str, str], ...]
    disagreement_count: int
    calls_attempted: int
    identity_writes: int = 0


def containing_git_repository(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def validate_destination(label: str, destination: Path) -> Path:
    if not destination.is_absolute() or ".." in destination.parts:
        raise DestinationError(f"{label} must be an absolute path without parent traversal")
    if containing_git_repository(destination):
        raise DestinationError(f"{label} must be outside a Git repository")
    if destination.is_symlink():
        raise DestinationError(f"{label} must not be a symlink")
    ancestor = destination if destination.is_dir() else destination.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    try:
        with open_directory(ancestor):
            pass
    except (ValueError, OSError) as error:
        raise DestinationError(f"{label} must not traverse a symlink") from error
    return destination


def _publish_immutable(destination: Path, content: bytes) -> bool:
    return publish_bytes(destination, content)


def publish_bundle(publications: Sequence[tuple[Path, bytes]]) -> tuple[Path, ...]:
    for destination, content in publications:
        if destination.exists() and read_bytes(destination, maximum=len(content)) != content:
            raise FileExistsError("SEC immutable publication conflicts")
    for destination, content in publications:
        _publish_immutable(destination, content)
    return tuple(destination for destination, _content in publications)


def raw_blob_path(raw_store_root: Path, dataset: DatasetKind, content: bytes) -> Path:
    digest = hashlib.sha256(content).hexdigest()
    return raw_store_root / "sec" / dataset.value / "sha256" / digest[:2] / f"{digest}.raw"


def raw_provenance_path(raw_store_root: Path, provenance: bytes) -> Path:
    digest = hashlib.sha256(provenance).hexdigest()
    return raw_store_root / "sec" / "provenance" / "sha256" / digest[:2] / f"{digest}.json"


def build_run_plan(
    *,
    mode: CollectionMode,
    parameters: Mapping[str, object],
    created_at_utc: datetime,
) -> CollectionRunPlan:
    provisional = CollectionRunPlan(
        plan_id="sec-plan-provisional",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=DATASET,
        mode=mode,
        requested_window_start=None,
        requested_window_end=None,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )
    digest = collection_plan_digest(provisional)
    return CollectionRunPlan(
        plan_id=f"sec-plan-{digest}",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=DATASET,
        mode=mode,
        requested_window_start=None,
        requested_window_end=None,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )


def new_run_id() -> str:
    return f"sec-run-{uuid.uuid4().hex}"


def _require_matching_payload_cik(payload: Mapping[str, object], admission: Admission) -> str:
    """Keep the unpadded payload CIK, but fail closed if it is not the admitted CIK."""

    raw_cik_source = extract_cik_source(payload)
    try:
        padded = pad_cik(raw_cik_source)
    except IdentityError as error:
        raise CollectorError("payload CIK is not a normalizable 1-10 digit value") from error
    if padded != admission.cik:
        raise CollectorError("payload CIK does not match the admitted CIK")
    return raw_cik_source


def _arrow_type(name: str, sample: object) -> pa.DataType:
    if name in {"accepted_at", "observed_at_utc"}:
        return pa.timestamp("us", tz="UTC")
    if isinstance(sample, bool):
        return pa.bool_()
    if isinstance(sample, int) and not isinstance(sample, bool):
        return pa.int64()
    if isinstance(sample, float):
        return pa.float64()
    return pa.string()


def _rows_to_table(rows: Sequence[Mapping[str, object]], columns: Sequence[str]) -> pa.Table:
    arrays: dict[str, list[object]] = {name: [] for name in columns}
    samples: dict[str, object] = {}
    for row in rows:
        for name in columns:
            value = row.get(name)
            if isinstance(value, datetime):
                stored: object = value.astimezone(UTC)
            elif value is None or isinstance(value, (str, int, float, bool)):
                stored = value
            else:
                stored = str(value)
            arrays[name].append(stored)
            if stored is not None and name not in samples:
                samples[name] = stored
    fields = [pa.field(name, _arrow_type(name, samples.get(name))) for name in columns]
    return pa.Table.from_pydict(arrays, schema=pa.schema(fields))


def partitioned_parquet_publications(
    dataset_root: Path,
    dataset: str,
    rows: Sequence[Mapping[str, object]],
    columns: Sequence[str],
) -> list[tuple[Path, bytes]]:
    grouped: dict[tuple[str, str], list[Mapping[str, object]]] = {}
    for row in rows:
        key = partition_key(str(row["cik"]), row_partition_moment(row, dataset))
        grouped.setdefault(key, []).append(row)
    publications: list[tuple[Path, bytes]] = []
    for (prefix, year), group in sorted(grouped.items()):
        relative = Path(dataset) / f"cik_prefix={prefix}" / f"year={year}" / "part-000.parquet"
        destination = dataset_root / relative
        ordered = sort_fact_rows(group) if dataset == FACTS_DATASET else list(group)
        publications.append((destination, _table_bytes(_rows_to_table(ordered, columns))))
    return publications


def _table_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def disagreement_sidecar_publications(
    dataset_root: Path,
    rows: Sequence[DisagreementRow],
) -> list[tuple[Path, bytes]]:
    grouped: dict[int, list[DisagreementRow]] = {}
    for row in rows:
        grouped.setdefault(row.observed_at_utc.year, []).append(row)
    publications: list[tuple[Path, bytes]] = []
    for year, group in sorted(grouped.items()):
        destination = dataset_root / sidecar_relative_path(year)
        table = _rows_to_table([item.projection() for item in group], tuple(group[0].projection()))
        publications.append((destination, _table_bytes(table)))
    return publications


@dataclass
class SecCollector:
    """Bounded, fail-closed SEC first-wave collection engine."""

    config: CollectorConfig
    transport: Transport
    control_plane: ControlPlanePort
    limiter: RateLimiter
    clock: Callable[[], datetime]
    user_agent: str | None = None
    _requests: list[dict[str, object]] = field(default_factory=list)
    _captured: dict[str, bytes] = field(default_factory=dict)
    _provenance_records: list[bytes] = field(default_factory=list)
    _attempt_seq: int = 0
    _identity_writes: int = 0

    def collect(
        self,
        *,
        identity: IdentityPort,
        run_id: str | None = None,
        instrument_ids: Sequence[str] | None = None,
        fmp_facts: Sequence[ComparableFact] = (),
        fmp_source_bytes: Mapping[Path, bytes] | None = None,
    ) -> CollectorOutcome:
        """Run one probe/incremental/backfill attempt against admitted CIKs only."""

        as_of = self.config.as_of
        admissions = admit_universe(identity, instrument_ids, as_of)
        skips = tuple(
            SkipReceipt(
                instrument_id=item.instrument_id,
                state=item.state,
                reason=item.reason,
                cik=item.cik,
            )
            for item in admissions
            if item.state is not AdmissionState.ADMITTED
        )
        admitted = tuple(item for item in admissions if item.state is AdmissionState.ADMITTED)
        plan = build_run_plan(
            mode=self.config.mode,
            parameters={
                "allowed_host": ALLOWED_HOST,
                "instrument_ids": [item.instrument_id for item in admissions],
                "max_calls": self.config.max_calls,
                "normalized_version": self.config.normalized_version,
                "task_id": TASK_ID,
                "run_identity": self.config.run_identity,
                "as_of": self.config.as_of.isoformat(),
            },
            created_at_utc=self.clock(),
        )
        run_id = new_run_id() if run_id is None else run_id
        self._register(plan, run_id=run_id)
        fmp_snapshot = dict(fmp_source_bytes or {})
        try:
            submissions_rows: list[dict[str, object]] = []
            facts_rows: list[dict[str, object]] = []
            thirteen_f_rows: list[dict[str, object]] = []
            pending_watermarks: list[WatermarkAdvance] = []
            for admission in admitted:
                collected = self._collect_admitted(admission, run_id=run_id)
                submissions_rows.extend(collected.submissions)
                facts_rows.extend(collected.facts)
                thirteen_f_rows.extend(collected.thirteen_f)
                pending_watermarks.extend(collected.watermarks)
            disagreement_rows = compare_disagreements(
                sec_comparable_facts(facts_rows, snapshot_id=run_id),
                fmp_facts,
                observed_at_utc=self.clock(),
            )
            self._assert_fmp_untouched(fmp_snapshot)
            publication = PublicationInputs(
                run_id=run_id,
                plan=plan,
                admissions=admissions,
                skips=skips,
                submissions=tuple(submissions_rows),
                facts=tuple(facts_rows),
                thirteen_f=tuple(thirteen_f_rows),
                disagreements=disagreement_rows,
            )
            published = self._publish(publication)
            self._record_usage(run_id=run_id)
            self._succeed(run_id=run_id)
            advanced = self._advance_watermarks(run_id=run_id, advances=pending_watermarks)
            return CollectorOutcome(
                run_id=run_id,
                plan_id=plan.plan_id,
                terminal_event=RunEventType.RUN_SUCCEEDED,
                published_paths=published,
                receipt_path=self.config.receipt_path,
                admissions=admissions,
                skips=skips,
                watermarks_advanced=advanced,
                disagreement_count=len(disagreement_rows),
                calls_attempted=self.limiter.calls_attempted,
                identity_writes=self._identity_writes,
            )
        except (
            BudgetExhaustedError,
            RetryCeilingError,
            TransportError,
            ContractError,
            CollectorError,
            FileExistsError,
        ) as error:
            self._record_usage(run_id=run_id)
            self._fail(
                run_id=run_id,
                error_class=type(error).__name__,
                error_message="SEC collection failed",
            )
            return CollectorOutcome(
                run_id=run_id,
                plan_id=plan.plan_id,
                terminal_event=RunEventType.RUN_FAILED,
                published_paths=(),
                receipt_path=None,
                admissions=admissions,
                skips=skips,
                watermarks_advanced=(),
                disagreement_count=0,
                calls_attempted=self.limiter.calls_attempted,
                identity_writes=self._identity_writes,
            )

    def _collect_admitted(self, admission: Admission, *, run_id: str) -> _CollectedCik:
        if admission.cik is None or admission.cik_source is None or admission.issuer_id is None:
            raise CollectorError("admitted instrument is missing a CIK binding")
        cik = admission.cik
        submissions_response = self._execute(CollectorRequest(DatasetKind.SUBMISSIONS, cik))
        submissions_payload = parse_json_object(
            submissions_response.response.body, label="submissions"
        )
        raw_cik_source = _require_matching_payload_cik(submissions_payload, admission)
        context = NormalizationContext(
            cik_source=raw_cik_source,
            instrument_id=admission.instrument_id,
            issuer_id=admission.issuer_id,
            snapshot_id=submissions_response.snapshot_id,
            raw_content_sha256=submissions_response.response.content_sha256,
        )
        submissions = list(normalize_submissions(submissions_payload, context))
        thirteen_f = list(extract_13f_index(submissions))
        cursor = latest_filing_cursor(submissions)
        prior = self.control_plane.latest_watermark(PROVIDER, DATASET, submissions_stream(cik))
        prior_value = None if prior is None else prior.watermark_value
        fetch_facts = self.config.mode is not CollectionMode.INCREMENTAL or (
            cursor is not None and cursor_is_newer(cursor, prior_value)
        )
        facts: list[dict[str, object]] = []
        watermarks: list[WatermarkAdvance] = []
        if fetch_facts:
            facts_response = self._execute(CollectorRequest(DatasetKind.COMPANYFACTS, cik))
            facts_payload = parse_json_object(facts_response.response.body, label="companyfacts")
            facts.extend(
                normalize_companyfacts(
                    facts_payload,
                    NormalizationContext(
                        cik_source=_require_matching_payload_cik(facts_payload, admission),
                        instrument_id=admission.instrument_id,
                        issuer_id=admission.issuer_id,
                        snapshot_id=facts_response.snapshot_id,
                        raw_content_sha256=facts_response.response.content_sha256,
                    ),
                )
            )
            if (
                cursor is not None
                and self.config.mode is not CollectionMode.PROBE
                and self._watermark_moves_forward(
                    stream=companyfacts_stream(cik),
                    position=cursor.accepted_at,
                )
            ):
                watermarks.append(
                    WatermarkAdvance(
                        provider=PROVIDER,
                        dataset=DATASET,
                        stream=companyfacts_stream(cik),
                        run_id=run_id,
                        watermark_value=cursor.watermark_value,
                        watermark_position=cursor.accepted_at,
                    )
                )
        if (
            cursor is not None
            and self.config.mode is not CollectionMode.PROBE
            and self._watermark_moves_forward(
                stream=submissions_stream(cik),
                position=cursor.accepted_at,
            )
        ):
            watermarks.append(
                WatermarkAdvance(
                    provider=PROVIDER,
                    dataset=DATASET,
                    stream=submissions_stream(cik),
                    run_id=run_id,
                    watermark_value=cursor.watermark_value,
                    watermark_position=cursor.accepted_at,
                )
            )
        return _CollectedCik(
            submissions=submissions,
            facts=facts,
            thirteen_f=thirteen_f,
            watermarks=watermarks,
        )

    def _request_preflight(self) -> None:
        """Runtime policy revalidation runs after pacing and before call consumption."""

    def _before_transport(self, request: CollectorRequest) -> None:
        """Durable runtimes journal admission before transport; fixtures need no journal."""

    def _execute(self, request: CollectorRequest) -> CapturedResponse:
        require_allowed_url(request.source_uri)
        attempt_index = 1
        while True:
            self.limiter.before_request(revalidate=self._request_preflight)
            self._before_transport(request)
            try:
                response = self.transport(request)
            except TimeoutError:
                self.limiter.wait_before_retry(
                    attempt_index=attempt_index,
                    failure=classify_failure(None),
                )
                attempt_index += 1
                continue
            self.limiter.after_response(byte_count=len(response.body))
            if self.user_agent is not None:
                assert_user_agent_absent(self.user_agent, response.body)
            snapshot_id = self._capture(request, response)
            self._record_request(request, response, attempts=attempt_index)
            if HTTP_SUCCESS_MIN <= response.status_code < HTTP_REDIRECT_MIN:
                return CapturedResponse(response, snapshot_id)
            if response.status_code not in {HTTP_TOO_MANY_REQUESTS, HTTP_FORBIDDEN} and (
                response.status_code < HTTP_SERVER_ERROR_FLOOR
            ):
                raise TransportError(f"provider returned HTTP {response.status_code}")
            self.limiter.wait_before_retry(
                attempt_index=attempt_index,
                failure=classify_failure(response.status_code),
                headers=response.headers,
            )
            attempt_index += 1

    def _capture(self, request: CollectorRequest, response: CollectorResponse) -> str:
        self._attempt_seq += 1
        provenance = canonical_json_bytes(
            {
                "attempt_seq": self._attempt_seq,
                "content_sha256": response.content_sha256,
                "dataset": request.dataset.value,
                "raw_byte_length": len(response.body),
                "request_fingerprint": request.request_fingerprint,
                "requested_at_utc": response.requested_at_utc,
                "response_headers": redact_headers(response.headers),
                "retrieved_at_utc": response.retrieved_at_utc,
                "run_identity": self.config.run_identity,
                "source_uri": request.source_uri,
                "status_code": response.status_code,
            }
        )
        if self.user_agent is not None:
            assert_user_agent_absent(self.user_agent, response.body, provenance)
        root = self.config.raw_store_root
        publish_bundle(
            [
                (raw_blob_path(root, request.dataset, response.body), response.body),
                (raw_provenance_path(root, provenance), provenance),
            ]
        )
        self._captured.setdefault(response.content_sha256, response.body)
        self._provenance_records.append(provenance)
        return "sec-response-" + hashlib.sha256(provenance).hexdigest()

    def _record_request(
        self,
        request: CollectorRequest,
        response: CollectorResponse,
        *,
        attempts: int,
    ) -> None:
        self._requests.append(
            {
                "attempts": attempts,
                "cik": request.cik,
                "dataset": request.dataset.value,
                "disposition": (
                    "succeeded"
                    if HTTP_SUCCESS_MIN <= response.status_code < HTTP_REDIRECT_MIN
                    else "failed"
                ),
                "raw_byte_length": len(response.body),
                "raw_content_address": f"sha256:{response.content_sha256}",
                "request_fingerprint": request.request_fingerprint,
                "requested_at_utc": response.requested_at_utc,
                "response_headers": redact_headers(response.headers),
                "retrieved_at_utc": response.retrieved_at_utc,
                "source_uri": request.source_uri,
                "status_code": response.status_code,
            }
        )

    def _publish(self, publication: PublicationInputs) -> tuple[Path, ...]:
        version_root = (
            self.config.dataset_root / "normalized" / PROVIDER / self.config.normalized_version
        )
        publications: list[tuple[Path, bytes]] = []
        for dataset, rows, columns in (
            (SUBMISSIONS_DATASET, publication.submissions, SUBMISSIONS_COLUMNS),
            (FACTS_DATASET, publication.facts, FACTS_COLUMNS),
            (THIRTEEN_F_DATASET, publication.thirteen_f, THIRTEEN_F_COLUMNS),
        ):
            if rows:
                publications.extend(
                    partitioned_parquet_publications(version_root, dataset, rows, columns)
                )
        if publication.disagreements:
            publications.extend(
                disagreement_sidecar_publications(version_root, publication.disagreements)
            )
        receipt = self._build_receipt(publication)
        if self.user_agent is not None:
            assert_user_agent_absent(self.user_agent, receipt)
        publications.append((self.config.receipt_path, receipt))
        return publish_bundle(publications)

    def _build_receipt(self, publication: PublicationInputs) -> bytes:
        ledger = self.limiter.ledger()
        return canonical_json_bytes(
            {
                "admissions": [
                    {
                        "cik": item.cik,
                        "instrument_id": item.instrument_id,
                        "issuer_id": item.issuer_id,
                        "reason": item.reason,
                        "state": item.state.value,
                    }
                    for item in publication.admissions
                ],
                "allowed_host": ALLOWED_HOST,
                "calls_attempted": ledger.calls_attempted,
                "dataset": DATASET,
                "disagreement_count": len(publication.disagreements),
                "identity_writes": self._identity_writes,
                "mode": self.config.mode.value,
                "plan_id": publication.plan.plan_id,
                "plan_sha256": publication.plan.plan_sha256,
                "provider": PROVIDER,
                "row_counts": {
                    DISAGREEMENT_DATASET: len(publication.disagreements),
                    FACTS_DATASET: len(publication.facts),
                    SUBMISSIONS_DATASET: len(publication.submissions),
                    THIRTEEN_F_DATASET: len(publication.thirteen_f),
                },
                "run_id": publication.run_id,
                "run_identity": self.config.run_identity,
                "skips": [
                    {
                        "cik": item.cik,
                        "instrument_id": item.instrument_id,
                        "reason": item.reason,
                        "state": item.state.value,
                    }
                    for item in publication.skips
                ],
                "task_id": TASK_ID,
                "usage": {
                    "bytes_received": ledger.bytes_received,
                    "calls_attempted": ledger.calls_attempted,
                    "retries": ledger.retries,
                },
            }
        )

    def _assert_fmp_untouched(self, snapshot: Mapping[Path, bytes]) -> None:
        for path, original in snapshot.items():
            if path.exists() and path.read_bytes() != original:
                raise CollectorError("SEC disagreement sidecar must not mutate FMP source bytes")

    def _register(self, plan: CollectionRunPlan, *, run_id: str) -> None:
        self.control_plane.register_plan(plan)
        self.control_plane.start_run(
            CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=self.clock())
        )
        self.control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_STARTED,
                occurred_at_utc=self.clock(),
                attempt_number=1,
            )
        )

    def _succeed(self, *, run_id: str) -> None:
        self.control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_SUCCEEDED,
                occurred_at_utc=self.clock(),
                attempt_number=1,
            )
        )
        self.control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=self.clock(),
            )
        )

    def _fail(self, *, run_id: str, error_class: str, error_message: str) -> None:
        self.control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_FAILED,
                occurred_at_utc=self.clock(),
                attempt_number=1,
                error_class=error_class,
                error_message=error_message,
            )
        )
        self.control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_FAILED,
                occurred_at_utc=self.clock(),
                error_class=error_class,
                error_message=error_message,
            )
        )

    def _record_usage(self, *, run_id: str) -> None:
        for index, (metric, quantity, unit) in enumerate(
            self.limiter.ledger().as_usage_records(), start=1
        ):
            self.control_plane.record_usage(
                CollectionUsageRecord(
                    run_id=run_id,
                    usage_seq=index,
                    metric=metric,
                    quantity=Decimal(quantity),
                    unit=unit,
                    recorded_at_utc=self.clock(),
                )
            )

    def _watermark_moves_forward(self, *, stream: str, position: datetime) -> bool:
        current = self.control_plane.latest_watermark(PROVIDER, DATASET, stream)
        return current is None or position > current.watermark_position

    def _advance_watermarks(
        self,
        *,
        run_id: str,
        advances: Sequence[WatermarkAdvance],
    ) -> tuple[tuple[str, str, str], ...]:
        if self.config.mode is CollectionMode.PROBE:
            return ()
        state = self.control_plane.current_run_state(run_id)
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            raise CollectorError("watermarks advance only after a terminal run_succeeded event")
        advanced: list[tuple[str, str, str]] = []
        for advance in advances:
            current = self.control_plane.latest_watermark(PROVIDER, advance.dataset, advance.stream)
            if current is not None and advance.watermark_position <= current.watermark_position:
                continue
            self.control_plane.advance_watermark(advance)
            advanced.append((advance.dataset, advance.stream, advance.watermark_value))
        return tuple(advanced)


@dataclass(frozen=True, slots=True)
class _CollectedCik:
    submissions: list[dict[str, object]]
    facts: list[dict[str, object]]
    thirteen_f: list[dict[str, object]]
    watermarks: list[WatermarkAdvance]
