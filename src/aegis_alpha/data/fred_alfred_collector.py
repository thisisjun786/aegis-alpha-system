"""Vintage-preserving FRED collector with fixture defaults and durable runtime seams."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

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
from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.fred_alfred_evidence import capture_source, publish_evidence
from aegis_alpha.data.fred_alfred_normalize import (
    DATASET_VERSION,
    NormalizeError,
    assert_unique_keys,
    normalize_observations,
    normalize_series_dimension,
    observations_table,
    parquet_bytes,
    parse_fred_date,
    partition_relative_path,
    series_dimension_table,
)
from aegis_alpha.data.fred_alfred_rate_limit import (
    BudgetExhaustedError,
    RateLimiter,
    UsageLedger,
)
from aegis_alpha.data.fred_alfred_series import (
    ALLOWED_SERIES_IDS,
    DEFAULT_SERIES_IDS,
    PLAN_DATASET,
    PROVIDER,
    series_universe_sha256,
    validate_series_universe,
    watermark_dataset,
    watermark_stream,
)
from aegis_alpha.data.serialization import canonical_json_bytes

ALLOWED_HOST = "api.stlouisfed.org"
BASE_URL = f"https://{ALLOWED_HOST}"
CREDENTIAL_ENVIRONMENT_VARIABLE = "FRED_API_KEY"
USER_AGENT = "aegis-alpha-system/fred-alfred"
SCHEMA_VERSION = 1
PROBE_OBSERVATION_LIMIT = 20
VINTAGE_PAGE_LIMIT = 10_000
OBSERVATION_PAGE_LIMIT = 100_000
_HTTP_SUCCESS_MIN = 200
_HTTP_REDIRECT_MIN = 300
_MAX_RAW_RESPONSE_BYTES = 32 * 1024 * 1024
_MAX_PAGINATION_OFFSET = 1_000_000


class CollectorError(RuntimeError):
    """Fail-closed collector error that is safe to report without credentials."""


class CredentialLeakError(CollectorError):
    def __init__(self) -> None:
        super().__init__("credential material detected in provider or evidence bytes")


class DestinationError(CollectorError):
    """A live destination resolves inside a Git repository."""


class SeriesError(CollectorError):
    """One series failed; the run must not advance that series watermark."""


class ControlPlanePort(Protocol):
    """Exactly the seven AAS-DATA-005 operations this collector may call."""

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
class CollectorRequest:
    endpoint: str
    parameters: Mapping[str, str]
    series_id: str

    def __post_init__(self) -> None:
        if not self.endpoint.startswith("/fred/") or "?" in self.endpoint:
            raise ValueError("endpoint must be a credential-free /fred/ path")
        if self.series_id not in ALLOWED_SERIES_IDS:
            raise ValueError("collector must not invent extra series")
        for name, value in self.parameters.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("request parameters must be strings")
            folded = name.casefold().replace("_", "")
            if folded in {"apikey", "token"} or "key" in folded:
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


@dataclass(frozen=True, slots=True)
class CollectorResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    requested_at_utc: datetime
    retrieved_at_utc: datetime

    def __post_init__(self) -> None:
        if self.requested_at_utc.tzinfo is None or self.retrieved_at_utc.tzinfo is None:
            raise ValueError("collector timestamps must be timezone-aware")
        if len(self.body) > _MAX_RAW_RESPONSE_BYTES:
            raise ValueError("provider response exceeds the collector byte limit")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


Transport = Callable[[CollectorRequest, str], CollectorResponse]


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    raw_store_root: Path
    dataset_root: Path
    receipt_path: Path
    mode: CollectionMode
    max_calls: int
    run_identity: str
    series_ids: tuple[str, ...] = DEFAULT_SERIES_IDS
    observation_start: date | None = None
    probe_observation_limit: int = PROBE_OBSERVATION_LIMIT
    artifact_hashes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "series_ids", validate_series_universe(self.series_ids))
        if self.max_calls < 1:
            raise ValueError("--max-calls must be a positive integer")
        if self.mode is CollectionMode.BACKFILL and self.observation_start is None:
            raise ValueError("backfill requires observation_start")
        if self.probe_observation_limit < 1:
            raise ValueError("probe observation limit must be positive")


@dataclass(frozen=True, slots=True)
class SeriesOutcome:
    series_id: str
    succeeded: bool
    vintage_watermark: date | None
    row_count: int
    error_class: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class CollectorOutcome:
    run_id: str
    plan_id: str
    terminal_event: RunEventType
    published_paths: tuple[Path, ...]
    receipt_path: Path | None
    rows: tuple[Mapping[str, object], ...]
    series_outcomes: tuple[SeriesOutcome, ...]
    watermarks_advanced: tuple[tuple[str, str], ...]
    calls_attempted: int = 0
    recovered: bool = False


def containing_git_repository(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def validate_destination(label: str, destination: Path) -> Path:
    resolved = destination.resolve(strict=False)
    if repository_root := containing_git_repository(resolved):
        raise DestinationError(
            f"{label} must be outside a Git repository (resolved repository: {repository_root})"
        )
    return resolved


def assert_credential_absent(credential: str, *payloads: bytes) -> None:
    if not credential:
        return
    needle = credential.encode()
    for payload in payloads:
        if needle in payload:
            raise CredentialLeakError


def redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    redacted: dict[str, str] = {}
    for name, value in headers.items():
        folded = name.casefold()
        if folded in {"apikey", "authorization", "x-api-key", "cookie", "set-cookie"}:
            redacted[folded] = "[REDACTED]"
            continue
        redacted[folded] = value
    return redacted


def new_run_id() -> str:
    return f"fred-alfred-run-{uuid.uuid4().hex}"


def build_run_plan(  # noqa: PLR0913 - each field is one immutable 005 plan attribute
    *,
    mode: CollectionMode,
    series_ids: Sequence[str],
    created_at_utc: datetime,
    requested_window_start: datetime | None = None,
    requested_window_end: datetime | None = None,
    extra_parameters: Mapping[str, object] | None = None,
) -> CollectionRunPlan:
    validated = validate_series_universe(series_ids)
    parameters: dict[str, object] = {
        "series_ids": list(validated),
        "series_universe_sha256": series_universe_sha256(validated),
        "default_series_universe_sha256": series_universe_sha256(DEFAULT_SERIES_IDS),
    }
    if extra_parameters:
        parameters.update(extra_parameters)
    provisional = CollectionRunPlan(
        plan_id="fred-alfred-plan-provisional",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=PLAN_DATASET,
        mode=mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )
    digest = collection_plan_digest(provisional)
    return CollectionRunPlan(
        plan_id=f"fred-alfred-plan-{digest}",
        schema_version=SCHEMA_VERSION,
        provider=PROVIDER,
        dataset=PLAN_DATASET,
        mode=mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        parameters=parameters,
        created_at_utc=created_at_utc,
    )


def parse_json_object(body: bytes) -> Mapping[str, object]:
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CollectorError("provider response body is not valid JSON") from None
    if not isinstance(parsed, Mapping):
        raise CollectorError("provider response root must be a JSON object")
    return cast("Mapping[str, object]", parsed)


def parse_vintage_dates(document: Mapping[str, object]) -> tuple[date, ...]:
    raw = document.get("vintage_dates")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise CollectorError("vintagedates response must include a vintage_dates array")
    dates = [parse_fred_date("vintage_date", item) for item in raw]
    return tuple(sorted(set(dates)))


def parse_series_record(document: Mapping[str, object], *, series_id: str) -> Mapping[str, object]:
    block = document.get("seriess")
    if not isinstance(block, Sequence) or isinstance(block, (str, bytes)) or not block:
        raise CollectorError("series response must include a nonempty seriess array")
    first = block[0]
    if not isinstance(first, Mapping):
        raise CollectorError("series records must be JSON objects")
    record = cast("Mapping[str, object]", first)
    if record.get("id") != series_id:
        raise CollectorError("series response id does not match the requested series")
    return record


def parse_observation_records(document: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    raw = document.get("observations")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise CollectorError("observations response must include an observations array")
    records: list[Mapping[str, object]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise CollectorError("observation records must be JSON objects")
        records.append(cast("Mapping[str, object]", item))
    return tuple(records)


def optional_nonnegative_int(document: Mapping[str, object], field: str) -> int | None:
    raw = document.get(field)
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise CollectorError(f"{field} must be a nonnegative integer")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.isdigit():
        value = int(raw)
    else:
        raise CollectorError(f"{field} must be a nonnegative integer")
    if value < 0:
        raise CollectorError(f"{field} must be a nonnegative integer")
    return value


def page_is_truncated(
    *,
    page_length: int,
    accumulated_length: int,
    total_count: int | None,
    page_limit: int,
) -> bool:
    """True when a page cannot be treated as the complete result set."""

    if page_limit < 1:
        raise CollectorError("page limit must be positive")
    if total_count is not None and accumulated_length < total_count:
        return True
    return page_length == page_limit


def live_request_url(request: CollectorRequest, credential: str) -> str:
    """Build the live URL. The credential is attached only at this boundary."""

    if not credential:
        raise CollectorError("FRED_API_KEY is required for the live transport")
    query = urlencode(sorted({**dict(request.parameters), "api_key": credential}.items()))
    return f"{BASE_URL}{request.endpoint}?{query}"


class HostPinnedRedirectHandler(HTTPRedirectHandler):
    """Follow same-host redirects only. Cross-host hops leak the API key."""

    def redirect_request(  # noqa: PLR0913,PLR0917 - urllib redirect hook signature
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> Request | None:
        parsed = urlparse(newurl)
        hostname = (parsed.hostname or "").casefold()
        if hostname and hostname != ALLOWED_HOST:
            raise CollectorError(f"refusing cross-host redirect to {hostname}")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is None:
            return None
        host = (redirected.host or "").split("@")[-1].split(":")[0].casefold()
        if host != ALLOWED_HOST:
            raise CollectorError(f"refusing cross-host redirect to {host}")
        return redirected


class _NoAutomaticRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        # A redirect would be a second, unmetered request inside urllib. Return
        # its HTTP error response to the collector instead of following it.
        return None


def make_https_transport(*, timeout_seconds: float = 30.0) -> Transport:
    """Build host-pinned, non-redirecting HTTPS transport with bounded reads."""

    opener = build_opener(_NoAutomaticRedirect)

    def transport(request: CollectorRequest, credential: str) -> CollectorResponse:
        requested_at = datetime.now(UTC)
        url = live_request_url(request, credential)
        http_request = Request(  # noqa: S310 - URL is host-allowlisted to api.stlouisfed.org
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            method="GET",
        )
        request_host = (http_request.host or "").split("@")[-1].split(":")[0].casefold()
        if request_host != ALLOWED_HOST:
            raise CollectorError("live transport host allowlist is api.stlouisfed.org only")
        try:
            with opener.open(http_request, timeout=timeout_seconds) as response:
                payload = response.read(_MAX_RAW_RESPONSE_BYTES + 1)
                status = response.status
                headers = {name.lower(): value for name, value in response.headers.items()}
        except HTTPError as error:
            payload = error.read(_MAX_RAW_RESPONSE_BYTES + 1)
            status = error.code
            headers = (
                {name.lower(): value for name, value in error.headers.items()}
                if error.headers is not None
                else {}
            )
        except URLError as error:
            raise CollectorError(
                f"provider request failed: {type(error.reason).__name__}"
            ) from None
        return CollectorResponse(
            status_code=status,
            headers=redact_headers(headers),
            body=payload,
            requested_at_utc=requested_at,
            retrieved_at_utc=datetime.now(UTC),
        )

    return transport


class FredAlfredCollector:
    """Bounded vintage-preserving collector over an injected transport."""

    def __init__(  # noqa: PLR0913 - every dependency is an explicit injected port
        self,
        *,
        config: CollectorConfig,
        transport: Transport,
        control_plane: ControlPlanePort,
        limiter: RateLimiter,
        credential: str,
        clock: Callable[[], datetime],
        run_id: str | None = None,
        plan: CollectionRunPlan | None = None,
        startup: Callable[[CollectionRunPlan, str], None] | None = None,
        capture: Callable[[SourceSnapshot, Path], None] | None = None,
        finalize: Callable[[CollectionRunPlan, CollectorOutcome, UsageLedger], CollectorOutcome]
        | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._control_plane = control_plane
        self._limiter = limiter
        self._credential = credential
        self._clock = clock
        self._explicit_run_id = run_id
        self._plan = plan
        self._startup = startup
        self._capture_callback = capture
        self._finalize = finalize
        self._snapshots: list[SourceSnapshot] = []
        self._raw_addresses: list[str] = []
        self._requests: list[dict[str, object]] = []
        self._capture_seq = 0
        self._run_id = ""

    def collect(self) -> CollectorOutcome:
        for label, destination in (
            ("raw store root", self._config.raw_store_root),
            ("dataset root", self._config.dataset_root),
            ("receipt path", self._config.receipt_path),
        ):
            validate_destination(label, destination)
        created_at = self._clock()
        extra: dict[str, object] = {"max_calls": self._config.max_calls}
        if self._config.observation_start is not None:
            extra["observation_start"] = self._config.observation_start.isoformat()
        plan = self._plan or build_run_plan(
            mode=self._config.mode,
            series_ids=self._config.series_ids,
            created_at_utc=created_at,
            extra_parameters=extra,
        )
        run_id = self._explicit_run_id or new_run_id()
        self._run_id = run_id
        if self._startup is None:
            self._register(plan, run_id=run_id)
        else:
            self._startup(plan, run_id)
        try:
            outcomes, rows, dimensions = self._collect_universe()
        except (BudgetExhaustedError, CredentialLeakError, DestinationError):
            failed = CollectorOutcome(
                run_id=run_id,
                plan_id=plan.plan_id,
                terminal_event=RunEventType.RUN_FAILED,
                published_paths=(),
                receipt_path=None,
                rows=(),
                series_outcomes=(),
                watermarks_advanced=(),
            )
            return self._finish(plan, failed)
        try:
            published = self._publish(run_id, plan, rows, dimensions, outcomes)
        except (OSError, ValueError, CollectorError):
            failed = CollectorOutcome(
                run_id, plan.plan_id, RunEventType.RUN_FAILED, (), None, (), outcomes, ()
            )
            self._finish(plan, failed)
            raise
        succeeded = [item for item in outcomes if item.succeeded]
        return self._finish(
            plan,
            CollectorOutcome(
                run_id=run_id,
                plan_id=plan.plan_id,
                terminal_event=RunEventType.RUN_SUCCEEDED if succeeded else RunEventType.RUN_FAILED,
                published_paths=published,
                receipt_path=self._config.receipt_path if published else None,
                rows=tuple(rows),
                series_outcomes=outcomes,
                watermarks_advanced=(),
            ),
        )

    def _finish(self, plan: CollectionRunPlan, outcome: CollectorOutcome) -> CollectorOutcome:
        if self._finalize is not None:
            return self._finalize(plan, outcome, self._limiter.ledger())
        self._record_usage(outcome.run_id)
        advanced: tuple[tuple[str, str], ...] = ()
        if outcome.terminal_event is RunEventType.RUN_SUCCEEDED:
            self._succeed(outcome.run_id)
            advanced = self.advance_successful_watermarks(outcome.run_id, outcome.series_outcomes)
        else:
            self._fail(
                outcome.run_id, error_class="CollectorError", error_message="collection failed"
            )
        return dataclasses.replace(
            outcome, watermarks_advanced=advanced, calls_attempted=self._limiter.calls_attempted
        )

    def _collect_universe(
        self,
    ) -> tuple[tuple[SeriesOutcome, ...], list[dict[str, object]], list[dict[str, object]]]:
        outcomes: list[SeriesOutcome] = []
        rows: list[dict[str, object]] = []
        dimensions: list[dict[str, object]] = []
        for series_id in self._config.series_ids:
            try:
                outcome, series_rows, dimension = self._collect_series(series_id)
            except SeriesError as error:
                outcomes.append(
                    SeriesOutcome(
                        series_id=series_id,
                        succeeded=False,
                        vintage_watermark=None,
                        row_count=0,
                        error_class=type(error).__name__,
                        error_message=str(error),
                    )
                )
                continue
            outcomes.append(outcome)
            rows.extend(series_rows)
            if dimension is not None:
                dimensions.append(dimension)
        return (tuple(outcomes), rows, dimensions)

    def _collect_series(
        self, series_id: str
    ) -> tuple[SeriesOutcome, tuple[dict[str, object], ...], dict[str, object] | None]:
        try:
            meta_response, meta_snapshot = self._execute(
                CollectorRequest(
                    endpoint="/fred/series",
                    parameters={"file_type": "json", "series_id": series_id},
                    series_id=series_id,
                )
            )
            series_record = parse_series_record(
                parse_json_object(meta_response.body), series_id=series_id
            )
            vintages = self._fetch_vintage_dates(series_id)
            selected = self._select_vintages(series_id, vintages)
            observation_rows: list[dict[str, object]] = []
            last_vintage: date | None = None
            for vintage in selected:
                observation_rows.extend(self._collect_vintage(series_id, vintage))
                last_vintage = vintage
            dimension = normalize_series_dimension(
                series_record,
                series_id=series_id,
                source_snapshot_id=meta_snapshot.snapshot_id,
            )
        except (BudgetExhaustedError, CredentialLeakError, DestinationError):
            raise
        except (CollectorError, NormalizeError) as error:
            raise SeriesError(str(error)) from error
        return (
            SeriesOutcome(
                series_id=series_id,
                succeeded=True,
                vintage_watermark=last_vintage,
                row_count=len(observation_rows),
            ),
            tuple(observation_rows),
            dimension,
        )

    def _select_vintages(self, series_id: str, vintages: Sequence[date]) -> tuple[date, ...]:
        if not vintages:
            return ()
        if self._config.mode is CollectionMode.PROBE:
            return (vintages[-1],)
        current = self._control_plane.latest_watermark(
            PROVIDER, watermark_dataset(series_id), watermark_stream(series_id)
        )
        if current is None or self._config.mode is CollectionMode.BACKFILL:
            return tuple(vintages)
        watermark = date.fromisoformat(current.watermark_value)
        return tuple(item for item in vintages if item > watermark)

    def _collect_vintage(self, series_id: str, vintage: date) -> tuple[dict[str, object], ...]:
        parameters = {
            "file_type": "json",
            "realtime_end": vintage.isoformat(),
            "realtime_start": vintage.isoformat(),
            "series_id": series_id,
        }
        if self._config.observation_start is not None:
            parameters["observation_start"] = self._config.observation_start.isoformat()
        probe = self._config.mode is CollectionMode.PROBE
        page_limit = self._config.probe_observation_limit if probe else OBSERVATION_PAGE_LIMIT
        normalized, _snapshot = self._fetch_json_pages(
            series_id=series_id,
            endpoint="/fred/series/observations",
            base_parameters=parameters,
            page_limit=page_limit,
            parse_items=parse_observation_records,
            allow_truncation=probe,
            process_page=lambda items, snapshot: normalize_observations(
                tuple(cast("Mapping[str, object]", item) for item in items),
                series_id=series_id,
                source_snapshot_id=snapshot.snapshot_id,
                raw_content_sha256=snapshot.content_sha256,
                retrieved_at_utc=snapshot.retrieved_at_utc,
                availability_time_utc=snapshot.retrieved_at_utc,
            ),
        )
        rows = tuple(cast("dict[str, object]", item) for item in normalized)
        assert_unique_keys(rows)
        return rows

    def _fetch_vintage_dates(self, series_id: str) -> tuple[date, ...]:
        dates, _response = self._fetch_json_pages(
            series_id=series_id,
            endpoint="/fred/series/vintagedates",
            base_parameters={"file_type": "json", "series_id": series_id},
            page_limit=VINTAGE_PAGE_LIMIT,
            parse_items=parse_vintage_dates,
        )
        return tuple(sorted({cast("date", item) for item in dates}))

    def _fetch_json_pages(  # noqa: PLR0913 - explicit page-fetch contract
        self,
        *,
        series_id: str,
        endpoint: str,
        base_parameters: Mapping[str, str],
        page_limit: int,
        parse_items: Callable[[Mapping[str, object]], Sequence[object]],
        allow_truncation: bool = False,
        process_page: Callable[[Sequence[object], SourceSnapshot], Sequence[object]] | None = None,
    ) -> tuple[tuple[object, ...], SourceSnapshot]:
        if page_limit < 1:
            raise CollectorError("page limit must be positive")
        offset = 0
        collected: list[object] = []
        last_snapshot: SourceSnapshot | None = None
        while True:
            parameters = {
                **dict(base_parameters),
                "limit": str(page_limit),
                "offset": str(offset),
            }
            response, snapshot = self._execute(
                CollectorRequest(
                    endpoint=endpoint,
                    parameters=parameters,
                    series_id=series_id,
                )
            )
            last_snapshot = snapshot
            document = parse_json_object(response.body)
            items = list(parse_items(document))
            reported_limit = optional_nonnegative_int(document, "limit") or page_limit
            reported_count = optional_nonnegative_int(document, "count")
            if allow_truncation:
                items = items[:page_limit]
            collected.extend(items if process_page is None else process_page(items, snapshot))
            if allow_truncation:
                break
            truncated = page_is_truncated(
                page_length=len(items),
                accumulated_length=offset + len(items),
                total_count=reported_count,
                page_limit=reported_limit,
            )
            if not truncated:
                break
            if not items:
                raise CollectorError(
                    f"{endpoint} page is truncated (count exceeds returned rows or len == limit)"
                )
            offset += len(items)
            if reported_count is not None and offset >= reported_count:
                break
            if offset > _MAX_PAGINATION_OFFSET:
                raise CollectorError(f"{endpoint} pagination exceeded the collector offset bound")
        if last_snapshot is None:
            raise CollectorError(f"{endpoint} produced no provider page")
        return (tuple(collected), last_snapshot)

    def _execute(self, request: CollectorRequest) -> tuple[CollectorResponse, SourceSnapshot]:
        self._limiter.before_request()
        try:
            response = self._transport(request, self._credential)
        except NormalizeError as error:
            self._limiter.after_response(byte_count=0)
            raise SeriesError(str(error)) from error
        except Exception as error:
            self._limiter.after_response(byte_count=0)
            self._requests.append(
                {
                    "source_uri": request.source_uri,
                    "request_fingerprint": request.request_fingerprint,
                    "series_id": request.series_id,
                    "disposition": "failed",
                    "error_class": type(error).__name__,
                }
            )
            if isinstance(error, (BudgetExhaustedError, CredentialLeakError, DestinationError)):
                raise
            raise SeriesError(f"{type(error).__name__}") from error
        self._limiter.after_response(byte_count=len(response.body))
        assert_credential_absent(self._credential, response.body)
        snapshot = self._capture(request, response)
        if not (_HTTP_SUCCESS_MIN <= response.status_code < _HTTP_REDIRECT_MIN):
            raise SeriesError(f"provider status {response.status_code}")
        return response, snapshot

    def _capture(self, request: CollectorRequest, response: CollectorResponse) -> SourceSnapshot:
        self._capture_seq += 1
        run_hash = hashlib.sha256(self._run_id.encode()).hexdigest()
        snapshot = SourceSnapshot(
            snapshot_id=(
                f"fred-alfred-{run_hash}-{self._capture_seq:04d}-{response.content_sha256[:12]}"
            ),
            schema_version=SCHEMA_VERSION,
            provider=PROVIDER,
            dataset=f"series:{request.series_id}",
            source_uri=request.source_uri,
            request_fingerprint=request.request_fingerprint,
            parameters=dict(request.parameters),
            requested_at_utc=response.requested_at_utc,
            retrieved_at_utc=response.retrieved_at_utc,
            content_type="application/json",
            encoding="utf-8",
            compression=None,
            raw_byte_length=len(response.body),
            content_sha256=response.content_sha256,
            parser_name="fred_alfred_json",
            parser_version="1",
            validation_status=ValidationStatus.PASS,
        )
        provenance = canonical_json_bytes(snapshot)
        assert_credential_absent(self._credential, response.body, provenance)
        path = capture_source(self._config.raw_store_root, snapshot, response.body)
        self._snapshots.append(snapshot)
        if self._capture_callback is not None:
            self._capture_callback(snapshot, path)
        self._raw_addresses.append(f"sha256:{response.content_sha256}")
        self._requests.append(
            {
                "source_uri": request.source_uri,
                "request_fingerprint": request.request_fingerprint,
                "raw_content_address": f"sha256:{response.content_sha256}",
                "status_code": response.status_code,
                "series_id": request.series_id,
                "disposition": (
                    "succeeded"
                    if _HTTP_SUCCESS_MIN <= response.status_code < _HTTP_REDIRECT_MIN
                    else "failed"
                ),
            }
        )
        return snapshot

    def _publish(
        self,
        run_id: str,
        plan: CollectionRunPlan,
        rows: Sequence[Mapping[str, object]],
        dimensions: Sequence[Mapping[str, object]],
        outcomes: Sequence[SeriesOutcome],
    ) -> tuple[Path, ...]:
        published: list[Path] = []
        root = (
            self._config.dataset_root
            / "fred_alfred"
            / DATASET_VERSION
            / hashlib.sha256(run_id.encode()).hexdigest()
        )
        by_partition: dict[tuple[str, int], list[Mapping[str, object]]] = {}
        for row in rows:
            series_id = str(row["series_id"])
            observation_date = row["observation_date"]
            if not isinstance(observation_date, date):
                raise CollectorError("normalized observation_date must be a date")
            by_partition.setdefault((series_id, observation_date.year), []).append(row)
        for (series_id, year), partition_rows in sorted(by_partition.items()):
            relative = partition_relative_path(series_id, year, part_name="part-0000.parquet")
            destination = root / relative
            payload = parquet_bytes(observations_table(partition_rows))
            _publish_immutable(destination, payload)
            published.append(destination)
        if dimensions:
            dimension_path = root / "series_dimension" / "part-0000.parquet"
            _publish_immutable(dimension_path, parquet_bytes(series_dimension_table(dimensions)))
            published.append(dimension_path)
        receipt = {
            "run_id": run_id,
            "plan_id": plan.plan_id,
            "plan_sha256": plan.plan_sha256,
            "mode": self._config.mode.value,
            "provider": PROVIDER,
            "series_ids": list(self._config.series_ids),
            "series_universe_sha256": series_universe_sha256(self._config.series_ids),
            "artifact_hashes": dict(sorted(self._config.artifact_hashes.items())),
            "raw_content_addresses": list(self._raw_addresses),
            "requests": self._requests,
            "source_snapshots": self._snapshots,
            "created_at_utc": plan.created_at_utc,
            "collected_at_utc": self._clock(),
            "observation_start": self._config.observation_start,
            "published_artifacts": {
                str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in published
            },
            "series_outcomes": [
                {
                    "series_id": item.series_id,
                    "succeeded": item.succeeded,
                    "vintage_watermark": (
                        None
                        if item.vintage_watermark is None
                        else item.vintage_watermark.isoformat()
                    ),
                    "row_count": item.row_count,
                    "error_class": item.error_class,
                }
                for item in outcomes
            ],
            "usage": {
                "calls_attempted": self._limiter.ledger().calls_attempted,
                "bytes_received": self._limiter.ledger().bytes_received,
            },
            "published_paths": [str(path) for path in published],
        }
        receipt_bytes = canonical_json_bytes(receipt)
        assert_credential_absent(self._credential, receipt_bytes)
        _publish_immutable(self._config.receipt_path, receipt_bytes)
        published.append(self._config.receipt_path)
        return tuple(published)

    def _register(self, plan: CollectionRunPlan, *, run_id: str) -> None:
        self._control_plane.register_plan(plan)
        self._control_plane.start_run(
            CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=self._clock())
        )
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_STARTED,
                occurred_at_utc=self._clock(),
                attempt_number=1,
            )
        )

    def _succeed(self, run_id: str) -> None:
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.ATTEMPT_SUCCEEDED,
                occurred_at_utc=self._clock(),
                attempt_number=1,
            )
        )
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=self._clock(),
            )
        )

    def _fail(self, run_id: str, *, error_class: str, error_message: str) -> None:
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
        self._control_plane.append_event(
            CollectionRunEvent(
                run_id=run_id,
                event_type=RunEventType.RUN_FAILED,
                occurred_at_utc=self._clock(),
                error_class=error_class,
                error_message=error_message,
            )
        )

    def _record_usage(self, run_id: str) -> None:
        for index, (metric, quantity, unit) in enumerate(
            self._limiter.ledger().as_usage_records(), start=1
        ):
            self._control_plane.record_usage(
                CollectionUsageRecord(
                    run_id=run_id,
                    usage_seq=index,
                    metric=metric,
                    quantity=Decimal(quantity),
                    unit=unit,
                    recorded_at_utc=self._clock(),
                )
            )

    def advance_successful_watermarks(
        self, run_id: str, outcomes: Sequence[SeriesOutcome]
    ) -> tuple[tuple[str, str], ...]:
        """Advance succeeded streams only after a terminal run_succeeded event."""

        state = self._control_plane.current_run_state(run_id)
        if state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            raise CollectorError("watermarks advance only after a terminal run_succeeded event")
        advanced: list[tuple[str, str]] = []
        for outcome in outcomes:
            if not outcome.succeeded or outcome.vintage_watermark is None:
                continue
            if self._config.mode is CollectionMode.PROBE:
                continue
            position = datetime.combine(outcome.vintage_watermark, datetime.min.time(), tzinfo=UTC)
            current = self._control_plane.latest_watermark(
                PROVIDER,
                watermark_dataset(outcome.series_id),
                watermark_stream(outcome.series_id),
            )
            if current is not None and position <= current.watermark_position:
                continue
            self._control_plane.advance_watermark(
                WatermarkAdvance(
                    provider=PROVIDER,
                    dataset=watermark_dataset(outcome.series_id),
                    stream=watermark_stream(outcome.series_id),
                    run_id=run_id,
                    watermark_value=outcome.vintage_watermark.isoformat(),
                    watermark_position=position,
                )
            )
            advanced.append((outcome.series_id, outcome.vintage_watermark.isoformat()))
        return tuple(advanced)


def _publish_immutable(destination: Path, content: bytes) -> None:
    try:
        publish_evidence(destination, content)
    except ValueError as error:
        raise CollectorError(str(error)) from error


__all__ = [
    "ALLOWED_HOST",
    "BASE_URL",
    "CREDENTIAL_ENVIRONMENT_VARIABLE",
    "OBSERVATION_PAGE_LIMIT",
    "PROBE_OBSERVATION_LIMIT",
    "USER_AGENT",
    "VINTAGE_PAGE_LIMIT",
    "CollectorConfig",
    "CollectorError",
    "CollectorOutcome",
    "CollectorRequest",
    "CollectorResponse",
    "ControlPlanePort",
    "CredentialLeakError",
    "DestinationError",
    "FredAlfredCollector",
    "HostPinnedRedirectHandler",
    "SeriesError",
    "SeriesOutcome",
    "Transport",
    "assert_credential_absent",
    "build_run_plan",
    "containing_git_repository",
    "live_request_url",
    "make_https_transport",
    "new_run_id",
    "optional_nonnegative_int",
    "page_is_truncated",
    "parse_observation_records",
    "parse_series_record",
    "parse_vintage_dates",
    "redact_headers",
    "validate_destination",
]
