from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory as RealTemporaryDirectory
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select

from aegis_alpha.collection.records import CollectionMode, CollectionRunEvent, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_runs,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.data import (
    fmp_collection_work,
    fmp_collector_state,
    fmp_deferred_transport,
    fmp_historical_index,
    fmp_historical_state,
    fmp_universe_work,
)
from aegis_alpha.data.fmp_approval import ApprovalExpiredError
from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_collection_work import collection_parameters
from aegis_alpha.data.fmp_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
    ControlPlanePort,
    FmpCollector,
    Transport,
    build_run_plan,
    plan_resume_record,
    publish_bundle,
)
from aegis_alpha.data.fmp_collector_run import (
    LifecycleRecoveryRequiredError,
    PostSuccessWatermarkError,
    run_collection,
)
from aegis_alpha.data.fmp_collector_state import (
    _sha256_file,
    _validate_artifact_consistency,
    resolve_resume,
)
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_historical_state import (
    existing_rows,
    known_dates,
    preload_history,
    prior_cik,
)
from aegis_alpha.data.fmp_rate_limit import RateLimiter, TierArtifact, TrustedUsageSnapshot
from aegis_alpha.data.fmp_universe_recovery import COMPLETED_NAME, REQUIRED_NAME
from aegis_alpha.data.fmp_universe_run import run_universe_build
from aegis_alpha.data.fmp_windows import (
    REVALIDATION_TRADING_DAYS,
    CollectorContractError,
    DateWindow,
    parse_universe_manifest,
)
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.sql import FromClause

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
CREDENTIAL = "SYNTH-RUN-CREDENTIAL"
EXPECTED_USAGE_RECORDS = 3
SUCCESS_TRANSPORT_CALLS = 2
MAX_RETRY_ATTEMPTS = 5
_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_SERVICE_UNAVAILABLE = 503
PROFILE_TIMEOUT_RETRY_CALLS = 3
USAGE_METRIC_COUNT = 3
UNIVERSE_ENDPOINT_CALLS = 2
UNIVERSE_PUBLICATION_COUNT = 2
PARTIAL_PUBLICATION_FAILURE_PART = 2


class ReplayTransport:
    def __init__(self, bodies: list[bytes | Exception], *, profile_cik: str = "0000000001") -> None:
        self.bodies = bodies
        self.profile_cik = profile_cik
        self.calls: list[CollectorRequest] = []

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        assert credential == CREDENTIAL
        self.calls.append(request)
        if request.endpoint == "/stable/profile":
            item: bytes | Exception = json.dumps(
                [{"symbol": (request.symbol or "").upper(), "cik": self.profile_cik}]
            ).encode()
        else:
            item = self.bodies.pop(0)
        if isinstance(item, Exception):
            raise item
        return CollectorResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            body=item,
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )


class StatusReplayTransport:
    def __init__(self, responses: list[tuple[int, bytes]]) -> None:
        self.responses = responses
        self.calls: list[CollectorRequest] = []

    def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
        assert credential == CREDENTIAL
        self.calls.append(request)
        status, body = self.responses.pop(0)
        headers = {"content-type": "application/json"}
        if status == _HTTP_TOO_MANY_REQUESTS:
            headers["Retry-After"] = "1"
        return CollectorResponse(
            status_code=status,
            headers=headers,
            body=body,
            requested_at_utc=NOW,
            retrieved_at_utc=NOW,
        )


def _manifest() -> dict[str, object]:
    return {
        "generated_at_utc": NOW.isoformat(),
        "sources": [
            {
                "endpoint": "/stable/actively-trading-list",
                "retrieved_at_utc": NOW.isoformat(),
                "raw_content_sha256": "a" * 64,
            }
        ],
        "entries": [{"symbol": "synth.a", "ipoDate": None, "delistedDate": None, "active": True}],
    }


def _collector(  # noqa: PLR0913 - test fixture exposes independent run controls
    engine: Engine,
    tmp_path: Path,
    transport: Transport,
    *,
    max_calls: int | None = 5,
    mode: CollectionMode = CollectionMode.BACKFILL,
    receipt_name: str = "run.json",
    from_date: date = date(2026, 7, 27),
    approval_check: Callable[[], None] | None = None,
    recurring: bool = False,
    clock: datetime = NOW,
    shard: tuple[int, int] | None = None,
    receipt_evidence_path: Path | None = None,
) -> FmpCollector:
    artifact_hashes = {"tier": "b" * 64, "notification": "c" * 64}
    if recurring:
        artifact_hashes["recurring_authority"] = "d" * 64
    config = CollectorConfig(
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "normalized",
        receipt_path=tmp_path / "receipts" / receipt_name,
        as_of=NOW.date(),
        mode=mode,
        max_calls=max_calls,
        run_identity="synthetic-run-driver",
        operator_from=from_date,
        artifact_hashes=artifact_hashes,
        manifest_path=(tmp_path / "historical-manifest.json" if recurring else None),
        shard=shard,
        receipt_evidence_path=receipt_evidence_path,
    )
    limiter = RateLimiter(
        tier=TierArtifact(3000, None, None),
        max_calls=max_calls,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
        run_seed=7,
        usage_baseline=TrustedUsageSnapshot(
            source="signed",
            recorded_at_utc=NOW.isoformat(),
            integrity_sha256="d" * 64,
            authority_verified=True,
            calls_used_today=0,
            bytes_used_30d=0,
        ),
    )
    return FmpCollector(
        config=config,
        transport=transport,
        control_plane=CollectionRegistry(engine),
        limiter=limiter,
        credential=CREDENTIAL,
        clock=lambda: clock,
        approval_check=approval_check,
    )


def _count(engine: Engine, table: FromClause) -> int:
    with engine.connect() as connection:
        return int(connection.scalar(select(func.count()).select_from(table)) or 0)


@pytest.mark.parametrize("failure", [TimeoutError("synthetic"), OSError("synthetic")])
def test_universe_timeout_and_network_retry_ceiling_still_fail_normally(
    clean_postgres: Engine,
    tmp_path: Path,
    failure: Exception,
) -> None:
    transport = ReplayTransport([failure for _ in range(MAX_RETRY_ATTEMPTS)])
    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, transport, max_calls=MAX_RETRY_ATTEMPTS),
        generated_at_utc=NOW,
        destination=tmp_path / "manifests" / "failed.json",
        run_id="fmp-run-universe-ordinary-failure",
    )

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert len(transport.calls) == MAX_RETRY_ATTEMPTS
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type).order_by(
                    collection_run_events.c.event_seq
                )
            ).scalars()
        )
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == outcome.run_id,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert events == ["attempt_started", "attempt_failed", "run_failed"]
    assert calls == MAX_RETRY_ATTEMPTS
    assert outcome.artifact_path is None


def test_run_collection_persists_publication_usage_success_and_watermark(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
            }
        ]
    ).encode()
    transport = ReplayTransport([body])
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, transport),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-driver-success",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert len(transport.calls) == SUCCESS_TRANSPORT_CALLS
    assert _count(clean_postgres, collection_run_plans) == 1
    assert _count(clean_postgres, collection_runs) == 1
    assert _count(clean_postgres, collection_usage_records) == EXPECTED_USAGE_RECORDS
    assert _count(clean_postgres, collection_watermarks) == 1
    events = []
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type).order_by(
                    collection_run_events.c.event_seq
                )
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]
    receipt = json.loads((tmp_path / "receipts" / "run.json").read_bytes())
    assert receipt["usage"]["calls_attempted"] == SUCCESS_TRANSPORT_CALLS
    assert [request.endpoint for request in transport.calls] == [
        "/stable/profile",
        "/stable/historical-price-eod/full",
    ]
    assert "observed_coverage_start" in {result["kind"] for result in receipt["quality_results"]}
    parquet_paths = [path for path in outcome.published_paths if path.suffix == ".parquet"]
    assert {path.parts[-2] for path in parquet_paths} == {
        "fmp_price_eod_full",
        "fmp_profile",
    }
    price_path = next(path for path in parquet_paths if "fmp_price_eod_full" in path.parts)
    assert pq.read_table(price_path).num_rows == 1


def test_sharded_collection_outcome_points_at_per_shard_receipt(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
            }
        ]
    ).encode()
    transport = ReplayTransport([body])
    day_receipt = tmp_path / "receipts" / "collection-receipt.json"
    shard_receipt = tmp_path / "receipts" / "collection-receipt-shard2of2.json"
    collector = _collector(
        clean_postgres,
        tmp_path,
        transport,
        shard=(2, 2),
        receipt_evidence_path=shard_receipt,
        receipt_name="collection-receipt.json",
    )
    assert collector.config.receipt_path == day_receipt
    outcome = run_collection(
        collector,
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-driver-shard",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.receipt_path == shard_receipt
    assert shard_receipt.is_file()
    assert not day_receipt.is_file()


def test_planned_dataset_selection_collects_all_six_without_fabricating_gap_watermarks(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    fixtures = Path(__file__).resolve().parents[1] / "fixtures/provider_neutral/fmp_collector"
    payloads = {
        "/stable/historical-price-eod/full": (fixtures / "price_eod_full.json").read_bytes(),
        "/stable/historical-price-eod/non-split-adjusted": (
            fixtures / "price_eod_non_split_adjusted.json"
        ).read_bytes(),
        "/stable/historical-price-eod/dividend-adjusted": (
            fixtures / "price_eod_dividend_adjusted.json"
        ).read_bytes(),
        "/stable/splits": b"[]",
        "/stable/dividends": (fixtures / "dividends.json").read_bytes(),
    }

    class PlannedTransport(ReplayTransport):
        def __call__(self, request: CollectorRequest, credential: str) -> CollectorResponse:
            if request.endpoint == "/stable/profile":
                return super().__call__(request, credential)
            self.calls.append(request)
            return CollectorResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=payloads[request.endpoint],
                requested_at_utc=NOW,
                retrieved_at_utc=NOW,
            )

    selections = (
        DatasetSelection.PRICE_EOD_FULL,
        DatasetSelection.PRICE_EOD_NON_SPLIT_ADJUSTED,
        DatasetSelection.PRICE_EOD_DIVIDEND_ADJUSTED,
        DatasetSelection.SPLITS,
        DatasetSelection.DIVIDENDS,
        DatasetSelection.PROFILE,
    )
    published: set[str] = set()
    watermarked: set[str] = set()
    endpoints: set[str] = set()
    split_outcome = None
    for selection in selections:
        transport = PlannedTransport([])
        outcome = run_collection(
            _collector(
                clean_postgres,
                tmp_path / selection.value,
                transport,
                max_calls=1 if selection is DatasetSelection.PROFILE else 2,
            ),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="e" * 64,
            created_at_utc=NOW,
            run_id=f"fmp-run-{selection.value}",
            dataset_selection=selection,
        )
        assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
        endpoints.update(request.endpoint for request in transport.calls)
        published.update(
            path.parts[-2] for path in outcome.published_paths if path.suffix == ".parquet"
        )
        watermarked.update(dataset for dataset, _stream, _value in outcome.watermarks_advanced)
        if selection is DatasetSelection.SPLITS:
            split_outcome = outcome

    with clean_postgres.begin() as connection:
        connection.execute(collection_watermarks.delete())
    all_outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path / DatasetSelection.ALL.value,
            PlannedTransport([]),
            max_calls=len(DatasetSelection.ALL.datasets),
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="f" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-all-datasets",
        dataset_selection=DatasetSelection.ALL,
    )
    with clean_postgres.connect() as connection:
        all_errors = connection.execute(
            select(collection_run_events.c.error_class, collection_run_events.c.error_message)
            .where(collection_run_events.c.run_id == "fmp-run-all-datasets")
            .order_by(collection_run_events.c.event_seq)
        ).all()
    assert all_outcome.terminal_event is RunEventType.RUN_SUCCEEDED, all_errors
    assert {dataset for dataset, _stream, _value in all_outcome.watermarks_advanced} == {
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_dividends",
    }

    assert endpoints == {
        "/stable/profile",
        "/stable/historical-price-eod/full",
        "/stable/historical-price-eod/non-split-adjusted",
        "/stable/historical-price-eod/dividend-adjusted",
        "/stable/splits",
        "/stable/dividends",
    }
    assert published == {
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_dividends",
        "fmp_profile",
    }
    assert watermarked == {
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_dividends",
    }
    assert split_outcome is not None
    assert any(
        result.kind.value == "coverage_gap" and result.dataset == "fmp_splits"
        for result in split_outcome.quality_results
    )


def test_daily_selection_collects_all_six_datasets_in_one_run() -> None:
    assert DatasetSelection.ALL.datasets == (
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_splits",
        "fmp_dividends",
        "fmp_profile",
    )
    assert DatasetSelection.ALL.plan_dataset == "fmp_daily_all"


def test_build_universe_walks_both_sources_and_hashes_every_raw_page(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    active = b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]'
    delisted = b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]'
    transport = ReplayTransport([active, delisted])
    destination = tmp_path / "manifests" / "universe.json"

    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, transport, max_calls=None),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-success",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.receipt_path is None
    assert outcome.artifact_path == destination
    assert len(transport.calls) == UNIVERSE_ENDPOINT_CALLS
    document = json.loads(destination.read_bytes())
    manifest = parse_universe_manifest(document)
    assert manifest.symbols() == ("synth.active", "synth.old")
    assert document["sources"][0]["raw_content_sha256"] != hashlib.sha256(active).hexdigest()
    assert len(list((tmp_path / "raw" / "fmp" / "blobs").rglob("*.raw"))) == UNIVERSE_ENDPOINT_CALLS
    assert (
        len(list((tmp_path / "raw" / "fmp" / "provenance").rglob("*.json")))
        == UNIVERSE_ENDPOINT_CALLS
    )


def test_existing_rows_ignore_successful_universe_publication_marker(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    collector = _collector(
        clean_postgres,
        tmp_path,
        ReplayTransport(
            [
                b'[{"symbol":"synth.active"}]',
                b'[{"symbol":"synth.old"}]',
            ]
        ),
    )
    run_universe_build(
        collector,
        generated_at_utc=NOW,
        destination=(
            collector.config.dataset_root / "normalized" / "fmp" / "universe" / "universe.json"
        ),
        run_id="fmp-run-universe-before-history",
    )

    assert (
        existing_rows(
            _collector(clean_postgres, tmp_path, ReplayTransport([])),
            "fmp_price_eod_full",
        )
        == ()
    )


@pytest.mark.parametrize(
    ("retry_status", "rate_limited"),
    [(_HTTP_TOO_MANY_REQUESTS, 1), (_HTTP_SERVICE_UNAVAILABLE, 0)],
    ids=("retry-429-then-200", "retry-503-then-200"),
)
def test_universe_retry_then_success_composes_and_keeps_sanitized_attempts(
    clean_postgres: Engine,
    tmp_path: Path,
    retry_status: int,
    rate_limited: int,
) -> None:
    active = b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]'
    delisted = b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]'
    retry_body = b'{"Error Message":"temporary"}'
    transport = StatusReplayTransport(
        [
            (retry_status, retry_body),
            (200, active),
            (200, delisted),
        ]
    )
    destination = tmp_path / "manifests" / f"universe-{retry_status}.json"

    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, transport, max_calls=5),
        generated_at_utc=NOW,
        destination=destination,
        run_id=f"fmp-run-universe-retry-{retry_status}",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert [request.endpoint for request in transport.calls] == [
        "/stable/actively-trading-list",
        "/stable/actively-trading-list",
        "/stable/delisted-companies",
    ]
    document = json.loads(destination.read_bytes())
    manifest = parse_universe_manifest(document)
    assert manifest.symbols() == ("synth.active", "synth.old")
    provenance_files = list((tmp_path / "raw" / "fmp" / "provenance").rglob("*.json"))
    blob_files = list((tmp_path / "raw" / "fmp" / "blobs").rglob("*.raw"))
    attempt_files = list(
        (tmp_path / "raw" / "fmp" / "runs" / outcome.run_id / "attempts").glob("*.json")
    )
    expected_attempts = 3
    assert len(provenance_files) == expected_attempts
    assert len(attempt_files) == expected_attempts
    statuses = sorted(json.loads(path.read_bytes())["status_code"] for path in provenance_files)
    assert statuses == [200, 200, retry_status]
    with clean_postgres.connect() as connection:
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == outcome.run_id,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
        limited = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == outcome.run_id,
                collection_usage_records.c.metric == "rate_limited_attempts",
            )
        )
    assert calls == expected_attempts
    assert limited == rate_limited
    for path in (*provenance_files, *blob_files, *attempt_files, destination):
        assert CREDENTIAL.encode() not in path.read_bytes()
        assert CREDENTIAL not in path.read_text(encoding="utf-8", errors="ignore")


def test_universe_429_then_503_retries_compose_one_logical_page_each(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    active = b'[{"symbol":"synth.active","ipoDate":"2020-01-02"}]'
    delisted = b'[{"symbol":"synth.old","delistedDate":"2021-03-04"}]'
    retry_body = b'{"Error Message":"temporary"}'
    transport = StatusReplayTransport(
        [
            (_HTTP_TOO_MANY_REQUESTS, retry_body),
            (200, active),
            (_HTTP_SERVICE_UNAVAILABLE, retry_body),
            (200, delisted),
        ]
    )
    destination = tmp_path / "manifests" / "universe-429-503.json"

    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, transport, max_calls=5),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-retry-429-503",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert parse_universe_manifest(json.loads(destination.read_bytes())).symbols() == (
        "synth.active",
        "synth.old",
    )
    expected_attempts = 4
    assert len(transport.calls) == expected_attempts
    provenance = [
        json.loads(path.read_bytes())
        for path in (tmp_path / "raw" / "fmp" / "provenance").rglob("*.json")
    ]
    assert sorted(item["status_code"] for item in provenance) == [
        200,
        200,
        _HTTP_TOO_MANY_REQUESTS,
        _HTTP_SERVICE_UNAVAILABLE,
    ]
    for item in provenance:
        assert CREDENTIAL not in json.dumps(item)


def test_marker_only_universe_finalization_needs_no_new_provider_approval(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "manifest" / "expiry-universe.json"
    first = ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]'])
    original = FmpCollector.record_usage
    run_id = "fmp-run-universe-expiry-marker"

    class SimulatedCrash(BaseException):
        pass

    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedCrash()),
    )
    with pytest.raises(SimulatedCrash):
        run_universe_build(
            _collector(clean_postgres, tmp_path, first),
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )
    monkeypatch.setattr(FmpCollector, "record_usage", original)
    artifact = destination.read_bytes()

    def expire() -> None:
        raise ApprovalExpiredError("no new provider authority may be consumed")

    replay = ReplayTransport([])
    recovery_time = NOW + timedelta(days=1)
    recovered = run_universe_build(
        _collector(
            clean_postgres,
            tmp_path,
            replay,
            approval_check=expire,
            clock=recovery_time,
        ),
        generated_at_utc=recovery_time,
        destination=destination,
        run_id="unused-universe-expiry",
    )
    assert replay.calls == []
    assert recovered.run_id == run_id
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert destination.read_bytes() == artifact
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        usage_rows = connection.scalar(
            select(func.count())
            .select_from(collection_usage_records)
            .where(collection_usage_records.c.run_id == run_id)
        )
        usage_times = set(
            connection.execute(
                select(collection_usage_records.c.recorded_at_utc).where(
                    collection_usage_records.c.run_id == run_id
                )
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]
    assert usage_rows == EXPECTED_USAGE_RECORDS
    assert usage_times == {recovery_time}


def test_universe_manifest_does_not_overwrite_existing_output(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "manifests" / "universe.json"
    destination.parent.mkdir(parents=True)
    sentinel = b'{"existing":"operator-owned"}'
    destination.write_bytes(sentinel)

    with pytest.raises(LifecycleRecoveryRequiredError, match="durable universe boundary"):
        run_universe_build(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']),
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fmp-run-universe-no-overwrite",
        )

    assert destination.read_bytes() == sentinel


def test_universe_manifest_before_marker_interruption_resumes_without_rewrite(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "manifest" / "between-manifest-and-marker.json"
    transport = ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]'])
    original = fmp_universe_work.publish_bundle

    injected = False

    def publish_then_interrupt(
        publications: Sequence[tuple[Path, bytes]],
    ) -> tuple[Path, ...]:
        nonlocal injected
        items = list(publications)
        if len(items) == UNIVERSE_PUBLICATION_COUNT and items[0][0] == destination and not injected:
            injected = True
            original([items[0]])
            raise RuntimeError("synthetic manifest-before-marker interruption")
        return original(items)

    monkeypatch.setattr(fmp_universe_work, "publish_bundle", publish_then_interrupt)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_universe_build(
            _collector(clean_postgres, tmp_path, transport),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fmp-run-universe-manifest-only",
        )
    before = destination.stat()
    payload = destination.read_bytes()
    monkeypatch.setattr(fmp_universe_work, "publish_bundle", original)
    replay = ReplayTransport([])
    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, replay),
        generated_at_utc=NOW,
        destination=destination,
        run_id="unused-universe-run",
    )
    after = destination.stat()
    assert outcome.run_id == "fmp-run-universe-manifest-only"
    assert replay.calls == []
    assert destination.read_bytes() == payload
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def test_completed_universe_run_does_not_capture_fresh_candidate(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "manifest" / "fresh-logical-run.json"
    bodies = [b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']
    first = run_universe_build(
        _collector(clean_postgres, tmp_path, ReplayTransport(list(bodies))),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-completed-first",
    )
    fresh_transport = ReplayTransport(list(bodies))
    second = run_universe_build(
        _collector(clean_postgres, tmp_path, fresh_transport),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-fresh-second",
    )
    assert first.run_id == "fmp-run-universe-completed-first"
    assert second.run_id == "fmp-run-universe-fresh-second"
    assert len(fresh_transport.calls) == UNIVERSE_ENDPOINT_CALLS


def test_universe_interruption_resumes_same_run_and_truthful_artifact(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "manifest" / "universe.json"
    first = ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]'])
    original = FmpCollector.record_usage

    class SimulatedCrash(BaseException):
        pass

    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedCrash()),
    )
    with pytest.raises(SimulatedCrash):
        run_universe_build(
            _collector(clean_postgres, tmp_path, first),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fmp-run-universe-interrupted",
        )
    monkeypatch.setattr(FmpCollector, "record_usage", original)
    replay = ReplayTransport([])
    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, replay),
        generated_at_utc=NOW,
        destination=destination,
        run_id="fmp-run-universe-not-used",
    )
    assert outcome.run_id == "fmp-run-universe-interrupted"
    assert outcome.artifact_path == destination
    assert outcome.receipt_path is None
    assert replay.calls == []


@pytest.mark.parametrize("stage", ["record_usage", "succeed"])
def test_universe_post_publication_fault_requires_zero_call_same_run_recovery(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    destination = tmp_path / "manifest" / "recoverable.json"
    first = ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]'])
    original = getattr(FmpCollector, stage)

    def fail_after_marker(self: FmpCollector, *args: object, **kwargs: object) -> object:
        resolved_run = str(kwargs["run_id"])
        marker = self.config.raw_store_root / "fmp" / "runs" / resolved_run / "publication.json"
        if marker.is_file():
            raise RuntimeError("synthetic")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FmpCollector, stage, fail_after_marker)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_universe_build(
            _collector(clean_postgres, tmp_path, first),
            generated_at_utc=NOW,
            destination=destination,
            run_id=f"fmp-run-universe-{stage}",
        )
    artifact = destination.read_bytes()
    monkeypatch.setattr(FmpCollector, stage, original)
    replay = ReplayTransport([])
    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, replay),
        generated_at_utc=NOW,
        destination=destination,
        run_id="unused-universe-run",
    )
    assert outcome.run_id == f"fmp-run-universe-{stage}"
    assert replay.calls == []
    assert destination.read_bytes() == artifact


@pytest.mark.parametrize(
    "injected_event", [RunEventType.ATTEMPT_SUCCEEDED, RunEventType.RUN_SUCCEEDED]
)
def test_universe_success_commit_fault_recovers_legally(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    injected_event: RunEventType,
) -> None:
    destination = tmp_path / "manifest" / "attempt-succeeded.json"
    original = CollectionRegistry.append_event
    injected = False

    def append_then_interrupt(self: CollectionRegistry, event: CollectionRunEvent) -> None:
        nonlocal injected
        original(self, event)
        if event.event_type is injected_event and not injected:
            injected = True
            raise RuntimeError("synthetic post-commit interruption")

    monkeypatch.setattr(CollectionRegistry, "append_event", append_then_interrupt)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_universe_build(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']),
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id=f"fmp-run-universe-{injected_event.value}",
        )
    monkeypatch.setattr(CollectionRegistry, "append_event", original)
    replay = ReplayTransport([])
    outcome = run_universe_build(
        _collector(clean_postgres, tmp_path, replay),
        generated_at_utc=NOW,
        destination=destination,
        run_id="unused-universe-run",
    )
    assert outcome.run_id == f"fmp-run-universe-{injected_event.value}"
    assert replay.calls == []
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == outcome.run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]


def test_completed_universe_run_replays_with_a_later_invocation_clock(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "manifest" / "daily.json"
    run_id = "fmp-run-daily-universe-replay"
    first = run_universe_build(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']),
        ),
        generated_at_utc=NOW,
        destination=destination,
        run_id=run_id,
        approved_run_identity=run_id,
    )
    replay = ReplayTransport([])

    second = run_universe_build(
        _collector(clean_postgres, tmp_path, replay),
        generated_at_utc=NOW + timedelta(hours=1),
        destination=destination,
        run_id=run_id,
        approved_run_identity=run_id,
    )

    assert first.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second.run_id == run_id
    assert second.plan_id == first.plan_id
    assert replay.calls == []


def test_completed_collection_run_replays_with_a_later_invocation_clock(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures/provider_neutral/fmp_collector/price_eod_full.json"
    ).read_bytes()
    run_id = "fmp-run-daily-collection-replay"
    first = run_collection(
        _collector(clean_postgres, tmp_path, ReplayTransport([fixture]), max_calls=2),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id=run_id,
        approved_run_identity=run_id,
    )
    replay = ReplayTransport([])

    second = run_collection(
        _collector(clean_postgres, tmp_path, replay, max_calls=2),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW + timedelta(hours=1),
        run_id=run_id,
        approved_run_identity=run_id,
    )

    assert first.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second.run_id == run_id
    assert second.plan_id == first.plan_id
    assert replay.calls == []


def test_historical_rows_are_indexed_once_per_dataset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    rows = (
        {
            "symbol": "SYNTH.A",
            "date": date(2026, 7, 28),
            "cik": "0000000001",
            "retrieved_at_utc": NOW.isoformat(),
        },
        {
            "symbol": "SYNTH.B",
            "date": date(2026, 7, 29),
            "cik": "0000000002",
            "retrieved_at_utc": NOW.isoformat(),
        },
    )

    def load(_collector: FmpCollector, dataset: str) -> tuple[Mapping[str, object], ...]:
        calls.append(dataset)
        return rows

    monkeypatch.setattr(fmp_historical_state, "_projected_rows", load)
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_index_cache={},
                config=SimpleNamespace(raw_store_root=tmp_path),
            ),
        ),
    )

    assert known_dates(collector, "fmp_price_eod_full", "synth.a") == (date(2026, 7, 28),)
    assert known_dates(collector, "fmp_price_eod_full", "synth.b") == (date(2026, 7, 29),)
    assert prior_cik(collector, "fmp_profile", "synth.a") == "0000000001"
    assert prior_cik(collector, "fmp_profile", "synth.b") == "0000000002"
    assert calls == ["fmp_price_eod_full", "fmp_profile"]


def test_historical_index_retains_only_bounded_symbol_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dates = tuple(date(2026, 1, day) for day in range(1, 21))

    def load(
        _collector: FmpCollector,
        dataset: str,
    ) -> tuple[Mapping[str, object], ...]:
        if dataset == "fmp_profile":
            return tuple(
                {
                    "symbol": "SYNTH.A",
                    "cik": f"{index:010d}",
                    "retrieved_at_utc": datetime(2026, 1, index, tzinfo=UTC),
                }
                for index in range(1, 21)
            )
        return tuple({"symbol": "SYNTH.A", "date": value} for value in dates)

    monkeypatch.setattr(fmp_historical_state, "_projected_rows", load)
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_index_cache={},
                config=SimpleNamespace(raw_store_root=tmp_path),
            ),
        ),
    )

    assert (
        known_dates(collector, "fmp_price_eod_full", "synth.a")
        == dates[-(REVALIDATION_TRADING_DAYS + 1) :]
    )
    assert prior_cik(collector, "fmp_profile", "synth.a") == "0000000020"
    assert (
        sum(
            len(rows)
            for index in collector._historical_index_cache.values()  # noqa: SLF001
            for rows in index.values()
        )
        == REVALIDATION_TRADING_DAYS + 2
    )


@pytest.mark.parametrize(
    ("dataset", "prior_row", "new_row"),
    [
        (
            "fmp_price_eod_full",
            {"date": date(2026, 7, 28)},
            {"symbol": "SYNTH.B", "date": date(2026, 7, 29)},
        ),
        (
            "fmp_profile",
            {"cik": "0000000001", "retrieved_at_utc": NOW},
            {
                "symbol": "SYNTH.B",
                "cik": "0000000002",
                "retrieved_at_utc": NOW,
            },
        ),
    ],
)
def test_historical_index_merge_preserves_cached_symbol_keys(
    dataset: str,
    prior_row: Mapping[str, object],
    new_row: Mapping[str, object],
) -> None:
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_index_cache={
                    dataset: {"synth.a": (prior_row,)},
                }
            ),
        ),
    )

    fmp_historical_index.update_historical_index(
        collector,
        dataset,
        (new_row,),
    )

    assert set(collector._historical_index_cache[dataset]) == {  # noqa: SLF001
        "synth.a",
        "synth.b",
    }


@pytest.mark.parametrize("dataset", ["fmp_price_eod_full", "fmp_profile"])
def test_historical_index_update_preserves_untouched_symbol_tuple(
    dataset: str,
) -> None:
    untouched = (
        {"cik": "0000000001", "retrieved_at_utc": NOW}
        if dataset == "fmp_profile"
        else {"date": date(2026, 7, 28)},
    )
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_index_cache={
                    dataset: {
                        "synth.untouched": untouched,
                    }
                }
            ),
        ),
    )
    incoming = (
        {
            "symbol": "SYNTH.NEW",
            "cik": "0000000002",
            "retrieved_at_utc": NOW,
        }
        if dataset == "fmp_profile"
        else {"symbol": "SYNTH.NEW", "date": date(2026, 7, 29)}
    )

    fmp_historical_index.update_historical_index(collector, dataset, (incoming,))

    assert (
        collector._historical_index_cache[dataset]["synth.untouched"]  # noqa: SLF001
        is untouched
    )


def test_latest_cik_index_orders_mixed_utc_timestamp_representations() -> None:
    latest = fmp_historical_index.latest_cik_index(
        iter(
            (
                {
                    "symbol": "SYNTH.A",
                    "cik": None,
                    "retrieved_at_utc": "2026-08-22T09:00:00Z",
                },
                {
                    "symbol": "SYNTH.A",
                    "cik": "0000000002",
                    "retrieved_at_utc": datetime(2026, 8, 22, 14, 0, tzinfo=UTC),
                },
            )
        )
    )

    assert latest["synth.a"][0]["cik"] == "0000000002"


def test_latest_cik_index_preserves_newest_nonnull_identity() -> None:
    latest = fmp_historical_index.latest_cik_index(
        iter(
            (
                {
                    "symbol": "SYNTH.A",
                    "cik": "0000000002",
                    "retrieved_at_utc": "2026-08-22T09:00:00Z",
                },
                {
                    "symbol": "SYNTH.A",
                    "cik": None,
                    "retrieved_at_utc": "2026-08-22T14:00:00Z",
                },
            )
        )
    )

    assert latest["synth.a"][0] == {
        "cik": "0000000002",
        "retrieved_at_utc": "2026-08-22T09:00:00Z",
    }


def test_history_index_pointers_are_independent_per_dataset(
    tmp_path: Path,
) -> None:
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_index_cache={
                    "fmp_price_eod_full": {},
                    "fmp_profile": {},
                },
                config=SimpleNamespace(raw_store_root=tmp_path),
            ),
        ),
    )

    all_pointers = {
        path
        for path, _payload in fmp_historical_index.history_index_pointer_publications(
            collector,
            "fmp-run-all",
            "a" * 64,
        )
    }
    collector._historical_index_cache = {"fmp_profile": {}}  # noqa: SLF001
    profile_pointers = {
        path
        for path, _payload in fmp_historical_index.history_index_pointer_publications(
            collector,
            "fmp-run-profile",
            "b" * 64,
        )
    }

    assert {path.name for path in all_pointers} == {
        "latest-history-index-fmp_price_eod_full.json",
        "latest-history-index-fmp_profile.json",
    }
    assert {path.name for path in profile_pointers} == {"latest-history-index-fmp_profile.json"}
    assert all_pointers - profile_pointers


def test_sequential_shard_finalize_retains_prior_history_index_entries(tmp_path: Path) -> None:
    dataset = "fmp_price_eod_full"
    raw_root = tmp_path / "raw"
    dataset_root = tmp_path / "normalized"
    pointer = raw_root / "fmp" / f"latest-history-index-{dataset}.json"

    def _index_bytes(symbols: dict[str, list[str]]) -> bytes:
        return canonical_json_bytes({"dataset": dataset, "schema_version": 1, "symbols": symbols})

    def _write_run(run_id: str, symbols: dict[str, list[str]], marker_sha256: str) -> bytes:
        index_path = (
            dataset_root
            / "normalized"
            / "fmp"
            / ".runs"
            / f"run_id={run_id}"
            / dataset
            / "history-index.json"
        )
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_bytes(_index_bytes(symbols))
        marker_path = raw_root / "fmp" / "runs" / run_id / "publication.json"
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_bytes(canonical_json_bytes({"run_id": run_id}))
        return canonical_json_bytes(
            {"dataset": dataset, "marker_sha256": marker_sha256, "run_id": run_id}
        )

    first_pointer = _write_run("fmp-run-shard-1", {"aaa": ["2026-07-28"]}, "a" * 64)
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                config=SimpleNamespace(raw_store_root=raw_root, dataset_root=dataset_root)
            ),
        ),
    )
    fmp_collection_work._replace_history_index_pointer(collector, pointer, first_pointer)  # noqa: SLF001
    second_pointer = _write_run("fmp-run-shard-2", {"bbb": ["2026-07-28"]}, "b" * 64)
    fmp_collection_work._replace_history_index_pointer(collector, pointer, second_pointer)  # noqa: SLF001
    document = json.loads(pointer.read_bytes())
    retained_run = document["run_id"]
    index_path = (
        dataset_root
        / "normalized"
        / "fmp"
        / ".runs"
        / f"run_id={retained_run}"
        / dataset
        / "history-index.json"
    )
    symbols = json.loads(index_path.read_bytes())["symbols"]
    assert "aaa" in symbols
    assert "bbb" in symbols


@pytest.mark.parametrize(
    ("dataset", "expected_columns", "source_row"),
    [
        (
            "fmp_price_eod_full",
            ("symbol", "date"),
            {
                "symbol": "SYNTH.A",
                "date": date(2026, 7, 28),
                "close": 123.45,
                "raw_content_sha256": "a" * 64,
            },
        ),
        (
            "fmp_profile",
            ("symbol", "cik", "retrieved_at_utc"),
            {
                "symbol": "SYNTH.A",
                "cik": "0000000001",
                "retrieved_at_utc": NOW,
                "companyName": "payload that must not remain resident",
            },
        ),
    ],
)
def test_existing_rows_read_only_minimal_historical_projections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dataset: str,
    expected_columns: tuple[str, ...],
    source_row: dict[str, object],
) -> None:
    artifact = (
        tmp_path / "normalized" / "fmp" / dataset / "run_id=fmp-run-history" / "part-00000.parquet"
    )
    artifact.parent.mkdir(parents=True)
    artifact.touch()
    observed_columns: list[tuple[str, ...]] = []

    class FakeParquetFile:
        def __init__(self, _path: Path) -> None:
            pass

        def iter_batches(
            self,
            *,
            batch_size: int,
            columns: Sequence[str],
        ) -> Iterator[pa.RecordBatch]:
            assert batch_size > 0
            observed_columns.append(tuple(columns))
            yield from pa.Table.from_pylist(
                [{column: source_row[column] for column in columns}]
            ).to_batches()

    monkeypatch.setattr(fmp_historical_state.pq, "ParquetFile", FakeParquetFile)
    monkeypatch.setattr(
        fmp_historical_state,
        "_successful_run_parts",
        lambda _collector, _dataset: (artifact,),
    )
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _control_plane=SimpleNamespace(
                    current_run_state=lambda _run_id: SimpleNamespace(
                        state=RunEventType.RUN_SUCCEEDED,
                        plan_id="plan-history",
                    )
                ),
                _existing_rows_cache={},
                config=SimpleNamespace(dataset_root=tmp_path),
            ),
        ),
    )

    rows = existing_rows(collector, dataset)

    assert observed_columns == [expected_columns]
    assert len(rows) == 1
    assert tuple(rows[0]) == expected_columns


def test_historical_parts_resolve_run_state_once_for_all_declared_parts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "fmp-run-multipart-history"
    dataset = "fmp_price_eod_full"
    marker = tmp_path / "raw" / "fmp" / "runs" / run_id / "publication.json"
    marker.parent.mkdir(parents=True)
    root = tmp_path / "normalized" / "fmp" / ".runs" / f"run_id={run_id}" / dataset
    parts = tuple(root / f"part-{index:05d}.parquet" for index in range(3))
    legacy = tmp_path / "normalized" / "fmp" / dataset / f"run_id={run_id}" / "part-00003.parquet"
    declared = (*parts, legacy)
    marker.write_bytes(
        canonical_json_bytes(
            {"artifacts": [{"path": str(path), "sha256": "a" * 64} for path in declared]}
        )
    )
    state_calls: list[str] = []

    def current_run_state(value: str) -> SimpleNamespace:
        state_calls.append(value)
        return SimpleNamespace(state=RunEventType.RUN_SUCCEEDED, plan_id="plan-history")

    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _control_plane=SimpleNamespace(
                    current_run_state=current_run_state,
                    run_plan_dataset=lambda _run_id: dataset,
                ),
                config=SimpleNamespace(
                    raw_store_root=tmp_path / "raw",
                    dataset_root=tmp_path,
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        fmp_historical_state,
        "_validate_complete_artifact_inventory",
        lambda *_arguments: {str(path): "a" * 64 for path in declared},
    )

    assert fmp_historical_state._successful_run_parts(collector, dataset) == tuple(  # noqa: SLF001
        sorted(declared)
    )
    assert state_calls == [run_id]


def test_preload_history_reuses_latest_bounded_snapshot(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures/provider_neutral/fmp_collector/price_eod_full.json"
    ).read_bytes()
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, ReplayTransport([fixture])),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="1" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-bounded-history-snapshot",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    collector = _collector(clean_postgres, tmp_path, ReplayTransport([]))
    monkeypatch.setattr(
        fmp_historical_state.pq,
        "ParquetFile",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("validated history snapshot must avoid Parquet rescans")
        ),
    )

    preload_history(
        collector,
        ("fmp_price_eod_full", "fmp_profile"),
    )

    assert known_dates(collector, "fmp_price_eod_full", "synth.a")
    assert prior_cik(collector, "fmp_profile", "synth.a") == "0000000001"


@pytest.mark.parametrize(
    "mutation",
    [
        "truncated",
        "duplicate_key",
        "unknown_field",
        "wrong_run",
        "wrong_plan",
        "wrong_phase",
        "wrong_artifact",
        "wrong_marker",
        "stale_lifecycle",
        "wrong_type",
    ],
)
def test_universe_required_token_tamper_matrix_fails_closed(  # noqa: C901
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    destination = tmp_path / "manifest" / "token-tamper.json"
    run_id = "fmp-run-universe-token-tamper"
    original = CollectionRegistry.append_event

    def interrupt_success(self: CollectionRegistry, event: CollectionRunEvent) -> None:
        original(self, event)
        if event.event_type is RunEventType.RUN_SUCCEEDED:
            raise RuntimeError("synthetic committed-success interruption")

    monkeypatch.setattr(CollectionRegistry, "append_event", interrupt_success)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_universe_build(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']),
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )
    monkeypatch.setattr(CollectionRegistry, "append_event", original)
    token = tmp_path / "raw" / "fmp" / "runs" / run_id / "recovery-required.json"
    envelope = json.loads(token.read_bytes())
    assert isinstance(envelope, dict)
    payload = cast("dict[str, object]", envelope["payload"])
    if mutation == "truncated":
        replacement = b'{"domain":'
    elif mutation == "duplicate_key":
        replacement = b'{"domain":"x","domain":"y","payload":{},"payload_sha256":"z"}'
    else:
        if mutation == "unknown_field":
            envelope["unknown"] = True
        elif mutation == "wrong_run":
            payload["run_id"] = "another-run"
        elif mutation == "wrong_plan":
            payload["plan_id"] = "another-plan"
        elif mutation == "wrong_phase":
            payload["manifest_phase"] = "pre_marker"
        elif mutation == "wrong_artifact":
            payload["artifact_path"] = str(tmp_path / "other.json")
        elif mutation == "wrong_marker":
            payload["publication_marker_sha256"] = "0" * 64
        elif mutation == "stale_lifecycle":
            payload["expected_lifecycle"] = RunEventType.ATTEMPT_STARTED.value
        elif mutation == "wrong_type":
            payload["schema_version"] = "1"
        if mutation != "unknown_field":
            envelope["payload_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        replacement = canonical_json_bytes(envelope)
    token.write_bytes(replacement)
    replay = ReplayTransport([])
    with pytest.raises(ValueError, match="recovery token"):
        run_universe_build(
            _collector(clean_postgres, tmp_path, replay),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fresh-candidate-must-not-be-hijacked",
        )
    assert replay.calls == []
    assert not (token.parent / "recovery-completed.json").exists()


def test_malformed_completed_recovery_token_fails_closed_without_fresh_hijack(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "manifest" / "completed-token-tamper.json"
    original = CollectionRegistry.append_event

    def interrupt_success(self: CollectionRegistry, event: CollectionRunEvent) -> None:
        original(self, event)
        if event.event_type is RunEventType.RUN_SUCCEEDED:
            raise RuntimeError("synthetic committed-success interruption")

    monkeypatch.setattr(CollectionRegistry, "append_event", interrupt_success)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_universe_build(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']),
            ),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fmp-run-universe-completed-token",
        )
    monkeypatch.setattr(CollectionRegistry, "append_event", original)
    run_universe_build(
        _collector(clean_postgres, tmp_path, ReplayTransport([])),
        generated_at_utc=NOW,
        destination=destination,
        run_id="unused-recovery-candidate",
    )
    completed = (
        tmp_path
        / "raw"
        / "fmp"
        / "runs"
        / "fmp-run-universe-completed-token"
        / "recovery-completed.json"
    )
    completed.write_bytes(b"{")
    replay = ReplayTransport([])
    with pytest.raises(ValueError, match="recovery token"):
        run_universe_build(
            _collector(clean_postgres, tmp_path, replay),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fresh-after-malformed-completion",
        )
    assert replay.calls == []


@pytest.mark.parametrize(
    ("token_name", "entry_kind"),
    [
        (REQUIRED_NAME, "dangling"),
        (REQUIRED_NAME, "regular_symlink"),
        (REQUIRED_NAME, "fifo"),
        (COMPLETED_NAME, "dangling"),
        (COMPLETED_NAME, "regular_symlink"),
        (COMPLETED_NAME, "fifo"),
    ],
)
def test_universe_recovery_token_nonregular_entries_fail_before_transport(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    token_name: str,
    entry_kind: str,
) -> None:
    destination = tmp_path / "manifest" / f"nonregular-{token_name}-{entry_kind}.json"
    run_id = f"fmp-run-{token_name}-{entry_kind}"
    bodies = [b'[{"symbol":"synth.active"}]', b'[{"symbol":"synth.old"}]']
    if token_name == COMPLETED_NAME:
        original = CollectionRegistry.append_event

        def interrupt_success(self: CollectionRegistry, event: CollectionRunEvent) -> None:
            original(self, event)
            if event.event_type is RunEventType.RUN_SUCCEEDED:
                raise RuntimeError("synthetic committed-success interruption")

        monkeypatch.setattr(CollectionRegistry, "append_event", interrupt_success)
        with pytest.raises(LifecycleRecoveryRequiredError):
            run_universe_build(
                _collector(clean_postgres, tmp_path, ReplayTransport(list(bodies))),
                generated_at_utc=NOW,
                destination=destination,
                run_id=run_id,
            )
        monkeypatch.setattr(CollectionRegistry, "append_event", original)
    else:
        outcome = run_universe_build(
            _collector(clean_postgres, tmp_path, ReplayTransport(list(bodies))),
            generated_at_utc=NOW,
            destination=destination,
            run_id=run_id,
        )
        assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    token = tmp_path / "raw" / "fmp" / "runs" / run_id / token_name
    target = token.parent / f"{entry_kind}-target"
    if entry_kind == "dangling":
        token.symlink_to(target)
    elif entry_kind == "regular_symlink":
        target.write_bytes(b"{}")
        token.symlink_to(target)
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO creation is unavailable")
        os.mkfifo(token)
    replay = ReplayTransport([])
    with pytest.raises(ValueError, match="recovery token"):
        run_universe_build(
            _collector(clean_postgres, tmp_path, replay),
            generated_at_utc=NOW,
            destination=destination,
            run_id="fresh-candidate-must-not-call",
        )
    assert replay.calls == []
    assert os.path.lexists(token)
    if entry_kind == "dangling":
        assert not target.exists()


def test_resume_record_without_started_run_reuses_saved_identity_and_plan(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    collector = _collector(clean_postgres, tmp_path, ReplayTransport([]))
    saved_at = NOW - timedelta(hours=1)
    plan = build_run_plan(
        mode=collector.config.mode,
        dataset="fmp_collection",
        parameters=collection_parameters(collector, parse_universe_manifest(_manifest()), "e" * 64),
        created_at_utc=saved_at,
        requested_window_start=saved_at - timedelta(days=2),
        requested_window_end=saved_at,
    )
    saved_run_id = "fmp-run-record-only"
    path = collector.config.raw_store_root / "fmp" / "plans" / f"{plan.plan_id}-{saved_run_id}.json"
    publish_bundle([(path, canonical_json_bytes(plan_resume_record(plan, run_id=saved_run_id)))])

    resolved_run, state, resolved_plan = resolve_resume(collector, plan, "fmp-run-must-not-be-used")

    assert resolved_run == saved_run_id
    assert state is None
    assert resolved_plan.created_at_utc == saved_at
    assert resolved_plan.requested_window_start == saved_at - timedelta(days=2)
    assert resolved_plan.requested_window_end == saved_at


def test_recurring_backfill_parameters_preserve_recovery_inputs(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    historical_day = date(2026, 7, 27)
    collector = _collector(
        clean_postgres,
        tmp_path,
        ReplayTransport([]),
        max_calls=None,
        mode=CollectionMode.BACKFILL,
        from_date=historical_day,
        recurring=True,
    )

    parameters = collection_parameters(
        collector,
        parse_universe_manifest(_manifest()),
        "e" * 64,
        DatasetSelection.ALL,
    )

    assert parameters["recurring_recovery"] == {
        "contract": "fmp-recurring-recovery-v1",
        "manifest_path": str(tmp_path / "historical-manifest.json"),
        "mode": CollectionMode.BACKFILL.value,
        "operator_from": historical_day.isoformat(),
        "output_path": str(tmp_path / "receipts" / "run.json"),
        "service_day": historical_day.isoformat(),
    }


def test_approved_run_rejects_stale_resume_before_live_transport_construction(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed = 0

    def construct_live(**_kwargs: object) -> Transport:
        nonlocal constructed
        constructed += 1
        return ReplayTransport([])

    monkeypatch.setattr(fmp_deferred_transport, "make_fmp_https_transport", construct_live)
    transport = fmp_deferred_transport.deferred_live_transport(None)
    collector = _collector(clean_postgres, tmp_path, transport)
    manifest = parse_universe_manifest(_manifest())
    plan = build_run_plan(
        mode=collector.config.mode,
        dataset=DatasetSelection.PROBE.plan_dataset,
        parameters=collection_parameters(collector, manifest, "e" * 64),
        created_at_utc=NOW,
    )
    stale_run_id = "fmp-run-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    path = collector.config.raw_store_root / "fmp" / "plans" / f"{plan.plan_id}-{stale_run_id}.json"
    publish_bundle([(path, canonical_json_bytes(plan_resume_record(plan, run_id=stale_run_id)))])

    with pytest.raises(PreconditionError, match="approved run identity"):
        run_collection(
            collector,
            manifest=manifest,
            manifest_sha256="e" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            approved_run_identity="fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )

    assert constructed == 0
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_runs)) == 0


def test_timeout_attempt_ledger_resumes_with_exact_retry_inclusive_usage(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    original = FmpCollector.record_usage

    class SimulatedCrash(BaseException):
        pass

    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedCrash()),
    )
    with pytest.raises(SimulatedCrash):
        run_collection(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([TimeoutError("synthetic"), body]),
            ),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="8" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-timeout-resume",
        )
    monkeypatch.setattr(FmpCollector, "record_usage", original)
    replay = ReplayTransport([])
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, replay),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="8" * 64,
        created_at_utc=NOW,
        run_id="unused-run",
    )
    assert outcome.run_id == "fmp-run-timeout-resume"
    assert replay.calls == []
    with clean_postgres.connect() as connection:
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == outcome.run_id,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert calls == PROFILE_TIMEOUT_RETRY_CALLS


def test_attempt_ledger_truncated_final_success_cannot_restore_two_of_three_calls(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    collector = _collector(
        clean_postgres,
        tmp_path,
        ReplayTransport([TimeoutError("synthetic"), body]),
    )
    collector.collect_profile(symbol="synth.a")
    collector.collect_window(
        dataset="fmp_price_eod_full",
        symbol="synth.a",
        window=DateWindow(date(2026, 7, 27), date(2026, 7, 29)),
    )
    attempts = tmp_path / "raw" / "fmp" / "runs" / "synthetic-run-driver" / "attempts"
    assert len(list(attempts.glob("*.json"))) == PROFILE_TIMEOUT_RETRY_CALLS
    (attempts / "00000003.json").unlink()

    with pytest.raises(CollectorContractError, match="latest attempt pointer is invalid"):
        _collector(clean_postgres, tmp_path, ReplayTransport([]))


def test_interruption_after_publication_resumes_same_run_without_transport(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    first = ReplayTransport([body])
    original = FmpCollector.record_usage

    class SimulatedCrash(BaseException):
        pass

    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedCrash()),
    )
    with pytest.raises(SimulatedCrash):
        run_collection(
            _collector(clean_postgres, tmp_path, first),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="e" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-interrupted",
        )
    monkeypatch.setattr(FmpCollector, "record_usage", original)
    resumed_transport = ReplayTransport([])
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, resumed_transport),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-must-not-be-used",
    )
    assert outcome.run_id == "fmp-run-interrupted"
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert resumed_transport.calls == []


def test_marker_only_collection_finalization_needs_no_new_provider_approval(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    first = ReplayTransport([body])
    original = FmpCollector.record_usage
    run_id = "fmp-run-collection-expiry-marker"

    class SimulatedCrash(BaseException):
        pass

    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SimulatedCrash()),
    )
    with pytest.raises(SimulatedCrash):
        run_collection(
            _collector(clean_postgres, tmp_path, first, max_calls=None),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="e" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )
    monkeypatch.setattr(FmpCollector, "record_usage", original)

    def expire() -> None:
        raise ApprovalExpiredError("no new provider authority may be consumed")

    marker = tmp_path / "raw" / "fmp" / "runs" / run_id / "publication.json"
    original_marker = marker.read_bytes()
    tampered = json.loads(original_marker)
    expected_request_count = tampered["usage"]["calls_attempted"]
    tampered["usage"]["calls_attempted"] += 1
    marker.write_bytes(canonical_json_bytes(tampered))
    rejected_transport = ReplayTransport([])
    with pytest.raises(LifecycleRecoveryRequiredError) as rejection:
        run_collection(
            _collector(
                clean_postgres,
                tmp_path,
                rejected_transport,
                max_calls=None,
                approval_check=expire,
            ),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="e" * 64,
            created_at_utc=NOW + timedelta(days=1),
            run_id="unused-tampered-collection-expiry",
        )
    assert isinstance(rejection.value.__cause__, ValueError)
    assert "quantities" in str(rejection.value.__cause__)
    assert rejected_transport.calls == []
    marker.write_bytes(original_marker)

    replay = ReplayTransport([])
    recovery_time = NOW + timedelta(days=1)
    recovered = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            replay,
            max_calls=None,
            approval_check=expire,
            clock=recovery_time,
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW + timedelta(days=1),
        run_id="unused-collection-expiry",
    )
    assert replay.calls == []
    assert recovered.run_id == run_id
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED
    assert recovered.watermarks_advanced
    receipt = json.loads((tmp_path / "receipts" / "run.json").read_bytes())
    request_count = 0
    for address in receipt["request_metadata_chunk_addresses"]:
        digest = address.removeprefix("sha256:")
        chunk = json.loads(
            (
                tmp_path
                / "raw"
                / "fmp"
                / "receipt-metadata"
                / "sha256"
                / digest[:2]
                / f"{digest}.json"
            ).read_bytes()
        )
        request_count += len(chunk["requests"])
    assert request_count == expected_request_count
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
        watermarks = connection.scalar(select(func.count()).select_from(collection_watermarks))
        usage_rows = connection.scalar(
            select(func.count())
            .select_from(collection_usage_records)
            .where(collection_usage_records.c.run_id == run_id)
        )
        usage_times = set(
            connection.execute(
                select(collection_usage_records.c.recorded_at_utc).where(
                    collection_usage_records.c.run_id == run_id
                )
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]
    assert watermarks == 1
    assert usage_rows == EXPECTED_USAGE_RECORDS
    assert usage_times == {recovery_time}


def test_post_success_watermark_failure_recovers_same_run_without_failure_event(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    original = CollectionRegistry.advance_watermark
    monkeypatch.setattr(
        CollectionRegistry,
        "advance_watermark",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic")),
    )
    with pytest.raises(PostSuccessWatermarkError):
        run_collection(
            _collector(clean_postgres, tmp_path, ReplayTransport([body])),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="e" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-watermark-recovery",
        )
    monkeypatch.setattr(CollectionRegistry, "advance_watermark", original)
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, ReplayTransport([])),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-not-used",
    )
    assert outcome.run_id == "fmp-run-watermark-recovery"
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == outcome.run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]


def test_attempt_succeeded_commit_exception_recovers_without_illegal_failure(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    original = CollectionRegistry.append_event
    injected = False

    def append_then_interrupt(self: CollectionRegistry, event: CollectionRunEvent) -> None:
        nonlocal injected
        original(self, event)
        if event.event_type is RunEventType.ATTEMPT_SUCCEEDED and not injected:
            injected = True
            raise RuntimeError("synthetic post-commit interruption")

    monkeypatch.setattr(CollectionRegistry, "append_event", append_then_interrupt)
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_collection(
            _collector(clean_postgres, tmp_path, ReplayTransport([body])),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="7" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-attempt-success-recovery",
        )
    monkeypatch.setattr(CollectionRegistry, "append_event", original)
    replay = ReplayTransport([])
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, replay),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="7" * 64,
        created_at_utc=NOW,
        run_id="unused-run",
    )
    assert outcome.run_id == "fmp-run-attempt-success-recovery"
    assert replay.calls == []
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == outcome.run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_succeeded", "run_succeeded"]


def test_incremental_driver_loads_prior_parquet_dates_for_five_day_overlap(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    prior_body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": f"2026-07-{day:02d}",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
            for day in range(22, 29)
        ]
    ).encode()
    prior_manifest = _manifest()
    assert isinstance(prior_manifest, dict)
    entries = cast("list[dict[str, object]]", prior_manifest["entries"])
    entries[0]["ipoDate"] = "2026-07-22"
    prior = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([prior_body]),
            receipt_name="prior.json",
            from_date=date(2026, 7, 22),
        ),
        manifest=parse_universe_manifest(prior_manifest),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-prior-overlap",
    )
    with clean_postgres.connect() as connection:
        prior_errors = connection.execute(
            select(collection_run_events.c.error_class, collection_run_events.c.error_message)
            .where(collection_run_events.c.run_id == "fmp-run-prior-overlap")
            .order_by(collection_run_events.c.event_seq)
        ).all()
    assert prior.terminal_event is RunEventType.RUN_SUCCEEDED, prior_errors
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-29",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    replay = ReplayTransport([body])
    outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            replay,
            mode=CollectionMode.INCREMENTAL,
            receipt_name="incremental.json",
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-overlap",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    price_request = next(request for request in replay.calls if request.endpoint.endswith("/full"))
    assert price_request.parameters["from"] == "2026-07-23"


def test_existing_rows_ignores_unregistered_parquet_without_reading_it(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    orphan = (
        tmp_path
        / "normalized"
        / "normalized"
        / "fmp"
        / "fmp_price_eod_full"
        / "run_id=unregistered"
        / "part-00000.parquet"
    )
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"not parquet")
    assert (
        existing_rows(
            _collector(clean_postgres, tmp_path, ReplayTransport([])), "fmp_price_eod_full"
        )
        == ()
    )


def test_existing_rows_consistency_rejects_isolated_digest_tamper(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([body]),
            receipt_name="consistent.json",
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="6" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-consistent-prior",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    artifact = next(path for path in outcome.published_paths if "fmp_price_eod_full" in str(path))
    artifact.write_bytes(artifact.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="publication marker"):
        existing_rows(
            _collector(clean_postgres, tmp_path, ReplayTransport([])),
            "fmp_price_eod_full",
        )


def test_existing_rows_rejects_missing_artifact_declared_by_successful_marker(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([body]),
            receipt_name="missing-inventory.json",
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="7" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-missing-prior-artifact",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    artifacts = sorted(path for path in outcome.published_paths if path.suffix == ".parquet")
    assert len(artifacts) > 1
    missing, surviving = artifacts[:2]
    missing.unlink()

    with pytest.raises(ValueError, match="publication marker inventory"):
        existing_rows(
            _collector(clean_postgres, tmp_path, ReplayTransport([])),
            surviving.parent.name,
        )


@pytest.mark.parametrize("missing_binding", ["completion", "receipt"])
def test_successful_run_replay_validates_complete_publication_binding(
    clean_postgres: Engine,
    tmp_path: Path,
    missing_binding: str,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    run_id = f"fmp-run-missing-{missing_binding}-binding"
    outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([body]),
            receipt_name=f"{missing_binding}.json",
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="a" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    binding = (
        tmp_path / "raw" / "fmp" / "runs" / run_id / "completion.json"
        if missing_binding == "completion"
        else outcome.receipt_path
    )
    assert binding is not None
    binding.unlink()

    with pytest.raises((OSError, ValueError), match=r"publication binding|No such file"):
        run_collection(
            _collector(
                clean_postgres,
                tmp_path,
                ReplayTransport([]),
                receipt_name=f"{missing_binding}.json",
            ),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="a" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )


def test_normalized_parts_stage_on_the_capacity_managed_dataset_volume(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_roots: list[Path] = []

    def recording_directory(**values: object) -> RealTemporaryDirectory[str]:
        prefix = values["prefix"]
        directory = values["dir"]
        assert isinstance(prefix, str)
        assert isinstance(directory, Path)
        observed_roots.append(directory)
        return RealTemporaryDirectory(prefix=prefix, dir=directory)

    monkeypatch.setattr(fmp_collection_work, "TemporaryDirectory", recording_directory)
    collector = _collector(
        clean_postgres,
        tmp_path,
        ReplayTransport(
            [
                json.dumps(
                    [
                        {
                            "symbol": "SYNTH.A",
                            "date": "2026-07-28",
                            "open": 1.0,
                            "high": 2.0,
                            "low": 0.5,
                            "close": 1.5,
                            "volume": 10,
                        }
                    ]
                ).encode()
            ]
        ),
        receipt_name="capacity-managed-staging.json",
    )

    outcome = run_collection(
        collector,
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="8" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-capacity-managed-staging",
    )

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert observed_roots == [collector.config.dataset_root / "normalized" / ".staging"]


def test_partial_normalized_publication_requires_same_run_recovery(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures/provider_neutral/fmp_collector/price_eod_full.json"
    ).read_bytes()
    run_id = "fmp-run-partial-normalized-publication"
    original_publish_bundle = fmp_collection_work.publish_bundle
    parquet_publications = 0

    def fail_second_part(publications: Sequence[tuple[Path, bytes]]) -> tuple[Path, ...]:
        nonlocal parquet_publications
        if publications[0][0].suffix == ".parquet":
            parquet_publications += 1
            if parquet_publications == PARTIAL_PUBLICATION_FAILURE_PART:
                raise OSError("synthetic second-part publication failure")
        return original_publish_bundle(publications)

    with monkeypatch.context() as patch:
        patch.setattr(fmp_collection_work, "publish_bundle", fail_second_part)
        with pytest.raises(
            LifecycleRecoveryRequiredError,
            match="same-run completion",
        ):
            run_collection(
                _collector(clean_postgres, tmp_path, ReplayTransport([fixture])),
                manifest=parse_universe_manifest(_manifest()),
                manifest_sha256="d" * 64,
                created_at_utc=NOW,
                run_id=run_id,
            )

    published_parts = list((tmp_path / "normalized").rglob("*.parquet"))
    assert len(published_parts) == 1
    state = CollectionRegistry(clean_postgres).current_run_state(run_id)
    assert state is not None
    assert not state.terminal

    replay = ReplayTransport([])
    recovered = run_collection(
        _collector(clean_postgres, tmp_path, replay),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="d" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )

    assert replay.calls == []
    assert recovered.terminal_event is RunEventType.RUN_SUCCEEDED


def test_expired_approval_cancels_markerless_partial_publication(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures/provider_neutral/fmp_collector/price_eod_full.json"
    ).read_bytes()
    run_id = "fmp-run-expired-partial-publication"
    original_publish_bundle = fmp_collection_work.publish_bundle
    parquet_publications = 0
    collector = _collector(clean_postgres, tmp_path, ReplayTransport([fixture]))

    def expire() -> None:
        raise ApprovalExpiredError("synthetic approval crossed its boundary")

    def expire_after_first_part(
        publications: Sequence[tuple[Path, bytes]],
    ) -> tuple[Path, ...]:
        nonlocal parquet_publications
        published = original_publish_bundle(publications)
        if publications[0][0].suffix == ".parquet":
            parquet_publications += 1
            if parquet_publications == 1:
                monkeypatch.setattr(collector, "_approval_check", expire)
        return published

    monkeypatch.setattr(
        fmp_collection_work,
        "publish_bundle",
        expire_after_first_part,
    )

    outcome = run_collection(
        collector,
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="9" * 64,
        created_at_utc=NOW,
        run_id=run_id,
    )

    assert outcome.terminal_event is RunEventType.RUN_CANCELLED
    assert not (tmp_path / "raw" / "fmp" / "runs" / run_id / "publication.json").exists()
    parts = list((tmp_path / "normalized").rglob("*.parquet"))
    assert len(parts) == 1
    assert parts[0].is_relative_to(
        collector.config.dataset_root / "normalized" / "fmp" / ".runs" / f"run_id={run_id}"
    )
    state = CollectionRegistry(clean_postgres).current_run_state(run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_CANCELLED
    assert (
        existing_rows(
            _collector(clean_postgres, tmp_path, ReplayTransport([])),
            "fmp_price_eod_full",
        )
        == ()
    )


def test_normalized_artifact_digest_streams_without_path_read_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"streamed-parquet-digest" * 1024
    artifact = tmp_path / "part-00000.parquet"
    artifact.write_bytes(payload)
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path == artifact:
            raise AssertionError("artifact digest must not use Path.read_bytes")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    assert _sha256_file(artifact) == hashlib.sha256(payload).hexdigest()


def test_historical_parts_validate_their_run_binding_only_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "fmp-run-validation-cache"
    plan_id = "fmp-plan-validation-cache"
    artifacts = []
    for index in range(2):
        path = tmp_path / f"part-{index:05d}.parquet"
        path.write_bytes(f"part-{index}".encode())
        artifacts.append({"path": str(path), "sha256": _sha256_file(path)})
    marker_document = {
        "attempt_ledger_sha256": "a" * 64,
        "plan_id": plan_id,
        "run_id": run_id,
        "artifacts": artifacts,
    }
    marker_bytes = canonical_json_bytes(marker_document)
    receipt_path = tmp_path / "receipt.json"
    receipt = canonical_json_bytes(
        {
            "plan_id": plan_id,
            "publication_marker_sha256": hashlib.sha256(marker_bytes).hexdigest(),
            "request_metadata_chunk_addresses": [],
            "run_id": run_id,
            "run_identity": "synthetic-validation-cache",
        }
    )
    receipt_path.write_bytes(receipt)
    run_root = tmp_path / "raw" / "fmp" / "runs" / run_id
    run_root.mkdir(parents=True)
    (run_root / "publication.json").write_bytes(marker_bytes)
    (run_root / "completion.json").write_bytes(
        canonical_json_bytes(
            {
                "marker_sha256": hashlib.sha256(marker_bytes).hexdigest(),
                "plan_id": plan_id,
                "receipt_path": str(receipt_path),
                "receipt_sha256": hashlib.sha256(receipt).hexdigest(),
                "run_id": run_id,
            }
        )
    )
    digest_calls = 0

    def attempt_digest(_collector: FmpCollector, _run_id: str) -> str:
        nonlocal digest_calls
        digest_calls += 1
        return "a" * 64

    monkeypatch.setattr(fmp_collector_state, "_attempt_digest", attempt_digest)
    collector = cast(
        "FmpCollector",
        cast(
            "object",
            SimpleNamespace(
                _historical_run_validation_cache={},
                _historical_validated_runs=set(),
                config=SimpleNamespace(
                    raw_store_root=tmp_path / "raw",
                    receipt_path=receipt_path,
                ),
            ),
        ),
    )

    for artifact in artifacts:
        _validate_artifact_consistency(
            collector,
            Path(artifact["path"]),
            run_id,
            plan_id,
        )

    assert digest_calls == 1


def test_delisted_empty_window_records_shortfall_not_absence(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    document = _manifest()
    assert isinstance(document, dict)
    document["entries"] = [
        {
            "symbol": "synth.old",
            "ipoDate": "2026-07-27",
            "delistedDate": "2026-07-29",
            "active": False,
        }
    ]
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, ReplayTransport([b"[]"])),
        manifest=parse_universe_manifest(document),
        manifest_sha256="f" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-delisted-shortfall",
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    receipt = json.loads((tmp_path / "receipts" / "run.json").read_bytes())
    assert "delisted_shortfall" in {result["kind"] for result in receipt["quality_results"]}


def test_recycled_ticker_profile_blocks_price_and_watermark(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    prior = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            ReplayTransport([b"[]"], profile_cik="0000000002"),
            receipt_name="prior-profile.json",
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="9" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-prior-profile",
    )
    assert prior.terminal_event is RunEventType.RUN_SUCCEEDED
    replay = ReplayTransport([])
    outcome = run_collection(
        _collector(clean_postgres, tmp_path, replay, receipt_name="recycled.json"),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="a" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-recycled",
    )
    assert outcome.blocked_symbols == ("synth.a",)
    assert [request.endpoint for request in replay.calls] == ["/stable/profile"]
    assert list((tmp_path / "normalized").rglob("run_id=fmp-run-recycled/*.parquet")) == []
    assert _count(clean_postgres, collection_watermarks) == 0


def test_database_receipt_registration_is_absent_from_the_004c_port() -> None:
    assert "record_receipt" not in ControlPlanePort.__dict__


@pytest.mark.parametrize("stage", ["record_usage", "build_receipt"])
def test_durable_preterminal_stage_failure_requires_same_run_recovery(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    body = json.dumps(
        [
            {
                "symbol": "SYNTH.A",
                "date": "2026-07-28",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
            }
        ]
    ).encode()
    monkeypatch.setattr(
        FmpCollector,
        stage,
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic")),
    )
    run_id = f"fmp-run-failure-{stage}"
    with pytest.raises(LifecycleRecoveryRequiredError):
        run_collection(
            _collector(clean_postgres, tmp_path, ReplayTransport([body])),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="b" * 64,
            created_at_utc=NOW,
            run_id=run_id,
        )
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == run_id)
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
    assert events == ["attempt_started"]
    assert _count(clean_postgres, collection_watermarks) == 0


def test_usage_persistence_failure_before_terminal_failure_leaves_run_recoverable(
    clean_postgres: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        FmpCollector,
        "record_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("usage unavailable")),
    )
    with pytest.raises(LifecycleRecoveryRequiredError, match="before failure"):
        run_collection(
            _collector(clean_postgres, tmp_path, ReplayTransport([b'[{"symbol":"SYNTH.A"}]'])),
            manifest=parse_universe_manifest(_manifest()),
            manifest_sha256="5" * 64,
            created_at_utc=NOW,
            run_id="fmp-run-usage-before-fail",
        )
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == "fmp-run-usage-before-fail")
                .order_by(collection_run_events.c.event_seq)
            ).scalars()
        )
    assert events == ["attempt_started"]


def test_five_timeouts_record_failure_without_publication_or_watermark(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    transport = ReplayTransport([TimeoutError("synthetic") for _ in range(MAX_RETRY_ATTEMPTS)])
    collector = _collector(clean_postgres, tmp_path, transport, max_calls=MAX_RETRY_ATTEMPTS + 1)
    outcome = run_collection(
        collector,
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-driver-failure",
    )

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert len(transport.calls) == MAX_RETRY_ATTEMPTS + 1
    with clean_postgres.connect() as connection:
        calls = connection.scalar(
            select(collection_usage_records.c.quantity).where(
                collection_usage_records.c.run_id == outcome.run_id,
                collection_usage_records.c.metric == "calls_attempted",
            )
        )
    assert calls == MAX_RETRY_ATTEMPTS + 1
    collector.record_usage(run_id=outcome.run_id, recorded_at_utc=NOW)
    assert _count(clean_postgres, collection_usage_records) == USAGE_METRIC_COUNT
    assert _count(clean_postgres, collection_watermarks) == 0
    assert not (tmp_path / "receipts" / "run.json").exists()
    assert not list((tmp_path / "normalized").rglob("*.parquet"))
    with clean_postgres.connect() as connection:
        events = list(
            connection.execute(
                select(collection_run_events.c.event_type).order_by(
                    collection_run_events.c.event_seq
                )
            ).scalars()
        )
    assert events == ["attempt_started", "attempt_failed", "run_failed"]


def test_max_calls_one_allows_exactly_one_transport_attempt(
    clean_postgres: Engine,
    tmp_path: Path,
) -> None:
    transport = ReplayTransport([TimeoutError("synthetic")])
    terminal_time = NOW + timedelta(days=1)
    outcome = run_collection(
        _collector(
            clean_postgres,
            tmp_path,
            transport,
            max_calls=1,
            clock=terminal_time,
        ),
        manifest=parse_universe_manifest(_manifest()),
        manifest_sha256="e" * 64,
        created_at_utc=NOW,
        run_id="fmp-run-driver-budget",
    )

    assert outcome.terminal_event is RunEventType.RUN_CANCELLED
    assert len(transport.calls) == 1
    assert _count(clean_postgres, collection_watermarks) == 0
    with clean_postgres.connect() as connection:
        usage_times = set(
            connection.execute(
                select(collection_usage_records.c.recorded_at_utc).where(
                    collection_usage_records.c.run_id == outcome.run_id
                )
            ).scalars()
        )
    assert usage_times == {terminal_time}
