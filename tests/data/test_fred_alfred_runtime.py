"""Synthetic HTTP bodies through the durable FRED runtime and real PostgreSQL."""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from http import HTTPStatus
from pathlib import Path
from typing import ClassVar
from urllib.parse import parse_qs, urlsplit
from urllib.request import BaseHandler, Request, build_opener
from urllib.response import addinfourl

import pytest
from fred_alfred_authority_support import make_standing_authority
from fred_alfred_collector_support import CREDENTIAL, FakeClock, fixture_bytes, make_config
from sqlalchemy import Connection, Engine, select, text

from aegis_alpha.collection.records import CollectionRunEvent, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_run_receipts,
    collection_runs,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.data import fred_alfred_collector as collector_module
from aegis_alpha.data import fred_alfred_registration as registration_module
from aegis_alpha.data.fred_alfred_collector import (
    CollectorConfig,
    CollectorRequest,
    CollectorResponse,
)
from aegis_alpha.data.fred_alfred_evidence import publish_evidence, source_provenance_path
from aegis_alpha.data.fred_alfred_rate_limit import RateLimiter
from aegis_alpha.data.fred_alfred_recurring_authority import (
    VerifiedRecurringAuthority,
    verify_recurring_authority,
)
from aegis_alpha.data.fred_alfred_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fred_alfred_runtime import run_runtime
from aegis_alpha.data.fred_alfred_usage_budget import load_daily_usage_budget
from aegis_alpha.metadata.schema import dataset_artifacts, dataset_versions, source_snapshots

_HTTP_CALLS = 4
_OBSERVATIONS = 2


class Clock(FakeClock):
    def __init__(self) -> None:
        super().__init__()
        self.base = datetime.now(UTC)

    def now(self) -> datetime:
        return self.base + timedelta(seconds=self.seconds)


class HTTPFixture(io.BytesIO):
    status = 200
    headers: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}


class Opener:
    def __init__(self, engine: Engine, run_id: str) -> None:
        self.engine, self.run_id = engine, run_id
        self.urls: list[str] = []
        self.bodies: list[bytes] = []

    def open(self, request: Request, *, timeout: float) -> HTTPFixture:
        assert timeout > 0
        parsed = urlsplit(request.full_url)
        query = parse_qs(parsed.query)
        assert parsed.hostname == "api.stlouisfed.org"
        assert query.pop("api_key") == [CREDENTIAL]
        # No transport starts until its real run and bound reservation are committed.
        with self.engine.connect() as connection:
            assert (
                connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND state='idle in transaction'"
                    )
                )
                == 0
            )
            assert (
                connection.scalar(
                    select(collection_runs.c.run_id).where(collection_runs.c.run_id == self.run_id)
                )
                == self.run_id
            )
            bound = (
                connection.execute(
                    select(collection_run_plans.c.parameters_json).where(
                        collection_run_plans.c.dataset == "fred_alfred_daily_budget"
                    )
                )
                .scalars()
                .all()
            )
            assert any(item["actual_run_id"] == self.run_id for item in bound)
        self.urls.append(parsed.path + "?" + str(query))
        if parsed.path == "/fred/series":
            body = fixture_bytes("series_T10Y2Y.json")
        elif parsed.path == "/fred/series/vintagedates":
            body = b'{"vintage_dates":["2024-01-16"]}'
        else:
            rows = json.loads(fixture_bytes("observations_T10Y2Y_2024-01-16.json"))["observations"]
            offset = int(query["offset"][0])
            body = json.dumps(
                {
                    "observations": rows[offset : offset + 1],
                    "offset": offset,
                    "limit": 1,
                    "count": 2,
                }
            ).encode()
        self.bodies.append(body)
        return HTTPFixture(body)


def _setup(
    tmp_path: Path, engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> tuple[CollectorConfig, VerifiedRecurringAuthority, Clock, Opener]:
    clock = Clock()
    signed = make_standing_authority(tmp_path)
    authority = verify_recurring_authority(
        signed.payload, signed.signature, signed.owner_authority, now=clock.now()
    )
    config = make_config(tmp_path, series_ids=("T10Y2Y",), max_calls=10)
    opener = Opener(engine, config.run_identity)
    monkeypatch.setattr(collector_module, "build_opener", lambda *_args: opener)
    return config, authority, clock, opener


def test_http_roundtrip_page_lineage_and_zero_http_replay(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    outcome = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert outcome.calls_attempted == len(opener.urls) == _HTTP_CALLS
    assert len(outcome.rows) == _OBSERVATIONS
    assert len({row["source_snapshot_id"] for row in outcome.rows}) == _OBSERVATIONS
    with clean_postgres.connect() as connection:
        snapshots = {row.snapshot_id: row for row in connection.execute(select(source_snapshots))}
        for row in outcome.rows:
            assert isinstance(row["observation_date"], date)
            source = snapshots[row["source_snapshot_id"]]
            raw = (
                config.raw_store_root
                / "blobs"
                / "sha256"
                / source.content_sha256[:2]
                / f"{source.content_sha256}.raw"
            )
            assert hashlib.sha256(raw.read_bytes()).hexdigest() == row["raw_content_sha256"]
            assert row["retrieved_at_utc"] == source.retrieved_at_utc
            assert row["availability_time_utc"] == source.retrieved_at_utc
            assert (
                json.loads(raw.read_bytes())["observations"][0]["date"]
                == row["observation_date"].isoformat()
            )
        receipt = connection.execute(select(collection_run_receipts)).one()
        aggregate = snapshots[receipt.source_snapshot_id]
        assert aggregate.dataset == "fred_alfred_observations"
        catalog = connection.execute(select(dataset_versions)).one()
        assert catalog.dataset_id == aggregate.dataset
        assert catalog.dataset_version == hashlib.sha256(outcome.run_id.encode()).hexdigest()
        assert catalog.row_count == _OBSERVATIONS
        assert not any(
            (
                catalog.canonical_eligible,
                catalog.backtest_eligible,
                catalog.paper_eligible,
                catalog.order_eligible,
            )
        )
        artifacts = connection.execute(select(dataset_artifacts)).all()
        assert {config.dataset_root / item.relative_path for item in artifacts} == {
            path for path in outcome.published_paths if path.suffix == ".parquet"
        }
        for artifact in artifacts:
            payload = (config.dataset_root / artifact.relative_path).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == artifact.content_sha256
            assert len(payload) == artifact.size_bytes
        raw = (
            config.raw_store_root
            / "blobs"
            / "sha256"
            / aggregate.content_sha256[:2]
            / f"{aggregate.content_sha256}.raw"
        )
        assert len(json.loads(raw.read_bytes())["sources"]) == len(opener.urls)
        assert connection.scalar(select(collection_watermarks.c.run_id)) == config.run_identity
        assert connection.execute(
            select(collection_usage_records.c.quantity)
            .where(collection_usage_records.c.run_id == config.run_identity)
            .order_by(collection_usage_records.c.usage_seq)
        ).scalars().all() == [4, sum(map(len, opener.bodies))]
    replay = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert replay.run_id == outcome.run_id
    assert replay.recovered is True
    assert len(opener.urls) == _HTTP_CALLS
    assert (
        load_daily_usage_budget(
            engine=clean_postgres, authority=authority, now=clock.now()
        ).calls_used
        == _HTTP_CALLS
    )


@pytest.mark.parametrize("resume_delay", [0, 86400])
def test_finalization_failure_preserves_accounting_and_recovers_without_http(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch, resume_delay: int
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    append = CollectionRegistry.append_event

    def fail_terminal(
        self: CollectionRegistry, event: CollectionRunEvent, *, connection: Connection | None = None
    ) -> int:
        if event.event_type is RunEventType.RUN_SUCCEEDED:
            raise RuntimeError("synthetic finalization failure")
        return append(self, event, connection=connection)

    with monkeypatch.context() as patch:
        patch.setattr(CollectionRegistry, "append_event", fail_terminal)
        with pytest.raises(RuntimeError, match="synthetic finalization failure"):
            run_runtime(
                config=config,
                engine=clean_postgres,
                authority=authority,
                credential=CREDENTIAL,
                clock=clock.now,
                monotonic=clock.time,
                sleep=clock.sleep,
            )
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_run_events.c.event_type)).scalars().all() == [
            "attempt_started"
        ]
        assert connection.execute(select(collection_run_receipts)).all() == []
        assert connection.execute(select(collection_watermarks)).all() == []
        assert connection.execute(select(dataset_versions)).all() == []
        usage = {
            (row.run_id, row.metric): row.quantity
            for row in connection.execute(select(collection_usage_records))
        }
        assert usage[(config.run_identity, "calls_attempted")] == _HTTP_CALLS
        assert usage[(config.run_identity, "bytes_received")] == sum(map(len, opener.bodies))
        assert (
            sum(
                quantity
                for (run_id, metric), quantity in usage.items()
                if metric == "calls_attempted"
            )
            == _HTTP_CALLS
        )
    clock.seconds += resume_delay
    if resume_delay:

        def no_new_settlement(**_kwargs: object) -> None:
            raise AssertionError("past committed usage must not be settled again")

        monkeypatch.setattr(registration_module, "record_run_consumption", no_new_settlement)
    outcome = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert len(opener.urls) == _HTTP_CALLS


def test_failed_http_is_accounted_without_watermark(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, authority, clock, _opener = _setup(tmp_path, clean_postgres, monkeypatch)

    def failing(_request: CollectorRequest, _credential: str) -> CollectorResponse:
        raise OSError("synthetic network failure")

    outcome = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        transport=failing,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.calls_attempted == 1
    assert (
        load_daily_usage_budget(
            engine=clean_postgres, authority=authority, now=clock.now()
        ).calls_used
        == 1
    )
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_watermarks)).all() == []


def test_expiry_after_pacing_consumes_no_slot(tmp_path: Path) -> None:
    signed = make_standing_authority(tmp_path)
    clock = Clock()
    authority = verify_recurring_authority(
        signed.payload, signed.signature, signed.owner_authority, now=clock.now()
    )
    authority = replace(authority, key_valid_until_utc=clock.now() + timedelta(seconds=1))
    limiter = RateLimiter(
        max_calls=2,
        calls_per_minute=30,
        clock=clock.time,
        sleep=clock.sleep,
        revalidate=lambda: authority.require_request(clock.now()),
    )
    limiter.before_request()
    limiter.after_response(byte_count=0)
    with pytest.raises(RecurringAuthorityError, match="expired"):
        limiter.before_request()
    assert limiter.calls_attempted == 1
    assert clock.waits == [2]


def test_no_clobber_and_symlink_refusal(tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    publish_evidence(path, b"original")
    with pytest.raises(ValueError, match="clobber"):
        publish_evidence(path, b"replacement")
    assert path.read_bytes() == b"original"
    alias = tmp_path / "alias"
    alias.symlink_to(path)
    with pytest.raises(ValueError, match="regular file"):
        publish_evidence(alias, b"original")


def test_http_redirects_are_not_unmetered_followup_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[Request] = []

    class RedirectResponse(addinfourl):
        msg = "Found"

    class FixtureHTTPS(BaseHandler):
        handler_order = 400

        def https_open(self, request: Request) -> addinfourl:
            requests.append(request)
            headers = Message()
            headers["Location"] = "https://api.stlouisfed.org/next"
            return RedirectResponse(
                io.BytesIO(b"fixture redirect"), headers, request.full_url, HTTPStatus.FOUND
            )

    monkeypatch.setattr(
        collector_module, "build_opener", lambda *handlers: build_opener(*handlers, FixtureHTTPS())
    )
    transport = collector_module.make_https_transport()
    response = transport(
        CollectorRequest("/fred/series", {"series_id": "T10Y2Y"}, "T10Y2Y"), CREDENTIAL
    )
    assert response.status_code == HTTPStatus.FOUND
    assert response.body == b"fixture redirect"
    assert len(requests) == 1


@pytest.mark.parametrize("snapshot_id", ["../escape", "/absolute", "..", "nested/file"])
def test_source_provenance_cannot_escape_raw_root(tmp_path: Path, snapshot_id: str) -> None:

    with pytest.raises(ValueError, match="safe leaf"):
        source_provenance_path(tmp_path, snapshot_id)
    assert list(tmp_path.iterdir()) == []
