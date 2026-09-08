from __future__ import annotations

import json
from datetime import UTC, date, datetime
from http.client import HTTPMessage
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.request import Request

import pyarrow.parquet as pq
import pytest
from fred_alfred_collector_support import (
    CREDENTIAL,
    NOW,
    FakeControlPlane,
    ScriptedTransport,
    default_script,
    make_collector,
    make_config,
    observation_keys,
    response_from_fixture,
)

from aegis_alpha.collection.records import CollectionMode, RunEventType, WatermarkAdvance
from aegis_alpha.collection.registry import CollectionRegistry, CollectionStateError
from aegis_alpha.data.fred_alfred_collector import (
    ALLOWED_HOST,
    CollectorError,
    CollectorRequest,
    CollectorResponse,
    CredentialLeakError,
    DestinationError,
    HostPinnedRedirectHandler,
    SeriesOutcome,
    assert_credential_absent,
    make_https_transport,
    page_is_truncated,
    validate_destination,
)
from aegis_alpha.data.fred_alfred_normalize import CURRENT_VINTAGE_END, observation_key
from aegis_alpha.data.fred_alfred_series import (
    DEFAULT_SERIES_UNIVERSE_SHA256,
    PLAN_DATASET,
    PROVIDER,
    series_universe_sha256,
    watermark_dataset,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

RESTATED_VINTAGE_ROWS = 2

_005_EVENTS = {
    "attempt_started",
    "attempt_succeeded",
    "attempt_failed",
    "run_succeeded",
    "run_failed",
    "run_cancelled",
}


def test_request_is_credential_free_and_host_allowlisted() -> None:
    request = CollectorRequest(
        endpoint="/fred/series/observations",
        parameters={
            "file_type": "json",
            "realtime_end": "2024-01-16",
            "realtime_start": "2024-01-16",
            "series_id": "T10Y2Y",
        },
        series_id="T10Y2Y",
    )
    assert ALLOWED_HOST in request.source_uri
    assert "api_key" not in request.source_uri
    assert CREDENTIAL not in request.source_uri
    assert request.request_fingerprint.startswith("sha256:")
    with pytest.raises(ValueError, match="credential parameters"):
        CollectorRequest(
            endpoint="/fred/series",
            parameters={"api_key": "x", "series_id": "T10Y2Y"},
            series_id="T10Y2Y",
        )
    with pytest.raises(ValueError, match="must not invent extra series"):
        CollectorRequest(
            endpoint="/fred/series",
            parameters={"file_type": "json", "series_id": "GDPPOT"},
            series_id="GDPPOT",
        )


def test_request_accepts_macro_catalog_series_beyond_legacy_default() -> None:
    request = CollectorRequest(
        endpoint="/fred/series/observations",
        parameters={"file_type": "json", "series_id": "CPIAUCSL"},
        series_id="CPIAUCSL",
    )
    assert request.series_id == "CPIAUCSL"
    assert "series_id=CPIAUCSL" in request.source_uri


def _macro_series_transport(series_id: str, *, title: str, value: str) -> ScriptedTransport:
    """Synthetic in-memory responses for one macro-catalog series. No fixture files."""

    return ScriptedTransport(
        {
            ("/fred/series", series_id, None): _json_response(
                {
                    "seriess": [
                        {
                            "id": series_id,
                            "title": title,
                            "frequency": "Monthly",
                            "units": "Index",
                            "seasonal_adjustment": "Seasonally Adjusted",
                        }
                    ]
                }
            ),
            ("/fred/series/vintagedates", series_id, None): _json_response(
                {"vintage_dates": ["2024-02-13"]}
            ),
            ("/fred/series/observations", series_id, "2024-02-13"): _json_response(
                {
                    "observations": [
                        {
                            "realtime_start": "2024-02-13",
                            "realtime_end": "9999-12-31",
                            "date": "2024-01-01",
                            "value": value,
                        }
                    ]
                }
            ),
        }
    )


def test_non_default_macro_series_runs_captures_and_advances_its_own_stream(
    tmp_path: Path,
) -> None:
    # Given a synthetic transport for a macro-catalog series outside the legacy four
    transport = _macro_series_transport(
        "CPIAUCSL", title="Synthetic CPI All Urban", value="308.417"
    )
    collector, plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("CPIAUCSL",)),
    )

    # When the collector runs
    outcome = collector.collect()

    # Then every request names only that series and the run succeeds
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert {request.series_id for request in transport.requests} == {"CPIAUCSL"}
    assert all(request.parameters["series_id"] == "CPIAUCSL" for request in transport.requests)
    assert [row["series_id"] for row in outcome.rows] == ["CPIAUCSL"]
    assert outcome.rows[0]["value"] == "308.417"
    # And the raw capture and normalized partition exist under the series identity
    raw_blobs = list((tmp_path / "raw").rglob("*.raw"))
    assert raw_blobs
    assert not any(CREDENTIAL.encode() in path.read_bytes() for path in raw_blobs)
    partitions = [path for path in outcome.published_paths if path.suffix == ".parquet"]
    assert any("series_id=CPIAUCSL" in path.as_posix() for path in partitions)
    # And the watermark advanced on the per-series stream inside the shared plan dataset
    assert outcome.watermarks_advanced == (("CPIAUCSL", "2024-02-13"),)
    assert isinstance(plane, FakeControlPlane)
    watermark = plane.latest_watermark(PROVIDER, watermark_dataset("CPIAUCSL"), "CPIAUCSL")
    assert watermark is not None
    assert watermark.watermark_value == "2024-02-13"
    assert plane.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y") is None
    # And the receipt hashes the selected universe, which differs from the legacy default
    receipt = json.loads((tmp_path / "receipts" / "run.receipt.json").read_text())
    assert receipt["series_ids"] == ["CPIAUCSL"]
    assert receipt["series_universe_sha256"] == series_universe_sha256(("CPIAUCSL",))
    assert receipt["series_universe_sha256"] != DEFAULT_SERIES_UNIVERSE_SHA256
    plan = next(iter(plane.plans.values()))
    assert plan.parameters["series_ids"] in (["CPIAUCSL"], ("CPIAUCSL",))
    assert plan.parameters["default_series_universe_sha256"] == DEFAULT_SERIES_UNIVERSE_SHA256


def test_mixed_legacy_and_macro_selection_is_catalog_ordered(tmp_path: Path) -> None:
    config = make_config(tmp_path, series_ids=("UNRATE", "DGS2", "T10Y2Y"))
    assert config.series_ids == ("T10Y2Y", "DGS2", "UNRATE")
    with pytest.raises(ValueError, match="duplicate series_id"):
        make_config(tmp_path, series_ids=("UNRATE", "UNRATE"))
    with pytest.raises(ValueError, match="must not invent extra series"):
        make_config(tmp_path, series_ids=("UNRATE", "GDPPOT"))


def test_incremental_keeps_restated_vintage_rows_and_unique_keys(tmp_path: Path) -> None:
    transport = default_script()
    collector, plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert isinstance(plane, FakeControlPlane)
    assert set(plane.event_types) <= _005_EVENTS
    assert plane.event_types == [
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    ]
    restated = [
        row
        for row in outcome.rows
        if row["series_id"] == "T10Y2Y" and row["observation_date"] == date(2024, 1, 15)
    ]
    assert len(restated) == RESTATED_VINTAGE_ROWS
    values = {row["value"] for row in restated}
    assert values == {"0.45", "0.41"}
    keys = {observation_key(row).as_tuple() for row in restated}
    assert keys == {
        ("T10Y2Y", date(2024, 1, 15), date(2024, 1, 16), date(2024, 3, 14)),
        ("T10Y2Y", date(2024, 1, 15), date(2024, 3, 15), CURRENT_VINTAGE_END),
    }
    assert len(set(observation_keys(outcome.rows))) == len(outcome.rows)
    missing = [
        row
        for row in outcome.rows
        if row["observation_date"] == date(2024, 1, 16) and row["series_id"] == "T10Y2Y"
    ]
    assert missing
    assert missing[0]["value"] is None
    raw_blobs = list((tmp_path / "raw").rglob("*.raw"))
    assert raw_blobs
    assert not any(CREDENTIAL.encode() in path.read_bytes() for path in raw_blobs)


def test_watermark_does_not_advance_a_failed_series(tmp_path: Path) -> None:
    transport = default_script(fail_series=frozenset({"T10Y3M"}))
    collector, plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y", "T10Y3M")),
    )

    outcome = collector.collect()

    assert isinstance(plane, FakeControlPlane)
    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert plane.event_types == [
        "attempt_started",
        "attempt_succeeded",
        "run_succeeded",
    ]
    advanced = {series_id for series_id, _value in outcome.watermarks_advanced}
    assert advanced == {"T10Y2Y"}
    assert plane.latest_watermark(PROVIDER, watermark_dataset("T10Y2Y"), "T10Y2Y") is not None
    assert plane.latest_watermark(PROVIDER, watermark_dataset("T10Y3M"), "T10Y3M") is None
    failed = [item for item in outcome.series_outcomes if item.series_id == "T10Y3M"]
    assert failed
    assert failed[0].succeeded is False


def test_all_series_failed_is_run_failed_and_advances_nothing(tmp_path: Path) -> None:
    transport = default_script(fail_series=frozenset({"T10Y2Y"}))
    collector, plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert isinstance(plane, FakeControlPlane)
    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.watermarks_advanced == ()
    assert plane.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y") is None
    with pytest.raises(CollectorError, match="run_succeeded"):
        collector.advance_successful_watermarks(
            outcome.run_id,
            (
                SeriesOutcome(
                    series_id="T10Y2Y",
                    succeeded=True,
                    vintage_watermark=date(2024, 3, 15),
                    row_count=1,
                ),
            ),
        )


def test_incremental_does_not_refetch_vintages_at_or_before_watermark(tmp_path: Path) -> None:
    first_transport = default_script()
    first, plane, clock = make_collector(
        tmp_path,
        first_transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )
    first.collect()
    second_transport = default_script(clock=clock)
    second, _plane, _clock = make_collector(
        tmp_path,
        second_transport,
        control_plane=plane,
        clock=clock,
        config=make_config(tmp_path, series_ids=("T10Y2Y",), receipt_name="run-2.receipt.json"),
    )

    outcome = second.collect()

    observation_calls = [
        request
        for request in second_transport.requests
        if request.endpoint == "/fred/series/observations"
    ]
    assert observation_calls == []
    assert outcome.rows == ()
    assert outcome.watermarks_advanced == ()


def test_probe_uses_latest_vintage_only_and_does_not_advance_watermark(
    tmp_path: Path,
) -> None:
    transport = default_script()
    collector, plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(
            tmp_path,
            mode=CollectionMode.PROBE,
            series_ids=("T10Y2Y",),
            observation_start=None,
        ),
    )

    outcome = collector.collect()

    vintages = [
        request.parameters.get("realtime_start")
        for request in transport.requests
        if request.endpoint == "/fred/series/observations"
    ]
    assert vintages == ["2024-03-15"]
    assert outcome.watermarks_advanced == ()
    assert isinstance(plane, FakeControlPlane)
    assert plane.watermarks == {}
    assert all(row["series_id"] == "T10Y2Y" for row in outcome.rows)


def test_backfill_streams_the_full_vintage_list(tmp_path: Path) -> None:
    transport = default_script()
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(
            tmp_path,
            mode=CollectionMode.BACKFILL,
            series_ids=("T10Y2Y",),
            observation_start=date(2024, 1, 1),
        ),
    )

    outcome = collector.collect()

    vintages = [
        request.parameters.get("realtime_start")
        for request in transport.requests
        if request.endpoint == "/fred/series/observations"
    ]
    assert vintages == ["2024-01-16", "2024-03-15"]
    assert ("T10Y2Y", "2024-03-15") in outcome.watermarks_advanced


def test_dgs_legs_are_stored_without_replacing_t10y2y(tmp_path: Path) -> None:
    transport = default_script()
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y", "DGS10", "DGS2")),
    )

    outcome = collector.collect()

    by_series = {row["series_id"]: row["value"] for row in outcome.rows if row["value"] is not None}
    assert by_series["T10Y2Y"] in {"0.45", "0.41"}
    assert by_series["DGS10"] == "4.20"
    assert by_series["DGS2"] == "4.00"
    assert by_series["T10Y2Y"] != "0.20"


def test_normalized_parquet_is_hive_partitioned(tmp_path: Path) -> None:
    transport = default_script()
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )
    outcome = collector.collect()
    partitions = [path for path in outcome.published_paths if path.suffix == ".parquet"]
    observation_parts = [path for path in partitions if "series_id=T10Y2Y" in path.as_posix()]
    assert observation_parts
    assert any("/year=2024/" in path.as_posix() for path in observation_parts)
    table = pq.read_table(observation_parts[0])
    assert "series_id" in table.schema.names
    assert "realtime_start" in table.schema.names


def test_git_contained_destinations_are_rejected(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    with pytest.raises(DestinationError, match="outside a Git repository"):
        validate_destination("raw store root", repository / "raw")


def test_credential_scan_never_quotes_the_secret() -> None:
    with pytest.raises(CredentialLeakError) as excinfo:
        assert_credential_absent(CREDENTIAL, f"leak {CREDENTIAL}".encode())
    assert CREDENTIAL not in str(excinfo.value)


def test_collect_run_never_opens_a_socket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("G-A must not open a network connection")

    monkeypatch.setattr("urllib.request.urlopen", forbidden)
    monkeypatch.setattr("urllib.request.build_opener", forbidden)
    monkeypatch.setattr("socket.create_connection", forbidden)
    transport = default_script()
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )
    outcome = collector.collect()
    assert outcome.rows
    assert transport.requests
    assert make_https_transport is not transport


def test_live_transport_is_not_used_by_ga_collect(tmp_path: Path) -> None:
    transport = default_script()
    collector, _plane, _clock = make_collector(tmp_path, transport)
    collector.collect()
    assert all(
        request.source_uri.startswith("https://api.stlouisfed.org/fred/")
        for request in transport.requests
    )
    assert all("api_key" not in request.source_uri for request in transport.requests)


def test_page_is_truncated_when_count_exceeds_length_or_page_is_full() -> None:
    assert (
        page_is_truncated(
            page_length=2,
            accumulated_length=2,
            total_count=5,
            page_limit=10,
        )
        is True
    )
    assert (
        page_is_truncated(
            page_length=10,
            accumulated_length=10,
            total_count=None,
            page_limit=10,
        )
        is True
    )
    assert (
        page_is_truncated(
            page_length=2,
            accumulated_length=2,
            total_count=2,
            page_limit=10,
        )
        is False
    )


def _json_response(payload: dict[str, object]) -> CollectorResponse:
    return CollectorResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
        requested_at_utc=NOW,
        retrieved_at_utc=NOW,
    )


def _paged_vintages_transport() -> ScriptedTransport:
    return ScriptedTransport(
        {
            ("/fred/series", "T10Y2Y", None): response_from_fixture("series_T10Y2Y.json"),
            ("/fred/series/vintagedates", "T10Y2Y", None, "0"): _json_response(
                {
                    "vintage_dates": ["2024-01-16"],
                    "count": 2,
                    "offset": 0,
                    "limit": 1,
                }
            ),
            ("/fred/series/vintagedates", "T10Y2Y", None, "1"): _json_response(
                {
                    "vintage_dates": ["2024-03-15"],
                    "count": 2,
                    "offset": 1,
                    "limit": 1,
                }
            ),
            ("/fred/series/observations", "T10Y2Y", "2024-01-16"): response_from_fixture(
                "observations_T10Y2Y_2024-01-16.json"
            ),
            ("/fred/series/observations", "T10Y2Y", "2024-03-15"): response_from_fixture(
                "observations_T10Y2Y_2024-03-15.json"
            ),
        }
    )


def test_vintagedates_pagination_collects_every_page(tmp_path: Path) -> None:
    transport = _paged_vintages_transport()
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    vintage_offsets = [
        request.parameters.get("offset")
        for request in transport.requests
        if request.endpoint == "/fred/series/vintagedates"
    ]
    assert vintage_offsets == ["0", "1"]
    vintages = [
        request.parameters.get("realtime_start")
        for request in transport.requests
        if request.endpoint == "/fred/series/observations"
    ]
    assert vintages == ["2024-01-16", "2024-03-15"]
    assert ("T10Y2Y", "2024-03-15") in outcome.watermarks_advanced


def test_truncated_vintagedates_page_fails_closed(tmp_path: Path) -> None:
    transport = ScriptedTransport(
        {
            ("/fred/series", "T10Y2Y", None): response_from_fixture("series_T10Y2Y.json"),
            ("/fred/series/vintagedates", "T10Y2Y", None, "0"): _json_response(
                {
                    "vintage_dates": ["2024-01-16"],
                    "count": 4,
                    "offset": 0,
                    "limit": 1,
                }
            ),
            ("/fred/series/vintagedates", "T10Y2Y", None, "1"): _json_response(
                {"vintage_dates": [], "count": 4, "offset": 1, "limit": 1}
            ),
        }
    )
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.watermarks_advanced == ()
    failed = outcome.series_outcomes[0]
    assert failed.succeeded is False
    assert failed.error_message is not None
    assert "truncated" in failed.error_message


def test_truncated_observations_page_fails_closed(tmp_path: Path) -> None:
    transport = ScriptedTransport(
        {
            ("/fred/series", "T10Y2Y", None): response_from_fixture("series_T10Y2Y.json"),
            ("/fred/series/vintagedates", "T10Y2Y", None): response_from_fixture(
                "vintagedates_T10Y2Y.json"
            ),
            ("/fred/series/observations", "T10Y2Y", "2024-01-16", "0"): _json_response(
                {
                    "observations": [
                        {
                            "realtime_start": "2024-01-16",
                            "realtime_end": "2024-03-14",
                            "date": "2024-01-15",
                            "value": "0.45",
                        }
                    ],
                    "count": 3,
                    "offset": 0,
                    "limit": 1,
                }
            ),
            ("/fred/series/observations", "T10Y2Y", "2024-01-16", "1"): _json_response(
                {"observations": [], "count": 3, "offset": 1, "limit": 1}
            ),
        }
    )
    collector, _plane, _clock = make_collector(
        tmp_path,
        transport,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    assert outcome.watermarks_advanced == ()


def test_https_transport_refuses_cross_host_redirects() -> None:
    handler = HostPinnedRedirectHandler()
    request = Request("https://api.stlouisfed.org/fred/series?series_id=T10Y2Y")
    headers = HTTPMessage()
    headers["Location"] = "https://evil.example/steal"
    with pytest.raises(CollectorError, match="cross-host redirect"):
        handler.redirect_request(
            request,
            BytesIO(),
            302,
            "Found",
            headers,
            "https://evil.example/steal",
        )
    same_host = HTTPMessage()
    same_host["Location"] = "https://api.stlouisfed.org/fred/series/observations"
    redirected = handler.redirect_request(
        request,
        BytesIO(),
        302,
        "Found",
        same_host,
        "https://api.stlouisfed.org/fred/series/observations",
    )
    assert redirected is not None
    assert redirected.host == ALLOWED_HOST


def test_real_005_advances_plan_dataset_and_series_stream(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    registry = CollectionRegistry(clean_postgres)
    collector, _plane, _clock = make_collector(
        tmp_path,
        default_script(),
        control_plane=registry,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    current = registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y")
    assert current is not None
    assert current.dataset == PLAN_DATASET
    assert current.stream == "T10Y2Y"
    assert current.watermark_value == "2024-03-15"
    assert current.run_id == outcome.run_id
    assert registry.latest_watermark(PROVIDER, "series:T10Y2Y", "vintage") is None
    with pytest.raises(CollectionStateError, match="provider/dataset"):
        registry.advance_watermark(
            WatermarkAdvance(
                provider=PROVIDER,
                dataset="series:T10Y2Y",
                stream="vintage",
                run_id=outcome.run_id,
                watermark_value="2024-03-16",
                watermark_position=datetime(2024, 3, 16, tzinfo=UTC),
            )
        )


def test_real_005_mixed_failure_succeeds_and_skips_failed_stream(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    registry = CollectionRegistry(clean_postgres)
    collector, _plane, _clock = make_collector(
        tmp_path,
        default_script(fail_series=frozenset({"T10Y3M"})),
        control_plane=registry,
        config=make_config(tmp_path, series_ids=("T10Y2Y", "T10Y3M")),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    state = registry.current_run_state(outcome.run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_SUCCEEDED
    succeeded = registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y")
    assert succeeded is not None
    assert succeeded.watermark_value == "2024-03-15"
    assert registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y3M") is None
    assert {series_id for series_id, _value in outcome.watermarks_advanced} == {"T10Y2Y"}


def test_real_005_all_failed_run_cannot_advance_watermark(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    registry = CollectionRegistry(clean_postgres)
    collector, _plane, _clock = make_collector(
        tmp_path,
        default_script(fail_series=frozenset({"T10Y2Y"})),
        control_plane=registry,
        config=make_config(tmp_path, series_ids=("T10Y2Y",)),
    )

    outcome = collector.collect()

    assert outcome.terminal_event is RunEventType.RUN_FAILED
    state = registry.current_run_state(outcome.run_id)
    assert state is not None
    assert state.state is RunEventType.RUN_FAILED
    assert registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y") is None
    with pytest.raises(CollectorError, match="run_succeeded"):
        collector.advance_successful_watermarks(
            outcome.run_id,
            (
                SeriesOutcome(
                    series_id="T10Y2Y",
                    succeeded=True,
                    vintage_watermark=date(2024, 3, 15),
                    row_count=1,
                ),
            ),
        )
    with pytest.raises(CollectionStateError, match="run_succeeded"):
        registry.advance_watermark(
            WatermarkAdvance(
                provider=PROVIDER,
                dataset=PLAN_DATASET,
                stream="T10Y2Y",
                run_id=outcome.run_id,
                watermark_value="2024-03-15",
                watermark_position=datetime(2024, 3, 15, tzinfo=UTC),
            )
        )


def test_real_005_second_backfill_skips_unchanged_watermark(
    tmp_path: Path, clean_postgres: Engine
) -> None:
    registry = CollectionRegistry(clean_postgres)
    first, _plane, _clock = make_collector(
        tmp_path,
        default_script(),
        control_plane=registry,
        config=make_config(tmp_path, mode=CollectionMode.BACKFILL, series_ids=("T10Y2Y",)),
    )
    first_outcome = first.collect()

    assert first_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    current = registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y")
    assert current is not None
    assert current.watermark_value == "2024-03-15"

    second, _plane2, _clock2 = make_collector(
        tmp_path / "second",
        default_script(),
        control_plane=registry,
        config=make_config(
            tmp_path / "second",
            mode=CollectionMode.BACKFILL,
            series_ids=("T10Y2Y",),
            receipt_name="second.receipt.json",
        ),
    )
    second_outcome = second.collect()

    assert second_outcome.terminal_event is RunEventType.RUN_SUCCEEDED
    assert second_outcome.watermarks_advanced == ()
    replay = registry.latest_watermark(PROVIDER, PLAN_DATASET, "T10Y2Y")
    assert replay is not None
    assert replay.run_id == first_outcome.run_id
