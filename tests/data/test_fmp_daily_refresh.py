from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data import (
    fmp_collector_http,
    fmp_daily_refresh,
    fmp_deferred_transport,
    fmp_recurring_runtime,
)
from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_daily_refresh import (
    DailyDisposition,
    NorgateUniverseRecord,
    build_daily_refresh_plan,
    classification_document,
    classify_daily_universe,
    collection_manifest_document,
    load_norgate_universe,
    require_backfill_manifest_binding,
)
from aegis_alpha.data.fmp_recurring_authorization import RecurringOperation
from aegis_alpha.data.fmp_windows import (
    ManifestSource,
    UniverseEntry,
    UniverseManifest,
    parse_universe_manifest,
)


def _manifest() -> object:
    return {
        "generated_at_utc": "2026-08-21T00:00:00+00:00",
        "sources": [
            {
                "endpoint": "/stable/actively-trading-list",
                "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                "raw_content_sha256": "1" * 64,
            },
            {
                "endpoint": "/stable/delisted-companies",
                "retrieved_at_utc": "2026-08-21T00:00:01+00:00",
                "raw_content_sha256": "2" * 64,
            },
        ],
        "entries": [
            {"symbol": "known", "ipoDate": "2020-01-02", "delistedDate": None, "active": True},
            {"symbol": "new", "ipoDate": "2026-08-20", "delistedDate": None, "active": True},
            {"symbol": "old-gap", "ipoDate": "2010-01-02", "delistedDate": None, "active": True},
            {"symbol": "recycled", "ipoDate": "2026-08-20", "delistedDate": None, "active": True},
            {
                "symbol": "gone",
                "ipoDate": "2000-01-02",
                "delistedDate": "2010-01-02",
                "active": False,
            },
        ],
    }


def test_daily_universe_classifies_overlap_new_and_unresolved_symbols() -> None:
    manifest = parse_universe_manifest(_manifest())
    result = classify_daily_universe(
        manifest,
        (
            NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),
            NorgateUniverseRecord(assetid=2, symbol="RECYCLED", is_delisted=True),
            NorgateUniverseRecord(assetid=3, symbol="RECYCLED", is_delisted=False),
        ),
        norgate_snapshot_date=date(2026, 7, 28),
    )

    assert result.symbols(DailyDisposition.NORGATE_OVERLAP) == ("known",)
    assert result.symbols(DailyDisposition.NEW_LISTING_CANDIDATE) == ("new",)
    assert result.symbols(DailyDisposition.UNRESOLVED) == ("old-gap", "recycled")
    assert result.ignored_delisted_symbols == ("gone",)
    assert result.collection_manifest.symbols() == ("known", "new")
    document = classification_document(
        result,
        norgate_security_master_sha256="f" * 64,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    assert document["contract"] == "aegis-alpha/fmp-daily-universe-classification"
    assert document["counts"] == {
        "norgate_overlap": 1,
        "new_listing_candidate": 1,
        "unresolved": 2,
        "provider_active_delisted_overlap": 0,
    }
    assert collection_manifest_document(result)["entries"] == [
        {
            "symbol": "known",
            "ipoDate": date(2020, 1, 2),
            "delistedDate": None,
            "active": True,
        },
        {
            "symbol": "new",
            "ipoDate": date(2026, 8, 20),
            "delistedDate": None,
            "active": True,
        },
    ]


def test_daily_universe_classifies_provider_active_delisted_overlap() -> None:
    document = cast("dict[str, object]", _manifest())
    entries = cast("list[dict[str, object]]", document["entries"])
    entries.append(
        {
            "symbol": "known",
            "ipoDate": "2020-01-02",
            "delistedDate": "2026-12-30",
            "active": False,
        }
    )
    manifest = parse_universe_manifest(document)
    result = classify_daily_universe(
        manifest,
        (NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),),
        norgate_snapshot_date=date(2026, 7, 28),
    )

    assert result.symbols(DailyDisposition.PROVIDER_ACTIVE_DELISTED_OVERLAP) == ("known",)
    assert result.ignored_delisted_symbols == ("gone", "known")
    assert "known" not in result.collection_manifest.symbols()
    document = classification_document(
        result,
        norgate_security_master_sha256="f" * 64,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    assert cast("dict[str, object]", document["counts"])["provider_active_delisted_overlap"] == 1


def test_daily_plan_is_repeatable_per_service_day_and_separates_operations(
    tmp_path: Path,
) -> None:
    first = build_daily_refresh_plan(
        schedule_id="fmp-daily-refresh-v1",
        service_day=date(2026, 8, 21),
        output_root=tmp_path,
        authority_payload_sha256="a" * 64,
    )
    replay = build_daily_refresh_plan(
        schedule_id="fmp-daily-refresh-v1",
        service_day=date(2026, 8, 21),
        output_root=tmp_path,
        authority_payload_sha256="a" * 64,
    )

    assert first == replay
    assert first.universe_run_id != first.collection_run_id
    assert first.universe_path.parent == first.classification_path.parent
    assert first.receipt_path.parent == first.universe_path.parent
    assert first.universe_run_id.startswith("fmp-run-")
    assert first.collection_run_id.startswith("fmp-run-")


@pytest.mark.parametrize(
    ("schedule_id", "digest", "message"),
    [
        ("", "a" * 64, "schedule_id"),
        ("fmp-daily-refresh-v1", "not-a-digest", "digest"),
    ],
)
def test_daily_plan_rejects_invalid_signed_identity(
    tmp_path: Path,
    schedule_id: str,
    digest: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_daily_refresh_plan(
            schedule_id=schedule_id,
            service_day=date(2026, 8, 21),
            output_root=tmp_path,
            authority_payload_sha256=digest,
        )


def test_norgate_universe_loader_binds_exact_parquet_bytes_and_rows(tmp_path: Path) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1, 2], type=pa.int64()),
                "symbol": pa.array(["KNOWN", "GONE"], type=pa.string()),
                "is_delisted": pa.array([False, True], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    assert load_norgate_universe(
        path,
        expected_sha256=digest,
        expected_row_count=2,
    ) == (
        NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),
        NorgateUniverseRecord(assetid=2, symbol="GONE", is_delisted=True),
    )
    with pytest.raises(ValueError, match="digest"):
        load_norgate_universe(
            path,
            expected_sha256="0" * 64,
            expected_row_count=2,
        )


def test_norgate_universe_loader_rejects_null_identity_fields(tmp_path: Path) -> None:
    path = tmp_path / "invalid-security-master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array([None], type=pa.string()),
                "is_delisted": pa.array([False], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="invalid universe fields"):
        load_norgate_universe(path, expected_sha256=digest, expected_row_count=1)


def test_backfill_manifest_must_equal_the_collection_derived_from_signed_norgate(
    tmp_path: Path,
) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array(["KNOWN"], type=pa.string()),
                "is_delisted": pa.array([False], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    supplied = parse_universe_manifest(_manifest())
    bound = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "1" * 64,
                }
            ],
            "entries": [
                {
                    "symbol": "known",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": True,
                }
            ],
        }
    )

    require_backfill_manifest_binding(
        bound,
        norgate_security_master=path,
        norgate_security_master_sha256=digest,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            supplied,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )


def test_backfill_manifest_rejects_missing_signed_master_symbol(
    tmp_path: Path,
) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1, 2], type=pa.int64()),
                "symbol": pa.array(["KNOWN", "OLD"], type=pa.string()),
                "is_delisted": pa.array([False, True], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    incomplete = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "1" * 64,
                }
            ],
            "entries": [
                {
                    "symbol": "known",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": True,
                }
            ],
        }
    )

    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            incomplete,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=2,
            norgate_snapshot_date=date(2026, 7, 28),
        )


def test_backfill_manifest_rejects_untrusted_post_snapshot_listing(
    tmp_path: Path,
) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array(["KNOWN"], type=pa.string()),
                "is_delisted": pa.array([False], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    supplied = parse_universe_manifest(_manifest())
    classified = classify_daily_universe(
        supplied,
        (NorgateUniverseRecord(assetid=1, symbol="KNOWN", is_delisted=False),),
        norgate_snapshot_date=date(2026, 7, 28),
    )

    assert "new" in classified.collection_manifest.symbols()
    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            classified.collection_manifest,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )


def test_backfill_manifest_accepts_signed_delisted_symbol(
    tmp_path: Path,
) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array(["GONE"], type=pa.string()),
                "is_delisted": pa.array([True], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/delisted-companies",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "2" * 64,
                }
            ],
            "entries": [
                {
                    "symbol": "gone",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": False,
                }
            ],
        }
    )

    require_backfill_manifest_binding(
        manifest,
        norgate_security_master=path,
        norgate_security_master_sha256=digest,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )


@pytest.mark.parametrize(
    ("record_status", "manifest_status"),
    [
        ("active", "inactive"),
        ("delisted", "active"),
    ],
)
def test_backfill_manifest_rejects_status_not_bound_to_signed_master(
    tmp_path: Path,
    record_status: str,
    manifest_status: str,
) -> None:
    is_delisted = record_status == "delisted"
    active = manifest_status == "active"
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array(["KNOWN"], type=pa.string()),
                "is_delisted": pa.array([is_delisted], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "1" * 64,
                }
            ],
            "entries": [
                {
                    "symbol": "known",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": active,
                }
            ],
        }
    )

    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            manifest,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ipoDate", "2026-08-20"),
        ("delistedDate", "2026-08-20"),
    ],
)
def test_backfill_manifest_rejects_unauthenticated_listing_dates(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    path = tmp_path / "security_master.parquet"
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array([1], type=pa.int64()),
                "symbol": pa.array(["KNOWN"], type=pa.string()),
                "is_delisted": pa.array([False], type=pa.bool_()),
            }
        ),
        path,
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    entry: dict[str, object] = {
        "symbol": "known",
        "ipoDate": None,
        "delistedDate": None,
        "active": True,
    }
    entry[field] = value
    manifest = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "1" * 64,
                }
            ],
            "entries": [entry],
        }
    )

    with pytest.raises(ValueError, match="listing dates"):
        require_backfill_manifest_binding(
            manifest,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )


def _typed_manifest(*entries: UniverseEntry) -> UniverseManifest:
    return UniverseManifest(
        generated_at_utc=datetime(2026, 8, 21, tzinfo=UTC),
        sources=(
            ManifestSource(
                endpoint="/stable/actively-trading-list",
                retrieved_at_utc=datetime(2026, 8, 21, tzinfo=UTC),
                raw_content_sha256="1" * 64,
            ),
        ),
        entries=entries,
    )


def _write_signed_master(
    path: Path,
    *,
    symbols: tuple[str, ...],
    delisted: tuple[bool, ...],
) -> str:
    pq.write_table(
        pa.table(
            {
                "assetid": pa.array(list(range(1, len(symbols) + 1)), type=pa.int64()),
                "symbol": pa.array(list(symbols), type=pa.string()),
                "is_delisted": pa.array(list(delisted), type=pa.bool_()),
            }
        ),
        path,
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "symbol",
    [
        " known ",
        "KNOWN",
        "Known",
        "known\t",
    ],
)
def test_backfill_manifest_rejects_noncanonical_symbols_before_master_or_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    symbol: str,
) -> None:
    path = tmp_path / "security_master.parquet"
    digest = _write_signed_master(path, symbols=("KNOWN",), delisted=(False,))
    loaded = False

    def load_master(*_args: object, **_kwargs: object) -> object:
        nonlocal loaded
        loaded = True
        raise AssertionError("signed master must not be read for a non-canonical symbol")

    def construct_transport(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("provider transport must not be constructed")

    monkeypatch.setattr(fmp_daily_refresh, "load_norgate_universe", load_master)
    monkeypatch.setattr(fmp_collector_http, "make_fmp_https_transport", construct_transport)
    monkeypatch.setattr(fmp_deferred_transport, "make_fmp_https_transport", construct_transport)
    manifest = _typed_manifest(
        UniverseEntry(symbol=symbol, ipo_date=None, delisted_date=None, active=True)
    )

    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            manifest,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )
    assert loaded is False


def test_backfill_manifest_rejects_normalized_symbol_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "security_master.parquet"
    digest = _write_signed_master(path, symbols=("KNOWN",), delisted=(False,))
    loaded = False

    def load_master(*_args: object, **_kwargs: object) -> object:
        nonlocal loaded
        loaded = True
        raise AssertionError("signed master must not be read for a duplicate symbol")

    monkeypatch.setattr(fmp_daily_refresh, "load_norgate_universe", load_master)
    manifest = _typed_manifest(
        UniverseEntry(symbol="known", ipo_date=None, delisted_date=None, active=True),
        UniverseEntry(symbol="known", ipo_date=None, delisted_date=None, active=True),
    )

    with pytest.raises(ValueError, match="signed Norgate"):
        require_backfill_manifest_binding(
            manifest,
            norgate_security_master=path,
            norgate_security_master_sha256=digest,
            norgate_security_master_row_count=1,
            norgate_snapshot_date=date(2026, 7, 28),
        )
    assert loaded is False


def test_backfill_manifest_accepts_exact_complete_signed_universe(tmp_path: Path) -> None:
    path = tmp_path / "security_master.parquet"
    digest = _write_signed_master(
        path,
        symbols=("KNOWN", "GONE"),
        delisted=(False, True),
    )
    manifest = parse_universe_manifest(
        {
            "generated_at_utc": "2026-08-21T00:00:00+00:00",
            "sources": [
                {
                    "endpoint": "/stable/actively-trading-list",
                    "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                    "raw_content_sha256": "1" * 64,
                },
                {
                    "endpoint": "/stable/delisted-companies",
                    "retrieved_at_utc": "2026-08-21T00:00:01+00:00",
                    "raw_content_sha256": "2" * 64,
                },
            ],
            "entries": [
                {
                    "symbol": "known",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": True,
                },
                {
                    "symbol": "gone",
                    "ipoDate": None,
                    "delistedDate": None,
                    "active": False,
                },
            ],
        }
    )

    require_backfill_manifest_binding(
        manifest,
        norgate_security_master=path,
        norgate_security_master_sha256=digest,
        norgate_security_master_row_count=2,
        norgate_snapshot_date=date(2026, 7, 28),
    )


def test_padded_backfill_symbol_is_rejected_before_usage_or_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_document = {
        "generated_at_utc": "2026-08-21T00:00:00+00:00",
        "sources": [
            {
                "endpoint": "/stable/actively-trading-list",
                "retrieved_at_utc": "2026-08-21T00:00:00+00:00",
                "raw_content_sha256": "1" * 64,
            }
        ],
        "entries": [
            {
                "symbol": " known ",
                "ipoDate": None,
                "delistedDate": None,
                "active": True,
            }
        ],
    }
    authority = SimpleNamespace(
        norgate_security_master=tmp_path / "security-master.parquet",
        norgate_security_master_sha256="a" * 64,
        norgate_security_master_row_count=1,
        norgate_snapshot_date=date(2026, 7, 28),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "load_recurring_authority",
        lambda *_: (b"{}", b"sig"),
    )
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "recurring_authority_issued_at",
        lambda _: datetime(2026, 8, 21, tzinfo=UTC),
    )
    monkeypatch.setattr(fmp_recurring_runtime, "load_owner_approval_authority", lambda *_: object())
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "verify_recurring_authority",
        lambda *_args, **_kwargs: authority,
    )
    monkeypatch.setattr(fmp_recurring_runtime, "load_fmp_policy", lambda _: object())
    monkeypatch.setattr(fmp_recurring_runtime, "load_notification_artifact", lambda _: object())
    monkeypatch.setattr(
        fmp_recurring_runtime,
        "_read_json",
        lambda label, _path: (
            (
                {
                    "calls_per_minute": 3000,
                    "calls_per_day": 5,
                    "bandwidth_gb_30d": 150,
                },
                "b" * 64,
            )
            if label == "tier"
            else (manifest_document, "c" * 64)
        ),
    )
    usage_loaded = False

    def load_usage(**_kwargs: object) -> object:
        nonlocal usage_loaded
        usage_loaded = True
        raise AssertionError("usage and transport setup must not run")

    def construct_transport(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("provider transport must not be constructed")

    monkeypatch.setattr(fmp_recurring_runtime, "load_trusted_fmp_usage_snapshot", load_usage)
    monkeypatch.setattr(fmp_collector_http, "make_fmp_https_transport", construct_transport)
    monkeypatch.setattr(fmp_deferred_transport, "make_fmp_https_transport", construct_transport)
    operation = RecurringOperation(
        command="collect",
        service_day=date(2026, 8, 21),
        output_path=tmp_path / "output" / "receipt.json",
        manifest_path=tmp_path / "output" / "manifest.json",
        mode=CollectionMode.BACKFILL,
    )

    with pytest.raises(PreconditionError, match="signed Norgate"):
        fmp_recurring_runtime.authorize_recurring_operation(
            operation=operation,
            recurring_authority_path=tmp_path / "standing.json",
            recurring_signature_path=tmp_path / "standing.sig",
            registry_path=tmp_path / "registry.json",
            notification_path=tmp_path / "notification.json",
            tier_path=tmp_path / "tier.json",
            environment={"AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH": str(tmp_path / "owner.json")},
            moment=datetime(2026, 8, 21, tzinfo=UTC),
            approval_clock=lambda: datetime(2026, 8, 21, tzinfo=UTC),
            allow_historical_service_day=True,
        )

    assert usage_loaded is False
