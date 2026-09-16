"""Pinned storage histories, explicit research mode and total coverage on synthetic stores."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import publication
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_research_inputs import (
    _change,
    _hash_json,
    _proxy_spec,
    _register,
    _register_domain,
    _sessions_spec,
    _source_row,
    _spec,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from aegis_alpha.storage.market_inputs import GenerationPin, PriceInputRequest
    from aegis_alpha.storage.workspace import Workspace

DAY = date(2026, 1, 2)
BUDGET = ComputeBudget(Fraction(1), 64 * 1024 * 1024)


def api() -> ModuleType:
    name = "aegis_alpha.storage.market_inputs"
    assert importlib.util.find_spec(name) is not None, (
        "pinned revision-bearing research reader is unavailable"
    )
    return importlib.import_module(name)


def prices(workspace: Workspace, root: Path, rows: list[dict[str, object]], version: str) -> None:
    path = _spec(workspace, root / f"source{version}.sqlite3", rows)
    _change(
        path,
        "calendar",
        {"calendar_id": "CAL", "timezone": "UTC", "timezone_version": "synthetic-v1"},
    )
    _change(path, "price", {key: rows[0][key] for key in ("basis", "currency", "price_role")})
    _change(
        path,
        "dataset",
        {
            "dataset_id": "synthetic-prices",
            "version": version,
            "generation_id": "g" + version,
            "operation_id": "op" + version,
            "parent_id": None if version == "1" else "g1",
        },
    )
    _register(workspace, path)


@pytest.fixture
def stored(tmp_path: Path) -> Iterator[Workspace]:
    # Given an actual admitted installation and retained source transforms.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        prices(workspace, tmp_path, [_source_row()], "1")
        path = _sessions_spec(
            workspace,
            tmp_path / "sessions.sqlite3",
            {
                "session_date": DAY.isoformat(),
                "available_at_us": 20,
                "revision_known_at_us": 20,
            },
        )
        _register_domain(workspace, path, "sessions")
        yield workspace


def pin(
    workspace: Workspace, dataset: str = "synthetic-prices", version: str = "1"
) -> GenerationPin:
    row = publication.read_dataset(workspace, dataset, version)
    return api().GenerationPin(
        **{
            key: row[key]
            for key in (
                "dataset_id",
                "version",
                "generation_id",
                "chain_hash",
                "manifest_hash",
            )
        }
    )


def request(workspace: Workspace, **changes: object) -> PriceInputRequest:
    return api().PriceInputRequest(
        **{
            "pin": pin(workspace),
            "sessions_pin": pin(workspace, "sessions"),
            "instrument_ids": ("ASSET_A",),
            "session_dates": (DAY,),
            "currency": "USD",
            "basis": "unadjusted",
            "price_role": "canonical",
            "calendar_id": "CAL",
            "venue": "SYNTHETIC",
            "timezone_version": "synthetic-v1",
            **changes,
        }
    )


def test_complete_chain_projects_original_and_corrected_without_future_leak(
    stored: Workspace,
    tmp_path: Path,
) -> None:
    # Given an old immutable pin and a correction known only at40.
    old = api().load_pinned_prices(stored, request(stored), budget=BUDGET)
    original = old.project_as_of(30, session_date=DAY)
    prices(
        stored,
        tmp_path,
        [
            {
                **_source_row(),
                "revision_id": "r2",
                "op": "SUPERSEDE",
                "supersedes_revision_id": "r1",
                "close": "11.125",
                "available_at_us": 40,
                "revision_known_at_us": 40,
                "ingested_at_us": 50,
            }
        ],
        "2",
    )
    # When the newer pin is prepared, each decision projects the full chain.
    new = api().load_pinned_prices(
        stored, request(stored, pin=pin(stored, version="2")), budget=BUDGET
    )
    # Then independent literal values and complete revision columns remain available.
    expected_revisions = 2
    assert len(new.history) == expected_revisions
    assert new.project_as_of(30, session_date=DAY) == original
    assert new.project_as_of(40, session_date=DAY).rows[0]["close"] == Decimal("11.125")
    assert old.project_as_of(40, session_date=DAY).rows[0]["close"] == Decimal(11)
    assert new.history[1]["supersedes_revision_id"] == "r1"
    assert new.history[0]["price_role"] == "canonical"
    with pytest.raises(TypeError):
        new.history[0]["close"] = Decimal(999)
    with pytest.raises(TypeError):
        original.rows[0]["close"] = Decimal(999)


def test_reference_snapshot_is_explicit_uncertified_and_economically_bounded(
    tmp_path: Path,
) -> None:
    # Given a fixed adjusted snapshot with unknown knowledge.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        row = {
            **_source_row(),
            "basis": "adjusted",
            "price_role": "reference",
            "available_at_us": None,
            "revision_known_at_us": None,
        }
        # Recompute the source natural identity for the deliberately changed basis/role.
        row["record_id"] = _hash_json(
            [
                "aas-record-v1",
                "prices",
                [
                    [key, row[key]]
                    for key in (
                        "instrument_id",
                        "session_date",
                        "interval",
                        "bar_end_us",
                        "basis",
                        "currency",
                        "price_role",
                    )
                ],
            ]
        )
        prices(workspace, tmp_path, [row], "1")
        values = {
            "pin": pin(workspace),
            "sessions_pin": None,
            "instrument_ids": ("ASSET_A",),
            "session_dates": (DAY,),
            "currency": "USD",
            "basis": "adjusted",
            "price_role": "reference",
            "calendar_id": "CAL",
            "venue": "SYNTHETIC",
            "timezone_version": "synthetic-v1",
        }
        strict = api().load_pinned_prices(
            workspace, api().PriceInputRequest(**values), budget=BUDGET
        )
        observed = api().load_pinned_prices(
            workspace,
            api().PriceInputRequest(**values, mode="observed_snapshot_research"),
            budget=BUDGET,
        )
    # When deciding from the detached snapshot, Then strict never silently becomes research.
    assert strict.project_as_of(30, session_date=DAY).rows == ()
    result = observed.project_as_of(30, session_date=DAY)
    assert result.rows[0]["close"] == Decimal(11)
    assert result.coverage.certified is False
    assert {"observed_snapshot_research", "unknown_price_evidence"} <= set(result.coverage.reasons)
    assert observed.project_as_of(30, session_date=date(2026, 1, 1)).rows == ()
    assert observed.project_as_of(19, session_date=DAY).rows == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("currency", "EUR"),
        ("basis", "adjusted"),
        ("price_role", "reference"),
        ("calendar_id", "OTHER"),
        ("venue", "OTHER"),
        ("timezone_version", "OTHER"),
        ("instrument_ids", ("ASSET_A", "ASSET_A")),
        ("instrument_ids", ("UNKNOWN",)),
        ("mode", "latest"),
    ],
)
def test_incompatible_request_fails_closed(stored: Workspace, field: str, value: object) -> None:
    # Given an incompatible convention/identity, When loading, Then fail rather than substitute.
    with pytest.raises(
        ValueError, match=r"currency|basis|role|calendar|venue|timezone|identit|mode"
    ):
        api().load_pinned_prices(stored, request(stored, **{field: value}), budget=BUDGET)


@pytest.mark.parametrize(
    "field", ["chain_hash", "manifest_hash", "generation_id", "version", "dataset_id"]
)
def test_wrong_exact_pin_fails(stored: Workspace, field: str) -> None:
    # Given an exact pin with one substituted field, When loading, Then it is rejected.
    values = asdict(pin(stored))
    values[field] = "f" * 64
    with pytest.raises(ValueError, match=r"generation|pin"):
        api().load_pinned_prices(
            stored, request(stored, pin=api().GenerationPin(**values)), budget=BUDGET
        )


def test_total_coverage_includes_missing_session_and_sell_open(stored: Workspace) -> None:
    # Given an explicit requested grid including a date absent from the stored calendar.
    series = api().load_pinned_prices(
        stored, request(stored, session_dates=(DAY, date(2026, 1, 3))), budget=BUDGET
    )
    # When projecting, Then coverage accounts for both cells, never a shortened successful history.
    result = series.project_as_of(30, session_date=date(2026, 1, 3))
    expected_cells = 2
    assert result.coverage.expected_count == expected_cells
    assert result.coverage.present_count == 1
    assert result.coverage.complete is False
    assert result.coverage.cells[1].reasons == (
        "missing_session",
        "missing_price",
        "missing_sell_open",
    )
    assert {"identity_unpinned", "universe_unpinned", "catalog_unverified"} <= set(
        result.coverage.reasons
    )


def test_unknown_evidence_and_missing_ohlcv_remain_explicit(tmp_path: Path) -> None:
    # Given missing OHLCV and no knowledge timestamps, When projected, Then no price is invented.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        row = {
            **_source_row(),
            **dict.fromkeys(("open", "high", "low", "close", "volume")),
            "value_state": "missing",
            "available_at_us": None,
            "revision_known_at_us": None,
        }
        prices(workspace, tmp_path, [row], "1")
        series = api().load_pinned_prices(
            workspace,
            api().PriceInputRequest(
                pin=pin(workspace),
                sessions_pin=None,
                instrument_ids=("ASSET_A",),
                session_dates=(DAY,),
                currency="USD",
                basis="unadjusted",
                price_role="canonical",
                calendar_id="CAL",
                venue="SYNTHETIC",
                timezone_version="synthetic-v1",
            ),
            budget=BUDGET,
        )
    result = series.project_as_of(30, session_date=DAY)
    assert result.rows == ()
    assert {"unknown_price_evidence", "missing_sell_open"} <= set(result.coverage.cells[0].reasons)


def test_combined_inputs_exceeding_memory_budget_fail(stored: Workspace) -> None:
    # Given insufficient budget for both admitted histories, When loading, Then never truncate.
    with pytest.raises(ComputeResourceError):
        api().load_pinned_prices(
            stored, request(stored), budget=ComputeBudget(Fraction(1), 2 * 1024 * 1024)
        )


def test_sessions_and_proxy_pins_read_in_a_fresh_process(tmp_path: Path) -> None:
    # Given retained sessions and non-executable proxy points; source files are not runtime inputs.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as stored:
        path = _sessions_spec(
            stored,
            tmp_path / "sessions.sqlite3",
            {
                "available_at_us": 20,
                "revision_known_at_us": 20,
            },
        )
        _register_domain(stored, path, "sessions")
        path = _proxy_spec(stored, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        _register_domain(stored, path, "proxy")
        session_pin = asdict(pin(stored, "sessions"))
        proxy_pin = asdict(pin(stored, "proxy"))
    for path in tmp_path.glob("*.sqlite3"):
        path.unlink()
    code = """
import json, sys
from pathlib import Path
from fractions import Fraction
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.workspace import open_workspace
from aegis_alpha.storage.market_inputs import GenerationPin, load_pinned_sessions, load_pinned_proxy
with open_workspace(Path(sys.argv[1])) as w:
    budget = ComputeBudget(Fraction(1), 64*1024*1024)
    sessions = load_pinned_sessions(w, GenerationPin(**json.loads(sys.argv[2])), budget=budget)
    proxy = load_pinned_proxy(w, GenerationPin(**json.loads(sys.argv[3])), budget=budget)
    row = sessions.project_as_of(30).rows[0]
    result = proxy.project_as_of(30, mode='observed_snapshot_research')
    print(json.dumps([row['open_at_us'], row['close_at_us'], row['timezone_version'],
        result.rows[0]['value'].hex(), proxy.non_executable, result.coverage.certified,
        len(proxy.project_as_of(19, mode='observed_snapshot_research').rows),
        len(proxy.project_as_of(30).rows)]))
"""
    # When a bounded fresh interpreter opens the independent installation.
    result = subprocess.run(  # noqa: S603 -- fixed offline script, bounded and reaped
        [
            sys.executable,
            "-c",
            code,
            str(tmp_path / "home"),
            json.dumps(session_pin),
            json.dumps(proxy_pin),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    # Then literal session times and binary64 value survive, without PIT or execution certification.
    assert json.loads(result.stdout) == [
        10,
        20,
        "synthetic-v1",
        "0x1.999999999999ap-4",
        True,
        False,
        0,
        0,
    ]


@pytest.mark.parametrize("domain", ["sessions", "proxy"])
def test_coverage_of_invisible_rows_is_not_empty_success(
    stored: Workspace, tmp_path: Path, domain: str
) -> None:
    # Given a hidden point, When projected, Then retain its coverage cell.
    if domain == "proxy":
        path = _proxy_spec(stored, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        _register_domain(stored, path, "proxy")
        result = (
            api().load_pinned_proxy(stored, pin(stored, "proxy"), budget=BUDGET).project_as_of(30)
        )
    else:
        result = (
            api()
            .load_pinned_sessions(stored, pin(stored, "sessions"), budget=BUDGET)
            .project_as_of(19)
        )
    assert result.rows == ()
    assert result.coverage.expected_count == 1
    assert result.coverage.present_count == 0
    assert result.coverage.complete is False


def test_transform_calendar_is_checked_without_sessions_pin(stored: Workspace) -> None:
    # Given CAL, When requesting OTHER without sessions, Then still reject the mismatch.
    with pytest.raises(ValueError, match="calendar"):
        api().load_pinned_prices(
            stored, request(stored, sessions_pin=None, calendar_id="OTHER"), budget=BUDGET
        )


def mixed_proxy_publications(workspace: Workspace, root: Path) -> tuple[GenerationPin, ...]:
    """Publish real versioned source/spec fixtures under one parent-linked dataset."""
    pins = []
    for version, value in (("1", "0.1"), ("2", "125.5")):
        path = _proxy_spec(
            workspace, root / f"proxy{version}.sqlite3", ("PROXY", "v" + version, value)
        )
        _change(
            path,
            "dataset",
            {
                "dataset_id": "proxy",
                "version": version,
                "generation_id": "proxy" + version,
                "operation_id": "op-proxy" + version,
                "parent_id": None if version == "1" else "proxy1",
            },
        )
        _register_domain(workspace, path, "proxy")
        pins.append(pin(workspace, "proxy", version))
    return tuple(pins)


def test_mixed_proxy_history_rejects_specialized_read_without_changing_old_pin(
    tmp_path: Path,
) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        old_pin, mixed_pin = mixed_proxy_publications(workspace, tmp_path)
        old = api().load_pinned_proxy(workspace, old_pin, budget=BUDGET)
        assert [row["contract_version"] for row in old.history] == ["v1"]
        assert old.history[0]["value"].hex() == "0x1.999999999999ap-4"
        assert old.non_executable is True
        with pytest.raises(ValueError, match=r"proxy.*conflict"):
            api().load_pinned_proxy(workspace, mixed_pin, budget=BUDGET)
        assert api().load_pinned_proxy(workspace, old_pin, budget=BUDGET) == old


def test_native_proxy_registrar_rejects_empty_source_publication(tmp_path: Path) -> None:
    from aegis_alpha.storage import source_library  # noqa: PLC0415

    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        empty = tmp_path / "empty.sqlite3"
        empty.write_bytes((tmp_path / "proxy.sqlite3").read_bytes())
        empty.chmod(0o600)
        with closing(sqlite3.connect(empty)) as connection:
            connection.execute("DELETE FROM bars")
            connection.commit()
        digest = hashlib.sha256(empty.read_bytes()).hexdigest()
        source_library.import_sqlite(workspace, empty, "empty", digest)
        table = source_library.list_tables(workspace, "empty")[0]
        _change(
            path,
            "source",
            {
                "source_id": "empty",
                "source_sha256": digest,
                "table": "bars",
                "table_digest": table["digest"],
            },
        )
        with pytest.raises(ValueError, match="nonempty array"):
            _register_domain(workspace, path, "proxy")
        assert workspace.state.execute("SELECT * FROM feature_contracts").fetchall() == []
        assert workspace.market.execute("SELECT * FROM market_generations").fetchall() == []


def test_proxy_referenced_publication_keeps_its_transform_linkage(tmp_path: Path) -> None:
    from aegis_alpha.data.serialization import canonical_json_bytes  # noqa: PLC0415
    from aegis_alpha.storage.import_document import parse_import  # noqa: PLC0415
    from aegis_alpha.storage.verification import verify_workspace  # noqa: PLC0415

    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
        _register_domain(workspace, path, "proxy")
        origin = pin(workspace, "proxy")
        raw = workspace.paths.raw / origin.manifest_hash[:2] / origin.manifest_hash
        body = json.loads(raw.read_bytes())
        body.update(dataset_id="proxy-copy", generation_id="proxy-copy", operation_id="op-copy")
        body["rows"][0]["revision_id"] = "copy-r1"
        publication.publish_document(workspace, parse_import(canonical_json_bytes(body)))
        exact = pin(workspace, "proxy-copy")
        assert api().load_pinned_proxy(workspace, exact, budget=BUDGET).non_executable is True
        workspace.state.execute("DROP TRIGGER immutable_dataset_versions_update")
        workspace.state.execute(
            "UPDATE dataset_versions SET transform_hash=? WHERE generation_id='proxy-copy'",
            ("0" * 64,),
        )
        with pytest.raises(ValueError, match="transform"):
            api().load_pinned_proxy(workspace, exact, budget=BUDGET)
        with pytest.raises(ValueError, match="transform"):
            verify_workspace(workspace)


def test_proxy_contract_inputs_are_verified(stored: Workspace, tmp_path: Path) -> None:
    # Given a physical metadata corruption outside immutable SQL admission.
    path = _proxy_spec(stored, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
    _register_domain(stored, path, "proxy")
    stored.state.execute("DROP TRIGGER immutable_feature_inputs_update")
    stored.state.execute(
        "UPDATE feature_inputs SET content_hash=? WHERE ref_kind='transform'", ("0" * 64,)
    )
    # When resolving a proxy, Then point hashes cannot hide corrupt input contracts.
    with pytest.raises(ValueError, match="input"):
        api().load_pinned_proxy(stored, pin(stored, "proxy"), budget=BUDGET)


def test_retained_tombstone_removes_head_and_records_reason(
    stored: Workspace, tmp_path: Path
) -> None:
    # Given a future tombstone with explicit same-record ancestry.
    prices(
        stored,
        tmp_path,
        [
            {
                **_source_row(),
                "revision_id": "r2",
                "op": "TOMBSTONE",
                "supersedes_revision_id": "r1",
                "available_at_us": 40,
                "revision_known_at_us": 40,
                "ingested_at_us": 50,
            }
        ],
        "2",
    )
    series = api().load_pinned_prices(
        stored, request(stored, pin=pin(stored, version="2")), budget=BUDGET
    )
    # When replayed at40, Then the deleted record is not a silently missing success.
    assert series.project_as_of(30, session_date=DAY).rows[0]["close"] == Decimal(11)
    result = series.project_as_of(40, session_date=DAY)
    assert result.rows == ()
    assert "tombstone" in result.coverage.cells[0].reasons


@pytest.mark.parametrize("kind", ["identity", "universe"])
def test_identity_and_universe_temporal_pins_filter_without_latest(
    stored: Workspace, kind: str
) -> None:
    # Given native immutable projected memberships, not today's ticker membership.
    from tests.storage.test_membership_pins import candidate  # noqa: PLC0415

    changes = {kind + "_pin": candidate(stored, kind)}
    reason = "identity_unavailable" if kind == "identity" else "outside_universe"
    series = api().load_pinned_prices(stored, request(stored, **changes), budget=BUDGET)
    # When crossing the exclusive knowledge end, Then the prepared membership governs.
    assert series.project_as_of(30, session_date=DAY).rows[0]["close"] == Decimal(11)
    result = series.project_as_of(35, session_date=DAY)
    assert result.rows == ()
    assert reason in result.coverage.cells[0].reasons


@pytest.mark.parametrize("kind", ["identity", "universe"])
def test_same_membership_pin_rejects_legal_insert(stored: Workspace, kind: str) -> None:
    from tests.storage.test_membership_pins import candidate, legal_insert  # noqa: PLC0415

    exact = candidate(stored, kind)
    req = request(stored, **{kind + "_pin": exact})
    detached = api().load_pinned_prices(stored, req, budget=BUDGET)
    assert detached.project_as_of(30, session_date=DAY).rows[0]["close"] == Decimal(11)
    assert detached.project_as_of(40, session_date=DAY).rows == ()
    legal_insert(stored, kind)
    stored.state.commit()
    assert stored.state.execute("PRAGMA foreign_key_check").fetchall() == []
    assert detached.project_as_of(40, session_date=DAY).rows == ()
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        api().load_pinned_prices(stored, req, budget=BUDGET)


def test_ancestor_catalog_is_checked_against_its_own_delta(
    stored: Workspace, tmp_path: Path
) -> None:
    # Given a valid head over a physically corrupted ancestor catalog.
    prices(
        stored,
        tmp_path,
        [
            {
                **_source_row(),
                "revision_id": "r2",
                "op": "SUPERSEDE",
                "supersedes_revision_id": "r1",
                "close": "11.125",
                "available_at_us": 40,
                "revision_known_at_us": 40,
                "ingested_at_us": 50,
            }
        ],
        "2",
    )
    req = request(stored, pin=pin(stored, version="2"))
    stored.state.execute("DROP TRIGGER immutable_dataset_versions_update")
    stored.state.execute("UPDATE dataset_versions SET row_count=7 WHERE generation_id='g1'")
    # When the newer pin loads, Then checking only the head cannot hide the ancestor mismatch.
    with pytest.raises(ValueError, match="catalog"):
        api().load_pinned_prices(stored, req, budget=BUDGET)


def test_duplicate_daily_identity_is_rejected(tmp_path: Path) -> None:
    # Given two bars for a daily slot, When loading, Then no arbitrary first row wins.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        second = {**_source_row(), "bar_end_us": 21}
        second["record_id"] = _hash_json(
            [
                "aas-record-v1",
                "prices",
                [
                    [key, second[key]]
                    for key in (
                        "instrument_id",
                        "session_date",
                        "interval",
                        "bar_end_us",
                        "basis",
                        "currency",
                        "price_role",
                    )
                ],
            ]
        )
        prices(workspace, tmp_path, [_source_row(), second], "1")
        with pytest.raises(ValueError, match="duplicate"):
            api().load_pinned_prices(
                workspace,
                api().PriceInputRequest(
                    pin=pin(workspace),
                    sessions_pin=None,
                    instrument_ids=("ASSET_A",),
                    session_dates=(DAY,),
                    currency="USD",
                    basis="unadjusted",
                    price_role="canonical",
                    calendar_id="CAL",
                    venue="SYNTHETIC",
                    timezone_version="synthetic-v1",
                ),
                budget=BUDGET,
            )


def test_more_than_legacy_limit_is_complete(tmp_path: Path) -> None:
    # Given101 distinct economic dates, When prepared, Then no legacy100-row cap remains.
    initialize(tmp_path / "home")
    expected_count = 101
    days = tuple(DAY + timedelta(days=offset) for offset in range(expected_count))
    rows = []
    for day in days:
        row = {**_source_row(), "session_date": day.isoformat()}
        row["record_id"] = _hash_json(
            [
                "aas-record-v1",
                "prices",
                [
                    [key, row[key]]
                    for key in (
                        "instrument_id",
                        "session_date",
                        "interval",
                        "bar_end_us",
                        "basis",
                        "currency",
                        "price_role",
                    )
                ],
            ]
        )
        rows.append(row)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        prices(workspace, tmp_path, rows, "1")
        series = api().load_pinned_prices(
            workspace,
            api().PriceInputRequest(
                pin=pin(workspace),
                sessions_pin=None,
                instrument_ids=("ASSET_A",),
                session_dates=days,
                currency="USD",
                basis="unadjusted",
                price_role="canonical",
                calendar_id="CAL",
                venue="SYNTHETIC",
                timezone_version="synthetic-v1",
            ),
            budget=BUDGET,
        )
    result = series.project_as_of(30, session_date=days[-1])
    assert len(series.history) == expected_count
    assert len(result.rows) == expected_count
    assert result.coverage.expected_count == result.coverage.present_count == expected_count


def future_proxy_spec(stored: Workspace, root: Path) -> Path:
    original = _proxy_spec(stored, root / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
    body = json.loads(original.read_bytes())
    with closing(sqlite3.connect(root / "proxy.sqlite3")) as connection:
        connection.row_factory = sqlite3.Row
        row = dict(connection.execute("SELECT * FROM bars").fetchone())
    future = {**row, "feature_at_us": 80, "value": "999"}
    future["record_id"] = _hash_json(
        [
            "aas-record-v1",
            "feature_values",
            [
                [key, future[key]]
                for key in (
                    "contract_id",
                    "contract_version",
                    "input_bundle_hash",
                    "instrument_id",
                    "feature_at_us",
                )
            ],
        ]
    )
    points = _spec(stored, root / "points.sqlite3", [row, future])
    body["source"] = json.loads(points.read_bytes())["source"]
    original.write_text(json.dumps(body))
    return original


def test_future_proxy_prefix_and_coverage_are_independently_identifiable(
    stored: Workspace, tmp_path: Path
) -> None:
    # Given future999, When reading30, Then no future calibration occurs.
    path = future_proxy_spec(stored, tmp_path)
    with closing(sqlite3.connect(tmp_path / "points.sqlite3")) as connection:
        expected_record = connection.execute(
            "SELECT record_id FROM bars WHERE feature_at_us=80"
        ).fetchone()[0]
    _register_domain(stored, path, "proxy")
    proxy = api().load_pinned_proxy(stored, pin(stored, "proxy"), budget=BUDGET)
    result = proxy.project_as_of(30, mode="observed_snapshot_research")
    assert [row["value"].hex() for row in result.rows] == ["0x1.999999999999ap-4"]
    expected_cells = 2
    assert result.coverage.expected_count == expected_cells
    assert result.coverage.present_count == 1
    assert result.coverage.cells[1].reasons == ("future_observation",)
    assert result.coverage.cells[1].record_id == expected_record


def test_unknown_session_evidence_is_preserved_in_price_coverage(tmp_path: Path) -> None:
    # Given unknown calendar knowledge, When reading strict prices, Then report that reason.
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        prices(workspace, tmp_path, [_source_row()], "1")
        path = _sessions_spec(
            workspace, tmp_path / "sessions.sqlite3", {"session_date": DAY.isoformat()}
        )
        _register_domain(workspace, path, "sessions")
        result = (
            api()
            .load_pinned_prices(workspace, request(workspace), budget=BUDGET)
            .project_as_of(30, session_date=DAY)
        )
    assert {"missing_session", "unknown_session_evidence"} <= set(result.coverage.cells[0].reasons)


def test_oversized_proxy_contract_is_rejected_before_materialization(
    stored: Workspace, tmp_path: Path
) -> None:
    # Given physical metadata corruption much larger than its reserved JSON budget.
    path = _proxy_spec(stored, tmp_path / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
    _register_domain(stored, path, "proxy")
    stored.state.execute("DROP TRIGGER immutable_feature_contracts_update")
    stored.state.execute("UPDATE feature_contracts SET definition=?", ("x" * 100000,))
    # When preparing, Then capacity admission precedes fetching corrupt bytes.
    with pytest.raises(ComputeResourceError):
        api().load_pinned_proxy(
            stored, pin(stored, "proxy"), budget=ComputeBudget(Fraction(1), 8 * 1024 * 1024)
        )


@pytest.mark.parametrize("cutoff", [-1, True, "30"])
def test_malformed_decision_cannot_bypass_mode(stored: Workspace, cutoff: object) -> None:
    # Given a non-UTC-microsecond decision, When projecting, Then reject it rather than coerce.
    series = api().load_pinned_prices(stored, request(stored), budget=BUDGET)
    with pytest.raises(ValueError, match="decision"):
        series.project_as_of(cutoff, session_date=DAY)


@pytest.mark.parametrize("kind", ["identity", "universe"])
def test_wrong_membership_pin_fails(stored: Workspace, kind: str) -> None:
    # Given absent snapshots, When loading, Then never substitute current IDs.
    value = (
        api().IdentityPin("absent", "a" * 64)
        if kind == "identity"
        else api().UniversePin("absent", "1", "b" * 64)
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        api().load_pinned_prices(stored, request(stored, **{kind + "_pin": value}), budget=BUDGET)
