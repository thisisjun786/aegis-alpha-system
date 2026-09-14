"""Synthetic native-store preparation through the public Python boundary."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
from collections.abc import MutableMapping
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import UTC, date, datetime, timedelta
from decimal import localcontext
from fractions import Fraction
from pathlib import Path
from types import FrameType
from typing import Any, cast

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import (
    CALCULATION_MODULES,
    PreparedBacktest,
    PrepareRequest,
    calculation_identity,
    environment_identity,
    parse_prepare_request,
    prepare_backtest,
)
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.descriptor_tree import DescriptorTreeError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.errors import BlockReason, ReplayBlockedError
from aegis_alpha.engine.models import (
    DerivedInputBinding,
    DerivedSeriesSpec,
    FeatureMatrixSpec,
    MacroSignalSpec,
)
from aegis_alpha.storage import publication
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.input_pins import register_convention, register_definition
from aegis_alpha.storage.market_inputs import (
    PinnedPriceSeries,
    PriceInputRequest,
    load_pinned_prices,
)
from aegis_alpha.storage.market_schema import NATURAL_KEYS
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    UniversePin,
    read_membership_pins,
    register_identity_snapshot,
    register_universe_version,
)
from aegis_alpha.storage.research_inputs import (
    register_price_input,
    register_proxy_input,
    register_sessions_input,
)
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle
from tests.engine.test_backtest_request import add_ref, fixture
from tests.storage.test_research_inputs import _source_row, _spec, registration_state

J = "aas-canonical-json-sha256-v1"
BUDGET = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
DAYS = tuple(
    date.fromisoformat(value)
    for value in (
        "2025-11-28",
        "2025-12-30",
        "2026-01-29",
        "2026-02-02",
        "2026-02-26",
        "2026-03-02",
        "2026-03-30",
        "2026-04-01",
    )
)
type Document = dict[str, Any]


def micros(day: date, hour: int = 16) -> int:
    return int(datetime(day.year, day.month, day.day, hour, tzinfo=UTC).timestamp()) * 1_000_000


@pytest.fixture(autouse=True)
def publication_clock(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    # Explicit ingestion events, never elapsed runtime or microsecond timing luck.
    clock = [micros(date(2026, 6, 1)) * 1000]
    monkeypatch.setattr(time, "time_ns", lambda: clock[0])
    return clock


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def prepare(workspace: Workspace, body: Document) -> PreparedBacktest:
    return prepare_backtest(
        workspace, PrepareRequest(parse_prepare_request(canonical_json_bytes(body))), budget=BUDGET
    )


def native(
    workspace: Workspace,
    root: Path,
    name: str,
    rows: list[Document],
    *,
    destination: Document | None = None,
) -> Document:
    rows = [{**row, "revision_id": name + ":" + row["revision_id"]} for row in rows]
    path = _spec(workspace, root / (name + ".sqlite3"), rows)
    body = json.loads(path.read_bytes())
    body["dataset"] = destination or {
        "dataset_id": name,
        "version": "1",
        "generation_id": name,
        "operation_id": "op-" + name,
        "parent_id": None,
    }
    body["instruments"] = [
        {"instrument_id": key, "asset_type": "etf", "venue": "SYN"}
        for key in sorted({str(row["instrument_id"]) for row in rows if "instrument_id" in row})
    ]
    if "calendar_id" in rows[0]:
        for key in ("price", "decimal_conversion"):
            del body[key]
        body["schema_version"] = "aas-sessions-transform-v1"
        body["calendar"] = {
            "calendar_id": "synthetic-calendar",
            "venue": "SYN",
            "timezone": "UTC",
            "timezone_version": "synthetic-utc-1",
        }
        register = register_sessions_input
    else:
        body["calendar"]["timezone_version"] = "synthetic-utc-1"
        body["price"] = {key: rows[0][key] for key in ("basis", "currency", "price_role")}
        register = register_price_input
    raw = canonical_json_bytes(body)
    path.write_bytes(raw)
    register(workspace, path, hashlib.sha256(raw).hexdigest())
    return body


def row_identity(row: Document, domain: str) -> Document:
    row["record_id"] = digest(
        ["aas-record-v1", domain, [[key, row[key]] for key in NATURAL_KEYS[domain]]]
    )
    row["revision_id"] = "r-" + row["record_id"]
    return row


def price_rows(*, signal: bool) -> list[Document]:
    rows = []
    for symbol, values in {
        "ASSET_A": (10, 12, 15, 15, 12, 12, 12, 12),
        "ASSET_B": (10, 10, 11, 11, 20, 20, 20, 20),
        "REF_X": (10, 10, 10, 10, 10, 10, 10, 10),
    }.items():
        for day, value in zip(DAYS, values, strict=True):
            row = {
                **_source_row(),
                "instrument_id": symbol,
                "session_date": day.isoformat(),
                "bar_end_us": micros(day),
                "ingested_at_us": micros(DAYS[-1]),
                "available_at_us": None if signal else micros(day),
                "revision_known_at_us": None if signal else micros(day),
                "basis": "split_adjusted" if signal else "unadjusted",
                "price_role": "reference" if signal else "canonical",
                **dict.fromkeys(("open", "high", "low", "close"), str(value)),
            }
            rows.append(row_identity(row, "prices"))
    return rows


def session_rows() -> list[Document]:
    common = {
        key: value
        for key, value in _source_row().items()
        if key
        in {
            "generation_id",
            "record_id",
            "revision_id",
            "supersedes_revision_id",
            "op",
            "available_at_us",
            "revision_known_at_us",
            "ingested_at_us",
            "source_snapshot_id",
            "source_row_hash",
        }
    }
    return [
        row_identity(
            {
                **common,
                "calendar_id": "synthetic-calendar",
                "venue": "SYN",
                "session_date": day.isoformat(),
                "open_at_us": micros(day, 9) if day in DAYS else None,
                "close_at_us": micros(day) if day in DAYS else None,
                "status": "open" if day in DAYS else "closed",
                "timezone_version": "synthetic-utc-1",
                "available_at_us": 1,
                "revision_known_at_us": 1,
            },
            "calendar_sessions",
        )
        for day in (DAYS[0] + timedelta(days=i) for i in range((DAYS[-1] - DAYS[0]).days + 1))
    ]


def memberships(workspace: Workspace) -> tuple[Document, Document]:
    source = dict(
        workspace.state.execute(
            "SELECT * FROM source_snapshots WHERE snapshot_id IN "
            "(SELECT source_snapshot_id FROM dataset_sources WHERE dataset_id='sessions')"
        ).fetchone()
    )
    source["files"] = [
        dict(row)
        for row in workspace.state.execute(
            "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=?",
            (source["snapshot_id"],),
        )
    ]
    instruments = [
        {"instrument_id": key, "issuer_id": None, "asset_type": "etf", "venue": "SYN"}
        for key in ("ASSET_A", "ASSET_B", "REF_X")
    ]
    interval = {"valid_from_us": 0, "valid_to_us": None, "known_from_us": 0, "known_to_us": None}
    identity = {
        "schema": "aas-identity-snapshot-v1",
        "hash_format": J,
        "snapshot_id": "ids",
        "instruments": instruments,
        "sources": [source],
        "assertions": [
            {
                "assertion_id": key,
                "instrument_id": key,
                "provider": "synthetic",
                "namespace": "opaque",
                "token": key,
                "valid_from_us": 0,
                "valid_to_us": None,
                "known_from_us": 0,
                "supersedes_assertion_id": None,
                "source_snapshot_id": source["snapshot_id"],
                "source_hash": source["files"][0]["byte_hash"],
            }
            for key in ("ASSET_A", "ASSET_B", "REF_X")
        ],
        "members": [
            {"ordinal": i, "assertion_id": item["instrument_id"], **interval}
            for i, item in enumerate(instruments)
        ],
    }
    universe = {
        "schema": "aas-universe-version-v1",
        "hash_format": J,
        "universe_id": "universe",
        "version": "1",
        "instruments": instruments,
        "sources": [source],
        "members": [
            {
                "instrument_id": item["instrument_id"],
                **interval,
                "source_snapshot_id": source["snapshot_id"],
            }
            for item in instruments
        ],
    }
    raw = canonical_json_bytes(identity)
    ip = register_identity_snapshot(
        workspace.state, raw, expected_file_sha256=digest(identity), created_at_us=1
    )
    raw = canonical_json_bytes(universe)
    up = register_universe_version(workspace.state, raw, expected_file_sha256=digest(universe))
    return asdict(ip), asdict(up)


def stored_request(
    workspace: Workspace, root: Path, *, signal_known_at_us: int | None = None
) -> Document:
    body, _, conventions = fixture()
    raw = raw_bundle(contract())
    path = root / "strategy.json"
    path.write_bytes(raw)
    registered = register_strategy(
        workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "1"
    )
    assert workspace.strategies is not None
    body["strategy"].update(
        strategy_store_id=workspace.strategies.execute(
            "SELECT store_id FROM store_info"
        ).fetchone()[0],
        raw_sha256=registered["raw_sha256"],
        contract_sha256=registered["contract_sha256"],
    )
    signal_rows = price_rows(signal=True)
    if signal_known_at_us is not None:
        for row in signal_rows:
            if row["session_date"] <= "2026-01-29":
                row.update(
                    available_at_us=signal_known_at_us, revision_known_at_us=signal_known_at_us
                )
    native(workspace, root, "signal", signal_rows)
    native(workspace, root, "outcomes", price_rows(signal=False))
    native(workspace, root, "sessions", session_rows())
    ip, up = memberships(workspace)
    member = {
        "schema": "aas-ensemble-membership-v1",
        "hash_format": J,
        "id": "synthetic-membership",
        "version": "1",
        "membership_sha256": contract().ensemble_membership_reference.partition(":")[2],
        "rows": [{"name": "synthetic-choice", "weight": "1"}],
    }
    mp = register_definition(
        workspace, canonical_json_bytes(member), expected_file_sha256=digest(member), budget=BUDGET
    )
    pins = {"identity": ip, "universe": up, "membership": asdict(mp)}
    for raw in conventions:
        pin = register_convention(
            workspace.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
        pins[pin.kind] = asdict(pin)
    for role, dataset in (
        ("signal_prices", "signal"),
        ("execution_prices", "outcomes"),
        ("sessions", "sessions"),
    ):
        record = publication.read_dataset(workspace, dataset, "1")
        pins[role] = {
            key: record[key]
            for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
        }
    for binding in body["bindings"]:
        ref = next(ref for ref in body["refs"] if ref["ref_id"] == binding["ref_id"])
        pin = pins[binding["role"]]
        ref["pin"] = pin
        ref["ref_id"] = pin.get(
            "dataset_id", pin.get("snapshot_id", pin.get("universe_id", pin.get("id")))
        )
        ref["hash"] = pin.get("chain_hash", pin.get("content_hash", pin.get("hash")))
        binding.update(ref_id=ref["ref_id"], hash=ref["hash"])
    body["period"] = {"start": DAYS[2].isoformat(), "end": DAYS[6].isoformat()}
    body["history"] = {"start": DAYS[0].isoformat(), "end": DAYS[4].isoformat()}
    body["explicit_decision_dates"] = None
    body["cutoff"]["knowledge_cutoff_us"] = micros(DAYS[-1])
    return body


@pytest.fixture(scope="module")
def stored_template(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, bytes]:
    root = tmp_path_factory.mktemp("stored-template")
    # Module fixtures precede the function-scoped clock. Seed at the same explicit
    # ingestion event without depending on which test first requests the template.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
        _ = initialize(root / "home")
        with open_workspace(root / "home", writable=True, strategy_write=True) as workspace:
            body = stored_request(workspace, root)
            workspace.state.commit()
            assert workspace.strategies is not None
            workspace.strategies.commit()
            _ = workspace.market.execute("CHECKPOINT")
    # All source-import and workspace handles are closed before any ordinary copy.
    assert not any(path.name.endswith(("-wal", "-shm", ".wal")) for path in root.rglob("*"))
    return root, canonical_json_bytes(body)


def copy_request(template: tuple[Path, bytes], root: Path) -> Document:
    source, raw = template
    # Include incoming transforms used by proxy_recipe as well as the entire home.
    # copy2 preserves private modes; copytree creates new regular files, not links.
    _ = shutil.copytree(source, root, dirs_exist_ok=True)
    return cast("Document", json.loads(raw))


@pytest.fixture
def copied_request(stored_template: tuple[Path, bytes], tmp_path: Path) -> Document:
    return copy_request(stored_template, tmp_path)


def fixture_files(root: Path) -> dict[Path, bytes]:
    return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def test_copied_requests_isolate_native_integrity(
    stored_template: tuple[Path, bytes], copied_request: Document, tmp_path: Path
) -> None:
    template, raw = stored_template
    second = tmp_path / "second"
    second_body = copy_request(stored_template, second)
    original = fixture_files(template)
    for relative, content in original.items():
        paths = (template / relative, tmp_path / relative, second / relative)
        infos = [path.lstat() for path in paths]
        assert all(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 for info in infos)
        assert len({(info.st_dev, info.st_ino) for info in infos}) == len(paths)
        assert len({stat.S_IMODE(info.st_mode) for info in infos}) == 1
        assert all(path.read_bytes() == content for path in paths)
    for directory in (path for path in template.rglob("*") if path.is_dir()):
        relative = directory.relative_to(template)
        assert (tmp_path / relative).stat().st_mode == directory.stat().st_mode
        assert (second / relative).stat().st_mode == directory.stat().st_mode
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        transform_hash = str(
            workspace.state.execute(
                "SELECT transform_hash FROM dataset_versions WHERE dataset_id='signal'"
            ).fetchone()[0]
        )
        retained = workspace.paths.raw / transform_hash[:2] / transform_hash
        _ = retained.write_bytes(retained.read_bytes() + b" ")
        with pytest.raises(ValueError, match="pinned transform hash mismatch"):
            _ = prepare(workspace, copied_request)
    copied_request["account"]["initial_cash"] = 999
    assert canonical_json_bytes(second_body) == raw
    assert fixture_files(template) == fixture_files(second) == original
    for root, body in ((template, cast("Document", json.loads(raw))), (second, second_body)):
        # Writable SQLite handles remove their empty WAL/SHM on close; read-only
        # handles leave those sidecars behind even though preparation is SELECT-only.
        with open_workspace(root / "home", writable=True, strategy_write=True) as workspace:
            prepared = prepare(workspace, body)
        assert prepared.targets == {DAYS[2]: {"ASSET_A": 1.0}, DAYS[4]: {"ASSET_B": 1.0}}
        result = cast(
            "Document",
            run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
        )
        assert result["result"]["nav"][-1]["equity"] == pytest.approx(80)
    assert fixture_files(template) == fixture_files(second) == original


@pytest.mark.parametrize("history_end", [date(2026, 2, 26), date(2026, 4, 1)])
def test_short_preparation_reuses_long_calendar_with_bounded_price_coverage(
    tmp_path: Path, copied_request: Document, monkeypatch: pytest.MonkeyPatch, history_end: date
) -> None:
    captured: list[PriceInputRequest] = []

    def inspect_request(
        workspace: Workspace, request: PriceInputRequest, *, budget: ComputeBudget
    ) -> PinnedPriceSeries:
        captured.append(request)
        return load_pinned_prices(workspace, request, budget=budget)

    monkeypatch.setattr(
        "aegis_alpha.application.backtest_prepare.load_pinned_prices", inspect_request
    )
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["history"]["end"] = history_end.isoformat()
        baseline = prepare(workspace, body)
        captured.clear()
        rows = session_rows()
        closed = next(row for row in rows if row["status"] == "closed")
        first = date(2024, 11, 28)
        rows.extend(
            row_identity(
                {**closed, "session_date": (first + timedelta(days=i)).isoformat()},
                "calendar_sessions",
            )
            for i in range((DAYS[0] - first).days)
        )
        native(workspace, tmp_path, "long-sessions", rows)
        replace_pin(body, "sessions", generation_pin(workspace, "long-sessions"))
        prepared = prepare(workspace, body)
        assert prepared.slots == baseline.slots
        assert prepared.targets == baseline.targets
        assert (
            run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)[
                "result"
            ]
            == run_document(baseline.envelope.canonical_bytes, baseline.envelope.envelope_sha256)[
                "result"
            ]
        )
        assert prepared.request_hash != baseline.request_hash
        assert len(captured) == len(body["price_inputs"])
        expected_end = date(2026, 3, 30) if history_end == date(2026, 2, 26) else date(2026, 4, 1)
        expected = tuple(
            date(2025, 11, 28) + timedelta(days=i)
            for i in range((expected_end - date(2025, 11, 28)).days + 1)
        )
        assert all(request.session_dates == expected for request in captured)
        assert date(2026, 1, 30) in expected  # Preserve declared closed-day coverage.


def test_native_preparation_literal_targets_and_repeatability(tmp_path: Path) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        request = PrepareRequest(parse_prepare_request(canonical_json_bytes(body)))
        prepared = prepare_backtest(workspace, request, budget=BUDGET)
        assert dict(prepared.targets) == {DAYS[2]: {"ASSET_A": 1.0}, DAYS[4]: {"ASSET_B": 1.0}}
        assert prepared == prepare_backtest(workspace, request, budget=BUDGET)
        assert tuple(slot.decision_date for slot in prepared.slots) == (DAYS[2], DAYS[4])
        assert tuple(slot.execution_date for slot in prepared.slots) == (DAYS[3], DAYS[5])
        assert {
            key: value.returns[2] for key, value in prepared.features[DAYS[2]].items()
        } == pytest.approx({"ASSET_A": 0.5, "ASSET_B": 0.1, "REF_X": 0.0})
        assert {
            key: value.returns[2] for key, value in prepared.features[DAYS[4]].items()
        } == pytest.approx({"ASSET_A": 0.0, "ASSET_B": 1.0, "REF_X": 0.0})
        assert [dict(item.master_switch) for item in prepared.decisions] == [
            {"synthetic-choice": False},
            {"synthetic-choice": False},
        ]
        assert [dict(item.per_strategy["synthetic-choice"]) for item in prepared.decisions] == [
            {"ASSET_A": 1.0},
            {"ASSET_B": 1.0},
        ]
        assert prepared.definition.executable is False
        assert prepared.definition.unresolved_convention_roles == (
            "calendar",
            "basis",
            "cost",
            "execution",
        )
        assert prepared.certified is False
        expected_sources = 3
        assert len(prepared.inputs.source_pins) == expected_sources
        envelope = json.loads(prepared.envelope.canonical_bytes)
        assert envelope["dates"] == [day.isoformat() for day in DAYS[2:7]]
        assert "CASH_X" not in envelope["instrument_types"]
        assert (
            prepared.request_hash == hashlib.sha256(prepared.projection.canonical_bytes).hexdigest()
        )
        assert (
            prepared.envelope.envelope_sha256
            == hashlib.sha256(prepared.envelope.canonical_bytes).hexdigest()
        )
        outcome = cast(
            "Document",
            run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
        )
        assert outcome["result"]["nav"][-1]["equity"] == pytest.approx(80)
        assert [
            (row["decision_date"], row["execution_date"], row["symbol"], row["shares"])
            for row in outcome["result"]["fills"]
        ] == [
            ("2026-01-29", "2026-02-02", "ASSET_A", 100 / 15),
            ("2026-02-26", "2026-03-02", "ASSET_A", -100 / 15),
            ("2026-02-26", "2026-03-02", "ASSET_B", 4.0),
        ]


@pytest.mark.parametrize("latency_hours", [24, 72])
def test_admitted_publication_after_decision_midnight(
    tmp_path: Path, latency_hours: int, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["decision_latency_us"] = latency_hours * 60 * 60 * 1_000_000
        body["explicit_decision_dates"] = ["2026-01-29"]
        cutoff = micros(DAYS[2]) + body["decision_latency_us"]
        known = cutoff - 7 * 60 * 60 * 1_000_000
        rows = price_rows(signal=True)
        for row in rows:
            if row["session_date"] == "2026-01-29":
                row.update(available_at_us=known, revision_known_at_us=known)
        alternate_prices(workspace, tmp_path, body, "signal_prices", rows)
        before = registration_state(workspace)
        assert workspace.strategies is not None
        connections = (workspace.state, workspace.strategies)
        traces: list[str] = []
        for connection in connections:
            connection.set_authorizer(select_only)
            connection.set_trace_callback(traces.append)
        sys.setprofile(reject_accounting)
        try:
            prepared = prepare(workspace, body)
            assert prepared == prepare(workspace, body)
        finally:
            sys.setprofile(None)
            for connection in connections:
                connection.set_authorizer(None)
                connection.set_trace_callback(None)
        assert registration_state(workspace) == before
        assert traces
        assert all(sql.lstrip().upper().startswith(("SELECT", "WITH")) for sql in traces)
        assert prepared.targets == {date(2026, 1, 29): {"ASSET_A": 1.0}}
        assert [
            (slot.decision_date, slot.execution_date, slot.cutoff_us) for slot in prepared.slots
        ] == [(date(2026, 1, 29), date(2026, 2, 2), cutoff)]
        assert [receipt.as_of for receipt in prepared.decisions] == [date(2026, 1, 29)]
        assert {
            key: row.returns[2] for key, row in prepared.features[DAYS[2]].items()
        } == pytest.approx({"ASSET_A": 0.5, "ASSET_B": 0.1, "REF_X": 0.0})
        assert all(row.as_of == date(2026, 1, 29) for row in prepared.features[DAYS[2]].values())
    result = cast(
        "Document",
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    )
    assert result["result"]["fills"] == [
        {
            "decision_date": "2026-01-29",
            "execution_date": "2026-02-02",
            "symbol": "ASSET_A",
            "shares": 100 / 15,
            "price": 15.0,
            "fee": 0.0,
        }
    ]
    assert result["result"]["nav"][-1]["equity"] == pytest.approx(80)


@pytest.mark.parametrize("field", ["available_at_us", "revision_known_at_us"])
@pytest.mark.parametrize("offset_us", [-1, 0, 1])
def test_exact_publication_cutoff_precedes_date_level_engine_guard(
    tmp_path: Path, field: str, offset_us: int, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["decision_latency_us"] = 24 * 60 * 60 * 1_000_000
        body["explicit_decision_dates"] = ["2026-01-29"]
        cutoff = micros(date(2026, 1, 30))
        rows = price_rows(signal=True)
        for row in rows:
            if row["session_date"] == "2026-01-29":
                row.update(available_at_us=cutoff - 1, revision_known_at_us=cutoff - 1)
                row[field] = cutoff + offset_us
        alternate_prices(workspace, tmp_path, body, "signal_prices", rows)
        before = registration_state(workspace)
        if offset_us > 0:
            with pytest.raises(ValueError, match="insufficient eligible buckets"):
                prepare(workspace, body)
        else:
            prepared = prepare(workspace, body)
            assert prepared.targets == {DAYS[2]: {"ASSET_A": 1.0}}
            assert prepared.features[DAYS[2]]["ASSET_A"].returns[2] == pytest.approx(0.5)
        assert registration_state(workspace) == before


@pytest.mark.parametrize("latency_hours", [24, 72])
def test_late_native_price_and_macro_derived_preserve_prior_month_signals(
    tmp_path: Path, latency_hours: int
) -> None:
    known = micros(DAYS[2]) + (latency_hours - 7) * 60 * 60 * 1_000_000
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path, signal_known_at_us=known)
        second_recipe(workspace, tmp_path, body, macro_known_at_us=known)
        body["decision_latency_us"] = latency_hours * 60 * 60 * 1_000_000
        body["explicit_decision_dates"] = ["2026-01-29"]
        before = registration_state(workspace)
        prepared = prepare(workspace, body)
        assert registration_state(workspace) == before
        assert prepared.targets == {DAYS[2]: {}}
        assert prepared.decisions[0].as_of == DAYS[2]
        assert prepared.decisions[0].signals["synthetic-choice"] == {"MACRO": True, "YIELD": True}
        assert prepared.decisions[0].ensemble == {"CASH_X": 1.0}
        # December FLOW/ASSET_A = 0.6/12 = 0.05; January would be 3/15 = 0.2.
        assert {
            key: row.ma_ratios[2] for key, row in prepared.features[DAYS[2]].items()
        } == pytest.approx({"ASSET_A": 10 / 9, "ASSET_B": 22 / 21, "REF_X": 1.0})
    result = cast(
        "Document",
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    )
    assert result["result"]["fills"] == []
    assert result["result"]["nav"][-1]["equity"] == pytest.approx(100)


def test_latency_ages_price_evidence_instead_of_refreshing_it(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        value = contract()
        value = replace(value, stale_gates=replace(value.stale_gates, price_stale_after_days=1))
        doc = json.loads(raw_bundle(value))
        doc["bundle_version"] = "stale"
        raw = canonical_json_bytes(doc)
        path = tmp_path / "stale-strategy.json"
        path.write_bytes(raw)
        registered = register_strategy(
            workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "stale"
        )
        body["strategy"].update(
            version="stale",
            raw_sha256=registered["raw_sha256"],
            contract_sha256=registered["contract_sha256"],
        )
        body["explicit_decision_dates"] = ["2026-01-29"]
        assert prepare(workspace, body).targets == {DAYS[2]: {"ASSET_A": 1.0}}
        body["decision_latency_us"] = 48 * 60 * 60 * 1_000_000
        before = registration_state(workspace)
        with pytest.raises(ReplayBlockedError) as blocked:
            prepare(workspace, body)
        assert blocked.value.reason == BlockReason.STALE_PRICE
        assert registration_state(workspace) == before


def replace_pin(body: Document, role: str, pin: Document, ordinal: int = 0) -> None:
    binding = next(
        item for item in body["bindings"] if (item["role"], item["ordinal"]) == (role, ordinal)
    )
    ref = next(
        item
        for item in body["refs"]
        if (item["ref_kind"], item["ref_id"], item["ref_version"])
        == (binding["ref_kind"], binding["ref_id"], binding["ref_version"])
    )
    ref["pin"] = pin
    ref["ref_id"] = pin.get(
        "dataset_id", pin.get("snapshot_id", pin.get("universe_id", pin.get("id")))
    )
    ref["ref_version"] = pin.get("version", ref["ref_version"])
    ref["hash"] = pin.get("chain_hash", pin.get("content_hash", pin.get("hash")))
    binding.update(ref_id=ref["ref_id"], ref_version=ref["ref_version"], hash=ref["hash"])


def generation_pin(workspace: Workspace, dataset: str, version: str = "1") -> Document:
    record = publication.read_dataset(workspace, dataset, version)
    return {
        key: record[key]
        for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
    }


def alternate_prices(
    workspace: Workspace, root: Path, body: Document, role: str, rows: list[Document]
) -> None:
    name = "changed-" + role
    native(workspace, root, name, rows)
    replace_pin(body, role, generation_pin(workspace, name))


def macro_generation(workspace: Workspace, *, known_at_us: int | None = None) -> None:
    common = {
        key: value
        for key, value in session_rows()[0].items()
        if key
        in {
            "revision_id",
            "supersedes_revision_id",
            "op",
            "available_at_us",
            "revision_known_at_us",
            "ingested_at_us",
        }
    }
    rows = [
        {
            **common,
            "series_id": series,
            "observation_period": day,
            "unit": "ratio",
            "source_vintage_start": None,
            "source_vintage_end": None,
            "value": value,
            "value_state": "present",
            "available_at_us": known_at_us
            if known_at_us is not None and day == "2025-12-31"
            else micros(date.fromisoformat(day)),
            "revision_known_at_us": known_at_us
            if known_at_us is not None and day == "2025-12-31"
            else micros(date.fromisoformat(day)),
            "ingested_at_us": micros(DAYS[-1]),
            "revision_id": series + day,
        }
        for series, values in (("MACRO", ("0", "2")), ("FLOW", ("0.6", "3")))
        for day, value in zip(("2025-12-31", "2026-01-31"), values, strict=True)
    ]
    raw = canonical_json_bytes(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": "macro",
            "version": "1",
            "generation_id": "macro",
            "operation_id": "op-macro",
            "parent_id": None,
            "domain": "macro_observations",
            "provider": "synthetic",
            "publication_at_us": None,
            "normalizer_version": "synthetic-v1",
            "transform_sha256": hashlib.sha256(b"synthetic macro declaration").hexdigest(),
            "instruments": [],
            "rows": rows,
        }
    )
    publication.publish_document(workspace, parse_import(raw))


def second_recipe(
    workspace: Workspace, root: Path, body: Document, *, macro_known_at_us: int | None = None
) -> None:
    macro_generation(workspace, known_at_us=macro_known_at_us)
    derived = DerivedSeriesSpec(
        series_id="YIELD",
        operation="trailing_sum_over_price",
        trailing_months=1,
        consumes_capital=True,
        consumes_totalreturn=False,
        input_bindings=(
            DerivedInputBinding("signal", "1", "ASSET_A", "price"),
            DerivedInputBinding("macro", "1", "FLOW", "addend_a"),
        ),
        signal_lag_months=(0,),
        signal_thresholds=(0.1,),
        reference_provenance="synthetic cashflows",
    )
    value = contract()
    strategy = replace(
        value.pack[0],
        offensive_config={
            **value.pack[0].offensive_config,
            "strategy_type": "relative_absolute_cash",
            "top_n": 2,
            "scoring": {"method": "moving_average", "horizon": 2},
        },
    )
    value = replace(
        value,
        pack=(strategy,),
        feature_matrix=FeatureMatrixSpec(
            momentum_scores=(),
            moving_average_months=(2,),
            ma_window_includes_current_month=True,
            return_months=(2,),
            includes_latest_price=True,
        ),
        macro_signals=(
            MacroSignalSpec("MACRO", (0,), "EXACT", "LT", (1.0,)),
            MacroSignalSpec("YIELD", (0,), "EXACT", "LT", (0.1,)),
        ),
        derived_series=(derived,),
    )
    raw = raw_bundle(value)
    document = json.loads(raw)
    document["bundle_version"] = "2"
    raw = canonical_json_bytes(document)
    path = root / "second-strategy.json"
    path.write_bytes(raw)
    registered = register_strategy(
        workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "2"
    )
    body["strategy"].update(
        version="2",
        raw_sha256=registered["raw_sha256"],
        contract_sha256=registered["contract_sha256"],
    )
    key = add_ref(body, "macro", "macro")
    replace_pin(body, "macro", generation_pin(workspace, "macro"))
    body["macro_inputs"] = [{"binding": key, "series_id": "MACRO", "unit": "ratio"}]
    signal = next(ref for ref in body["refs"] if ref["ref_id"] == "signal")
    macro = next(ref for ref in body["refs"] if ref["ref_id"] == "macro")
    doc = {
        "schema": "aas-derived-definition-v1",
        "hash_format": J,
        "id": "YIELD",
        "version": "1",
        "definition": derived,
        "inputs": [
            {"ordinal": 0, "field": "price", "pin": signal},
            {"ordinal": 1, "field": "addend_a", "pin": macro},
        ],
    }
    pin = register_definition(
        workspace, canonical_json_bytes(doc), expected_file_sha256=digest(doc), budget=BUDGET
    )
    key = add_ref(body, "derived", "YIELD")
    replace_pin(body, "derived", asdict(pin))
    body["derived_inputs"] = [{"binding": key, "series_id": "YIELD"}]


def test_independent_macro_derived_recipe_switches_and_residual_cash(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        second_recipe(workspace, tmp_path, body)
        prepared = prepare(workspace, body)
        assert prepared.targets == {DAYS[2]: {}, DAYS[4]: {"ASSET_B": 0.5}}
        assert {
            key: value.ma_ratios[2] for key, value in prepared.features[DAYS[2]].items()
        } == pytest.approx({"ASSET_A": 10 / 9, "ASSET_B": 22 / 21, "REF_X": 1.0})
        assert {
            key: value.ma_ratios[2] for key, value in prepared.features[DAYS[4]].items()
        } == pytest.approx({"ASSET_A": 8 / 9, "ASSET_B": 40 / 31, "REF_X": 1.0})
        assert [dict(item.signals["synthetic-choice"]) for item in prepared.decisions] == [
            {"MACRO": True, "YIELD": True},
            {"MACRO": False, "YIELD": False},
        ]
        assert [dict(item.master_switch) for item in prepared.decisions] == [
            {"synthetic-choice": True},
            {"synthetic-choice": False},
        ]
        assert [dict(item.ensemble) for item in prepared.decisions] == [
            {"CASH_X": 1.0},
            {"ASSET_B": 0.5, "CASH_X": 0.5},
        ]
        assert [item.as_of for item in prepared.decisions] == [DAYS[2], DAYS[4]]
        result = cast(
            "Document",
            run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
        )
        assert result["result"]["fills"] == [
            {
                "decision_date": "2026-02-26",
                "execution_date": "2026-03-02",
                "symbol": "ASSET_B",
                "shares": 2.5,
                "price": 20.0,
                "fee": 0.0,
            }
        ]
        assert result["result"]["nav"][-1] == {
            "date": "2026-03-30",
            "equity": 100.0,
            "cash": 50.0,
            "fee": 0.0,
        }


def reject_accounting(frame: FrameType, event: str, argument: object) -> None:
    del argument
    if event == "call" and frame.f_globals.get("__name__") == "aegis_alpha.engine.execution":
        raise AssertionError("preparation called accounting")


def select_only(
    action: int, first: str | None, second: str | None, database: str | None, trigger: str | None
) -> int:
    del first, second, database, trigger
    return (
        sqlite3.SQLITE_OK
        if action
        in {
            sqlite3.SQLITE_SELECT,
            sqlite3.SQLITE_READ,
            sqlite3.SQLITE_FUNCTION,
            sqlite3.SQLITE_RECURSIVE,
        }
        else sqlite3.SQLITE_DENY
    )


def test_select_only_no_accounting_and_deeply_detached(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        before = registration_state(workspace)
        assert workspace.strategies is not None
        strategies = tuple(workspace.strategies.iterdump())
        traces: list[str] = []
        connections = (workspace.state, workspace.strategies)
        for connection in connections:
            connection.set_authorizer(select_only)
            connection.set_trace_callback(traces.append)
        sys.setprofile(reject_accounting)
        try:
            prepared = prepare(workspace, body)
        finally:
            sys.setprofile(None)
            for connection in connections:
                connection.set_authorizer(None)
                connection.set_trace_callback(None)
        assert traces
        assert all(
            statement.lstrip().upper().startswith(("SELECT", "WITH")) for statement in traces
        )
        assert registration_state(workspace) == before
        assert tuple(workspace.strategies.iterdump()) == strategies
    body["account"]["initial_cash"] = 999
    assert prepared.inputs.targets[DAYS[2]]["ASSET_A"] == 1.0
    with pytest.raises(TypeError):
        cast("MutableMapping[str, float]", prepared.targets[DAYS[2]])["ASSET_A"] = 0.0
    with pytest.raises(TypeError):
        cast("MutableMapping[str, float]", prepared.inputs.opens[1])["ASSET_A"] = 0.0
    with pytest.raises(TypeError):
        cast("MutableMapping[int, float]", prepared.features[DAYS[2]]["ASSET_A"].returns)[2] = 0.0
    attribute = "provenance"
    with pytest.raises(FrozenInstanceError):
        setattr(prepared, attribute, b"")
    assert json.loads(prepared.projection.canonical_bytes)["account"] == {
        "currency": "USD",
        "initial_cash": 100.0,
        "cashflows": [],
    }


@pytest.mark.parametrize(
    "fault",
    [
        "store",
        "raw",
        "contract",
        "version",
        "generation",
        "manifest",
        "basis",
        "missing-binding",
        "narrow",
        "history-end",
        "buckets",
        "buy-open",
        "strict",
        "ingestion",
        "calendar",
        "identity",
        "membership",
    ],
)
def test_distinct_admission_failures(tmp_path: Path, fault: str, copied_request: Document) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        expected = corrupt_request(workspace, tmp_path, body, fault)
        before = registration_state(workspace)
        with pytest.raises((ValueError, TypeError), match=expected):
            prepare(workspace, body)
        assert registration_state(workspace) == before


def corrupt_request(workspace: Workspace, root: Path, body: Document, fault: str) -> str:
    if fault in {"store", "raw", "contract", "version"}:
        key = {
            "store": "strategy_store_id",
            "raw": "raw_sha256",
            "contract": "contract_sha256",
            "version": "version",
        }[fault]
        body["strategy"][key] = "f" * 64
        return "strategy|SHA|registered"
    if fault in {"generation", "manifest"}:
        ref = next(ref for ref in body["refs"] if ref["ref_id"] == "signal")
        ref["pin"]["generation_id" if fault == "generation" else "manifest_hash"] = "f" * 64
        return "generation|pin"
    if fault == "basis":
        body["price_inputs"][0]["basis"] = "total_return"
        return "basis"
    if fault == "missing-binding":
        body["bindings"] = [item for item in body["bindings"] if item["role"] != "membership"]
        body["refs"] = [item for item in body["refs"] if item["ref_kind"] != "membership"]
        return "required"
    return corrupt_inputs(workspace, root, body, fault)


def corrupt_inputs(workspace: Workspace, root: Path, body: Document, fault: str) -> str:
    if fault == "narrow":
        body["history"]["start"] = "2025-12-01"
        return "history window too narrow"
    if fault == "history-end":
        body["history"]["end"] = "2026-02-25"
        return "history window ends"
    if fault == "buckets":
        rows = [
            row for row in price_rows(signal=True) if row["session_date"] != DAYS[0].isoformat()
        ]
        alternate_prices(workspace, root, body, "signal_prices", rows)
        return "insufficient eligible buckets"
    if fault == "buy-open":
        rows = [
            row
            for row in price_rows(signal=False)
            if (row["instrument_id"], row["session_date"]) != ("ASSET_A", DAYS[3].isoformat())
        ]
        alternate_prices(workspace, root, body, "execution_prices", rows)
        return "open"
    return corrupt_visibility(workspace, body, fault)


def corrupt_visibility(workspace: Workspace, body: Document, fault: str) -> str:
    if fault == "strict":
        body["cutoff"]["mode"] = "strict_pit"
        return "insufficient eligible buckets"
    if fault == "ingestion":
        body["cutoff"]["ingestion_cutoff_us"] = 0
        return "period|session|calendar"
    if fault == "calendar":
        body["history"]["start"] = "2025-11-27"
        return "incomplete pinned calendar"
    if fault == "identity":
        raw = canonical_json_bytes(
            {
                "schema": "aas-identity-snapshot-v1",
                "hash_format": J,
                "snapshot_id": "empty",
                "instruments": [],
                "assertions": [],
                "members": [],
                "sources": [],
            }
        )
        pin = register_identity_snapshot(
            workspace.state,
            raw,
            expected_file_sha256=hashlib.sha256(raw).hexdigest(),
            created_at_us=1,
        )
        replace_pin(body, "identity", asdict(pin))
        return "insufficient eligible buckets"
    return corrupt_membership(workspace, body)


def corrupt_membership(workspace: Workspace, body: Document) -> str:
    from decimal import Decimal  # noqa: PLC0415

    from aegis_alpha.engine.membership import MembershipRow, membership_hash  # noqa: PLC0415

    raw = canonical_json_bytes(
        {
            "schema": "aas-ensemble-membership-v1",
            "hash_format": J,
            "id": "wrong",
            "version": "1",
            "membership_sha256": membership_hash((MembershipRow("other", Decimal(1)),)),
            "rows": [{"name": "other", "weight": "1"}],
        }
    )
    pin = register_definition(
        workspace, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest(), budget=BUDGET
    )
    replace_pin(body, "membership", asdict(pin))
    return "membership"


def test_sale_only_missing_open_is_left_to_real_accounting(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        rows = [
            row
            for row in price_rows(signal=False)
            if (row["instrument_id"], row["session_date"]) != ("ASSET_A", DAYS[5].isoformat())
        ]
        alternate_prices(workspace, tmp_path, body, "execution_prices", rows)
        prepared = prepare(workspace, body)
        assert prepared.targets[DAYS[4]] == {"ASSET_B": 1.0}
        assert "ASSET_A" not in prepared.inputs.opens[3]
    with pytest.raises(ValueError, match=r"price|open"):
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)


def test_installed_inventory_and_actual_decimal_context(
    tmp_path: Path, copied_request: Document
) -> None:
    package = Path(__file__).resolve().parents[2] / "src" / "aegis_alpha"
    observed = {"aegis_alpha.engine." + path.stem for path in (package / "engine").glob("*.py")}
    assert tuple(sorted(set(CALCULATION_MODULES))) == CALCULATION_MODULES
    assert set(CALCULATION_MODULES) == observed | {
        "aegis_alpha.application.backtest_prepare",
        "aegis_alpha.data.serialization",
        "aegis_alpha.data.canonical_records",
        "aegis_alpha.storage.import_document",
        "aegis_alpha.storage.input_pins",
        "aegis_alpha.storage.market",
        "aegis_alpha.storage.market_inputs",
        "aegis_alpha.storage.market_schema",
        "aegis_alpha.storage.membership_pins",
        "aegis_alpha.storage.research_inputs",
        "aegis_alpha.storage.rowset",
        "aegis_alpha.storage.source_library",
        "aegis_alpha.storage.source_library_digest",
        "aegis_alpha.storage.source_library_schema",
        "aegis_alpha.storage.source_reader",
        "aegis_alpha.storage.state",
        "aegis_alpha.storage.strategies",
        "aegis_alpha.storage.strategy_import",
        "aegis_alpha.storage.strategy_requirements",
    }
    inventory = [
        {
            "module": name,
            "sha256": hashlib.sha256(
                (
                    package / (name.removeprefix("aegis_alpha.").replace(".", "/") + ".py")
                ).read_bytes()
            ).hexdigest(),
        }
        for name in sorted(CALCULATION_MODULES)
    ]
    assert calculation_identity()["calculation_source_hash"] == digest(
        {"schema": "aas-calculation-sources-v1", "hash_format": J, "files": inventory}
    )
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        first = prepare(workspace, body)
        context = environment_identity()
        with localcontext() as decimal:
            decimal.prec += 1
            second = prepare(workspace, body)
        assert first.targets == second.targets
        assert first.request_hash != second.request_hash
        assert environment_identity() == context


def test_fresh_process_without_incoming_files(tmp_path: Path, copied_request: Document) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        first = prepare(workspace, body)
    for path in tmp_path.iterdir():
        if path.is_file():
            path.unlink()
    code = """
import hashlib, json, sys
from pathlib import Path
from fractions import Fraction
from aegis_alpha.application.backtest_prepare import (
    PrepareRequest, parse_prepare_request, prepare_backtest)
from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.workspace import open_workspace
request = PrepareRequest(parse_prepare_request(sys.stdin.buffer.read()))
with open_workspace(Path(sys.argv[1])) as workspace:
    prepared = prepare_backtest(workspace, request,
        budget=ComputeBudget(Fraction(1), 512*1024*1024))
result = run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)
print(json.dumps({"request_hash": prepared.request_hash,
    "envelope_sha256": prepared.envelope.envelope_sha256,
    "provenance_sha256": hashlib.sha256(prepared.provenance).hexdigest(),
    "nav": result["result"]["nav"][-1]}))
"""
    result = subprocess.run(  # noqa: S603 -- fixed offline bounded child, reaped by run
        [sys.executable, "-c", code, str(tmp_path / "home")],
        input=canonical_json_bytes(body),
        capture_output=True,
        timeout=60,
        check=True,
    )
    assert json.loads(result.stdout) == {
        "request_hash": first.request_hash,
        "envelope_sha256": first.envelope.envelope_sha256,
        "provenance_sha256": hashlib.sha256(first.provenance).hexdigest(),
        "nav": {"date": "2026-03-30", "equity": 80.0, "cash": 0.0, "fee": 0.0},
    }


@pytest.mark.parametrize("operation", ["SUPERSEDE", "TOMBSTONE"])
def test_future_revisions_preserve_past_and_ingestion_is_independent(
    tmp_path: Path, operation: str, publication_clock: list[int]
) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        first = prepare(workspace, body)
        original = next(
            row
            for row in price_rows(signal=True)
            if row["instrument_id"] == "ASSET_A" and row["session_date"] == DAYS[2].isoformat()
        )
        row = {
            **original,
            "revision_id": "correction",
            "op": operation,
            "supersedes_revision_id": "signal:" + original["revision_id"],
            "available_at_us": micros(DAYS[6]),
            "revision_known_at_us": micros(DAYS[6]),
            **dict.fromkeys(("open", "high", "low", "close"), "1"),
        }
        ingestion = publication_clock[0] // 1000
        publication_clock[0] += 1000
        native(
            workspace,
            tmp_path,
            "revision",
            [row],
            destination={
                "dataset_id": "signal",
                "version": "2",
                "generation_id": "revision",
                "operation_id": "op-revision",
                "parent_id": "signal",
            },
        )
        replace_pin(body, "signal_prices", generation_pin(workspace, "signal", "2"))
        revised = prepare(workspace, body)
        assert revised.targets == first.targets
        assert revised.decisions == first.decisions
        assert revised.features == first.features
        assert revised.request_hash != first.request_hash
        body["cutoff"]["ingestion_cutoff_us"] = ingestion
        assert prepare(workspace, body).targets == first.targets


@pytest.mark.parametrize("operation", ["SUPERSEDE", "TOMBSTONE", "unavailable"])
def test_known_revisions_and_retractions_respect_explicit_ingestion(
    tmp_path: Path, operation: str, publication_clock: list[int]
) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        first = prepare(workspace, body)
        original = next(
            row
            for row in price_rows(signal=True)
            if row["instrument_id"] == "ASSET_A" and row["session_date"] == DAYS[0].isoformat()
        )
        row = {
            **original,
            "revision_id": "known-correction",
            "op": "SUPERSEDE" if operation == "unavailable" else operation,
            "supersedes_revision_id": "signal:" + original["revision_id"],
            "revision_known_at_us": micros(DAYS[1]),
            "available_at_us": micros(DAYS[6] if operation == "unavailable" else DAYS[1]),
            **dict.fromkeys(("open", "high", "low", "close"), "100"),
        }
        limit = publication_clock[0] // 1000
        publication_clock[0] += 1000
        native(
            workspace,
            tmp_path,
            "known-revision",
            [row],
            destination={
                "dataset_id": "signal",
                "version": "2",
                "generation_id": "known-revision",
                "operation_id": "op-known-revision",
                "parent_id": "signal",
            },
        )
        replace_pin(body, "signal_prices", generation_pin(workspace, "signal", "2"))
        if operation == "SUPERSEDE":
            revised = prepare(workspace, body)
            assert revised.targets[DAYS[2]] == {"ASSET_B": 1.0}
            assert revised.features[DAYS[2]]["ASSET_A"].returns[2] == pytest.approx(-0.85)
        else:
            with pytest.raises(ValueError, match="insufficient eligible buckets"):
                prepare(workspace, body)
        body["cutoff"]["ingestion_cutoff_us"] = limit
        assert prepare(workspace, body).targets == first.targets


def calendar_revision(
    workspace: Workspace, root: Path, body: Document, day: date, changes: Document
) -> None:
    original = next(row for row in session_rows() if row["session_date"] == day.isoformat())
    native(
        workspace,
        root,
        "calendar-correction",
        [
            {
                **original,
                "revision_id": "calendar-correction",
                "op": "SUPERSEDE",
                "supersedes_revision_id": "sessions:" + original["revision_id"],
                "revision_known_at_us": micros(date(2026, 3, 30)),
                "available_at_us": micros(date(2026, 3, 30)),
                "ingested_at_us": micros(date(2026, 4, 1)),
                **changes,
            }
        ],
        destination={
            "dataset_id": "sessions",
            "version": "2",
            "generation_id": "calendar-correction",
            "operation_id": "op-calendar-correction",
            "parent_id": "sessions",
        },
    )
    replace_pin(body, "sessions", generation_pin(workspace, "sessions", "2"))


def incompatible_calendar(workspace: Workspace, root: Path, body: Document, change: str) -> None:
    if change == "insert-open":
        inserted = date(2026, 1, 30)
        rows = price_rows(signal=False)
        rows.extend(
            row_identity(
                {
                    **row,
                    "session_date": inserted.isoformat(),
                    "bar_end_us": micros(inserted),
                    "available_at_us": micros(inserted),
                    "revision_known_at_us": micros(inserted),
                },
                "prices",
            )
            for row in price_rows(signal=False)
            if row["session_date"] == "2026-02-02"
        )
        alternate_prices(workspace, root, body, "execution_prices", rows)
        calendar_revision(
            workspace,
            root,
            body,
            inserted,
            {"status": "open", "open_at_us": micros(inserted, 9), "close_at_us": micros(inserted)},
        )
    else:
        calendar_revision(
            workspace,
            root,
            body,
            date(2026, 2, 2) if change == "close-execution" else date(2026, 1, 29),
            {"status": "closed", "open_at_us": None, "close_at_us": None},
        )


def reject_export_and_accounting(frame: FrameType, event: str, argument: object) -> None:
    reject_accounting(frame, event, argument)
    if (
        event == "call"
        and frame.f_globals.get("__name__") == "aegis_alpha.engine.backtest_request"
        and frame.f_code.co_name == "export_envelope"
    ):
        raise AssertionError("incompatible calendar reached export")


@pytest.mark.parametrize("change", ["close-execution", "insert-open", "close-decision"])
@pytest.mark.parametrize("all_cash", [False, True])
def test_incompatible_outcome_calendar_rejects_before_export(
    tmp_path: Path, change: str, copied_request: Document, *, all_cash: bool
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        if all_cash:
            second_recipe(workspace, tmp_path, body)
        body["explicit_decision_dates"] = ["2026-01-29"]
        baseline = prepare(workspace, body)
        assert [(slot.decision_date, slot.execution_date) for slot in baseline.slots] == [
            (date(2026, 1, 29), date(2026, 2, 2))
        ]
        result = cast(
            "Document",
            run_document(baseline.envelope.canonical_bytes, baseline.envelope.envelope_sha256),
        )["result"]
        assert [(fill["decision_date"], fill["execution_date"]) for fill in result["fills"]] == (
            [] if all_cash else [("2026-01-29", "2026-02-02")]
        )
        assert baseline.targets == {date(2026, 1, 29): {} if all_cash else {"ASSET_A": 1.0}}
        incompatible_calendar(workspace, tmp_path, body, change)
        before = registration_state(workspace)
        assert workspace.strategies is not None
        strategies = tuple(workspace.strategies.iterdump())
        connections = (workspace.state, workspace.strategies)
        for connection in connections:
            connection.set_authorizer(select_only)
        sys.setprofile(reject_export_and_accounting)
        try:
            with pytest.raises(ValueError, match=r"incompatible.*calendar|calendar.*incompatible"):
                prepare(workspace, body)
        finally:
            sys.setprofile(None)
            for connection in connections:
                connection.set_authorizer(None)
        assert registration_state(workspace) == before
        assert tuple(workspace.strategies.iterdump()) == strategies


@pytest.mark.parametrize("change", ["close-execution", "insert-open"])
def test_empty_schedule_preserves_revised_outcome_calendar(
    tmp_path: Path, change: str, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["explicit_decision_dates"] = []
        incompatible_calendar(workspace, tmp_path, body, change)
        prepared = prepare(workspace, body)
        assert prepared.slots == prepared.decisions == ()
        assert prepared.targets == {}
        dates = json.loads(prepared.envelope.canonical_bytes)["dates"]
        assert dates == (
            ["2026-01-29", "2026-02-26", "2026-03-02", "2026-03-30"]
            if change == "close-execution"
            else [
                "2026-01-29",
                "2026-01-30",
                "2026-02-02",
                "2026-02-26",
                "2026-03-02",
                "2026-03-30",
            ]
        )
    result = cast(
        "Document",
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    )["result"]
    assert result["fills"] == []
    assert [row["date"] for row in result["nav"]] == dates
    assert result["nav"][-1]["equity"] == pytest.approx(100)


def target_membership_interval(
    workspace: Workspace, body: Document, role: str, interval: Document
) -> None:
    identity = next(ref["pin"] for ref in body["refs"] if ref["ref_kind"] == "identity")
    universe = next(ref["pin"] for ref in body["refs"] if ref["ref_kind"] == "universe")
    verified = read_membership_pins(
        workspace.state,
        IdentityPin(**identity),
        UniversePin(**universe),
        max_materialization_bytes=BUDGET.memory_limit_bytes,
    )
    stored = verified.identity if role == "identity" else verified.universe
    assert stored is not None
    doc = json.loads(stored.canonical_bytes)
    for member in doc["members"]:
        if member.get("instrument_id", member.get("assertion_id")) == "ASSET_A":
            member.update(interval)
    if role == "identity":
        doc["snapshot_id"] = "execution-interval"
        pin = register_identity_snapshot(
            workspace.state,
            canonical_json_bytes(doc),
            expected_file_sha256=digest(doc),
            created_at_us=1,
        )
    else:
        doc["version"] = "execution-interval"
        pin = register_universe_version(
            workspace.state, canonical_json_bytes(doc), expected_file_sha256=digest(doc)
        )
    replace_pin(body, role, asdict(pin))


@pytest.mark.parametrize("role", ["identity", "universe"])
@pytest.mark.parametrize("revised_hour", [7, 11])
def test_target_membership_uses_decision_local_execution_open(
    tmp_path: Path, role: str, revised_hour: int, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["explicit_decision_dates"] = ["2026-01-29"]
        # Original execution is 09:00. Expiry at 08:00 excludes it; 10:00 admits it.
        # A later revision to 07:00/11:00 must not reverse either decision.
        earlier_hour = 7
        expiry_hour = 8 if revised_hour == earlier_hour else 10
        target_membership_interval(
            workspace, body, role, {"valid_to_us": micros(date(2026, 2, 2), expiry_hour)}
        )
        baseline = None
        if revised_hour == earlier_hour:
            with pytest.raises(ValueError, match="selected target"):
                prepare(workspace, body)
        else:
            baseline = prepare(workspace, body)
        calendar_revision(
            workspace,
            tmp_path,
            body,
            date(2026, 2, 2),
            {"open_at_us": micros(date(2026, 2, 2), revised_hour)},
        )
        before = registration_state(workspace)
        if revised_hour == earlier_hour:
            with pytest.raises(ValueError, match="selected target"):
                prepare(workspace, body)
        else:
            prepared = prepare(workspace, body)
            assert baseline is not None
            assert prepared.slots == baseline.slots
            assert prepared.decisions == baseline.decisions
            assert prepared.targets == {date(2026, 1, 29): {"ASSET_A": 1.0}}
            result = cast(
                "Document",
                run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
            )["result"]
            assert [(row["decision_date"], row["execution_date"]) for row in result["fills"]] == [
                ("2026-01-29", "2026-02-02")
            ]
            assert result["nav"][-1]["equity"] == pytest.approx(80)
        assert registration_state(workspace) == before


@pytest.mark.parametrize("role", ["identity", "universe"])
@pytest.mark.parametrize("offset_us", [-1, 0, 1])
def test_membership_knowledge_visibility_is_not_execution_economic_time(
    tmp_path: Path, role: str, offset_us: int, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["explicit_decision_dates"] = ["2026-01-29"]
        cutoff = micros(date(2026, 1, 29))
        target_membership_interval(
            workspace,
            body,
            role,
            {"known_from_us": cutoff + offset_us, "valid_to_us": micros(date(2026, 2, 2), 10)},
        )
        if offset_us > 0:
            with pytest.raises(ValueError, match="insufficient eligible buckets"):
                prepare(workspace, body)
        else:
            assert prepare(workspace, body).targets == {date(2026, 1, 29): {"ASSET_A": 1.0}}


@pytest.mark.parametrize("offset_us", [-1, 0, 1])
def test_execution_session_revision_respects_exact_cutoff_and_ingestion(
    tmp_path: Path, offset_us: int, publication_clock: list[int]
) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        body["explicit_decision_dates"] = ["2026-01-29"]
        target_membership_interval(
            workspace, body, "universe", {"valid_to_us": micros(date(2026, 2, 2), 10)}
        )
        baseline = prepare(workspace, body)
        ingestion_before = publication_clock[0] // 1000
        publication_clock[0] += 1000
        cutoff = micros(date(2026, 1, 29))
        calendar_revision(
            workspace,
            tmp_path,
            body,
            date(2026, 2, 2),
            {
                "open_at_us": micros(date(2026, 2, 2), 11),
                "available_at_us": cutoff + offset_us,
                "revision_known_at_us": cutoff + offset_us,
            },
        )
        if offset_us <= 0:
            with pytest.raises(ValueError, match="selected target outside pinned universe"):
                prepare(workspace, body)
        else:
            assert prepare(workspace, body).targets == baseline.targets
        body["cutoff"]["ingestion_cutoff_us"] = ingestion_before
        assert prepare(workspace, body).targets == baseline.targets
        body["cutoff"]["ingestion_cutoff_us"] = ingestion_before + 1
        if offset_us <= 0:
            with pytest.raises(ValueError, match="selected target outside pinned universe"):
                prepare(workspace, body)
        else:
            assert prepare(workspace, body).targets == baseline.targets


def test_compatible_inserted_outcome_session_keeps_fill_and_all_dates(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["explicit_decision_dates"] = ["2026-01-29"]
        baseline = prepare(workspace, body)
        inserted = date(2026, 2, 3)
        rows = price_rows(signal=False)
        rows.extend(
            row_identity(
                {**row, "session_date": inserted.isoformat(), "bar_end_us": micros(inserted)},
                "prices",
            )
            for row in price_rows(signal=False)
            if row["session_date"] == "2026-02-02"
        )
        alternate_prices(workspace, tmp_path, body, "execution_prices", rows)
        calendar_revision(
            workspace,
            tmp_path,
            body,
            inserted,
            {"status": "open", "open_at_us": micros(inserted, 9), "close_at_us": micros(inserted)},
        )
        prepared = prepare(workspace, body)
        assert prepared.slots == baseline.slots
        assert prepared.decisions == baseline.decisions
        assert prepared.features == baseline.features
        assert prepared.targets == baseline.targets
        assert prepared.request_hash != baseline.request_hash
        assert json.loads(prepared.envelope.canonical_bytes)["dates"] == [
            "2026-01-29",
            "2026-02-02",
            "2026-02-03",
            "2026-02-26",
            "2026-03-02",
            "2026-03-30",
        ]
    result = cast(
        "Document",
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    )["result"]
    assert [
        (row["decision_date"], row["execution_date"], row["shares"]) for row in result["fills"]
    ] == [("2026-01-29", "2026-02-02", 100 / 15)]
    assert [row["date"] for row in result["nav"]] == [
        "2026-01-29",
        "2026-02-02",
        "2026-02-03",
        "2026-02-26",
        "2026-03-02",
        "2026-03-30",
    ]
    assert result["nav"][-1]["equity"] == pytest.approx(80)


def test_calendar_future_revision_does_not_rewrite_signal_eligibility(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        first = prepare(workspace, body)
        original = session_rows()[0]
        row = {
            **original,
            "revision_id": "closed-later",
            "op": "SUPERSEDE",
            "supersedes_revision_id": "sessions:" + original["revision_id"],
            "status": "closed",
            "open_at_us": None,
            "close_at_us": None,
            "revision_known_at_us": micros(DAYS[6]),
            "available_at_us": micros(DAYS[6]),
            "ingested_at_us": micros(DAYS[-1]),
        }
        native(
            workspace,
            tmp_path,
            "calendar-revision",
            [row],
            destination={
                "dataset_id": "sessions",
                "version": "2",
                "generation_id": "calendar-revision",
                "operation_id": "op-calendar-revision",
                "parent_id": "sessions",
            },
        )
        replace_pin(body, "sessions", generation_pin(workspace, "sessions", "2"))
        revised = prepare(workspace, body)
        assert revised.targets == first.targets
        assert revised.features == first.features
        assert revised.slots == first.slots


@pytest.mark.parametrize(
    "fault",
    [
        "macro-unit",
        "macro-domain",
        "derived-mismatch",
        "missing-month",
        "closed-price",
        "terminal",
        "universe",
    ],
)
def test_declared_inputs_and_calendar_eligibility(
    tmp_path: Path, fault: str, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        if fault.startswith(("macro", "derived")):
            second_recipe(workspace, tmp_path, body)
        expected = modify_declarations(workspace, tmp_path, body, fault)
        with pytest.raises(ValueError, match=expected):
            prepare(workspace, body)


def modify_declarations(workspace: Workspace, root: Path, body: Document, fault: str) -> str:
    if fault == "macro-unit":
        body["macro_inputs"][0]["unit"] = "different"
        return "macro unit mismatch"
    if fault == "macro-domain":
        replace_pin(body, "macro", generation_pin(workspace, "outcomes"))
        # Same generation can be bound to both roles, with only one descriptor.
        seen: set[tuple[str, str, str]] = set()
        refs = []
        for ref in body["refs"]:
            key = ref["ref_kind"], ref["ref_id"], ref["ref_version"]
            if key not in seen:
                refs.append(ref)
            seen.add(key)
        body["refs"] = refs
        return "macro binding"
    if fault == "derived-mismatch":
        ref = next(ref for ref in body["refs"] if ref["ref_kind"] == "derived")
        stored = workspace.state.execute(
            "SELECT definition FROM feature_contracts WHERE name='YIELD'"
        ).fetchone()[0]
        doc = json.loads(stored)
        doc["version"] = "2"
        spec = DerivedSeriesSpec(
            series_id="YIELD",
            operation="trailing_sum_over_price",
            trailing_months=2,
            consumes_capital=True,
            consumes_totalreturn=False,
            input_bindings=(
                DerivedInputBinding("signal", "1", "ASSET_A", "price"),
                DerivedInputBinding("macro", "1", "FLOW", "addend_a"),
            ),
            signal_lag_months=(0,),
            signal_thresholds=(0.1,),
            reference_provenance="synthetic cashflows",
        )
        doc["definition"] = spec
        pin = register_definition(
            workspace, canonical_json_bytes(doc), expected_file_sha256=digest(doc), budget=BUDGET
        )
        assert ref["ref_version"] == "1"
        replace_pin(body, "derived", asdict(pin))
        return "derived definition differs"
    return modify_calendar_inputs(workspace, root, body, fault)


def modify_calendar_inputs(workspace: Workspace, root: Path, body: Document, fault: str) -> str:
    if fault == "missing-month":
        rows = [
            row for row in price_rows(signal=True) if row["session_date"] != DAYS[1].isoformat()
        ]
        alternate_prices(workspace, root, body, "signal_prices", rows)
        body["explicit_decision_dates"] = [DAYS[4].isoformat()]
        return "missing monthly bucket"
    if fault == "closed-price":
        rows = price_rows(signal=True)
        for row in rows:
            if row["session_date"] == DAYS[2].isoformat():
                row["session_date"] = "2026-01-28"
                row["bar_end_us"] = micros(date(2026, 1, 28))
                row_identity(row, "prices")
        alternate_prices(workspace, root, body, "signal_prices", rows)
        return "insufficient eligible buckets"
    if fault == "terminal":
        body["explicit_decision_dates"] = [DAYS[6].isoformat()]
        return "explicit decision"
    raw = canonical_json_bytes(
        {
            "schema": "aas-universe-version-v1",
            "hash_format": J,
            "universe_id": "empty",
            "version": "1",
            "instruments": [],
            "members": [],
            "sources": [],
        }
    )
    pin = register_universe_version(
        workspace.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
    )
    replace_pin(body, "universe", asdict(pin))
    return "insufficient eligible buckets"


@pytest.mark.parametrize("dataset", ["signal", "sessions"])
def test_selected_native_evidence_cannot_fall_back_to_opaque_publication(
    tmp_path: Path, dataset: str, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        digest_value = workspace.state.execute(
            "SELECT transform_hash FROM dataset_versions WHERE dataset_id=?", (dataset,)
        ).fetchone()[0]
        (workspace.paths.raw / digest_value[:2] / digest_value).unlink()
        before = registration_state(workspace)
        with pytest.raises(DescriptorTreeError, match="regular file cannot be opened") as error:
            prepare(workspace, body)
        assert isinstance(error.value.__cause__, FileNotFoundError)
        assert registration_state(workspace) == before


def test_empty_explicit_schedule_and_v2_cashflow_are_not_defaults(
    tmp_path: Path, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        body["explicit_decision_dates"] = []
        body["envelope"]["schema_version"] = "aas-etf-backtest-v2"
        body["account"]["cashflows"] = [{"date": DAYS[3].isoformat(), "amount": 20}]
        prepared = prepare(workspace, body)
        assert prepared.slots == ()
        assert prepared.decisions == ()
        assert prepared.targets == {}
        result = cast(
            "Document",
            run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
        )
        assert result["result"]["account"]["fills"] == []
        assert result["result"]["account"]["nav"][-1] == {
            "date": "2026-03-30",
            "equity": 120.0,
            "cash": 120.0,
            "fee": 0.0,
        }


def proxy_recipe(workspace: Workspace, root: Path, body: Document) -> Document:
    value = contract()
    record = replace(
        value.pack[0],
        offensive_config={**value.pack[0].offensive_config, "assets": ("EXPOSURE", "ASSET_B")},
    )
    document = json.loads(raw_bundle(replace(value, pack=(record,))))
    document["bundle_version"] = "proxy"
    raw = canonical_json_bytes(document)
    path = root / "proxy-strategy.json"
    path.write_bytes(raw)
    registered = register_strategy(
        workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "proxy"
    )
    body["strategy"].update(
        version="proxy",
        raw_sha256=registered["raw_sha256"],
        contract_sha256=registered["contract_sha256"],
    )
    for selection in body["price_inputs"]:
        if selection["binding"]["role"] == "signal_prices":
            selection["instrument_ids"].remove("ASSET_A")
    execution = next(
        ref
        for ref in body["refs"]
        if ref["ref_id"] == "outcomes" or ref["ref_id"] == "changed-execution_prices"
    )
    source = json.loads((root / (execution["ref_id"] + ".json")).read_bytes())["source"]
    transition = {
        "donor_id": "ASSET_A",
        "target_id": "ASSET_B",
        "logical_exposure_id": "EXPOSURE",
        "switch_decision_date": DAYS[4].isoformat(),
        "mode": "observed_instrument_switch",
        "donor_source": source,
        "target_source": source,
    }
    for role in ("basis", "calendar", "cost"):
        ref = next(ref for ref in body["refs"] if ref["ref_kind"] == "convention:" + role)
        transition[role + "_ref"] = {
            "id": ref["ref_id"],
            "version": ref["ref_version"],
            "sha256": ref["hash"],
        }
    definition = {
        "proxy_id": "synthetic-proxy",
        "version": "1",
        "normalization": {"input_number": "decimal_string", "output": "ieee754_binary64"},
        "transition": transition,
    }
    return publish_proxy(workspace, root, body, definition)


def publish_proxy(
    workspace: Workspace, root: Path, body: Document, definition: Document
) -> Document:
    common = {
        key: value
        for key, value in _source_row().items()
        if key
        in {
            "generation_id",
            "record_id",
            "revision_id",
            "supersedes_revision_id",
            "op",
            "available_at_us",
            "revision_known_at_us",
            "ingested_at_us",
            "source_snapshot_id",
            "source_row_hash",
        }
    }
    input_hash = digest(
        [
            definition["transition"][key]
            for key in ("donor_source", "target_source", "basis_ref", "calendar_ref", "cost_ref")
        ]
    )
    rows = [
        row_identity(
            {
                **common,
                "contract_id": definition["proxy_id"],
                "contract_version": "1",
                "contract_hash": digest(definition),
                "input_bundle_hash": input_hash,
                "instrument_id": "EXPOSURE",
                "feature_at_us": micros(day),
                "value": str(value),
                "value_state": "present",
                "available_at_us": micros(day),
                "revision_known_at_us": micros(day),
                "ingested_at_us": micros(DAYS[-1]),
            },
            "feature_values",
        )
        for day, value in zip((DAYS[0], DAYS[1], DAYS[2], DAYS[4]), (10, 12, 20, 30), strict=True)
    ]
    path = _spec(workspace, root / "proxy-points.sqlite3", rows)
    document = json.loads(path.read_bytes())
    for key in ("price", "calendar", "decimal_conversion"):
        del document[key]
    document.update(
        schema_version="aas-proxy-transform-v1",
        proxy=definition,
        instruments=[{"instrument_id": "EXPOSURE", "asset_type": "proxy", "venue": "SYN"}],
        dataset={
            "dataset_id": "proxy",
            "version": "1",
            "generation_id": "proxy",
            "operation_id": "op-proxy",
            "parent_id": None,
        },
    )
    raw = canonical_json_bytes(document)
    path.write_bytes(raw)
    register_proxy_input(workspace, path, hashlib.sha256(raw).hexdigest())
    key = add_ref(body, "proxy", "proxy")
    replace_pin(body, "proxy", generation_pin(workspace, "proxy"))
    body["proxy_rules"] = [{"binding": key, "logical_exposure_id": "EXPOSURE"}]
    return document


@pytest.mark.parametrize("held", [True, False])
def test_actual_proxy_donor_target_accounting_without_inferred_holdings(
    tmp_path: Path, copied_request: Document, *, held: bool
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        proxy_recipe(workspace, tmp_path, body)
        if not held:
            body["explicit_decision_dates"] = [DAYS[4].isoformat()]
        prepared = prepare(workspace, body)
        expected = (
            {DAYS[2]: {"ASSET_A": 1.0}, DAYS[4]: {"ASSET_B": 1.0}}
            if held
            else {DAYS[4]: {"ASSET_B": 1.0}}
        )
        assert prepared.targets == expected
        assert prepared.features[DAYS[4]]["EXPOSURE"].returns[2] == pytest.approx(1.5)
        assert {pin["source_id"] for pin in prepared.inputs.source_pins} == {
            "signal",
            "outcomes",
            "sessions",
            "proxy-points",
        }
        assert all("EXPOSURE" not in row for row in prepared.inputs.opens)
    result = cast(
        "Document",
        run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    )
    fills = result["result"]["fills"]
    assert [(row["symbol"], row["shares"]) for row in fills] == (
        [("ASSET_A", 100 / 15), ("ASSET_A", -100 / 15), ("ASSET_B", 4.0)]
        if held
        else [("ASSET_B", 5.0)]
    )
    assert all(row["execution_date"] == "2026-03-02" for row in (fills[1:] if held else fills))
    assert result["result"]["nav"][-1]["equity"] == pytest.approx(80 if held else 100)


def test_proxy_future_suffix_and_ingestion_cutoff_are_decision_local(
    tmp_path: Path, publication_clock: list[int]
) -> None:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        publication_clock[0] += 1000
        proxy = proxy_recipe(workspace, tmp_path, body)
        first = prepare(workspace, body)
        definition = proxy["proxy"]
        suffix = {
            "contract_id": definition["proxy_id"],
            "contract_version": "1",
            "contract_hash": digest(definition),
            "input_bundle_hash": digest(
                [
                    definition["transition"][key]
                    for key in (
                        "donor_source",
                        "target_source",
                        "basis_ref",
                        "calendar_ref",
                        "cost_ref",
                    )
                ]
            ),
            "instrument_id": "EXPOSURE",
            "feature_at_us": micros(DAYS[-1]),
            "value": 999.0,
            "value_state": "present",
            "revision_id": "future-proxy",
            "op": "ASSERT",
            "supersedes_revision_id": None,
            "available_at_us": micros(DAYS[-1]),
            "revision_known_at_us": micros(DAYS[-1]),
            "ingested_at_us": micros(DAYS[-1]),
        }
        publication.publish_document(
            workspace,
            parse_import(
                canonical_json_bytes(
                    {
                        "schema_version": "aas-market-import-v1",
                        "dataset_id": "proxy",
                        "version": "2",
                        "generation_id": "proxy-suffix",
                        "operation_id": "op-proxy-suffix",
                        "parent_id": "proxy",
                        "domain": "feature_values",
                        "provider": "synthetic",
                        "publication_at_us": None,
                        "normalizer_version": "synthetic-v1",
                        "transform_sha256": digest(proxy),
                        "instruments": proxy["instruments"],
                        "rows": [suffix],
                    }
                )
            ),
        )
        replace_pin(body, "proxy", generation_pin(workspace, "proxy", "2"))
        revised = prepare(workspace, body)
        assert revised.targets == first.targets
        assert revised.features == first.features
        assert revised.decisions == first.decisions
        assert revised.request_hash != first.request_hash
        limit = workspace.state.execute(
            "SELECT created_at_us FROM storage_operations WHERE operation_id='op-sessions'"
        ).fetchone()[0]
        body["cutoff"]["ingestion_cutoff_us"] = limit
        with pytest.raises(ValueError, match="insufficient eligible buckets"):
            prepare(workspace, body)


@pytest.mark.parametrize("fault", ["convention", "source", "selected", "sale-open"])
def test_proxy_pin_agreement_and_sale_only_accounting(
    tmp_path: Path, fault: str, copied_request: Document
) -> None:
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        body = copied_request
        if fault == "sale-open":
            rows = [
                row
                for row in price_rows(signal=False)
                if (row["instrument_id"], row["session_date"]) != ("ASSET_A", DAYS[5].isoformat())
            ]
            alternate_prices(workspace, tmp_path, body, "execution_prices", rows)
        proxy_recipe(workspace, tmp_path, body)
        if fault == "convention":
            raw = json.loads(next(raw for raw in fixture()[2] if json.loads(raw)["kind"] == "cost"))
            raw["id"] = "other-cost"
            pin = register_convention(
                workspace.state, canonical_json_bytes(raw), expected_file_sha256=digest(raw)
            )
            replace_pin(body, "cost", asdict(pin))
        if fault == "source":
            alternate_prices(
                workspace, tmp_path, body, "execution_prices", price_rows(signal=False)
            )
        if fault == "selected":
            next(
                row for row in body["price_inputs"] if row["binding"]["role"] == "execution_prices"
            )["instrument_ids"].remove("ASSET_A")
        if fault != "sale-open":
            with pytest.raises(ValueError, match=r"proxy|execution"):
                prepare(workspace, body)
        else:
            prepared = prepare(workspace, body)
            assert prepared.targets[DAYS[4]] == {"ASSET_B": 1.0}
            with pytest.raises(ValueError, match=r"price|open"):
                run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256)
