"""Synthetic retained prices: exact admission, revision pins and raw provenance."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.storage import market, publication, source_library
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_publication import document

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

_RECORD = "5ed0dd479a146fe98960d7848d39b527f85b9ffeb0d151a2edf747b0f2611ac9"
_NUMBERS = ("open", "high", "low", "close", "volume")
_INEXACT_FLOAT = 0.1


def _register(workspace: Workspace, path: Path) -> dict[str, object]:
    name = "aegis_alpha.storage.research_inputs"
    assert importlib.util.find_spec(name) is not None, "retained price registration is unavailable"
    module = importlib.import_module(name)
    return module.register_price_input(
        workspace, path, hashlib.sha256(path.read_bytes()).hexdigest()
    )


def _source_row() -> dict[str, object]:
    row = json.loads(document())["rows"][0]
    row.update(
        generation_id="retained-generation",
        record_id=_RECORD,
        source_snapshot_id="retained-snapshot",
        source_row_hash="c" * 64,
    )
    return row


def _spec(workspace: Workspace, path: Path, rows: list[dict[str, object]]) -> Path:
    # SQLite affinity is absent: IEEE floats must remain floats.
    with closing(sqlite3.connect(path)) as connection:
        columns = list(rows[0])
        connection.execute("CREATE TABLE bars (" + ",".join('"' + c + '"' for c in columns) + ")")
        connection.executemany(
            "INSERT INTO bars VALUES (" + ",".join("?" for _ in columns) + ")",  # noqa: S608 -- only bound placeholders
            [[row[c] for c in columns] for row in rows],
        )
        connection.commit()
    path.chmod(0o600)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    source_library.import_sqlite(workspace, path, path.stem, digest)
    table = source_library.list_tables(workspace, path.stem)[0]
    body = {
        "schema_version": "aas-price-transform-v1",
        "source": {
            "source_id": path.stem,
            "source_sha256": digest,
            "table": "bars",
            "table_digest": table["digest"],
        },
        "dataset": {
            "dataset_id": "synthetic-prices",
            "version": "1",
            "generation_id": "g1",
            "operation_id": "op1",
            "parent_id": None,
        },
        "columns": {column: column for column in columns},
        "instruments": [{"instrument_id": "ASSET_A", "asset_type": "equity", "venue": "SYNTHETIC"}],
        "price": {"basis": "unadjusted", "currency": "USD", "price_role": "canonical"},
        "calendar": {
            "calendar_id": "synthetic-calendar",
            "timezone": "UTC",
            "timezone_version": "synthetic-v1",
        },
        "publication_at_us": None,
        "provider": "synthetic",
        "normalizer_version": "synthetic-v1",
        "decimal_conversion": dict.fromkeys(_NUMBERS, "decimal_string"),
    }
    spec = path.with_suffix(".json")
    spec.write_text(json.dumps(body, indent=2))
    return spec


def _change(path: Path, field: str, value: object) -> None:
    body = json.loads(path.read_bytes())
    body[field] = value
    path.write_text(json.dumps(body, indent=2))


def test_retained_versions_reread_exactly_in_fresh_process(tmp_path: Path) -> None:
    # Given two source revisions with explicit ancestry, not inferred ticker updates.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = _spec(workspace, tmp_path / "source1.sqlite3", [_source_row()])
        result = _register(workspace, first)
        assert _register(workspace, first) == result
        old = publication.read_dataset(workspace, "synthetic-prices", "1")
        second = _spec(
            workspace,
            tmp_path / "source2.sqlite3",
            [
                {
                    **_source_row(),
                    "revision_id": "r2",
                    "supersedes_revision_id": "r1",
                    "op": "SUPERSEDE",
                    "close": "11.125",
                    "available_at_us": 40,
                    "revision_known_at_us": 40,
                    "ingested_at_us": 50,
                    "source_row_hash": "d" * 64,
                }
            ],
        )
        _change(
            second,
            "dataset",
            {
                "dataset_id": "synthetic-prices",
                "version": "2",
                "generation_id": "g2",
                "operation_id": "op2",
                "parent_id": "g1",
            },
        )
        _register(workspace, second)
        assert publication.read_dataset(workspace, "synthetic-prices", "1") == old
    # When original source files are removed, a fresh interpreter reads both pins.
    for path in tmp_path.glob("source*.sqlite3"):
        path.unlink()
    code = """
import json, sys
from pathlib import Path
from aegis_alpha.storage.workspace import open_workspace
from aegis_alpha.storage import market, publication
with open_workspace(Path(sys.argv[1])) as w:
    values = {}
    for version in ('1', '2'):
        pin = publication.read_dataset(w, 'synthetic-prices', version)
        row = market.read_generation(w.market, pin['generation_id'])[0]
        values[version] = [
            str(row['close']), row['revision_id'], row['supersedes_revision_id'], pin['row_count']
        ]
    print(json.dumps(values))
"""
    result = subprocess.run(  # noqa: S603 -- fixed offline reader, bounded and reaped
        [sys.executable, "-c", code, str(home)],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    # Then literal prices, links and delta counts survive the process boundary.
    assert json.loads(result.stdout) == {
        "1": ["11.000000000000", "r1", None, 1],
        "2": ["11.125000000000", "r2", "r1", 1],
    }


def test_full_mapping_and_exact_transform_are_raw_evidence(tmp_path: Path) -> None:
    # Given a transform with original common provenance, When published, Then retain exact bytes.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "source.sqlite3", [_source_row()])
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        _register(workspace, path)
        row = workspace.state.execute(
            "SELECT transform_hash, normalizer_version FROM dataset_versions"
        ).fetchone()
        assert tuple(row) == (digest, "synthetic-v1")
        assert (workspace.paths.raw / digest[:2] / digest).read_bytes() == raw
        assert set(json.loads(raw)["columns"]) >= {
            "record_id",
            "revision_id",
            "supersedes_revision_id",
            "op",
            "available_at_us",
            "revision_known_at_us",
            "ingested_at_us",
            "source_snapshot_id",
            "source_row_hash",
            "generation_id",
        }
        assert (
            workspace.state.execute("SELECT publication_at_us FROM source_snapshots").fetchone()[0]
            is None
        )
        stored = market.read_generation(workspace.market, "g1")[0]
        assert stored["record_id"] == _RECORD
        # Publication lineage identifies normalized import bytes, not original source bytes.
        assert stored["source_snapshot_id"] != "retained-snapshot"
        assert stored["source_row_hash"] != "c" * 64


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("currency", "EUR"),
        ("basis", "adjusted"),
        ("price_role", "reference"),
        ("record_id", "ticker-only"),
        ("source_row_hash", "bad"),
        ("instrument_id", "UNKNOWN"),
        ("open", None),
        ("volume", None),
        ("close", "0.0000000000001"),
        ("close", "1e26"),
        ("close", 11.0),
        ("op", "SUPERSEDE"),
        ("revision_id", None),
        ("available_at_us", -1),
    ],
)
def test_invalid_source_rejected_before_publication(
    tmp_path: Path, field: str, value: object
) -> None:
    # Given a retained inadmissible row, When transforming, Then no publication is created.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "source.sqlite3", [{**_source_row(), field: value}])
        with pytest.raises((ValueError, TypeError)):
            _register(workspace, path)
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM storage_operations WHERE kind='market_publish'"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize(
    "value", ["0.000000000001", "9999999999999999999999999.999999999999", None, 0.5, 0.1]
)
def test_number_policy_and_whole_row_missingness(tmp_path: Path, value: str | float | None) -> None:
    # Given declared numeric policy, When converting, Then exact values or whole-row nulls survive.
    home = tmp_path / "home"
    initialize(home)
    row = _source_row()
    row.update(dict.fromkeys(_NUMBERS, value))
    if value is None:
        row["value_state"] = "missing"
        row["available_at_us"] = row["revision_known_at_us"] = None
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "source.sqlite3", [row])
        if isinstance(value, float):
            _change(path, "decimal_conversion", dict.fromkeys(_NUMBERS, "ieee_float"))
        if value == _INEXACT_FLOAT:
            # 1/10 is not a binary fraction: from_float(0.1) cannot fit scale 12.
            with pytest.raises(ValueError, match="exact"):
                _register(workspace, path)
        else:
            _register(workspace, path)
            stored = market.read_generation(workspace.market, "g1")[0]
            expected = None if value is None else Decimal(str(value))
            assert [stored[field] for field in _NUMBERS] == [expected] * len(_NUMBERS)
            if value is None:
                assert market.read_generation(workspace.market, "g1", cutoff_us=100) == []


@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "missing-column",
        "missing-mapping",
        "duplicate-instrument",
        "instrument-conflict",
        "source-pin",
        "schema",
        "numeric-policy",
    ],
)
def test_conflicts_and_missing_fields_fail_closed(tmp_path: Path, fault: str) -> None:
    # Given malformed or conflicting inputs, When registering, Then no generation appears.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        rows = [_source_row()] * (2 if fault == "duplicate" else 1)
        path = _spec(workspace, tmp_path / "source.sqlite3", rows)
        body = json.loads(path.read_bytes())
        match fault:
            case "missing-column":
                body["columns"]["revision_id"] = "absent"
            case "missing-mapping":
                del body["columns"]["revision_known_at_us"]
            case "duplicate-instrument":
                body["instruments"] *= 2
            case "instrument-conflict":
                workspace.state.execute(
                    "INSERT INTO instruments(instrument_id, asset_type, venue) "
                    "VALUES ('ASSET_A', 'bond', 'OTHER')"
                )
                workspace.state.commit()
            case "source-pin":
                body["source"]["table_digest"] = "0" * 64
            case "schema":
                body["schema_version"] = "unsupported"
            case "numeric-policy":
                body["decimal_conversion"]["close"] = "round"
            case "duplicate":
                pass
        path.write_text(json.dumps(body))
        with pytest.raises((ValueError, TypeError)):
            _register(workspace, path)
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )


def test_wrong_spec_hash_rejected_before_source_resolution(tmp_path: Path) -> None:
    # Given a published pin, When a wrong spec hash is supplied, Then its old count is unchanged.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "source.sqlite3", [_source_row()])
        _register(workspace, path)
        module = importlib.import_module("aegis_alpha.storage.research_inputs")
        with pytest.raises(ValueError, match="SHA-256"):
            module.register_price_input(workspace, path, "0" * 64)
        assert publication.read_dataset(workspace, "synthetic-prices", "1")["row_count"] == 1


def test_reference_prices_cannot_enter_canonical_generation_chain(tmp_path: Path) -> None:
    # Given a canonical pin, When a reference ASSERT appends, Then require a separate dataset.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = _spec(workspace, tmp_path / "first.sqlite3", [_source_row()])
        _register(workspace, first)
        second = _spec(
            workspace,
            tmp_path / "reference.sqlite3",
            [
                {
                    **_source_row(),
                    "price_role": "reference",
                    "record_id": "6332d942dd3ac8aca79649e5f79b7d786511abf3e5fd3c25bef06a95b4308d5a",
                }
            ],
        )
        _change(
            second, "price", {"basis": "unadjusted", "currency": "USD", "price_role": "reference"}
        )
        _change(
            second,
            "dataset",
            {
                "dataset_id": "synthetic-prices",
                "version": "2",
                "generation_id": "g2",
                "operation_id": "op2",
                "parent_id": "g1",
            },
        )
        with pytest.raises(ValueError, match="separate dataset"):
            _register(workspace, second)
        assert workspace.market.execute(
            "SELECT generation_id FROM market_generations"
        ).fetchall() == [("g1",)]


def test_final_import_envelope_respects_byte_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given 2000 rows of 510 bytes each, JSON array framing alone totals 1,022,001 bytes.
    # The independent fixed limit excludes that envelope, even if per-row sums fit.
    rows = []
    for index in range(2000):
        row = {
            **_source_row(),
            "session_date": (date(2020, 1, 1) + timedelta(days=index)).isoformat(),
        }
        natural = [
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
        ]
        row["record_id"] = hashlib.sha256(
            json.dumps(
                ["aas-record-v1", "prices", natural], sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        rows.append(row)
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "source.sqlite3", rows)
        path.write_text(json.dumps(json.loads(path.read_bytes()), separators=(",", ":")))
        monkeypatch.setattr("aegis_alpha.storage.research_inputs._MAX_BYTES", 1_022_000)
        # When admitting the complete serialized envelope, Then reject before publication.
        with pytest.raises(ValueError, match="bounded import size"):
            _register(workspace, path)
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )
