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

from aegis_alpha.storage import import_document, market, publication, source_library
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_publication import document

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

_RECORD = "5ed0dd479a146fe98960d7848d39b527f85b9ffeb0d151a2edf747b0f2611ac9"
_NUMBERS = ("open", "high", "low", "close", "volume")
_INEXACT_FLOAT = 0.1
NON_UTF8_TRANSFORMS = [
    pytest.param(encoding, bom, id=encoding + ("-bom" if bom else "-bomless"))
    for encoding, marker in (
        ("utf-16-le", b"\xff\xfe"),
        ("utf-16-be", b"\xfe\xff"),
        ("utf-32-le", b"\xff\xfe\x00\x00"),
        ("utf-32-be", b"\x00\x00\xfe\xff"),
    )
    for bom in (marker, b"")
]


def registration_spec(workspace: Workspace, path: Path, kind: str) -> Path:
    match kind:
        case "prices":
            return _spec(workspace, path, [_source_row()])
        case "sessions":
            return _sessions_spec(workspace, path, {})
        case "proxy":
            return _proxy_spec(workspace, path, ("PROXY", "v1", "0.1"))
        case _:
            raise AssertionError(kind)


def registration_state(workspace: Workspace) -> dict[str, object]:
    """Snapshot catalog/operations/contracts, market rows and immutable raw files."""
    tables: list[tuple[str]] = workspace.market.execute("SHOW TABLES").fetchall()
    return {
        "state": "\n".join(workspace.state.iterdump()),
        "market": {table: workspace.market.table(table).fetchall() for (table,) in tables},
        "raw": {
            str(path.relative_to(workspace.paths.raw)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in workspace.paths.raw.rglob("*")
            if path.is_file()
        },
    }


@pytest.mark.parametrize("kind", ["price", "sessions", "proxy"])
def test_latest_destination_rejected_before_publication(tmp_path: Path, kind: str) -> None:
    # Given a historical generic latest publication and otherwise valid retained sources.
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        historical = document().replace(b'"version": "1"', b'"version": "latest"')
        historical = historical.replace(b'"revision_id": "r1"', b'"revision_id": "historical-r1"')
        _ = publication.publish_document(workspace, import_document.parse_import(historical))
        old = publication.read_dataset(workspace, "synthetic-prices", "latest")
        path = registration_spec(
            workspace, tmp_path / "source.sqlite3", "prices" if kind == "price" else kind
        )
        destination = {
            "dataset_id": "candidate",
            "version": "latest",
            "generation_id": "candidate",
            "operation_id": "op-candidate",
            "parent_id": None,
        }
        _change(path, "dataset", destination)
        before = registration_state(workspace)
        # When admitting lowercase latest, Then no raw, contract, intent or market writes occur.
        with pytest.raises(ValueError, match=r"version.*latest"):
            _ = _register_domain(workspace, path, kind)
        assert registration_state(workspace) == before
        assert publication.read_dataset(workspace, "synthetic-prices", "latest") == old
        assert market.read_generation(workspace.market, "synthetic-generation")[0][
            "close"
        ] == Decimal(11)
        # The same retained sources really publish with an ordinary exact destination version.
        _change(path, "dataset", {**destination, "version": "1"})
        result = _register_domain(workspace, path, kind)
        assert result["published"] is True
        committed = registration_state(workspace)
        assert _register_domain(workspace, path, kind) == result
        assert registration_state(workspace) == committed
        assert publication.read_dataset(workspace, "synthetic-prices", "latest") == old


@pytest.mark.parametrize("kind", ["price", "sessions", "proxy"])
@pytest.mark.parametrize(("encoding", "bom"), NON_UTF8_TRANSFORMS)
def test_non_utf8_transform_rejected_before_publication(
    tmp_path: Path, kind: str, encoding: str, bom: bytes
) -> None:
    # Given otherwise valid retained sources and the exact hash of each transport encoding.
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = registration_spec(
            workspace, tmp_path / "source.sqlite3", "prices" if kind == "price" else kind
        )
        _ = path.write_bytes(bom + path.read_text(encoding="utf-8").encode(encoding))
        before = registration_state(workspace)
        # When registering, Then reject the encoding without changing any publication state.
        with pytest.raises(ValueError, match=r"UTF-8|utf-8"):
            _ = _register_domain(workspace, path, kind)
        assert registration_state(workspace) == before


@pytest.mark.parametrize("kind", ["price", "sessions", "proxy"])
@pytest.mark.parametrize("fault", ["duplicate", "nonfinite"])
def test_utf8_transform_keeps_strict_json_validation(tmp_path: Path, kind: str, fault: str) -> None:
    # Given valid source pins but duplicate fields or nonfinite JSON in a UTF-8 spec.
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = registration_spec(
            workspace, tmp_path / "source.sqlite3", "prices" if kind == "price" else kind
        )
        raw = path.read_bytes()
        if fault == "duplicate":
            raw = raw[:-1] + b',"provider":"synthetic"}'
        else:
            raw = raw.replace(b'"publication_at_us": null', b'"publication_at_us": NaN')
        _ = path.write_bytes(raw)
        before = registration_state(workspace)
        # When decoding through the boundary, Then strict shared-codec validation still applies.
        with pytest.raises(ValueError, match=r"duplicate|non-finite"):
            _ = _register_domain(workspace, path, kind)
        assert registration_state(workspace) == before


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


def _register_domain(workspace: Workspace, path: Path, domain: str) -> dict[str, object]:
    module = importlib.import_module("aegis_alpha.storage.research_inputs")
    register = getattr(module, "register_" + domain + "_input", None)
    assert callable(register), f"retained {domain} registration is unavailable"
    result = register(workspace, path, hashlib.sha256(path.read_bytes()).hexdigest())
    assert isinstance(result, dict)
    return result


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _sessions_spec(workspace: Workspace, path: Path, fields: dict[str, object]) -> Path:
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
    common.update(available_at_us=None, revision_known_at_us=None)
    row = {
        **common,
        "calendar_id": "CAL",
        "venue": "SYNTHETIC",
        "session_date": "2020-01-02",
        "open_at_us": 10,
        "close_at_us": 20,
        "status": "open",
        "timezone_version": "synthetic-v1",
        **fields,
    }
    natural = ["calendar_id", "venue", "session_date"]
    row["record_id"] = _hash_json(
        ["aas-record-v1", "calendar_sessions", [[k, row[k]] for k in natural]]
    )
    spec = _spec(workspace, path, [row])
    body = json.loads(spec.read_bytes())
    for key in ("price", "decimal_conversion", "calendar"):
        del body[key]
    body["schema_version"] = "aas-sessions-transform-v1"
    body["dataset"] = {
        "dataset_id": "sessions",
        "version": "1",
        "generation_id": "sessions",
        "operation_id": "op-sessions",
        "parent_id": None,
    }
    body["instruments"] = []
    body["calendar"] = {
        "calendar_id": "CAL",
        "venue": "SYNTHETIC",
        "timezone": "Etc/UTC",
        "timezone_version": "synthetic-v1",
    }
    spec.write_text(json.dumps(body, indent=2))
    return spec


def _proxy_definition(source: dict[str, object]) -> dict[str, object]:
    return {
        "proxy_id": "PROXY",
        "version": "v1",
        "normalization": {"input_number": "decimal_string", "output": "ieee754_binary64"},
        "transition": {
            "donor_id": "DONOR",
            "target_id": "TARGET",
            "logical_exposure_id": "EXPOSURE",
            "switch_decision_date": "2020-01-02",
            "mode": "signal_only",
            "donor_source": source,
            "target_source": source,
            "basis_ref": {"id": "basis", "version": "1", "sha256": "d" * 64},
            "calendar_ref": {"id": "CAL", "version": "1", "sha256": "e" * 64},
            "cost_ref": {"id": "cost", "version": "1", "sha256": "f" * 64},
        },
    }


def _proxy_spec(
    workspace: Workspace, path: Path, identity: tuple[str, str, str | float | None]
) -> Path:
    # The proxy definition pins donor/target inputs independently of its point table.
    donor = _spec(workspace, path.with_name(path.stem + "-donor.sqlite3"), [_source_row()])
    definition = _proxy_definition(json.loads(donor.read_bytes())["source"])
    proxy_id, version, value = identity
    definition.update(proxy_id=proxy_id, version=version)
    if isinstance(value, float):
        definition["normalization"] = {"input_number": "ieee_float", "output": "ieee754_binary64"}
    transition = definition["transition"]
    assert isinstance(transition, dict)
    # Bundle hashes cover immutable inputs, not generated floating-point output.
    inputs = [
        transition[k]
        for k in ("donor_source", "target_source", "basis_ref", "calendar_ref", "cost_ref")
    ]
    row = {
        k: v
        for k, v in _source_row().items()
        if k
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
    row.update(
        contract_id=proxy_id,
        contract_version=version,
        contract_hash=_hash_json(definition),
        input_bundle_hash=_hash_json(inputs),
        instrument_id="EXPOSURE",
        feature_at_us=20,
        value=value,
        value_state="missing" if value is None else "present",
        available_at_us=None,
        revision_known_at_us=None,
    )
    natural = [
        "contract_id",
        "contract_version",
        "input_bundle_hash",
        "instrument_id",
        "feature_at_us",
    ]
    row["record_id"] = _hash_json(
        ["aas-record-v1", "feature_values", [[k, row[k]] for k in natural]]
    )
    spec = _spec(workspace, path, [row])
    body = json.loads(spec.read_bytes())
    for key in ("price", "calendar", "decimal_conversion"):
        del body[key]
    body.update(
        schema_version="aas-proxy-transform-v1",
        proxy=definition,
        instruments=[{"instrument_id": "EXPOSURE", "asset_type": "proxy", "venue": "SYNTHETIC"}],
    )
    body["dataset"] = {
        "dataset_id": path.stem,
        "version": "1",
        "generation_id": path.stem,
        "operation_id": "op-" + path.stem,
        "parent_id": None,
    }
    spec.write_text(json.dumps(body, indent=2))
    return spec


def test_sessions_preserve_explicit_times_and_unknown_publication(tmp_path: Path) -> None:
    # Given explicit source times, When registered, Then neither knowledge nor timezone is invented.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _sessions_spec(workspace, tmp_path / "sessions.sqlite3", {})
        result = _register_domain(workspace, path, "sessions")
        assert _register_domain(workspace, path, "sessions") == result
        row = market.read_generation(workspace.market, "sessions")[0]
        assert [
            row[k] for k in ("session_date", "open_at_us", "close_at_us", "timezone_version")
        ] == [date(2020, 1, 2), 10, 20, "synthetic-v1"]
        assert row["available_at_us"] is row["revision_known_at_us"] is None
        assert (
            workspace.state.execute("SELECT publication_at_us FROM source_snapshots").fetchall()[0][
                0
            ]
            is None
        )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert (workspace.paths.raw / digest[:2] / digest).read_bytes() == path.read_bytes()


def test_proxy_versions_and_distinct_ids_preserve_contracts(tmp_path: Path) -> None:
    # Given two versions and a second ID, When registered, Then each has its own immutable contract.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        for name, identity in (
            ("p1", ("PROXY", "v1", "0.1")),
            ("p2", ("PROXY", "v2", "125.5")),
            ("p3", ("OTHER", "v1", "0.1")),
        ):
            path = _proxy_spec(workspace, tmp_path / (name + ".sqlite3"), identity)
            body = json.loads(path.read_bytes())
            result = _register_domain(workspace, path, "proxy")
            assert _register_domain(workspace, path, "proxy") == result
            row = market.read_generation(workspace.market, name)[0]
            assert (row["contract_id"], row["contract_version"], row["contract_hash"]) == (
                *identity[:2],
                _hash_json(body["proxy"]),
            )
            assert isinstance(row["value"], float)
            assert row["value"].hex() == (
                "0x1.f600000000000p+6" if name == "p2" else "0x1.999999999999ap-4"
            )
            contract = workspace.state.execute(
                "SELECT definition, record_schema, content_hash FROM feature_contracts "
                "WHERE name=? AND version=?",
                identity[:2],
            ).fetchone()
            assert tuple(contract) == (
                json.dumps(
                    body["proxy"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ),
                "aas-market-rowset-v1",
                _hash_json(body["proxy"]),
            )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            spec_pin = workspace.state.execute(
                "SELECT ref_id, ref_version, content_hash FROM feature_inputs "
                "WHERE name=? AND version=? AND ref_kind='transform'",
                identity[:2],
            ).fetchone()
            assert spec_pin is not None
            assert tuple(spec_pin) == (digest, "aas-proxy-transform-v1", digest)
        assert workspace.market.execute("SELECT count(*) FROM prices").fetchone() == (0,)


@pytest.mark.parametrize(
    "fault", ["switch", "time", "policy", "source", "execution", "held", "asset-type"]
)
def test_proxy_invalid_contracts_fail_before_publication(tmp_path: Path, fault: str) -> None:
    # Given inadmissible proxy metadata, When registering, Then no generation appears.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "p1.sqlite3", ("PROXY", "v1", "0.1"))
        body = json.loads(path.read_bytes())
        match fault:
            case "switch":
                del body["proxy"]["transition"]["switch_decision_date"]
            case "time":
                del body["columns"]["feature_at_us"]
            case "policy":
                body["proxy"]["normalization"]["output"] = "decimal_exact"
            case "source":
                body["proxy"]["transition"]["donor_source"]["source_sha256"] = "0" * 64
            case "execution":
                body["price"] = {"price_role": "canonical"}
            case "held":
                body["proxy"]["transition"]["held"] = True
            case "asset-type":
                body["instruments"][0]["asset_type"] = "equity"
        path.write_text(json.dumps(body))
        with pytest.raises((ValueError, TypeError)):
            _register_domain(workspace, path, "proxy")
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )


@pytest.mark.parametrize("fault", ["time", "timezone", "calendar"])
def test_sessions_missing_sources_fail_before_publication(tmp_path: Path, fault: str) -> None:
    # Given a missing time mapping or conflicting calendar, When registering, Then fail closed.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _sessions_spec(workspace, tmp_path / "sessions.sqlite3", {})
        body = json.loads(path.read_bytes())
        if fault == "time":
            del body["columns"]["open_at_us"]
        else:
            body["calendar"]["timezone" if fault == "timezone" else "calendar_id"] = "INVALID"
        path.write_text(json.dumps(body))
        with pytest.raises((ValueError, TypeError)):
            _register_domain(workspace, path, "sessions")
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )


@pytest.mark.parametrize(
    ("value", "expected"), [(None, None), (0.5, "0x1.0000000000000p-1"), ("1e-400", "0x0.0p+0")]
)
def test_proxy_numeric_policy_and_missingness(
    tmp_path: Path, value: str | float | None, expected: str | None
) -> None:
    # Given declared original types, When admitted, Then binary64 and null policy is observable.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "points.sqlite3", ("PROXY", "v1", value))
        _register_domain(workspace, path, "proxy")
        row = market.read_generation(workspace.market, "points")[0]
        stored = row["value"]
        assert (stored.hex() if isinstance(stored, float) else stored) == expected
        assert row["value_state"] == ("missing" if value is None else "present")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1e309", "not-a-number"])
def test_proxy_nonfinite_and_malformed_values_rejected(tmp_path: Path, value: str) -> None:
    # Given inadmissible source numbers, When converting, Then no generation is visible.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _proxy_spec(workspace, tmp_path / "points.sqlite3", ("PROXY", "v1", value))
        with pytest.raises(ValueError, match="proxy"):
            _register_domain(workspace, path, "proxy")
        assert workspace.market.execute("SELECT count(*) FROM market_generations").fetchone() == (
            0,
        )


@pytest.mark.parametrize(
    "fields",
    [
        {"status": "closed", "open_at_us": None, "close_at_us": None},
        {"open_at_us": None},
        {"close_at_us": 10},
        {"open_at_us": -1},
    ],
)
def test_sessions_closed_nulls_and_invalid_times(tmp_path: Path, fields: dict[str, object]) -> None:
    # Given explicit closed nulls or invalid open times, When registering, Then never fill them.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _sessions_spec(workspace, tmp_path / "sessions.sqlite3", fields)
        if fields.get("status") == "closed":
            _register_domain(workspace, path, "sessions")
            row = market.read_generation(workspace.market, "sessions")[0]
            assert row["open_at_us"] is row["close_at_us"] is None
            assert row["status"] == "closed"
        else:
            with pytest.raises(ValueError, match="session status"):
                _register_domain(workspace, path, "sessions")
            assert workspace.market.execute(
                "SELECT count(*) FROM market_generations"
            ).fetchone() == (0,)


def test_proxy_conflicting_same_key_definition_preserves_old_pin(tmp_path: Path) -> None:
    # Given a committed contract, When a new definition reuses its key, Then the old pin survives.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = _proxy_spec(workspace, tmp_path / "first.sqlite3", ("PROXY", "v1", "0.1"))
        _register_domain(workspace, first, "proxy")
        old = publication.read_dataset(workspace, "first", "1")
        second = _proxy_spec(workspace, tmp_path / "second.sqlite3", ("PROXY", "v1", "125.5"))
        with pytest.raises(ValueError, match="conflict"):
            _register_domain(workspace, second, "proxy")
        assert publication.read_dataset(workspace, "first", "1") == old
        assert workspace.market.execute(
            "SELECT generation_id FROM market_generations"
        ).fetchall() == [("first",)]


def test_proxy_instrument_cannot_be_registered_as_ohlc(tmp_path: Path) -> None:
    # Given complete but synthetic OHLC, When labeled as a proxy, Then it cannot enter prices.
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        path = _spec(workspace, tmp_path / "proxy.sqlite3", [_source_row()])
        _change(
            path,
            "instruments",
            [{"instrument_id": "ASSET_A", "asset_type": "proxy", "venue": "SYNTHETIC"}],
        )
        with pytest.raises(ValueError, match="proxy"):
            _register(workspace, path)
        assert workspace.market.execute("SELECT count(*) FROM prices").fetchone() == (0,)


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
