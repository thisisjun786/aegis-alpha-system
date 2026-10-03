"""Source retirement: references, equivalence digest and other-device backup, on synthetic data."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from aegis_alpha.storage import publication, source_library, source_retirement
from aegis_alpha.storage.backup import backup
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.raw import put_raw, verify_raw
from aegis_alpha.storage.rowset import rowset_hash
from aegis_alpha.storage.source_retirement import (
    RetirementError,
    plan_retirement,
    request_hash,
    retire_sources,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.promotion_support import (
    add_source,
    at,
    bar,
    register_symbols,
)
from tests.storage.promotion_support import (
    spec as promotion_spec,
)
from tests.storage.retirement_support import (
    ROWS,
    SEEN,
    commit,
    document,
    group,
    other_device,
    spec,
)

_ROOT = Path(__file__).resolve().parents[2]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_TYPES = (
    ("symbol", "text"),
    ("day", "date"),
    ("close", "float"),
    ("volume", "int"),
    ("seen", "utc_us"),
    ("ok", "bool"),
)


def _expected_digest(rows: tuple[tuple[object, ...], ...]) -> str:
    """The aas-rowset-v1 digest of ROWS computed independently in Python."""
    return rowset_hash(
        _TYPES,
        [
            {
                "symbol": row[0],
                "day": row[1],
                "close": row[2],
                "volume": row[3],
                "seen": None
                if row[4] is None
                else (cast("datetime", row[4]) - _EPOCH) // cast("datetime", row[4]).resolution,
                "ok": row[5],
            }
            for row in rows
        ],
    )


@pytest.fixture
def home(tmp_path: Path) -> Path:
    root = tmp_path / "aas"
    initialize(root)
    return root


@pytest.fixture
def writable(home: Path) -> Iterator[Workspace]:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        yield workspace


def _present(workspace: Workspace, source_id: str) -> bool:
    row = workspace.market.execute(
        "SELECT manifest_json FROM source_library_commits WHERE source_id=?", [source_id]
    ).fetchone()
    assert row is not None
    target = json.loads(row[0])["tables"][0]["target"]
    return source_library.table_present(workspace.market, target)


def _records(workspace: Workspace) -> dict[str, dict[str, object]]:
    return source_library.retired_sources(workspace)


def _group_status(report: dict[str, object]) -> list[tuple[str, list[str]]]:
    groups = cast("list[dict[str, object]]", report["groups"])
    return [(str(item["status"]), cast("list[str]", item["reasons"])) for item in groups]


def test_retirement_requires_proof(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
        changed = commit(
            workspace, "changed", (*ROWS[:-1], ("AAA", date(2025, 1, 2), 1.0, 1, SEEN, True))
        )
        referenced = commit(workspace, "referenced", ROWS[1:] + ROWS[:1])
        register_symbols(workspace, referenced)
    backup(home, tmp_path / "backup")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        late = commit(workspace, "late", ROWS[2:] + ROWS[:2])
        request = spec(
            group([copy], [export]),
            group([changed], [export]),
            group([referenced], [export]),
            group([late], [export]),
        )
        same_device = retire_sources(
            workspace, request, backup_root=tmp_path / "backup", apply=False
        )
        assert {reason for _, reasons in _group_status(same_device) for reason in reasons} >= {
            "backup_on_installation_device"
        }
        missing = retire_sources(workspace, request, backup_root=None, apply=False)
        assert all("backup_missing" in reasons for _, reasons in _group_status(missing))
        other_device(monkeypatch, tmp_path / "backup")
        planned = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=False)
        assert planned["writes"] == 0
        assert _group_status(planned) == [
            ("retire", []),
            ("refused", ["not_equivalent"]),
            ("refused", ["referenced"]),
            ("refused", ["backup_lacks_source"]),
        ]
        sources = {
            item["source_id"]: item for item in cast("list[dict[str, object]]", planned["sources"])
        }
        assert sources[referenced]["references"] == ["state.identity_assertions.source_snapshot_id"]
        refused = spec(
            group([changed], [export]), group([referenced], [export]), group([late], [export])
        )
        applied = retire_sources(workspace, refused, backup_root=tmp_path / "backup", apply=True)
        assert applied["retired"] == []
        assert applied["operation_id"] is None
        assert all(_present(workspace, source) for source in (changed, referenced, late))
        assert _records(workspace) == {}
        with pytest.raises(RetirementError, match="--backup"):
            retire_sources(workspace, refused, backup_root=None, apply=True)


def test_promotion_pins_and_rows_are_references(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        identity_source = commit(workspace, "identity")
        pin = add_source(
            workspace,
            [bar("AAA.KO", date(2025, 1, 2), 100.0, retrieved=at("2025-01-10T00:00:00"))],
            tag="p",
        )
        identity = register_symbols(workspace, identity_source)
        raw, digest = promotion_spec([pin], identity)
        promote(workspace, raw, digest, apply=True)
        found = source_retirement.source_references(workspace, {pin["source_id"]})[pin["source_id"]]
    assert "state.dataset_sources.source_snapshot_id" in found
    assert "market.prices.source_snapshot_id" in found
    assert any(place.startswith("generation ") and digest in place for place in found)


def test_unreferenced_equivalent_backed_up_source_is_retired(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
        first = commit(workspace, "first", ROWS[:2], path="part-0")
        second = commit(workspace, "second", ROWS[2:], path="part-1")
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    request = spec(group([copy], [export]), group([first, second], [export]))
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        links = workspace.state.execute(
            "SELECT count(*) FROM source_snapshots WHERE snapshot_id IN (?,?,?)",
            ("sl:" + copy, "sl:" + first, "sl:" + second),
        ).fetchone()
        assert tuple(links) == (3,)
        applied = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=True)
        assert applied["retired"] == sorted([copy, first, second])
        assert applied["retired_rows"] == 2 * len(ROWS)
        assert not any(_present(workspace, source) for source in (copy, first, second))
        assert _present(workspace, export)
        records = _records(workspace)
        manifest = workspace.market.execute(
            "SELECT manifest_json FROM source_library_commits WHERE source_id=?", [copy]
        ).fetchone()
        assert manifest is not None
        backup_id = hashlib.sha256((tmp_path / "backup" / "backup.json").read_bytes()).hexdigest()
        record = records[copy]
        assert record["digest"] == hashlib.sha256(manifest[0].encode()).hexdigest()
        assert record["rows"] == len(ROWS)
        assert record["equivalence_digest"] == _expected_digest(ROWS)
        assert records[first]["equivalence_digest"] == _expected_digest(ROWS)
        assert record["equivalent_to_source_id"] == export
        assert record["backup_id"] == backup_id
        assert json.loads(str(records[first]["equivalence_spec"]))["retire"]["sources"] == [
            first,
            second,
        ]
        operation = workspace.state.execute(
            "SELECT kind,phase FROM storage_operations WHERE operation_id=?",
            (record["operation_id"],),
        ).fetchone()
        assert tuple(operation) == ("source-retire", "COMPLETED")
        # The source's own link is lineage: it stays, and so do its raw files.
        assert tuple(links) == tuple(
            workspace.state.execute(
                "SELECT count(*) FROM source_snapshots WHERE snapshot_id IN (?,?,?)",
                ("sl:" + copy, "sl:" + first, "sl:" + second),
            ).fetchone()
        )
        listed = {row["source_id"] for row in source_library.list_sources(workspace)}
        assert listed == {export}
        with pytest.raises(ValueError, match="is retired"):
            source_library.list_tables(workspace, copy)
        report = cast("dict[str, dict[str, object]]", verify_workspace(workspace))
        assert report["source_library"]["retired"] == {"sources": 3, "rows": 2 * len(ROWS)}
        assert report["source_library"]["sources"] == 1
        again = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=True)
        assert again["retired"] == []
        assert _group_status(again) == [("already_retired", []), ("already_retired", [])]
        # Importing the same unit again is its earlier import, reused and never rebuilt.
        assert commit(workspace, "copy", ROWS[::-1], path="normalized") == copy
        assert not _present(workspace, copy)
        assert source_library.committed_source_ids(workspace) == {export, copy, first, second}
        with pytest.raises(ValueError, match="different content"):
            commit(workspace, "copy", ROWS, path="other")
        target = retire_sources(
            workspace,
            spec(group([export], [first])),
            backup_root=tmp_path / "backup",
            apply=False,
        )
        assert "equivalence_target_of_a_retired_source" in _group_status(target)[0][1]
        assert "equivalent_source_retired" in _group_status(target)[0][1]


def test_retirement_keeps_raw_bytes(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
        files = workspace.state.execute(
            "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=?",
            ("sl:" + copy,),
        ).fetchall()
    before = {
        path.relative_to(home / "raw") for path in (home / "raw").rglob("*") if path.is_file()
    }
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = retire_sources(
            workspace, spec(group([copy], [export])), backup_root=tmp_path / "backup", apply=True
        )
        assert applied["raw_deleted"] is False
        assert files
        for relative, digest, size in files:
            verify_raw(workspace.paths.raw, relative, digest, size)
    after = {path.relative_to(home / "raw") for path in (home / "raw").rglob("*") if path.is_file()}
    # Nothing in raw/ goes; the apply adds only its retained records.
    assert before < after
    assert len(after - before) == 1


def test_equivalence_digest_is_rowset_v1_of_typed_cells(writable: Workspace) -> None:
    schema = pa.schema(
        [
            ("small", pa.int8()),
            ("wide", pa.uint32()),
            ("single", pa.float32()),
            ("amount", pa.decimal128(20, 4)),
            ("seen", pa.timestamp("us")),
            ("label", pa.string()),
            ("exact", pa.timestamp("ns")),
        ]
    )
    rows = (
        (
            -3,
            4_000_000_000,
            0.1,
            Decimal("-12.5000"),
            datetime(2024, 2, 29, 23, 59, 59, 999999),  # noqa: DTZ001 -- a naive TIMESTAMP
            "é",
            1_700_000_000_123_456_000,
        ),
        (None, 0, None, Decimal("0.0000"), None, "", -1_000),
        (127, None, -2.5, None, datetime(1969, 12, 31, 23, 59, 59), None, None),  # noqa: DTZ001
    )
    left = commit(writable, "left", rows, schema=schema)
    right = commit(writable, "right", rows[::-1], schema=schema)
    names = tuple(schema.names)
    plan = plan_retirement(writable, spec(group([left], [right], columns=names)), backup_root=None)
    naive = datetime(1970, 1, 1)  # noqa: DTZ001 -- a naive TIMESTAMP is read as UTC microseconds
    expected = rowset_hash(
        tuple(
            zip(names, ("int", "int", "float", "decimal", "utc_us", "text", "utc_us"), strict=True)
        ),
        [
            {
                "small": row[0],
                "wide": row[1],
                "single": None
                if row[2] is None
                else float(pa.scalar(row[2], pa.float32()).as_py()),
                "amount": None if row[3] is None else row[3].quantize(Decimal("1e-12")),
                "seen": None if row[4] is None else (row[4] - naive) // row[4].resolution,
                "label": row[5],
                "exact": None if row[6] is None else row[6] // 1000,
            }
            for row in rows
        ],
    )
    assert plan.groups[0].retire_digest == plan.groups[0].equivalent_digest == expected
    assert plan.groups[0].reasons == ["backup_missing"]


@pytest.mark.parametrize(
    ("field", "values", "reason"),
    [
        (pa.field("x", pa.float64()), [1.0, float("nan")], "non_finite_float"),
        (pa.field("x", pa.uint64()), [1, 2**63], "values_not_encodable"),
        (pa.field("x", pa.binary()), [b"a", b"b"], "column_type_unsupported"),
        (pa.field("x", pa.timestamp("ns")), [1_000, 1_001], "values_not_encodable"),
    ],
)
def test_values_without_an_exact_rowset_form_refuse_the_group(
    writable: Workspace, field: pa.Field, values: list[object], reason: str
) -> None:
    schema = pa.schema([field])
    left = commit(writable, "left", [(value,) for value in values], schema=schema)
    right = commit(writable, "right", [(value,) for value in values[::-1]], schema=schema)
    plan = plan_retirement(writable, spec(group([left], [right], columns=["x"])), backup_root=None)
    assert reason in plan.groups[0].reasons


def test_retirement_documents_are_refused_when_ambiguous(writable: Workspace) -> None:
    export = commit(writable, "export")
    copy = commit(writable, "copy")
    bad = [
        {"schema_version": "aas-source-retirement-v2", "groups": [group([copy], [export])]},
        {"schema_version": "aas-source-retirement-v1", "groups": []},
        {"schema_version": "aas-source-retirement-v1", "groups": [group([copy], [copy])]},
        {
            "schema_version": "aas-source-retirement-v1",
            "groups": [group([copy], [export]), group([copy], [export])],
        },
        {
            "schema_version": "aas-source-retirement-v1",
            "groups": [group([copy], [export], equivalent_columns=["symbol"])],
        },
        {
            "schema_version": "aas-source-retirement-v1",
            "groups": [{**group([copy], [export]), "extra": 1}],
        },
    ]
    for body in bad:
        raw = json.dumps(body).encode()
        with pytest.raises(RetirementError):
            source_retirement.parse_spec(raw, hashlib.sha256(raw).hexdigest())
    raw, digest = document(group([copy], [export]))
    with pytest.raises(RetirementError, match="SHA-256"):
        source_retirement.parse_spec(raw, "0" * 64)
    plan = plan_retirement(
        writable, spec(group(["synthetic-bars-" + "0" * 64], [export])), backup_root=None
    )
    assert "unknown_source" in plan.groups[0].reasons
    short = commit(writable, "short", ROWS[:2])
    plan = plan_retirement(writable, spec(group([short], [export])), backup_root=None)
    assert "row_counts_differ" in plan.groups[0].reasons
    assert raw
    assert digest


def test_retirement_request_hash_format_is_frozen() -> None:
    record: dict[str, object] = {
        "source_id": "synthetic-bars-" + "a" * 64,
        "digest": "b" * 64,
        "rows": 5,
        "reason": "a copy of the export",
        "equivalent_to_source_id": "synthetic-bars-" + "c" * 64,
        "equivalence_spec": '{"format":"aas-source-equivalence-v1"}',
        "equivalence_digest": "d" * 64,
        "backup_id": "e" * 64,
    }
    preimage = (
        b'{"backup_id":"'
        + b"e" * 64
        + b'","records":[{"backup_id":"'
        + b"e" * 64
        + b'","digest":"'
        + b"b" * 64
        + b'","equivalence_digest":"'
        + b"d" * 64
        + b'","equivalence_spec":"{\\"format\\":\\"aas-source-equivalence-v1\\"}",'
        + b'"equivalent_to_source_id":"synthetic-bars-'
        + b"c" * 64
        + b'","reason":"a copy of the export","rows":5,"source_id":"synthetic-bars-'
        + b"a" * 64
        + b'"}],"schema":"aas-source-retirement-request-v1","spec_sha256":"'
        + b"f" * 64
        + b'"}'
    )
    assert request_hash("f" * 64, "e" * 64, [record]) == hashlib.sha256(preimage).hexdigest()
    assert (
        request_hash("f" * 64, "e" * 64, [record])
        == "c908ecba57f78ee0b3b04795d2b692273d4bef43b2922ccbdc5fb6bee05a32ae"
    )


def test_interrupted_retirement_is_finished_not_quarantined(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    request = spec(group([copy], [export]))
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with monkeypatch.context() as patched:

            def crash(_connection: object) -> object:
                raise RuntimeError("stopped after the tables were dropped")

            patched.setattr(source_retirement, "atomic", crash)
            with pytest.raises(RuntimeError, match="stopped"):
                retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=True)
        assert not _present(workspace, copy)
        assert _records(workspace) == {}
        (operation,) = workspace.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind='source-retire' "
            "AND phase='PREPARED'"
        ).fetchall()
        with pytest.raises(ValueError, match="must be recovered"):
            publication.quarantine(workspace, operation[0], "operator stop")
        recovered = recover_operations(workspace)
        assert recovered["recovered"] == [operation[0]]
        assert set(_records(workspace)) == {copy}
        assert verify_workspace(workspace)["pending_operations"] == 0


def test_prepared_retirement_with_nothing_dropped_can_be_quarantined(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with monkeypatch.context() as patched:

            def stop(*_args: object, **_kwargs: object) -> bool:
                raise RuntimeError("stopped before any table was dropped")

            patched.setattr(source_retirement, "finish_retirement", stop)
            with pytest.raises(RuntimeError, match="stopped"):
                retire_sources(
                    workspace,
                    spec(group([copy], [export])),
                    backup_root=tmp_path / "backup",
                    apply=True,
                )
        (operation,) = workspace.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind='source-retire'"
        ).fetchall()
        assert _present(workspace, copy)
        publication.quarantine(workspace, operation[0], "operator stop")
        assert _present(workspace, copy)
        assert _records(workspace) == {}


def test_retirement_apply_needs_core_schema_v2(tmp_path: Path) -> None:
    from tests.storage.test_migration import v1_installation  # noqa: PLC0415

    home = v1_installation(tmp_path)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1])
        request = spec(group([copy], [export]))
        planned = retire_sources(workspace, request, backup_root=None, apply=False)
        assert planned["apply_needs_v2"] is True
        with pytest.raises(RetirementError, match="migrate --to 2"):
            retire_sources(workspace, request, backup_root=tmp_path, apply=True)


def _cli(*args: str, home: Path) -> dict[str, object]:
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "db", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_source_retire_command_plans_without_writing(home: Path, tmp_path: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        copy = commit(workspace, "copy", ROWS[::-1], path="normalized")
    raw, digest = document(group([copy], [export]))
    request = tmp_path / "retire.json"
    request.write_bytes(raw)
    files = {path: path.read_bytes() for path in (home / "market.duckdb", home / "state.sqlite3")}
    planned = _cli("source-retire", "--spec", str(request), "--sha256", digest, "--plan", home=home)
    assert planned["candidate_rows"] == len(ROWS)
    assert planned["references"] == 0
    assert _group_status(planned) == [("refused", ["backup_missing"])]
    assert {path: path.read_bytes() for path in files} == files
    backup(home, tmp_path / "backup")
    applied = _cli(
        "source-retire",
        "--spec",
        str(request),
        "--sha256",
        digest,
        "--backup",
        str(tmp_path / "backup"),
        "--apply",
        home=home,
    )
    # A backup on the installation's own device is no backup for retirement.
    assert applied["retired"] == []
    assert _group_status(applied) == [("refused", ["backup_on_installation_device"])]


def test_a_column_left_out_is_declared_and_recorded(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        export = commit(workspace, "export")
        # Every symbol differs: compared on ``ok`` alone the two are equal multisets.
        renamed = commit(
            workspace, "renamed", [("ZZZ", *row[1:]) for row in ROWS], path="normalized"
        )
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    left_out = ["symbol", "day", "close", "volume", "seen", "path"]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        silent = plan_retirement(
            workspace,
            spec(group([renamed], [export], columns=["ok"], uncompared=["path"])),
            backup_root=tmp_path / "backup",
        )
        assert silent.groups[0].reasons == ["uncompared_columns_differ"]
        assert silent.groups[0].retire_digest == silent.groups[0].equivalent_digest
        over = plan_retirement(
            workspace,
            spec(group([renamed], [export], uncompared=["path", "absent"])),
            backup_root=tmp_path / "backup",
        )
        assert "uncompared_columns_differ" in over.groups[0].reasons
        request = spec(group([renamed], [export], columns=["ok"], uncompared=left_out))
        planned = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=False)
        (reported,) = cast("list[dict[str, object]]", planned["groups"])
        assert (reported["status"], reported["compared"]) == ("retire", "partial_columns")
        assert reported["uncompared_columns"] == left_out
        assert planned["partial_column_groups"] == 1
        (source,) = cast("list[dict[str, object]]", planned["sources"])
        assert source["uncompared_columns"] == left_out
        applied = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=True)
        assert applied["retired"] == [renamed]
        recorded = json.loads(str(_records(workspace)[renamed]["equivalence_spec"]))
        assert recorded["uncompared"] == left_out
        assert recorded["retire"]["columns"] == ["ok"]
    raw = json.dumps(
        {
            "schema_version": "aas-source-retirement-v1",
            "groups": [group([renamed], [export], uncompared=["path", "ok"])],
        }
    ).encode()
    with pytest.raises(RetirementError, match="uncompared"):
        source_retirement.parse_spec(raw, hashlib.sha256(raw).hexdigest())


def test_feature_inputs_and_bindings_are_references(writable: Workspace) -> None:
    donor = commit(writable, "donor")
    bound = commit(writable, "bound")
    digest = "a" * 64
    with writable.state:
        writable.state.execute(
            "INSERT INTO feature_contracts VALUES (?,?,?,?,?)",
            ("synthetic-proxy", "1", "{}", "aas-proxy-transform-v1", digest),
        )
        writable.state.execute(
            "INSERT INTO feature_inputs VALUES (?,?,?,?,?,?,?)",
            ("synthetic-proxy", "1", 0, "donor_source:bars", donor, digest, digest),
        )
        writable.state.execute(
            "INSERT INTO input_bundles VALUES (?,?,?)", ("b-synthetic", digest, "synthetic")
        )
        writable.state.execute(
            "INSERT INTO input_bindings VALUES (?,?,?,?,?,?,?)",
            ("b-synthetic", "prices", 0, "source", bound, "v1", digest),
        )
    found = source_retirement.source_references(writable, {donor, bound})
    assert found == {
        donor: ["state.feature_inputs.ref_id"],
        bound: ["state.input_bindings.ref_id"],
    }
    export = commit(writable, "export")
    plan = plan_retirement(
        writable, spec(group([donor], [export]), group([bound], [export])), backup_root=None
    )
    assert all("referenced" in group.reasons for group in plan.groups)


def test_a_source_a_strategy_registration_names_is_referenced(home: Path, tmp_path: Path) -> None:
    from tests.storage.test_strategy_registry import (  # noqa: PLC0415
        _import,
        _register,
        _strategies,
    )

    digest = _import(home, tmp_path, "synthetic-records", _strategies())
    with open_workspace(home) as workspace:
        assert source_retirement.source_references(workspace, {"synthetic-records"}) == {
            "synthetic-records": []
        }
    _register(home, "synthetic-records", digest)
    with open_workspace(home) as workspace:
        assert source_retirement.source_references(workspace, {"synthetic-records"}) == {
            "synthetic-records": ["strategies.strategy_registrations.source_id"]
        }


def test_a_retired_legacy_unit_is_reused_and_verified(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aegis_alpha.storage.legacy_import.engine import (  # noqa: PLC0415
        apply_import,
        verify_import,
    )
    from aegis_alpha.storage.source_identity import SourceContent, SourceFile  # noqa: PLC0415
    from tests.storage.test_legacy_import import (  # noqa: PLC0415
        _entry,
        _export,
        _manifest,
        _sources,
    )

    manifest = _manifest([_entry("equity", "norgate.history_export@1", _export(tmp_path))])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        unit = _sources(apply_import(workspace, manifest))[0]
        (manifest_json,) = cast(
            "tuple[str]",
            workspace.market.execute(
                "SELECT manifest_json FROM source_library_commits WHERE source_id=?",
                [unit["source_id"]],
            ).fetchone(),
        )
        (table,) = json.loads(manifest_json)["tables"]
        rows = workspace.market.execute(
            f'SELECT * EXCLUDE (_aas_ordinal) FROM "{table["target"]}"'  # noqa: S608 -- marker name
        ).to_arrow_table()
        _, raw_digest, size = put_raw(workspace.paths.raw, b"synthetic-copy")
        copy = SourceContent("synthetic", "history", 1, (SourceFile(raw_digest, size),))
        source_library.import_content_arrow(workspace, copy, table["name"], rows.to_reader())
    backup(home, tmp_path / "backup")
    other_device(monkeypatch, tmp_path / "backup")
    request = spec(
        {
            "reason": "the same rows under another source",
            "uncompared": [],
            "retire": {
                "sources": [unit["source_id"]],
                "table": table["name"],
                "columns": table["columns"],
            },
            "equivalent": {
                "sources": [copy.source_id],
                "table": table["name"],
                "columns": table["columns"],
            },
        }
    )
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        applied = retire_sources(workspace, request, backup_root=tmp_path / "backup", apply=True)
        assert applied["retired"] == [unit["source_id"]]
        again = _sources(apply_import(workspace, manifest))
    assert [source["reused"] for source in again] == [True, True]
    with open_workspace(home) as workspace:
        verified = verify_import(workspace, manifest)
    assert [source["status"] for source in _sources(verified)] == ["retired", "committed"]
    assert verified["unmatched"] == 0
    assert verified["complete"] is True
