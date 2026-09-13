from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import replay
from aegis_alpha.engine.models import EngineContract
from aegis_alpha.storage import state
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.strategies import LineageSpec, load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle, request
from tests.engine.test_requirements import rich_contract

MACRO_ROWS = [
    (
        "macro",
        1,
        "engine-macro-v1",
        "MACRO_Y",
        "macro_observations",
        2,
        "not_applicable",
        "calendar_month_end",
    ),
    (
        "macro",
        2,
        "engine-macro-v1",
        "YIELD_Y",
        "macro_observations",
        1,
        "not_applicable",
        "calendar_month_end",
    ),
]


@pytest.mark.parametrize(
    ("value", "warmup", "macro_rows"),
    [
        pytest.param(contract(), 3, [], id="prices-only"),
        pytest.param(rich_contract(), 8, MACRO_ROWS, id="derived-macro-retained"),
        pytest.param(
            replace(
                rich_contract(), calendar=replace(rich_contract().calendar, history_observations=1)
            ),
            1,
            MACRO_ROWS,
            id="history-cap-not-feature-minimum",
        ),
    ],
)
def test_registered_requirements_preserve_complete_v1_rows(
    tmp_path: Path,
    value: EngineContract,
    warmup: int,
    macro_rows: list[tuple[str | int, ...]],
) -> None:
    # Given independent historical v1 rows, including derived signals as macro rows.
    expected = [
        ("synthetic-probe", "1", *row)
        for row in [
            *macro_rows,
            (
                "prices",
                1,
                "engine-price-v1",
                "close",
                "prices",
                warmup,
                "explicit-input",
                "calendar_month_end",
            ),
        ]
    ]
    payload = raw_bundle(value)
    digest = hashlib.sha256(payload).hexdigest()
    contract_digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    home = tmp_path / "aas"
    initialize(home)
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)

    # When registration (including idempotent reimport) is reloaded without the source.
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        first = register_strategy(workspace, source, digest, "synthetic-probe", "1")
        assert register_strategy(workspace, source, digest, "synthetic-probe", "1") == first
    source.unlink()
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        loaded = load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        rows = workspace.strategies.execute(
            "SELECT * FROM strategy_requirements ORDER BY role,ordinal"
        ).fetchall()

    # Then all ten stored columns and both identity hashes retain their v1 values.
    assert [tuple(row) for row in rows] == expected
    assert (loaded.source_sha256, loaded.contract_sha256) == (digest, contract_digest)
    assert loaded.contract == value


def test_registered_bundle_replays_after_original_file_removed(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        register_strategy(workspace, source, digest, "synthetic-probe", "1")
    source.unlink()
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        bundle = load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        result = replay(bundle, request())
        assert dict(result.ensemble) == {"ASSET_A": 1.0}
        assert result.source_sha256 == digest
        operations = workspace.state.execute("SELECT phase FROM storage_operations").fetchall()
        assert [row[0] for row in operations] == ["COMPLETED"]


def test_lineage_registration_failure_rolls_back_then_retries(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)
    lineage = LineageSpec("parent", "7", "derived", "synthetic")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        workspace.strategies.execute(
            "CREATE TEMP TRIGGER fail_lineage BEFORE INSERT ON strategy_lineage "
            "BEGIN SELECT RAISE(ABORT,'synthetic insertion failure'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="synthetic insertion failure"):
            register_strategy(workspace, source, digest, "synthetic-probe", "1", lineage=lineage)
        counts = workspace.strategies.execute(
            "SELECT (SELECT count(*) FROM strategies),"
            "(SELECT count(*) FROM strategy_versions),"
            "(SELECT count(*) FROM strategy_sources),"
            "(SELECT count(*) FROM strategy_requirements),"
            "(SELECT count(*) FROM strategy_lineage),"
            "(SELECT count(*) FROM strategy_imports)"
        ).fetchone()
        assert tuple(counts) == (0, 0, 0, 0, 0, 0)
        assert [
            row[0] for row in workspace.state.execute("SELECT phase FROM storage_operations")
        ] == ["PREPARED"]
        workspace.strategies.execute("DROP TRIGGER fail_lineage")
        first = register_strategy(
            workspace, source, digest, "synthetic-probe", "1", lineage=lineage
        )
        assert (
            register_strategy(workspace, source, digest, "synthetic-probe", "1", lineage=lineage)
            == first
        )
        with pytest.raises(ValueError, match="different lineage"):
            register_strategy(workspace, source, digest, "synthetic-probe", "1")
        assert [
            row[0] for row in workspace.state.execute("SELECT phase FROM storage_operations")
        ] == ["COMPLETED"]
        assert (
            workspace.strategies.execute("SELECT count(*) FROM strategy_imports").fetchone()[0] == 1
        )


def _interrupted_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parent_status: str
) -> tuple[Path, str, str]:
    home = tmp_path / "aas"
    initialize(home)
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    operation_id = (
        "strategy-" + hashlib.sha256(("synthetic-probe\x001\x00" + digest).encode()).hexdigest()
    )
    source = tmp_path / "synthetic.json"
    source.write_bytes(payload)
    lineage = (
        None if parent_status == "none" else LineageSpec("parent", "7", "derived", "synthetic")
    )
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if parent_status == "resolved":
            parent = tmp_path / "parent.json"
            parent_raw = payload.replace(b'"synthetic-probe"', b'"parent"').replace(
                b'"bundle_version":"1"', b'"bundle_version":"7"'
            )
            parent.write_bytes(parent_raw)
            register_strategy(
                workspace, parent, hashlib.sha256(parent_raw).hexdigest(), "parent", "7"
            )
            parent.unlink()

        def interrupt(connection: sqlite3.Connection, op_id: str, request_hash: str) -> None:
            assert connection is workspace.state
            assert (op_id, request_hash) == (operation_id, digest)
            assert workspace.strategies is not None
            assert not workspace.strategies.in_transaction
            # An independent connection acknowledges the actual private COMMIT boundary.
            reader = connect(workspace.paths.strategies, read_only=True)
            try:
                receipt = reader.execute(
                    "SELECT operation_id,request_hash,strategy_id,version FROM strategy_imports "
                    "WHERE operation_id=?",
                    (op_id,),
                ).fetchone()
                assert tuple(receipt) == (operation_id, digest, "synthetic-probe", "1")
            finally:
                reader.close()
            raise RuntimeError("synthetic interruption after private commit")

        with monkeypatch.context() as fault:
            fault.setattr(state, "complete_operation", interrupt)
            with pytest.raises(RuntimeError, match="synthetic interruption after private commit"):
                register_strategy(
                    workspace, source, digest, "synthetic-probe", "1", lineage=lineage
                )
        assert (
            workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()[0]
            == "PREPARED"
        )
    source.unlink()
    return home, digest, operation_id


@pytest.mark.parametrize("parent_status", ["unresolved", "none", "resolved"])
def test_recovery_finalizes_private_commit_without_changing_execution_eligibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parent_status: str
) -> None:
    home, digest, operation_id = _interrupted_registration(tmp_path, monkeypatch, parent_status)
    expected_lineage = (
        []
        if parent_status == "none"
        else [
            (
                "synthetic-probe",
                "1",
                "parent",
                "7",
                "derived",
                "synthetic",
                hashlib.sha256(b'"synthetic"').hexdigest(),
                parent_status,
            )
        ]
    )
    with open_workspace(home, writable=True) as workspace:
        assert workspace.strategies is not None
        before = "\n".join(workspace.strategies.iterdump())
        assert recover_operations(workspace) == {
            "recovered": [operation_id],
            "pending": [],
            "provider_calls": 0,
        }
        assert recover_operations(workspace) == {
            "recovered": [],
            "pending": [],
            "provider_calls": 0,
        }
        assert "\n".join(workspace.strategies.iterdump()) == before
        assert [
            tuple(row) for row in workspace.strategies.execute("SELECT * FROM strategy_lineage")
        ] == expected_lineage
        assert (
            workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()[0]
            == "COMPLETED"
        )
        if parent_status == "unresolved":
            with pytest.raises(ValueError, match="unresolved parent lineage"):
                load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        else:
            loaded = load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
            assert (loaded.source_sha256, loaded.contract_sha256) == (
                digest,
                hashlib.sha256(canonical_json_bytes(contract())).hexdigest(),
            )


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ("UPDATE strategy_imports SET request_hash='" + "0" * 64 + "'", "receipt"),
        ("UPDATE strategy_imports SET strategy_id='other'", "not registered"),
        ("UPDATE strategy_imports SET version='2'", "not registered"),
        ("UPDATE strategy_versions SET raw_bundle=x'7b7d'", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET raw_sha256='" + "0" * 64 + "'", "execution pin"),
        ("UPDATE strategy_versions SET contract_json='{}'", "parsed contract hash"),
        ("UPDATE strategy_versions SET contract_sha256='" + "0" * 64 + "'", "parsed contract hash"),
    ],
    ids=[
        "receipt-hash",
        "receipt-id",
        "receipt-version",
        "raw",
        "raw-pin",
        "contract",
        "contract-pin",
    ],
)
def test_recovery_rejects_corrupt_unresolved_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str, error: str
) -> None:
    home, _digest, operation_id = _interrupted_registration(tmp_path, monkeypatch, "unresolved")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        connection = workspace.strategies
        triggers = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name IN "
            "('strategy_versions_reject_update', 'immutable_strategy_versions_update', "
            "'immutable_strategy_imports_update')"
        ).fetchall()
        # Synthetic corruption only; restore the exact schema before admission/recovery.
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.executescript(
            "DROP TRIGGER strategy_versions_reject_update;"
            "DROP TRIGGER immutable_strategy_versions_update;"
            "DROP TRIGGER immutable_strategy_imports_update;"
        )
        connection.execute(mutation)
        for trigger in triggers:
            connection.execute(trigger[0])
        connection.commit()
    with open_workspace(home, writable=True) as workspace:
        before = "\n".join(workspace.state.iterdump())
        with pytest.raises(ValueError, match=error):
            recover_operations(workspace)
        assert "\n".join(workspace.state.iterdump()) == before
        assert (
            workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()[0]
            == "PREPARED"
        )


def test_strategy_recovery_without_private_receipt_stays_pending(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as workspace:
        state.prepare_operation(
            workspace.state,
            operation_id="no-receipt",
            kind="strategy_import",
            request_hash="a" * 64,
            target_id="synthetic-probe:1",
            expected_parent=None,
            payload_hash="a" * 64,
        )
        before = "\n".join(workspace.state.iterdump())
        assert recover_operations(workspace) == {
            "recovered": [],
            "pending": ["no-receipt"],
            "provider_calls": 0,
        }
        assert "\n".join(workspace.state.iterdump()) == before
