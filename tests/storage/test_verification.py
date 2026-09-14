from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.bundle import load_bundle
from aegis_alpha.engine.errors import ContractDefinitionError
from aegis_alpha.storage import publication, state
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.input_pins import register_convention
from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.strategies import (
    LineageSpec,
    import_strategy,
    load_strategy,
    validate_strategy_import,
    verify_strategy_content,
)
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.strategy_requirements import read_execution_definition
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle
from tests.engine.test_requirements import rich_contract, scoring_contract
from tests.storage.test_backup import seed_workspace
from tests.storage.test_input_pins import A
from tests.storage.test_market_inputs import BUDGET, mixed_proxy_publications
from tests.storage.test_publication import document
from tests.storage.test_research_inputs import _hash_json
from tests.storage.test_strategy_import import MACRO_ROWS, _interrupted_registration
from tests.storage.test_strategy_requirements import select_only


def test_valid_mixed_proxy_publications_verify_without_mutation(tmp_path: Path) -> None:
    from aegis_alpha.storage.market import read_chain_rows  # noqa: PLC0415

    home = tmp_path / "proxy-home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        _, mixed = mixed_proxy_publications(workspace, tmp_path)
        rows = read_chain_rows(workspace.market, mixed.generation_id, budget=BUDGET)
        assert [row["contract_version"] for row in rows] == ["v1", "v2"]
        before = "\n".join(workspace.state.iterdump())
        report = verify_workspace(workspace)
        assert report["verified"] is True
        assert report["dataset_versions"] == len(rows)
        assert "\n".join(workspace.state.iterdump()) == before


REQUIREMENT_CORRUPTIONS = [
    pytest.param("UPDATE strategy_requirements SET warmup=9", id="altered"),
    pytest.param("DELETE FROM strategy_requirements", id="deleted"),
    pytest.param(
        "INSERT INTO strategy_requirements SELECT strategy_id,version,'extra',ordinal,"
        "required_schema,required_field,domain,warmup,basis,cadence "
        "FROM strategy_requirements WHERE role='prices'",
        id="extra",
    ),
]


@pytest.mark.parametrize("mutation", REQUIREMENT_CORRUPTIONS)
@pytest.mark.parametrize("boundary", ["content", "verify", "backup", "restore"])
def test_persisted_requirement_corruption_rejected(
    tmp_path: Path, mutation: str, boundary: str
) -> None:
    home = tmp_path / "original"
    initialize(home)
    raw = raw_bundle(rich_contract())
    digest = hashlib.sha256(raw).hexdigest()
    source = tmp_path / "synthetic.json"
    source.write_bytes(raw)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        register_strategy(workspace, source, digest, "synthetic-probe", "1")
        assert workspace.strategies is not None
        assert [
            tuple(row)
            for row in workspace.strategies.execute(
                "SELECT * FROM strategy_requirements ORDER BY role,ordinal"
            )
        ] == [
            ("synthetic-probe", "1", *row)
            for row in [
                *MACRO_ROWS,
                (
                    "prices",
                    1,
                    "engine-price-v1",
                    "close",
                    "prices",
                    8,
                    "explicit-input",
                    "calendar_month_end",
                ),
            ]
        ]
    source.unlink()
    original = evidence_snapshot(home)
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(copied, "strategies", "strategy_requirements", mutation)
    before = evidence_snapshot(copied)
    # The existing inspection boundary already rejects these same logical corruptions.
    with open_workspace(copied) as workspace:
        assert workspace.strategies is not None
        with select_only(workspace.strategies), pytest.raises(ValueError, match="requirements"):
            read_execution_definition(workspace.strategies, "synthetic-probe", "1", digest)
    output = tmp_path / "backup"
    target = tmp_path / "restored"
    if boundary == "restore":
        backup(home, output)
        corrupt_closed_store(output, "strategies", "strategy_requirements", mutation)
        manifest_path = output / "backup.json"
        manifest = json.loads(manifest_path.read_text())
        changed = (output / "strategies.sqlite3").read_bytes()
        manifest["files"]["strategies.sqlite3"] = {
            "size_bytes": len(changed),
            "sha256": hashlib.sha256(changed).hexdigest(),
        }
        manifest_path.write_text(json.dumps(manifest))
    if boundary == "backup":
        with pytest.raises(ValueError, match="requirements"):
            backup(copied, output)
        assert not output.exists()
    elif boundary == "restore":
        with pytest.raises(ValueError, match="requirements"):
            restore(output, target)
        assert (
            json.loads((target / "installation.json").read_text())["phase"] == "restore-incomplete"
        )
    else:
        with open_workspace(copied) as workspace:
            assert workspace.strategies is not None
            if boundary == "content":
                with (
                    select_only(workspace.strategies),
                    pytest.raises(ValueError, match="requirements"),
                ):
                    verify_strategy_content(workspace.strategies, "synthetic-probe", "1", digest)
            else:
                with pytest.raises(ValueError, match="requirements"):
                    verify_workspace(workspace)
    assert evidence_snapshot(copied) == before
    assert evidence_snapshot(home) == original


@pytest.mark.parametrize("mutation", REQUIREMENT_CORRUPTIONS)
@pytest.mark.parametrize("boundary", ["retry", "private", "recovery"])
def test_corrupt_requirements_cannot_complete_pending_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str, boundary: str
) -> None:
    home, digest, _operation_id = _interrupted_registration(tmp_path, monkeypatch, "none")
    raw = raw_bundle(contract())
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        with select_only(workspace.strategies):
            assert validate_strategy_import(
                workspace.strategies, load_bundle(raw, digest, "synthetic-probe", "1"), "new-op"
            ) == (True, False)
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(copied, "strategies", "strategy_requirements", mutation)
    before = evidence_snapshot(copied)
    source = tmp_path / "retry.json"
    source.write_bytes(raw)
    with open_workspace(copied, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        if boundary == "retry":
            with pytest.raises(ValueError, match="requirements"):
                register_strategy(workspace, source, digest, "synthetic-probe", "1")
        elif boundary == "private":
            statements: list[str] = []
            workspace.strategies.set_trace_callback(statements.append)
            try:
                with pytest.raises(ValueError, match="requirements"):
                    import_strategy(
                        workspace.strategies, raw, digest, "synthetic-probe", "1", "new-op"
                    )
            finally:
                workspace.strategies.set_trace_callback(None)
            assert statements[0] == "BEGIN IMMEDIATE"
            assert statements[-1] == "ROLLBACK"
        else:
            with pytest.raises(ValueError, match="requirements"):
                publication.recover_operations(workspace)
    assert evidence_snapshot(copied) == before


@pytest.mark.parametrize("consumer", ["offensive", "canary", "negative"])
def test_stored_legacy_scoring_evidence_round_trips_without_execution_admission(
    tmp_path: Path, consumer: str
) -> None:
    home = tmp_path / "legacy"
    initialize(home)
    value = scoring_contract(consumer, {"method": "return_rate", "horizon": 12})
    raw = raw_bundle(value)
    digest = hashlib.sha256(raw).hexdigest()
    bundle = load_bundle(raw, digest, "synthetic-probe", "1")
    expected = (
        "synthetic-probe",
        "1",
        "prices",
        1,
        "engine-price-v1",
        "close",
        "prices",
        3,
        "explicit-input",
        "calendar_month_end",
    )
    # Author a persisted v1 fixture, not an admitted ExecutionDefinition or a fake reader.
    # These raw/contract/row values were writable before active-score-reference admission.
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        state.prepare_operation(
            workspace.state,
            operation_id="legacy-op",
            kind="strategy_import",
            request_hash=digest,
            target_id="synthetic-probe:1",
            expected_parent=None,
            payload_hash=digest,
        )
        with workspace.strategies:
            workspace.strategies.execute(
                "INSERT INTO strategies VALUES ('synthetic-probe','synthetic-probe','active')"
            )
            workspace.strategies.execute(
                "INSERT INTO strategy_versions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    "synthetic-probe",
                    "1",
                    raw,
                    digest,
                    canonical_json_bytes(value).decode(),
                    bundle.contract_sha256,
                    bundle.schema_version,
                    value.contract_version,
                    1,
                ),
            )
            workspace.strategies.execute(
                "INSERT INTO strategy_requirements VALUES (?,?,?,?,?,?,?,?,?,?)", expected
            )
            workspace.strategies.execute(
                "INSERT INTO strategy_imports VALUES ('legacy-op',?,'synthetic-probe','1',1)",
                (digest,),
            )
        state.complete_operation(workspace.state, "legacy-op", digest)
    before = evidence_snapshot(home)
    output, restored = tmp_path / "backup", tmp_path / "restored"
    backup(home, output)
    restore(output, restored)
    for root in (home, restored):
        with open_workspace(root) as workspace:
            assert workspace.strategies is not None
            assert verify_workspace(workspace)["verified"] is True
            with select_only(workspace.strategies):
                assert (
                    verify_strategy_content(workspace.strategies, "synthetic-probe", "1", digest)
                    == bundle
                )
                assert [
                    tuple(row)
                    for row in workspace.strategies.execute("SELECT * FROM strategy_requirements")
                ] == [expected]
                with pytest.raises(ContractDefinitionError):
                    read_execution_definition(workspace.strategies, "synthetic-probe", "1", digest)
        assert evidence_snapshot(root) == before


def test_verify_workspace_reports_nonempty_logical_refs(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
    assert report["verified"] is True
    assert report["dataset_versions"] == 1
    assert report["strategy_versions"] == 1
    assert report["pending_operations"] == 0
    assert report["orphan_generations"] == []


def test_verify_unresolved_import_checks_integrity_not_execution(tmp_path: Path) -> None:
    # Given a real completed import with a missing direct parent.
    home = tmp_path / "aas"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        before = "\n".join(workspace.strategies.iterdump())
        state = "\n".join(workspace.state.iterdump())
        digest = workspace.strategies.execute(
            "SELECT raw_sha256 FROM strategy_versions"
        ).fetchone()[0]

        # When workspace integrity is verified, unresolved lineage is valid stored state.
        report = verify_workspace(workspace)

        # Then verification is read-only and does not grant execution eligibility.
        assert report == {
            "verified": True,
            "dataset_versions": 1,
            "strategy_versions": 1,
            "pending_operations": 0,
            "orphan_generations": [],
        }
        assert "\n".join(workspace.strategies.iterdump()) == before
        assert "\n".join(workspace.state.iterdump()) == state
        with pytest.raises(ValueError, match="unresolved parent lineage"):
            load_strategy(workspace.strategies, "synthetic-probe", "1", digest)


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ("UPDATE strategy_versions SET raw_bundle=x'7b7d'", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET raw_sha256='" + "0" * 64 + "'", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET contract_json='{}'", "parsed contract hash"),
        ("UPDATE strategy_versions SET contract_sha256='" + "0" * 64 + "'", "parsed contract hash"),
    ],
    ids=["raw", "raw-pin", "contract", "contract-pin"],
)
def test_copied_unresolved_corruption_fails_verify_and_backup(
    tmp_path: Path, mutation: str, error: str
) -> None:
    # Given a copy of a legitimately imported unresolved child, corrupt only that copy.
    home = tmp_path / "aas"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    with open_workspace(copied, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        connection = workspace.strategies
        triggers = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name IN "
            "('strategy_versions_reject_update', 'immutable_strategy_versions_update')"
        ).fetchall()
        connection.executescript(
            "DROP TRIGGER strategy_versions_reject_update;"
            "DROP TRIGGER immutable_strategy_versions_update;"
        )
        connection.execute(mutation)
        for trigger in triggers:
            connection.execute(trigger[0])
        connection.commit()

    # When either integrity surface examines the copy, actual content validation still fails.
    with open_workspace(copied) as workspace, pytest.raises(ValueError, match=error):
        verify_workspace(workspace)
    output = tmp_path / "rejected-backup"
    with pytest.raises(ValueError, match=error):
        backup(copied, output)

    # Then no partial backup is published and the original unresolved workspace remains valid.
    assert not output.exists()
    with open_workspace(home) as workspace:
        assert verify_workspace(workspace)["verified"] is True


def test_verify_reports_pending_and_orphan_and_backup_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    complete = publication._complete_publication  # noqa: SLF001 -- inject crash before catalog completion

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic verification pending")

    monkeypatch.setattr(publication, "_complete_publication", crash)
    with (
        open_workspace(home, writable=True) as workspace,
        pytest.raises(RuntimeError, match="pending"),
    ):
        publication.publish_document(workspace, parse_import(document()))
    monkeypatch.setattr(publication, "_complete_publication", complete)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
        assert report["verified"] is True
        assert report["pending_operations"] == 1
        assert report["orphan_generations"] == ["synthetic-generation"]
        assert report["dataset_versions"] == 0
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home)


def test_raw_corruption_fails_verify(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    seed_workspace(home)
    with open_workspace(home) as workspace:
        relative = workspace.state.execute("SELECT relative_path FROM source_files").fetchone()[0]
        raw_file = workspace.paths.raw / relative
    raw_file.write_bytes(b"corrupt-raw")
    raw_file.chmod(0o600)
    with open_workspace(home) as workspace, pytest.raises(ValueError, match=r"checksum|hash"):
        verify_workspace(workspace)


# Every mutation preserves valid SQLite constraints and restores the exact shipped schema.
_EVIDENCE_CORRUPTIONS = [
    pytest.param(
        "state",
        "conventions",
        "UPDATE conventions SET payload=replace(payload,'capital','total_return')",
        id="convention-payload",
    ),
    pytest.param(
        "state",
        "conventions",
        "UPDATE conventions SET content_hash='" + "0" * 64 + "'",
        id="convention-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET reason_hash='" + "0" * 64 + "'",
        id="lineage-reason-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET reason='changed',"
        "reason_hash='9abf9994927acc503ec87e4433239aee75bb937357616bf8cca0290a1f3aa60a'",
        id="lineage-rehashed-reason",
    ),
    pytest.param(
        "strategies",
        "strategy_imports",
        "UPDATE strategy_imports SET request_hash='" + "0" * 64 + "'",
        id="private-receipt-hash",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET request_hash='"
        + "0" * 64
        + "' WHERE kind='strategy_import'",
        id="completed-intent-hash",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET parent_status='resolved'",
        id="resolved-absent-parent",
    ),
    pytest.param(
        "strategies",
        "strategy_lineage",
        "DELETE FROM strategy_lineage",
        id="lineage-removed",
    ),
    pytest.param(
        "strategies",
        "strategy_imports",
        "DELETE FROM strategy_imports",
        id="receipt-removed",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "DELETE FROM storage_operations WHERE kind='strategy_import'",
        id="intent-removed",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET kind='other' WHERE kind='strategy_import'",
        id="intent-kind",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET target_id='other:1' WHERE kind='strategy_import'",
        id="intent-target",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET payload_hash='"
        + "0" * 64
        + "' WHERE kind='strategy_import'",
        id="intent-payload",
    ),
    pytest.param(
        "state",
        "storage_operations",
        "UPDATE storage_operations SET expected_parent='other' WHERE kind='strategy_import'",
        id="intent-parent",
    ),
]


def corrupt_closed_store(
    home: Path, store: str, table: str, mutation: str, parameters: tuple[object, ...] = ()
) -> None:
    connection = sqlite3.connect(home / (store + ".sqlite3"))
    try:
        schema_sql = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        schema = connection.execute(schema_sql).fetchall()
        triggers = connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,)
        ).fetchall()
        for name, _sql in triggers:
            connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        connection.execute(mutation, parameters)
        for _name, sql in triggers:
            connection.execute(sql)
        connection.commit()
        assert connection.execute(schema_sql).fetchall() == schema
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()


def corrupt_proxy(home: Path, fault: str) -> None:
    """Alter only a disposable store; keep its schemas and outer integrity valid."""
    if fault.startswith("child-"):
        corrupt_closed_store(
            home,
            "state",
            "feature_inputs",
            "UPDATE feature_inputs SET content_hash=? WHERE name='PROXY' AND version='v2' "
            "AND ordinal=?",
            ("0" * 64, int(fault.removeprefix("child-"))),
        )
    elif fault == "ordinal":
        corrupt_closed_store(
            home,
            "state",
            "feature_inputs",
            "UPDATE feature_inputs SET ordinal=8 WHERE name='PROXY' AND version='v2' AND ordinal=5",
        )
    elif fault == "definition":
        corrupt_closed_store(
            home,
            "state",
            "feature_contracts",
            "UPDATE feature_contracts SET definition='{}' WHERE name='PROXY' AND version='v2'",
        )
    elif fault in {"raw-transform", "repinned-transform"}:
        with closing(sqlite3.connect(home / "state.sqlite3")) as connection:
            digest = connection.execute(
                "SELECT ref_id FROM feature_inputs WHERE name='PROXY' AND version='v2' "
                "AND ordinal=5"
            ).fetchone()[0]
        raw_path = home / "raw" / digest[:2] / digest
        transform = json.loads(raw_path.read_bytes())
        # The same null source columns produce the same points, but the immutable
        # publication independently pins the ORIGINAL mapping/transform bytes.
        columns = transform["columns"]
        columns["available_at_us"], columns["revision_known_at_us"] = (
            columns["revision_known_at_us"],
            columns["available_at_us"],
        )
        raw = json.dumps(transform).encode()
        if fault == "raw-transform":
            raw_path.write_bytes(raw)
        else:
            from aegis_alpha.storage.raw import put_raw  # noqa: PLC0415

            _, changed_hash, _ = put_raw(home / "raw", raw)
            corrupt_closed_store(
                home,
                "state",
                "feature_inputs",
                "UPDATE feature_inputs SET ref_id=?,content_hash=? "
                "WHERE name='PROXY' AND version='v2' AND ordinal=5",
                (changed_hash, changed_hash),
            )
            corrupt_closed_store(
                home,
                "state",
                "dataset_versions",
                "UPDATE dataset_versions SET transform_hash=? WHERE generation_id='proxy2'",
                (changed_hash,),
            )
    elif fault.startswith("catalog-"):
        column = fault.removeprefix("catalog-")
        assert column in {"manifest_hash", "transform_hash", "normalizer_version"}
        corrupt_closed_store(
            home,
            "state",
            "dataset_versions",
            f"UPDATE dataset_versions SET {column}=? WHERE generation_id='proxy2'",  # noqa: S608 -- fixed allowlist
            ("0" * 64,),
        )
    else:
        corrupt_proxy_market(home, fault)


def corrupt_proxy_market(home: Path, fault: str) -> None:
    import duckdb  # noqa: PLC0415

    from aegis_alpha.storage import market  # noqa: PLC0415

    with duckdb.connect(str(home / "market.duckdb")) as connection:
        if fault == "marker":
            connection.execute(
                "UPDATE market_generations SET request_hash=? WHERE generation_id='proxy2'",
                ["0" * 64],
            )
            return
        column = fault.removeprefix("row-")
        assert column in {"contract_hash", "input_bundle_hash", "contract_version", "instrument_id"}
        rows = [
            dict(row)
            for row in market.read_chain_rows(connection, "proxy2", budget=BUDGET)
            if row["generation_id"] == "proxy2"
        ]
        rows[0][column] = "0" * 64
        connection.execute(
            f"UPDATE feature_values SET {column}=? WHERE generation_id='proxy2'",  # noqa: S608 -- fixed allowlist
            [rows[0][column]],
        )
        marker = market.marker_for(connection, "proxy2")
        delta_hash = market._delta_hash("feature_values", rows)  # noqa: SLF001 -- rehash physical corruption, not an expected-value oracle
        chain_hash = _hash_json(
            [
                marker["record_schema"],
                market.marker_for(connection, "proxy1")["chain_hash"],
                marker["dataset_id"],
                marker["version"],
                marker["generation_id"],
                marker["domain"],
                delta_hash,
                marker["parent_id"],
                marker["operation_id"],
                marker["request_hash"],
                marker["row_count"],
            ]
        )
        connection.execute(
            "UPDATE market_generations SET delta_hash=?,chain_hash=? WHERE generation_id='proxy2'",
            [delta_hash, chain_hash],
        )
        assert market.verify_generation(connection, "proxy2")["chain_hash"] == chain_hash
    corrupt_closed_store(
        home,
        "state",
        "dataset_versions",
        "UPDATE dataset_versions SET chain_hash=? WHERE generation_id='proxy2'",
        (chain_hash,),
    )


@pytest.mark.parametrize(
    ("fault", "error"),
    [
        *(("child-" + str(ordinal), "inputs mismatch") for ordinal in range(6)),
        ("ordinal", "inputs mismatch"),
        ("definition", "contract.*corrupt"),
        ("raw-transform", "transform hash mismatch"),
        ("repinned-transform", "publication evidence"),
        ("catalog-manifest_hash", "catalog"),
        ("catalog-transform_hash", "transform/catalog"),
        ("catalog-normalizer_version", "publication evidence"),
        ("marker", "logical hash/count"),
        ("row-contract_hash", "publication evidence"),
        ("row-input_bundle_hash", "publication evidence"),
        ("row-contract_version", "no retained feature generation"),
        ("row-instrument_id", "publication evidence"),
    ],
)
def test_proxy_corruption_blocks_verify_backup_and_rehashed_restore(
    tmp_path: Path,
    fault: str,
    error: str,
) -> None:
    home = tmp_path / "original"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        mixed_proxy_publications(workspace, tmp_path)
    original = evidence_snapshot(home)
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_proxy(copied, fault)
    with open_workspace(copied) as workspace, pytest.raises(ValueError, match=error):
        verify_workspace(workspace)
    output = tmp_path / "rejected-backup"
    with pytest.raises(ValueError, match=error):
        backup(copied, output)
    assert not output.exists()

    archive = tmp_path / "backup"
    backup(home, archive)
    corrupt_proxy(archive, fault)
    manifest_path = archive / "backup.json"
    manifest = json.loads(manifest_path.read_bytes())
    # Recompute all outer hashes, including a deliberately added replacement spec.
    files = set(manifest["files"]) | {
        path.relative_to(archive).as_posix()
        for path in (archive / "raw").rglob("*")
        if path.is_file()
    }
    for relative in files:
        raw = (archive / relative).read_bytes()
        manifest["files"][relative] = {
            "size_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    manifest_path.write_text(json.dumps(manifest))
    target = tmp_path / "rejected-restore"
    with pytest.raises(ValueError, match=error):
        restore(archive, target)
    assert json.loads((target / "installation.json").read_bytes())["phase"] == "restore-incomplete"
    assert evidence_snapshot(home) == original


def evidence_snapshot(home: Path) -> tuple[str, str]:
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        return "\n".join(workspace.state.iterdump()), "\n".join(workspace.strategies.iterdump())


@pytest.mark.parametrize(("store", "table", "mutation"), _EVIDENCE_CORRUPTIONS)
def test_logical_evidence_rejected_at_all_integrity_boundaries(
    tmp_path: Path, store: str, table: str, mutation: str
) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    with open_workspace(home, writable=True) as workspace:
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    original = evidence_snapshot(home)
    clean_backup = tmp_path / "clean-backup"
    backup(home, clean_backup)
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(copied, store, table, mutation)
    corrupt = evidence_snapshot(copied)
    with (
        open_workspace(copied) as workspace,
        pytest.raises(ValueError, match=r"convention|strategy"),
    ):
        verify_workspace(workspace)
    with pytest.raises(ValueError, match=r"convention|strategy"):
        backup(copied, tmp_path / "rejected-backup")
    assert not (tmp_path / "rejected-backup").exists()
    assert evidence_snapshot(copied) == corrupt

    # Outer file hashes are honest: restore must reject *logical* corruption beneath them.
    corrupt_closed_store(clean_backup, store, table, mutation)
    manifest_path = clean_backup / "backup.json"
    manifest = json.loads(manifest_path.read_text())
    name = store + ".sqlite3"
    raw = (clean_backup / name).read_bytes()
    manifest["files"][name] = {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest))
    target = tmp_path / "rejected-restore"
    with pytest.raises(ValueError, match=r"convention|strategy"):
        restore(clean_backup, target)
    assert json.loads((target / "installation.json").read_text())["phase"] == "restore-incomplete"
    assert evidence_snapshot(home) == original


def test_matching_receipt_and_intent_must_match_current_exact_lineage(tmp_path: Path) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    for store, table in (("state", "storage_operations"), ("strategies", "strategy_imports")):
        where = " WHERE kind='strategy_import'" if store == "state" else ""
        corrupt_closed_store(
            copied,
            store,
            table,
            "UPDATE " + table + " SET request_hash='" + "0" * 64 + "'" + where,
        )
    with open_workspace(copied) as workspace, pytest.raises(ValueError, match="stored lineage"):
        verify_workspace(workspace)


def test_load_rejects_resolved_edge_without_exact_parent(tmp_path: Path) -> None:
    home = tmp_path / "original"
    seed_workspace(home, lineage=LineageSpec("absent-parent", "7", "derived", "synthetic"))
    copied = tmp_path / "corrupt-copy"
    shutil.copytree(home, copied)
    corrupt_closed_store(
        copied,
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET parent_status='resolved'",
    )
    before = evidence_snapshot(copied)
    with open_workspace(copied) as workspace:
        assert workspace.strategies is not None
        digest = workspace.strategies.execute(
            "SELECT raw_sha256 FROM strategy_versions"
        ).fetchone()[0]
        assert (
            workspace.strategies.execute(
                "SELECT count(*) FROM strategy_versions "
                "WHERE strategy_id='absent-parent' AND version='7'"
            ).fetchone()[0]
            == 0
        )
        with pytest.raises(ValueError, match="stored lineage"):
            load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
    assert evidence_snapshot(copied) == before


@pytest.mark.parametrize("parent_status", ["unresolved", "none", "resolved"])
def test_verify_private_commit_remains_pending_and_select_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parent_status: str
) -> None:
    home, _digest, operation_id = _interrupted_registration(tmp_path, monkeypatch, parent_status)
    with open_workspace(home, writable=True) as workspace:
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    before = evidence_snapshot(home)
    actions: set[int] = set()

    def select_only(action: int, *_args: str | None) -> int:
        actions.add(action)
        return (
            sqlite3.SQLITE_OK
            if action
            in {
                sqlite3.SQLITE_SELECT,
                sqlite3.SQLITE_READ,
                sqlite3.SQLITE_FUNCTION,
                sqlite3.SQLITE_PRAGMA,
                sqlite3.SQLITE_RECURSIVE,
            }
            else sqlite3.SQLITE_DENY
        )

    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        workspace.state.set_authorizer(select_only)
        workspace.strategies.set_authorizer(select_only)
        try:
            report = verify_workspace(workspace)
        finally:
            workspace.state.set_authorizer(None)
            workspace.strategies.set_authorizer(None)
        assert report["verified"] is True
        assert report["pending_operations"] == 1
        assert (
            workspace.state.execute(
                "SELECT phase FROM storage_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()[0]
            == "PREPARED"
        )
    assert sqlite3.SQLITE_READ in actions
    assert evidence_snapshot(home) == before
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home, tmp_path / "pending-backup")
    assert evidence_snapshot(home) == before


@pytest.mark.parametrize("status", ["none", "unresolved", "resolved", "late-parent", "ancestor"])
def test_valid_evidence_round_trip_keeps_immutable_status(tmp_path: Path, status: str) -> None:
    home = tmp_path / "original"
    initialize(home)
    child = raw_bundle(contract())
    digest = hashlib.sha256(child).hexdigest()
    parent = child.replace(b'"synthetic-probe"', b'"parent"')
    source = tmp_path / "synthetic.json"
    lineage = None if status == "none" else LineageSpec("parent", "1", "derived", " exact\n")
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if status in {"resolved", "ancestor"}:
            source.write_bytes(parent)
            register_strategy(
                workspace,
                source,
                hashlib.sha256(parent).hexdigest(),
                "parent",
                "1",
                lineage=LineageSpec("absent", "7", "derived", "ancestor")
                if status == "ancestor"
                else None,
            )
        source.write_bytes(child)
        register_strategy(workspace, source, digest, "synthetic-probe", "1", lineage=lineage)
        if status == "late-parent":
            source.write_bytes(parent)
            register_strategy(workspace, source, hashlib.sha256(parent).hexdigest(), "parent", "1")
        register_convention(workspace.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    source.unlink()
    before = evidence_snapshot(home)
    identity = json.loads((home / "installation.json").read_text())
    output, restored = tmp_path / "backup", tmp_path / "restored"
    backup(home, output)
    restore(output, restored)
    restored_identity = json.loads((restored / "installation.json").read_text())
    assert restored_identity["installation_id"] == identity["installation_id"]
    assert restored_identity["stores"] == identity["stores"]
    assert restored_identity["deployment_id"] != identity["deployment_id"]
    for root in (home, restored):
        assert evidence_snapshot(root) == before
        with open_workspace(root) as workspace:
            assert workspace.strategies is not None
            assert verify_workspace(workspace)["verified"] is True
            rows = workspace.strategies.execute(
                "SELECT parent_status FROM strategy_lineage WHERE strategy_id='synthetic-probe'"
            ).fetchall()
            assert [row[0] for row in rows] == (
                []
                if status == "none"
                else ["unresolved"]
                if status in {"unresolved", "late-parent"}
                else ["resolved"]
            )
            if status in {"unresolved", "late-parent"}:
                with pytest.raises(ValueError, match="unresolved parent"):
                    load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
            else:
                assert (
                    load_strategy(
                        workspace.strategies,
                        "synthetic-probe",
                        "1",
                        digest,
                    ).source_sha256
                    == digest
                )
            if status == "none":
                assert (
                    workspace.strategies.execute(
                        "SELECT request_hash FROM strategy_imports"
                    ).fetchone()[0]
                    == digest
                )
                assert workspace.state.execute(
                    "SELECT request_hash,payload_hash FROM storage_operations"
                ).fetchone()[:] == (digest, digest)
        assert evidence_snapshot(root) == before


def sealed_child_with_parent(home: Path, source: Path, original_status: str) -> bytes:
    initialize(home)
    child = raw_bundle(contract())
    parent = child.replace(b'"synthetic-probe"', b'"parent"')
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if original_status == "resolved":
            source.write_bytes(parent)
            register_strategy(workspace, source, hashlib.sha256(parent).hexdigest(), "parent", "1")
        source.write_bytes(child)
        register_strategy(
            workspace,
            source,
            hashlib.sha256(child).hexdigest(),
            "synthetic-probe",
            "1",
            lineage=LineageSpec("parent", "1", "derived", " exact\n"),
        )
        if original_status == "unresolved":
            source.write_bytes(parent)
            register_strategy(workspace, source, hashlib.sha256(parent).hexdigest(), "parent", "1")
        assert workspace.strategies is not None
        assert verify_workspace(workspace)["verified"] is True
    source.unlink()
    return child


@pytest.mark.parametrize("original_status", ["unresolved", "resolved"])
@pytest.mark.parametrize("boundary", ["load", "verify", "backup", "restore"])
def test_registration_status_cannot_be_forged_after_parent_arrival(
    tmp_path: Path, original_status: str, boundary: str
) -> None:
    home = tmp_path / "original"
    child = sealed_child_with_parent(home, tmp_path / "synthetic.json", original_status)
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        receipts = [
            tuple(row) for row in workspace.strategies.execute("SELECT * FROM strategy_imports")
        ]
    original = evidence_snapshot(home)
    copied = tmp_path / "copy"
    if boundary == "restore":
        backup(home, copied)
    else:
        shutil.copytree(home, copied)
    forged = "resolved" if original_status == "unresolved" else "unresolved"
    corrupt_closed_store(
        copied,
        "strategies",
        "strategy_lineage",
        "UPDATE strategy_lineage SET parent_status=?",
        (forged,),
    )
    with open_workspace(copied) as workspace:
        assert workspace.strategies is not None
        assert [
            tuple(row) for row in workspace.strategies.execute("SELECT * FROM strategy_imports")
        ] == receipts
        assert "\n".join(workspace.state.iterdump()) == original[0]
    corrupt = evidence_snapshot(copied)
    if boundary == "load":
        with open_workspace(copied) as workspace:
            assert workspace.strategies is not None
            with pytest.raises(ValueError, match="stored lineage"):
                load_strategy(
                    workspace.strategies, "synthetic-probe", "1", hashlib.sha256(child).hexdigest()
                )
    elif boundary == "verify":
        with open_workspace(copied) as workspace, pytest.raises(ValueError, match="stored lineage"):
            verify_workspace(workspace)
    elif boundary == "backup":
        with pytest.raises(ValueError, match="stored lineage"):
            backup(copied, tmp_path / "rejected-backup")
        assert not (tmp_path / "rejected-backup").exists()
    else:
        manifest_path = copied / "backup.json"
        manifest = json.loads(manifest_path.read_text())
        raw = (copied / "strategies.sqlite3").read_bytes()
        manifest["files"]["strategies.sqlite3"] = {
            "size_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
        manifest_path.write_text(json.dumps(manifest))
        target = tmp_path / "rejected-restore"
        with pytest.raises(ValueError, match="stored lineage"):
            restore(copied, target)
        assert (
            json.loads((target / "installation.json").read_text())["phase"] == "restore-incomplete"
        )
    assert evidence_snapshot(copied) == corrupt
    assert evidence_snapshot(home) == original


@pytest.mark.parametrize("status", ["unresolved", "resolved"])
@pytest.mark.parametrize("boundary", ["load", "retry", "recover", "verify", "backup", "restore"])
def test_interim_unsealed_lineage_is_preserved_but_never_authenticated(
    tmp_path: Path, status: str, boundary: str
) -> None:
    home = tmp_path / "original"
    source = tmp_path / "synthetic.json"
    raw = sealed_child_with_parent(home, source, status)
    digest = hashlib.sha256(raw).hexdigest()
    copied = tmp_path / "copy"
    backup(home, copied)
    document = {
        "schema_version": "aas-strategy-import-request-v1",
        "strategy_id": "synthetic-probe",
        "version": "1",
        "raw_sha256": digest,
        "lineage": {
            "parent_id": "parent",
            "parent_version": "1",
            "change_kind": "derived",
            "reason": " exact\n",
        },
    }
    legacy = hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    corrupt_closed_store(
        copied,
        "strategies",
        "strategy_imports",
        "UPDATE strategy_imports SET request_hash=? WHERE strategy_id='synthetic-probe'",
        (legacy,),
    )
    corrupt_closed_store(
        copied,
        "state",
        "storage_operations",
        "UPDATE storage_operations SET request_hash=?,phase='PREPARED',completed_at_us=NULL "
        "WHERE target_id='synthetic-probe:1'",
        (legacy,),
    )
    before = evidence_snapshot(copied)
    if boundary in {"load", "retry", "recover", "verify"}:
        with open_workspace(copied, writable=True, strategy_write=True) as workspace:
            assert workspace.strategies is not None
            if boundary == "load":
                with pytest.raises(ValueError, match="unsealed"):
                    load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
            elif boundary == "retry":
                source.write_bytes(raw)
                with pytest.raises(ValueError, match="unsealed"):
                    register_strategy(
                        workspace,
                        source,
                        digest,
                        "synthetic-probe",
                        "1",
                        lineage=LineageSpec("parent", "1", "derived", " exact\n"),
                    )
            elif boundary == "recover":
                with pytest.raises(ValueError, match="unsealed"):
                    publication.recover_operations(workspace)
            else:
                with pytest.raises(ValueError, match="unsealed"):
                    verify_workspace(workspace)
    elif boundary == "backup":
        with pytest.raises(ValueError, match="unsealed"):
            backup(copied, tmp_path / "rejected-backup")
    else:
        manifest_path = copied / "backup.json"
        manifest = json.loads(manifest_path.read_text())
        for name in ("state.sqlite3", "strategies.sqlite3"):
            payload = (copied / name).read_bytes()
            manifest["files"][name] = {
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        manifest_path.write_text(json.dumps(manifest))
        target = tmp_path / "rejected-restore"
        with pytest.raises(ValueError, match="unsealed"):
            restore(copied, target)
        assert (
            json.loads((target / "installation.json").read_text())["phase"] == "restore-incomplete"
        )
    assert evidence_snapshot(copied) == before


@pytest.mark.parametrize("mutation", ["state-hash", "missing-intent"])
def test_private_load_has_no_hidden_state_connection(tmp_path: Path, mutation: str) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "synthetic.json"
    raw = sealed_child_with_parent(home, source, "resolved")
    digest = hashlib.sha256(raw).hexdigest()
    corrupt_closed_store(
        home,
        "state",
        "storage_operations",
        "UPDATE storage_operations SET request_hash='"
        + "0" * 64
        + "' WHERE target_id='synthetic-probe:1'"
        if mutation == "state-hash"
        else "DELETE FROM storage_operations WHERE target_id='synthetic-probe:1'",
    )
    private = tmp_path / "only-private.sqlite3"
    shutil.copy2(home / "strategies.sqlite3", private)
    connection = connect(private, read_only=True)
    try:
        assert load_strategy(connection, "synthetic-probe", "1", digest).source_sha256 == digest
    finally:
        connection.close()
    before = evidence_snapshot(home)
    source.write_bytes(raw)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with pytest.raises(ValueError, match="strategy"):
            verify_workspace(workspace)
        with pytest.raises(ValueError, match="strategy"):
            register_strategy(
                workspace,
                source,
                digest,
                "synthetic-probe",
                "1",
                lineage=LineageSpec("parent", "1", "derived", " exact\n"),
            )
    assert evidence_snapshot(home) == before


def test_verify_before_private_commit_keeps_missing_receipt_pending(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    raw = raw_bundle(contract())
    source = tmp_path / "synthetic.json"
    source.write_bytes(raw)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        assert workspace.strategies is not None
        workspace.strategies.execute(
            "CREATE TEMP TRIGGER interrupt BEFORE INSERT ON strategy_versions "
            "BEGIN SELECT RAISE(ABORT,'before private commit'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="before private commit"):
            register_strategy(
                workspace, source, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "1"
            )
    source.unlink()
    before = evidence_snapshot(home)
    with open_workspace(home) as workspace:
        report = verify_workspace(workspace)
        assert report["verified"] is True
        assert report["strategy_versions"] == 0
        assert report["pending_operations"] == 1
    assert evidence_snapshot(home) == before
