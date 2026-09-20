"""Migrating an installed run add-on to the version that admits both request contracts.

Every case here runs against a disposable installation this module created. The old
add-on is installed from the module's own recorded v1 objects, which is what makes this
a migration test rather than a fresh install under a different name.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage import run_schema
from aegis_alpha.storage.publication import quarantine, recover_operations
from aegis_alpha.storage.run_schema import (
    BACKTEST_REQUEST_SCHEMA,
    MIGRATION_OPERATION,
    RESEARCH_REQUEST_SCHEMA,
    STATE_CHECKSUMS,
    STATE_VERSION,
    RunSchemaError,
    inspect_run_schema,
    install_run_schema,
    migrate_run_schema,
    require_request_schema,
)
from aegis_alpha.storage.runs import RunResult, commit_run, open_run, read_run
from aegis_alpha.storage.state import complete_operation, prepare_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.storage.test_runs import (
    BUDGET,
    BUNDLE,
    RESULT,
    Fixture,
    admit_strategy,
    intent,
    request_document,
)

# The exact digest every installation made before this migration recorded for its state
# add-on. Written down rather than only recomputed: if the v1 DDL text ever drifts, this
# is where that shows up, instead of in an operator's store being declared invalid.
RECORDED_V1 = "2ff94360fb0847fa7f53efafd48d4c3ed83e2fac3419709fca7741ec5bce4c6c"


def install_v1(home: Path) -> None:
    """Install the add-on exactly as the pre-migration code did, from its own v1 bytes."""
    with open_workspace(home, writable=True) as workspace:
        recorded = run_schema._intent(workspace, 1)  # noqa: SLF001 -- reproduce the old install
        prepare_operation(
            workspace.state,
            operation_id=run_schema._OPERATION,  # noqa: SLF001
            kind="run-schema-install",
            request_hash=str(recorded["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=None,
            payload_hash=str(recorded["payload_hash"]),
        )
        run_schema._install_state(workspace, 1)  # noqa: SLF001
        run_schema._install_market(workspace)  # noqa: SLF001
        complete_operation(
            workspace.state,
            run_schema._OPERATION,  # noqa: SLF001
            str(recorded["request_hash"]),
        )
        workspace.state.commit()


def prepared_v1(root: Path) -> Fixture:
    """An installation on the old add-on, carrying one recorded executable run."""
    import hashlib  # noqa: PLC0415 -- local to the fixture's own digest

    from aegis_alpha.storage.backtest_requests import register_backtest_request  # noqa: PLC0415
    from aegis_alpha.storage.input_pins import register_input_bundle  # noqa: PLC0415

    home = root / "home"
    initialize(home)
    install_v1(home)
    pin = admit_strategy(home, root)
    body = request_document(pin)
    digest = hashlib.sha256(body).hexdigest()
    with open_workspace(home, writable=True) as workspace:
        bundle = register_input_bundle(
            workspace,
            BUNDLE,
            expected_file_sha256=hashlib.sha256(BUNDLE).hexdigest(),
            budget=BUDGET,
        )
        register_backtest_request(
            workspace, bundle, body, expected_request_hash=digest, budget=BUDGET
        )
    return Fixture(home=home, pin=pin, request_hash=digest)


def record_run(fx: Fixture, run_id: str) -> dict[str, object]:
    with open_workspace(fx.home, writable=True) as workspace:
        handle = open_run(workspace, intent(fx, run_id))
        return commit_run(workspace, handle, RunResult(RESULT), budget=BUDGET)


def read(home: Path, run_id: str) -> dict[str, object]:
    with open_workspace(home) as workspace:
        return read_run(workspace, run_id, budget=BUDGET)


def receipts(home: Path) -> list[tuple[int, str]]:
    with open_workspace(home) as workspace:
        return [
            (int(row[0]), str(row[1]))
            for row in workspace.state.execute(
                "SELECT version,checksum FROM run_schema ORDER BY version"
            )
        ]


def test_the_recorded_v1_schema_is_still_the_one_installations_carry() -> None:
    """The old checksum is a fact on disk, not a value this code may recompute freely."""
    assert STATE_CHECKSUMS[1] == RECORDED_V1
    assert "CHECK(request_schema='aas-backtest-request-v1')" in run_schema._RUN_DETAILS[1]  # noqa: SLF001
    assert (
        "CHECK(request_schema IN ('aas-backtest-request-v1','aas-research-run-v2'))"
        in run_schema._RUN_DETAILS[STATE_VERSION]  # noqa: SLF001
    )


def test_a_fresh_install_carries_the_current_version_and_an_old_one_says_so(
    tmp_path: Path,
) -> None:
    fresh, old = tmp_path / "fresh", tmp_path / "old"
    initialize(fresh)
    assert install_run_schema(fresh)["state_version"] == STATE_VERSION
    initialize(old)
    install_v1(old)
    with open_workspace(old) as workspace:
        status = inspect_run_schema(workspace)
    assert (status.state, status.state_version, status.install_version) == ("complete", 1, 1)
    # Repeating the install is not a quiet migration: widening the add-on is its own
    # explicit command, with its own backup.
    assert install_run_schema(old)["state_version"] == 1
    assert receipts(old) == [(1, STATE_CHECKSUMS[1])]


def test_migration_carries_an_existing_run_and_leaves_it_readable(tmp_path: Path) -> None:
    """The point of the whole exercise: an installed add-on is widened, not replaced."""
    fx = prepared_v1(tmp_path)
    record_run(fx, "run-old")
    before = read(fx.home, "run-old")
    with open_workspace(fx.home) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True
    report = migrate_run_schema(fx.home)
    assert report["migrated"] is True
    assert report["state_version"] == STATE_VERSION
    assert report["request_schemas"] == [BACKTEST_REQUEST_SCHEMA, RESEARCH_REQUEST_SCHEMA]
    assert receipts(fx.home) == [(1, STATE_CHECKSUMS[1]), (2, STATE_CHECKSUMS[2])]
    assert read(fx.home, "run-old") == before
    assert before["request_schema"] == BACKTEST_REQUEST_SCHEMA
    with open_workspace(fx.home) as workspace:
        assert verify_workspace(workspace, budget=BUDGET)["verified"] is True


def test_an_executable_run_still_records_after_the_migration(tmp_path: Path) -> None:
    """The widened CHECK admits a second contract without loosening the first one."""
    fx = prepared_v1(tmp_path)
    migrate_run_schema(fx.home)
    record_run(fx, "run-new")
    assert read(fx.home, "run-new")["request_schema"] == BACKTEST_REQUEST_SCHEMA


def test_repeating_a_finished_migration_changes_nothing(tmp_path: Path) -> None:
    fx = prepared_v1(tmp_path)
    migrate_run_schema(fx.home)
    again = migrate_run_schema(fx.home)
    assert again["migrated"] is False
    assert again["state_version"] == STATE_VERSION
    assert receipts(fx.home) == [(1, STATE_CHECKSUMS[1]), (2, STATE_CHECKSUMS[2])]


def test_a_migration_interrupted_before_its_rebuild_is_resumed_by_repeating_it(
    tmp_path: Path,
) -> None:
    """A crash between the durable intent and the rebuild leaves work, not damage."""
    fx = prepared_v1(tmp_path)
    record_run(fx, "run-old")
    with open_workspace(fx.home, writable=True) as workspace:
        recorded = run_schema._migration_intent(workspace)  # noqa: SLF001 -- simulate the crash
        prepare_operation(
            workspace.state,
            operation_id=MIGRATION_OPERATION,
            kind=str(recorded["kind"]),
            request_hash=str(recorded["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=recorded["expected_parent"],
            payload_hash=str(recorded["payload_hash"]),
        )
        workspace.state.commit()
    with open_workspace(fx.home) as workspace:
        interrupted = inspect_run_schema(workspace)
    assert (interrupted.state_version, interrupted.migration_phase) == (1, "PREPARED")
    assert migrate_run_schema(fx.home)["migrated"] is True
    assert read(fx.home, "run-old")["run_id"] == "run-old"
    with open_workspace(fx.home) as workspace:
        assert inspect_run_schema(workspace).migration_phase == "COMPLETED"


def test_recover_finishes_a_migration_whose_rebuild_already_landed(tmp_path: Path) -> None:
    """db recover completes from stored content only; it never rebuilds anything."""
    fx = prepared_v1(tmp_path)
    record_run(fx, "run-old")
    with open_workspace(fx.home, writable=True) as workspace:
        recorded = run_schema._migration_intent(workspace)  # noqa: SLF001
        prepare_operation(
            workspace.state,
            operation_id=MIGRATION_OPERATION,
            kind=str(recorded["kind"]),
            request_hash=str(recorded["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=recorded["expected_parent"],
            payload_hash=str(recorded["payload_hash"]),
        )
        run_schema._rebuild_state(workspace)  # noqa: SLF001 -- the rebuild that already landed
        workspace.state.commit()
    with open_workspace(fx.home, writable=True) as workspace:
        assert recover_operations(workspace, budget=BUDGET) == {
            "recovered": [MIGRATION_OPERATION],
            "pending": [],
            "provider_calls": 0,
        }
        workspace.state.commit()
    assert read(fx.home, "run-old")["run_id"] == "run-old"


def test_recover_leaves_a_migration_whose_rebuild_never_ran_pending(tmp_path: Path) -> None:
    """Recovery has no backup decision to make, so it does not start the rebuild."""
    fx = prepared_v1(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        recorded = run_schema._migration_intent(workspace)  # noqa: SLF001
        prepare_operation(
            workspace.state,
            operation_id=MIGRATION_OPERATION,
            kind=str(recorded["kind"]),
            request_hash=str(recorded["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=recorded["expected_parent"],
            payload_hash=str(recorded["payload_hash"]),
        )
        workspace.state.commit()
    with open_workspace(fx.home, writable=True) as workspace:
        assert recover_operations(workspace, budget=BUDGET)["pending"] == [MIGRATION_OPERATION]
        assert inspect_run_schema(workspace).state_version == 1


def test_the_backup_happens_before_the_rebuild(tmp_path: Path) -> None:
    """An operator's way back has to exist before the table it protects is replaced."""
    from aegis_alpha.storage import backup as backup_owner  # noqa: PLC0415

    fx = prepared_v1(tmp_path)
    phases: list[str] = []
    backed_up = backup_owner.backup_workspace
    rebuilt = run_schema._rebuild_state  # noqa: SLF001

    def observe_backup(
        workspace: Workspace, output: Path | None = None, *, budget: ComputeBudget | None = None
    ) -> dict[str, object]:
        phases.append("backup")
        return backed_up(workspace, output, budget=budget)

    def observe_rebuild(workspace: Workspace) -> None:
        phases.append("rebuild")
        rebuilt(workspace)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(run_schema, "backup_workspace", observe_backup, raising=False)
        patch.setattr(backup_owner, "backup_workspace", observe_backup)
        patch.setattr(run_schema, "_rebuild_state", observe_rebuild)
        migrate_run_schema(fx.home)
    assert phases == ["backup", "rebuild"]


def test_migration_refuses_while_a_run_is_still_open(tmp_path: Path) -> None:
    fx = prepared_v1(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        open_run(workspace, intent(fx, "run-open"))
    with pytest.raises(RunSchemaError, match="stop running analyses"):
        migrate_run_schema(fx.home)


def test_migration_refuses_beside_an_unrelated_prepared_operation(tmp_path: Path) -> None:
    fx = prepared_v1(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        prepare_operation(
            workspace.state,
            operation_id="unrelated-1",
            kind="market_publish",
            request_hash="a" * 64,
            target_id="somewhere",
            expected_parent=None,
            payload_hash="b" * 64,
        )
        workspace.state.commit()
    with pytest.raises(RunSchemaError, match="recover unrelated operations"):
        migrate_run_schema(fx.home)


def test_a_declared_research_run_is_refused_before_the_migration(tmp_path: Path) -> None:
    """The refusal names the migration rather than arriving as a constraint error."""
    fx = prepared_v1(tmp_path)
    with open_workspace(fx.home) as workspace:
        require_request_schema(workspace, BACKTEST_REQUEST_SCHEMA)
        with pytest.raises(RunSchemaError, match="run aas db run-migrate") as refusal:
            require_request_schema(workspace, RESEARCH_REQUEST_SCHEMA)
    assert refusal.value.reason == "run_schema_outdated"
    migrate_run_schema(fx.home)
    with open_workspace(fx.home) as workspace:
        require_request_schema(workspace, RESEARCH_REQUEST_SCHEMA)


def test_the_widened_check_is_an_allow_list_and_not_an_opening(tmp_path: Path) -> None:
    """Two named contracts, not any text a writer happens to supply."""
    fx = prepared_v1(tmp_path)
    migrate_run_schema(fx.home)
    with (
        open_workspace(fx.home, writable=True) as workspace,
        pytest.raises(sqlite3.IntegrityError),
    ):
        workspace.state.execute(
            "INSERT INTO run_details(run_id,request_hash,prior_run_id,request_schema) "
            "VALUES ('run-x',?,NULL,'aas-something-else-v1')",
            ("c" * 64,),
        )
    with open_workspace(fx.home) as workspace:
        require_request_schema(workspace, RESEARCH_REQUEST_SCHEMA)
        with pytest.raises(RunSchemaError, match="run_schema_unsupported_request"):
            require_request_schema(workspace, "aas-something-else-v1")


def test_a_receipt_history_nobody_wrote_is_refused(tmp_path: Path) -> None:
    """The version is read from receipts, so an invented one cannot rename the schema."""
    fx = prepared_v1(tmp_path)
    with open_workspace(fx.home, writable=True) as workspace:
        workspace.state.execute("INSERT INTO run_schema VALUES (3,?)", ("d" * 64,))
        with pytest.raises(RunSchemaError, match="state receipt mismatch"):
            inspect_run_schema(workspace)
        workspace.state.rollback()


def test_migrating_an_installation_without_the_add_on_is_refused(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with pytest.raises(RunSchemaError, match="install the run add-on"):
        migrate_run_schema(home)


def test_a_migration_intent_cannot_be_quarantined_into_a_dead_end(tmp_path: Path) -> None:
    """A quarantined intent is never prepared again, so ending this one would strand it."""
    fx = prepared_v1(tmp_path)
    record_run(fx, "run-old")
    with open_workspace(fx.home, writable=True) as workspace:
        recorded = run_schema._migration_intent(workspace)  # noqa: SLF001
        prepare_operation(
            workspace.state,
            operation_id=MIGRATION_OPERATION,
            kind=str(recorded["kind"]),
            request_hash=str(recorded["request_hash"]),
            target_id=workspace.installation_id,
            expected_parent=recorded["expected_parent"],
            payload_hash=str(recorded["payload_hash"]),
        )
        with pytest.raises(ValueError, match="finished by aas db run-migrate"):
            quarantine(workspace, MIGRATION_OPERATION, "operator gave up")
        workspace.state.commit()
    # Still finishable, and the run it protects is still there.
    assert migrate_run_schema(fx.home)["migrated"] is True
    assert read(fx.home, "run-old")["run_id"] == "run-old"
