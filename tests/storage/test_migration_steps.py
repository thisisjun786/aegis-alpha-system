"""The core schema migration as a runner of single steps, and what a step's backup carries.

The newest real version is followed by a synthetic one (``core_step_support``) whose texts
change nothing, so every multi-step path runs before a real later version exists. Every
case uses a disposable installation this module created; the promotion is synthetic.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Mapping
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import cast

import duckdb
import pytest

from aegis_alpha.application.cutover import backup_check, read_backup
from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage import migration, workspace
from aegis_alpha.storage.backup import backup, backup_workspace, restore
from aegis_alpha.storage.bulk_generation import BulkPlan, BulkRequest
from aegis_alpha.storage.market import marker_for, publish_generation
from aegis_alpha.storage.migration import (
    MIGRATION_OPERATION,
    CoreSchemaError,
    inspect_core_schema,
    migrate_core_schema,
    plan_core_migration,
    step_operation,
)
from aegis_alpha.storage.promotion import engine, formats
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.promotion.spec import PromotionSpec, parse_spec
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_reader import resolve_source
from aegis_alpha.storage.state import get_operation, prepare_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.capacity_support import FailingCommit
from tests.storage.core_step_support import add_synthetic_step
from tests.storage.promotion_support import (
    ZONE,
    add_bulk_source,
    add_source,
    at,
    bar,
    bulk_row,
    register_symbols,
    spec,
)
from tests.storage.test_migration import (
    RECORDED_MARKET,
    RECORDED_STATE,
    V1_MARKERS,
    file_digest,
    receipts,
    recorded_hashes,
    v1_installation,
    versions,
)
from tests.storage.test_observation_inputs import _observation_spec
from tests.storage.test_research_inputs import _register_domain

_KILLED = 137
_ROOT = Path(__file__).resolve().parents[2]
_HASH = "c" * 64
# The v2 step's identity, frozen from the code that shipped it: an installation whose
# intent was recorded then must still match it under every later version.
_V2_REQUEST = "c5fd3fe558418f3ba31cfab4dfac789054fa1364c4f92dc4894685e4f495aab6"
_V2_PARENT = "d17ecf0055f145969094bce9879c58f0ef73ec82a526909efb99e56924d6d515"
_IDS = {
    "installation_id": "synthetic-installation",
    "state_store_id": "a" * 32,
    "market_store_id": "b" * 32,
}
_DAY = date(2025, 1, 2)
_CHILD = """
import os, sys
from pathlib import Path
import pytest
from aegis_alpha.storage import backup, migration
from tests.storage.core_step_support import add_synthetic_step
add_synthetic_step(pytest.MonkeyPatch())
home, step, output, target = sys.argv[1:5]
name, _, nth = step.partition(":")
calls = []
def nth_call(original):
    def call(*args, **kwargs):
        calls.append(None)
        if len(calls) == int(nth or 1):
            os._exit(137)
        return original(*args, **kwargs)
    return call
def die_in_market(connection, *args):
    connection.execute("BEGIN TRANSACTION")
    connection.execute("CREATE TABLE killed_inside_market (x INTEGER)")
    os._exit(137)
if name == "inside-market":
    migration.upgrade_market = die_in_market
elif name == "backup_workspace":
    backup.backup_workspace = nth_call(backup.backup_workspace)
else:
    setattr(migration, name, nth_call(getattr(migration, name)))
migration.migrate_core_schema(Path(home), to_version=int(target), backup_output=Path(output))
os._exit(0)
"""


@pytest.fixture
def synthetic(monkeypatch: pytest.MonkeyPatch) -> int:
    """This code knows one more core version than it ships; that version."""
    return add_synthetic_step(monkeypatch)


def kill_at(home: Path, step: str, output: Path, target: int) -> int:
    """Migrate in a child that knows the synthetic step and dies at ``step``.

    ``name:n`` dies at the n-th call of that function, so a multi-step run can be
    stopped in a later step.
    """
    return subprocess.run(  # noqa: S603 -- fixed interpreter and script, disposable home
        [sys.executable, "-c", _CHILD, str(home), step, str(output), str(target)],
        check=False,
        cwd=_ROOT,
        env=os.environ.copy(),
    ).returncode


def intents(home: Path) -> dict[str, dict[str, object]]:
    """Every core step intent, as the installation's state store holds it."""
    with open_workspace(home, migrating=True, require_strategies=False) as admitted:
        rows = admitted.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind='core-schema-migrate'"
        ).fetchall()
        return {
            str(row[0]): cast("dict[str, object]", get_operation(admitted.state, str(row[0])))
            for row in rows
        }


def snapshot_operation(root: Path, operation_id: str) -> dict[str, object]:
    """One intent as a backup's state copy holds it, read without touching the copy."""
    uri = (root / "state.sqlite3").absolute().as_uri() + "?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return cast("dict[str, object]", get_operation(connection, operation_id))
    finally:
        connection.close()


def pending_promotion(
    root: Path,
    *,
    crash: str = "commit",
    parented: bool = False,
    partial: bool = False,
    moved: bool = False,
) -> tuple[Path, dict[str, object]]:
    """A v2 installation whose one promotion stopped where the rehearsal's did.

    ``commit`` fails the promotion's COMMIT with the rehearsal's pin-block error, so the
    intent and its retained evidence stay with no marker, rows or flags. ``catalog``
    stops after the market commit instead, leaving a committed generation uncataloged.
    ``parented`` first publishes a generation the failed one names as its parent;
    ``partial`` also makes the failed one a bulk response of provider-reported partial
    rows, so its manifest records a ``partition_row_count`` check against that parent.
    ``moved`` then publishes another child of that parent, so the parent is no longer
    the head. Returns the home and the plan computed before the failure.
    """
    home = root / "home"
    with pytest.MonkeyPatch.context() as patch:
        # At v2 also when the synthetic step is known, which would install v3.
        patch.setattr(workspace, "_INSTALL_VERSION", 2)
        initialize(home)
    publish = engine.publish_generation_bulk

    def exhausted(
        connection: duckdb.DuckDBPyConnection,
        request: BulkRequest,
        *,
        budget: ComputeBudget,
        plan: BulkPlan | None = None,
        companion: Callable[[duckdb.DuckDBPyConnection], None] | None = None,
    ) -> dict[str, object]:
        failing = FailingCommit(connection).borrowed
        return publish(failing, request, budget=budget, plan=plan, companion=companion)

    def killed(*_: object, **__: object) -> None:
        raise RuntimeError("process killed")

    with open_workspace(home, writable=True, strategy_write=True) as admitted:
        pin = add_source(
            admitted, [bar("AAA.KO", _DAY, 100.0, retrieved=at("2025-01-10T00:00:00"))], tag="a"
        )
        identity = register_symbols(admitted, pin["source_id"])
        parent, mapper = None, None
        if parented or partial:
            first = promote(admitted, *spec([pin], identity), apply=True)
            parent = str(first["generation_id"])
        if parented and not partial:
            pin = add_source(
                admitted, [bar("AAA.KO", _DAY, 101.0, retrieved=at("2025-01-11T00:00:00"))], tag="b"
            )
        if partial:
            pin = add_bulk_source(
                admitted,
                [bulk_row("AAA", "KO", _DAY, 101)],
                tag="partial",
                linked=at("2025-01-20T00:00:00"),
            )
            mapper = {
                "name": "eodhd.bulk_quarantine@1",
                "args": {"timezone": ZONE, "currencies": {"KO": "KRW", "KQ": "KRW"}},
            }
        document = spec([pin], identity, parent=parent, mapper=mapper)
        planned = promote(admitted, *document, apply=False)
        with pytest.MonkeyPatch.context() as patch:
            if crash == "commit":
                patch.setattr(engine, "publish_generation_bulk", exhausted)
            else:
                patch.setattr(engine, "_complete", killed)
            with pytest.raises(ComputeResourceError if crash == "commit" else RuntimeError):
                promote(admitted, *document, apply=True)
        if moved:
            # The intent is set aside only so promote admits another request, then
            # recorded again exactly as it was.
            failed = cast(
                "dict[str, object]", get_operation(admitted.state, str(planned["operation_id"]))
            )
            _delete_operation(admitted, str(failed["operation_id"]))
            other = add_source(
                admitted, [bar("AAA.KO", _DAY, 102.0, retrieved=at("2025-01-12T00:00:00"))], tag="c"
            )
            promote(admitted, *spec([other], identity, parent=parent), apply=True)
            _prepare_again(admitted, failed)
    return home, planned


def _delete_operation(admitted: workspace.Workspace, operation_id: str) -> None:
    admitted.state.execute("DELETE FROM storage_operations WHERE operation_id=?", (operation_id,))
    admitted.state.commit()


def _prepare_again(admitted: workspace.Workspace, intent: dict[str, object]) -> None:
    prepare_operation(
        admitted.state,
        operation_id=str(intent["operation_id"]),
        kind=str(intent["kind"]),
        request_hash=str(intent["request_hash"]),
        target_id=str(intent["target_id"]),
        expected_parent=cast("str | None", intent["expected_parent"]),
        payload_hash=str(intent["payload_hash"]),
        created_at_us=cast("int", intent["created_at_us"]),
    )


def test_step_identities_are_frozen_and_outlive_later_steps(synthetic: int) -> None:
    assert step_operation(2) == MIGRATION_OPERATION == "core-schema-migrate-v2"
    # The synthetic step is known, yet the v2 step's identity is the one that shipped.
    assert content_sha256(migration._step_request(2, **_IDS)) == _V2_REQUEST  # noqa: SLF001
    assert migration._expected_parent(2) == _V2_PARENT  # noqa: SLF001
    later = cast("dict[str, list[str]]", migration._step_request(synthetic, **_IDS))  # noqa: SLF001
    assert (later["from_version"], later["to_version"]) == (2, 3)
    assert later["market_checksums"][:2] == list(RECORDED_MARKET)
    assert later["state_checksums"][:2] == list(RECORDED_STATE)
    assert migration._expected_parent(synthetic) == content_sha256(  # noqa: SLF001
        {"market": RECORDED_MARKET[1], "state": RECORDED_STATE[1]}
    )
    for unknown in (1, synthetic + 1):
        with pytest.raises(CoreSchemaError, match="core_schema_unknown_version"):
            migration._target(unknown)  # noqa: SLF001


def test_one_invocation_takes_every_step_with_its_own_backup(
    tmp_path: Path, synthetic: int
) -> None:
    home = v1_installation(tmp_path)
    plan = plan_core_migration(home, to_version=synthetic)
    steps = cast("list[dict[str, object]]", plan["migrations"])
    assert [(entry["operation_id"], entry["backup_required"]) for entry in steps] == [
        ("core-schema-migrate-v2", True),
        ("core-schema-migrate-v3", True),
    ]
    output = tmp_path / "backups"
    report = migrate_core_schema(home, to_version=synthetic, backup_output=output)
    assert (report["state"], report["market_version"], report["state_version"]) == ("current", 3, 3)
    steps = cast("list[dict[str, object]]", report["migrations"])
    assert [entry["operation_id"] for entry in steps] == [step_operation(2), step_operation(3)]
    recorded = intents(home)
    for entry in steps:
        # Each step's intent names its own backup, taken right before that step.
        root = output / str(entry["operation_id"])
        assert entry["backup_root"] == str(root)
        assert recorded[str(entry["operation_id"])]["payload_hash"] == file_digest(
            root / "backup.json"
        )
    # The v2 step's backup is the v1 installation and the v3 step's the v2 one.
    for step in (2, 3):
        stores = json.loads((output / step_operation(step) / "installation.json").read_text())
        assert stores["stores"]["market"]["schema_version"] == step - 1
    with open_workspace(home) as admitted:
        assert inspect_core_schema(admitted).migration_operation == step_operation(3)
    assert [row[0] for row in receipts(home)["market"]] == [1, 2, 3]
    assert recorded_hashes(home) == V1_MARKERS


def test_to_2_stops_there_and_a_later_step_starts_from_it(tmp_path: Path, synthetic: int) -> None:
    home = v1_installation(tmp_path)
    report = migrate_core_schema(home, to_version=2, backup_output=tmp_path / "v2")
    # Only the v2 step ran, under its shipped identity; the installation is now outdated.
    assert (report["state"], report["market_version"], report["operation_id"]) == (
        "outdated",
        2,
        MIGRATION_OPERATION,
    )
    v2 = intents(home)[MIGRATION_OPERATION]
    with open_workspace(home) as admitted:
        status = inspect_core_schema(admitted)
        assert (status.state, status.migration_phase) == ("outdated", "COMPLETED")
    plan = plan_core_migration(home, to_version=synthetic)
    assert (plan["steps"], plan["backup_required"]) == (list(migration._STEPS), True)  # noqa: SLF001
    report = migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "v3")
    assert report["backup_root"] == str(tmp_path / "v3")
    assert intents(home)[MIGRATION_OPERATION] == v2
    # Nothing is downgraded: an earlier target on a later store is a no-op.
    again = migrate_core_schema(home, to_version=2, backup_output=None)
    assert (again["migrated"], again["market_version"]) == (False, 3)
    assert recorded_hashes(home) == V1_MARKERS


@pytest.mark.parametrize(
    "step",
    ["inside-market", "_upgrade_market", "_upgrade_state", "_write_receipt", "complete_operation"],
)
def test_a_kill_in_a_later_step_is_finished_by_repeating_it(
    tmp_path: Path, synthetic: int, step: str
) -> None:
    home = tmp_path / "home"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(workspace, "_INSTALL_VERSION", 2)
        initialize(home)
    assert kill_at(home, step, tmp_path / "backup", synthetic) == _KILLED
    with pytest.raises(ValueError, match="repeat aas db migrate --to 3"):
        open_workspace(home).__enter__()
    plan = plan_core_migration(home, to_version=synthetic)
    assert (plan["state"], plan["migration_operation"], plan["backup_required"]) == (
        "incomplete",
        step_operation(3),
        False,
    )
    # An earlier target cannot leave the prepared step half done.
    with pytest.raises(CoreSchemaError, match="repeat aas db migrate --to 3"):
        migrate_core_schema(home, to_version=2, backup_output=None)
    report = migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "unused")
    assert not (tmp_path / "unused").exists()
    assert report["migrations"] == [
        {
            "operation_id": step_operation(3),
            "from_version": 2,
            "to_version": 3,
            "resumed": True,
            "backup_root": None,
            "backup_manifest_sha256": file_digest(tmp_path / "backup" / "backup.json"),
        }
    ]
    assert [row[0] for row in receipts(home)["market"]] == [1, 2, 3]
    with open_workspace(home) as admitted:
        assert admitted.market.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name='killed_inside_market'"
        ).fetchone() == (0,)


def test_a_kill_between_steps_needs_the_next_steps_own_backup(
    tmp_path: Path, synthetic: int
) -> None:
    home = v1_installation(tmp_path)
    output = tmp_path / "backups"
    # The v2 step completed; the run died taking the v3 step's backup, before its intent.
    assert kill_at(home, "backup_workspace:2", output, synthetic) == _KILLED
    assert set(intents(home)) == {MIGRATION_OPERATION}
    assert versions(home) == {"state": 2, "market": 2}
    plan = plan_core_migration(home, to_version=synthetic)
    assert (plan["state"], plan["migration_phase"], plan["backup_required"]) == (
        "outdated",
        "COMPLETED",
        True,
    )
    with pytest.raises(CoreSchemaError, match="core_schema_backup_required"):
        migrate_core_schema(home, to_version=synthetic, backup_output=None)
    # The interrupted invocation's directory is not new, so it is not reused.
    with pytest.raises(ValueError, match="new directory"):
        migrate_core_schema(home, to_version=synthetic, backup_output=output)
    assert set(intents(home)) == {MIGRATION_OPERATION}
    report = migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "v3")
    assert report["backup_manifest_sha256"] == file_digest(tmp_path / "v3" / "backup.json")
    assert recorded_hashes(home) == V1_MARKERS


@pytest.mark.parametrize("step", ["_upgrade_state", "_write_receipt", "complete_operation"])
def test_a_step_whose_market_landed_resumes_into_the_next_step(
    tmp_path: Path, synthetic: int, step: str
) -> None:
    home = v1_installation(tmp_path)
    # The v2 step died after its market COMMIT, before the receipt named v2.
    assert kill_at(home, step, tmp_path / "v2", synthetic) == _KILLED
    plan = plan_core_migration(home, to_version=synthetic)
    assert (plan["state"], plan["market_version"]) == ("incomplete", synthetic - 1)
    assert set(intents(home)) == {MIGRATION_OPERATION}
    taken = tmp_path / "taken"
    taken.mkdir()
    # The v3 step's destination is admitted as its backup would admit it, before the v2
    # step is finished: it must be new and outside raw, runs and secrets.
    with pytest.raises(ValueError, match="new directory"):
        migrate_core_schema(home, to_version=synthetic, backup_output=taken)
    with pytest.raises(ValueError, match="inside raw, runs, or secrets"):
        migrate_core_schema(home, to_version=synthetic, backup_output=home / "raw" / "new")
    assert not (home / "raw" / "new").exists()
    assert intents(home)[MIGRATION_OPERATION]["phase"] == "PREPARED"
    report = migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "v3")
    assert (report["state"], report["market_version"], report["state_version"]) == ("current", 3, 3)
    assert [
        (entry["operation_id"], entry["resumed"])
        for entry in cast("list[dict[str, object]]", report["migrations"])
    ] == [(step_operation(2), True), (step_operation(3), False)]
    # The v3 step's backup is of the finished v2 installation.
    stores = json.loads((tmp_path / "v3" / "installation.json").read_text())
    assert stores["stores"]["market"]["schema_version"] == synthetic - 1
    assert recorded_hashes(home) == V1_MARKERS


def test_an_unknown_step_intent_is_refused(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as admitted:
        prepare_operation(
            admitted.state,
            operation_id="core-schema-migrate-v9",
            kind="core-schema-migrate",
            request_hash=_HASH,
            target_id=admitted.installation_id,
            expected_parent=None,
            payload_hash=_HASH,
        )
    with pytest.raises(CoreSchemaError, match="names a step this code lacks"):
        plan_core_migration(home, to_version=2)


@pytest.mark.parametrize(("parented", "partial"), [(False, False), (True, False), (True, True)])
def test_an_untouched_promotion_is_carried_through_a_step_and_then_recovered(
    tmp_path: Path, synthetic: int, *, parented: bool, partial: bool
) -> None:
    home, planned = pending_promotion(tmp_path, parented=parented, partial=partial)
    operation_id, generation_id = str(planned["operation_id"]), str(planned["generation_id"])
    with open_workspace(home) as admitted:
        intent = get_operation(admitted.state, operation_id)
    assert intent is not None
    assert intent["phase"] == "PREPARED"
    # An ordinary backup still refuses every prepared operation.
    with pytest.raises(ValueError, match="recovered operations"):
        backup(home, tmp_path / "ordinary")
    assert not (tmp_path / "ordinary").exists()
    plan = plan_core_migration(home, to_version=synthetic)
    # The plan names the carried intent and says it was proven by planning it again.
    assert plan["carried_operations"] == [
        {
            "operation_id": operation_id,
            "kind": "promotion",
            "generation_id": generation_id,
            "manifest_sha256": intent["payload_hash"],
            "proof": "replanned: the retained spec recomputes the intent's manifest",
        }
    ]
    assert plan["blocking_operations"] == []
    snapshot = tmp_path / "snapshot"
    report = migrate_core_schema(home, to_version=synthetic, backup_output=snapshot)
    assert (report["state"], report["pending_operations"]) == ("current", [operation_id])
    manifest = json.loads((snapshot / "backup.json").read_text())
    # The snapshot says what it holds: the pending intent, counted, with its evidence.
    assert manifest["carried_operations"] == [operation_id]
    assert manifest["logical"]["pending_operations"] == 1
    for digest in (str(intent["payload_hash"]), str(intent["request_hash"])):
        assert f"raw/{digest[:2]}/{digest}" in manifest["files"]
    assert snapshot_operation(snapshot, operation_id) == intent
    with open_workspace(home, writable=True) as admitted:
        # The migration neither ended nor changed the intent.
        assert get_operation(admitted.state, operation_id) == intent
        assert recover_operations(admitted)["recovered"] == [operation_id]
        marker = marker_for(admitted.market, generation_id)
        # Recovery published exactly what was planned before the failed COMMIT, under
        # the manifest the intent recorded then.
        expected = cast("dict[str, object]", planned["marker"])
        keys = ("generation_id", "request_hash", "delta_hash", "chain_hash", "row_count")
        for key in (*keys, "version", "sequence", "parent_id"):
            assert marker[key] == expected[key]
        retained = json.loads(engine._read_raw(admitted, str(intent["payload_hash"])))  # noqa: SLF001
        assert [retained[key] for key in keys] == [expected[key] for key in keys]
        recovered = get_operation(admitted.state, operation_id)
        assert recovered is not None
        assert (recovered["phase"], recovered["payload_hash"]) == (
            "COMPLETED",
            intent["payload_hash"],
        )
        cataloged = admitted.state.execute(
            "SELECT chain_hash, manifest_hash FROM dataset_versions WHERE generation_id=?",
            (generation_id,),
        ).fetchone()
        assert tuple(cataloged) == (expected["chain_hash"], expected["request_hash"])
        assert verify_workspace(admitted)["pending_operations"] == 0
        # The snapshot is no recovered installation's backup: the cutover check sees the
        # promotion and the migration completed after it.
        assert backup_check(read_backup(snapshot), admitted)["holds_every_operation"] is False


def test_the_snapshot_restores_for_v2_code_and_migrates_again(tmp_path: Path) -> None:
    home, planned = pending_promotion(tmp_path)
    operation_id = str(planned["operation_id"])
    snapshot = tmp_path / "snapshot"
    with pytest.MonkeyPatch.context() as patch:
        synthetic = add_synthetic_step(patch)
        migrate_core_schema(home, to_version=synthetic, backup_output=snapshot)
    # Code that knows only v2 restores it, verifies it like the snapshot was, and reads
    # its frozen receipts and the still pending promotion.
    restored = restore(snapshot, tmp_path / "restored")
    assert cast("dict[str, object]", restored["verification"])["pending_operations"] == 1
    assert receipts(tmp_path / "restored") == {
        "state": list(enumerate(RECORDED_STATE, 1)),
        "market": list(enumerate(RECORDED_MARKET, 1)),
    }
    with pytest.MonkeyPatch.context() as patch:
        synthetic = add_synthetic_step(patch)
        report = migrate_core_schema(
            tmp_path / "restored", to_version=synthetic, backup_output=tmp_path / "again"
        )
        assert report["pending_operations"] == [operation_id]
        with open_workspace(tmp_path / "restored", writable=True) as admitted:
            assert recover_operations(admitted)["recovered"] == [operation_id]
            marker = marker_for(admitted.market, str(planned["generation_id"]))
            expected = cast("dict[str, object]", planned["marker"])
            assert (marker["delta_hash"], marker["chain_hash"]) == (
                expected["delta_hash"],
                expected["chain_hash"],
            )


def _committed(home: Path, operation_id: str) -> None:
    del home, operation_id  # the fixture's own crash left the generation committed


def _untouched(home: Path, operation_id: str) -> None:
    del home, operation_id  # the fixture's own state is the case


def _manifest_gone(home: Path, operation_id: str) -> None:
    intent = intents_any(home, operation_id)
    digest = str(intent["payload_hash"])
    (home / "raw" / digest[:2] / digest).unlink()


def _request_altered(home: Path, operation_id: str) -> None:
    intent = intents_any(home, operation_id)
    digest = str(intent["request_hash"])
    (home / "raw" / digest[:2] / digest).write_bytes(b"{}")


def intents_any(home: Path, operation_id: str) -> dict[str, object]:
    with open_workspace(home) as admitted:
        return cast("dict[str, object]", get_operation(admitted.state, operation_id))


def _intent_rewritten(**fields: object) -> Callable[[Path, str], None]:
    """Record the intent again with ``fields`` changed and everything else as it was."""

    def alter(home: Path, operation_id: str) -> None:
        with open_workspace(home, writable=True) as admitted:
            intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
            _delete_operation(admitted, operation_id)
            _prepare_again(admitted, intent | fields)

    return alter


def _manifest_rewritten(
    change: Callable[[dict[str, object]], dict[str, object]],
) -> Callable[[Path, str], None]:
    """Retain a changed manifest and point a fresh intent of the same request at it.

    The manifest is content-addressed and the intent names it, so its bytes are
    consistent with the intent; only planning the retained spec again can tell that no
    plan records it.
    """

    def alter(home: Path, operation_id: str) -> None:
        with open_workspace(home, writable=True) as admitted:
            intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
            body = json.loads(engine._read_raw(admitted, str(intent["payload_hash"])))  # noqa: SLF001
            _, digest, _ = put_raw(admitted.paths.raw, formats.canonical(change(body)))
            _delete_operation(admitted, operation_id)
            _prepare_again(admitted, intent | {"payload_hash": digest})

    return alter


def _identity_only(body: dict[str, object]) -> dict[str, object]:
    kept = ("schema", "request_hash", "spec_sha256", "generation_id", "operation_id", "parent")
    return {key: body[key] for key in kept}


def _other_dataset(body: dict[str, object]) -> dict[str, object]:
    return body | {"domain": "filings", "dataset_id": "wrong.dataset"}


def _uncounted(body: dict[str, object]) -> dict[str, object]:
    return body | {"row_count": cast("int", body["row_count"]) + 1}


def _restated(body: dict[str, object]) -> dict[str, object]:
    # Every field keeps its type and agrees with every other; only the plan disagrees.
    return body | {"unchanged": cast("int", body["unchanged"]) + 1}


def _refused_rows(body: dict[str, object]) -> dict[str, object]:
    return body | {"rows": {"refused_required": 1}}


def _restated_partition(body: dict[str, object]) -> dict[str, object]:
    check = cast("dict[str, object]", body["partition_row_count"])
    return body | {"partition_row_count": check | {"result": "no_reference"}}


def _source_changed(home: Path, operation_id: str) -> None:
    """Change a value of the pinned source table, keeping its row count."""
    with open_workspace(home, writable=True) as admitted:
        intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
        manifest = json.loads(engine._read_raw(admitted, str(intent["payload_hash"])))  # noqa: SLF001
        spec_sha = str(manifest["spec_sha256"])
        retained = parse_spec(engine._read_raw(admitted, spec_sha), spec_sha)  # noqa: SLF001
        target = resolve_source(admitted, retained.sources[0])["target"]
        admitted.market.execute(f'UPDATE "{target}" SET close = close + 1')  # noqa: S608


def _cataloged(rule: str) -> Callable[[Path, str], None]:
    """One quality check a catalog completion writes, without its other rows."""

    def alter(home: Path, operation_id: str) -> None:
        with open_workspace(home, writable=True) as admitted:
            intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
            check_id = "qc-" + hashlib.sha256(f"{intent['target_id']}/{rule}".encode()).hexdigest()
            admitted.state.execute(
                "INSERT INTO quality_checks(check_id, dataset_id, version, rule_id, "
                "rule_version, result, reason, checked_at_us) VALUES (?,?,?,?,?,?,?,?)",
                (check_id, "prices.kr.eodhd", "1", rule, "1", "recorded", "[]", 0),
            )
            admitted.state.commit()

    return alter


_REPLANNED = "no longer plans its manifest"


@pytest.mark.parametrize(
    ("fixture", "alter", "reason"),
    [
        # What reached the market or the catalog.
        ("catalog", _committed, "committed"),
        # A quality check needs a catalog version, so these follow a cataloged parent.
        ("partial", _cataloged("promotion"), "cataloged"),
        ("partial", _cataloged("partition_row_count"), "cataloged"),
        # Evidence that is not the intent's own.
        ("commit", _manifest_gone, "absent from raw"),
        ("commit", _request_altered, "does not match its address"),
        ("commit", _intent_rewritten(expected_parent="prm-" + "0" * 64), "parent is not its"),
        # Manifests consistent with the intent that no plan of its spec records.
        ("commit", _manifest_rewritten(_identity_only), _REPLANNED),
        ("commit", _manifest_rewritten(_other_dataset), _REPLANNED),
        ("commit", _manifest_rewritten(_uncounted), _REPLANNED),
        ("commit", _manifest_rewritten(_restated), _REPLANNED),
        ("commit", _manifest_rewritten(_refused_rows), _REPLANNED),
        ("partial", _manifest_rewritten(_restated_partition), _REPLANNED),
        # A plan that no longer reproduces the intent: a moved head, a changed source.
        ("moved", _untouched, _REPLANNED),
        ("commit", _source_changed, "differs from the pin"),
    ],
)
def test_only_a_promotion_recovery_would_publish_is_carried(
    tmp_path: Path, synthetic: int, fixture: str, alter: Callable[[Path, str], None], reason: str
) -> None:
    home, planned = pending_promotion(
        tmp_path,
        crash="catalog" if fixture == "catalog" else "commit",
        parented=fixture == "moved",
        partial=fixture == "partial",
        moved=fixture == "moved",
    )
    operation_id = str(planned["operation_id"])
    alter(home, operation_id)
    plan = plan_core_migration(home, to_version=synthetic)
    assert (plan["carried_operations"], plan["blocking_operations"]) == ([], [operation_id])
    with pytest.raises(CoreSchemaError, match="core_schema_busy"):
        migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()
    assert versions(home) == {"state": 2, "market": 2}
    with open_workspace(home, writable=True) as admitted:
        intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
        refusal = engine.untouched_promotion_refusal(admitted, intent)
        assert refusal is not None
        assert reason in refusal
        # The check leaves no plan behind on the connection.
        assert admitted.market.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE temporary"
        ).fetchone() == (0,)
        # Naming the operation does not admit it: the backup checks it again itself.
        with pytest.raises(ValueError, match="recovered operations"):
            backup_workspace(admitted, tmp_path / "named", carry=[operation_id])
        if reason == _REPLANNED:
            # Recovery, by the same test, would not publish it either.
            assert engine.recover_promotion(admitted, intent) is False
    assert not (tmp_path / "named").exists()


def test_a_promotion_intent_of_another_request_blocks(tmp_path: Path, synthetic: int) -> None:
    home, planned = pending_promotion(tmp_path)
    with open_workspace(home, writable=True) as admitted:
        # A promotion's operation and generation IDs come from its request hash.
        prepare_operation(
            admitted.state,
            operation_id="promotion:" + "d" * 64,
            kind="promotion",
            request_hash="e" * 64,
            target_id="prm-" + "d" * 64,
            expected_parent=None,
            payload_hash=_HASH,
        )
    plan = plan_core_migration(home, to_version=synthetic)
    carried = cast("list[dict[str, object]]", plan["carried_operations"])
    assert [entry["operation_id"] for entry in carried] == [planned["operation_id"]]
    assert plan["blocking_operations"] == ["promotion:" + "d" * 64]
    with pytest.raises(CoreSchemaError, match="promotion:d"):
        migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()


@pytest.mark.parametrize("kind", ["market_publish", "source_import", "run_commit", "source-retire"])
def test_a_prepared_operation_of_another_kind_blocks(
    tmp_path: Path, synthetic: int, kind: str
) -> None:
    home, planned = pending_promotion(tmp_path)
    with open_workspace(home, writable=True) as admitted:
        carried = cast(
            "dict[str, object]", get_operation(admitted.state, str(planned["operation_id"]))
        )
        # Even one naming the carried promotion's own request, generation and manifest.
        other = carried | {"operation_id": f"{kind}:other", "kind": kind}
        _prepare_again(admitted, other)
        assert engine.untouched_promotion_refusal(admitted, other) == "not a prepared promotion"
    plan = plan_core_migration(home, to_version=synthetic)
    assert plan["blocking_operations"] == [f"{kind}:other"]
    with pytest.raises(CoreSchemaError, match="core_schema_busy"):
        migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()
    assert versions(home) == {"state": 2, "market": 2}


def test_recovery_and_the_carry_share_one_replan(
    tmp_path: Path, synthetic: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, planned = pending_promotion(tmp_path)
    operation_id = str(planned["operation_id"])
    shared = engine._replanned  # noqa: SLF001
    calls: list[str] = []

    def refusing(
        admitted: workspace.Workspace,
        operation: Mapping[str, object],
        retained: PromotionSpec,
        budget: ComputeBudget,
    ) -> None:
        calls.append(str(operation["operation_id"]))
        assert shared(admitted, operation, retained, budget) is not None

    with monkeypatch.context() as patch:
        patch.setattr(engine, "_replanned", refusing)
        with open_workspace(home, writable=True) as admitted:
            intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
            # Refusing the one replan refuses both the carry and the publication.
            assert engine.untouched_promotion_refusal(admitted, intent) == (
                "its retained spec " + _REPLANNED
            )
            assert engine.recover_promotion(admitted, intent) is False
            assert calls == [operation_id, operation_id]
        plan = plan_core_migration(home, to_version=synthetic)
        assert (plan["carried_operations"], plan["blocking_operations"]) == ([], [operation_id])
    plan = plan_core_migration(home, to_version=synthetic)
    assert plan["blocking_operations"] == []


def test_a_carry_is_planned_alike_after_a_verification_lowered_the_connection(
    tmp_path: Path, synthetic: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, planned = pending_promotion(tmp_path)
    operation_id = str(planned["operation_id"])
    with open_workspace(home, writable=True, strategy_write=True) as admitted:
        # A published observation input, whose verification runs on a component share
        # of the lease and leaves the connection there.
        intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
        _delete_operation(admitted, operation_id)
        observed = _observation_spec(admitted, tmp_path / "observed.sqlite3")
        _register_domain(admitted, observed, "observation")
        _prepare_again(admitted, intent)
    lease = ComputeBudget(Fraction(1), 32 * 1024 * 1024)
    original = engine.plan_promotion
    limits: list[tuple[object, ...]] = []

    def traced(
        admitted: workspace.Workspace, retained: PromotionSpec, *, budget: ComputeBudget
    ) -> object:
        limits.append(
            cast(
                "tuple[object, ...]",
                admitted.market.execute(
                    "SELECT current_setting('threads'), current_setting('memory_limit')"
                ).fetchone(),
            )
        )
        return original(admitted, retained, budget=budget)

    monkeypatch.setattr(engine, "plan_promotion", traced)
    plan = plan_core_migration(home, to_version=synthetic, budget=lease)
    assert [entry["operation_id"] for entry in cast("list[dict]", plan["carried_operations"])] == [
        operation_id
    ]
    report = migrate_core_schema(
        home, to_version=synthetic, backup_output=tmp_path / "snapshot", budget=lease
    )
    assert (report["state"], report["pending_operations"]) == ("current", [operation_id])
    # The plan, the migration's check and its backup's recheck each start from the
    # installation's own limits, so the lease the plan admitted admits the migration.
    assert len(limits) == 3  # noqa: PLR2004 -- plan, quiet check, backup recheck
    assert len(set(limits)) == 1


def test_a_head_moved_after_the_replan_leaves_the_promotion_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, planned = pending_promotion(tmp_path)
    operation_id = str(planned["operation_id"])
    shared = engine._replanned  # noqa: SLF001

    def moved(
        admitted: workspace.Workspace,
        operation: Mapping[str, object],
        retained: PromotionSpec,
        budget: ComputeBudget,
    ) -> object:
        plan = shared(admitted, operation, retained, budget)
        assert plan is not None
        # Another writer's generation of the same dataset lands between the replan and
        # its publication.
        staged = admitted.market.execute("SELECT * FROM _aas_p_stage").fetchone()
        assert staged is not None
        row = dict(zip([item[0] for item in admitted.market.description], staged, strict=True))
        row.pop("record_id")
        row["revision_id"] = "independent-revision"
        other = admitted.market.cursor()
        try:
            publish_generation(
                other,
                dataset_id=retained.dataset_id,
                version="independent",
                generation_id="independent-generation",
                operation_id="independent-operation",
                request_hash="f" * 64,
                parent_id=None,
                domain=retained.domain,
                rows=[row],
            )
        finally:
            other.close()
        return plan

    monkeypatch.setattr(engine, "_replanned", moved)
    with open_workspace(home, writable=True) as admitted:
        intent = cast("dict[str, object]", get_operation(admitted.state, operation_id))
        # The publication's parent check refuses it; recovery reports the intent pending
        # rather than failing, and drops the plan.
        assert recover_operations(admitted) == {
            "recovered": [],
            "pending": [operation_id],
            "provider_calls": 0,
        }
        assert get_operation(admitted.state, operation_id) == intent
        assert admitted.market.execute(
            "SELECT count(*) FROM market_generations WHERE generation_id=?",
            [planned["generation_id"]],
        ).fetchone() == (0,)
        assert admitted.market.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE temporary"
        ).fetchone() == (0,)


def test_a_replan_beyond_the_lease_refuses_the_migration(
    tmp_path: Path, synthetic: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, planned = pending_promotion(tmp_path)

    def exhausted(*_: object, **__: object) -> None:
        raise duckdb.OutOfMemoryException("Out of Memory Error: synthetic exhaustion")

    monkeypatch.setattr(engine, "plan_promotion", exhausted)
    # Never a silent carry, and never a refusal that hides the cause.
    with pytest.raises(ComputeResourceError, match="planning the prepared promotion again"):
        plan_core_migration(home, to_version=synthetic)
    with pytest.raises(ComputeResourceError, match="planning the prepared promotion again"):
        migrate_core_schema(home, to_version=synthetic, backup_output=tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()
    assert versions(home) == {"state": 2, "market": 2}
    with open_workspace(home, writable=True) as admitted:
        intent = cast(
            "dict[str, object]", get_operation(admitted.state, str(planned["operation_id"]))
        )
        with pytest.raises(ComputeResourceError):
            backup_workspace(admitted, tmp_path / "named", carry=[str(intent["operation_id"])])
        with pytest.raises(ComputeResourceError):
            engine.recover_promotion(admitted, intent)
        assert get_operation(admitted.state, str(intent["operation_id"])) == intent


def test_an_evidence_read_error_refuses_the_carry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, planned = pending_promotion(tmp_path)

    def failing(*_: object) -> bytes:
        raise OSError("synthetic read error")

    with open_workspace(home) as admitted:
        intent = cast(
            "dict[str, object]", get_operation(admitted.state, str(planned["operation_id"]))
        )
        assert engine.untouched_promotion_refusal(admitted, intent) is None
        monkeypatch.setattr(engine, "_read_raw", failing)
        assert engine.untouched_promotion_refusal(admitted, intent) == "synthetic read error"
