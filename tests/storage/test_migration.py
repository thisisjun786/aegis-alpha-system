"""The versioned core schema migration of the state and market stores, v1 to v2.

Every case runs against a disposable installation this module created. The v1 stores are
made by the installer itself at version 1, which is the shape every installation made
before v2 carries, so these are migrations rather than fresh installs under another name.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import duckdb
import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market, migration, publication, workspace
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.market import MARKET_CHECKSUMS, read_generation, verify_generation
from aegis_alpha.storage.migration import (
    CORE_VERSION,
    MIGRATION_OPERATION,
    STATE_CHECKSUMS,
    CoreSchemaError,
    inspect_core_schema,
    migrate_core_schema,
    plan_core_migration,
)
from aegis_alpha.storage.state import prepare_operation
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.capacity_support import PIN_BLOCK, FailingCommit
from tests.storage.core_step_support import add_synthetic_step
from tests.storage.test_publication import document

# The digests installations record. v1 is what every store made before v2 carries, so a
# drift in its DDL text shows up here instead of as an operator's store being refused.
# v2 and v3 are frozen the same way from the moment each ships.
RECORDED_MARKET = (
    "ab2383d7cb1181e7b98e7dc054042f82024a6aafc0c0fabb27ee5f94dbe0db7c",
    "094e607049afb422201481d745b584cdf88d077b10dc2e7e83b1e56a09a3038a",
    "432dd69954ad28e03d9e79bcc330f31862441f82f36c9e424cd6b2f6425cfa37",
)
RECORDED_STATE = (
    "da574cef54b69961c341e3e5e92ee16334a5049911ee6b417db643f44bd881dc",
    "80f98378e53e79a8580d5b2c966309e0737d976f78ae3332e345e0c0f3d9e296",
    "81ece2dac7ff62b5b26cd8fc28e73f85cbf9f2b8e32c2bd17e8597d4c65c9181",
)
_KILLED = 137
_HASH = "c" * 64
_GENERATIONS = ("synthetic-generation", "mixed-generation")
# The (delta_hash, chain_hash) markers v1 code records for the two generations
# v1_installation publishes. They were produced by the code before v2 existed, so a
# change to the hashed shape of a v1 row fails here rather than on an operator's store.
V1_MARKERS = {
    "synthetic-generation": (
        "f877f98d647418a53ce92f3ae01eff2ad9ddfea250bd9e38e76f358cd8684ebf",
        "1a44014585a39d0b1799cb952b95cda5fde20ab6ae1cabcf9a38cc2610fa4bba",
    ),
    "mixed-generation": (
        "189c775c11945d79a7851c486795ca5b444c4a5acc0788a474dda7dfba1b8fa0",
        "08da0282252b2bfc2345e9b8ffc06b859acf1a6744d4729f6b4d582a49c19e6d",
    ),
}


def price(day: str, **changes: object) -> dict[str, object]:
    row = json.loads(document())["rows"][0]
    return {**row, "session_date": day, "revision_id": "r-" + day, **changes}


def mixed_document() -> bytes:
    """A v1 generation of a present, a missing and an adjusted reference row."""
    imported = json.loads(document())
    absent = dict.fromkeys(("open", "high", "low", "close", "volume"))
    imported.update(
        dataset_id="mixed-prices",
        generation_id="mixed-generation",
        operation_id="mixed-import",
        rows=[
            price("2026-01-05"),
            price(
                "2026-01-06",
                **absent,
                value_state="missing",
                available_at_us=None,
                revision_known_at_us=None,
            ),
            price(
                "2026-01-07",
                basis="split_adjusted",
                price_role="reference",
                open="5",
                high="6",
                low="4.5",
                close="5.5",
                volume="200",
            ),
        ],
    )
    return json.dumps(imported).encode()


def v1_installation(root: Path) -> Path:
    """A v1 installation holding two committed prices generations.

    The publication clock is fixed, so each generation's recorded hashes are the same
    on every run and can be compared with the ones v1 code recorded.
    """
    home = root / "home"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(workspace, "_INSTALL_VERSION", 1)
        initialize(home)
        patch.setattr(publication, "time", SimpleNamespace(time_ns=lambda: 10**15))
        with open_workspace(home, writable=True) as admitted:
            for imported in (document(), mixed_document()):
                publication.publish_document(admitted, parse_import(imported))
    return home


def receipts(home: Path) -> dict[str, list[tuple[int, str]]]:
    with open_workspace(home) as admitted:
        query = "SELECT version,checksum FROM schema_migrations ORDER BY version"
        return {
            "state": [(int(row[0]), str(row[1])) for row in admitted.state.execute(query)],
            "market": [
                (int(row[0]), str(row[1])) for row in admitted.market.execute(query).fetchall()
            ],
        }


def versions(home: Path) -> dict[str, object]:
    with open_workspace(home) as admitted:
        return cast("dict[str, object]", admitted.doctor()["schema_versions"])


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stored_v1_generation(home: Path) -> dict[str, object]:
    """Each v1 generation's verified marker and rows, as the installation reads them."""
    with open_workspace(home) as admitted:
        return {
            generation: (
                verify_generation(admitted.market, generation),
                read_generation(admitted.market, generation),
            )
            for generation in _GENERATIONS
        }


def recorded_hashes(home: Path) -> dict[str, tuple[object, object]]:
    stored = cast("dict[str, tuple[dict[str, object], object]]", stored_v1_generation(home))
    return {
        generation: (marker["delta_hash"], marker["chain_hash"])
        for generation, (marker, _) in stored.items()
    }


def kill_at(home: Path, step: str, backup: Path) -> int:
    """Run the migration in a child that dies at ``step`` without unwinding anything."""
    script = (
        "import os, sys\n"
        "from pathlib import Path\n"
        "from aegis_alpha.storage import migration\n"
        "from aegis_alpha.storage.market_schema import V2_DDL\n"
        "from aegis_alpha.storage.state_schema import V2_DDL as STATE_V2\n"
        "step = sys.argv[2]\n"
        "def die(*args, **kwargs):\n"
        "    os._exit(137)\n"
        "def die_in_market(connection, *args):\n"
        "    connection.execute('BEGIN TRANSACTION')\n"
        "    connection.execute(V2_DDL.split(';')[0])\n"
        "    os._exit(137)\n"
        "def die_in_state(connection, *args):\n"
        "    connection.execute('BEGIN IMMEDIATE')\n"
        "    connection.execute(STATE_V2.split(';')[0])\n"
        "    os._exit(137)\n"
        "if step == 'inside-market':\n"
        "    migration.upgrade_market = die_in_market\n"
        "elif step == 'inside-state':\n"
        "    migration.upgrade_state = die_in_state\n"
        "else:\n"
        "    setattr(migration, step, die)\n"
        "migration.migrate_core_schema(\n"
        "    Path(sys.argv[1]), to_version=2, backup_output=Path(sys.argv[3])\n"
        ")\n"
        "os._exit(0)\n"
    )
    return subprocess.run(  # noqa: S603 -- fixed interpreter and script, disposable home
        [sys.executable, "-c", script, str(home), step, str(backup)],
        check=False,
        env=os.environ.copy(),
    ).returncode


def test_recorded_checksums_are_the_ones_installations_carry() -> None:
    assert MARKET_CHECKSUMS == RECORDED_MARKET
    assert STATE_CHECKSUMS == RECORDED_STATE
    assert CORE_VERSION == len(RECORDED_MARKET) == len(RECORDED_STATE)


def test_a_fresh_install_is_current_and_records_both_versions(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    assert versions(home) == {"state": 3, "market": 3}
    assert receipts(home) == {
        "state": list(enumerate(RECORDED_STATE, 1)),
        "market": list(enumerate(RECORDED_MARKET, 1)),
    }
    # Nothing to migrate, so no backup is asked for and nothing is written.
    report = migrate_core_schema(home, to_version=2, backup_output=None)
    assert (report["migrated"], report["state"], report["operation_id"]) == (
        False,
        "current",
        None,
    )


def test_migration_requires_backup_and_resumes(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    before = stored_v1_generation(home)
    with pytest.raises(CoreSchemaError, match="core_schema_backup_required"):
        migrate_core_schema(home, to_version=2, backup_output=None)
    # The refusal touched nothing: the installation is still an ordinary v1 one.
    assert versions(home) == {"state": 1, "market": 1}
    assert receipts(home) == {
        "state": [(1, RECORDED_STATE[0])],
        "market": [(1, RECORDED_MARKET[0])],
    }
    assert kill_at(home, "_upgrade_state", tmp_path / "backup") == _KILLED
    with pytest.raises(ValueError, match="migration is incomplete"):
        open_workspace(home).__enter__()
    # The backup was taken and verified before the intent that the kill left prepared.
    assert json.loads((tmp_path / "backup" / "backup.json").read_text())["complete"] is True
    # Repeating the command finishes it from the intent; the backup is not taken again.
    report = migrate_core_schema(home, to_version=2, backup_output=None)
    # A v2 installation is complete but older than this code's newest version.
    assert (report["migrated"], report["state"]) == (True, "outdated")
    assert report["backup_manifest_sha256"] == file_digest(tmp_path / "backup" / "backup.json")
    assert versions(home) == {"state": 2, "market": 2}
    assert stored_v1_generation(home) == before
    with open_workspace(home) as admitted:
        assert verify_workspace(admitted)["verified"] is True


@pytest.mark.parametrize(
    "step",
    [
        "inside-market",
        "_upgrade_market",
        "_upgrade_state",
        "inside-state",
        "_write_receipt",
        "complete_operation",
    ],
)
def test_a_kill_at_every_step_is_finished_by_repeating_the_command(
    tmp_path: Path, step: str
) -> None:
    home = v1_installation(tmp_path)
    before = stored_v1_generation(home)
    assert kill_at(home, step, tmp_path / "backup") == _KILLED
    with pytest.raises(ValueError, match="migration is incomplete"):
        open_workspace(home).__enter__()
    plan = plan_core_migration(home, to_version=2)
    assert plan["state"] == "incomplete"
    assert cast("list[str]", plan["steps"])[-1] == "complete"
    assert migrate_core_schema(home, to_version=2, backup_output=tmp_path / "unused")["migrated"]
    assert not (tmp_path / "unused").exists()
    assert receipts(home) == {
        "state": list(enumerate(RECORDED_STATE[:2], 1)),
        "market": list(enumerate(RECORDED_MARKET[:2], 1)),
    }
    assert stored_v1_generation(home) == before
    with open_workspace(home) as admitted:
        assert inspect_core_schema(admitted).migration_phase == "COMPLETED"
        assert verify_workspace(admitted)["pending_operations"] == 0


def test_incomplete_migration_refuses_normal_open(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    assert kill_at(home, "_write_receipt", tmp_path / "backup") == _KILLED
    # Both stores already moved on while the receipt still names v1: only the migration
    # command may admit that, and every ordinary reader and writer is refused.
    for writable in (False, True):
        with pytest.raises(ValueError, match="migration is incomplete"):
            open_workspace(home, writable=writable).__enter__()
    plan = plan_core_migration(home, to_version=2)
    assert (plan["state_version"], plan["market_version"]) == (2, 2)
    assert (plan["receipt_state_version"], plan["receipt_market_version"]) == (1, 1)
    assert plan["steps"] == ["receipt", "complete"]
    assert plan["migration_phase"] == "PREPARED"
    migrate_core_schema(home, to_version=2, backup_output=None)
    # Ending the intent instead of finishing it would strand the installation.
    with (
        open_workspace(home, writable=True) as admitted,
        pytest.raises(ValueError, match="finished by aas db migrate"),
    ):
        publication.quarantine(admitted, MIGRATION_OPERATION, "operator")


def test_migration_keeps_v1_receipt_and_rejects_unknown(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    before = stored_v1_generation(home)
    assert recorded_hashes(home) == V1_MARKERS
    report = migrate_core_schema(home, to_version=2, backup_output=tmp_path / "backup")
    assert report["receipts"] == {
        "state": [[1, RECORDED_STATE[0]], [2, RECORDED_STATE[1]]],
        "market": [[1, RECORDED_MARKET[0]], [2, RECORDED_MARKET[1]]],
    }
    # A v1 generation keeps the hashes v1 code recorded and reads back unchanged.
    assert recorded_hashes(home) == V1_MARKERS
    assert stored_v1_generation(home) == before
    assert migrate_core_schema(home, to_version=2, backup_output=None)["migrated"] is False
    unknown = CORE_VERSION + 1
    for target in (1, unknown):
        with pytest.raises(CoreSchemaError, match="core_schema_unknown_version"):
            migrate_core_schema(home, to_version=target, backup_output=None)
    state = sqlite3.connect(home / "state.sqlite3")
    state.execute("INSERT INTO schema_migrations VALUES (?, ?, 0)", (unknown, _HASH))
    state.commit()
    state.close()
    with pytest.raises(ValueError, match="unknown store schema version"):
        open_workspace(home).__enter__()
    state = sqlite3.connect(home / "state.sqlite3")
    state.execute("DELETE FROM schema_migrations WHERE version=?", (unknown,))
    state.execute("UPDATE schema_migrations SET checksum=? WHERE version=2", (_HASH,))
    state.commit()
    state.close()
    with pytest.raises(ValueError, match="checksum mismatch"):
        open_workspace(home).__enter__()


def test_unknown_market_version_is_refused(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    connection = duckdb.connect(str(home / "market.duckdb"))
    unknown = CORE_VERSION + 1
    connection.execute("INSERT INTO schema_migrations VALUES (?, ?, 0)", [unknown, _HASH])
    connection.execute("UPDATE store_info SET schema_version=?", [unknown])
    connection.close()
    with pytest.raises(ValueError, match="store identity/schema mismatch"):
        open_workspace(home).__enter__()


def close_row(**changes: object) -> dict[str, object]:
    row = parse_import(document()).rows[0]
    return {
        **row,
        "source_snapshot_id": "synthetic",
        "source_row_hash": _HASH,
        "basis": "split_adjusted",
        "price_role": "reference",
        "fields": "close",
        "open": None,
        "high": None,
        "low": None,
        "volume": None,
        **changes,
    }


def publish(connection: duckdb.DuckDBPyConnection, row: dict[str, object]) -> dict[str, object]:
    return market.publish_generation(
        connection,
        dataset_id="prices.ref.synthetic",
        version="1",
        generation_id="close-generation",
        operation_id="close-operation",
        request_hash="d" * 64,
        parent_id=None,
        domain="prices",
        rows=[row],
    )


def test_close_only_prices_are_reference(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    # A v1 store has no fields column; the refusal names the migration.
    with (
        open_workspace(home, writable=True) as admitted,
        pytest.raises(ValueError, match=r"aas db migrate --to 2"),
    ):
        publish(admitted.market, close_row())
    migrate_core_schema(home, to_version=2, backup_output=tmp_path / "backup")
    with open_workspace(home, writable=True) as admitted:
        for bad in (
            close_row(price_role="canonical", basis="unadjusted"),
            close_row(open="10"),
            close_row(volume="100"),
            close_row(fields="hlc"),
        ):
            with pytest.raises(ValueError, match=r"close-only|fields must be"):
                publish(admitted.market, bad)
        # The store holds the same rule on its own, beneath the Python path.
        columns = ",".join(f'"{name}"' for name, _ in market.COMMON + market.DOMAINS["prices"])
        values = ",".join("?" for _ in range(len(market.COMMON + market.DOMAINS["prices"]) + 1))
        stored = market.normalize_rows("prices", "synthetic-generation", [close_row()])[0]
        stored = {**stored, "record_id": "x", "revision_id": "x", "price_role": "canonical"}
        names = [name for name, _ in market.COMMON + market.DOMAINS["prices"]]
        with pytest.raises(duckdb.ConstraintException, match="CHECK"):
            admitted.market.execute(
                f'INSERT INTO prices ({columns},"fields") VALUES ({values})',  # noqa: S608 -- test
                [*(stored[name] for name in names), "close"],
            )
        marker = publish(admitted.market, close_row())
        assert verify_generation(admitted.market, "close-generation") == marker
        [read] = read_generation(admitted.market, "close-generation")
        assert (read["fields"], read["close"], read["open"]) == ("close", Decimal(11), None)
        # Changing what a close-only row says it holds breaks its recorded hash.
        admitted.market.execute(
            "UPDATE prices SET \"fields\"='ohlcv' WHERE generation_id='close-generation'"
        )
        with pytest.raises(ValueError, match="hash/count mismatch"):
            verify_generation(admitted.market, "close-generation")
        # An OHLCV row reads back in exactly its v1 shape.
        assert "fields" not in read_generation(admitted.market, "synthetic-generation")[0]


def test_v2_tables_hold_their_own_rules(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as admitted:
        common = "'g','r','v',NULL,'ASSERT',NULL,NULL,0,'s','" + _HASH + "'"
        for sql in (
            f"INSERT INTO filings VALUES ({common},'i','f','10-K','2026-01-02',-1,NULL)",  # noqa: S608
            (
                f"INSERT INTO classifications VALUES ({common},'i','instrument','sic','1','x',"  # noqa: S608
                "'2026-01-02','2026-01-02')"
            ),
            "INSERT INTO quality_flags VALUES ('absent','r','v','krw_tick','1','flag',NULL)",
        ):
            with pytest.raises(duckdb.ConstraintException):
                admitted.market.execute(sql)
        insert = (
            "INSERT INTO source_retirements VALUES "
            "('sl-a',?,1,'replaced','sl-b','{}',?,'backup','op',0)"
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            admitted.state.execute(insert, (_HASH, _HASH))
        admitted.state.rollback()
        prepare_operation(
            admitted.state,
            operation_id="op",
            kind="source-retire",
            request_hash=_HASH,
            target_id="sl-a",
            expected_parent=None,
            payload_hash=_HASH,
        )
        admitted.state.execute(insert, (_HASH, _HASH))
        with pytest.raises(sqlite3.IntegrityError, match="immutable record"):
            admitted.state.execute("DELETE FROM source_retirements")
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            admitted.state.execute(
                "INSERT INTO source_retirements VALUES "
                "('sl-c',?,1,'replaced','sl-c','{}',?,'backup','op',0)",
                (_HASH, _HASH),
            )
        admitted.state.rollback()


def test_v2_domains_are_refused_on_a_v1_store(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    row: dict[str, object] = {
        "issuer_id": "issuer",
        "filing_id": "0000000000-26-000001",
        "form": "10-K",
        "filed_date": "2026-01-02",
        "accepted_at_us": 10,
        "period_end": None,
        "revision_id": "r1",
        "supersedes_revision_id": None,
        "op": "ASSERT",
        "available_at_us": 10,
        "revision_known_at_us": 10,
        "ingested_at_us": 20,
        "source_snapshot_id": "synthetic",
        "source_row_hash": _HASH,
    }
    with (
        open_workspace(home, writable=True) as admitted,
        pytest.raises(ValueError, match="filings domain needs core schema v2"),
    ):
        market.publish_generation(
            admitted.market,
            dataset_id="filings.us.synthetic",
            version="1",
            generation_id="filing-generation",
            operation_id="filing-operation",
            request_hash="e" * 64,
            parent_id=None,
            domain="filings",
            rows=[row],
        )
    migrate_core_schema(home, to_version=2, backup_output=tmp_path / "backup")
    with open_workspace(home, writable=True) as admitted:
        marker = market.publish_generation(
            admitted.market,
            dataset_id="filings.us.synthetic",
            version="1",
            generation_id="filing-generation",
            operation_id="filing-operation",
            request_hash="e" * 64,
            parent_id=None,
            domain="filings",
            rows=[row],
        )
        assert verify_generation(admitted.market, "filing-generation") == marker


def test_migration_refuses_beside_a_prepared_operation(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    with open_workspace(home, writable=True) as admitted:
        prepare_operation(
            admitted.state,
            operation_id="unrelated",
            kind="market_publish",
            request_hash=_HASH,
            target_id="x",
            expected_parent=None,
            payload_hash=_HASH,
        )
    with pytest.raises(CoreSchemaError, match="core_schema_busy"):
        migrate_core_schema(home, to_version=2, backup_output=tmp_path / "backup")
    assert not (tmp_path / "backup").exists()
    assert versions(home) == {"state": 1, "market": 1}


def test_a_completed_step_intent_outlives_later_steps(tmp_path: Path) -> None:
    home = v1_installation(tmp_path)
    migrate_core_schema(home, to_version=2, backup_output=tmp_path / "backup")
    # A later step records its own intent under the same kind; the v2 intent still
    # matches its own step, so the installation reads as current, not invalid.
    with pytest.MonkeyPatch.context() as patch:
        later = add_synthetic_step(patch)
        migrate_core_schema(home, to_version=later, backup_output=tmp_path / "later")
        with open_workspace(home) as admitted:
            status = inspect_core_schema(admitted)
            assert (status.state, status.migration_phase) == ("current", "COMPLETED")
            assert [
                tuple(row)
                for row in admitted.state.execute(
                    "SELECT operation_id, phase FROM storage_operations "
                    "WHERE kind='core-schema-migrate' ORDER BY operation_id"
                )
            ] == [
                (MIGRATION_OPERATION, "COMPLETED"),
                ("core-schema-migrate-v3", "COMPLETED"),
                ("core-schema-migrate-v4", "COMPLETED"),
            ]


def test_plan_writes_nothing_and_names_the_recorded_checksums(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = v1_installation(tmp_path)
    stores = [home / name for name in ("state.sqlite3", "market.duckdb", "installation.json")]
    before = [file_digest(path) for path in stores]
    assert main(["--home", str(home), "db", "migrate", "--to", "2", "--plan"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert [file_digest(path) for path in stores] == before
    assert (plan["plan"], plan["writes"], plan["state"]) == (True, 0, "outdated")
    assert plan["recognized"] == {
        "state": [[1, RECORDED_STATE[0]]],
        "market": [[1, RECORDED_MARKET[0]]],
    }
    assert plan["steps"] == ["backup", "intent", "market", "state", "receipt", "complete"]
    assert plan["backup_required"] is True
    backup = tmp_path / "backup"
    assert (
        main(["--home", str(home), "db", "migrate", "--to", "2", "--backup-output", str(backup)])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["state"] == "outdated"


def _exhausted_market_step(
    monkeypatch: pytest.MonkeyPatch, observed: list[tuple[int, str]]
) -> None:
    """Make the market step's COMMIT fail as the rehearsal's did, recording DuckDB's limits."""

    def exhausted(connection: duckdb.DuckDBPyConnection, installation_id: str, target: int) -> int:
        failing = FailingCommit(connection)
        try:
            return market.upgrade_market(failing.borrowed, installation_id, target)
        finally:
            observed.extend(failing.settings)

    monkeypatch.setattr(migration, "upgrade_market", exhausted)


def test_an_exhausted_market_step_is_a_budget_error_under_the_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = v1_installation(tmp_path)
    before = stored_v1_generation(home)
    budget = ComputeBudget(Fraction(1), 64 * 1024 * 1024)
    observed: list[tuple[int, str]] = []
    with monkeypatch.context() as patch:
        _exhausted_market_step(patch, observed)
        with pytest.raises(ComputeResourceError, match="core schema migration") as caught:
            migrate_core_schema(
                home, to_version=2, backup_output=tmp_path / "backup", budget=budget
            )
    assert str(caught.value.__cause__) == PIN_BLOCK
    # The step ran at the lease's DuckDB share, not at the installation's own limits,
    # although the backup before it reopened the market at those.
    assert observed == [(1, "48.0 MiB")]
    # Nothing of the market step landed; the prepared intent makes the command finish it.
    plan = plan_core_migration(home, to_version=2)
    assert (plan["state"], plan["market_version"], plan["migration_phase"]) == (
        "incomplete",
        1,
        "PREPARED",
    )
    report = migrate_core_schema(home, to_version=2, backup_output=None, budget=budget)
    assert (report["migrated"], report["state"]) == (True, "outdated")
    assert stored_v1_generation(home) == before


def test_the_cli_names_an_exhausted_migration_instead_of_a_database_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    home = v1_installation(tmp_path)
    _exhausted_market_step(monkeypatch, [])
    backup = tmp_path / "backup"
    assert (
        main(["--home", str(home), "db", "migrate", "--to", "2", "--backup-output", str(backup)])
        == 1
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error == "DuckDB cannot complete the core schema migration within admitted memory limits"


def test_a_resumed_market_step_keeps_a_non_capacity_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = v1_installation(tmp_path)
    assert kill_at(home, "_upgrade_market", tmp_path / "backup") == _KILLED
    conflict = (
        "TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint "
        'violation: duplicate key "2"'
    )

    def refused(connection: duckdb.DuckDBPyConnection, installation_id: str, target: int) -> int:
        return market.upgrade_market(
            FailingCommit(connection, conflict).borrowed, installation_id, target
        )

    monkeypatch.setattr(migration, "upgrade_market", refused)
    with pytest.raises(duckdb.TransactionException) as caught:
        migrate_core_schema(home, to_version=2, backup_output=None)
    assert str(caught.value) == conflict
    assert plan_core_migration(home, to_version=2)["market_version"] == 1
