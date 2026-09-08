"""Exercise stage020 contracts on migration-created, disposable PostgreSQL only."""

from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from queue import Queue
from typing import TYPE_CHECKING

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError

from aegis_alpha.metadata.adoption_schema import data_adoptions
from aegis_alpha.metadata.schema import (
    aegis_daily_output,
    alpha_daily_output,
    collection_targets,
    engine_run_events,
    engine_runs,
    metadata,
    risk_daily_output,
)

if TYPE_CHECKING:
    from alembic.config import Config
    from sqlalchemy import Connection, Engine, Table

_NOW = datetime(2026, 9, 5, tzinfo=UTC)
_DATE = date(2026, 9, 5)
_OUTPUTS = (aegis_daily_output, alpha_daily_output, risk_daily_output)
_PREVIOUS = "20260829_0010"
_HEAD = "20260905_0011"


def _run(**overrides: object) -> dict[str, object]:
    return {
        "id": "synthetic-run",
        "engine": "aegis",
        "mode": "replay",
        "run_ts": _NOW,
        "git_sha": "a" * 40,
        "config_sha256": "a" * 64,
        "inputs_parquet_sha256": "b" * 64,
        "dataset_digest": "c" * 64,
        "feature_contract_version": None,
        "outputs_sha256": "d" * 64,
        "status": "succeeded",
        **overrides,
    }


def _receipt(**overrides: object) -> dict[str, object]:
    return {
        "adoption_id": "synthetic-adoption",
        "source_manifest_sha256": "e" * 64,
        "source_system_identifier": "123456789",
        "source_database": "synthetic_source",
        "source_alembic_head": "20260818_0004",
        "adopted_at_utc": _NOW,
        "table_count": 19,
        "row_count": 0,
        "file_count": 0,
        **overrides,
    }


@pytest.fixture
def runtime_connection(clean_postgres: Engine) -> Iterator[Connection]:
    # Roll back immutable rows instead of changing the shared fixture's deletion contract.
    with clean_postgres.connect() as connection, connection.begin():
        try:
            yield connection
        finally:
            connection.rollback()


def _output(connection: Connection, table: Table, **overrides: object) -> None:
    connection.execute(
        table.insert().values(
            {"as_of": _DATE, "run_id": "synthetic-run", "payload": {}, **overrides}
        )
    )


def test_model_matches_physical_receipt_and_output_keys(runtime_connection: Connection) -> None:
    assert data_adoptions.metadata is metadata
    inspector = inspect(runtime_connection)
    for table in (*_OUTPUTS, data_adoptions):
        expected = ["adoption_id"] if table is data_adoptions else ["as_of", "run_id"]
        assert list(table.primary_key.columns.keys()) == expected
        assert (
            inspector.get_pk_constraint(table.name, schema=table.schema)["constrained_columns"]
            == expected
        )
    physical = inspector.get_columns("data_adoptions", schema="engine")
    assert [column["name"] for column in physical] == list(_receipt())
    assert all(not column["nullable"] for column in physical)
    assert inspector.get_unique_constraints("data_adoptions", schema="engine")[0][
        "column_names"
    ] == ["source_manifest_sha256"]


@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
def test_matching_successful_output_and_composite_pk(
    runtime_connection: Connection, table: Table
) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(engine=table.schema)))
    _output(runtime_connection, table)
    _output(runtime_connection, table, as_of=date(2026, 9, 6))
    runtime_connection.execute(engine_runs.insert().values(_run(id="second", engine=table.schema)))
    _output(runtime_connection, table, run_id="second")
    with pytest.raises(IntegrityError, match="duplicate key"), runtime_connection.begin_nested():
        _output(runtime_connection, table)
    assert set(runtime_connection.execute(select(table.c.as_of, table.c.run_id))) == {
        (_DATE, "synthetic-run"),
        (date(2026, 9, 6), "synthetic-run"),
        (_DATE, "second"),
    }


@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
@pytest.mark.parametrize("status", ["running", "failed"])
def test_output_requires_success(runtime_connection: Connection, table: Table, status: str) -> None:
    runtime_connection.execute(
        engine_runs.insert().values(_run(engine=table.schema, status=status))
    )
    with (
        pytest.raises(IntegrityError, match="requires a succeeded"),
        runtime_connection.begin_nested(),
    ):
        _output(runtime_connection, table)


@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
@pytest.mark.parametrize("parent", ["aegis", "alpha", "risk", "data"])
def test_output_module_binding(runtime_connection: Connection, table: Table, parent: str) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(engine=parent)))
    if parent == table.schema:
        _output(runtime_connection, table)
    else:
        with (
            pytest.raises(IntegrityError, match="module does not match"),
            runtime_connection.begin_nested(),
        ):
            _output(runtime_connection, table)


@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
@pytest.mark.parametrize("payload", [None, [], "text", 1, True])
def test_output_payload_must_be_object(
    runtime_connection: Connection, table: Table, payload: object
) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(engine=table.schema)))
    with pytest.raises(IntegrityError, match="JSON object"), runtime_connection.begin_nested():
        _output(runtime_connection, table, payload=payload)


@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
def test_output_cannot_reference_missing_run(runtime_connection: Connection, table: Table) -> None:
    with (
        pytest.raises(IntegrityError, match="existing engine run"),
        runtime_connection.begin_nested(),
    ):
        _output(runtime_connection, table)


def test_success_requires_digest_on_insert_and_transition(runtime_connection: Connection) -> None:
    with (
        pytest.raises(IntegrityError, match="requires output digest"),
        runtime_connection.begin_nested(),
    ):
        runtime_connection.execute(engine_runs.insert().values(_run(outputs_sha256=None)))
    runtime_connection.execute(
        engine_runs.insert().values(_run(status="running", outputs_sha256=None))
    )
    with (
        pytest.raises(IntegrityError, match="requires output digest"),
        runtime_connection.begin_nested(),
    ):
        runtime_connection.execute(engine_runs.update().values(status="succeeded"))
    runtime_connection.execute(
        engine_runs.update().values(status="succeeded", outputs_sha256="f" * 64)
    )
    _output(runtime_connection, aegis_daily_output)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "different"),
        ("engine", "risk"),
        ("mode", "paper"),
        ("run_ts", datetime(2026, 9, 6, tzinfo=UTC)),
        ("git_sha", "b" * 40),
        ("config_sha256", "f" * 64),
        ("inputs_parquet_sha256", "f" * 64),
        ("dataset_digest", "f" * 64),
        ("feature_contract_version", "v1"),
    ],
)
def test_running_identity_is_immutable(
    runtime_connection: Connection, field: str, value: object
) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(status="running")))
    with (
        pytest.raises(IntegrityError, match="identity is immutable"),
        runtime_connection.begin_nested(),
    ):
        runtime_connection.execute(engine_runs.update().values({field: value}))


@pytest.mark.parametrize("status", ["succeeded", "failed"])
@pytest.mark.parametrize(
    "change",
    [
        {"status": "running"},
        {"status": "failed"},
        {"outputs_sha256": "f" * 64},
        {"git_sha": "f" * 40},
        None,
    ],
)
def test_terminal_run_is_immutable(
    runtime_connection: Connection, status: str, change: dict[str, object] | None
) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(status=status)))
    statement = engine_runs.delete() if change is None else engine_runs.update().values(change)
    with (
        pytest.raises(IntegrityError, match="terminal engine run is immutable"),
        runtime_connection.begin_nested(),
    ):
        runtime_connection.execute(statement)


@pytest.mark.parametrize(
    "table", [*_OUTPUTS, engine_run_events, data_adoptions], ids=lambda table: table.fullname
)
@pytest.mark.parametrize("delete", [False, True])
def test_receipts_events_outputs_are_immutable(
    runtime_connection: Connection, table: Table, *, delete: bool
) -> None:
    if table is data_adoptions:
        runtime_connection.execute(table.insert().values(_receipt()))
        change = {"row_count": 1}
    else:
        runtime_connection.execute(
            engine_runs.insert().values(_run(engine=table.schema if table in _OUTPUTS else "aegis"))
        )
        if table is engine_run_events:
            runtime_connection.execute(
                table.insert().values(
                    id="event", run_id="synthetic-run", event_type="done", payload={}
                )
            )
        else:
            _output(runtime_connection, table)
        change = {"payload": {"rewritten": True}}
    statement = table.delete() if delete else table.update().values(change)
    with pytest.raises(IntegrityError, match="is immutable"), runtime_connection.begin_nested():
        runtime_connection.execute(statement)


@pytest.mark.parametrize("field", list(_receipt()))
def test_receipt_requires_every_field(runtime_connection: Connection, field: str) -> None:
    with pytest.raises(IntegrityError), runtime_connection.begin_nested():
        runtime_connection.execute(data_adoptions.insert().values(_receipt(**{field: None})))


@pytest.mark.parametrize(
    "change",
    [
        {"adoption_id": " "},
        {"source_database": " "},
        {"source_system_identifier": " "},
        {"source_alembic_head": " "},
        {"source_manifest_sha256": "A" * 64},
        {"source_manifest_sha256": "a" * 63},
        {"source_manifest_sha256": "g" * 64},
        {"table_count": 0},
        {"table_count": -1},
        {"row_count": -1},
        {"file_count": -1},
    ],
)
def test_receipt_rejects_invalid_values(
    runtime_connection: Connection, change: dict[str, object]
) -> None:
    with pytest.raises(IntegrityError, match="check constraint"), runtime_connection.begin_nested():
        runtime_connection.execute(data_adoptions.insert().values(_receipt(**change)))


@pytest.mark.parametrize(
    "change", [{"adoption_id": "second"}, {"source_manifest_sha256": "f" * 64}]
)
def test_receipt_identity_and_manifest_are_unique(
    runtime_connection: Connection, change: dict[str, object]
) -> None:
    runtime_connection.execute(data_adoptions.insert().values(_receipt()))
    with pytest.raises(IntegrityError, match="duplicate key"), runtime_connection.begin_nested():
        runtime_connection.execute(data_adoptions.insert().values(_receipt(**change)))


def test_migration_roundtrip_and_model_drift(
    clean_postgres: Engine, alembic_config: Config
) -> None:
    current_head = ScriptDirectory.from_config(alembic_config).get_current_head()
    command.downgrade(alembic_config, "base")
    command.upgrade(alembic_config, "head")
    command.check(alembic_config)
    command.downgrade(alembic_config, _PREVIOUS)
    with clean_postgres.connect() as connection:
        assert connection.scalar(text("SELECT to_regclass('engine.data_adoptions')")) is None
        for function in ("guard_runtime_run", "guard_runtime_output", "reject_runtime_mutation"):
            assert (
                connection.scalar(
                    text("SELECT to_regprocedure(:function)"), {"function": f"engine.{function}()"}
                )
                is None
            )
    command.upgrade(alembic_config, "head")
    with clean_postgres.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == current_head


def test_engine_hardening_preserves_existing_data(
    clean_postgres: Engine, alembic_config: Config
) -> None:
    command.downgrade(alembic_config, _PREVIOUS)
    values = {
        "domain": "synthetic",
        "symbol": "EXAMPLE",
        "valid_from": _DATE,
        "valid_until": None,
        "reason": "preservation fixture",
    }
    with clean_postgres.begin() as connection:
        connection.execute(collection_targets.insert().values(values))
    command.upgrade(alembic_config, "head")
    command.downgrade(alembic_config, _PREVIOUS)
    command.upgrade(alembic_config, "head")
    with clean_postgres.connect() as connection:
        assert dict(connection.execute(select(collection_targets)).mappings().one()) == values


def test_guard_removal_exposes_unsafe_output(runtime_connection: Connection) -> None:
    """An ablation proves the negative cases reach the new admission trigger."""
    runtime_connection.execute(engine_runs.insert().values(_run(status="running")))
    with (
        pytest.raises(IntegrityError, match="requires a succeeded"),
        runtime_connection.begin_nested(),
    ):
        _output(runtime_connection, aegis_daily_output)
    # DDL and unsafe row both roll back with this owned synthetic transaction.
    runtime_connection.execute(
        text("ALTER TABLE aegis.daily_output DISABLE TRIGGER guard_runtime_output")
    )
    _output(runtime_connection, aegis_daily_output)
    assert runtime_connection.scalar(select(aegis_daily_output.c.run_id)) == "synthetic-run"


def test_event_cannot_be_deleted_by_running_parent_cascade(runtime_connection: Connection) -> None:
    runtime_connection.execute(engine_runs.insert().values(_run(status="running")))
    runtime_connection.execute(
        engine_run_events.insert().values(
            id="event", run_id="synthetic-run", event_type="started", payload={}
        )
    )
    with pytest.raises(IntegrityError, match="is immutable"), runtime_connection.begin_nested():
        runtime_connection.execute(engine_runs.delete())
    assert runtime_connection.scalar(select(engine_run_events.c.id)) == "event"


def test_upgrade_refuses_existing_engine_data_without_conversion(
    clean_postgres: Engine, alembic_config: Config
) -> None:
    command.downgrade(alembic_config, _PREVIOUS)
    with clean_postgres.begin() as connection:
        connection.execute(engine_runs.insert().values(_run(outputs_sha256=None)))
    try:
        with pytest.raises(RuntimeError, match=r"refusing runtime engine upgrade.*contains rows"):
            command.upgrade(alembic_config, _HEAD)
        with clean_postgres.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == _PREVIOUS
            assert dict(connection.execute(select(engine_runs)).mappings().one()) == _run(
                outputs_sha256=None
            )
            assert connection.scalar(text("SELECT to_regclass('engine.data_adoptions')")) is None
    finally:
        with clean_postgres.begin() as connection:
            connection.execute(engine_runs.delete())
        command.upgrade(alembic_config, "head")


@pytest.mark.parametrize("table", [data_adoptions, engine_runs], ids=lambda table: table.name)
def test_downgrade_refuses_protected_rows(
    clean_postgres: Engine, alembic_config: Config, table: Table
) -> None:
    current_head = ScriptDirectory.from_config(alembic_config).get_current_head()
    with clean_postgres.begin() as connection:
        connection.execute(table.insert().values(_receipt() if table is data_adoptions else _run()))
    try:
        with pytest.raises(
            RuntimeError, match=r"refusing destructive runtime downgrade.*contains rows"
        ):
            command.downgrade(alembic_config, _PREVIOUS)
        with clean_postgres.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM alembic_version")) == current_head
            )
            assert connection.execute(select(table)).one()
    finally:
        # Administrative cleanup only in clean_postgres's owned synthetic database.
        with clean_postgres.begin() as connection:
            connection.execute(text(f"TRUNCATE {table.fullname} CASCADE"))


def _blocked_by(engine: Engine, follower: int, holder: int) -> bool:
    deadline = time.monotonic() + 5
    with engine.connect() as connection:
        while time.monotonic() < deadline:
            blockers = connection.scalar(text("SELECT pg_blocking_pids(:pid)"), {"pid": follower})
            if holder in blockers:
                return True
            time.sleep(0.01)
    return False


@pytest.mark.parametrize("direction", ["output_first", "failed_first", "success_first"])
@pytest.mark.parametrize("table", _OUTPUTS, ids=lambda table: table.schema)
def test_output_state_races_share_parent_lock(
    clean_postgres: Engine, direction: str, table: Table
) -> None:
    with clean_postgres.begin() as seed:
        seed.execute(
            engine_runs.insert().values(
                _run(
                    engine=table.schema,
                    status="succeeded" if direction == "output_first" else "running",
                )
            )
        )
    pids: Queue[int] = Queue()

    def follower() -> str:
        try:
            with clean_postgres.begin() as connection:
                connection.execute(text("SET LOCAL statement_timeout = '8s'"))
                pids.put(connection.scalar(text("SELECT pg_backend_pid()")))
                if direction == "output_first":
                    connection.execute(engine_runs.update().values(status="failed"))
                else:
                    _output(connection, table)
        except IntegrityError as error:
            return str(error.orig)
        else:
            return "committed"

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with clean_postgres.begin() as holder:
                holder_pid = holder.scalar(text("SELECT pg_backend_pid()"))
                if direction == "output_first":
                    _output(holder, table)
                else:
                    holder.execute(
                        engine_runs.update().values(
                            status="failed" if direction == "failed_first" else "succeeded"
                        )
                    )
                future = executor.submit(follower)
                assert _blocked_by(clean_postgres, pids.get(timeout=5), holder_pid), (
                    "follower must actually block on the parent row"
                )
            outcome = future.result(timeout=10)
        expected = {
            "output_first": "terminal engine run is immutable",
            "failed_first": "requires a succeeded engine run",
            "success_first": "committed",
        }[direction]
        assert expected in outcome
        with clean_postgres.connect() as connection:
            assert connection.scalar(select(engine_runs.c.status)) == (
                "failed" if direction == "failed_first" else "succeeded"
            )
            rows = connection.execute(select(table.c.run_id, table.c.payload)).all()
            assert rows == ([] if direction == "failed_first" else [("synthetic-run", {})])
    finally:
        with clean_postgres.begin() as connection:
            connection.execute(text("TRUNCATE engine.engine_runs CASCADE"))
