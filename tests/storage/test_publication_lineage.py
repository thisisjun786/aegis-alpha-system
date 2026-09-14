"""Sealed content and explicit native admission are different integrity purposes."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tracemalloc
from collections.abc import Iterator
from contextlib import closing, contextmanager
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, cast

import duckdb
import pyarrow as pa
import pytest

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage import market, publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.research_inputs import register_price_input
from aegis_alpha.storage.source_library import import_arrow, list_tables
from aegis_alpha.storage.source_library_schema import quoted
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.test_market_inputs import BUDGET, DAY, api, pin, prices, request
from tests.storage.test_publication import document
from tests.storage.test_research_inputs import (
    _change,
    _hash_json,
    _register_domain,
    _sessions_spec,
    _source_row,
)
from tests.storage.test_verification import corrupt_closed_store, rehash_backup_inventory

if TYPE_CHECKING:
    from aegis_alpha.storage.market_inputs import GenerationPin
    from aegis_alpha.storage.workspace import Workspace


@contextmanager
def select_only(connection: sqlite3.Connection) -> Iterator[None]:
    def authorize(action: int, *_args: str | None) -> int:
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

    changes = connection.total_changes
    connection.set_authorizer(authorize)
    try:
        yield
    finally:
        connection.set_authorizer(None)
    assert connection.total_changes == changes


def seed_native(workspace: Workspace, root: Path) -> None:
    prices(workspace, root, [_source_row()], "1")
    path = _sessions_spec(
        workspace,
        root / "sessions.sqlite3",
        {"session_date": DAY.isoformat(), "available_at_us": 20, "revision_known_at_us": 20},
    )
    _register_domain(workspace, path, "sessions")


@pytest.fixture
def stored(tmp_path: Path) -> Iterator[Workspace]:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        yield workspace


def revise(workspace: Workspace, root: Path) -> None:
    prices(
        workspace,
        root,
        [
            {
                **_source_row(),
                "revision_id": "r2",
                "op": "SUPERSEDE",
                "supersedes_revision_id": "r1",
                "close": "11.125",
                "available_at_us": 40,
                "revision_known_at_us": 40,
                "ingested_at_us": 50,
            }
        ],
        "2",
    )
    path = _sessions_spec(
        workspace,
        root / "sessions2.sqlite3",
        {
            "session_date": DAY.isoformat(),
            "revision_id": "s2",
            "op": "SUPERSEDE",
            "supersedes_revision_id": "r1",
            "available_at_us": 40,
            "revision_known_at_us": 40,
            "ingested_at_us": 50,
        },
    )
    _change(
        path,
        "dataset",
        {
            "dataset_id": "sessions",
            "version": "2",
            "generation_id": "sessions2",
            "operation_id": "op-sessions2",
            "parent_id": "sessions",
        },
    )
    _register_domain(workspace, path, "sessions")


def transform_path(workspace: Workspace, selected: GenerationPin) -> Path:
    digest = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?",
        (selected.generation_id,),
    ).fetchone()[0]
    return workspace.paths.raw / digest[:2] / digest


def replace_transform(workspace: Workspace, dataset: str, replacement: str) -> None:
    before = list(workspace.state.execute("SELECT sql FROM sqlite_master ORDER BY name"))
    trigger = workspace.state.execute(
        "SELECT sql FROM sqlite_master WHERE name='immutable_dataset_versions_update'"
    ).fetchone()[0]
    with workspace.state:
        workspace.state.execute("DROP TRIGGER immutable_dataset_versions_update")
        workspace.state.execute(
            "UPDATE dataset_versions SET transform_hash=? WHERE dataset_id=?",
            (replacement, dataset),
        )
        workspace.state.execute(trigger)
    assert list(workspace.state.execute("SELECT sql FROM sqlite_master ORDER BY name")) == before


@pytest.mark.parametrize("kind", ["price", "sessions"])
@pytest.mark.parametrize("boundary", ["reader", "integrity"])
def test_same_pin_catalog_substitution_rejected(
    stored: Workspace, kind: str, boundary: str
) -> None:
    dataset = "synthetic-prices" if kind == "price" else "sessions"
    selected = pin(stored, dataset)
    req = request(stored)
    if kind == "price":
        body = json.loads(transform_path(stored, selected).read_bytes())
        body["source"]["source_id"] = "NONEXISTENT-SOURCE"
        _, replacement, _ = put_raw(stored.paths.raw, json.dumps(body).encode())
    else:
        replacement = "f" * 64
        assert not (stored.paths.raw / replacement[:2] / replacement).exists()
    replace_transform(stored, dataset, replacement)
    assert pin(stored, dataset) == selected
    if boundary == "integrity":
        with pytest.raises(ValueError, match=r"publication|transform"):
            verify_workspace(stored)
    elif kind == "price":
        with pytest.raises(ValueError, match=r"publication|transform"):
            api().load_pinned_prices(stored, req, budget=BUDGET)
    else:
        with pytest.raises(ValueError, match=r"publication|transform"):
            api().load_pinned_sessions(stored, selected, budget=BUDGET)


@pytest.mark.parametrize("kind", ["price", "sessions"])
@pytest.mark.parametrize("revision", ["1", "2"])
@pytest.mark.parametrize(
    "fault", ["missing-transform", "corrupt-transform", "source-row", "source-marker"]
)
def test_native_admission_requires_every_revision_evidence(
    stored: Workspace, tmp_path: Path, kind: str, revision: str, fault: str
) -> None:
    revise(stored, tmp_path)
    dataset = "synthetic-prices" if kind == "price" else "sessions"
    selected = pin(stored, dataset, "2")
    affected = pin(stored, dataset, revision)
    schema = "aas-" + kind + "-transform-v1"
    expected = api().admit_native_input(stored, selected, expected_schema=schema, budget=BUDGET)
    assert [source.source_id for source in expected.source_pins] == (
        ["source1", "source2"] if kind == "price" else ["sessions", "sessions2"]
    )
    path = transform_path(stored, affected)
    body = json.loads(path.read_bytes())
    assert stored.strategies is not None
    if fault == "missing-transform":
        path.unlink()
    elif fault == "corrupt-transform":
        path.write_bytes(b"{}")
    elif fault == "source-row":
        table = list_tables(stored, body["source"]["source_id"])[0]
        stored.strategies.execute(
            " ".join(("UPDATE", quoted(str(table["target"])), "SET source_row_hash=?")),
            ("d" * 64,),
        )
        stored.strategies.commit()
    else:
        stored.strategies.execute(
            "DELETE FROM source_library_commits WHERE source_id=?", (body["source"]["source_id"],)
        )
        stored.strategies.commit()
    # Sealed content is unchanged: ordinary v1 never recorded a native-retention duty.
    assert (
        api().verify_sealed_publication(stored, selected.generation_id, budget=BUDGET)
        == expected.history
    )
    with pytest.raises((ValueError, FileNotFoundError)):
        api().admit_native_input(stored, selected, expected_schema=schema, budget=BUDGET)


@pytest.mark.parametrize("kind", ["price", "sessions"])
def test_expected_schema_is_explicit_and_never_discovered(stored: Workspace, kind: str) -> None:
    selected = pin(stored, "synthetic-prices" if kind == "price" else "sessions")
    wrong = "aas-sessions-transform-v1" if kind == "price" else "aas-price-transform-v1"
    with pytest.raises(ValueError, match="domain"):
        api().admit_native_input(stored, selected, expected_schema=wrong, budget=BUDGET)
    with pytest.raises(ValueError, match="schema"):
        api().admit_native_input(stored, selected, expected_schema="unknown", budget=BUDGET)


def test_native_source_budget_precedes_cell_materialization(stored: Workspace) -> None:
    selected = pin(stored)
    body = json.loads(transform_path(stored, selected).read_bytes())
    table = list_tables(stored, body["source"]["source_id"])[0]
    assert stored.strategies is not None
    stored.strategies.execute(
        " ".join(("UPDATE", quoted(str(table["target"])), "SET source_row_hash=?")),
        ("x" * 1024 * 1024,),
    )
    stored.strategies.commit()
    # A content reader/digest would report a digest mismatch; capacity must fail first.
    with pytest.raises(ComputeResourceError, match="source table"):
        api().admit_native_input(
            stored, selected, expected_schema="aas-price-transform-v1", budget=BUDGET
        )


METADATA_BUDGET = ComputeBudget(Fraction(1), 8 * 1024 * 1024)
OVERSIZED_METADATA_BYTES = 16 * 1024 * 1024


def seed_metadata_budget(workspace: Workspace, root: Path, store: str) -> str:
    seed_native(workspace, root)
    if store == "strategies":
        return "synthetic-prices"
    row = {**_source_row(), "revision_id": "arrow-r1"}
    arrow = pa.Table.from_pylist(
        [row],
        schema=pa.schema(
            [(key, pa.int64() if type(value) is int else pa.string()) for key, value in row.items()]
        ),
    )
    digest = _hash_json(row)
    imported = import_arrow(workspace, "arrow", digest, "bars", arrow.to_reader())
    table = cast("list[dict[str, object]]", imported["tables"])[0]
    body = json.loads(transform_path(workspace, pin(workspace)).read_bytes())
    body["source"] = {
        "source_id": "arrow",
        "source_sha256": digest,
        "table": "bars",
        "table_digest": table["digest"],
    }
    body["dataset"].update(
        dataset_id="arrow-prices", generation_id="arrow-prices", operation_id="arrow-prices"
    )
    raw = json.dumps(body).encode()
    path = root / "arrow.json"
    path.write_bytes(raw)
    register_price_input(workspace, path, hashlib.sha256(raw).hexdigest())
    return "arrow-prices"


@contextmanager
def metadata_allocation_bound(
    workspace: Workspace, max_bytes: int = METADATA_BUDGET.memory_limit_bytes
) -> Iterator[None]:
    assert workspace.strategies is not None
    with select_only(workspace.state), select_only(workspace.strategies):
        tracemalloc.start()
        try:
            yield
        finally:
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        assert peak < max_bytes


@pytest.mark.parametrize("store", ["strategies", "market"])
@pytest.mark.parametrize("selected_field", [False, True])
@pytest.mark.parametrize(
    "field",
    ["source_id", "operation_id", "request_hash", "source_sha256", "store_kind", "manifest_json"],
)
def test_source_marker_metadata_budget(
    tmp_path: Path, store: str, field: str, *, selected_field: bool
) -> None:
    home = tmp_path / "home"
    # Arrow's real column scan needs more DuckDB working memory than SQLite's.
    budget = ComputeBudget(Fraction(1), 32 * 1024 * 1024) if store == "market" else METADATA_BUDGET
    size = 4 * 1024 * 1024 if store == "market" else OVERSIZED_METADATA_BYTES
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        dataset = seed_metadata_budget(workspace, tmp_path, store)
        selected = pin(workspace, dataset if selected_field else "synthetic-prices")
        affected = "arrow" if store == "market" else ("source1" if selected_field else "sessions")
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace, size // 2):
        expected = api().admit_native_input(
            workspace, selected, expected_schema="aas-price-transform-v1", budget=budget
        )
        assert expected.source_pins[0].source_id == (affected if selected_field else "source1")
    # Whitespace keeps JSON valid; NUL + multibyte text exercises byte, not text length.
    value = " " * size if field == "manifest_json" else "\x00" + "\u00e9" * (size // 2)
    assignment = "=? || manifest_json" if field == "manifest_json" else "=?"
    mutation = " ".join(
        ("UPDATE source_library_commits SET", quoted(field), assignment, "WHERE source_id=?")
    )
    if store == "market":
        with duckdb.connect(str(home / "market.duckdb")) as connection:
            before = connection.execute(
                "SELECT sql FROM duckdb_tables() ORDER BY table_name"
            ).fetchall()
            connection.execute(mutation, [value, affected])
            assert (
                connection.execute("SELECT sql FROM duckdb_tables() ORDER BY table_name").fetchall()
                == before
            )
    else:
        corrupt_closed_store(home, store, "source_library_commits", mutation, (value, affected))
    del value
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace, size // 2):
        # Universal content and the exact selected publication pin remain intact.
        assert (
            api().verify_sealed_publication(workspace, selected.generation_id, budget=budget)
            == expected.history
        )
        with pytest.raises(ComputeResourceError, match=r"source .*materialization budget"):
            api().admit_native_input(
                workspace, selected, expected_schema="aas-price-transform-v1", budget=budget
            )


def test_null_source_identity_cannot_hide_metadata_budget(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        selected = pin(workspace)
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace):
        assert api().admit_native_input(
            workspace, selected, expected_schema="aas-price-transform-v1", budget=METADATA_BUDGET
        )
    # SQLite's VARCHAR PRIMARY KEY permits NULL. It must not null out the
    # row's entire size expression and hide other materialized fields from SUM.
    corrupt_closed_store(
        home,
        "strategies",
        "source_library_commits",
        "UPDATE source_library_commits SET source_id=NULL,operation_id=? "
        "WHERE source_id='sessions'",
        ("X" * OVERSIZED_METADATA_BYTES,),
    )
    with (
        open_workspace(home) as workspace,
        metadata_allocation_bound(workspace),
        pytest.raises(ComputeResourceError, match=r"source .*materialization budget"),
    ):
        api().admit_native_input(
            workspace,
            selected,
            expected_schema="aas-price-transform-v1",
            budget=METADATA_BUDGET,
        )


@pytest.mark.parametrize("selected_field", [False, True])
@pytest.mark.parametrize(
    "field",
    ["kind", "request_hash", "target_id", "expected_parent", "payload_hash", "failure_reason"],
)
def test_source_operation_metadata_budget(
    tmp_path: Path, field: str, *, selected_field: bool
) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        selected = pin(workspace)
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace):
        expected = api().admit_native_input(
            workspace, selected, expected_schema="aas-price-transform-v1", budget=METADATA_BUDGET
        )
    # SQLite length(TEXT) stops at NUL: retain the real 64-character hash CHECK.
    value = "a" * 64 + "\x00" + "X" * OVERSIZED_METADATA_BYTES
    corrupt_closed_store(
        home,
        "state",
        "storage_operations",
        " ".join(
            (
                "UPDATE storage_operations SET",
                quoted(field),
                "=? WHERE kind='source_import' AND target_id=?",
            )
        ),
        (value, "source1" if selected_field else "sessions"),
    )
    del value
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace):
        assert (
            api().verify_sealed_publication(
                workspace, selected.generation_id, budget=METADATA_BUDGET
            )
            == expected.history
        )
        with pytest.raises(ComputeResourceError, match=r"source .*materialization budget"):
            api().admit_native_input(
                workspace,
                selected,
                expected_schema="aas-price-transform-v1",
                budget=METADATA_BUDGET,
            )


@pytest.mark.parametrize("target", ["g1", "sessions"])
def test_unfetched_native_operation_metadata_stays_bounded(tmp_path: Path, target: str) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        selected = pin(workspace)
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace):
        expected = api().admit_native_input(
            workspace, selected, expected_schema="aas-price-transform-v1", budget=METADATA_BUDGET
        )
    corrupt_closed_store(
        home,
        "state",
        "storage_operations",
        "UPDATE storage_operations SET failure_reason=? "
        "WHERE kind='market_publish' AND target_id=?",
        ("X" * OVERSIZED_METADATA_BYTES, target),
    )
    with open_workspace(home) as workspace, metadata_allocation_bound(workspace):
        assert (
            api().admit_native_input(
                workspace,
                selected,
                expected_schema="aas-price-transform-v1",
                budget=METADATA_BUDGET,
            )
            == expected
        )


def test_oversized_publication_is_bounded_before_decode(stored: Workspace) -> None:
    selected = pin(stored)
    path = stored.paths.raw / selected.manifest_hash[:2] / selected.manifest_hash
    path.write_bytes(b" " * (BUDGET.memory_limit_bytes // 32))
    with pytest.raises(ValueError, match=r"size|bound|large"):
        api().verify_sealed_publication(stored, selected.generation_id, budget=BUDGET)


def test_true_old_native_and_opaque_generic_pins_round_trip(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        old = request(workspace)
        old_prices = api().load_pinned_prices(workspace, old, budget=BUDGET)
        revise(workspace, tmp_path)
        newer = request(
            workspace, pin=pin(workspace, version="2"), sessions_pin=pin(workspace, "sessions", "2")
        )
        revised = api().load_pinned_prices(workspace, newer, budget=BUDGET)
        generic = []
        for selected in (old.pin, old.sessions_pin):
            assert selected is not None
            raw = workspace.paths.raw / selected.manifest_hash[:2] / selected.manifest_hash
            body = json.loads(raw.read_bytes())
            identity = "generic-" + selected.dataset_id
            body.update(
                dataset_id=identity,
                generation_id=identity,
                operation_id=identity,
                transform_sha256="e" * 64,
            )
            body["rows"][0]["revision_id"] = identity
            publication.publish_document(workspace, parse_import(json.dumps(body).encode()))
            generic.append(pin(workspace, identity))
        assert not (workspace.paths.raw / "ee" / ("e" * 64)).exists()
    for path in (*tmp_path.glob("*.sqlite3"), *tmp_path.glob("*.json")):
        path.unlink()
    archive, target = tmp_path / "backup", tmp_path / "restored"
    backup(home, archive)
    restore(archive, target)
    for root in (home, target):
        with open_workspace(root) as workspace:
            assert workspace.strategies is not None
            before = "\n".join(workspace.state.iterdump())
            with select_only(workspace.state), select_only(workspace.strategies):
                assert api().load_pinned_prices(workspace, old, budget=BUDGET) == old_prices
                assert api().load_pinned_prices(workspace, newer, budget=BUDGET) == revised
                for selected, schema in zip(generic, ("price", "sessions"), strict=True):
                    assert api().verify_sealed_publication(
                        workspace, selected.generation_id, budget=BUDGET
                    )
                    with pytest.raises((ValueError, FileNotFoundError)):
                        api().admit_native_input(
                            workspace,
                            selected,
                            expected_schema="aas-" + schema + "-transform-v1",
                            budget=BUDGET,
                        )
                assert api().admit_native_input(
                    workspace,
                    newer.sessions_pin,
                    expected_schema="aas-sessions-transform-v1",
                    budget=BUDGET,
                )
                assert verify_workspace(workspace)["verified"]
            assert "\n".join(workspace.state.iterdump()) == before
    assert old_prices.project_as_of(40, session_date=DAY).rows[0]["close"] == Decimal(11)
    assert revised.project_as_of(30, session_date=DAY).rows[0]["close"] == Decimal(11)
    assert revised.project_as_of(40, session_date=DAY).rows[0]["close"] == Decimal("11.125")


def rehash_rows(home: Path, kind: str, revision: str) -> None:
    """Rehash physical rows AND every descendant marker/catalog; retain sealed imports."""
    domain = "prices" if kind == "price" else "calendar_sessions"
    head = "g2" if kind == "price" else "sessions2"
    affected = (
        "g" + revision if kind == "price" else ("sessions" if revision == "1" else "sessions2")
    )
    with duckdb.connect(str(home / "market.duckdb")) as connection, duckdb.connect() as rebuilt:
        market.initialize_market(rebuilt, "synthetic-rehash")
        before = connection.execute(
            "SELECT sql FROM duckdb_tables() ORDER BY table_name"
        ).fetchall()
        history = [dict(row) for row in market.read_chain_rows(connection, head, budget=BUDGET)]
        for row in history:
            if row["generation_id"] == affected:
                row["source_row_hash"] = "d" * 64
        connection.execute(
            "UPDATE prices SET source_row_hash=? WHERE generation_id=?"
            if kind == "price"
            else "UPDATE calendar_sessions SET source_row_hash=? WHERE generation_id=?",
            ["d" * 64, affected],
        )
        parent_hash = None
        hashes = []
        for marker in market.generation_chain(connection, head):
            delta = [row for row in history if row["generation_id"] == marker["generation_id"]]
            forged = market.publish_generation(
                rebuilt,
                dataset_id=str(marker["dataset_id"]),
                version=str(marker["version"]),
                generation_id=str(marker["generation_id"]),
                operation_id=str(marker["operation_id"]),
                request_hash=str(marker["request_hash"]),
                parent_id=cast("str | None", marker["parent_id"]),
                domain=domain,
                rows=delta,
            )
            digest, chain = forged["delta_hash"], forged["chain_hash"]
            connection.execute(
                "UPDATE market_generations SET delta_hash=?,chain_hash=? WHERE generation_id=?",
                [digest, chain, marker["generation_id"]],
            )
            hashes.append((chain, marker["generation_id"]))
            parent_hash = chain
        assert market.verify_generation(connection, head)["chain_hash"] == parent_hash
        assert (
            connection.execute("SELECT sql FROM duckdb_tables() ORDER BY table_name").fetchall()
            == before
        )
    for values in hashes:
        corrupt_closed_store(
            home,
            "state",
            "dataset_versions",
            "UPDATE dataset_versions SET chain_hash=? WHERE generation_id=?",
            values,
        )


@pytest.mark.parametrize("kind", ["price", "sessions"])
@pytest.mark.parametrize("revision", ["1", "2"])
def test_rehashed_ancestor_and_head_attacks_reject_integrity_backup_restore(
    tmp_path: Path, kind: str, revision: str
) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        revise(workspace, tmp_path)
    archive = tmp_path / "backup"
    backup(home, archive)
    for root in (home, archive):
        rehash_rows(root, kind, revision)
        with open_workspace(root) as workspace:
            selected = pin(workspace, "synthetic-prices" if kind == "price" else "sessions", "2")
            with pytest.raises(ValueError, match="publication evidence"):
                api().verify_sealed_publication(workspace, selected.generation_id, budget=BUDGET)
            with pytest.raises(ValueError, match="publication evidence"):
                verify_workspace(workspace)
    with pytest.raises(ValueError, match="publication evidence"):
        backup(home, tmp_path / "rejected-backup")
    assert not (tmp_path / "rejected-backup").exists()
    rehash_backup_inventory(archive)
    target = tmp_path / "rejected-restore"
    with pytest.raises(ValueError, match="publication evidence"):
        restore(archive, target)
    assert json.loads((target / "installation.json").read_bytes())["phase"] == "restore-incomplete"


@pytest.mark.parametrize("fault", ["missing-transform", "corrupt-transform", "source-row"])
@pytest.mark.parametrize("kind", ["price", "sessions"])
def test_honestly_rehashed_backup_does_not_certify_native_retention(
    tmp_path: Path, fault: str, kind: str
) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        selected = pin(workspace, "synthetic-prices" if kind == "price" else "sessions")
        path = transform_path(workspace, selected).relative_to(home)
        body = json.loads((home / path).read_bytes())
        table = list_tables(workspace, body["source"]["source_id"])[0]
    archive = tmp_path / "backup"
    backup(home, archive)
    if fault == "missing-transform":
        (archive / path).unlink()
        manifest = json.loads((archive / "backup.json").read_bytes())
        del manifest["files"][path.as_posix()]
        (archive / "backup.json").write_text(json.dumps(manifest))
    elif fault == "corrupt-transform":
        (archive / path).write_bytes(b"{}")
    else:
        with closing(sqlite3.connect(archive / "strategies.sqlite3")) as connection:
            connection.execute(
                " ".join(("UPDATE", quoted(str(table["target"])), "SET source_row_hash=?")),
                ("d" * 64,),
            )
            connection.commit()
    rehash_backup_inventory(archive)
    target = tmp_path / "restored"
    if fault == "source-row":
        with pytest.raises(ValueError, match="source library content"):
            restore(archive, target)
    else:
        # Honest ordinary integrity cannot certify a duty not recorded by generic v1.
        assert restore(archive, target)["restored"]
        with open_workspace(target) as workspace:
            assert verify_workspace(workspace)["verified"]
            with pytest.raises((ValueError, FileNotFoundError)):
                api().admit_native_input(
                    workspace,
                    selected,
                    expected_schema="aas-" + kind + "-transform-v1",
                    budget=BUDGET,
                )


@pytest.mark.parametrize("kind", ["price", "sessions"])
@pytest.mark.parametrize(
    "fault",
    ["catalog-transform", "catalog-normalizer", "catalog-manifest", "intent", "source-link"],
)
def test_sealed_references_reject_honestly_rehashed_backups(
    tmp_path: Path, kind: str, fault: str
) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        selected = pin(workspace, "synthetic-prices" if kind == "price" else "sessions")
    archive = tmp_path / "backup"
    backup(home, archive)
    if fault.startswith("catalog-"):
        mutations = {
            "catalog-transform": (
                "UPDATE dataset_versions SET transform_hash=? WHERE generation_id=?"
            ),
            "catalog-normalizer": (
                "UPDATE dataset_versions SET normalizer_version=? WHERE generation_id=?"
            ),
            "catalog-manifest": "UPDATE dataset_versions SET manifest_hash=? WHERE generation_id=?",
        }
        corrupt_closed_store(
            archive,
            "state",
            "dataset_versions",
            mutations[fault],
            ("f" * 64, selected.generation_id),
        )
    elif fault == "intent":
        corrupt_closed_store(
            archive,
            "state",
            "storage_operations",
            "UPDATE storage_operations SET payload_hash=? WHERE target_id=?",
            ("f" * 64, selected.generation_id),
        )
    else:
        corrupt_closed_store(
            archive,
            "state",
            "dataset_sources",
            "DELETE FROM dataset_sources WHERE dataset_id=?",
            (selected.dataset_id,),
        )
    rehash_backup_inventory(archive)
    with (
        open_workspace(archive) as workspace,
        pytest.raises(ValueError, match=r"publication|catalog"),
    ):
        verify_workspace(workspace)
    with pytest.raises(ValueError, match=r"publication|catalog"):
        backup(archive, tmp_path / "rejected-backup")
    with pytest.raises(ValueError, match=r"publication|catalog"):
        restore(archive, tmp_path / "rejected-restore")


@pytest.mark.parametrize("kind", ["price", "sessions"])
@pytest.mark.parametrize(
    "fault", ["schema", "missing-field", "source-id", "source-hash", "table-digest", "mapping"]
)
def test_native_schema_and_source_checks_do_not_reclassify_generic_content(
    stored: Workspace, kind: str, fault: str
) -> None:
    selected = pin(stored, "synthetic-prices" if kind == "price" else "sessions")
    transform = json.loads(transform_path(stored, selected).read_bytes())
    if fault == "schema":
        transform["schema_version"] = "opaque-schema"
    elif fault == "missing-field":
        del transform["calendar"]
    elif fault == "mapping":
        transform["columns"]["source_row_hash"] = "not-a-column"
    else:
        field = {
            "source-id": "source_id",
            "source-hash": "source_sha256",
            "table-digest": "table_digest",
        }[fault]
        transform["source"][field] = "f" * 64
    _, digest, _ = put_raw(stored.paths.raw, json.dumps(transform).encode())
    raw = stored.paths.raw / selected.manifest_hash[:2] / selected.manifest_hash
    body = json.loads(raw.read_bytes())
    body.update(
        dataset_id="generic",
        generation_id="generic",
        operation_id="generic",
        transform_sha256=digest,
    )
    body["rows"][0]["revision_id"] = "generic"
    publication.publish_document(stored, parse_import(json.dumps(body).encode()))
    generic = pin(stored, "generic")
    assert api().verify_sealed_publication(stored, generic.generation_id, budget=BUDGET)
    assert verify_workspace(stored)["verified"]
    with pytest.raises(ValueError, match=r"schema|field|source|column|digest"):
        api().admit_native_input(
            stored, generic, expected_schema="aas-" + kind + "-transform-v1", budget=BUDGET
        )


def test_arrow_source_native_admission_survives_fresh_restore(tmp_path: Path) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        seed_native(workspace, tmp_path)
        row = {**_source_row(), "revision_id": "arrow-r1"}
        schema = pa.schema(
            [(key, pa.int64() if type(value) is int else pa.string()) for key, value in row.items()]
        )
        table = pa.Table.from_pylist([row], schema=schema)
        digest = _hash_json(row)
        imported = import_arrow(workspace, "arrow", digest, "bars", table.to_reader())
        source_table = cast("list[dict[str, object]]", imported["tables"])[0]
        body = json.loads(transform_path(workspace, pin(workspace)).read_bytes())
        body["source"] = {
            "source_id": "arrow",
            "source_sha256": digest,
            "table": "bars",
            "table_digest": source_table["digest"],
        }
        body["dataset"].update(
            dataset_id="arrow-prices", generation_id="arrow-prices", operation_id="arrow-prices"
        )
        body_path = tmp_path / "arrow.json"
        raw = json.dumps(body).encode()
        body_path.write_bytes(raw)
        register_price_input(workspace, body_path, hashlib.sha256(raw).hexdigest())
        selected = pin(workspace, "arrow-prices")
        expected = api().admit_native_input(
            workspace, selected, expected_schema="aas-price-transform-v1", budget=BUDGET
        )
    for path in (*tmp_path.glob("*.sqlite3"), *tmp_path.glob("*.json")):
        path.unlink()
    archive, target = tmp_path / "backup", tmp_path / "restored"
    backup(home, archive)
    restore(archive, target)
    with open_workspace(target) as workspace:
        assert (
            api().admit_native_input(
                workspace, selected, expected_schema="aas-price-transform-v1", budget=BUDGET
            )
            == expected
        )


@pytest.mark.parametrize("version", ["latest", "LATEST"])
def test_historical_generic_versions_retain_ordinary_content_semantics(
    tmp_path: Path, version: str
) -> None:
    home = tmp_path / "home"
    initialize(home)
    body = json.loads(document())
    body["version"] = version
    with open_workspace(home, writable=True) as workspace:
        publication.publish_document(workspace, parse_import(json.dumps(body).encode()))
        assert api().verify_sealed_publication(workspace, body["generation_id"], budget=BUDGET)
        assert verify_workspace(workspace)["verified"]
    archive, target = tmp_path / "backup", tmp_path / "restored"
    backup(home, archive)
    assert restore(archive, target)["restored"]
