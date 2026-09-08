from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import ENGINE_BUNDLE_SCHEMA_V1
from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.strategies import (
    import_strategy,
    initialize_strategies,
    list_strategies,
    load_strategy,
)
from tests.engine.engine_support import contract, raw_bundle, strategy

INSTALLATION_ID = "synthetic"
BUNDLE_ID = "synthetic-probe"
VERSION = "1"
HEX_A = "ab" * 32
HEX_B = "cd" * 32


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _envelope(bundle_id: str, version: str, value: object | None = None) -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": ENGINE_BUNDLE_SCHEMA_V1,
            "bundle_id": bundle_id,
            "bundle_version": version,
            "contract": value if value is not None else contract(),
        }
    )


def _import(
    connection: sqlite3.Connection,
    raw: bytes,
    operation_id: str,
    strategy_id: str = BUNDLE_ID,
    version: str = VERSION,
) -> dict[str, object]:
    return import_strategy(connection, raw, _digest(raw), strategy_id, version, operation_id)


@pytest.fixture
def store(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = connect(tmp_path / "strategies.sqlite3")
    initialize_strategies(connection, INSTALLATION_ID)
    try:
        yield connection
    finally:
        connection.close()


def test_import_list_load_preserves_raw_bytes_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "strategies.sqlite3"
    connection = connect(path)
    initialize_strategies(connection, INSTALLATION_ID)
    raw = raw_bundle(contract())
    digest = _digest(raw)
    result = import_strategy(connection, raw, digest, BUNDLE_ID, VERSION, "op-1")
    listed = list_strategies(connection)
    loaded = load_strategy(connection, BUNDLE_ID, VERSION, digest)
    stored = connection.execute(
        "SELECT raw_bundle, request_hash FROM strategy_versions "
        "JOIN strategy_imports USING (strategy_id, version) WHERE operation_id='op-1'"
    ).fetchone()
    assert result["raw_sha256"] == digest
    assert stored["raw_bundle"] == raw
    assert stored["request_hash"] == digest
    assert listed == [
        {
            "strategy_id": BUNDLE_ID,
            "name": BUNDLE_ID,
            "lifecycle": "active",
            "version": VERSION,
            "raw_sha256": digest,
            "contract_sha256": loaded.contract_sha256,
            "imported_at_us": listed[0]["imported_at_us"],
        }
    ]
    assert loaded.source_sha256 == digest
    assert loaded.bundle_id == BUNDLE_ID
    assert loaded.bundle_version == VERSION
    assert connection.execute("SELECT count(*) FROM reference_metrics").fetchone()[0] == 0
    connection.close()
    restored = connect(path)
    try:
        initialize_strategies(restored, INSTALLATION_ID)
        again = load_strategy(restored, BUNDLE_ID, VERSION, digest)
        assert again.contract == loaded.contract
        assert again.source_sha256 == digest
    finally:
        restored.close()


def test_same_id_version_hash_conflict_and_idempotent_reimport(store: sqlite3.Connection) -> None:
    raw = raw_bundle(contract())
    digest = _digest(raw)
    first = import_strategy(store, raw, digest, BUNDLE_ID, VERSION, "op-1")
    assert import_strategy(store, raw, digest, BUNDLE_ID, VERSION, "op-1") == first
    second = import_strategy(store, raw, digest, BUNDLE_ID, VERSION, "op-2")
    assert second["raw_sha256"] == digest
    assert [row["version"] for row in list_strategies(store)] == [VERSION]
    receipts = [
        row[0]
        for row in store.execute("SELECT operation_id FROM strategy_imports ORDER BY operation_id")
    ]
    assert receipts == ["op-1", "op-2"]
    other = _envelope(
        BUNDLE_ID,
        VERSION,
        replace(contract(), pack=(replace(strategy(), description="other"),)),
    )
    with pytest.raises(ValueError, match="different content"):
        import_strategy(store, other, _digest(other), BUNDLE_ID, VERSION, "op-3")
    sibling = _envelope("synthetic-other", VERSION)
    _import(store, sibling, "op-4", "synthetic-other", VERSION)
    ids = [row["strategy_id"] for row in list_strategies(store)]
    assert ids == ["synthetic-other", BUNDLE_ID]
    with pytest.raises(ValueError, match="different import"):
        _import(store, sibling, "op-1", "synthetic-other", VERSION)


@pytest.mark.parametrize(
    ("raw", "digest", "strategy_id", "version"),
    [
        (raw_bundle(contract()), "0" * 64, BUNDLE_ID, VERSION),
        (raw_bundle(contract()), None, "wrong-id", VERSION),
        (raw_bundle(contract()), None, BUNDLE_ID, "2"),
        (b"{", None, BUNDLE_ID, VERSION),
        (raw_bundle(contract()).replace(b'"1"', b'"1", "1"'), None, BUNDLE_ID, VERSION),
    ],
)
def test_malformed_bundle_rejected(
    store: sqlite3.Connection,
    raw: bytes,
    digest: str | None,
    strategy_id: str,
    version: str,
) -> None:
    pin = digest or _digest(raw)
    with pytest.raises(ValueError, match=r"match|JSON|identity|version|schema|document"):
        import_strategy(store, raw, pin, strategy_id, version, "op-bad")
    assert list_strategies(store) == []


def test_read_only_initialize_and_load(tmp_path: Path) -> None:
    path = tmp_path / "strategies.sqlite3"
    raw = raw_bundle(contract())
    digest = _digest(raw)
    writable = connect(path)
    initialize_strategies(writable, INSTALLATION_ID)
    import_strategy(writable, raw, digest, BUNDLE_ID, VERSION, "op-1")
    writable.close()
    reader = connect(path, read_only=True)
    try:
        initialize_strategies(reader, INSTALLATION_ID)
        assert load_strategy(reader, BUNDLE_ID, VERSION, digest).source_sha256 == digest
        with pytest.raises(ValueError, match="not registered"):
            load_strategy(reader, BUNDLE_ID, "missing", digest)
        with pytest.raises(ValueError, match="execution pin"):
            load_strategy(reader, BUNDLE_ID, VERSION, HEX_A)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            import_strategy(reader, raw, digest, BUNDLE_ID, VERSION, "op-2")
    finally:
        reader.close()


def test_version_rows_are_immutable(store: sqlite3.Connection) -> None:
    raw = raw_bundle(contract())
    _import(store, raw, "op-1")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        store.execute("UPDATE strategy_versions SET contract_json='tamper'")
    store.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        store.execute("DELETE FROM strategy_imports")
    store.rollback()
    assert store.execute("SELECT count(*) FROM strategy_versions").fetchone()[0] == 1


def test_lineage_fk_missing_parent_and_cycle(store: sqlite3.Connection) -> None:
    _import(store, _envelope("child", VERSION), "op-child", "child", VERSION)
    _import(store, _envelope("parent", VERSION), "op-parent", "parent", VERSION)
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        store.execute(
            "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
            ("missing", VERSION, "parent", VERSION, "derived", "synthetic", HEX_A, "resolved"),
        )
    store.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="resolved parent missing"):
        store.execute(
            "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
            ("child", VERSION, "parent", "absent", "derived", "synthetic", HEX_A, "resolved"),
        )
    store.rollback()
    store.execute(
        "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
        ("child", VERSION, "parent", VERSION, "derived", "synthetic", HEX_A, "resolved"),
    )
    with pytest.raises(sqlite3.IntegrityError, match="cycle"):
        store.execute(
            "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
            ("parent", VERSION, "child", VERSION, "derived", "synthetic", HEX_B, "resolved"),
        )
    store.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="cycle"):
        store.execute(
            "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
            ("child", VERSION, "child", VERSION, "self", "synthetic", HEX_B, "resolved"),
        )
    store.rollback()
    store.execute(
        "INSERT INTO strategy_lineage VALUES (?,?,?,?,?,?,?,?)",
        (
            "child",
            VERSION,
            "external",
            None,
            "pack_variant",
            "unresolved parent",
            HEX_B,
            "unresolved",
        ),
    )
    digest = _digest(_envelope("child", VERSION))
    with pytest.raises(ValueError, match="unresolved parent"):
        load_strategy(store, "child", VERSION, digest)
