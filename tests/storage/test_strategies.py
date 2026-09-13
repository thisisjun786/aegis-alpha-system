from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine import ENGINE_BUNDLE_SCHEMA_V1
from aegis_alpha.engine.bundle import load_bundle
from aegis_alpha.storage.sqlite import connect
from aegis_alpha.storage.strategies import (
    LineageSpec,
    import_strategy,
    initialize_strategies,
    list_strategies,
    load_strategy,
    strategy_request_hash,
    validate_strategy_import,
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


def _import(  # noqa: PLR0913 -- mirror explicit import pins and optional lineage
    connection: sqlite3.Connection,
    raw: bytes,
    operation_id: str,
    strategy_id: str = BUNDLE_ID,
    version: str = VERSION,
    *,
    lineage: LineageSpec | None = None,
) -> dict[str, object]:
    return import_strategy(
        connection, raw, _digest(raw), strategy_id, version, operation_id, lineage=lineage
    )


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


def test_preflight_works_on_read_only_connection(tmp_path: Path, store: sqlite3.Connection) -> None:
    raw = raw_bundle(contract())
    bundle = load_bundle(raw, _digest(raw), BUNDLE_ID, VERSION)
    lineage = LineageSpec("missing-parent", "7", "derived", " exact synthetic\n")
    reader = connect(tmp_path / "strategies.sqlite3", read_only=True)
    try:
        before = tuple(reader.iterdump())
        assert validate_strategy_import(reader, bundle, "op", lineage=lineage) == (False, False)
        assert tuple(reader.iterdump()) == before
        _import(store, raw, "op", lineage=lineage)
        before = tuple(reader.iterdump())
        assert validate_strategy_import(reader, bundle, "op", lineage=lineage) == (True, True)
        assert validate_strategy_import(reader, bundle, "new-op", lineage=lineage) == (True, False)
        with pytest.raises(ValueError, match="different lineage"):
            validate_strategy_import(reader, bundle, "op")
        assert tuple(reader.iterdump()) == before
    finally:
        reader.close()


@pytest.mark.parametrize("conflict", ["bytes", "lineage", "cycle", "receipt"])
def test_private_import_revalidates_after_successful_preflight(
    store: sqlite3.Connection, conflict: str
) -> None:
    raw = raw_bundle(contract())
    bundle = load_bundle(raw, _digest(raw), BUNDLE_ID, VERSION)
    lineage = LineageSpec("parent", "7", "derived", "synthetic")
    assert validate_strategy_import(store, bundle, "op", lineage=lineage) == (False, False)
    # Commit changed private state after preflight, using the real owner, not a stub.
    if conflict == "bytes":
        _import(store, raw + b"\n", "other-op", lineage=lineage)
    elif conflict == "lineage":
        _import(store, raw, "other-op")
    elif conflict == "cycle":
        _import(
            store,
            _envelope("parent", "7"),
            "other-op",
            "parent",
            "7",
            lineage=LineageSpec(BUNDLE_ID, VERSION, "derived", "synthetic"),
        )
    else:
        _import(store, _envelope("parent", "7"), "op", "parent", "7")
    before = tuple(store.iterdump())
    statements: list[str] = []
    store.set_trace_callback(statements.append)
    try:
        with pytest.raises(
            ValueError, match=r"different content|different lineage|cycle|different import"
        ):
            _import(store, raw, "op", lineage=lineage)
    finally:
        store.set_trace_callback(None)
    assert statements[0] == "BEGIN IMMEDIATE"
    assert statements[-1] == "ROLLBACK"
    assert tuple(store.iterdump()) == before


@pytest.mark.parametrize(
    ("mutation", "value", "error"),
    [
        ("UPDATE strategy_versions SET raw_bundle=?", b"{}", "raw payload SHA-256"),
        ("UPDATE strategy_versions SET raw_sha256=?", "0" * 64, "different content"),
        ("UPDATE strategy_versions SET contract_json=?", "{}", "parsed contract hash"),
        ("UPDATE strategy_versions SET contract_sha256=?", "0" * 64, "different content"),
    ],
)
def test_preflight_preserves_existing_stored_content_checks(
    store: sqlite3.Connection, mutation: str, value: str | bytes, error: str
) -> None:
    raw = raw_bundle(contract())
    bundle = load_bundle(raw, _digest(raw), BUNDLE_ID, VERSION)
    _import(store, raw, "op")
    triggers = store.execute(
        "SELECT name,sql FROM sqlite_master WHERE name IN "
        "('strategy_versions_reject_update','immutable_strategy_versions_update')"
    ).fetchall()
    # Synthetic corruption; restore the exact schema before validating.
    for trigger in triggers:
        store.execute(f'DROP TRIGGER "{trigger["name"]}"')
    store.execute(mutation, (value,))
    for trigger in triggers:
        store.execute(trigger["sql"])
    store.commit()
    before = tuple(store.iterdump())
    with pytest.raises(ValueError, match=error):
        validate_strategy_import(store, bundle, "new-op")
    assert tuple(store.iterdump()) == before


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
    # Direct DDL insertion is not authenticated registration evidence.
    with pytest.raises(ValueError, match="lineage"):
        load_strategy(store, "child", VERSION, digest)


@pytest.mark.parametrize("parent_first", [True, False])
def test_lineage_status_is_fixed_at_registration(
    store: sqlite3.Connection, *, parent_first: bool
) -> None:
    parent = _envelope("parent", "7")
    lineage = LineageSpec("parent", "7", "derived", " synthetic reason\n")
    # A different registered parent version must not resolve the supplied version.
    _import(store, _envelope("parent", "1"), "parent-1", "parent")
    if parent_first:
        _import(store, parent, "parent-7", "parent", "7")
    raw = _envelope("child", "1")
    first = _import(store, raw, "child-1", "child", lineage=lineage)
    expected = (
        "child",
        "1",
        "parent",
        "7",
        "derived",
        " synthetic reason\n",
        hashlib.sha256(b'" synthetic reason\\n"').hexdigest(),
        "resolved" if parent_first else "unresolved",
    )
    assert tuple(store.execute("SELECT * FROM strategy_lineage").fetchone()) == expected
    request_hash = _digest(
        canonical_json_bytes(
            {
                "schema_version": "aas-strategy-import-request-v2",
                "parent_status": expected[-1],
                "strategy_id": "child",
                "version": "1",
                "raw_sha256": _digest(raw),
                "lineage": {
                    "parent_id": "parent",
                    "parent_version": "7",
                    "change_kind": "derived",
                    "reason": " synthetic reason\n",
                },
            }
        )
    )
    assert tuple(
        store.execute(
            "SELECT request_hash,raw_sha256,contract_sha256 FROM strategy_imports "
            "JOIN strategy_versions USING(strategy_id,version) WHERE operation_id='child-1'"
        ).fetchone()
    ) == (request_hash, _digest(raw), _digest(canonical_json_bytes(contract())))
    if not parent_first:
        with pytest.raises(ValueError, match="unresolved parent"):
            load_strategy(store, "child", "1", _digest(raw))
        _import(store, parent, "parent-7", "parent", "7")
    before = "\n".join(store.iterdump())
    assert _import(store, raw, "child-1", "child", lineage=lineage) == first
    assert "\n".join(store.iterdump()) == before
    if parent_first:
        assert load_strategy(store, "child", "1", _digest(raw)).bundle_version == "1"
    else:
        with pytest.raises(ValueError, match="unresolved parent"):
            load_strategy(store, "child", "1", _digest(raw))
    corrected = _envelope("child", "2")
    _import(store, corrected, "child-2", "child", "2", lineage=lineage)
    assert load_strategy(store, "child", "2", _digest(corrected)).bundle_version == "2"
    assert [
        tuple(row)
        for row in store.execute(
            "SELECT version,parent_status FROM strategy_lineage ORDER BY version"
        )
    ] == [("1", expected[-1]), ("2", "resolved")]
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        store.execute("UPDATE strategy_lineage SET parent_status='resolved'")
    store.rollback()


@pytest.mark.parametrize(
    "other",
    [
        None,
        LineageSpec("other", "7", "derived", "synthetic"),
        LineageSpec("parent", "8", "derived", "synthetic"),
        LineageSpec("parent", "7", "correction", "synthetic"),
        LineageSpec("parent", "7", "derived", "synthetic "),
    ],
)
def test_conflicting_lineage_never_changes_existing_evidence(
    store: sqlite3.Connection, other: LineageSpec | None
) -> None:
    raw = raw_bundle(contract())
    _import(store, raw, "child", lineage=LineageSpec("parent", "7", "derived", "synthetic"))
    before = "\n".join(store.iterdump())
    for operation in ("child", "other-operation"):
        with pytest.raises(ValueError, match="different lineage"):
            _import(store, raw, operation, lineage=other)
        assert "\n".join(store.iterdump()) == before


def test_status_commitment_literal_vectors() -> None:
    # Hash-algebra pins, not a claim that this synthetic contract hashes to a*64.
    raw = _envelope("child", "1")
    bundle = replace(load_bundle(raw, _digest(raw), "child", "1"), source_sha256="a" * 64)
    lineage = LineageSpec("parent", "7", "derived", " exact\n")
    document = {
        "schema_version": "aas-strategy-import-request-v2",
        "strategy_id": "child",
        "version": "1",
        "raw_sha256": "a" * 64,
        "lineage": {
            "parent_id": "parent",
            "parent_version": "7",
            "change_kind": "derived",
            "reason": " exact\n",
        },
        "parent_status": "unresolved",
    }

    def independent(value: object) -> str:
        return hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
        ).hexdigest()

    for status, expected in (
        ("unresolved", "5cb4df616642f167342c946051fbac471d21b232cc99a4d383a150f4e0d6b79d"),
        ("resolved", "264bdef280c2a0cf60794aee3aa0ef50c5da4e755bd56fe44fc1b738c7bf2c9f"),
    ):
        assert independent({**document, "parent_status": status}) == expected
        assert strategy_request_hash(bundle, lineage, parent_status=status) == expected
    changed = replace(lineage, reason=" exact\n ")
    assert (
        strategy_request_hash(bundle, changed, parent_status="unresolved")
        == "15f96950fe815a02faa5ec8c4630fc412da1679bcc9feb34d751650d68a74504"
    )
    old = {key: value for key, value in document.items() if key != "parent_status"}
    old["schema_version"] = "aas-strategy-import-request-v1"
    assert independent(old) == "a469eccd2f87eb1e9cd02d6697f2bd2ce38350b7a344b862abe7604c4d6fa4fd"
    assert (
        independent(lineage.reason)
        == "030019ad868b140268253350e32ff60b333b3428a25b5a4d7ed3c8e5ea2dce27"
    )
    assert (
        "strategy-" + hashlib.sha256(("child\x001\x00" + "a" * 64).encode()).hexdigest()
        == "strategy-a35b6c4bcbb03a071d4b715fafe2eebcceb3b037043fb8f1cdbd5dbafa5e3089"
    )
    assert strategy_request_hash(bundle, None) == "a" * 64
    for status in (None, "unknown", ""):
        with pytest.raises(ValueError, match="parent status"):
            strategy_request_hash(bundle, lineage, parent_status=status)


@pytest.mark.parametrize("lineage", [None, LineageSpec("parent", "7", "derived", "synthetic")])
@pytest.mark.parametrize("mutation", ["missing", "extra-bad", "first-bad"])
def test_load_and_retry_check_every_private_receipt(
    store: sqlite3.Connection, lineage: LineageSpec | None, mutation: str
) -> None:
    raw = raw_bundle(contract())
    _import(store, raw, "first", lineage=lineage)
    _import(store, raw, "second", lineage=lineage)
    triggers = store.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='strategy_imports'"
    ).fetchall()
    for row in triggers:
        store.execute('DROP TRIGGER "' + row[0] + '"')
    if mutation == "missing":
        store.execute("DELETE FROM strategy_imports")
    else:
        store.execute(
            "UPDATE strategy_imports SET request_hash=? WHERE operation_id=?",
            ("0" * 64, "second" if mutation == "extra-bad" else "first"),
        )
    for row in triggers:
        store.execute(row[1])
    store.commit()
    before = tuple(store.iterdump())
    with pytest.raises(ValueError, match="stored lineage"):
        load_strategy(store, BUNDLE_ID, VERSION, _digest(raw))
    for operation in ("first", "new"):
        with pytest.raises(ValueError, match="stored lineage"):
            _import(store, raw, operation, lineage=lineage)
        assert tuple(store.iterdump()) == before


def test_lineage_cannot_be_added_to_existing_version(store: sqlite3.Connection) -> None:
    raw = raw_bundle(contract())
    _import(store, raw, "original")
    with pytest.raises(ValueError, match="different lineage"):
        _import(store, raw, "other", lineage=LineageSpec("parent", "7", "derived", "synthetic"))
    assert store.execute("SELECT count(*) FROM strategy_lineage").fetchone()[0] == 0
    assert store.execute("SELECT count(*) FROM strategy_imports").fetchone()[0] == 1


@pytest.mark.parametrize("depth", [0, 1, 3])
def test_lineage_cycles_roll_back_the_new_version(store: sqlite3.Connection, depth: int) -> None:
    for index in range(depth):
        name, parent = str(index), str(index + 1)
        _import(
            store,
            _envelope(name, "1"),
            name,
            name,
            lineage=LineageSpec(parent, "1", "derived", "synthetic"),
        )
    name = str(depth)
    before = "\n".join(store.iterdump())
    with pytest.raises(ValueError, match="cycle"):
        _import(
            store,
            _envelope(name, "1"),
            name,
            name,
            lineage=LineageSpec("0", "1", "derived", "synthetic"),
        )
    assert "\n".join(store.iterdump()) == before


@pytest.mark.parametrize(
    "fields",
    [
        ("", "1", "derived", "reason"),
        (" parent", "1", "derived", "reason"),
        ("parent", "", "derived", "reason"),
        ("parent", "1 ", "derived", "reason"),
        ("parent", "1", " derived", "reason"),
        ("parent", "1", "", "reason"),
        ("parent", "1", "derived", " \n"),
    ],
)
def test_lineage_spec_rejects_invalid_fields(fields: tuple[str, str, str, str]) -> None:
    with pytest.raises(ValueError, match="lineage"):
        LineageSpec(*fields)
