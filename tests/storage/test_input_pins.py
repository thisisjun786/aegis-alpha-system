from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage.input_pins import ConventionPin, read_convention, register_convention
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.workspace import initialize, open_workspace

A = (
    b'{"hash_format":"aas-canonical-json-sha256-v1","id":"synthetic-capital","kind":"basis",'
    b'"payload":{"price_basis":"capital","schema":"aas-basis-v1"},'
    b'"schema":"aas-convention-v1","version":"1"}'
)
B = (
    b'{"hash_format":"aas-canonical-json-sha256-v1","id":"synthetic-total-return","kind":"basis",'
    b'"payload":{"price_basis":"total_return","schema":"aas-basis-v1"},'
    b'"schema":"aas-convention-v1","version":"1"}'
)
PIN_A = ConventionPin("basis", "synthetic-capital", "1", hashlib.sha256(A).hexdigest())
PIN_B = ConventionPin("basis", "synthetic-total-return", "1", hashlib.sha256(B).hexdigest())


def test_register_and_read_exact_documents(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        for raw, expected in ((A, PIN_A), (B, PIN_B)):
            pin = register_convention(
                ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
            )
            assert pin == expected
            assert read_convention(ws.state, pin) == raw
        assert ws.state.execute("SELECT count(*) FROM conventions").fetchone()[0] == len((A, B))
        assert tuple(
            ws.state.execute(
                "SELECT (SELECT count(*) FROM input_bundles), "
                "(SELECT count(*) FROM input_bindings), "
                "(SELECT count(*) FROM runs), (SELECT count(*) FROM storage_operations)"
            ).fetchone()
        ) == (0, 0, 0, 0)


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"])
def test_bomless_encoding_is_rejected_without_mutation(tmp_path: Path, encoding: str) -> None:
    raw = A.decode("utf-8").encode(encoding)
    with pytest.raises(json.JSONDecodeError):
        json.loads(raw.decode("utf-8", errors="strict"))
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        assert _rows(ws.state) == []
        assert ws.state.total_changes == 0
        try:
            register_convention(ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest())
        except ValueError as error:
            rejection = type(error)
        else:
            rejection = None
        assert (rejection, _rows(ws.state), ws.state.total_changes) == (ValueError, [], 0)


def test_escaped_json_strings_retain_semantics(tmp_path: Path) -> None:
    raw = (
        b'{"schema":"aas-convention-v1","hash_format":"aas-canonical-json-sha256-v1",'
        b'"kind":"cost","id":"synthetic-escapes","version":"1",'
        b'"payload":{"schema":"synthetic-opaque-v1","text":"\\u0000\\n\\u00e9\\\\u0000"}}'
    )
    expected = (
        b'{"hash_format":"aas-canonical-json-sha256-v1","id":"synthetic-escapes","kind":"cost",'
        b'"payload":{"schema":"synthetic-opaque-v1","text":"\\u0000\\n\xc3\xa9\\\\u0000"},'
        b'"schema":"aas-convention-v1","version":"1"}'
    )
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        pin = register_convention(
            ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
        assert pin == ConventionPin(
            "cost", "synthetic-escapes", "1", hashlib.sha256(expected).hexdigest()
        )
        assert read_convention(ws.state, pin) == expected
        assert json.loads(read_convention(ws.state, pin))["payload"]["text"] == "\x00\né\\u0000"


def test_wrong_file_hash(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(ws.state, A, expected_file_sha256="0" * 64)
        assert ws.state.execute("SELECT count(*) FROM conventions").fetchone()[0] == 0


def test_same_key_different_content_conflicts(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    changed = A.replace(b'"price_basis":"capital"', b'"price_basis":"total_return"')
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
        before = ws.state.total_changes
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(
                ws.state, changed, expected_file_sha256=hashlib.sha256(changed).hexdigest()
            )
        assert ws.state.total_changes == before
        assert read_convention(ws.state, PIN_A) == A
        version_two = changed.replace(b'"version":"1"', b'"version":"2"')
        second = register_convention(
            ws.state, version_two, expected_file_sha256=hashlib.sha256(version_two).hexdigest()
        )
        assert second == ConventionPin(
            "basis", PIN_A.id, "2", hashlib.sha256(version_two).hexdigest()
        )
        assert read_convention(ws.state, second) == version_two
        assert read_convention(ws.state, PIN_A) == A


_CHANGED = A.replace(b'"price_basis":"capital"', b'"price_basis":"total_return"')


@pytest.mark.parametrize(
    ("payload", "stored_hash"),
    [
        pytest.param(_CHANGED, PIN_A.hash, id="payload"),
        pytest.param(A, "0" * 64, id="hash"),
        pytest.param(A.replace(b"synthetic-capital", b"synthetic-other"), PIN_A.hash, id="id"),
        pytest.param(A + b"\n", PIN_A.hash, id="whitespace"),
        pytest.param(_CHANGED, hashlib.sha256(_CHANGED).hexdigest(), id="payload-and-hash"),
        pytest.param(
            A.replace(b"aas-convention-v1", b"aas-convention-v9"), PIN_A.hash, id="schema"
        ),
        pytest.param(A.replace(b"aas-basis-v1", b"aas-basis-v9"), PIN_A.hash, id="basis-schema"),
        pytest.param(
            A.replace(b'"price_basis":', b'"extra":true,"price_basis":'),
            PIN_A.hash,
            id="basis-extra",
        ),
        pytest.param(
            A.replace(b'"price_basis":"capital"', b'"price_basis":"raw"'),
            PIN_A.hash,
            id="basis-invalid",
        ),
        pytest.param(b"{", PIN_A.hash, id="malformed"),
        pytest.param(A.replace(b'"version":"1"', b'"version":"2"'), PIN_A.hash, id="version"),
        pytest.param(A.replace(b'"kind":"basis"', b'"kind":"cost"'), PIN_A.hash, id="kind"),
        pytest.param(A, "G" * 64, id="hash-syntax"),
    ],
)
def test_read_rejects_copied_store_tampering(
    tmp_path: Path, payload: bytes, stored_hash: str
) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
    copied = tmp_path / "copy"
    shutil.copytree(home, copied)
    connection = sqlite3.connect(load_paths(copied).state)
    try:
        connection.execute("DROP TRIGGER immutable_conventions_update")
        connection.execute(
            "UPDATE conventions SET payload=?,content_hash=?", (payload.decode(), stored_hash)
        )
        connection.commit()
    finally:
        connection.close()
    with open_workspace(copied) as ws:
        before = _rows(ws.state)
        changes = ws.state.total_changes
        trace: list[str] = []
        ws.state.set_authorizer(_select_only)
        ws.state.set_trace_callback(trace.append)
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            read_convention(ws.state, PIN_A)
        assert _rows(ws.state) == before
        assert ws.state.total_changes == changes
        assert trace
        assert all(sql.startswith("SELECT ") for sql in trace)
    with open_workspace(copied, writable=True) as ws:
        before = _rows(ws.state)
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(ws.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
        assert _rows(ws.state) == before
        assert ws.state.total_changes == 0
    with open_workspace(home) as ws:
        assert read_convention(ws.state, PIN_A) == A


def test_wrong_requested_hash(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=hashlib.sha256(A).hexdigest())
        before = ws.state.total_changes
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            read_convention(ws.state, replace(PIN_A, hash="0" * 64))
        assert ws.state.total_changes == before
        assert read_convention(ws.state, PIN_A) == A


def _rows(connection: sqlite3.Connection) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in connection.execute(
            "SELECT kind,convention_id,version,payload,content_hash FROM conventions "
            "ORDER BY kind,convention_id,version"
        )
    ]


def _select_only(action: int, *_args: str | None) -> int:
    return (
        sqlite3.SQLITE_OK
        if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ)
        else sqlite3.SQLITE_DENY
    )


def _raw(**changes: object) -> bytes:
    document = json.loads(A)
    document.update(changes)
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()


@pytest.mark.parametrize(
    "kind", ["calendar", "fx", "basis", "cost", "execution", "benchmark", "risk_free"]
)
def test_every_allowed_kind_is_structurally_registered(tmp_path: Path, kind: str) -> None:
    home = tmp_path / "aas"
    initialize(home)
    raw = (
        A
        if kind == "basis"
        else _raw(
            kind=kind,
            payload={
                "schema": "synthetic-opaque-v7",
                "ref": "unresolved",
                "values": [None, True, False, 17, 1.25, "17", "é"],
            },
        )
    )
    expected = json.dumps(
        json.loads(raw), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    with open_workspace(home, writable=True) as ws:
        pin = register_convention(
            ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
        assert pin == ConventionPin(kind, PIN_A.id, "1", hashlib.sha256(expected).hexdigest())
        assert read_convention(ws.state, pin) == expected
        assert json.loads(read_convention(ws.state, pin)) == json.loads(raw)


def test_reimport_is_semantically_idempotent(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    pretty = (json.dumps(dict(reversed(list(json.loads(A).items()))), indent=2) + "\n").encode()
    assert hashlib.sha256(pretty).hexdigest() != PIN_A.hash
    with open_workspace(home, writable=True) as ws:
        assert register_convention(ws.state, A, expected_file_sha256=PIN_A.hash) == PIN_A
        before = _rows(ws.state)
        changes = ws.state.total_changes
        trace: list[str] = []
        ws.state.set_trace_callback(trace.append)
        for raw in (A, pretty):
            assert (
                register_convention(
                    ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
                )
                == PIN_A
            )
        assert _rows(ws.state) == before
        assert ws.state.total_changes == changes
        assert all(sql.split()[0] in {"BEGIN", "SELECT", "COMMIT"} for sql in trace)
        returned = read_convention(ws.state, PIN_A)
        assert isinstance(returned, bytes)
        assert returned == A
        assert not hasattr(PIN_A, "__dict__")
        field = "version"
        with pytest.raises(FrozenInstanceError):
            setattr(PIN_A, field, "2")


_INVALID_DOCUMENTS = [
    pytest.param(b"", id="empty"),
    pytest.param(b"{", id="invalid-json"),
    pytest.param(b"[]", id="array-envelope"),
    pytest.param(b"null", id="null-envelope"),
    pytest.param(A.replace(b'"schema":"aas-convention-v1",', b""), id="missing-key"),
    pytest.param(_raw(extra=True), id="extra-key"),
    pytest.param(_raw(schema="aas-convention-v2"), id="envelope-schema"),
    pytest.param(_raw(hash_format="sha256"), id="hash-format"),
    pytest.param(_raw(kind="unknown"), id="unknown-kind"),
    pytest.param(_raw(kind=[]), id="nonstring-kind"),
    pytest.param(_raw(id=""), id="empty-id"),
    pytest.param(_raw(id=" x"), id="untrimmed-id"),
    pytest.param(_raw(id="x\x00y"), id="control-id"),
    pytest.param(_raw(id="x\x9fy"), id="c1-control-id"),
    pytest.param(_raw(id=123), id="nonstring-id"),
    pytest.param(_raw(version="latest"), id="latest"),
    pytest.param(_raw(version="LATEST"), id="uppercase-latest"),
    pytest.param(_raw(version=""), id="empty-version"),
    pytest.param(_raw(version=" "), id="blank-version"),
    pytest.param(_raw(version="1\x7f"), id="control-version"),
    pytest.param(_raw(version=1), id="numeric-version"),
    pytest.param(_raw(payload=[]), id="nonobject-payload"),
    pytest.param(_raw(payload={}), id="missing-payload-schema"),
    pytest.param(_raw(payload={"schema": " "}), id="blank-payload-schema"),
    pytest.param(_raw(kind="cost", payload={"schema": 1}), id="nonstring-payload-schema"),
    pytest.param(_raw(kind="cost", payload={"schema": "x\x80"}), id="control-payload-schema"),
    pytest.param(
        _raw(payload={"schema": "aas-basis-v9", "price_basis": "capital"}),
        id="unknown-basis-schema",
    ),
    pytest.param(_raw(payload={"schema": "aas-basis-v1"}), id="missing-basis"),
    pytest.param(
        _raw(payload={"schema": "aas-basis-v1", "price_basis": "raw"}), id="invalid-basis"
    ),
    pytest.param(
        _raw(payload={"schema": "aas-basis-v1", "price_basis": ["capital"]}), id="nonstring-basis"
    ),
    pytest.param(
        _raw(payload={"schema": "aas-basis-v1", "price_basis": "capital", "extra": 1}),
        id="extra-basis-key",
    ),
    pytest.param(
        A.replace(b'"kind":"basis"', b'"kind":"basis","kind":"basis"'), id="duplicate-top-key"
    ),
    pytest.param(
        A.replace(b'"price_basis":"capital"', b'"price_basis":"capital","price_basis":"capital"'),
        id="duplicate-nested-key",
    ),
    pytest.param(_raw(kind="cost", payload={"schema": "s", "n": float("nan")}), id="nan"),
    pytest.param(_raw(kind="cost", payload={"schema": "s", "n": float("inf")}), id="infinity"),
    pytest.param(
        _raw(kind="cost", payload={"schema": "s", "n": -float("inf")}), id="negative-infinity"
    ),
    pytest.param(
        _raw(kind="cost", payload={"schema": "s", "n": 1}).replace(b'"n":1', b'"n":1e999'),
        id="exponent-overflow",
    ),
    pytest.param(A + b"\xff", id="bad-utf8"),
    pytest.param(b"\xef\xbb\xbf" + A, id="utf8-bom"),
    pytest.param(A.decode().encode("utf-16"), id="utf16"),
    pytest.param(A.replace(b"synthetic-capital", b"\\ud800"), id="unpaired-surrogate"),
    pytest.param(b"[" * 2000 + b"0" + b"]" * 2000, id="recursion"),
    pytest.param(A + b" " * (1024 * 1024), id="oversize-raw"),
    pytest.param(
        _raw(kind="cost", payload={"schema": "s", "n": []}).replace(
            b"[]", b"[" + b"1e1," * 250000 + b"1e1]"
        ),
        id="oversize-canonical",
    ),
]


@pytest.mark.parametrize("raw", _INVALID_DOCUMENTS)
def test_strict_document_and_pin_rejections(tmp_path: Path, raw: bytes) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=PIN_A.hash)
        before = _rows(ws.state)
        changes = ws.state.total_changes
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest())
        assert _rows(ws.state) == before
        assert ws.state.total_changes == changes


@pytest.mark.parametrize("field", ["id", "version"])
@pytest.mark.parametrize(
    "value", ["", " ", " leading", "trailing ", "x\x00y", "x\x1fy", "x\x7fy", "x\x9fy"]
)
def test_pin_identity_rejections(field: str, value: str) -> None:
    with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
        replace(PIN_A, **{field: value})


@pytest.mark.parametrize("value", ["latest", "LATEST", "LaTeSt"])
def test_latest_pin_rejections(value: str) -> None:
    with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
        replace(PIN_A, version=value)


@pytest.mark.parametrize("value", ["", "a" * 63, "a" * 65, "A" * 64, "g" * 64, "0" * 64 + "\n"])
def test_hash_syntax_rejections(tmp_path: Path, value: str) -> None:
    with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
        replace(PIN_A, hash=value)
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(ws.state, A, expected_file_sha256=value)
        assert _rows(ws.state) == []


@pytest.mark.parametrize("value", ["unknown", " BASIS", "Basis", ""])
def test_pin_kind_rejections(value: str) -> None:
    with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
        replace(PIN_A, kind=value)


@pytest.mark.parametrize("value", [None, 1, [], {}])
def test_nonstring_pin_values(value: object) -> None:
    for field in ("kind", "id", "version", "hash"):
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            replace(PIN_A, **{field: value})


@pytest.mark.parametrize("value", ["{}", bytearray(A), None])
def test_ingress_requires_bytes(tmp_path: Path, value: object) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(ws.state, cast("bytes", value), expected_file_sha256=PIN_A.hash)
        assert _rows(ws.state) == []


def test_identity_is_exact_and_versions_are_opaque(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    identities = ("é", "e\u0301")
    versions = ("01", "1", "v-next")
    with open_workspace(home, writable=True) as ws:
        for identity in identities:
            for version in versions:
                raw = _raw(id=identity, version=version)
                pin = register_convention(
                    ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
                )
                assert (pin.id, pin.version) == (identity, version)
                assert json.loads(read_convention(ws.state, pin))["id"] == identity
        assert len(_rows(ws.state)) == len(identities) * len(versions)


def test_exact_size_limit_is_readable(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    empty = _raw(kind="cost", payload={"schema": "s", "text": ""})
    raw = empty.replace(b'"text":""', b'"text":"' + b"x" * (1024 * 1024 - len(empty)) + b'"')
    assert len(raw) == 1024 * 1024
    with open_workspace(home, writable=True) as ws:
        pin = register_convention(
            ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
        returned = read_convention(ws.state, pin)
        assert len(returned) == len(raw)
        assert json.loads(returned) == json.loads(raw)


@pytest.mark.parametrize(
    "raw",
    [A + b"\xff", A.replace(b"synthetic-capital", b"\\ud800"), b"[" * 2000 + b"0" + b"]" * 2000],
)
def test_malformed_conversion_preserves_cause(tmp_path: Path, raw: bytes) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError) as caught:
            register_convention(ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest())
        assert caught.value.__cause__ is not None
        assert _rows(ws.state) == []


def test_read_is_select_only(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=PIN_A.hash)
    with open_workspace(home) as ws:
        assert ws.state.execute("PRAGMA query_only").fetchone()[0] == 1
        before = _rows(ws.state)
        changes = ws.state.total_changes
        trace: list[str] = []
        ws.state.set_authorizer(_select_only)
        ws.state.set_trace_callback(trace.append)
        assert read_convention(ws.state, PIN_A) == A
        for pin in (
            replace(PIN_A, hash="0" * 64),
            replace(PIN_A, version="missing"),
            replace(PIN_A, id="missing"),
        ):
            with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
                read_convention(ws.state, pin)
        assert ws.state.total_changes == changes
        assert _rows(ws.state) == before
        assert not ws.state.in_transaction
        assert trace
        assert all(sql.startswith("SELECT ") for sql in trace)
    with open_workspace(home) as ws:
        before = _rows(ws.state)
        with pytest.raises(sqlite3.OperationalError):
            register_convention(ws.state, B, expected_file_sha256=PIN_B.hash)
        assert _rows(ws.state) == before
        assert ws.state.total_changes == 0


def test_missing_table_does_not_install(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        before = list(ws.strategies.execute("SELECT * FROM sqlite_schema"))
        with pytest.raises(sqlite3.OperationalError):
            read_convention(ws.strategies, PIN_A)
        assert list(ws.strategies.execute("SELECT * FROM sqlite_schema")) == before
    file = tmp_path / "empty.sqlite3"
    connection = sqlite3.connect(file)
    connection.close()
    before_bytes = file.read_bytes()
    connection = sqlite3.connect(file.as_uri() + "?mode=ro", uri=True)
    try:
        connection.set_authorizer(_select_only)
        with pytest.raises(sqlite3.OperationalError):
            read_convention(connection, PIN_A)
        assert list(connection.execute("SELECT * FROM sqlite_schema")) == []
    finally:
        connection.close()
    assert file.read_bytes() == before_bytes
    with open_workspace(home, writable=True, strategy_write=True) as ws:
        assert ws.strategies is not None
        before = list(ws.strategies.execute("SELECT * FROM sqlite_schema"))
        with pytest.raises(sqlite3.OperationalError):
            register_convention(ws.strategies, A, expected_file_sha256=PIN_A.hash)
        assert list(ws.strategies.execute("SELECT * FROM sqlite_schema")) == before
        assert ws.strategies.total_changes == 0


def _outer_rollback(connection: sqlite3.Connection) -> None:
    with atomic(connection):
        register_convention(connection, B, expected_file_sha256=PIN_B.hash)
        assert connection.in_transaction
        assert read_convention(connection, PIN_B) == B
        changed = A.replace(b'"price_basis":"capital"', b'"price_basis":"total_return"')
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            register_convention(
                connection, changed, expected_file_sha256=hashlib.sha256(changed).hexdigest()
            )
        assert connection.in_transaction
        assert read_convention(connection, PIN_B) == B
        raise RuntimeError("outer caller rolls back")


def test_nested_rollback_preserves_transaction_owner(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=PIN_A.hash)
        with pytest.raises(RuntimeError):
            _outer_rollback(ws.state)
        assert not ws.state.in_transaction
        assert read_convention(ws.state, PIN_A) == A
        with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
            read_convention(ws.state, PIN_B)


@pytest.mark.parametrize(
    "sql", ["UPDATE conventions SET content_hash=content_hash", "DELETE FROM conventions"]
)
def test_shipped_immutability_triggers_remain_enforced(tmp_path: Path, sql: str) -> None:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True) as ws:
        register_convention(ws.state, A, expected_file_sha256=PIN_A.hash)
        with pytest.raises(sqlite3.IntegrityError) as caught:
            ws.state.execute(sql)
        ws.state.rollback()
        assert caught.value.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_TRIGGER
        assert read_convention(ws.state, PIN_A) == A
