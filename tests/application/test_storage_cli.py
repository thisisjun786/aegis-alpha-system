from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

from aegis_alpha.application import storage_cli
from aegis_alpha.storage.input_pins import register_convention
from aegis_alpha.storage.strategies import load_strategy
from aegis_alpha.storage.workspace import open_workspace
from tests.engine.engine_support import contract, raw_bundle
from tests.engine.test_requirements import rich_contract, scoring_contract
from tests.storage.test_publication import document
from tests.storage.test_strategy_requirements import A, B

_ROOT = Path(__file__).resolve().parents[2]


def run_cli(*args: str, home: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )


def test_configured_budget_verifies_backs_up_restores_and_installs_large_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    assert run_cli("init", home=home).returncode == 0
    source = tmp_path / "large-import.json"
    raw = document() + b" " * (4 * 1024 * 1024)
    source.write_bytes(raw)
    imported = run_cli(
        "data", "import", str(source), "--sha256", hashlib.sha256(raw).hexdigest(), home=home
    )
    assert imported.returncode == 0, imported.stderr
    for name in ("AAS_CPU_LIMIT", "AAS_HOST_CPU_LIMIT"):
        monkeypatch.setenv(name, "1")
    for name in ("AAS_MEMORY_LIMIT_BYTES", "AAS_HOST_MEMORY_LIMIT_BYTES"):
        monkeypatch.setenv(name, str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))
    checked = run_cli("db", "verify", home=home)
    assert checked.returncode == 0, checked.stderr
    verification = json.loads(checked.stdout)
    archive = tmp_path / "archive"
    saved = run_cli("db", "backup", "--output", str(archive), home=home)
    assert saved.returncode == 0, saved.stderr
    restored = run_cli(
        "--home", str(tmp_path / "restored"), "db", "restore", "--backup", str(archive), home=home
    )
    assert restored.returncode == 0, restored.stderr
    assert json.loads(restored.stdout)["verification"] == verification
    installed = run_cli(
        "db", "run-install", "--backup-output", str(tmp_path / "before-run-schema"), home=home
    )
    assert installed.returncode == 0, installed.stderr
    assert json.loads(installed.stdout)["state"] == "complete"


@pytest.mark.parametrize("command", ["verify", "backup", "restore", "run-install"])
def test_maintenance_compute_lock_alias_rejected_before_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    home = tmp_path / "home"
    assert run_cli("init", home=home).returncode == 0
    target = tmp_path / "new-home" if command == "restore" else home
    for name in ("AAS_CPU_LIMIT", "AAS_HOST_CPU_LIMIT"):
        monkeypatch.setenv(name, "1")
    for name in ("AAS_MEMORY_LIMIT_BYTES", "AAS_HOST_MEMORY_LIMIT_BYTES"):
        monkeypatch.setenv(name, str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(target / ".storage.lock"))
    args = ["--home", str(target), "db", command]
    if command == "restore":
        args.extend(["--backup", str(tmp_path / "absent-archive")])
    result = run_cli(*args, home=home)
    assert result.returncode == 1
    assert "compute lock aliases" in result.stderr
    if command == "restore":
        assert not target.exists()
    assert not any((home / "backups").iterdir())


def test_init_doctor_repeat_without_database_server(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    result = run_cli("init", home=home)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["initialized"] is True
    result = run_cli("doctor", home=home)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["home"] == str(home)
    assert report["strategy_versions"] == 0
    assert report["docker_required"] is False
    assert run_cli("init", home=home).returncode == 0
    assert run_cli("strategy", "list", home=home).returncode == 0


def test_uninitialized_doctor_no_autoinstall(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    result = run_cli("doctor", home=home)
    assert result.returncode == 1
    assert "error" in json.loads(result.stderr)
    assert not home.exists()


def test_explicit_nested_home_wins(tmp_path: Path) -> None:
    env_home, explicit = tmp_path / "env", tmp_path / "explicit"
    result = run_cli("init", "--home", str(explicit), home=env_home)
    assert result.returncode == 0, result.stderr
    assert explicit.exists()
    assert not env_home.exists()


def test_strategy_lineage_cli_roundtrip(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "synthetic.json"
    payload = raw_bundle(contract())
    digest = hashlib.sha256(payload).hexdigest()
    source.write_bytes(payload)
    assert run_cli("init", home=home).returncode == 0
    arguments = (
        "strategy",
        "import",
        str(source),
        "--id",
        "synthetic-probe",
        "--version",
        "1",
        "--sha256",
        digest,
        "--parent-id",
        "synthetic-parent",
        "--parent-version",
        "7",
        "--change-kind",
        "derived",
        "--reason",
        " synthetic reason\n",
    )
    result = run_cli(*arguments, home=home)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["imported"] is True
    again = run_cli(*arguments, home=home)
    assert again.returncode == 0, again.stderr
    assert again.stdout == result.stdout
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        rows = workspace.strategies.execute("SELECT * FROM strategy_lineage").fetchall()
        assert [tuple(row) for row in rows] == [
            (
                "synthetic-probe",
                "1",
                "synthetic-parent",
                "7",
                "derived",
                " synthetic reason\n",
                hashlib.sha256(b'" synthetic reason\\n"').hexdigest(),
                "unresolved",
            )
        ]
        with pytest.raises(ValueError, match="unresolved parent"):
            load_strategy(workspace.strategies, "synthetic-probe", "1", digest)
        assert [
            row[0] for row in workspace.state.execute("SELECT phase FROM storage_operations")
        ] == ["COMPLETED"]


@pytest.mark.parametrize("mask", range(1, 15))
def test_strategy_lineage_cli_rejects_partial_flags(tmp_path: Path, mask: int) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "synthetic.json"
    payload = raw_bundle(contract())
    source.write_bytes(payload)
    assert run_cli("init", home=home).returncode == 0
    options = [
        ("--parent-id", "parent"),
        ("--parent-version", "1"),
        ("--change-kind", "derived"),
        ("--reason", "synthetic"),
    ]
    flags = [value for index, pair in enumerate(options) if mask & (1 << index) for value in pair]
    result = run_cli(
        "strategy",
        "import",
        str(source),
        "--id",
        "synthetic-probe",
        "--version",
        "1",
        "--sha256",
        hashlib.sha256(payload).hexdigest(),
        *flags,
        home=home,
    )
    assert result.returncode == 1, result.stderr
    assert "error" in json.loads(result.stderr)
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        assert (
            workspace.strategies.execute("SELECT count(*) FROM strategy_versions").fetchone()[0]
            == 0
        )
        assert workspace.state.execute("SELECT count(*) FROM storage_operations").fetchone()[0] == 0


def test_invalid_scoring_cli_import_and_show_preserve_evidence(
    show_home: tuple[Path, dict[str, dict[str, str]]],
) -> None:
    home, receipts = show_home
    payload = json.loads(
        raw_bundle(scoring_contract("offensive", {"method": "return_rate", "horizon": 12}))
    )
    payload["bundle_version"] = "2"
    raw = json.dumps(payload).encode()
    source = home.parent / "invalid.json"
    source.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        before = (tuple(workspace.state.iterdump()), tuple(workspace.strategies.iterdump()))
    result = run_cli(
        "strategy",
        "import",
        str(source),
        "--id",
        "synthetic-probe",
        "--version",
        "2",
        "--sha256",
        digest,
        home=home,
    )
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert isinstance(json.loads(result.stderr)["error"], str)
    source.unlink()
    result = run_cli(
        "strategy",
        "show",
        "--id",
        "synthetic-probe",
        "--version",
        "2",
        "--sha256",
        digest,
        home=home,
    )
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert isinstance(json.loads(result.stderr)["error"], str)
    assert run_cli(*show_arguments(receipts["synthetic-probe"]), home=home).returncode == 0
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        assert (tuple(workspace.state.iterdump()), tuple(workspace.strategies.iterdump())) == before


def test_conflicting_strategy_bytes_leave_next_backup_healthy(tmp_path: Path) -> None:
    home = tmp_path / "aas"
    source = tmp_path / "synthetic.json"
    payload = raw_bundle(contract())
    source.write_bytes(payload)
    assert run_cli("init", home=home).returncode == 0
    arguments = ("strategy", "import", str(source), "--id", "synthetic-probe", "--version", "1")
    result = run_cli(*arguments, "--sha256", hashlib.sha256(payload).hexdigest(), home=home)
    assert result.returncode == 0, result.stderr
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        before = (tuple(workspace.state.iterdump()), tuple(workspace.strategies.iterdump()))
    # Valid JSON, identical parsed contract, but genuinely different pinned bytes.
    payload += b"\n"
    source.write_bytes(payload)
    result = run_cli(*arguments, "--sha256", hashlib.sha256(payload).hexdigest(), home=home)
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert isinstance(json.loads(result.stderr)["error"], str)
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        assert (tuple(workspace.state.iterdump()), tuple(workspace.strategies.iterdump())) == before
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM storage_operations WHERE phase='PREPARED'"
            ).fetchone()[0]
            == 0
        )
    output = tmp_path / "backup"
    result = run_cli("db", "backup", "--output", str(output), home=home)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["backed_up"] is True
    manifest = json.loads((output / "backup.json").read_text())
    assert manifest["complete"] is True
    assert manifest["logical"]["pending_operations"] == 0
    with open_workspace(home) as workspace:
        assert workspace.strategies is not None
        assert (tuple(workspace.state.iterdump()), tuple(workspace.strategies.iterdump())) == before


def test_restore_never_defaults_over_current_home(tmp_path: Path) -> None:
    result = run_cli("db", "restore", "--backup", str(tmp_path / "missing"), home=tmp_path / "aas")
    assert result.returncode == 1
    assert "explicit --home" in json.loads(result.stderr)["error"]


@pytest.mark.parametrize("name", ["init", "doctor", "db", "data", "strategy"])
def test_native_help_without_site_packages(name: str) -> None:
    result = subprocess.run(  # noqa: S603 -- verify dependency-free argument parser
        [sys.executable, "-S", "-m", "aegis_alpha", name, "--help"],
        env={**os.environ, "PYTHONPATH": str(_ROOT / "src")},
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_source_catalog_cli_roundtrip(tmp_path: Path) -> None:
    import hashlib  # noqa: PLC0415 -- synthetic source fixture
    import sqlite3  # noqa: PLC0415

    home = tmp_path / "aas"
    source = tmp_path / "snapshot"
    with sqlite3.connect(source) as origin:
        origin.execute("CREATE TABLE catalog (name TEXT, value INTEGER)")
        origin.execute("INSERT INTO catalog VALUES ('synthetic',7)")
    source.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    assert run_cli("init", home=home).returncode == 0
    result = run_cli(
        "db", "source-import", str(source), "--id", "synthetic", "--sha256", digest, home=home
    )
    assert result.returncode == 0, result.stderr
    result = run_cli("db", "sources", home=home)
    assert json.loads(result.stdout)["sources"][0]["source_id"] == "synthetic"
    result = run_cli(
        "db",
        "source-read",
        "--source",
        "synthetic",
        "--table",
        "catalog",
        "--limit",
        "1",
        home=home,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["rows"] == [{"name": "synthetic", "value": 7}]
    assert json.loads(run_cli("strategy", "list", home=home).stdout)["strategies"] == []
    result = run_cli("db", "verify", home=home)
    assert json.loads(result.stdout)["source_library"] == {"sources": 1, "tables": 1, "rows": 1}


@pytest.mark.parametrize(
    "arguments",
    [
        ("sources",),
        ("source-tables", "--source", "source"),
        ("source-read", "--source", "source", "--table", "table"),
    ],
)
def test_source_reads_require_strategy_store_at_admission(
    tmp_path: Path, arguments: tuple[str, ...]
) -> None:
    home = tmp_path / "aas"
    assert run_cli("init", home=home).returncode == 0
    (home / "strategies.sqlite3").rename(home / "held-strategies.sqlite3")
    result = run_cli("db", *arguments, home=home)
    assert result.returncode == 1
    assert "error" in json.loads(result.stderr)
    assert "strategy database is missing" in result.stderr
    assert "Traceback" not in result.stderr
    assert run_cli("doctor", home=home).returncode == 0


@pytest.fixture
def show_home(tmp_path: Path) -> tuple[Path, dict[str, dict[str, str]]]:
    home = tmp_path / "aas"
    assert run_cli("init", home=home).returncode == 0
    receipts = {}
    for identity, value in (("synthetic-probe", contract()), ("synthetic-rich", rich_contract())):
        document = json.loads(raw_bundle(value))
        document["bundle_id"] = identity
        payload = json.dumps(document).encode()
        source = tmp_path / "bundle.json"
        source.write_bytes(payload)
        result = run_cli(
            "strategy",
            "import",
            str(source),
            "--id",
            identity,
            "--version",
            "1",
            "--sha256",
            hashlib.sha256(payload).hexdigest(),
            home=home,
        )
        assert result.returncode == 0, result.stderr
        receipts[identity] = json.loads(result.stdout)
        source.unlink()
    return home, receipts


def show_arguments(receipt: dict[str, str]) -> tuple[str, ...]:
    return (
        "strategy",
        "show",
        "--id",
        receipt["strategy_id"],
        "--version",
        receipt["version"],
        "--sha256",
        receipt["raw_sha256"],
    )


@pytest.mark.parametrize(
    ("identity", "prices", "cash", "minimum"),
    [
        ("synthetic-probe", ["ASSET_A", "ASSET_B", "REF_X"], ["CASH_X"], 3),
        ("synthetic-rich", ["ASSET_Z", "CANARY_ON", "REF_Y"], ["CASH_Y"], 8),
    ],
)
def test_strategy_show_definition(
    show_home: tuple[Path, dict[str, dict[str, str]]],
    identity: str,
    prices: list[str],
    cash: list[str],
    minimum: int,
) -> None:
    home, receipts = show_home
    receipt = receipts[identity]
    result = run_cli(*show_arguments(receipt), home=home)
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert (definition["bundle_id"], definition["bundle_version"]) == (identity, "1")
    assert definition["source_sha256"] == receipt["raw_sha256"]
    assert definition["contract_sha256"] == receipt["contract_sha256"]
    assert definition["price_asset_ids"] == prices
    assert definition["cash_asset_ids"] == cash
    assert definition["input_requirements"][0]["minimum_observations"] == minimum
    assert definition["input_requirements"][0]["basis"] is None
    assert definition["unresolved_convention_roles"] == ["calendar", "basis", "cost", "execution"]
    assert definition["executable"] is False
    assert {"raw_bundle", "pack", "home", "paths"}.isdisjoint(definition)
    assert str(home) not in result.stdout
    assert run_cli(*show_arguments(receipt), home=home).stdout == result.stdout


@pytest.mark.parametrize(
    ("identity", "raw", "basis", "exit_code"),
    [
        ("synthetic-probe", A, "capital", 0),
        ("synthetic-probe", B, "total_return", 0),
        ("synthetic-rich", A, "capital", 0),
        ("synthetic-rich", B, "total_return", 1),
    ],
)
def test_strategy_show_registered_basis(
    show_home: tuple[Path, dict[str, dict[str, str]]],
    identity: str,
    raw: bytes,
    basis: str,
    exit_code: int,
) -> None:
    home, receipts = show_home
    with open_workspace(home, writable=True) as workspace:
        pin = register_convention(
            workspace.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
    document = home.parent / "requirements.json"
    payload = json.dumps(
        {
            "schema_version": "aas-execution-requirements-v1",
            "convention_bindings": [asdict(pin)],
        }
    ).encode()
    document.write_bytes(payload)
    before = {path.name: path.read_bytes() for path in home.glob("*.sqlite3")}
    result = run_cli(
        *show_arguments(receipts[identity]),
        "--requirements",
        str(document),
        "--requirements-sha256",
        hashlib.sha256(payload).hexdigest(),
        home=home,
    )
    assert result.returncode == exit_code, result.stderr
    if exit_code:
        assert result.stdout == ""
        assert isinstance(json.loads(result.stderr)["error"], str)
    else:
        definition = json.loads(result.stdout)
        assert definition["input_requirements"][0]["basis"] == basis
        assert definition["unresolved_convention_roles"] == ["calendar", "cost", "execution"]
        assert definition["executable"] is False
    assert {path.name: path.read_bytes() for path in home.glob("*.sqlite3")} == before


@pytest.mark.parametrize("failure", ["hash", "requirements-only", "hash-only", "lineage"])
def test_strategy_show_controlled_failure(
    show_home: tuple[Path, dict[str, dict[str, str]]],
    failure: str,
) -> None:
    home, receipts = show_home
    receipt = receipts["synthetic-probe"]
    extra = ()
    if failure == "lineage":
        source = home.parent / "child.json"
        document = json.loads(raw_bundle(contract()))
        document["bundle_id"] = "synthetic-child"
        payload = json.dumps(document).encode()
        source.write_bytes(payload)
        result = run_cli(
            "strategy",
            "import",
            str(source),
            "--id",
            "synthetic-child",
            "--version",
            "1",
            "--sha256",
            hashlib.sha256(payload).hexdigest(),
            "--parent-id",
            "absent-parent",
            "--parent-version",
            "7",
            "--change-kind",
            "derived",
            "--reason",
            "synthetic",
            home=home,
        )
        assert result.returncode == 0, result.stderr
        receipt = json.loads(result.stdout)
        source.unlink()
    elif failure == "hash":
        receipt = {**receipt, "raw_sha256": "0" * 64}
    elif failure == "requirements-only":
        extra = ("--requirements", str(home.parent / "missing.json"))
    else:
        extra = ("--requirements-sha256", "0" * 64)
    before = {path.name: path.read_bytes() for path in home.glob("*.sqlite3")}
    result = run_cli(*show_arguments(receipt), *extra, home=home)
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert isinstance(json.loads(result.stderr)["error"], str)
    assert str(home) not in result.stderr
    assert {path.name: path.read_bytes() for path in home.glob("*.sqlite3")} == before


_EMPTY_REQUIREMENTS = b'{"schema_version":"aas-execution-requirements-v1","convention_bindings":[]}'


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b"[]",
        b"null",
        b"true",
        b"\xff",
        b"\x00",
        b" " * (1024 * 1024 + 1),
        b"[" * 2000,
        _EMPTY_REQUIREMENTS.replace(b"v1", b"v2"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b"null"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b"{}"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b"[false]"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b"[{}]"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b"[NaN]"),
        _EMPTY_REQUIREMENTS.replace(b"[]", b'[],"unknown":0'),
        _EMPTY_REQUIREMENTS.replace(b"[]", b'[],"convention_bindings":[]'),
        b"\xef\xbb\xbf" + _EMPTY_REQUIREMENTS,
        *(
            _EMPTY_REQUIREMENTS.decode().encode(encoding)
            for encoding in (
                "utf-16",
                "utf-16-le",
                "utf-16-be",
                "utf-32",
                "utf-32-le",
                "utf-32-be",
            )
        ),
        *(
            _EMPTY_REQUIREMENTS.replace(b"[]", json.dumps([pin]).encode())
            for pin in (
                {"kind": "basis", "id": "a", "version": "1", "hash": "A" * 64},
                {"kind": "basis", "id": "a", "version": "latest", "hash": "0" * 64},
                {"kind": "basis", "id": 7, "version": "1", "hash": "0" * 64},
                {"kind": "other", "id": "a", "version": "1", "hash": "0" * 64},
                {"kind": "basis", "id": "a", "version": "1", "hash": "short"},
                {"kind": "basis", "id": "a", "version": "1", "hash": "0" * 64, "payload": {}},
            )
        ),
        _EMPTY_REQUIREMENTS.replace(
            b"[]",
            b'[{"kind":"basis","kind":"basis","id":"a","version":"1","hash":"' + b"0" * 64 + b'"}]',
        ),
    ],
    ids=lambda payload: hashlib.sha256(payload).hexdigest()[:12],
)
def test_execution_requirements_reject_malformed(tmp_path: Path, payload: bytes) -> None:
    source = tmp_path / "requirements.json"
    source.write_bytes(payload)
    with pytest.raises(ValueError, match="requirements"):
        storage_cli._execution_bindings(  # noqa: SLF001 -- strict external document boundary
            source, hashlib.sha256(payload).hexdigest()
        )


def test_strategy_show_rejects_fifo_with_writer(
    show_home: tuple[Path, dict[str, dict[str, str]]],
) -> None:
    home, receipts = show_home
    source = home.parent / "requirements.fifo"
    os.mkfifo(source)
    before = {path.name: path.read_bytes() for path in home.glob("*.sqlite3")}
    # The kernel open rendezvous synchronizes the real writer with the reader.
    # Valid bytes plus EOF make the old blocking reader succeed, not time out.
    writer_code = """
import json, os, stat, sys
fd = os.open(sys.argv[1], os.O_WRONLY)
try:
    count = os.write(fd, bytes.fromhex(sys.argv[2]))
    print(json.dumps({"fifo": stat.S_ISFIFO(os.fstat(fd).st_mode), "written": count}))
finally:
    os.close(fd)
"""
    writer = subprocess.Popen(  # noqa: S603 -- real FIFO writer, synthetic bytes
        [sys.executable, "-c", writer_code, str(source), _EMPTY_REQUIREMENTS.hex()],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        result = run_cli(
            *show_arguments(receipts["synthetic-probe"]),
            "--requirements",
            str(source),
            "--requirements-sha256",
            hashlib.sha256(_EMPTY_REQUIREMENTS).hexdigest(),
            home=home,
        )
        if result.returncode == 0:
            # A successful old reader must have consumed the genuine writer's EOF.
            out, err = writer.communicate(timeout=30)
            assert writer.returncode == 0, err
            assert json.loads(out) == {"fifo": True, "written": len(_EMPTY_REQUIREMENTS)}
    finally:
        if writer.poll() is None:
            writer.kill()
        writer.communicate(timeout=30)
        source.unlink()
    assert writer.returncode is not None
    assert run_cli("doctor", home=home).returncode == 0
    assert {path.name: path.read_bytes() for path in home.glob("*.sqlite3")} == before
    assert result.returncode == 1, result.stdout
    assert result.stdout == ""
    assert isinstance(json.loads(result.stderr)["error"], str)


@pytest.mark.parametrize(
    "kind", ["fifo", "directory", "socket", "symlink", "ancestor-symlink", "hardlink"]
)
def test_strategy_show_requirements_filesystem_admission(
    show_home: tuple[Path, dict[str, dict[str, str]]], kind: str
) -> None:
    home, receipts = show_home
    regular = home.parent / "requirements.json"
    regular.write_bytes(_EMPTY_REQUIREMENTS)
    source = home.parent / "alias"
    baseline = run_cli(*show_arguments(receipts["synthetic-probe"]), home=home)
    assert baseline.returncode == 0, baseline.stderr
    before = {path.name: path.read_bytes() for path in home.glob("*.sqlite3")}
    with socket.socket(socket.AF_UNIX) as listener:
        if kind == "fifo":
            os.mkfifo(source)  # Deliberately no writer; timeout is a failure, never proof.
        elif kind == "directory":
            source.mkdir()
        elif kind == "socket":
            listener.bind(str(source))
        elif kind == "symlink":
            source.symlink_to(regular)
        elif kind == "ancestor-symlink":
            source.symlink_to(home.parent, target_is_directory=True)
            source /= regular.name
        else:
            source.hardlink_to(regular)
        result = run_cli(
            *show_arguments(receipts["synthetic-probe"]),
            "--requirements",
            str(source),
            "--requirements-sha256",
            hashlib.sha256(_EMPTY_REQUIREMENTS).hexdigest(),
            home=home,
        )
    if kind == "hardlink":
        # External document readers allow hardlinks; do not invent a stricter policy.
        assert result.returncode == 0, result.stderr
        assert result.stdout == baseline.stdout
    else:
        assert result.returncode == 1, result.stderr
        assert result.stdout == ""
        assert isinstance(json.loads(result.stderr)["error"], str)
        assert str(home.parent) not in result.stderr
    assert run_cli("doctor", home=home).returncode == 0
    assert {path.name: path.read_bytes() for path in home.glob("*.sqlite3")} == before
    assert regular.read_bytes() == _EMPTY_REQUIREMENTS


def test_execution_requirements_exact_size_and_relative_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "requirements.json"
    payload = _EMPTY_REQUIREMENTS.ljust(1024 * 1024, b" ")
    source.write_bytes(payload)
    monkeypatch.chdir(tmp_path)
    assert (
        storage_cli._execution_bindings(  # noqa: SLF001 -- real bounded transport
            Path(source.name), hashlib.sha256(payload).hexdigest()
        )
        == ()
    )
    with pytest.raises(ValueError, match="requirements"):
        storage_cli._execution_bindings(  # noqa: SLF001
            source, hashlib.sha256(_EMPTY_REQUIREMENTS).hexdigest()
        )
    payload += b" "
    source.write_bytes(payload)
    with pytest.raises(ValueError, match="requirements"):
        storage_cli._execution_bindings(source, hashlib.sha256(payload).hexdigest())  # noqa: SLF001


def test_execution_requirements_empty_and_transport_hash(tmp_path: Path) -> None:
    source = tmp_path / "requirements.json"
    source.write_bytes(_EMPTY_REQUIREMENTS)
    digest = hashlib.sha256(_EMPTY_REQUIREMENTS).hexdigest()
    assert storage_cli._execution_bindings(None, None) is None  # noqa: SLF001
    assert storage_cli._execution_bindings(source, digest) == ()  # noqa: SLF001
    for invalid in ("0" * 64, digest.upper(), "short"):
        with pytest.raises(ValueError, match="requirements"):
            storage_cli._execution_bindings(source, invalid)  # noqa: SLF001
    source.unlink()
    with pytest.raises(ValueError, match="requirements"):
        storage_cli._execution_bindings(source, digest)  # noqa: SLF001
