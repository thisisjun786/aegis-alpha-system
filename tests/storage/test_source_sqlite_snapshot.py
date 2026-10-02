"""Synthetic byte-pinned SQLite imports against an uncooperative WAL writer."""

from __future__ import annotations

import hashlib
import json
import os
import selectors
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.storage import source_library
from aegis_alpha.storage.workspace import initialize, open_workspace

if TYPE_CHECKING:
    from _hashlib import HASH
    from io import BufferedIOBase

_WRITER = """
import json, sqlite3, sys
from contextlib import closing
sys.stdin.readline()
with closing(sqlite3.connect(sys.argv[1])) as connection:
    connection.execute("INSERT INTO sample VALUES ('wal-only')")
    connection.commit()
    print(json.dumps(connection.execute('SELECT value FROM sample').fetchall()), flush=True)
    sys.stdin.readline()
"""
_PRIVATE_DIRECTORY = 0o700


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE sample(value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('hashed')")
        connection.commit()
    path.chmod(0o600)
    return path


@contextmanager
def _wal_writer(source: Path) -> Iterator[None]:
    # Subscribe before allowing the independent process to open or modify the source.
    with subprocess.Popen(  # noqa: S603 -- fixed synthetic offline child
        [sys.executable, "-c", _WRITER, str(source)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as writer:
        assert writer.stdout is not None
        assert writer.stdin is not None
        try:
            with selectors.DefaultSelector() as ready:
                ready.register(writer.stdout, selectors.EVENT_READ)
                writer.stdin.write("write\n")
                writer.stdin.flush()
                assert ready.select(timeout=15), "writer did not signal its commit"
                assert json.loads(writer.stdout.readline()) == [["hashed"], ["wal-only"]]
            yield
        finally:
            try:
                stdout, stderr = writer.communicate(input="close\n", timeout=15)
            except subprocess.TimeoutExpired:
                writer.kill()
                writer.communicate()
                raise
            assert writer.returncode == 0, (stdout, stderr)


def test_import_excludes_wal_rows_when_writer_commits_after_hash(
    tmp_path: Path, source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a closed WAL-mode main file containing only the independently expected row.
    home = tmp_path / "home"
    initialize(home)
    original = source.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    file_digest = hashlib.file_digest
    artifacts: dict[Path, bytes] = {}
    with (
        open_workspace(home, writable=True, strategy_write=True) as workspace,
        ExitStack() as writers,
    ):

        def after_hash(handle: BufferedIOBase, algorithm: str) -> HASH:
            result = file_digest(handle, algorithm)
            writers.enter_context(_wal_writer(source))
            assert source.read_bytes() == original
            artifacts.update((p, p.read_bytes()) for p in tmp_path.glob("source.sqlite3*"))
            return result

        monkeypatch.setattr(hashlib, "file_digest", after_hash)
        # When an ordinary independent SQLite writer commits before the import opens SQL.
        source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then published rows belong only to the declared main bytes, not the new WAL.
        assert source_library.read_table(workspace, "pinned", "sample")["rows"] == [
            {"value": "hashed"}
        ]
        assert source_library.list_sources(workspace)[0]["sha256"] == digest
        assert source.read_bytes() == original
        assert {p: p.read_bytes() for p in artifacts} == artifacts


@pytest.mark.parametrize("fail", [False, True])
def test_snapshot_connection_lifetime_when_import_succeeds_or_fails(
    source: Path, monkeypatch: pytest.MonkeyPatch, *, fail: bool
) -> None:
    # Given a closed source and an observer of the actual SQL reconciliation connection.
    home = source.parent / "home"
    initialize(home)
    original = source.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    reconcile = source_library.sqlite_digest
    images: list[tuple[Path, sqlite3.Connection]] = []

    def observe(cursor: sqlite3.Cursor) -> tuple[int, str]:
        connection = cursor.connection
        path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
        if path != home / "strategies.sqlite3":
            assert path != source
            assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
            assert path.parent.stat().st_mode & 0o777 == _PRIVATE_DIRECTORY
            images.append((path, connection))
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                connection.execute("INSERT INTO sample VALUES ('forbidden')")
            if fail:
                raise RuntimeError("synthetic reconciliation failure")
        return reconcile(cursor)

    monkeypatch.setattr(source_library, "sqlite_digest", observe)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the real importer consumes the image, including an injected failure path.
        if fail:
            with pytest.raises(RuntimeError, match="synthetic reconciliation failure"):
                source_library.import_sqlite(workspace, source, "pinned", digest)
            assert source_library.list_sources(workspace) == []
        else:
            source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then the SQL handle is closed before its private image is removed, on either path.
        assert images
        for path, connection in images:
            assert not path.parent.exists()
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
        assert source.read_bytes() == original


@pytest.mark.parametrize("changed", [False, True])
def test_rejects_hash_mismatch_when_declared_bytes_differ(source: Path, *, changed: bool) -> None:
    # Given either an incorrect hash or changed content under a previously correct hash.
    home = source.parent / "home"
    initialize(home)
    digest = hashlib.sha256(source.read_bytes()).hexdigest() if changed else "0" * 64
    if changed:
        with closing(sqlite3.connect(source)) as writer:
            writer.execute("INSERT INTO sample VALUES ('changed')")
            writer.commit()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the declared source bytes do not match.
        with pytest.raises(ValueError, match="sha256 mismatch"):
            source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then there is no publication intent to recover or complete.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []


@pytest.mark.parametrize("wal", [False, True])
def test_rejects_active_sidecars_when_writer_precedes_admission(source: Path, *, wal: bool) -> None:
    # Given a real writer holding a WAL or rollback journal open before admission.
    home = source.parent / "home"
    initialize(home)
    with closing(sqlite3.connect(source)) as writer:
        if not wal:
            writer.execute("PRAGMA journal_mode=DELETE")
        writer.execute("INSERT INTO sample VALUES ('uncommitted')")
        sidecar = Path(str(source) + ("-wal" if wal else "-journal"))
        assert sidecar.exists()
        artifacts = {p: p.read_bytes() for p in source.parent.glob("source.sqlite3*")}
        with open_workspace(home, writable=True, strategy_write=True) as workspace:
            # When the importer is given an active source rather than a closed backup.
            with pytest.raises(ValueError, match="closed SQLite backup"):
                source_library.import_sqlite(workspace, source, "pinned", "0" * 64)
            # Then admission neither publishes nor repairs any source artifact.
            assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
            assert {p: p.read_bytes() for p in artifacts} == artifacts


def test_rejects_replacement_when_file_identity_changes_before_open(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given an identical-byte replacement between private-file admission and descriptor open.
    home = source.parent / "home"
    initialize(home)
    replacement = source.with_name("replacement.sqlite3")
    replacement.write_bytes(source.read_bytes())
    replacement.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    admit = source_library.private_file

    def replace(path: Path) -> os.stat_result:
        info = admit(path)
        replacement.replace(path)
        return info

    monkeypatch.setattr(source_library, "private_file", replace)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When copying would consume a different inode than was admitted.
        with pytest.raises(ValueError, match="changed before import"):
            source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then identical bytes do not authorize silently adopting the substituted file.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []


def test_writer_error_aborts_import_when_hash_hook_fails(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a failing independent writer at the same post-hash boundary as the WAL race.
    home = source.parent / "home"
    initialize(home)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    file_digest = hashlib.file_digest

    def fail_writer(handle: BufferedIOBase, algorithm: str) -> HASH:
        result = file_digest(handle, algorithm)
        subprocess.run(  # noqa: S603 -- fixed synthetic writer, no provider access
            [sys.executable, "-c", _WRITER, str(source.parent / "empty.sqlite3")],
            check=True,
            input="write\n",
            capture_output=True,
            text=True,
            timeout=15,
        )
        return result

    monkeypatch.setattr(hashlib, "file_digest", fail_writer)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the independent writer fails, its process error propagates, not a fake signal.
        with pytest.raises(subprocess.CalledProcessError) as error:
            source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then no completed publication can hide the failed handshake.
        assert "no such table: sample" in error.value.stderr
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []


def test_source_read_in_fresh_process_when_clean_import_completes(source: Path) -> None:
    # Given a clean snapshot published through the installed CLI in a separate process.
    home = source.parent / "home"
    initialize(home)
    cli = [str(Path(sys.executable).parent / "aas"), "db", "--home", str(home)]
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    subprocess.run(  # noqa: S603 -- installed CLI and synthetic paths only
        [*cli, "source-import", str(source), "--id", "pinned", "--sha256", digest],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    # When a fresh process reopens the committed native source via its real CLI surface.
    read = subprocess.run(  # noqa: S603 -- installed CLI and synthetic paths only
        [*cli, "source-read", "--source", "pinned", "--table", "sample"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Then the independent expected source row survives process and connection lifetimes.
    assert json.loads(read.stdout)["rows"] == [{"value": "hashed"}]


def test_rejects_main_change_when_writer_checkpoints_after_hash(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a writer that changes the main bytes after they have been copied and hashed.
    home = source.parent / "home"
    initialize(home)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    file_digest = hashlib.file_digest

    def checkpoint(handle: BufferedIOBase, algorithm: str) -> HASH:
        result = file_digest(handle, algorithm)
        with closing(sqlite3.connect(source)) as writer:
            writer.execute("INSERT INTO sample VALUES ('checkpointed')")
            writer.commit()
        return result

    monkeypatch.setattr(hashlib, "file_digest", checkpoint)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the original file changes, retain the existing fail-closed import contract.
        with pytest.raises(ValueError, match="changed during import"):
            source_library.import_sqlite(workspace, source, "pinned", digest)
        # Then rollback leaves no visible source or recoverable target marker.
        assert source_library.list_sources(workspace) == []
        assert source_library.verify_sources(workspace) == {"sources": 0, "tables": 0, "rows": 0}
