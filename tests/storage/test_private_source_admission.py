"""Offline regressions at the private source-consumption boundary."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from typing import Literal, assert_never

import pytest

from aegis_alpha.storage import raw, source_library
from aegis_alpha.storage.workspace import initialize, open_workspace


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE sample(value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('synthetic')")
        connection.commit()
    path.chmod(0o600)
    return path


@pytest.mark.parametrize("git_file", [False, True])
def test_import_rejects_source_when_inside_checkout(source: Path, *, git_file: bool) -> None:
    # Given a private source in a checkout or linked-worktree marker.
    home = source.parent / "home"
    initialize(home)
    checkout = source.parent / "checkout"
    checkout.mkdir(mode=0o700)
    marker = checkout / ".git"
    if git_file:
        marker.write_text("gitdir: synthetic-unused\n")
    else:
        marker.mkdir()
    moved = source.replace(checkout / source.name)
    original = moved.read_bytes()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When importing Git-contained bytes, reject before preparing an intent.
        with pytest.raises(ValueError, match="Git checkout"):
            source_library.import_sqlite(
                workspace, moved, "rejected", hashlib.sha256(original).hexdigest()
            )
        # Then neither a publication nor a pending intent escapes admission.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
        assert source_library.list_sources(workspace) == []
        assert moved.read_bytes() == original


def test_archive_rejects_source_when_inside_checkout(source: Path) -> None:
    # Given independent private source and raw directories, with a Git source marker.
    root = source.parent / "raw"
    root.mkdir(mode=0o700)
    checkout = source.parent / "checkout"
    checkout.mkdir(mode=0o700)
    (checkout / ".git").mkdir()
    moved = source.replace(checkout / source.name)
    # When archival attempts to admit that source.
    with pytest.raises(ValueError, match="Git checkout"):
        raw.put_raw_file(root, moved)
    # Then raw storage contains no publication or temporary bytes.
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("mutation", ["private-replacement", "public-replacement", "mode"])
def test_archive_rejects_source_when_admission_changes(
    source: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Literal["private-replacement", "public-replacement", "mode"],
) -> None:
    # Given immutable archived bytes and a deterministic post-admission mutation hook.
    root = source.parent / "raw"
    root.mkdir(mode=0o700)
    original = source.read_bytes()
    raw.put_raw(root, original)
    preserved = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    replacement = source.with_name("replacement")
    replacement.write_bytes(b"unadmitted synthetic bytes")
    replacement.chmod(0o600 if mutation == "private-replacement" else 0o644)
    admit = raw.private_file

    def mutate(path: Path) -> os.stat_result:
        info = admit(path)
        match mutation:
            case "mode":
                path.chmod(0o644)
            case "private-replacement" | "public-replacement":
                replacement.replace(path)
            case unreachable:
                assert_never(unreachable)
        return info

    monkeypatch.setattr(raw, "private_file", mutate)
    # When the reader would open different bytes or permissions from those admitted.
    with pytest.raises(ValueError, match="changed before copying"):
        raw.put_raw_file(root, source)
    # Then existing immutable bytes survive without new or partial archive files.
    assert {path: path.read_bytes() for path in root.rglob("*") if path.is_file()} == preserved


def test_import_rejects_before_intent_when_admitted_mode_changes(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a private SQLite file made public immediately after pathname admission.
    home = source.parent / "home"
    initialize(home)
    original = source.read_bytes()
    admit = source_library.private_file

    def expose(path: Path) -> os.stat_result:
        info = admit(path)
        path.chmod(0o644)
        return info

    monkeypatch.setattr(source_library, "private_file", expose)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When import attempts to consume the now non-private descriptor.
        with pytest.raises(ValueError, match="changed before import"):
            source_library.import_sqlite(
                workspace, source, "rejected", hashlib.sha256(original).hexdigest()
            )
        # Then rejection precedes even a prepared intent or temporary-image residue.
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
        assert source_library.list_sources(workspace) == []
        assert list(workspace.paths.runtime.glob("aas-sqlite-*")) == []
        assert source.read_bytes() == original


@pytest.mark.parametrize("restriction", ["mode", "owner", "symlink", "hardlink"])
def test_private_admission_rejects_when_existing_policy_fails(
    source: Path,
    monkeypatch: pytest.MonkeyPatch,
    restriction: Literal["mode", "owner", "symlink", "hardlink"],
) -> None:
    # Given a source violating an existing rule; a synthetic UID models foreign ownership.
    original = source.read_bytes()
    candidate = source
    match restriction:
        case "mode":
            source.chmod(0o644)
        case "owner":
            owner = os.getuid()
            monkeypatch.setattr(os, "getuid", lambda: owner + 1)
        case "symlink":
            candidate = source.with_name("alias")
            candidate.symlink_to(source)
        case "hardlink":
            source.with_name("alias").hardlink_to(source)
        case unreachable:
            assert_never(unreachable)
    # When the source admission function encounters that invalid input.
    with pytest.raises(ValueError, match="private, owned and not linked"):
        raw.private_file(candidate)
    # Then rejection leaves the source bytes untouched.
    assert source.read_bytes() == original


def test_private_source_works_when_imported_and_archived(source: Path) -> None:
    # Given an ordinary private snapshot and independently byte-derived provenance.
    home = source.parent / "home"
    initialize(home)
    original = source.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # When the real import and archive APIs consume that snapshot.
        result = source_library.import_sqlite(workspace, source, "accepted", digest)
        relative, archived_digest, size = raw.put_raw_file(workspace.paths.raw, source)
        # Then expected rows and exact hash-addressed bytes are published.
        assert result["reused"] is False
        assert source_library.read_table(workspace, "accepted", "sample")["rows"] == [
            {"value": "synthetic"}
        ]
        assert (archived_digest, size) == (digest, len(original))
        assert (workspace.paths.raw / relative).read_bytes() == original


def test_cli_rejects_source_when_inside_checkout(source: Path) -> None:
    # Given a real installed CLI and synthetic Git-contained SQLite source.
    home = source.parent / "home"
    initialize(home)
    checkout = source.parent / "checkout"
    checkout.mkdir(mode=0o700)
    (checkout / ".git").mkdir()
    moved = source.replace(checkout / source.name)
    digest = hashlib.sha256(moved.read_bytes()).hexdigest()
    # When source-import crosses the real subprocess CLI surface.
    result = subprocess.run(  # noqa: S603 -- installed CLI, offline synthetic paths
        [
            str(Path(sys.executable).parent / "aas"),
            "db",
            "--home",
            str(home),
            "source-import",
            str(moved),
            "--id",
            "rejected",
            "--sha256",
            digest,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Then the CLI fails and a fresh workspace has no publication intent.
    assert result.returncode != 0, result.stdout
    with open_workspace(home) as workspace:
        assert workspace.state.execute("SELECT * FROM storage_operations").fetchall() == []
        assert source_library.list_sources(workspace) == []
