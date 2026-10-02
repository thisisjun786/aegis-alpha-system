"""Owned SQLite connections and checksummed, versioned schemas."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

_COMMON_DDL = """
CREATE TABLE store_info (
    store_id TEXT PRIMARY KEY, installation_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version > 0), kind TEXT NOT NULL
) STRICT;
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, applied_at_us INTEGER NOT NULL
) STRICT;
"""


def connect(path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a private regular file without silently creating a missing read store."""
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    with DescriptorTree.open_path(path.parent) as tree:
        flags = os.O_RDONLY if read_only else os.O_RDWR | os.O_CREAT
        descriptor = os.open(
            path.name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=tree.descriptor
        )
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise ValueError("SQLite file must be private, owned and not linked")
            uri = path.absolute().as_uri() + ("?mode=ro" if read_only else "?mode=rw")
            connection = sqlite3.connect(uri, uri=True, timeout=5)
            with DescriptorTree.open_path(path.parent) as visible:
                current = visible.stat(path.name)
                if visible.identity != tree.identity or (current.st_dev, current.st_ino) != (
                    info.st_dev,
                    info.st_ino,
                ):
                    connection.close()
                    raise ValueError("SQLite file changed while opening")
        finally:
            os.close(descriptor)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise RuntimeError("SQLite foreign key enforcement is unavailable")  # noqa: TRY301 -- close failed connection below
        connection.execute("PRAGMA busy_timeout=5000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        else:
            connection.execute("PRAGMA synchronous=FULL")
    except BaseException:
        connection.close()
        raise
    return connection


def schema_checksums(migrations: Sequence[str]) -> tuple[str, ...]:
    """The recorded checksum of each schema version, oldest first.

    Version 1 covers the common identity tables as well as the store's own text, exactly
    as every installed store recorded it. A later version covers only the text it adds.
    """
    return tuple(
        hashlib.sha256(((_COMMON_DDL if index == 0 else "") + text).encode()).hexdigest()
        for index, text in enumerate(migrations)
    )


def validate_schema(
    connection: sqlite3.Connection, installation_id: str, kind: str, migrations: Sequence[str]
) -> int:
    """Return the store's schema version after checking its whole receipt history.

    A store records one schema_migrations row per version it has applied, from 1 up, and
    store_info names the last one. A version this code does not know, a gap, or a checksum
    that differs from the recorded text is refused rather than adopted.
    """
    checksums = schema_checksums(migrations)
    rows = connection.execute(
        "SELECT installation_id, schema_version, kind FROM store_info"
    ).fetchall()
    history = [
        (int(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT version, checksum FROM schema_migrations ORDER BY version"
        )
    ]
    if any(not 1 <= version <= len(checksums) for version, _ in history):
        raise ValueError("unknown store schema version")
    version = len(history)
    if (
        len(rows) != 1
        or tuple(rows[0]) != (installation_id, version, kind)
        or history != [(number, checksums[number - 1]) for number in range(1, version + 1)]
    ):
        raise ValueError("store identity or schema checksum mismatch")
    return version


def initialize(  # noqa: PLR0913 -- store identity plus its versioned schema
    connection: sqlite3.Connection,
    installation_id: str,
    kind: str,
    ddl: str,
    upgrades: Sequence[str] = (),
    *,
    version: int | None = None,
) -> int:
    """Install every version atomically, or validate an existing store; return its version.

    ``upgrades`` holds the text of versions 2 and later. A new store applies them all in
    the one transaction that creates it, so it records the same receipts as a store that
    was migrated. An existing store is validated and never upgraded here.
    """
    migrations = (ddl, *upgrades)
    target = len(migrations) if version is None else version
    if not 1 <= target <= len(migrations):
        raise ValueError("unknown store schema version")
    existing = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='store_info'"
    ).fetchone()
    if existing:
        return validate_schema(connection, installation_id, kind, migrations)
    tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    if tables:
        raise ValueError("refusing to initialize an unrecognized SQLite database")
    checksums = schema_checksums(migrations)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript("BEGIN IMMEDIATE;\n" + _COMMON_DDL + "".join(migrations[:target]))
        connection.execute(
            "INSERT INTO store_info VALUES (?, ?, ?, ?)",
            (uuid.uuid4().hex, installation_id, target, kind),
        )
        applied = time.time_ns() // 1000
        connection.executemany(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            [(number, checksums[number - 1], applied) for number in range(1, target + 1)],
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return target


def upgrade(
    connection: sqlite3.Connection,
    installation_id: str,
    kind: str,
    migrations: Sequence[str],
    target: int,
) -> int:
    """Apply the versions after the store's own up to ``target`` in one transaction.

    A store already at ``target`` is left alone. Nothing is downgraded: a store newer than
    the target is refused. The old receipts stay, and one row is added per applied version.
    """
    current = validate_schema(connection, installation_id, kind, migrations)
    if not 1 <= target <= len(migrations) or current > target:
        raise ValueError("unknown or older store schema version requested")
    if current == target:
        return current
    checksums = schema_checksums(migrations)
    try:
        connection.executescript("BEGIN IMMEDIATE;\n" + "".join(migrations[current:target]))
        applied = time.time_ns() // 1000
        connection.executemany(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            [(number, checksums[number - 1], applied) for number in range(current + 1, target + 1)],
        )
        connection.execute("UPDATE store_info SET schema_version=?", (target,))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return validate_schema(connection, installation_id, kind, migrations)
