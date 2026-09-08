"""Owned SQLite connections and checksummed initial schemas."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import time
import uuid
from pathlib import Path

SCHEMA_VERSION = 1
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


def initialize(connection: sqlite3.Connection, installation_id: str, kind: str, ddl: str) -> None:
    """Apply v1 atomically, or validate its identity and exact schema checksum."""
    checksum = hashlib.sha256((_COMMON_DDL + ddl).encode()).hexdigest()
    existing = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='store_info'"
    ).fetchone()
    if existing:
        rows = connection.execute(
            "SELECT installation_id, schema_version, kind FROM store_info"
        ).fetchall()
        migration = connection.execute(
            "SELECT checksum FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
        ).fetchone()
        if (
            len(rows) != 1
            or tuple(rows[0]) != (installation_id, SCHEMA_VERSION, kind)
            or migration is None
            or migration[0] != checksum
        ):
            raise ValueError("store identity or schema checksum mismatch")
        return
    tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    if tables:
        raise ValueError("refusing to initialize an unrecognized SQLite database")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript("BEGIN IMMEDIATE;\n" + _COMMON_DDL + ddl)
        connection.execute(
            "INSERT INTO store_info VALUES (?, ?, ?, ?)",
            (uuid.uuid4().hex, installation_id, SCHEMA_VERSION, kind),
        )
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (SCHEMA_VERSION, checksum, time.time_ns() // 1000),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
