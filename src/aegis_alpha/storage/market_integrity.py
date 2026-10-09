"""Generation integrity that holds whether or not the domain tables carry key constraints.

Both market writers (``market.publish_generation`` and
``bulk_generation.publish_generation_bulk``) check a new generation here inside their
publication transaction, and ``verification.verify_workspace`` audits stored rows here:

- ``begin_publication`` opens the publication transaction, and its first write claims
  the store: it writes the single ``store_info`` row and leaves every value as it was.
  DuckDB lets only one of two transactions that wrote the same row commit, so of two
  concurrent publications, whether from two cursors of one connection or from two
  connections of one process, only one gets past its claim; the other is refused there
  or at COMMIT (``commit_publication``). DuckDB's file lock alone serializes processes, not
  two cursors of one process, whose transactions each see only what committed before
  they began. The claim lives in the transaction, so ROLLBACK or closing the connection
  releases it; no lock is held in Python. ``run_publication`` is the transaction both
  writers run: it rolls back on any error and rolls back again when an interrupt stops
  the first ROLLBACK, so one ``KeyboardInterrupt`` anywhere, in the cleanup as well,
  leaves no claim held. A connection or cursor object serves one caller at a time; two
  threads sharing one object share its transaction, which the claim cannot tell apart.
- ``check_new_generation`` and ``check_inserted_generation`` run before and after the
  insert: no row named the generation before, the planned row count arrived, one
  revision per record, and no other generation of the domain holds any inserted
  ``(record_id, revision_id)``. That last check builds the delta's keys and scans the
  whole domain; it is never narrowed to the dataset or the chain.
- ``check_generation_flags`` holds a generation's quality flags to one row per key and
  to revisions the generation stores, before COMMIT.
- ``audit_market`` is the at-rest audit: the core catalog's exact shape, every
  generation's rows counted per domain both ways against its marker, every quality
  flag's reference, and with ``deep`` the domain-wide and flag key duplicates.

What these checks defend, and what they leave to the caller:

- In contract: concurrent publications in one process through these APIs (separate
  cursors or connections), an interrupt or crash at any point, malformed, duplicate or
  forged input documents, retained evidence that no longer matches, and stored rows lost
  or corrupted at rest, which the audits detect.
- Out of contract: code in the same process that manipulates the admitted DuckDB
  connection itself, by creating catalog objects that shadow core tables, changing the
  ``search_path``, ATTACHing other databases, sharing one ``DuckDBPyConnection`` object
  across threads, or issuing DML directly against the stored tables. Such code can delete
  stored rows outright, so no per-statement defense adds a guarantee, and every statement
  here names the core tables unqualified.
- Defense in depth against the shadowing class is one check, ``check_core_names``: the
  connection's current database is persistent and its ``search_path`` is DuckDB's
  default, and no temporary or attached table, nor any view, carries a core table's
  name. ``begin_publication`` runs it before BEGIN, so both writers and their reuse of
  an identical generation are refused before they read or write anything; the
  promotion's reuse answer, ``audit_market`` and ``verification.verify_workspace`` run
  it first as well.

Every statement runs under ``market.budgeted``, so DuckDB's memory exhaustion is a
``ComputeResourceError``. An audit either passes or raises; it never adds to a report.
"""

from __future__ import annotations

import functools
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage.market import (
    budgeted,
    capacity_error,
    initialize_market,
    rollback,
)
from aegis_alpha.storage.market_schema import DOMAIN_VERSIONS, DOMAINS, MIGRATIONS

if TYPE_CHECKING:
    import duckdb

_WORK: Final = "the generation integrity checks"
_AUDIT: Final = "the market audit"
_FLAGS: Final = "quality_flags"
_FLAG_KEYS: Final = ("record_id", "revision_id", "rule_id", "rule_version", "flag")
_REVISION_KEYS: Final = ("record_id", "revision_id")
# A grouped key costs its bytes plus DuckDB's row layout, hash and pointer slots.
_AUDIT_ROW_BYTES: Final = 128
# One duplicate pass may use this share of DuckDB's memory limit.
_PASS_SHARE: Final = 4
_DIGEST_HEX: Final = 64
_OPEN: Final = "another generation publication is open on this market store"
# DuckDB's write-write conflict, at the write or at COMMIT. Anchored at DuckDB's own
# prefix, so a constraint violation quoting a key with these words never matches.
# Negated twice, schema_version ends as it began; see begin_publication.
_CLAIM: Final = "UPDATE store_info SET schema_version = -schema_version"
_CONFLICT: Final = re.compile(r"TransactionContext Error: (?:Failed to commit: )?Conflict on ")


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _catalog_name(connection: duckdb.DuckDBPyConnection) -> str:
    """The connection's current database, which must be a persistent catalog."""
    row = connection.execute(
        "SELECT database_name FROM duckdb_databases() "
        "WHERE database_name = current_database() AND NOT internal"
    ).fetchone()
    if row is None:
        raise ValueError("market connection's current database is not a persistent catalog")
    return str(row[0])


@functools.cache
def _core_names() -> frozenset[str]:
    """Every table the newest core schema creates; no other object may carry these names."""
    return frozenset(_expected_catalog(len(MIGRATIONS)))


def check_core_names(connection: duckdb.DuckDBPyConnection) -> None:
    """Refuse a connection on which a core table name can resolve to anything but the store.

    The store is the connection's current database, which must be persistent, and the
    ``search_path`` must be DuckDB's default, so an unqualified name is looked up in
    ``temp.main`` and then in the store's ``main``. No table outside the store's ``main``
    (in ``temp``, another schema or an attached database) and no view anywhere, a
    registered Python object included, may carry a core table's name; DuckDB compares
    names without case, and so does this check. It reads only the catalog, so it runs
    before anything else at every entry that relies on it; DuckDB's memory exhaustion
    in it is a ``ComputeResourceError``.
    """
    with budgeted(connection, None, "the market name check"):
        database = _catalog_name(connection)
        if _one(connection, "SELECT current_setting('search_path')", []) != "":
            raise ValueError("market connection's search path is not DuckDB's default")
        # Two plain catalog scans, filtered here: no join or sort for DuckDB to hold.
        others = connection.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name <> ? OR schema_name <> 'main'",
            [database],
        ).fetchall()
        views = connection.execute("SELECT view_name FROM duckdb_views()").fetchall()
    names = _core_names()
    row = next((row for row in (*others, *views) if str(row[0]).lower() in names), None)
    if row is not None:
        raise ValueError(f"market core table {row[0]} resolves to an object other than the store's")


def _refuse_conflict(error: BaseException) -> None:
    """Raise the open-publication refusal for DuckDB's write conflict; else return.

    Exhausted memory is classified first and never becomes the refusal.
    """
    if not capacity_error(error) and _CONFLICT.match(str(error)) is not None:
        raise ValueError(_OPEN) from error


def begin_publication(connection: duckdb.DuckDBPyConnection) -> None:
    """BEGIN, then claim the store with the transaction's first write.

    The claim negates ``schema_version`` and negates it back, so every ``store_info``
    value is unchanged when the claim returns and at COMMIT, and the row is still
    written: DuckDB records no write for an UPDATE that sets the value the row already
    holds, and such an UPDATE never conflicts. A publication open or committed since
    this transaction's snapshot on the same store makes the claim a write conflict,
    raised as ``ValueError`` naming the open publication. The claim is budgeted here,
    so exhaustion is a ``ComputeResourceError`` in both writers. ``check_core_names``
    runs before BEGIN, so a connection on which ``store_info`` could resolve to anything
    but the store's own row is refused with nothing opened.

    ``run_publication`` calls this inside its cleanup, so an interrupt arriving right
    after BEGIN or after either claim statement leaves nothing open. Rolling back after
    a refused BEGIN loses nothing: DuckDB has already aborted the transaction that was
    open on the connection.
    """
    import duckdb  # noqa: PLC0415 -- the driver's error types

    check_core_names(connection)
    _ = connection.execute("BEGIN TRANSACTION")
    with budgeted(connection, None, _WORK):
        try:
            claimed = _one(connection, _CLAIM, [])
            _ = connection.execute(_CLAIM)
        except duckdb.TransactionException as error:
            _refuse_conflict(error)
            raise
    if claimed != 1:
        raise ValueError("market store has no identity")


def commit_publication(connection: duckdb.DuckDBPyConnection) -> None:
    """COMMIT; a write conflict reported there is the same refusal as at the claim.

    Any other failed COMMIT, exhausted memory included, keeps its own error, which the
    caller's ROLLBACK (``market.rollback``) never replaces.
    """
    import duckdb  # noqa: PLC0415 -- the driver's error types

    try:
        _ = connection.execute("COMMIT")
    except duckdb.TransactionException as error:
        _refuse_conflict(error)
        raise


def run_publication[T](connection: duckdb.DuckDBPyConnection, work: Callable[[], T]) -> T:
    """Claim the store, run ``work`` and COMMIT in one transaction, and return its result.

    Any error, an interrupt included, rolls the transaction back and propagates. The
    ROLLBACK runs a second time in ``finally``: an interrupt that lands in the error
    handler before the first ROLLBACK ran, or inside it, would otherwise leave the
    transaction and its claim open on the connection, refusing every other publication on
    the store until the connection closes. After COMMIT or a finished ROLLBACK the second
    finds no transaction, which ``market.rollback`` tolerates, so it never replaces the
    error being raised.
    """
    try:
        try:
            begin_publication(connection)
            result = work()
            commit_publication(connection)
        except BaseException:
            rollback(connection)
            raise
    finally:
        rollback(connection)
    return result


def _version(connection: duckdb.DuckDBPyConnection) -> int:
    """The schema version ``store_info`` names; admission has validated it."""
    return int(cast("int", _one(connection, "SELECT schema_version FROM store_info", [])))


def _held_domains(connection: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    version = _version(connection)
    return tuple(name for name in DOMAINS if DOMAIN_VERSIONS[name] <= version)


def _flags_held(connection: duckdb.DuckDBPyConnection) -> bool:
    return _version(connection) >= 2  # noqa: PLR2004 -- quality_flags arrive in v2


def _one(connection: duckdb.DuckDBPyConnection, sql: str, parameters: list[object]) -> object:
    row = connection.execute(sql, parameters).fetchone()
    if row is None:
        raise ValueError("integrity query returned no row")
    return row[0]


def check_new_generation(connection: duckdb.DuckDBPyConnection, generation_id: str) -> None:
    """No domain or flag row names ``generation_id`` before its publication inserts any."""
    with budgeted(connection, None, _WORK):
        tables = [
            *_held_domains(connection),
            *((_FLAGS,) if _flags_held(connection) else ()),
        ]
        for table in tables:
            if _one(
                connection,
                f"SELECT EXISTS (SELECT 1 FROM {_quote(table)} WHERE generation_id=?)",  # noqa: S608 -- code-owned table
                [generation_id],
            ):
                raise ValueError("market rows already name this generation")


def check_inserted_generation(
    connection: duckdb.DuckDBPyConnection, domain: str, generation_id: str, row_count: int
) -> None:
    """The generation's stored marker and planned rows, one revision per record, all new.

    Each rule is its own statement over the stored tables. The marker must be stored
    under the domain. The collision check builds the generation's keys and scans every
    other generation of the domain, whatever dataset holds it.
    """
    if domain not in DOMAINS:
        raise ValueError("unknown market domain")
    with budgeted(connection, None, _WORK):
        table = _quote(domain)
        if not _one(
            connection,
            "SELECT EXISTS (SELECT 1 FROM market_generations WHERE generation_id=? AND domain=?)",
            [generation_id, domain],
        ):
            raise ValueError("the generation's marker is not stored in the market store")
        if (
            _one(
                connection,
                f"SELECT count(*) FROM {table} WHERE generation_id=?",  # noqa: S608 -- code-owned table
                [generation_id],
            )
            != row_count
        ):
            raise ValueError("generation stored a different row count than it planned")
        if _one(
            connection,
            f"SELECT count(*) - count(DISTINCT record_id) FROM {table} WHERE generation_id=?",  # noqa: S608 -- code-owned table
            [generation_id],
        ):
            raise ValueError("one delta can contain only one revision per natural record")
        if _one(
            connection,
            f"SELECT EXISTS (SELECT 1 FROM {table} p SEMI JOIN "  # noqa: S608 -- code-owned table
            f"(SELECT record_id, revision_id FROM {table} WHERE generation_id=?) n "
            "USING (record_id, revision_id) WHERE p.generation_id <> ?)",
            [generation_id, generation_id],
        ):
            raise ValueError("duplicate market revision")


def check_generation_flags(
    connection: duckdb.DuckDBPyConnection, domain: str, generation_id: str
) -> None:
    """The generation's flags repeat no key and name only revisions it stores."""
    if domain not in DOMAINS:
        raise ValueError("unknown market domain")
    keys = ", ".join(_FLAG_KEYS)
    with budgeted(connection, None, _WORK):
        flags = _FLAGS
        if _one(
            connection,
            f"SELECT EXISTS (SELECT 1 FROM {flags} WHERE generation_id=? "  # noqa: S608 -- code-owned table
            f"GROUP BY {keys} HAVING count(*) > 1)",
            [generation_id],
        ):
            raise ValueError("a quality flag repeats its key within the generation")
        if _one(
            connection,
            # Both sides are this generation's own rows, so neither builds the whole domain.
            f"SELECT EXISTS (SELECT 1 FROM (SELECT record_id, revision_id FROM {flags} "  # noqa: S608 -- code-owned tables
            "WHERE generation_id=?) f ANTI JOIN (SELECT record_id, revision_id "
            f"FROM {_quote(domain)} WHERE generation_id=?) d USING (record_id, revision_id))",
            [generation_id, generation_id],
        ):
            raise ValueError("a quality flag names a revision the generation does not store")


def audit_market(
    connection: duckdb.DuckDBPyConnection, budget: ComputeBudget | None, *, deep: bool
) -> None:
    """Pass or raise: catalog shape, counts and flag references; ``deep`` adds duplicates.

    ``check_core_names`` runs first, so the audit never reads a table standing in for
    a stored one.
    """
    check_core_names(connection)
    with budgeted(connection, budget, _AUDIT):
        check_core_catalog(connection)
        audit_generation_counts(connection)
        audit_flag_references(connection)
        if deep:
            audit_duplicates(connection)


def audit_generation_counts(connection: duckdb.DuckDBPyConnection) -> None:
    """Every domain's rows per generation agree with the markers, in both directions.

    One domain per statement, each building only the markers or that domain's
    per-generation counts, so the audit runs within a small DuckDB limit.
    """
    markers = "market_generations"
    domains = _held_domains(connection)
    held = ", ".join(f"'{name}'" for name in domains)
    with budgeted(connection, None, _AUDIT):
        if _one(
            connection,
            f"SELECT EXISTS (SELECT 1 FROM {markers} WHERE domain NOT IN ({held}))",  # noqa: S608 -- code-owned names
            [],
        ):
            raise ValueError("generation marker names a domain the store does not hold")
        for name in domains:
            table = _quote(name)
            if _one(
                connection,
                f"SELECT EXISTS (SELECT 1 FROM {table} ANTI JOIN {markers} "  # noqa: S608 -- code-owned table
                "USING (generation_id))",
                [],
            ):
                raise ValueError("market rows name a generation without a marker")
            if _one(
                connection,
                f"SELECT EXISTS (SELECT 1 FROM {table} SEMI JOIN (SELECT generation_id "  # noqa: S608 -- code-owned table
                f"FROM {markers} WHERE domain <> ?) g USING (generation_id))",
                [name],
            ):
                raise ValueError("generation contains rows in a foreign domain")
            if _one(
                connection,
                "SELECT EXISTS (SELECT 1 FROM (SELECT generation_id, count(*) AS n "  # noqa: S608 -- code-owned table
                f"FROM {table} GROUP BY generation_id) c FULL JOIN (SELECT generation_id, "
                f"row_count FROM {markers} WHERE domain = ?) g USING (generation_id) "
                "WHERE coalesce(c.n, 0) <> coalesce(g.row_count, -1))",
                [name],
            ):
                raise ValueError("market generation row count differs from its marker")


def audit_flag_references(connection: duckdb.DuckDBPyConnection) -> None:
    """Every quality flag names a marker of a held domain and a revision that marker stores.

    The flags are the small side, so each domain's check builds their keys and scans the
    domain once rather than building the domain, one statement per domain.
    """
    if not _flags_held(connection):
        return
    domains = _held_domains(connection)
    flags = _FLAGS
    markers = "market_generations"
    with budgeted(connection, None, _AUDIT):
        if _one(
            connection,
            # A marker's own domain is held by audit_generation_counts.
            f"SELECT EXISTS (SELECT 1 FROM {flags} ANTI JOIN {markers} "  # noqa: S608 -- code-owned tables
            "USING (generation_id))",
            [],
        ):
            raise ValueError("a quality flag names no generation marker")
        flagged = {
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT g.domain FROM {flags} f JOIN {markers} g "  # noqa: S608 -- code-owned tables
                "USING (generation_id)"
            ).fetchall()
        }
        for name in (name for name in domains if name in flagged):
            # DuckDB builds the anti join on the smaller side, the flags' keys.
            if _one(
                connection,
                "SELECT EXISTS (SELECT 1 FROM (SELECT f.generation_id, f.record_id, "  # noqa: S608 -- code-owned tables
                f"f.revision_id FROM {flags} f JOIN {markers} g "
                "USING (generation_id) WHERE g.domain = ?) k "
                f"ANTI JOIN {_quote(name)} d USING (generation_id, record_id, revision_id))",
                [name],
            ):
                raise ValueError("a quality flag names a revision its generation does not store")


def audit_duplicates(
    connection: duckdb.DuckDBPyConnection, *, pass_bytes: int | None = None
) -> None:
    """No ``(record_id, revision_id)`` repeats in any domain, nor any flag key.

    Each table is checked in passes whose measured rows and key bytes fit ``pass_bytes``
    (by default a quarter of the connection's DuckDB memory limit); see ``_duplicates``.
    """
    with budgeted(connection, None, _AUDIT):
        if pass_bytes is None:
            limit = cast(
                "int",
                _one(
                    connection,
                    "SELECT parse_formatted_bytes(current_setting('memory_limit'))",
                    [],
                ),
            )
            pass_bytes = max(1, limit // _PASS_SHARE)
        for name in _held_domains(connection):
            if _duplicates(connection, name, _REVISION_KEYS, pass_bytes)[0]:
                raise ValueError("a market revision is stored more than once")
        if (
            _flags_held(connection)
            and _duplicates(connection, _FLAGS, ("generation_id", *_FLAG_KEYS), pass_bytes)[0]
        ):
            raise ValueError("a quality flag is stored more than once")


@dataclass(frozen=True, slots=True)
class _Part:
    """Rows whose first key starts with ``prefix`` and whose key digest with ``hashed``."""

    prefix: str
    depth: int
    hashed: str
    rows: int
    size: int


def _duplicates(
    connection: duckdb.DuckDBPyConnection, table: str, keys: tuple[str, ...], pass_bytes: int
) -> tuple[bool, list[int]]:
    """Whether a full key repeats in the stored ``table``, and the measured bytes of each pass.

    Partitions come from measured row counts and key bytes, never from an assumed even
    spread: first by the first character of the leading key, then any partition still
    too large by the leading hex digits of a SHA-256 over every key column, each framed
    by its byte length, deepening until each part fits. Equal keys always share both,
    so every repeat stays inside one pass, and each pass compares the full VARCHAR keys.
    A part that cannot shrink (one row, or one digest over unequal keys) is a
    ``ComputeResourceError``, never a skipped check.
    """
    quoted = [_quote(key) for key in keys]
    width = f"{_AUDIT_ROW_BYTES} + " + " + ".join(
        f"coalesce(octet_length(encode({key})), 0)" for key in quoted
    )
    framed = " || ".join(
        f"coalesce(CAST(octet_length(encode({key})) AS VARCHAR), 'n') || ':' || coalesce({key}, '')"
        for key in quoted
    )
    digest = f"sha256({framed})"
    first = f"coalesce(substr({quoted[0]}, 1, 1), '')"
    source = _quote(table)
    size = int(cast("int", _one(connection, f"SELECT coalesce(sum({width}), 0) FROM {source}", [])))  # noqa: S608 -- code-owned table
    if size <= pass_bytes:
        return _repeats(connection, source, quoted, "true", []), [size]
    pending = [
        _Part(str(prefix), 0, "", int(count), int(bytes_))
        for prefix, count, bytes_ in connection.execute(
            f"SELECT {first}, count(*), sum({width}) FROM {source} GROUP BY ALL"  # noqa: S608 -- code-owned table
        ).fetchall()
    ]
    fitting: list[_Part] = []
    while pending:
        part = pending.pop()
        if part.size <= pass_bytes:
            fitting.append(part)
            continue
        if part.depth == _DIGEST_HEX:
            return _shared_digest(
                connection, source=source, quoted=quoted, first=first, digest=digest, part=part
            ), []
        deeper = min(
            _DIGEST_HEX,
            part.depth + max(1, math.ceil(math.log(2 * part.size / pass_bytes, 16))),
        )
        pending.extend(
            _Part(part.prefix, deeper, str(hashed), int(count), int(bytes_))
            for hashed, count, bytes_ in connection.execute(
                f"SELECT left({digest}, {deeper}), count(*), sum({width}) FROM {source} "  # noqa: S608 -- code-owned table
                f"WHERE {first} = ? AND left({digest}, {part.depth}) = ? GROUP BY ALL",
                [part.prefix, part.hashed],
            ).fetchall()
        )
    sizes: list[int] = []
    for group in _bins(fitting, pass_bytes):
        sizes.append(sum(part.size for part in group))
        predicate, parameters = _predicate(group, first, digest)
        if _repeats(connection, source, quoted, predicate, parameters):
            return True, sizes
    return False, sizes


def _shared_digest(  # noqa: PLR0913 -- one undividable part of one audit
    connection: duckdb.DuckDBPyConnection,
    *,
    source: str,
    quoted: list[str],
    first: str,
    digest: str,
    part: _Part,
) -> bool:
    """A full-depth part too large for a pass: one wide row, or rows of one key."""
    if part.rows > 1:
        # Rows of one digest agree on which keys are NULL, so min and max decide equality.
        equal = " AND ".join(f"min({key}) IS NOT DISTINCT FROM max({key})" for key in quoted)
        if _one(
            connection,
            f"SELECT {equal} FROM {source} WHERE {first} = ? AND {digest} = ?",  # noqa: S608 -- code-owned table
            [part.prefix, part.hashed],
        ):
            return True
    raise ComputeResourceError(
        f"a duplicate audit part of {part.size} bytes cannot be split below its admitted pass size"
    )


def _bins(parts: list[_Part], pass_bytes: int) -> list[list[_Part]]:
    """Pack measured parts into passes of at most ``pass_bytes`` each, in key order."""
    bins: list[list[_Part]] = []
    current: list[_Part] = []
    used = 0
    for part in sorted(parts, key=lambda p: (p.prefix, p.depth, p.hashed)):
        if current and used + part.size > pass_bytes:
            bins.append(current)
            current, used = [], 0
        current.append(part)
        used += part.size
    if current:
        bins.append(current)
    return bins


def _predicate(group: list[_Part], first: str, digest: str) -> tuple[str, list[object]]:
    whole = [part.prefix for part in group if part.depth == 0]
    split: dict[tuple[str, int], list[str]] = {}
    for part in group:
        if part.depth:
            split.setdefault((part.prefix, part.depth), []).append(part.hashed)
    terms: list[str] = []
    parameters: list[object] = []
    if whole:
        terms.append(f"{first} IN (SELECT unnest(?::VARCHAR[]))")
        parameters.append(whole)
    for (prefix, depth), hashed in split.items():
        terms.append(f"({first} = ? AND left({digest}, {depth}) IN (SELECT unnest(?::VARCHAR[])))")
        parameters.extend((prefix, hashed))
    return " OR ".join(terms), parameters


def _repeats(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    quoted: list[str],
    predicate: str,
    parameters: list[object],
) -> bool:
    keys = ", ".join(quoted)
    return bool(
        _one(
            connection,
            f"SELECT EXISTS (SELECT 1 FROM {source} WHERE {predicate} "  # noqa: S608 -- code-owned table and predicate
            f"GROUP BY {keys} HAVING count(*) > 1)",
            parameters,
        )
    )


_Catalog = dict[str, tuple[tuple[object, ...], tuple[tuple[object, ...], ...]]]


def _catalog(connection: duckdb.DuckDBPyConnection) -> _Catalog:
    """Each stored main table's ordered columns and its sorted constraint structure."""
    database = _catalog_name(connection)
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'main' AND NOT temporary",
            [database],
        ).fetchall()
    }
    columns: dict[str, list[tuple[object, ...]]] = {name: [] for name in tables}
    for row in connection.execute(
        "SELECT table_name, column_index, column_name, data_type, is_nullable, column_default "
        "FROM duckdb_columns() WHERE database_name = ? "
        "AND schema_name = 'main' ORDER BY table_name, column_index",
        [database],
    ).fetchall():
        if row[0] in columns:
            columns[str(row[0])].append(tuple(row[1:]))
    constraints: dict[str, list[tuple[object, ...]]] = {name: [] for name in tables}
    for row in connection.execute(
        "SELECT table_name, constraint_type, constraint_name, constraint_text, expression, "
        "constraint_column_names, referenced_table, referenced_column_names "
        "FROM duckdb_constraints() WHERE database_name = ? "
        "AND schema_name = 'main'",
        [database],
    ).fetchall():
        if row[0] in constraints:
            constraints[str(row[0])].append(
                tuple(tuple(value) if isinstance(value, list) else value for value in row[1:])
            )
    return {
        name: (tuple(columns[name]), tuple(sorted(constraints[name], key=repr))) for name in tables
    }


@functools.cache
def _expected_catalog(version: int) -> _Catalog:
    """The core catalog an empty store of ``version`` has in this DuckDB build."""
    import duckdb  # noqa: PLC0415 -- an empty in-memory reference store

    fresh = duckdb.connect(config={"threads": 1})
    try:
        _ = initialize_market(fresh, "core-catalog", version=version)
        return _catalog(fresh)
    finally:
        fresh.close()


def check_core_catalog(connection: duckdb.DuckDBPyConnection) -> None:
    """The core tables match an empty store of the same version, column for column.

    Columns compare in order with their types, nullability and defaults; constraints by
    kind, columns, text and referenced table. Tables the core does not own (the run
    add-on, the source library) and temporary tables are not compared.
    """
    expected = _expected_catalog(_version(connection))
    actual = _catalog(connection)
    for name, shape in expected.items():
        if actual.get(name) != shape:
            raise ValueError(f"market core table {name} differs from its schema version")
