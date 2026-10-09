"""Bulk typed generations: one INSERT ... SELECT with aas-rowset-v1 streaming parity.

``market.publish_generation`` takes Python rows and rehashes the whole chain in Python.
This module publishes a staged DuckDB table instead and keeps the same contract:

- The marker is the one ``market.plan_generation`` would record for the same rows.
  DuckDB encodes every row in the aas-rowset-v1 byte format and sorts the encodings;
  Python reads them back in bounded batches and digests them with ``RowsetStream``,
  which refuses any row that sorts before its predecessor. The delta hash therefore
  equals ``market._delta_hash`` over the stored rows, and ``market.verify_generation``
  verifies a bulk generation exactly as it verifies any other. No new hash format.
- Row validation is ``market.normalize_rows``'s, expressed as one SQL aggregate, and
  every ``record_id`` is recomputed with ``market.record_identity`` in bounded batches.
- Revision rules are ``market._validate_revisions``'s, checked in SQL against the
  previous head of each delta record only, so planning never loads the chain into
  Python. Verification checks every marker link from recorded hashes and rehashes the
  rows of the requested generation alone; ``deep=True`` rehashes every generation.
- Marker and rows commit in one DuckDB transaction. The plan is recomputed inside that
  transaction, so a dataset head that moved since planning fails the parent CAS with
  ``ParentChangedError`` and the caller plans again on the new head; a plan whose
  marker no longer matches fails with ``PlanChangedError``. An existing marker with
  the same generation or operation ID is returned only when its content is exactly
  the requested content, so a leftover generation is never adopted by another request.
- The transaction (``market_integrity.run_publication``) first claims the store, before
  the plan, so no other publication commits between the plan and COMMIT, and the
  inserted rows pass ``market_integrity``'s checks:
  no earlier row of the generation, the planned count, one revision per record, and no
  ``(record_id, revision_id)`` any other generation of the domain already holds.
- Memory follows decision 0016: before any row is fetched, an SQL aggregate bounds
  the widest row, and each batch is sized so that its charge fits the caller's
  allocation. A row that cannot fit raises ``ComputeResourceError``. DuckDB's own
  share (sorting, spilling, index maintenance) is bounded by the connection limits
  derived from the same allocation.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage.market import (
    RECORD_SCHEMA,
    budgeted,
    chain_digest,
    delta_columns,
    delta_digest,
    generation_chain,
    market_version,
    record_identity,
    rollback,
    rowset_schema,
)
from aegis_alpha.storage.market_integrity import (
    check_generation_flags,
    check_inserted_generation,
    check_new_generation,
    other_flags,
    run_publication,
)
from aegis_alpha.storage.market_schema import (
    COMMON,
    DOMAIN_VERSIONS,
    DOMAINS,
    NATURAL_KEYS,
    PRICE_FIELDS,
    TEXT_CHARACTER_BYTES,
    text_bytes,
)
from aegis_alpha.storage.rowset import RowsetStream

if TYPE_CHECKING:
    import duckdb

BATCH_ROWS: Final = 65_536
_IDENTIFIER: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
_MARKER_KEYS: Final = (
    "generation_id",
    "dataset_id",
    "version",
    "parent_id",
    "sequence",
    "domain",
    "record_schema",
    "delta_hash",
    "chain_hash",
    "row_count",
    "operation_id",
    "request_hash",
)
_WORK: Final = "the bulk generation"
_COLUMN_TYPES: Final = ("VARCHAR", "BIGINT", "DATE", "DOUBLE", "DECIMAL(38,12)")
# Every code point str.strip() removes. A text cell made only of these is empty to
# normalize_rows, so the SQL check uses exactly this class rather than trim()'s spaces.
_WHITESPACE: Final = (
    0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x85, 0xA0, 0x1680,
    0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009,
    0x200A, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000,
)  # fmt: skip
_BLANK: Final = "[" + "".join(f"\\x{{{point:x}}}" for point in _WHITESPACE) + "]*"
_STATES: Final = "('present','missing','not_collected','unsupported','invalid')"
_OPS: Final = "('ASSERT','SUPERSEDE','TOMBSTONE')"
# Widest aas-rowset-v1 cell per type: tag, then the fixed payload. Text adds its bytes;
# a DECIMAL(38,12) coefficient has at most 38 digits.
_FIXED_WIDTH: Final = {"int": 9, "utc_us": 9, "float": 9, "date": 15, "decimal": 48}
_TEXT_FRAMING: Final = 5
# Python holds a fetched batch as a list of tuples of bytes, and the stream keeps the
# previous row; per row this is the encoding twice over plus fixed object headers.
_ROW_OBJECT_BYTES: Final = 256
# One record-identity check holds the fetched tuple, the natural-key JSON list, its
# serialized text (up to twelve characters per code point when escaped) and digests.
_IDENTITY_ROW_BYTES: Final = 1024
_IDENTITY_VALUE_BYTES: Final = 512
_IDENTITY_CHARACTER_BYTES: Final = 16


class ParentChangedError(ValueError):
    """The dataset head is not the planned parent; plan again on the current head."""


class PlanChangedError(ValueError):
    """The generation recomputed at publication differs from the reviewed plan."""


@dataclass(frozen=True, slots=True)
class BulkRequest:
    """Identity of one bulk generation and the staged table that holds its rows.

    ``staged`` names a table or view visible to the connection with exactly the
    domain's typed columns except ``generation_id``, including ``record_id``, and
    for prices optionally ``fields``. Its column types must be the domain's. Rows
    staged in anything but a table are rehashed after insertion, before COMMIT,
    because a view can evaluate differently on each read.
    """

    dataset_id: str
    version: str
    generation_id: str
    operation_id: str
    request_hash: str
    parent_id: str | None
    domain: str
    staged: str


@dataclass(frozen=True, slots=True)
class BulkPlan:
    """The marker a bulk publication records, and whether it already exists."""

    marker: Mapping[str, object]
    reused: bool
    hash_batch_rows: int
    identity_batch_rows: int


@dataclass(frozen=True, slots=True)
class _Stats:
    count: int
    close_rows: bool
    row_bytes: int
    identity_characters: int


def plan_generation_bulk(
    connection: duckdb.DuckDBPyConnection, request: BulkRequest, *, budget: ComputeBudget
) -> BulkPlan:
    """Validate and hash the staged rows and return the marker, writing nothing."""
    with budgeted(connection, budget, _WORK):
        _ = connection.execute("BEGIN TRANSACTION")
        try:
            return _plan(connection, request, budget)
        finally:
            rollback(connection)


def publish_generation_bulk(
    connection: duckdb.DuckDBPyConnection,
    request: BulkRequest,
    *,
    budget: ComputeBudget,
    plan: BulkPlan | None = None,
    companion: Callable[[duckdb.DuckDBPyConnection], None] | None = None,
) -> dict[str, object]:
    """Commit the marker and every staged row in one transaction and return the marker.

    The plan is recomputed inside the transaction. When ``plan`` is given, the
    recomputed marker must equal it, so what commits is what was reviewed.
    ``companion`` runs inside the same transaction after a new generation's rows are
    inserted, so rows that belong to the generation (its quality flags) commit or roll
    back with its marker. It does not run when an identical generation is reused. What
    it leaves must pass ``check_generation_flags`` and change no other generation's flags.
    An error or one interrupt at any point rolls the transaction back.
    """

    def publish() -> BulkPlan:
        current = _plan(connection, request, budget)
        _check_reviewed(plan, current)
        if not current.reused:
            check_new_generation(connection, request.generation_id)
            _insert(connection, request, current)
            check_inserted_generation(
                connection,
                request.domain,
                request.generation_id,
                int(str(current.marker["row_count"])),
            )
            if not _is_table(connection, request.staged):
                _check_stored(connection, request, budget)
            if companion is not None:
                _run_companion(connection, request, companion)
        return current

    with budgeted(connection, budget, _WORK):
        current = run_publication(connection, publish)
    return dict(current.marker)


def _run_companion(
    connection: duckdb.DuckDBPyConnection,
    request: BulkRequest,
    companion: Callable[[duckdb.DuckDBPyConnection], None],
) -> None:
    """Run the companion, then check the flags it stored before the generation commits.

    No table key holds the flags to one row per key, so the writer itself refuses a
    repeated or orphan flag of the generation, and any change to another generation's.
    """
    others = other_flags(connection, request.generation_id)
    companion(connection)
    check_generation_flags(connection, request.domain, request.generation_id)
    if other_flags(connection, request.generation_id) != others:
        raise ValueError("a companion changed another generation's quality flags")


def _is_table(connection: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Whether ``name`` is a stored table, which every statement of a transaction reads alike."""
    found = connection.execute(
        "SELECT EXISTS (SELECT 1 FROM duckdb_tables() WHERE table_name=?)"
        " AND NOT EXISTS (SELECT 1 FROM duckdb_views() WHERE view_name=? AND NOT internal)",
        [name, name],
    ).fetchone()
    return found is not None and bool(found[0])


def _check_stored(
    connection: duckdb.DuckDBPyConnection, request: BulkRequest, budget: ComputeBudget
) -> None:
    """Rehash the inserted rows; a view or scan can evaluate differently on each read."""
    try:
        _ = verify_generation_bulk(connection, request.generation_id, budget=budget)
    except ComputeResourceError:
        raise
    except ValueError as error:
        raise PlanChangedError(
            "staged rows changed between planning and insertion; stage them in a table"
        ) from error


def _check_reviewed(plan: BulkPlan | None, current: BulkPlan) -> None:
    if plan is not None and dict(plan.marker) != dict(current.marker):
        raise PlanChangedError("bulk generation changed since it was planned; plan again")


def verify_generation_bulk(
    connection: duckdb.DuckDBPyConnection,
    generation_id: str,
    *,
    budget: ComputeBudget,
    deep: bool = False,
) -> dict[str, object]:
    """Verify a generation's chain links and its own rows; ``deep`` rehashes every delta.

    Every marker's chain hash is recomputed from its parent's and its recorded delta,
    so a broken link anywhere in the chain fails without reading ancestor rows. The
    generation's rows are counted, kept to its domain, rehashed with streaming parity
    and checked as revisions of the heads before it.
    """
    with budgeted(connection, budget, _WORK):
        chain = generation_chain(connection, generation_id)
        verify_chain_links(chain)
        for index in range(len(chain)) if deep else (len(chain) - 1,):
            _verify_rows(connection, chain, index, budget)
        return chain[-1]


def _plan(
    connection: duckdb.DuckDBPyConnection, request: BulkRequest, budget: ComputeBudget
) -> BulkPlan:
    domain = request.domain
    if domain not in DOMAINS:
        raise ValueError("unknown market domain or empty generation")
    _require_version(connection, DOMAIN_VERSIONS[domain], f"the {domain} domain")
    staged = _quote(request.staged)
    fields = _staged_columns(connection, domain, staged)
    stats = _stats(connection, domain, staged, fields=fields, generation_id=request.generation_id)
    if stats.count == 0:
        raise ValueError("unknown market domain or empty generation")
    if stats.close_rows:
        _require_version(connection, 2, "a close-only price")
    hash_rows, identity_rows = _batch_rows(stats, budget)
    existing = connection.execute(
        "SELECT generation_id FROM market_generations WHERE generation_id=? OR operation_id=?",
        [request.generation_id, request.operation_id],
    ).fetchall()
    marker: dict[str, object]
    if existing:
        marker = verify_generation_bulk(connection, str(existing[0][0]), budget=budget)
    else:
        marker = _new_marker_head(connection, request, staged)
    _check_identities(connection, domain, staged, identity_rows)
    delta = _stream_delta(
        connection,
        domain,
        f"SELECT * FROM {staged}",  # noqa: S608 -- quoted validated identifier
        [],
        generation_id=request.generation_id,
        close_rows=stats.close_rows,
        count=stats.count,
        batch_rows=hash_rows,
    )
    if existing:
        expected = {
            "dataset_id": request.dataset_id,
            "version": request.version,
            "generation_id": request.generation_id,
            "operation_id": request.operation_id,
            "request_hash": request.request_hash,
            "parent_id": request.parent_id,
            "domain": domain,
            "delta_hash": delta,
            "row_count": stats.count,
        }
        if any(marker[key] != value for key, value in expected.items()):
            raise ValueError("generation or operation ID conflicts with existing content")
        return BulkPlan(
            MappingProxyType(marker),
            reused=True,
            hash_batch_rows=hash_rows,
            identity_batch_rows=identity_rows,
        )
    marker.update(
        delta_hash=delta,
        row_count=stats.count,
        chain_hash=chain_digest(
            parent_chain_hash=marker.pop("parent_chain_hash"),
            dataset_id=request.dataset_id,
            version=request.version,
            generation_id=request.generation_id,
            domain=domain,
            delta_hash=delta,
            parent_id=request.parent_id,
            operation_id=request.operation_id,
            request_hash=request.request_hash,
            row_count=stats.count,
        ),
    )
    ordered = {key: marker[key] for key in _MARKER_KEYS}
    return BulkPlan(
        MappingProxyType(ordered),
        reused=False,
        hash_batch_rows=hash_rows,
        identity_batch_rows=identity_rows,
    )


def _new_marker_head(
    connection: duckdb.DuckDBPyConnection, request: BulkRequest, staged: str
) -> dict[str, object]:
    """Check the parent CAS, the chain links and the delta's revisions; no hashing yet."""
    latest = connection.execute(
        "SELECT generation_id FROM market_generations WHERE dataset_id=? "
        "ORDER BY sequence DESC LIMIT 1",
        [request.dataset_id],
    ).fetchone()
    if (latest[0] if latest else None) != request.parent_id:
        raise ParentChangedError(
            "market parent changed or awaits catalog recovery; plan again on the current head"
        )
    if connection.execute(
        "SELECT 1 FROM market_generations WHERE dataset_id=? AND version=?",
        [request.dataset_id, request.version],
    ).fetchone():
        raise ValueError("dataset version already identifies another generation")
    chain = generation_chain(connection, request.parent_id) if request.parent_id else []
    if chain:
        verify_chain_links(chain)
        if chain[-1]["dataset_id"] != request.dataset_id or chain[-1]["domain"] != request.domain:
            raise ValueError("generation parent belongs to another dataset/domain")
    _check_revisions(
        connection,
        request.domain,
        f"SELECT * FROM {staged}",  # noqa: S608 -- quoted validated identifier
        [],
        [str(marker["generation_id"]) for marker in chain],
    )
    return {
        "generation_id": request.generation_id,
        "dataset_id": request.dataset_id,
        "version": request.version,
        "parent_id": request.parent_id,
        "sequence": len(chain) + 1,
        "domain": request.domain,
        "record_schema": RECORD_SCHEMA,
        "operation_id": request.operation_id,
        "request_hash": request.request_hash,
        "parent_chain_hash": chain[-1]["chain_hash"] if chain else None,
    }


def _insert(connection: duckdb.DuckDBPyConnection, request: BulkRequest, plan: BulkPlan) -> None:
    marker = plan.marker
    _ = connection.execute(
        "INSERT INTO market_generations VALUES ("  # noqa: S608 -- placeholder count only
        + ",".join("?" for _ in _MARKER_KEYS)
        + ")",
        [marker[key] for key in _MARKER_KEYS],
    )
    staged = _quote(request.staged)
    names = [name for name, _ in COMMON + DOMAINS[request.domain]]
    # A v1 store has no fields column; planning already refused a close-only row there,
    # so the OHLCV rows it receives take the shape every v1 row has.
    if _staged_has_fields(connection, staged) and market_version(connection) >= 2:  # noqa: PLR2004 -- fields arrive in v2
        names.append(PRICE_FIELDS[0])
    selected = ", ".join("?" if name == "generation_id" else _quote(name) for name in names)
    target = ", ".join(_quote(name) for name in names)
    _ = connection.execute(
        f"INSERT INTO {_quote(request.domain)} ({target}) SELECT {selected} FROM {staged}",  # noqa: S608 -- code-owned schema and validated identifier
        [request.generation_id],
    )


def _quote(name: str) -> str:
    if not isinstance(name, str) or _IDENTIFIER.fullmatch(name) is None:
        raise ValueError("bulk identifiers must be plain SQL names")
    return f'"{name}"'


def _require_version(connection: duckdb.DuckDBPyConnection, needed: int, what: str) -> None:
    if market_version(connection) < needed:
        raise ValueError(f"{what} needs core schema v{needed}; run aas db migrate --to {needed}")


def _staged_has_fields(connection: duckdb.DuckDBPyConnection, staged: str) -> bool:
    described = connection.execute(f"DESCRIBE SELECT * FROM {staged}").fetchall()  # noqa: S608 -- quoted validated identifier
    return any(row[0] == PRICE_FIELDS[0] for row in described)


def _staged_columns(connection: duckdb.DuckDBPyConnection, domain: str, staged: str) -> bool:
    """Require exactly the domain's typed columns; return whether a fields column is staged."""
    if staged.strip('"') in DOMAINS or staged.strip('"') == "market_generations":
        raise ValueError("bulk rows must be staged outside the market tables")
    described = {
        str(row[0]): str(row[1])
        for row in connection.execute(f"DESCRIBE SELECT * FROM {staged}").fetchall()  # noqa: S608 -- quoted validated identifier
    }
    expected = {
        name: kind.rstrip("?") for name, kind in COMMON + DOMAINS[domain] if name != "generation_id"
    }
    fields = domain == "prices" and PRICE_FIELDS[0] in described
    if fields:
        expected[PRICE_FIELDS[0]] = PRICE_FIELDS[1]
    if described != expected:
        raise ValueError("staged rows must carry exactly the domain's typed columns")
    if any(kind not in _COLUMN_TYPES for kind in described.values()):
        raise ValueError("staged rows must carry exactly the domain's typed columns")
    return fields


def _checks(domain: str, *, fields: bool, now_us: int) -> list[tuple[str, str]]:
    """normalize_rows' row rules as (SQL violation condition, its error message)."""
    columns = [(name, kind) for name, kind in COMMON + DOMAINS[domain] if name != "generation_id"]
    checks: list[tuple[str, str]] = []
    for name, kind in columns:
        column = _quote(name)
        if not kind.endswith("?"):
            checks.append((f"{column} IS NULL", "required market field is null"))
        base = kind.rstrip("?")
        if base == "VARCHAR":
            checks.append(
                (
                    f"regexp_full_match({column}, '{_BLANK}') OR contains({column}, chr(0))",
                    "market text must be nonempty",
                )
            )
        elif base == "DATE":
            checks.append(
                (
                    f"NOT isfinite({column}) OR year({column}) NOT BETWEEN 1 AND 9999",
                    "market date must be ISO YYYY-MM-DD",
                )
            )
        elif base == "DOUBLE":
            checks.append((f"NOT isfinite({column})", "feature must be a finite numeric value"))
    names = {name for name, _ in columns}
    checks.extend(
        (
            (
                "NOT regexp_full_match(source_row_hash, '[0-9a-f]{64}')",
                "invalid source row hash",
            ),
            (f"ingested_at_us > {now_us}", "ingestion timestamp cannot be in the future"),
            (
                "available_at_us > ingested_at_us OR revision_known_at_us > ingested_at_us",
                "ingestion cannot precede source knowledge",
            ),
            (
                "available_at_us < 0 OR revision_known_at_us < 0 OR ingested_at_us < 0",
                "market timestamps cannot be negative",
            ),
        )
    )
    if "value_state" in names:
        checks.append((f"value_state NOT IN {_STATES}", "unknown market value state"))
    value = "close" if domain == "prices" else "rate" if domain == "fx_rates" else "value"
    if value in names and "value_state" in names:
        checks.append(
            (
                f"({_quote(value)} IS NOT NULL) <> (value_state = 'present')",
                "market value and missing state disagree",
            )
        )
    if domain == "prices":
        checks.append(("price_role NOT IN ('canonical','reference')", "unknown market price role"))
        checks.append(
            (
                " OR ".join(
                    f"{_quote(name)} < 0" for name in ("open", "high", "low", "close", "volume")
                ),
                "market prices/volume cannot be negative",
            )
        )
        checks.append(
            (
                "basis <> 'unadjusted' AND price_role <> 'reference'",
                "adjusted provider prices must remain reference data",
            )
        )
    if fields:
        checks.append(
            ("\"fields\" IS NULL OR \"fields\" NOT IN ('ohlcv','close')",
             "price fields must be ohlcv or close")
        )  # fmt: skip
        checks.append(
            (
                (
                    "\"fields\" = 'close' AND (price_role <> 'reference' OR \"open\" IS NOT NULL "
                    'OR "high" IS NOT NULL OR "low" IS NOT NULL OR "volume" IS NOT NULL)'
                ),
                "a close-only price is reference data with no open, high, low or volume",
            )
        )
    return checks


def _row_bytes_sql(columns: tuple[tuple[str, str], ...], generation_id: str) -> str:
    """An SQL upper bound on any one row's aas-rowset-v1 encoding."""
    terms = []
    for name, rowset_type in rowset_schema(columns):
        if name == "generation_id":
            terms.append(str(_TEXT_FRAMING + len(generation_id.encode())))
        elif rowset_type == "text":
            terms.append(
                f"{_TEXT_FRAMING} + coalesce(max(octet_length(encode({_quote(name)}))), 0)"
            )
        else:
            terms.append(str(_FIXED_WIDTH[rowset_type]))
    return " + ".join(terms)


def _stats(
    connection: duckdb.DuckDBPyConnection,
    domain: str,
    staged: str,
    *,
    fields: bool,
    generation_id: str,
) -> _Stats:
    checks = _checks(domain, fields=fields, now_us=time.time_ns() // 1000)
    text = [
        name
        for name, kind in COMMON + DOMAINS[domain]
        if name in {*NATURAL_KEYS[domain], "record_id"} and kind.rstrip("?") == "VARCHAR"
    ]
    identity_characters = " + ".join(f"coalesce(length({_quote(name)}), 0)" for name in text)
    close = "bool_or(\"fields\" = 'close')" if fields else "false"
    # With a fields column staged, bound the wider of the two shapes the delta can take.
    row_bytes = _row_bytes_sql(delta_columns(domain, fields=fields), generation_id)
    violations = ", ".join(f"coalesce(bool_or({condition}), false)" for condition, _ in checks)
    sql = (
        f"SELECT count(*), coalesce({close}, false), {row_bytes}, "  # noqa: S608 -- code-owned schema and validated identifier
        f"coalesce(max({identity_characters}), 0), {violations} FROM {staged}"
    )
    result = connection.execute(sql).fetchone()
    if result is None:
        raise ValueError("staged rows could not be read")
    for (_, message), violated in zip(checks, result[4:], strict=True):
        if violated:
            raise ValueError(message)
    return _Stats(int(result[0]), bool(result[1]), int(result[2]), int(result[3]))


def _batch_rows(stats: _Stats, budget: ComputeBudget) -> tuple[int, int]:
    """Admit the hash and identity batches against the allocation before any fetch."""
    available = budget.available_bytes - stats.row_bytes
    hash_row = 2 * stats.row_bytes + _ROW_OBJECT_BYTES
    identity_values = 1 + max(len(keys) for keys in NATURAL_KEYS.values())
    identity_row = (
        _IDENTITY_ROW_BYTES
        + _IDENTITY_VALUE_BYTES * identity_values
        + text_bytes(identity_values, stats.identity_characters)
        + (_IDENTITY_CHARACTER_BYTES - TEXT_CHARACTER_BYTES) * stats.identity_characters
    )
    hash_rows = min(BATCH_ROWS, max(available, 0) // hash_row)
    identity_rows = min(BATCH_ROWS, budget.available_bytes // identity_row)
    if hash_rows < 1 or identity_rows < 1:
        raise ComputeResourceError(
            f"one staged row ({stats.row_bytes} encoded bytes) exceeds the admitted "
            f"materialization budget {budget.available_bytes} bytes"
        )
    return hash_rows, identity_rows


# Text that json.dumps writes verbatim between quotes: printable ASCII except '"' and '\\'.
JSON_PLAIN: Final = r"[\x{20}\x{21}\x{23}-\x{5b}\x{5d}-\x{7e}]*"


def record_identity_sql(domain: str) -> tuple[str, str]:
    """The aas-record-v1 record ID in SQL, and the rows whose keys it covers exactly.

    For those rows the concatenated text is byte for byte what ``record_identity``
    serializes (compact separators, no key sorting inside lists, ``null`` for NULL,
    ISO dates); every other row is checked in Python.
    """
    kinds = dict(COMMON + DOMAINS[domain])
    parts = []
    plain = []
    for name in NATURAL_KEYS[domain]:
        column = _quote(name)
        base = kinds[name].rstrip("?")
        if base == "BIGINT":
            value = f"CAST({column} AS VARCHAR)"
        elif base in {"VARCHAR", "DATE"}:
            value = f"'\"' || CAST({column} AS VARCHAR) || '\"'"
        else:
            raise ValueError(f"unsupported natural key type {base}")
        if base == "VARCHAR":
            plain.append(f"({column} IS NULL OR regexp_full_match({column}, '{JSON_PLAIN}'))")
        parts.append(f"'[\"{name}\",' || coalesce({value}, 'null') || ']'")
    document = f'\'["aas-record-v1","{domain}",[\' || ' + " || ',' || ".join(parts) + " || ']]'"
    return f"sha256({document})", " AND ".join(plain) or "true"


def _check_identities(
    connection: duckdb.DuckDBPyConnection, domain: str, staged: str, batch_rows: int
) -> None:
    """Every staged record_id is aas-record-v1 of its natural key.

    Keys that need no JSON escaping are checked in one SQL pass; the rest are
    recomputed with ``record_identity`` batch by batch.
    """
    expected, plain = record_identity_sql(domain)
    mismatched = connection.execute(
        f"SELECT count(*) FROM {staged} WHERE ({plain}) AND record_id <> {expected}"  # noqa: S608 -- code-owned schema and validated identifier
    ).fetchone()
    if mismatched is None or mismatched[0]:
        raise ValueError("market record identity does not match its natural key")
    names = NATURAL_KEYS[domain]
    selected = ", ".join(_quote(name) for name in ("record_id", *names))
    result = connection.execute(f"SELECT {selected} FROM {staged} WHERE NOT ({plain})")  # noqa: S608 -- code-owned schema and validated identifier
    while batch := result.fetchmany(batch_rows):
        for row in batch:
            if row[0] != record_identity(domain, row[1:]):
                raise ValueError("market record identity does not match its natural key")


def _u32(expression: str) -> str:
    return f"unhex(lpad(to_hex({expression}), 8, '0'))"


def float_bits_sql(column: str) -> str:
    """The IEEE-754 binary64 bits of a finite DOUBLE, with -0.0 as +0.0, as a HUGEINT."""
    magnitude = f"abs({column})"
    rough = f"CAST(floor(log2({magnitude})) AS INTEGER)"
    # log2 can land one either side of an exact power of two; pow(2, k) is exact.
    exponent = (
        f"({rough} - CASE WHEN pow(2.0, {rough}) > {magnitude} THEN 1 ELSE 0 END"
        f" + CASE WHEN {rough} < 1023 AND pow(2.0, {rough} + 1) <= {magnitude}"
        " THEN 1 ELSE 0 END)"
    )
    normal = (
        f"CAST({exponent} + 1023 AS HUGEINT) * 4503599627370496"
        f" + CAST(({magnitude} / pow(2.0, {exponent}) - 1.0) * 4503599627370496.0 AS HUGEINT)"
    )
    subnormal = f"CAST({magnitude} / pow(2.0, -1074) AS HUGEINT)"
    return (
        f"(CASE WHEN {column} = 0 THEN 0::HUGEINT ELSE"
        f" CASE WHEN {column} < 0 THEN 9223372036854775808::HUGEINT ELSE 0::HUGEINT END"
        f" + CASE WHEN {magnitude} < 2.2250738585072014e-308 THEN {subnormal} ELSE {normal} END"
        " END)"
    )


def encoded_cell_sql(name: str, rowset_type: str) -> str:
    """One cell's aas-rowset-v1 encoding as an SQL BLOB expression (see rowset.py)."""
    column = _quote(name)
    if rowset_type == "text":
        body = f"unhex('01') || {_u32(f'octet_length(encode({column}))')} || encode({column})"
    elif rowset_type in {"int", "utc_us"}:
        tag = "02" if rowset_type == "int" else "04"
        body = f"unhex('{tag}') || unhex(lpad(to_hex({column}), 16, '0'))"
    elif rowset_type == "date":
        body = f"unhex('050000000a') || encode(CAST({column} AS VARCHAR))"
    elif rowset_type == "decimal":
        # Stored DECIMAL(38,12) values reach Python with exponent -12; the coefficient
        # is the unscaled magnitude without leading zeros, and zero is unsigned.
        digits = (
            f"coalesce(nullif(ltrim(replace(replace(CAST({column} AS VARCHAR), '-', ''),"
            " '.', ''), '0'), ''), '0')"
        )
        body = (
            f"unhex('03') || CASE WHEN {column} < 0 THEN unhex('01') ELSE unhex('00') END"
            f" || unhex('fffffff4') || {_u32(f'length({digits})')} || encode({digits})"
        )
    elif rowset_type == "float":
        body = f"unhex('06') || unhex(lpad(to_hex({float_bits_sql(column)}), 16, '0'))"
    elif rowset_type == "bool":
        body = f"unhex('07') || CASE WHEN {column} THEN unhex('01') ELSE unhex('00') END"
    else:
        raise ValueError(f"unsupported rowset type {rowset_type!r}")
    return f"(CASE WHEN {column} IS NULL THEN unhex('00') ELSE {body} END)"


def _stream_delta(  # noqa: PLR0913 -- one delta's exact hashing inputs
    connection: duckdb.DuckDBPyConnection,
    domain: str,
    relation: str,
    parameters: list[object],
    *,
    generation_id: str,
    close_rows: bool,
    count: int,
    batch_rows: int,
) -> str:
    """The marker delta hash of ``relation``'s rows under ``generation_id``."""
    columns = delta_columns(domain, fields=close_rows)
    generation = generation_id.encode()
    constant = b"\x01" + len(generation).to_bytes(4, "big") + generation
    cells = [
        f"unhex('{constant.hex()}')"
        if name == "generation_id"
        else encoded_cell_sql(name, rowset_type)
        for name, rowset_type in rowset_schema(columns)
    ]
    digest = stream_rowset(
        connection,
        rowset_schema(columns),
        cells,
        relation,
        parameters,
        count=count,
        batch_rows=batch_rows,
    )
    return delta_digest(columns, digest)


def stream_rowset(  # noqa: PLR0913 -- one rowset's exact hashing inputs
    connection: duckdb.DuckDBPyConnection,
    schema: tuple[tuple[str, str], ...],
    cells: list[str],
    relation: str,
    parameters: list[object],
    *,
    count: int,
    batch_rows: int,
) -> str:
    """The aas-rowset-v1 digest of ``relation``, each row encoded in DuckDB by ``cells``.

    ``cells`` holds one SQL BLOB expression per schema field, in schema order, each
    producing that field's aas-rowset-v1 cell encoding (see ``encoded_cell_sql``).
    DuckDB sorts the encodings and ``RowsetStream`` digests them batch by batch, so
    the digest equals ``rowset.rowset_hash`` over the same rows.
    """
    stream = RowsetStream(schema, count)
    result = connection.execute(
        f"SELECT {' || '.join(cells)} AS encoded FROM ({relation}) ORDER BY encoded",  # noqa: S608 -- code-owned encoders over a code-owned relation
        parameters,
    )
    while batch := result.fetchmany(batch_rows):
        for (encoded,) in batch:
            stream.update(encoded)
    return stream.hexdigest()


def _check_revisions(
    connection: duckdb.DuckDBPyConnection,
    domain: str,
    relation: str,
    parameters: list[object],
    ancestors: list[str],
) -> None:
    """market._validate_revisions for one delta against the heads its ancestors left."""
    table = _quote(domain)
    sql = f"""
WITH delta AS (
 SELECT record_id, revision_id, supersedes_revision_id, op, available_at_us,
        revision_known_at_us FROM ({relation})
), prior AS (
 SELECT p.record_id, p.revision_id, p.available_at_us, p.revision_known_at_us, g.sequence
 FROM {table} p JOIN market_generations g ON g.generation_id = p.generation_id
 WHERE p.generation_id IN (SELECT unnest(?::VARCHAR[]))
   AND p.record_id IN (SELECT record_id FROM delta)
), head AS (
 SELECT * FROM prior
 QUALIFY row_number() OVER (PARTITION BY record_id ORDER BY sequence DESC) = 1
)
SELECT
 (SELECT count(*) - count(DISTINCT record_id) FROM delta) > 0,
 EXISTS (SELECT 1 FROM delta d JOIN prior p USING (record_id, revision_id)),
 EXISTS (SELECT 1 FROM delta WHERE op NOT IN {_OPS}),
 EXISTS (SELECT 1 FROM delta d LEFT JOIN head h USING (record_id) WHERE d.op = 'ASSERT'
         AND (d.supersedes_revision_id IS NOT NULL OR h.record_id IS NOT NULL)),
 EXISTS (SELECT 1 FROM delta d LEFT JOIN head h USING (record_id)
         WHERE d.op IN ('SUPERSEDE','TOMBSTONE')
         AND (d.supersedes_revision_id IS NULL
              OR h.revision_id IS DISTINCT FROM d.supersedes_revision_id)),
 EXISTS (SELECT 1 FROM delta d JOIN head h USING (record_id)
         WHERE d.op IN ('SUPERSEDE','TOMBSTONE')
         AND h.revision_id = d.supersedes_revision_id
         AND (d.available_at_us < h.available_at_us
              OR d.revision_known_at_us < h.revision_known_at_us))
"""  # noqa: S608 -- code-owned schema over a code-owned relation
    result = connection.execute(sql, [*parameters, ancestors]).fetchone()
    messages = (
        "one delta can contain only one revision per natural record",
        "duplicate market revision",
        "unsupported market revision operation",
        "ambiguous repeated ASSERT for natural key",
        "revision must supersede the same record's current ancestor",
        "revision knowledge cannot move backwards",
    )
    if result is None:
        raise ValueError("delta revisions could not be read")
    for message, violated in zip(messages, result, strict=True):
        if violated:
            raise ValueError(message)


def verify_chain_links(chain: list[dict[str, object]]) -> None:
    """Each marker's chain hash follows from its parent's and its own recorded fields."""
    parent_hash: object = None
    expected_dataset = chain[-1]["dataset_id"]
    expected_domain = chain[-1]["domain"]
    for ordinal, marker in enumerate(chain, 1):
        if marker["dataset_id"] != expected_dataset or marker["domain"] != expected_domain:
            raise ValueError("generation chain crosses dataset/domain ownership")
        domain = str(marker["domain"])
        if (
            domain not in DOMAINS
            or marker["record_schema"] != RECORD_SCHEMA
            or marker["sequence"] != ordinal
        ):
            raise ValueError("invalid generation schema/chain sequence")
        link = chain_digest(
            parent_chain_hash=parent_hash,
            dataset_id=marker["dataset_id"],
            version=marker["version"],
            generation_id=marker["generation_id"],
            domain=domain,
            delta_hash=str(marker["delta_hash"]),
            parent_id=marker["parent_id"],
            operation_id=marker["operation_id"],
            request_hash=marker["request_hash"],
            row_count=marker["row_count"],
        )
        if link != marker["chain_hash"]:
            raise ValueError("market generation chain link mismatch")
        parent_hash = link


def _verify_rows(
    connection: duckdb.DuckDBPyConnection,
    chain: list[dict[str, object]],
    index: int,
    budget: ComputeBudget,
) -> None:
    marker = chain[index]
    domain = str(marker["domain"])
    generation_id = str(marker["generation_id"])
    table = _quote(domain)
    fields = domain == "prices" and market_version(connection) >= 2  # noqa: PLR2004 -- fields arrive in v2
    close = "bool_or(\"fields\" = 'close')" if fields else "false"
    columns = delta_columns(domain, fields=fields)
    floats = [name for name, kind in DOMAINS[domain] if kind.rstrip("?") == "DOUBLE"]
    finite = " OR ".join(f"NOT isfinite({_quote(name)})" for name in floats) or "false"
    result = connection.execute(
        f"SELECT count(*), coalesce({close}, false), {_row_bytes_sql(columns, generation_id)}, "  # noqa: S608 -- code-owned schema
        f"coalesce(bool_or({finite}), false) FROM {table} WHERE generation_id=?",
        [generation_id],
    ).fetchone()
    if result is None or result[0] != marker["row_count"]:
        raise ValueError("market generation logical hash/count mismatch")
    if result[3]:
        raise ValueError("float fields must be finite")
    for other in DOMAINS:
        if (
            other != domain
            and DOMAIN_VERSIONS[other] <= market_version(connection)
            and connection.execute(
                f"SELECT 1 FROM {_quote(other)} WHERE generation_id=? LIMIT 1",  # noqa: S608 -- fixed domain allowlist
                [generation_id],
            ).fetchone()
        ):
            raise ValueError("generation contains rows in a foreign domain")
    stats = _Stats(int(result[0]), bool(result[1]), int(result[2]), 0)
    hash_rows, _ = _batch_rows(stats, budget)
    relation = f"SELECT * FROM {table} WHERE generation_id=?"  # noqa: S608 -- code-owned schema
    delta = _stream_delta(
        connection,
        domain,
        relation,
        [generation_id],
        generation_id=generation_id,
        close_rows=stats.close_rows,
        count=stats.count,
        batch_rows=hash_rows,
    )
    if delta != marker["delta_hash"]:
        raise ValueError("market generation logical hash/count mismatch")
    _check_revisions(
        connection,
        domain,
        relation,
        [generation_id],
        [str(ancestor["generation_id"]) for ancestor in chain[:index]],
    )
