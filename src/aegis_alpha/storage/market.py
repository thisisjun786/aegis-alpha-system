"""Native immutable generations, exact decimals, and revision-aware cutoff reads."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.storage.market_schema import (
    COMMON,
    DDL,
    DOMAINS,
    NATURAL_KEYS,
    rowset_encoding_bytes,
    text_bytes,
    text_columns,
)

if TYPE_CHECKING:
    import duckdb

_SCHEMA = "aas-market-rowset-v1"
_SCHEMA_CHECKSUM = hashlib.sha256(DDL.encode()).hexdigest()
_MARKER_COLUMNS = (
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
_SHA256_LENGTH = 64
_MAX_READ_ROWS = 100_000


def initialize_market(connection: duckdb.DuckDBPyConnection, installation_id: str) -> None:
    tables = connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    if ("store_info",) in tables:
        validate_market(connection, installation_id)
        return
    if tables:
        raise ValueError("refusing to initialize an unrecognized DuckDB store")
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(DDL)
        connection.execute(
            "INSERT INTO store_info VALUES (?, ?, 1, 'market')", [uuid.uuid4().hex, installation_id]
        )
        connection.execute(
            "INSERT INTO schema_migrations VALUES (1, ?, ?)",
            [_SCHEMA_CHECKSUM, time.time_ns() // 1000],
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def validate_market(connection: duckdb.DuckDBPyConnection, installation_id: str) -> None:
    rows = connection.execute(
        "SELECT installation_id,schema_version,kind FROM store_info"
    ).fetchall()
    checksum = connection.execute(
        "SELECT checksum FROM schema_migrations WHERE version=1"
    ).fetchall()
    if rows != [(installation_id, 1, "market")] or checksum != [(_SCHEMA_CHECKSUM,)]:
        raise ValueError("market store identity or schema checksum mismatch")


def _cell(value: object, kind: str) -> object:  # noqa: C901, PLR0911, PLR0912 -- fixed typed scalar boundary
    if value is None:
        if not kind.endswith("?"):
            raise ValueError("required market field is null")
        return None
    base = kind.rstrip("?")
    if base == "VARCHAR":
        if not isinstance(value, str) or not value.strip() or "\x00" in value:
            raise ValueError("market text must be nonempty")
        return value
    if base == "BIGINT":
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError("market timestamp/count must be an exact int64")
        return value
    if base == "DATE":
        if isinstance(value, str):
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError("market date must be ISO YYYY-MM-DD")
            return parsed
        if type(value) is date:
            return value
        raise ValueError("market date requires ISO date, without timestamp guessing")
    if base == "DOUBLE":
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError("feature must be a finite numeric value")
        return float(value)
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise TypeError("exact decimal fields reject floating point inputs")
    try:
        decimal = Decimal(value)
        with localcontext() as context:
            context.prec = 50
            quantized = decimal.quantize(Decimal("0.000000000001"))
        if not decimal.is_finite() or decimal != quantized or abs(decimal) >= Decimal(10) ** 26:
            raise ValueError("decimal exceeds exact DECIMAL(38,12) representation")
    except InvalidOperation:
        raise ValueError("invalid exact decimal") from None
    return quantized


def _json_cell(value: object) -> str | int | float | None:
    if value is None or type(value) in {str, int, float}:
        return cast("str | int | float | None", value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError("unsupported row identity value")


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def normalize_rows(
    domain: str, generation_id: str, rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    if domain not in DOMAINS or not rows:
        raise ValueError("unknown market domain or empty generation")
    schema = COMMON + DOMAINS[domain]
    expected = {name for name, _ in schema} - {"generation_id", "record_id"}
    normalized = []
    for input_row in rows:
        if set(input_row) - expected - {"record_id", "generation_id"} or expected - set(input_row):
            raise ValueError("market row has missing or unknown typed fields")
        row = {
            name: _cell(input_row.get(name), kind)
            for name, kind in schema
            if name not in {"generation_id", "record_id"}
        }
        natural = [[name, _json_cell(row[name])] for name in NATURAL_KEYS[domain]]
        record_id = _digest(["aas-record-v1", domain, natural])
        if (
            input_row.get("record_id", record_id) != record_id
            or input_row.get("generation_id", generation_id) != generation_id
        ):
            raise ValueError("market record identity does not match its natural key")
        row.update(generation_id=generation_id, record_id=record_id)
        _validate_row(domain, row)
        normalized.append(row)
    return normalized


def _validate_row(domain: str, row: dict[str, object]) -> None:  # noqa: C901 -- domain null/time/value boundary
    source_hash = row["source_row_hash"]
    if (
        not isinstance(source_hash, str)
        or len(source_hash) != _SHA256_LENGTH
        or any(c not in "0123456789abcdef" for c in source_hash)
    ):
        raise ValueError("invalid source row hash")
    ingested = cast("int", row["ingested_at_us"])
    if ingested > time.time_ns() // 1000:
        raise ValueError("ingestion timestamp cannot be in the future")
    if any(
        row[field] is not None and cast("int", row[field]) > ingested
        for field in ("available_at_us", "revision_known_at_us")
    ):
        raise ValueError("ingestion cannot precede source knowledge")
    for field in ("available_at_us", "revision_known_at_us", "ingested_at_us"):
        if row[field] is not None and cast("int", row[field]) < 0:
            raise ValueError("market timestamps cannot be negative")
    state = row.get("value_state")
    if state is not None and state not in {
        "present",
        "missing",
        "not_collected",
        "unsupported",
        "invalid",
    }:
        raise ValueError("unknown market value state")
    if domain == "prices" and row["price_role"] not in {"canonical", "reference"}:
        raise ValueError("unknown market price role")
    value_field = "close" if domain == "prices" else "rate" if domain == "fx_rates" else "value"
    if value_field in row and (row[value_field] is not None) != (state == "present"):
        raise ValueError("market value and missing state disagree")
    if domain == "prices":
        for field in ("open", "high", "low", "close", "volume"):
            value = row[field]
            if value is not None and isinstance(value, Decimal) and value < 0:
                raise ValueError("market prices/volume cannot be negative")
        if row["basis"] != "unadjusted" and row["price_role"] != "reference":
            raise ValueError("adjusted provider prices must remain reference data")


def _rows(
    connection: duckdb.DuckDBPyConnection, domain: str, generations: list[str]
) -> list[dict[str, object]]:
    names = [name for name, _ in COMMON + DOMAINS[domain]]
    placeholders = ",".join("?" for _ in generations)
    selected = ", ".join(f'"{name}"' for name in names)
    sql = f'SELECT {selected} FROM "{domain}" WHERE generation_id IN ({placeholders})'  # noqa: S608 -- code-owned schema
    return [
        dict(zip(names, row, strict=True))
        for row in connection.execute(sql, generations).fetchall()
    ]


def marker_for(connection: duckdb.DuckDBPyConnection, generation_id: str) -> dict[str, object]:
    result = connection.execute(
        "SELECT " + ",".join(_MARKER_COLUMNS) + " FROM market_generations WHERE generation_id=?",  # noqa: S608 -- code-owned columns
        [generation_id],
    ).fetchone()
    if result is None:
        raise ValueError("market generation does not exist")
    return dict(zip(_MARKER_COLUMNS, result, strict=True))


def generation_chain(
    connection: duckdb.DuckDBPyConnection, generation_id: str
) -> list[dict[str, object]]:
    chain = []
    seen = set()
    current: str | None = generation_id
    while current is not None:
        if current in seen:
            raise ValueError("market generation cycle")
        seen.add(current)
        marker = marker_for(connection, current)
        chain.append(marker)
        parent = marker["parent_id"]
        current = str(parent) if parent is not None else None
    chain.reverse()
    return chain


def _delta_hash(domain: str, rows: list[dict[str, object]]) -> str:
    from aegis_alpha.storage.rowset import rowset_hash  # noqa: PLC0415 -- shared typed codec

    kinds = {
        "VARCHAR": "text",
        "BIGINT": "int",
        "DATE": "date",
        "DOUBLE": "float",
        "DECIMAL(38,12)": "decimal",
    }
    schema = tuple(
        (name, "utc_us" if name.endswith("_us") else kinds[kind.rstrip("?")])
        for name, kind in COMMON + DOMAINS[domain]
    )
    return _digest([_SCHEMA, COMMON + DOMAINS[domain], rowset_hash(schema, rows)])


def _validate_revisions(  # noqa: C901 -- explicit immutable chain validation
    prior: list[dict[str, object]], rows: list[dict[str, object]]
) -> None:
    heads = {}
    known = {}
    current_records = [row["record_id"] for row in rows]
    if len(set(current_records)) != len(current_records):
        raise ValueError("one delta can contain only one revision per natural record")
    for row in prior:
        known[(row["record_id"], row["revision_id"])] = row
        heads[row["record_id"]] = row["revision_id"]
    for row in sorted(
        rows,
        key=lambda r: (
            str(r["record_id"]),
            r["revision_known_at_us"] if r["revision_known_at_us"] is not None else 2**63,
            str(r["revision_id"]),
        ),
    ):
        key = (row["record_id"], row["revision_id"])
        if key in known:
            raise ValueError("duplicate market revision")
        parent = row["supersedes_revision_id"]
        if row["op"] == "ASSERT":
            if parent is not None or row["record_id"] in heads:
                raise ValueError("ambiguous repeated ASSERT for natural key")
        elif row["op"] in {"SUPERSEDE", "TOMBSTONE"}:
            if parent is None or heads.get(row["record_id"]) != parent:
                raise ValueError("revision must supersede the same record's current ancestor")
            predecessor = known[(row["record_id"], parent)]
            for field in ("available_at_us", "revision_known_at_us"):
                now, then = row[field], predecessor[field]
                if then is not None and now is not None and cast("int", now) < cast("int", then):
                    raise ValueError("revision knowledge cannot move backwards")
        else:
            raise ValueError("unsupported market revision operation")
        known[key] = row
        heads[row["record_id"]] = row["revision_id"]


def plan_generation(  # noqa: PLR0913 -- immutable publication identity
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset_id: str,
    version: str,
    generation_id: str,
    operation_id: str,
    request_hash: str,
    parent_id: str | None,
    domain: str,
    rows: list[dict[str, object]],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    normalized = normalize_rows(domain, generation_id, rows)
    delta = _delta_hash(domain, normalized)
    existing = connection.execute(
        "SELECT generation_id FROM market_generations WHERE generation_id=? OR operation_id=?",
        [generation_id, operation_id],
    ).fetchall()
    if existing:
        marker = verify_generation(connection, str(existing[0][0]))
        expected = {
            "dataset_id": dataset_id,
            "version": version,
            "generation_id": generation_id,
            "operation_id": operation_id,
            "request_hash": request_hash,
            "parent_id": parent_id,
            "domain": domain,
            "delta_hash": delta,
        }
        if any(marker[key] != value for key, value in expected.items()):
            raise ValueError("generation or operation ID conflicts with existing content")
        return marker, normalized
    latest = connection.execute(
        "SELECT generation_id FROM market_generations WHERE dataset_id=? "
        "ORDER BY sequence DESC LIMIT 1",
        [dataset_id],
    ).fetchone()
    if (latest[0] if latest else None) != parent_id:
        raise ValueError("market parent changed or awaits catalog recovery")
    if connection.execute(
        "SELECT 1 FROM market_generations WHERE dataset_id=? AND version=?", [dataset_id, version]
    ).fetchone():
        raise ValueError("dataset version already identifies another generation")
    chain = generation_chain(connection, parent_id) if parent_id else []
    if chain and (chain[-1]["dataset_id"] != dataset_id or chain[-1]["domain"] != domain):
        raise ValueError("generation parent belongs to another dataset/domain")
    prior = []
    for ancestor in chain:
        prior.extend(_rows(connection, domain, [str(ancestor["generation_id"])]))
    _validate_revisions(prior, normalized)
    sequence = len(chain) + 1
    chain_hash = _digest(
        [
            _SCHEMA,
            chain[-1]["chain_hash"] if chain else None,
            dataset_id,
            version,
            generation_id,
            domain,
            delta,
            parent_id,
            operation_id,
            request_hash,
            len(normalized),
        ]
    )
    marker: dict[str, object] = dict(
        zip(
            _MARKER_COLUMNS,
            (
                generation_id,
                dataset_id,
                version,
                parent_id,
                sequence,
                domain,
                _SCHEMA,
                delta,
                chain_hash,
                len(normalized),
                operation_id,
                request_hash,
            ),
            strict=True,
        )
    )
    return marker, normalized


def publish_generation(  # noqa: PLR0913 -- exact publication pins
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset_id: str,
    version: str,
    generation_id: str,
    operation_id: str,
    request_hash: str,
    parent_id: str | None,
    domain: str,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    marker, normalized = plan_generation(
        connection,
        dataset_id=dataset_id,
        version=version,
        generation_id=generation_id,
        operation_id=operation_id,
        request_hash=request_hash,
        parent_id=parent_id,
        domain=domain,
        rows=rows,
    )
    if connection.execute(
        "SELECT 1 FROM market_generations WHERE generation_id=?", [generation_id]
    ).fetchone():
        return marker
    names = [name for name, _ in COMMON + DOMAINS[domain]]
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            "INSERT INTO market_generations VALUES ("  # noqa: S608 -- placeholder count only
            + ",".join("?" for _ in _MARKER_COLUMNS)
            + ")",
            list(marker.values()),
        )
        selected = ", ".join(f'"{name}"' for name in names)
        placeholders = ",".join("?" for _ in names)
        sql = f'INSERT INTO "{domain}" ({selected}) VALUES ({placeholders})'  # noqa: S608 -- schema allowlist
        connection.executemany(sql, [[row[name] for name in names] for row in normalized])
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    return marker


def verify_generation(
    connection: duckdb.DuckDBPyConnection, generation_id: str
) -> dict[str, object]:
    chain = generation_chain(connection, generation_id)
    _ = _verified_chain_rows(connection, chain)
    return chain[-1]


def _verified_chain_rows(
    connection: duckdb.DuckDBPyConnection, chain: list[dict[str, object]]
) -> list[dict[str, object]]:
    parent_hash = None
    prior: list[dict[str, object]] = []
    expected_dataset = chain[-1]["dataset_id"]
    expected_domain = chain[-1]["domain"]
    for ordinal, marker in enumerate(chain, 1):
        domain = str(marker["domain"])
        if marker["dataset_id"] != expected_dataset or marker["domain"] != expected_domain:
            raise ValueError("generation chain crosses dataset/domain ownership")
        if (
            domain not in DOMAINS
            or marker["record_schema"] != _SCHEMA
            or marker["sequence"] != ordinal
        ):
            raise ValueError("invalid generation schema/chain sequence")
        rows = _rows(connection, domain, [str(marker["generation_id"])])
        for other in DOMAINS:
            if (
                other != domain
                and connection.execute(
                    f'SELECT 1 FROM "{other}" WHERE generation_id=? LIMIT 1',  # noqa: S608 -- fixed domain allowlist
                    [marker["generation_id"]],
                ).fetchone()
            ):
                raise ValueError("generation contains rows in a foreign domain")
        delta = _delta_hash(domain, rows)
        chain_hash = _digest(
            [
                _SCHEMA,
                parent_hash,
                marker["dataset_id"],
                marker["version"],
                marker["generation_id"],
                domain,
                delta,
                marker["parent_id"],
                marker["operation_id"],
                marker["request_hash"],
                marker["row_count"],
            ]
        )
        if (
            delta != marker["delta_hash"]
            or chain_hash != marker["chain_hash"]
            or len(rows) != marker["row_count"]
        ):
            raise ValueError("market generation logical hash/count mismatch")
        _validate_revisions(prior, rows)
        prior.extend(sorted(rows, key=lambda row: str(row["record_id"])))
        parent_hash = chain_hash
    return prior


def read_chain_rows(
    connection: duckdb.DuckDBPyConnection,
    generation_id: str,
    *,
    budget: ComputeBudget,
) -> tuple[Mapping[str, object], ...]:
    """Read every verified delta, oldest generation first, with immutable rows.

    Call under workspace admission, as for read_generation. The caller supplies
    its compute allocation; this function neither resolves host limits nor takes
    a second compute lease. Admission reserves DuckDB's share of that allocation
    and estimates Python rows, revision indexes, projection copies and hash buffers
    from actual counts and text lengths before fetching any delta. This is a
    conservative materialization estimate, not a process RSS or performance bound.
    An over-budget history raises ComputeResourceError, never a truncated success.
    DuckDB limits are lowered before queries and remain lowered on success/error;
    the caller still owns the connection. Tighter existing limits are never raised.

    Catalog visibility remains publication.read_dataset's responsibility. Each
    market delta is checked against its own marker, not the head's delta count.
    """
    import duckdb  # noqa: PLC0415 -- capacity errors at the budgeted query boundary

    try:
        _ = connection.execute(
            "SET threads = least(current_setting('threads'), ?)", [budget.duckdb_threads]
        )
        # Compare the same rounded-down display precision on both sides. Leave a
        # clearly tighter limit untouched; in the same display bucket conservatively
        # use its lower bound, since the exact existing bytes are not exposed.
        memory, budget_floor = cast(
            "tuple[int, int]",
            connection.execute(
                """SELECT parse_formatted_bytes(current_setting('memory_limit')),
                          parse_formatted_bytes(format_bytes(?))""",
                [budget.duckdb_memory_limit_bytes],
            ).fetchone(),
        )
        if memory >= budget_floor:
            _ = connection.execute(
                "SET memory_limit = ?", [f"{min(memory, budget.duckdb_memory_limit_bytes)}B"]
            )
        chain = generation_chain(connection, generation_id)
        _admit_chain_memory(connection, chain, budget)
        return tuple(MappingProxyType(row) for row in _verified_chain_rows(connection, chain))
    except duckdb.OutOfMemoryException as error:
        raise ComputeResourceError(
            "DuckDB cannot execute chain read within admitted memory limits"
        ) from error


def _admit_chain_memory(
    connection: duckdb.DuckDBPyConnection,
    chain: list[dict[str, object]],
    budget: ComputeBudget,
) -> None:
    # Fixed row/cell allowances cover Python objects, maps, indexes and copies;
    # text has room for four-byte Unicode plus simultaneous rowset encodings.
    estimated_bytes = 64 * 1024
    for marker in chain:
        domain = str(marker["domain"])
        if domain not in DOMAINS:
            raise ValueError("invalid generation schema/chain sequence")
        schema = COMMON + DOMAINS[domain]
        text_lengths = " + ".join(
            f'coalesce(length("{name}"), 0)'
            for name, kind in schema
            if kind.rstrip("?") == "VARCHAR"
        )
        count, characters = cast(
            "tuple[int, int]",
            connection.execute(
                f'SELECT count(*), coalesce(sum({text_lengths}), 0) FROM "{domain}" '  # noqa: S608 -- code-owned schema
                "WHERE generation_id=?",
                [marker["generation_id"]],
            ).fetchone(),
        )
        if count != marker["row_count"]:
            raise ValueError("market generation logical hash/count mismatch")
        # At most one text value per VARCHAR cell, which is an upper bound because a
        # NULL holds none. Charged by value and by character rather than by character
        # alone; see market_schema.text_bytes for why a flat per-character rate is
        # wrong at both ends of the range this history actually contains. This read
        # also hashes what it fetches, so it carries the rowset codec's encoded copies
        # beside the decoded values rather than only the values.
        values = count * text_columns(schema)
        estimated_bytes += (
            1024
            + count * (1024 + 256 * len(schema))
            + text_bytes(values, characters)
            + rowset_encoding_bytes(values, characters)
        )
    available_bytes = budget.available_bytes
    if estimated_bytes > available_bytes:
        raise ComputeResourceError(
            f"full market chain memory estimate {estimated_bytes} exceeds "
            f"admitted materialization budget {available_bytes} bytes"
        )


def _validate_cutoffs(cutoff_us: int | None, ingestion_cutoff_us: int | None) -> None:
    for cutoff in (cutoff_us, ingestion_cutoff_us):
        if cutoff is not None and (type(cutoff) is not int or cutoff < 0):
            raise ValueError("cutoff must be UTC microseconds")


def project_heads(
    rows: Sequence[Mapping[str, object]],
    *,
    cutoff_us: int | None = None,
    ingestion_cutoff_us: int | None = None,
) -> list[dict[str, object]]:
    """Project a complete read_chain_rows history without mutating or limiting it.

    Input order is generation order, not knowledge-time order. With a strict
    cutoff, unknown knowledge is skipped; a known but unavailable superseding
    revision removes its prior head. Reference prices are excluded only in this
    strict mode. Without a cutoff this retains the legacy observed-head behavior.
    """
    _validate_cutoffs(cutoff_us, ingestion_cutoff_us)
    heads: dict[str, Mapping[str, object]] = {}
    for row in rows:
        record_id = str(row["record_id"])
        if (
            ingestion_cutoff_us is not None
            and cast("int", row["ingested_at_us"]) > ingestion_cutoff_us
        ):
            continue
        if cutoff_us is not None:
            known = row["revision_known_at_us"]
            available = row["available_at_us"]
            if known is None or cast("int", known) > cutoff_us:
                continue
            if available is None or cast("int", available) > cutoff_us:
                if row["op"] != "ASSERT":
                    _ = heads.pop(record_id, None)
                continue
            if row.get("price_role") == "reference":
                continue
        heads[record_id] = row
    return [dict(heads[key]) for key in sorted(heads) if heads[key]["op"] != "TOMBSTONE"]


def read_generation(
    connection: duckdb.DuckDBPyConnection,
    generation_id: str,
    *,
    cutoff_us: int | None = None,
    ingestion_cutoff_us: int | None = None,
    limit: int = 100,
) -> list[dict[str, object]]:
    if type(limit) is not int or not 1 <= limit <= _MAX_READ_ROWS:
        raise ValueError("limit must be between 1 and 100000")
    _validate_cutoffs(cutoff_us, ingestion_cutoff_us)
    # Preserve the legacy interface without requiring a new compute allocation.
    rows = _verified_chain_rows(connection, generation_chain(connection, generation_id))
    return project_heads(rows, cutoff_us=cutoff_us, ingestion_cutoff_us=ingestion_cutoff_us)[:limit]
