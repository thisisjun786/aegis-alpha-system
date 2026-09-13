"""Bounded immutable membership content, not resolution or execution eligibility.

The two v1 whole-document contracts are specified in dev-notes/design/membership-pins.md.
Connections and read lifetimes belong to the caller's admitted workspace.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast

from aegis_alpha.compute_resources import ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.state import atomic

_MAX_BYTES = 1024 * 1024
_MAX_CHARGE = 64 * 1024 * 1024
_I64_MAX = 2**63 - 1
_HASH_FORMAT = "aas-canonical-json-sha256-v1"
_INTERVAL = {
    "valid_from_us": "int",
    "valid_to_us": "int?",
    "known_from_us": "uint",
    "known_to_us": "uint?",
}
_INSTRUMENT = {"instrument_id": "text", "issuer_id": "text?", "asset_type": "text", "venue": "text"}
_ASSERTION = {
    "assertion_id": "text",
    "instrument_id": "text",
    "provider": "text",
    "namespace": "text",
    "token": "text",
    "valid_from_us": "int",
    "valid_to_us": "int?",
    "known_from_us": "uint",
    "supersedes_assertion_id": "text?",
    "source_snapshot_id": "text",
    "source_hash": "sha",
}
_ID_MEMBER = {"ordinal": "uint", "assertion_id": "text", **_INTERVAL}
_U_MEMBER = {"instrument_id": "text", **_INTERVAL, "source_snapshot_id": "text"}
_SOURCE = {
    "snapshot_id": "text",
    "provider": "text",
    "requested_at_us": "uint",
    "retrieved_at_us": "uint",
    "publication_at_us": "uint?",
    "status": "text",
}
_FILE = {"relative_path": "text", "byte_hash": "sha", "size_bytes": "uint"}
type Record = dict[str, object]
type Members = tuple[Mapping[str, object], ...]


def _text(value: object) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ValueError("identity/convention requires exact nonempty printable text")


def _digest(value: object) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("pin requires lowercase SHA256 hex")


def _integer(value: object, *, nonnegative: bool = True) -> None:
    if type(value) is not int or not (0 if nonnegative else -(2**63)) <= value <= _I64_MAX:
        raise ValueError("membership integer must be signed int64 with the required sign")


@dataclass(frozen=True, slots=True)
class IdentityPin:
    snapshot_id: str
    content_hash: str

    def __post_init__(self) -> None:
        _text(self.snapshot_id)
        _digest(self.content_hash)


@dataclass(frozen=True, slots=True)
class UniversePin:
    universe_id: str
    version: str
    content_hash: str

    def __post_init__(self) -> None:
        _text(self.universe_id)
        _text(self.version)
        _digest(self.content_hash)
        if self.version == "latest":
            raise ValueError("universe version must be exact")


@dataclass(frozen=True, slots=True)
class VerifiedMembership:
    pin: IdentityPin | UniversePin
    canonical_bytes: bytes
    members: Members


@dataclass(frozen=True, slots=True)
class VerifiedMemberships:
    identity: VerifiedMembership | None
    universe: VerifiedMembership | None


def _record(value: object, fields: Mapping[str, str]) -> Record:
    if not isinstance(value, dict) or value.keys() != fields.keys():
        raise ValueError("membership document has missing or unknown fields")
    record = cast("Record", value)
    for key, kind in fields.items():
        item = record[key]
        if kind.endswith("?") and item is None:
            continue
        match kind.rstrip("?"):
            case "text" | "version":
                _text(item)
                if kind == "version" and item == "latest":
                    raise ValueError("universe version must be exact")
            case "sha":
                _digest(item)
            case "int" | "uint":
                _integer(item, nonnegative=kind.startswith("uint"))
            case "array":
                if not isinstance(item, list):
                    raise ValueError("membership field must be an array")  # noqa: TRY004 -- public malformed-content ValueError contract
            case _:
                raise AssertionError("unknown internal field kind")
    return record.copy()


def _rows(value: object, fields: Mapping[str, str], keys: tuple[str, ...]) -> list[Record]:
    if not isinstance(value, list):
        raise ValueError("membership field must be an array")  # noqa: TRY004 -- public malformed-content ValueError contract
    rows = [_record(item, fields) for item in cast("list[object]", value)]
    identities = [tuple(row[key] for key in keys) for row in rows]
    if len(set(identities)) != len(rows):
        raise ValueError("duplicate membership reference")
    # Keys are validated scalar strings/integers, homogeneous at every position.
    return sorted(rows, key=lambda row: tuple(cast("str | int", row[key]) for key in keys))


def _interval(row: Record) -> None:
    for axis in ("valid", "known"):
        start, end = row[axis + "_from_us"], row.get(axis + "_to_us")
        if end is not None and cast("int", end) <= cast("int", start):
            raise ValueError("membership interval end must exceed start")


def _assertion_order(assertions: list[Record]) -> list[Record]:
    pending = {row["assertion_id"]: row for row in assertions}
    result: list[Record] = []
    while pending:
        ready = [row for row in pending.values() if row["supersedes_assertion_id"] not in pending]
        if not ready:
            raise ValueError("membership assertion predecessor cycle")
        for row in ready:
            result.append(row)
            del pending[row["assertion_id"]]
    return result


def _identity_intervals(members: list[Record], assertions: list[Record]) -> None:
    by_id = {row["assertion_id"]: row for row in assertions}
    groups: dict[tuple[object, ...], list[Record]] = {}
    for row in members:
        assertion = by_id[row["assertion_id"]]
        key = tuple(assertion[field] for field in ("provider", "namespace", "token"))
        prior = groups.setdefault(key, [])
        for other in prior:
            if all(
                (
                    other[axis + "_to_us"] is None
                    or cast("int", row[axis + "_from_us"]) < cast("int", other[axis + "_to_us"])
                )
                and (
                    row[axis + "_to_us"] is None
                    or cast("int", other[axis + "_from_us"]) < cast("int", row[axis + "_to_us"])
                )
                for axis in ("valid", "known")
            ):
                raise ValueError("identity snapshot interval overlap")
        prior.append(row)


def _sources(value: object) -> list[Record]:
    sources = _rows(value, {**_SOURCE, "files": "array"}, ("snapshot_id",))
    for source in sources:
        if source["status"] not in ("raw_verified", "quarantined") or cast(
            "int", source["retrieved_at_us"]
        ) < cast("int", source["requested_at_us"]):
            raise ValueError("invalid membership source evidence")
        files = _rows(source["files"], _FILE, ("relative_path",))
        for file in files:
            path = cast("str", file["relative_path"])
            if "\\" in path or any(part in ("", ".", "..") for part in path.split("/")):
                raise ValueError("unsafe membership source file path")
        source["files"] = files
    return sources


def _canonical(value: object, *, identity: bool) -> Record:
    root = {
        "schema": "text",
        "hash_format": "text",
        "instruments": "array",
        "members": "array",
        "sources": "array",
    }
    root.update(
        {"snapshot_id": "text", "assertions": "array"}
        if identity
        else {"universe_id": "text", "version": "version"}
    )
    body = _record(value, root)
    schema = "aas-identity-snapshot-v1" if identity else "aas-universe-version-v1"
    if body["schema"] != schema or body["hash_format"] != _HASH_FORMAT:
        raise ValueError("unsupported membership schema/hash format")
    instruments = _rows(body["instruments"], _INSTRUMENT, ("instrument_id",))
    sources = _sources(body["sources"])
    members = _rows(
        body["members"],
        _ID_MEMBER if identity else _U_MEMBER,
        ("assertion_id",) if identity else ("instrument_id", "valid_from_us", "known_from_us"),
    )
    body.update(instruments=instruments, members=members, sources=sources)
    for member in members:
        _interval(member)
    references = members
    if identity:
        assertions = _rows(body["assertions"], _ASSERTION, ("assertion_id",))
        if {row["assertion_id"] for row in assertions} != {row["assertion_id"] for row in members}:
            raise ValueError("membership assertion references must be exact")
        for ordinal, member in enumerate(members):
            if member["ordinal"] != ordinal:
                raise ValueError("membership ordinal must match canonical position")
        for assertion in assertions:
            _interval(assertion)
        body["assertions"] = assertions
        _admit(_body_charge(body))
        _ = _assertion_order(assertions)
        _identity_intervals(members, assertions)
        references = assertions
    if {row["instrument_id"] for row in instruments} != {
        row["instrument_id"] for row in references
    }:
        raise ValueError("membership instrument references must be exact")
    if {row["snapshot_id"] for row in sources} != {row["source_snapshot_id"] for row in references}:
        raise ValueError("membership source references must be exact")
    _admit(_body_charge(body))
    return body


def _records(body: Record, key: str) -> list[Record]:
    return cast("list[Record]", body[key])


def _body_charge(body: Record) -> int:
    rows = [
        *_records(body, "instruments"),
        *_records(body, "members"),
        *_records(body, "sources"),
        *cast("list[Record]", body.get("assertions", [])),
    ]
    for source in _records(body, "sources"):
        rows.extend(_records(source, "files"))
    byte_count = sum(
        len(value.encode("utf-8"))
        for row in rows
        for value in row.values()
        if isinstance(value, str)
    )
    byte_count += sum(
        len(cast("str", body[key]).encode("utf-8"))
        for key in ("snapshot_id", "universe_id", "version")
        if key in body
    )
    return 65536 + 16384 * len(rows) + 128 * byte_count


def _admit(charge: int, maximum: int = _MAX_CHARGE) -> None:
    if charge > maximum:
        raise ComputeResourceError("membership history exceeds admitted materialization budget")


def _payload(raw: bytes, expected: str, *, identity: bool) -> tuple[Record, bytes]:
    _digest(expected)
    if type(raw) is not bytes or len(raw) > _MAX_BYTES:
        raise ValueError("membership input exceeds byte limit or is not bytes")
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("membership incoming file SHA-256 mismatch")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise ValueError("membership document requires UTF-8 without BOM/NUL")
    try:
        _ = raw.decode("utf-8", errors="strict")
        body = _canonical(decode_json(raw), identity=identity)
    except (UnicodeError, RecursionError) as error:
        raise ValueError("invalid membership UTF-8 document") from error
    canonical = canonical_json_bytes(body)
    if len(canonical) > _MAX_BYTES:
        raise ValueError("membership canonical document exceeds byte limit")
    return body, canonical


@dataclass(frozen=True, slots=True)
class _Queries:
    prefix: str
    parameters: tuple[str, ...]
    tables: Mapping[str, Mapping[str, str]]


def _queries(pin: IdentityPin | UniversePin) -> _Queries:
    if isinstance(pin, IdentityPin):
        prefix = (
            "WITH m AS (SELECT * FROM identity_snapshot_members WHERE snapshot_id=?), "
            "a AS (SELECT * FROM identity_assertions "
            "WHERE assertion_id IN (SELECT assertion_id FROM m)), "
        )
        ref = "a"
        tables = {"m": _ID_MEMBER, "a": _ASSERTION, "i": _INSTRUMENT, "s": _SOURCE, "f": _FILE}
        parameters = (pin.snapshot_id,)
    else:
        prefix = "WITH m AS (SELECT * FROM universe_members WHERE universe_id=? AND version=?), "
        ref = "m"
        tables = {"m": _U_MEMBER, "i": _INSTRUMENT, "s": _SOURCE, "f": _FILE}
        parameters = (pin.universe_id, pin.version)
    prefix += (
        "i AS (SELECT * FROM instruments WHERE instrument_id IN "  # noqa: S608 -- fixed CTE aliases only
        f"(SELECT instrument_id FROM {ref})), "
        "s AS (SELECT * FROM source_snapshots WHERE snapshot_id IN "
        f"(SELECT source_snapshot_id FROM {ref})), "
        "f AS (SELECT * FROM source_files WHERE snapshot_id IN (SELECT snapshot_id FROM s)) "
    )
    return _Queries(prefix, parameters, tables)


def _header(connection: sqlite3.Connection, pin: IdentityPin | UniversePin) -> bool:
    if isinstance(pin, IdentityPin):
        sql, params = (
            "SELECT content_hash=? FROM identity_snapshots WHERE snapshot_id=?",
            (pin.content_hash, pin.snapshot_id),
        )
    else:
        sql, params = (
            "SELECT content_hash=? FROM universe_versions WHERE universe_id=? AND version=?",
            (pin.content_hash, pin.universe_id, pin.version),
        )
    row = connection.execute(sql, params).fetchone()
    if row is None:
        return False
    if not row[0]:
        raise ValueError("membership pin mismatch")
    return True


def _sql_charge(connection: sqlite3.Connection, queries: _Queries) -> int:
    charge = 65536 + 128 * sum(len(value.encode("utf-8")) for value in queries.parameters)
    for table, fields in queries.tables.items():
        lengths = "+".join(
            f"coalesce(length(CAST({key} AS BLOB)),0)"
            for key, kind in fields.items()
            if kind.startswith(("text", "sha"))
        )
        count, size = connection.execute(
            queries.prefix + f"SELECT count(*),coalesce(sum({lengths}),0) FROM {table}",  # noqa: S608 -- fixed internal schema/CTEs
            queries.parameters,
        ).fetchone()
        charge += 16384 * count + 128 * size
    return charge


def _missing_references(connection: sqlite3.Connection, queries: _Queries) -> None:
    ref = "a" if "a" in queries.tables else "m"
    checks = [
        (
            f"SELECT 1 FROM {ref} r LEFT JOIN i ON i.instrument_id=r.instrument_id "  # noqa: S608 -- fixed CTE alias
            "WHERE i.instrument_id IS NULL"
        ),
        (
            f"SELECT 1 FROM {ref} r LEFT JOIN s ON s.snapshot_id=r.source_snapshot_id "  # noqa: S608 -- fixed CTE alias
            "WHERE s.snapshot_id IS NULL"
        ),
        (
            "SELECT 1 FROM i LEFT JOIN issuers u ON u.issuer_id=i.issuer_id "
            "WHERE i.issuer_id IS NOT NULL AND u.issuer_id IS NULL"
        ),
    ]
    if ref == "a":
        checks.extend(
            [
                (
                    "SELECT 1 FROM m LEFT JOIN a ON a.assertion_id=m.assertion_id "
                    "WHERE a.assertion_id IS NULL"
                ),
                (
                    "SELECT 1 FROM a LEFT JOIN identity_assertions p "
                    "ON p.assertion_id=a.supersedes_assertion_id "
                    "WHERE a.supersedes_assertion_id IS NOT NULL AND p.assertion_id IS NULL"
                ),
            ]
        )
    for sql in checks:
        if connection.execute(
            queries.prefix + "SELECT EXISTS(" + sql + ")", queries.parameters
        ).fetchone()[0]:
            raise ValueError("membership has unknown reference")


def _reconstruct(
    connection: sqlite3.Connection, pin: IdentityPin | UniversePin, queries: _Queries
) -> VerifiedMembership:
    rows: dict[str, list[Record]] = {}
    for table, fields in queries.tables.items():
        if table == "f":
            continue
        columns = ",".join(fields)
        rows[table] = [
            dict(row)
            for row in connection.execute(
                queries.prefix + f"SELECT {columns} FROM {table}",  # noqa: S608 -- fixed internal schema/CTEs
                queries.parameters,
            )
        ]
    for source in rows["s"]:
        # Do not duplicate a potentially wide parent ID in every file tuple.
        # The complete inventory was already included in aggregate admission.
        source["files"] = [
            dict(row)
            for row in connection.execute(
                "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=?",
                (source["snapshot_id"],),
            )
        ]
    identity = isinstance(pin, IdentityPin)
    body: Record = {
        "schema": "aas-identity-snapshot-v1" if identity else "aas-universe-version-v1",
        "hash_format": _HASH_FORMAT,
        "instruments": rows["i"],
        "sources": rows["s"],
        "members": rows["m"],
    }
    if isinstance(pin, IdentityPin):
        body.update(snapshot_id=pin.snapshot_id, assertions=rows["a"])
    else:
        body.update(universe_id=pin.universe_id, version=pin.version)
    body = _canonical(body, identity=identity)
    raw = canonical_json_bytes(body)
    if len(raw) > _MAX_BYTES or content_sha256(body) != pin.content_hash:
        raise ValueError("membership content mismatch")
    mapping = (
        {row["assertion_id"]: row["instrument_id"] for row in _records(body, "assertions")}
        if identity
        else {}
    )
    members = tuple(
        MappingProxyType(
            {
                "instrument_id": mapping[row["assertion_id"]] if identity else row["instrument_id"],
                **{key: row[key] for key in _INTERVAL},
            }
        )
        for row in _records(body, "members")
    )
    return VerifiedMembership(pin, raw, members)


def read_membership_pins(
    connection: sqlite3.Connection,
    identity_pin: IdentityPin | None,
    universe_pin: UniversePin | None,
    *,
    max_materialization_bytes: int,
) -> VerifiedMemberships:
    """SELECT only; admit both complete documents before materializing either."""
    if type(max_materialization_bytes) is not int or max_materialization_bytes <= 0:
        raise ComputeResourceError("membership requires a positive materialization budget")
    if (identity_pin is not None and not isinstance(identity_pin, IdentityPin)) or (
        universe_pin is not None and not isinstance(universe_pin, UniversePin)
    ):
        raise ValueError("membership requires typed exact pins")
    admitted: list[tuple[IdentityPin | UniversePin, _Queries]] = []
    total = 0
    for pin in (identity_pin, universe_pin):
        if pin is None:
            continue
        if not _header(connection, pin):
            raise ValueError("membership pin mismatch: absent header")
        queries = _queries(pin)
        _missing_references(connection, queries)
        charge = _sql_charge(connection, queries)
        _admit(charge)
        total += charge
        admitted.append((pin, queries))
    _admit(total, max_materialization_bytes)
    evidence = [_reconstruct(connection, pin, queries) for pin, queries in admitted]
    return VerifiedMemberships(
        next((item for item in evidence if isinstance(item.pin, IdentityPin)), None),
        next((item for item in evidence if isinstance(item.pin, UniversePin)), None),
    )


def _matches(connection: sqlite3.Connection, table: str, row: Record) -> bool:
    return bool(
        connection.execute(
            f"SELECT EXISTS(SELECT 1 FROM {table} WHERE "  # noqa: S608 -- validated keys, fixed table names
            + " AND ".join(f"{key} IS ?" for key in row)
            + ")",
            tuple(row.values()),
        ).fetchone()[0]
    )


def _insert(connection: sqlite3.Connection, table: str, row: Record) -> None:
    connection.execute(
        f"INSERT INTO {table} (" + ",".join(row) + ") VALUES (" + ",".join("?" for _ in row) + ")",  # noqa: S608 -- validated keys, fixed table names
        tuple(row.values()),
    )


def _source_expectations(connection: sqlite3.Connection, body: Record) -> None:
    for source in _records(body, "sources"):
        header = {key: source[key] for key in _SOURCE}
        if not _matches(connection, "source_snapshots", header):
            raise ValueError("membership source evidence mismatch")
        files = _records(source, "files")
        count = connection.execute(
            "SELECT count(*) FROM source_files WHERE snapshot_id=?", (source["snapshot_id"],)
        ).fetchone()[0]
        if count != len(files) or any(
            not _matches(connection, "source_files", {"snapshot_id": source["snapshot_id"], **file})
            for file in files
        ):
            raise ValueError("membership source inventory mismatch")


def _declarations(connection: sqlite3.Connection, body: Record) -> None:
    _source_expectations(connection, body)
    for instrument in _records(body, "instruments"):
        if instrument["issuer_id"] is not None and not _matches(
            connection, "issuers", {"issuer_id": instrument["issuer_id"]}
        ):
            raise ValueError("membership unknown issuer reference")
    assertions = cast("list[Record]", body.get("assertions", []))
    supplied = {row["assertion_id"] for row in assertions}
    for assertion in assertions:
        predecessor = assertion["supersedes_assertion_id"]
        if (
            predecessor is not None
            and predecessor not in supplied
            and not _matches(connection, "identity_assertions", {"assertion_id": predecessor})
        ):
            raise ValueError("membership unknown predecessor reference")
    for table, key, declarations in (
        ("instruments", "instrument_id", _records(body, "instruments")),
        ("identity_assertions", "assertion_id", _assertion_order(assertions)),
    ):
        for row in declarations:
            if _matches(connection, table, {key: row[key]}):
                if not _matches(connection, table, row):
                    raise ValueError("membership shared declaration mismatch")
            else:
                _insert(connection, table, row)


def _registered(
    connection: sqlite3.Connection,
    body: Record,
    raw: bytes,
    pin: IdentityPin | UniversePin,
    created_at_us: int = 0,
) -> None:
    with atomic(connection):
        exists = _header(connection, pin)
        if not exists:
            _declarations(connection, body)
            if isinstance(pin, IdentityPin):
                parent = {"snapshot_id": pin.snapshot_id}
                _insert(
                    connection,
                    "identity_snapshots",
                    {**parent, "content_hash": pin.content_hash, "created_at_us": created_at_us},
                )
                table = "identity_snapshot_members"
            else:
                parent = {"universe_id": pin.universe_id, "version": pin.version}
                _insert(
                    connection, "universe_versions", {**parent, "content_hash": pin.content_hash}
                )
                table = "universe_members"
            for member in _records(body, "members"):
                _insert(connection, table, {**parent, **member})
        result = read_membership_pins(
            connection,
            pin if isinstance(pin, IdentityPin) else None,
            pin if isinstance(pin, UniversePin) else None,
            max_materialization_bytes=_MAX_CHARGE,
        )
        verified = result.identity if isinstance(pin, IdentityPin) else result.universe
        if verified is None or verified.canonical_bytes != raw:
            raise ValueError("membership registration content mismatch")


def register_identity_snapshot(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str, created_at_us: int
) -> IdentityPin:
    _integer(created_at_us)
    body, canonical = _payload(raw, expected_file_sha256, identity=True)
    pin = IdentityPin(cast("str", body["snapshot_id"]), content_sha256(body))
    _registered(connection, body, canonical, pin, created_at_us)
    return pin


def register_universe_version(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str
) -> UniversePin:
    body, canonical = _payload(raw, expected_file_sha256, identity=False)
    pin = UniversePin(
        cast("str", body["universe_id"]), cast("str", body["version"]), content_sha256(body)
    )
    _registered(connection, body, canonical, pin)
    return pin
