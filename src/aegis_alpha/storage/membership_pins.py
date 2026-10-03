"""Bounded immutable membership content, not resolution or execution eligibility.

The two v1 whole-document contracts and their chunked manifests are specified in
dev-notes/design/membership-pins.md. Connections and read lifetimes belong to the
caller's admitted workspace.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Mapping, Sequence
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
IDENTITY_MANIFEST_SCHEMA = "aas-identity-manifest-v1"
# Issuer-link assertions name one instrument's issuer, so they overlap per instrument.
ISSUER_LINK_NAMESPACE = "issuer"
UNIVERSE_MANIFEST_SCHEMA = "aas-universe-manifest-v1"
# A chunked document's parts are ordinary v1 documents named ``<root>#<5 digits>``.
# That suffix is reserved for parts, so a part can never be mistaken for a root.
_PART_SUFFIX = re.compile(r"#[0-9]{5}\Z")
_PART_DIGITS = 5
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
        last = "instrument_id" if assertion["namespace"] == ISSUER_LINK_NAMESPACE else "token"
        key = tuple(assertion[field] for field in ("provider", "namespace", last))
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


def _canonical(value: object, *, identity: bool, bounded: bool = True) -> Record:
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
        _admit(_body_charge(body) if bounded else 0)
        _ = _assertion_order(assertions)
        _identity_intervals(members, assertions)
        references = assertions
    if {row["instrument_id"] for row in instruments} != {
        row["instrument_id"] for row in references
    }:
        raise ValueError("membership instrument references must be exact")
    if {row["snapshot_id"] for row in sources} != {row["source_snapshot_id"] for row in references}:
        raise ValueError("membership source references must be exact")
    _admit(_body_charge(body) if bounded else 0)
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


def part_name(root: str, index: int) -> str:
    """Name part ``index`` of the chunked document whose root ID or version is ``root``."""
    if type(index) is not int or not 0 <= index < 10**_PART_DIGITS:
        raise ValueError("membership manifest part index is out of range")
    return f"{root}#{index:0{_PART_DIGITS}d}"


def is_part_name(name: str) -> bool:
    return _PART_SUFFIX.search(name) is not None


def _root_has_members(connection: sqlite3.Connection, pin: IdentityPin | UniversePin) -> bool:
    if isinstance(pin, IdentityPin):
        sql, params = (
            "SELECT EXISTS(SELECT 1 FROM identity_snapshot_members WHERE snapshot_id=?)",
            (pin.snapshot_id,),
        )
    else:
        sql, params = (
            "SELECT EXISTS(SELECT 1 FROM universe_members WHERE universe_id=? AND version=?)",
            (pin.universe_id, pin.version),
        )
    return bool(connection.execute(sql, params).fetchone()[0])


def _empty_document_hash(pin: IdentityPin | UniversePin) -> str:
    body: Record = {
        "schema": "aas-identity-snapshot-v1"
        if isinstance(pin, IdentityPin)
        else "aas-universe-version-v1",
        "hash_format": _HASH_FORMAT,
        "instruments": [],
        "members": [],
        "sources": [],
    }
    if isinstance(pin, IdentityPin):
        body.update(snapshot_id=pin.snapshot_id, assertions=[])
    else:
        body.update(universe_id=pin.universe_id, version=pin.version)
    return content_sha256(body)


def _part_pins(
    connection: sqlite3.Connection, pin: IdentityPin | UniversePin
) -> tuple[IdentityPin | UniversePin, ...]:
    """Return the parts a root names, in order; an empty tuple means a v1 document.

    A manifest root holds no members of its own, so a root with members, or one whose
    hash is the empty v1 document's, is a v1 document whatever its siblings are named
    (a store may hold v1 documents named like parts from before the suffix was reserved).
    Otherwise the siblings named as parts must be contiguous and rebuild the root's hash.
    """
    if _root_has_members(connection, pin) or pin.content_hash == _empty_document_hash(pin):
        return ()
    root = pin.snapshot_id if isinstance(pin, IdentityPin) else pin.version
    width = len(root) + 1 + _PART_DIGITS
    if isinstance(pin, IdentityPin):
        rows = connection.execute(
            "SELECT snapshot_id,content_hash FROM identity_snapshots "
            "WHERE length(snapshot_id)=? AND substr(snapshot_id,1,?)=?",
            (width, len(root) + 1, root + "#"),
        ).fetchall()
    else:
        rows = connection.execute(
            "SELECT version,content_hash FROM universe_versions WHERE universe_id=? "
            "AND length(version)=? AND substr(version,1,?)=?",
            (pin.universe_id, width, len(root) + 1, root + "#"),
        ).fetchall()
    found = sorted((str(name), str(digest)) for name, digest in rows if is_part_name(str(name)))
    if not found:
        return ()
    if [name for name, _ in found] != [part_name(root, index) for index in range(len(found))]:
        raise ValueError("membership manifest parts are not contiguous")
    parts: tuple[IdentityPin | UniversePin, ...] = (
        tuple(IdentityPin(name, digest) for name, digest in found)
        if isinstance(pin, IdentityPin)
        else tuple(UniversePin(pin.universe_id, name, digest) for name, digest in found)
    )
    if hashlib.sha256(_manifest_bytes(pin, parts)).hexdigest() != pin.content_hash:
        raise ValueError("membership manifest content mismatch")
    return parts


def _manifest_body(
    pin: IdentityPin | UniversePin, parts: Sequence[IdentityPin | UniversePin]
) -> Record:
    if isinstance(pin, IdentityPin):
        return {
            "schema": IDENTITY_MANIFEST_SCHEMA,
            "hash_format": _HASH_FORMAT,
            "snapshot_id": pin.snapshot_id,
            "parts": [
                {"snapshot_id": part.snapshot_id, "content_hash": part.content_hash}
                for part in cast("Sequence[IdentityPin]", parts)
            ],
        }
    return {
        "schema": UNIVERSE_MANIFEST_SCHEMA,
        "hash_format": _HASH_FORMAT,
        "universe_id": pin.universe_id,
        "version": pin.version,
        "parts": [
            {"version": part.version, "content_hash": part.content_hash}
            for part in cast("Sequence[UniversePin]", parts)
        ],
    }


def _manifest_bytes(
    pin: IdentityPin | UniversePin, parts: Sequence[IdentityPin | UniversePin]
) -> bytes:
    raw = canonical_json_bytes(_manifest_body(pin, parts))
    if len(raw) > _MAX_BYTES:
        raise ValueError("membership manifest exceeds document byte limit")
    return raw


def _part_bounds(
    connection: sqlite3.Connection, part: IdentityPin | UniversePin
) -> tuple[tuple[object, ...], tuple[object, ...]] | None:
    if isinstance(part, IdentityPin):
        low, high = connection.execute(
            "SELECT min(assertion_id),max(assertion_id) FROM identity_snapshot_members "
            "WHERE snapshot_id=?",
            (part.snapshot_id,),
        ).fetchone()
        return None if low is None else ((low,), (high,))
    bounds = []
    for direction in ("ASC", "DESC"):
        row = connection.execute(
            "SELECT instrument_id,valid_from_us,known_from_us FROM universe_members "  # noqa: S608 -- fixed sort direction
            f"WHERE universe_id=? AND version=? ORDER BY instrument_id {direction}, "
            f"valid_from_us {direction}, known_from_us {direction} LIMIT 1",
            (part.universe_id, part.version),
        ).fetchone()
        if row is None:
            return None
        bounds.append(tuple(row))
    return bounds[0], bounds[1]


# Whether two parts of one identity manifest hold the same (provider, namespace, key) over
# overlapping valid and known intervals. Parameters: ISSUER_LINK_NAMESPACE, then a JSON array
# of part snapshot ids. MATERIALIZED evaluates the member join once and lets SQLite index the
# self-join; an inlined CTE re-runs the join for every outer row and grows quadratically.
PART_OVERLAP_SQL = (
    "WITH m AS MATERIALIZED (SELECT s.snapshot_id AS part,a.provider,a.namespace,"
    "CASE WHEN a.namespace=? THEN a.instrument_id ELSE a.token END AS token,"
    "s.valid_from_us AS vf,s.valid_to_us AS vt,s.known_from_us AS kf,"
    "s.known_to_us AS kt FROM identity_snapshot_members s "
    "JOIN identity_assertions a ON a.assertion_id=s.assertion_id "
    "WHERE s.snapshot_id IN (SELECT value FROM json_each(?))) "
    "SELECT EXISTS(SELECT 1 FROM m x JOIN m y ON x.provider=y.provider "
    "AND x.namespace=y.namespace AND x.token=y.token AND x.part<y.part "
    "WHERE (x.vt IS NULL OR y.vf<x.vt) AND (y.vt IS NULL OR x.vf<y.vt) "
    "AND (x.kt IS NULL OR y.kf<x.kt) AND (y.kt IS NULL OR x.kf<y.kt))"
)


def _manifest_rules(
    connection: sqlite3.Connection, parts: Sequence[IdentityPin | UniversePin]
) -> None:
    """Hold the parts to one canonical document: ordered, disjoint, and non-overlapping.

    Each part is a v1 document checked on its own; what no single part can see is the
    member order across parts and an identity interval overlap between two parts.
    """
    previous: tuple[object, ...] | None = None
    for part in parts:
        bounds = _part_bounds(connection, part)
        if bounds is None:
            if len(parts) > 1:
                raise ValueError("membership manifest part is empty")
            continue
        low, high = bounds
        if previous is not None and not cast("tuple[str, ...]", previous) < cast(
            "tuple[str, ...]", low
        ):
            raise ValueError("membership manifest parts are not in canonical order")
        previous = high
    if parts and isinstance(parts[0], IdentityPin) and len(parts) > 1:
        names = json.dumps([cast("IdentityPin", part).snapshot_id for part in parts])
        overlap = connection.execute(PART_OVERLAP_SQL, (ISSUER_LINK_NAMESPACE, names)).fetchone()[0]
        if overlap:
            raise ValueError("identity snapshot interval overlap")


@dataclass(frozen=True, slots=True)
class _Admitted:
    pin: IdentityPin | UniversePin
    parts: tuple[tuple[IdentityPin | UniversePin, _Queries], ...]
    manifest: bytes | None
    charge: int


def _admitted(connection: sqlite3.Connection, pin: IdentityPin | UniversePin) -> _Admitted:
    """Check one pin's header, references and charge before anything is materialized."""
    if not _header(connection, pin):
        raise ValueError("membership pin mismatch: absent header")
    parts = _part_pins(connection, pin)
    manifest = None
    if parts:
        manifest = _manifest_bytes(pin, parts)
        _manifest_rules(connection, parts)
    charged = []
    total = 0
    for part in parts or (pin,):
        if not _header(connection, part):
            raise ValueError("membership pin mismatch: absent part header")
        queries = _queries(part)
        _missing_references(connection, queries)
        charge = _sql_charge(connection, queries)
        _admit(charge)
        total += charge
        charged.append((part, queries))
    return _Admitted(pin, tuple(charged), manifest, total)


def _evidence(connection: sqlite3.Connection, admitted: _Admitted) -> VerifiedMembership:
    documents = [_reconstruct(connection, part, queries) for part, queries in admitted.parts]
    if admitted.manifest is None:
        return documents[0]
    members = tuple(member for document in documents for member in document.members)
    return VerifiedMembership(admitted.pin, admitted.manifest, members)


def read_membership_pins(
    connection: sqlite3.Connection,
    identity_pin: IdentityPin | None,
    universe_pin: UniversePin | None,
    *,
    max_materialization_bytes: int,
) -> VerifiedMemberships:
    """SELECT only; admit both complete documents before materializing either.

    A pin naming a chunked manifest is admitted as the sum of its parts and returns the
    manifest's canonical bytes with every part's members in canonical order.
    """
    if type(max_materialization_bytes) is not int or max_materialization_bytes <= 0:
        raise ComputeResourceError("membership requires a positive materialization budget")
    if (identity_pin is not None and not isinstance(identity_pin, IdentityPin)) or (
        universe_pin is not None and not isinstance(universe_pin, UniversePin)
    ):
        raise ValueError("membership requires typed exact pins")
    admitted = [_admitted(connection, pin) for pin in (identity_pin, universe_pin) if pin]
    _admit(sum(item.charge for item in admitted), max_materialization_bytes)
    evidence = [_evidence(connection, item) for item in admitted]
    return VerifiedMemberships(
        next((item for item in evidence if isinstance(item.pin, IdentityPin)), None),
        next((item for item in evidence if isinstance(item.pin, UniversePin)), None),
    )


def membership_parts(
    connection: sqlite3.Connection, pin: IdentityPin | UniversePin
) -> tuple[IdentityPin | UniversePin, ...]:
    """SELECT only: the part pins of a chunked manifest, or empty for a v1 document."""
    if not _header(connection, pin):
        raise ValueError("membership pin mismatch: absent header")
    return _part_pins(connection, pin)


def verify_membership_pin(
    connection: sqlite3.Connection,
    pin: IdentityPin | UniversePin,
    *,
    max_materialization_bytes: int,
) -> None:
    """Verify one stored header without materializing a manifest's parts together.

    A v1 document is reconstructed in full. A manifest is checked from its parts' headers
    and member bounds alone, because every part is itself a stored header that this same
    verification reconstructs on its own allowance.
    """
    parts = membership_parts(connection, pin)
    if not parts:
        read_membership_pins(
            connection,
            pin if isinstance(pin, IdentityPin) else None,
            pin if isinstance(pin, UniversePin) else None,
            max_materialization_bytes=max_materialization_bytes,
        )
        return
    _manifest_rules(connection, parts)


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


def _unreserved(body: Record, *, identity: bool) -> None:
    root = cast("str", body["snapshot_id" if identity else "version"])
    if is_part_name(root):
        raise ValueError("a '#' and five digits ending a membership root names a manifest part")


def register_identity_snapshot(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str, created_at_us: int
) -> IdentityPin:
    _integer(created_at_us)
    body, canonical = _payload(raw, expected_file_sha256, identity=True)
    _unreserved(body, identity=True)
    pin = IdentityPin(cast("str", body["snapshot_id"]), content_sha256(body))
    _registered(connection, body, canonical, pin, created_at_us)
    return pin


def register_universe_version(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str
) -> UniversePin:
    body, canonical = _payload(raw, expected_file_sha256, identity=False)
    _unreserved(body, identity=False)
    pin = UniversePin(
        cast("str", body["universe_id"]), cast("str", body["version"]), content_sha256(body)
    )
    _registered(connection, body, canonical, pin)
    return pin


def _text_bytes(row: Record) -> int:
    return sum(len(value.encode("utf-8")) for value in row.values() if isinstance(value, str))


def _row_cost(row: Record, count: int) -> int:
    """Canonical bytes one more row adds to an array already holding ``count`` rows."""
    return len(canonical_json_bytes(row)) + (1 if count else 0)


@dataclass(slots=True)
class _Part:
    """One part being filled, with its exact canonical size and charge inputs."""

    members: list[Record]
    assertions: list[Record]
    instruments: dict[str, Record]
    sources: dict[str, Record]
    size: int
    rows: int
    text: int


@dataclass(frozen=True, slots=True)
class _Lookups:
    instruments: Mapping[str, Record]
    sources: Mapping[str, Record]
    assertions: Mapping[str, Record]


def _add(part: _Part, member: Record, lookups: _Lookups, *, identity: bool) -> bool:
    """Add one member and what it references if the part still fits; report whether it did."""
    row = {**member, "ordinal": len(part.members)} if identity else member
    size, rows, text = _row_cost(row, len(part.members)), 1, _text_bytes(row)
    reference = member
    if identity:
        reference = lookups.assertions[cast("str", member["assertion_id"])]
        size += _row_cost(reference, len(part.assertions))
        rows += 1
        text += _text_bytes(reference)
    instrument_id = cast("str", reference["instrument_id"])
    instrument = None if instrument_id in part.instruments else lookups.instruments[instrument_id]
    if instrument is not None:
        size += _row_cost(instrument, len(part.instruments))
        rows += 1
        text += _text_bytes(instrument)
    source_id = cast("str", reference["source_snapshot_id"])
    source = None if source_id in part.sources else lookups.sources[source_id]
    if source is not None:
        files = _records(source, "files")
        size += _row_cost(source, len(part.sources))
        rows += 1 + len(files)
        text += _text_bytes(source) + sum(_text_bytes(file) for file in files)
    charge = 65536 + 16384 * (part.rows + rows) + 128 * (part.text + text)
    if part.size + size > _MAX_BYTES or charge > _MAX_CHARGE:
        return False
    part.members.append(row)
    if identity:
        part.assertions.append(reference)
    if instrument is not None:
        part.instruments[instrument_id] = instrument
    if source is not None:
        part.sources[source_id] = source
    part.size += size
    part.rows += rows
    part.text += text
    return True


def _oversized(member: Record, lookups: _Lookups, *, identity: bool) -> str:
    reference = lookups.assertions[cast("str", member["assertion_id"])] if identity else member
    source_id = cast("str", reference["source_snapshot_id"])
    files = len(_records(lookups.sources[source_id], "files"))
    return (
        f"one membership member exceeds the part limits: its source {source_id} lists "
        f"{files} files, and a part carries the whole file inventory of every source it "
        "references"
    )


def chunk_membership(body: Record, *, identity: bool) -> tuple[Record, ...]:
    """Split one validated whole document into canonical v1 parts, deterministically.

    Members keep their canonical order and fill each part greedily up to both the
    1 MiB byte limit and the 64 MiB materialization charge, so the same content always
    yields the same parts. A part carries exactly the assertions, instruments and
    sources its own members reference, and its members' ordinals restart at zero.
    """
    key = "snapshot_id" if identity else "version"
    arrays = ("instruments", "members", "sources", "assertions")
    root = {name: value for name, value in body.items() if name not in arrays}
    names = [part_name(cast("str", body[key]), 0)]
    skeleton: Record = {**root, key: names[0], "instruments": [], "members": [], "sources": []}
    if identity:
        skeleton["assertions"] = []
    base = (
        len(canonical_json_bytes(skeleton)),
        sum(
            len(cast("str", skeleton[name]).encode("utf-8"))
            for name in (key, "universe_id")
            if name in skeleton
        ),
    )
    lookups = _Lookups(
        {cast("str", row["instrument_id"]): row for row in _records(body, "instruments")},
        {cast("str", row["snapshot_id"]): row for row in _records(body, "sources")},
        {
            cast("str", row["assertion_id"]): row
            for row in cast("list[Record]", body.get("assertions", []))
        },
    )
    filled = [_Part([], [], {}, {}, base[0], 0, base[1])]
    for member in _records(body, "members"):
        row = {name: value for name, value in member.items() if name != "ordinal"}
        if _add(filled[-1], row, lookups, identity=identity):
            continue
        filled.append(_Part([], [], {}, {}, base[0], 0, base[1]))
        if not filled[-2].members or not _add(filled[-1], row, lookups, identity=identity):
            raise ValueError(_oversized(row, lookups, identity=identity))
    if len(filled) > 10**_PART_DIGITS:
        raise ValueError("membership document needs more parts than a manifest can name")
    parts = []
    for index, part in enumerate(filled):
        document: Record = {
            **root,
            key: part_name(cast("str", body[key]), index),
            "instruments": list(part.instruments.values()),
            "members": part.members,
            "sources": list(part.sources.values()),
        }
        if identity:
            document["assertions"] = part.assertions
        document = _canonical(document, identity=identity)
        if len(canonical_json_bytes(document)) != part.size:
            raise AssertionError("membership part size disagrees with its accounting")
        parts.append(document)
    return tuple(parts)


def _part_pin(part: Record, *, identity: bool) -> IdentityPin | UniversePin:
    if identity:
        return IdentityPin(cast("str", part["snapshot_id"]), content_sha256(part))
    return UniversePin(
        cast("str", part["universe_id"]), cast("str", part["version"]), content_sha256(part)
    )


@dataclass(frozen=True, slots=True)
class MembershipPlan:
    """A validated whole document, its deterministic parts and the manifest pin they make."""

    pin: IdentityPin | UniversePin
    parts: tuple[Record, ...]
    part_pins: tuple[IdentityPin | UniversePin, ...]
    whole: Record


def plan_membership_manifest(body: Mapping[str, object], *, identity: bool) -> MembershipPlan:
    """Validate a whole document and derive its manifest pin and parts; writes nothing."""
    whole = _canonical(dict(body), identity=identity, bounded=False)
    _unreserved(whole, identity=identity)
    parts = chunk_membership(whole, identity=identity)
    part_pins = tuple(_part_pin(part, identity=identity) for part in parts)
    if identity:
        unhashed: IdentityPin | UniversePin = IdentityPin(
            cast("str", whole["snapshot_id"]), "0" * 64
        )
    else:
        unhashed = UniversePin(
            cast("str", whole["universe_id"]), cast("str", whole["version"]), "0" * 64
        )
    digest = hashlib.sha256(_manifest_bytes(unhashed, part_pins)).hexdigest()
    pin = (
        IdentityPin(unhashed.snapshot_id, digest)
        if isinstance(unhashed, IdentityPin)
        else UniversePin(unhashed.universe_id, unhashed.version, digest)
    )
    return MembershipPlan(pin, parts, part_pins, whole)


def _manifest_registered(
    connection: sqlite3.Connection, plan: MembershipPlan, created_at_us: int
) -> None:
    pin = plan.pin
    with atomic(connection):
        if not _header(connection, pin):
            _declarations(connection, plan.whole)
            for part, part_pin in zip(plan.parts, plan.part_pins, strict=True):
                _registered(connection, part, canonical_json_bytes(part), part_pin, created_at_us)
            if isinstance(pin, IdentityPin):
                _insert(
                    connection,
                    "identity_snapshots",
                    {
                        "snapshot_id": pin.snapshot_id,
                        "content_hash": pin.content_hash,
                        "created_at_us": created_at_us,
                    },
                )
            else:
                _insert(
                    connection,
                    "universe_versions",
                    {
                        "universe_id": pin.universe_id,
                        "version": pin.version,
                        "content_hash": pin.content_hash,
                    },
                )
        if membership_parts(connection, pin) != plan.part_pins:
            raise ValueError("membership manifest registration content mismatch")
        verify_membership_pin(connection, pin, max_materialization_bytes=_MAX_CHARGE)


def register_identity_manifest(
    connection: sqlite3.Connection, body: Mapping[str, object], *, created_at_us: int
) -> IdentityPin:
    """Register a whole identity document of any size as v1 parts under one manifest.

    The document has the ``aas-identity-snapshot-v1`` shape without the byte limit. A
    retry with the same content returns the same pin and keeps the original creation time.
    """
    _integer(created_at_us)
    plan = plan_membership_manifest(body, identity=True)
    _manifest_registered(connection, plan, created_at_us)
    return cast("IdentityPin", plan.pin)


def register_universe_manifest(
    connection: sqlite3.Connection, body: Mapping[str, object]
) -> UniversePin:
    """Register a whole universe document of any size as v1 parts under one manifest."""
    plan = plan_membership_manifest(body, identity=False)
    _manifest_registered(connection, plan, 0)
    return cast("UniversePin", plan.pin)
