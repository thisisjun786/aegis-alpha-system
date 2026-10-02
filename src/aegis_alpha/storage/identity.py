"""Native identity registry: issuers, opaque instrument mint and immutable assertions.

An issuer and an instrument are named by an opaque ID minted from one permanent anchor
(a SEC CIK, a DART corp code, a Norgate asset ID, a KRX ISIN). Tickers, paths and dates
are never anchors; they are ``identity_assertions`` with valid and knowledge intervals.
Every row is append-only: a correction is a new assertion that names the one it
supersedes, so nothing here issues an UPDATE or DELETE.

Snapshots project the registered assertions into a chunked identity document
(``membership_pins.register_identity_manifest``), which is what consumers pin.
The contract is in dev-notes/design/data-vertical.md.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.identity.records import IdentifierType, IdentifierValueError, normalize_identifier
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    MembershipPlan,
    membership_parts,
    plan_membership_manifest,
    register_identity_manifest,
)
from aegis_alpha.storage.state import atomic

REGISTRY_SCHEMA = "aas-identity-registry-v1"
INSTRUMENT_FORMAT = "aas-instrument-v1"
ISSUER_FORMAT = "aas-issuer-v1"
ASSERTION_FORMAT = "aas-assertion-v1"
MAX_REGISTRY_BYTES = 256 * 1024 * 1024
_REPORT_LIMIT = 100
_I64_MIN, _I64_MAX = -(2**63), 2**63 - 1

type Record = dict[str, object]


class IdentityAnchorError(ValueError):
    """A namespace is not a permanent anchor, or a token is not its canonical spelling."""


def _identifier(kind: IdentifierType, token: str) -> str:
    try:
        return normalize_identifier(kind, token)
    except IdentifierValueError as error:
        raise IdentityAnchorError(str(error)) from None


def _krx_isin(token: str) -> str:
    canonical = _identifier(IdentifierType.ISIN, token)
    if not canonical.startswith("KR"):
        raise IdentityAnchorError("krx_isin must be a KR ISIN")
    return canonical


def _dart_corp_code(token: str) -> str:
    if re.fullmatch(r"[0-9]{8}", token) is None:
        raise IdentityAnchorError("dart_corp_code must be eight digits")
    return token


# The permanent anchors an ID may be minted from, each with its one canonical spelling.
# A namespace joins only when the provider never reuses or reassigns its tokens.
INSTRUMENT_ANCHORS: Mapping[str, Callable[[str], str]] = {
    "norgate_assetid": lambda token: _identifier(IdentifierType.NORGATE_ASSETID, token),
    "krx_isin": _krx_isin,
}
ISSUER_ANCHORS: Mapping[str, Callable[[str], str]] = {
    "sec_cik": lambda token: _identifier(IdentifierType.CIK, token),
    "dart_corp_code": _dart_corp_code,
}


def _mint(
    prefix: str, fmt: str, anchors: Mapping[str, Callable[[str], str]], ns: str, token: str
) -> str:
    if not isinstance(ns, str) or not isinstance(token, str):
        raise IdentityAnchorError("anchor namespace and token must be text")
    validate = anchors.get(ns)
    if validate is None:
        raise IdentityAnchorError(
            f"{ns!r} is not a permanent anchor; tickers, symbols, paths and dates are "
            "identity assertions, never the source of an ID"
        )
    canonical = validate(token)
    if canonical != token:
        raise IdentityAnchorError(f"{ns} token must be spelled {canonical!r}")
    return prefix + hashlib.sha256(canonical_json_bytes([fmt, ns, token])).hexdigest()


def mint_instrument(anchor_namespace: str, token: str) -> str:
    """Return the opaque instrument ID of one permanent anchor (``ins-`` + SHA-256)."""
    return _mint("ins-", INSTRUMENT_FORMAT, INSTRUMENT_ANCHORS, anchor_namespace, token)


def mint_issuer(anchor_namespace: str, token: str) -> str:
    """Return the opaque issuer ID of one permanent anchor (``iss-`` + SHA-256)."""
    return _mint("iss-", ISSUER_FORMAT, ISSUER_ANCHORS, anchor_namespace, token)


_ASSERTION_COLUMNS = (
    "assertion_id",
    "instrument_id",
    "provider",
    "namespace",
    "token",
    "valid_from_us",
    "valid_to_us",
    "known_from_us",
    "supersedes_assertion_id",
    "source_snapshot_id",
    "source_hash",
)


def assertion_id(assertion: Mapping[str, object]) -> str:
    """Return the content ID of one assertion: every retained column but the ID itself."""
    values = [assertion[column] for column in _ASSERTION_COLUMNS[1:]]
    return "asr-" + hashlib.sha256(canonical_json_bytes([ASSERTION_FORMAT, *values])).hexdigest()


# --- registry document ------------------------------------------------------------------


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ValueError(f"identity registry {name} must be nonempty printable trimmed text")
    return value


def _integer(value: object, name: str, *, nonnegative: bool = False) -> int:
    if type(value) is not int or not (0 if nonnegative else _I64_MIN) <= value <= _I64_MAX:
        raise ValueError(f"identity registry {name} must be a signed int64")
    return value


def _object(value: object, keys: Iterable[str], name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(f"identity registry {name} has missing or unknown fields")
    return cast("Mapping[str, object]", value)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"identity registry {name} must be an array")  # noqa: TRY004 -- malformed-content ValueError contract
    return cast("list[object]", value)


_ANCHOR = ("anchor_namespace", "anchor_token")


def _anchor(value: object, name: str, mint: Callable[[str, str], str]) -> str:
    body = _object(value, _ANCHOR, name)
    return mint(_text(body["anchor_namespace"], name), _text(body["anchor_token"], name))


@dataclass(frozen=True, slots=True)
class RegistryDocument:
    """A validated ``aas-identity-registry-v1`` document with every ID already minted."""

    issuers: tuple[Record, ...]
    instruments: tuple[Record, ...]
    assertions: tuple[Record, ...]


def parse_registry(document: object) -> RegistryDocument:
    """Validate a registry document and mint its IDs; duplicates and unknown keys reject."""
    body = _object(document, ("schema", "issuers", "instruments", "assertions"), "document")
    if body["schema"] != REGISTRY_SCHEMA:
        raise ValueError("unsupported identity registry schema")
    issuers: dict[str, Record] = {}
    for item in _array(body["issuers"], "issuers"):
        row = _object(item, (*_ANCHOR, "name"), "issuer")
        issuer_id = _anchor({key: row[key] for key in _ANCHOR}, "issuer", mint_issuer)
        if issuer_id in issuers:
            raise ValueError("identity registry repeats an issuer anchor")
        issuers[issuer_id] = {"issuer_id": issuer_id, "name": _text(row["name"], "issuer name")}
    instruments: dict[str, Record] = {}
    for item in _array(body["instruments"], "instruments"):
        row = _object(item, (*_ANCHOR, "issuer", "asset_type", "venue"), "instrument")
        instrument_id = _anchor({key: row[key] for key in _ANCHOR}, "instrument", mint_instrument)
        if instrument_id in instruments:
            raise ValueError("identity registry repeats an instrument anchor")
        issuer = row["issuer"]
        instruments[instrument_id] = {
            "instrument_id": instrument_id,
            "issuer_id": None if issuer is None else _anchor(issuer, "issuer", mint_issuer),
            "asset_type": _text(row["asset_type"], "asset_type"),
            "venue": _text(row["venue"], "venue"),
        }
    assertions: dict[str, Record] = {}
    keys = ("instrument", *_ASSERTION_COLUMNS[2:])
    for item in _array(body["assertions"], "assertions"):
        row = _object(item, keys, "assertion")
        start = _integer(row["valid_from_us"], "valid_from_us")
        end = None if row["valid_to_us"] is None else _integer(row["valid_to_us"], "valid_to_us")
        if end is not None and end <= start:
            raise ValueError("identity registry valid interval end must exceed its start")
        source_hash = row["source_hash"]
        if not isinstance(source_hash, str) or re.fullmatch(r"[0-9a-f]{64}", source_hash) is None:
            raise ValueError("identity registry source_hash must be lowercase SHA-256 hex")
        predecessor = row["supersedes_assertion_id"]
        record: Record = {
            "instrument_id": _anchor(row["instrument"], "assertion instrument", mint_instrument),
            "provider": _text(row["provider"], "provider"),
            "namespace": _text(row["namespace"], "namespace"),
            "token": _text(row["token"], "token"),
            "valid_from_us": start,
            "valid_to_us": end,
            "known_from_us": _integer(row["known_from_us"], "known_from_us", nonnegative=True),
            "supersedes_assertion_id": None
            if predecessor is None
            else _text(predecessor, "supersedes_assertion_id"),
            "source_snapshot_id": _text(row["source_snapshot_id"], "source_snapshot_id"),
            "source_hash": source_hash,
        }
        identifier = assertion_id(record)
        if identifier in assertions:
            raise ValueError("identity registry repeats an assertion")
        assertions[identifier] = {"assertion_id": identifier, **record}
    return RegistryDocument(
        tuple(issuers.values()), tuple(instruments.values()), tuple(assertions.values())
    )


def decode_registry(raw: bytes, *, expected_file_sha256: str) -> RegistryDocument:
    """Admit exact incoming bytes against their SHA-256, then parse them."""
    if re.fullmatch(r"[0-9a-f]{64}", expected_file_sha256) is None:
        raise ValueError("identity registry hash must be lowercase SHA-256")
    if type(raw) is not bytes or len(raw) > MAX_REGISTRY_BYTES:
        raise ValueError("identity registry exceeds its byte limit or is not bytes")
    if hashlib.sha256(raw).hexdigest() != expected_file_sha256:
        raise ValueError("identity registry incoming file SHA-256 mismatch")
    return parse_registry(decode_json(raw))


# --- planning -----------------------------------------------------------------------------


@dataclass(slots=True)
class RegistrationPlan:
    """What registering one document would add, and why it cannot yet, if it cannot."""

    issuers: list[Record] = field(default_factory=list)
    instruments: list[Record] = field(default_factory=list)
    assertions: list[Record] = field(default_factory=list)
    existing: dict[str, int] = field(
        default_factory=lambda: {"issuers": 0, "instruments": 0, "assertions": 0}
    )
    conflicts: list[Record] = field(default_factory=list)
    missing: dict[str, set[str]] = field(
        default_factory=lambda: {
            "sources": set(),
            "issuers": set(),
            "instruments": set(),
            "predecessors": set(),
        }
    )
    issuer_name_differences: list[Record] = field(default_factory=list)

    @property
    def admissible(self) -> bool:
        return not self.conflicts and not any(self.missing.values())

    def report(self) -> dict[str, object]:
        return {
            "new": {
                "issuers": len(self.issuers),
                "instruments": len(self.instruments),
                "assertions": len(self.assertions),
            },
            "existing": dict(self.existing),
            "conflict_count": len(self.conflicts),
            "conflicts": self.conflicts[:_REPORT_LIMIT],
            "missing": {
                key: sorted(values)[:_REPORT_LIMIT] for key, values in self.missing.items()
            },
            "missing_count": {key: len(values) for key, values in self.missing.items()},
            "issuer_name_difference_count": len(self.issuer_name_differences),
            "issuer_name_differences": self.issuer_name_differences[:_REPORT_LIMIT],
        }


def _row(connection: sqlite3.Connection, table: str, key: str, value: str) -> Record | None:
    row = connection.execute(
        f"SELECT * FROM {table} WHERE {key}=?",  # noqa: S608 -- fixed internal table and key
        (value,),
    ).fetchone()
    return None if row is None else dict(row)


def _same(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    return all(left[key] == right[key] for key in left)


def _plan_entities(
    connection: sqlite3.Connection, document: RegistryDocument, plan: RegistrationPlan
) -> None:
    for issuer in document.issuers:
        stored = _row(connection, "issuers", "issuer_id", cast("str", issuer["issuer_id"]))
        if stored is None:
            plan.issuers.append(issuer)
            continue
        plan.existing["issuers"] += 1
        if stored["name"] != issuer["name"]:
            # A display name is the first registering source's label, not identity.
            plan.issuer_name_differences.append(
                {"issuer_id": issuer["issuer_id"], "stored": stored["name"], "new": issuer["name"]}
            )
    declared = {issuer["issuer_id"] for issuer in document.issuers}
    for instrument in document.instruments:
        issuer_id = instrument["issuer_id"]
        if (
            issuer_id is not None
            and issuer_id not in declared
            and _row(connection, "issuers", "issuer_id", cast("str", issuer_id)) is None
        ):
            plan.missing["issuers"].add(cast("str", issuer_id))
        stored = _row(
            connection, "instruments", "instrument_id", cast("str", instrument["instrument_id"])
        )
        if stored is None:
            plan.instruments.append(instrument)
        elif _same(instrument, stored):
            plan.existing["instruments"] += 1
        else:
            plan.conflicts.append(
                {
                    "kind": "instrument_attributes",
                    "instrument_id": instrument["instrument_id"],
                    "stored": stored,
                    "new": instrument,
                }
            )


class _Assertions:
    """Registered and incoming assertions, read lazily, with their supersession edges."""

    def __init__(self, connection: sqlite3.Connection, incoming: Sequence[Record]) -> None:
        self.connection = connection
        self.rows: dict[str, Record | None] = {}
        self.incoming = {cast("str", row["assertion_id"]): row for row in incoming}
        self.successor_known: dict[str, int] = {
            str(predecessor): int(known)
            for predecessor, known in connection.execute(
                "SELECT supersedes_assertion_id,min(known_from_us) FROM identity_assertions "
                "WHERE supersedes_assertion_id IS NOT NULL GROUP BY supersedes_assertion_id"
            )
        }
        for row in incoming:
            predecessor = cast("str | None", row["supersedes_assertion_id"])
            if predecessor is not None:
                known = cast("int", row["known_from_us"])
                self.successor_known[predecessor] = min(
                    known, self.successor_known.get(predecessor, known)
                )

    def get(self, identifier: str) -> Record | None:
        if identifier in self.incoming:
            return self.incoming[identifier]
        if identifier not in self.rows:
            self.rows[identifier] = _row(
                self.connection, "identity_assertions", "assertion_id", identifier
            )
        return self.rows[identifier]

    def ancestors(self, identifier: str) -> set[str]:
        found: set[str] = set()
        current = self.get(identifier)
        while current is not None:
            predecessor = cast("str | None", current["supersedes_assertion_id"])
            if predecessor is None or predecessor in found:
                break
            found.add(predecessor)
            current = self.get(predecessor)
        return found

    def known_to(self, identifier: str) -> int | None:
        return self.successor_known.get(identifier)

    def registered_with_key(self, key: tuple[object, ...]) -> list[Record]:
        rows = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM identity_assertions WHERE provider=? AND namespace=? AND token=?",
                key,
            )
        ]
        for row in rows:
            self.rows.setdefault(cast("str", row["assertion_id"]), row)
        return rows


def _overlap(start: int, end: int | None, other_start: int, other_end: int | None) -> bool:
    return (end is None or other_start < end) and (other_end is None or start < other_end)


def _plan_overlaps(plan: RegistrationPlan, graph: _Assertions, incoming: Sequence[Record]) -> None:
    """Any two assertions of one provider key whose valid and known times overlap must be
    one correction chain; otherwise a consumer could not tell which one to believe."""
    groups: dict[tuple[object, ...], list[Record]] = {}
    for row in incoming:
        groups.setdefault((row["provider"], row["namespace"], row["token"]), []).append(row)
    for key, rows in groups.items():
        new = {cast("str", row["assertion_id"]) for row in rows}
        members = [
            *rows,
            *(row for row in graph.registered_with_key(key) if row["assertion_id"] not in new),
        ]
        for index, left in enumerate(members):
            for right in members[index + 1 :]:
                if left["assertion_id"] not in new and right["assertion_id"] not in new:
                    continue
                ids = (cast("str", left["assertion_id"]), cast("str", right["assertion_id"]))
                if ids[0] in graph.ancestors(ids[1]) or ids[1] in graph.ancestors(ids[0]):
                    continue
                valid = _overlap(
                    cast("int", left["valid_from_us"]),
                    cast("int | None", left["valid_to_us"]),
                    cast("int", right["valid_from_us"]),
                    cast("int | None", right["valid_to_us"]),
                )
                known = _overlap(
                    cast("int", left["known_from_us"]),
                    graph.known_to(ids[0]),
                    cast("int", right["known_from_us"]),
                    graph.known_to(ids[1]),
                )
                if valid and known:
                    plan.conflicts.append(
                        {
                            "kind": "assertion_overlap",
                            "provider": key[0],
                            "namespace": key[1],
                            "token": key[2],
                            "assertion_ids": sorted(ids),
                            "same_instrument": left["instrument_id"] == right["instrument_id"],
                        }
                    )


def _plan_references(
    connection: sqlite3.Connection,
    incoming: Sequence[Record],
    declared: set[object],
    graph: _Assertions,
    plan: RegistrationPlan,
) -> None:
    sources: dict[str, bool] = {}
    for assertion in incoming:
        instrument_id = cast("str", assertion["instrument_id"])
        if instrument_id not in declared and (
            _row(connection, "instruments", "instrument_id", instrument_id) is None
        ):
            plan.missing["instruments"].add(instrument_id)
        source = cast("str", assertion["source_snapshot_id"])
        if source not in sources:
            sources[source] = (
                _row(connection, "source_snapshots", "snapshot_id", source) is not None
            )
        if not sources[source]:
            plan.missing["sources"].add(source)
        predecessor_id = cast("str | None", assertion["supersedes_assertion_id"])
        if predecessor_id is None:
            continue
        predecessor = graph.get(predecessor_id)
        if predecessor is None:
            plan.missing["predecessors"].add(predecessor_id)
        elif cast("int", assertion["known_from_us"]) <= cast("int", predecessor["known_from_us"]):
            plan.conflicts.append(
                {
                    "kind": "correction_not_later",
                    "assertion_id": assertion["assertion_id"],
                    "supersedes_assertion_id": predecessor_id,
                }
            )


def _plan_assertions(
    connection: sqlite3.Connection, document: RegistryDocument, plan: RegistrationPlan
) -> None:
    incoming: list[Record] = []
    for assertion in document.assertions:
        stored = _row(
            connection,
            "identity_assertions",
            "assertion_id",
            cast("str", assertion["assertion_id"]),
        )
        if stored is None:
            incoming.append(assertion)
        elif _same(assertion, stored):
            plan.existing["assertions"] += 1
        else:
            raise ValueError("stored identity assertion disagrees with its content ID")
    graph = _Assertions(connection, incoming)
    declared = {instrument["instrument_id"] for instrument in document.instruments}
    _plan_references(connection, incoming, declared, graph, plan)
    plan.assertions = _dependency_order(incoming)
    _plan_overlaps(plan, graph, incoming)


def _dependency_order(rows: Sequence[Record]) -> list[Record]:
    pending = {cast("str", row["assertion_id"]): row for row in rows}
    ordered: list[Record] = []
    while pending:
        ready = [row for row in pending.values() if row["supersedes_assertion_id"] not in pending]
        if not ready:
            raise ValueError("identity registry assertion supersession cycle")
        for row in ready:
            ordered.append(row)
            del pending[cast("str", row["assertion_id"])]
    return ordered


def plan_registration(
    connection: sqlite3.Connection, document: RegistryDocument
) -> RegistrationPlan:
    """SELECT only: compare a document with the registry and classify every row."""
    plan = RegistrationPlan()
    _plan_entities(connection, document, plan)
    _plan_assertions(connection, document, plan)
    return plan


def _insert(connection: sqlite3.Connection, table: str, rows: Sequence[Record]) -> None:
    if not rows:
        return
    columns = list(rows[0])
    connection.executemany(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",  # noqa: S608 -- fixed internal columns
        [tuple(row[column] for column in columns) for row in rows],
    )


def register_identities(
    connection: sqlite3.Connection, document: RegistryDocument, *, apply: bool
) -> dict[str, object]:
    """Plan a registry document, and with ``apply`` append its new rows in one transaction.

    Existing identical rows are reused, so a repeated document adds nothing. Any conflict
    or missing reference refuses the whole document; nothing already stored is changed.
    """
    if not apply:
        return {"mode": "plan", **plan_registration(connection, document).report()}
    with atomic(connection):
        plan = plan_registration(connection, document)
        if not plan.admissible:
            report = plan.report()
            raise ValueError(
                "identity registration refused: "
                f"{report['conflict_count']} conflicts, missing {report['missing_count']}"
            )
        _insert(connection, "issuers", plan.issuers)
        _insert(connection, "instruments", plan.instruments)
        _insert(connection, "identity_assertions", plan.assertions)
    return {"mode": "apply", **plan.report()}


# --- snapshots ----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AssertionSelection:
    """Which registered assertions a snapshot projects; empty means every one."""

    providers: tuple[str, ...] = ()
    namespaces: tuple[str, ...] = ()


def _filtered(column: str, values: Sequence[str]) -> tuple[str, list[str]]:
    if not values:
        return "", []
    return f" AND {column} IN ({','.join('?' for _ in values)})", list(values)


def identity_document(
    connection: sqlite3.Connection,
    snapshot_id: str,
    selection: AssertionSelection | None = None,
) -> Record:
    """Project the registered assertions into one whole identity snapshot document.

    Every selected assertion is a member with its asserted valid interval. Its knowledge
    starts at ``known_from_us`` and ends when the earliest correction superseding it became
    known, so a superseded assertion stays visible to a cutoff before its correction.
    """
    selection = selection or AssertionSelection()
    provider_sql, provider_args = _filtered("provider", selection.providers)
    namespace_sql, namespace_args = _filtered("namespace", selection.namespaces)
    assertions = [
        dict(row)
        for row in connection.execute(
            f"SELECT {','.join(_ASSERTION_COLUMNS)} FROM identity_assertions WHERE 1=1"  # noqa: S608 -- fixed columns and placeholders
            + provider_sql
            + namespace_sql
            + " ORDER BY assertion_id",
            (*provider_args, *namespace_args),
        )
    ]
    successors = {
        str(predecessor): int(known)
        for predecessor, known in connection.execute(
            "SELECT supersedes_assertion_id,min(known_from_us) FROM identity_assertions "
            "WHERE supersedes_assertion_id IS NOT NULL GROUP BY supersedes_assertion_id"
        )
    }
    members = [
        {
            "ordinal": ordinal,
            "assertion_id": row["assertion_id"],
            "valid_from_us": row["valid_from_us"],
            "valid_to_us": row["valid_to_us"],
            "known_from_us": row["known_from_us"],
            "known_to_us": successors.get(cast("str", row["assertion_id"])),
        }
        for ordinal, row in enumerate(assertions)
    ]
    instrument_ids = sorted({cast("str", row["instrument_id"]) for row in assertions})
    source_ids = sorted({cast("str", row["source_snapshot_id"]) for row in assertions})
    instruments = [
        dict(row)
        for row in connection.execute(
            "SELECT instrument_id,issuer_id,asset_type,venue FROM instruments "
            "WHERE instrument_id IN (SELECT value FROM json_each(?))",
            (canonical_json_bytes(instrument_ids).decode(),),
        )
    ]
    sources = []
    for source_id in source_ids:
        source = connection.execute(
            "SELECT snapshot_id,provider,requested_at_us,retrieved_at_us,publication_at_us,status "
            "FROM source_snapshots WHERE snapshot_id=?",
            (source_id,),
        ).fetchone()
        files = [
            dict(row)
            for row in connection.execute(
                "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=?",
                (source_id,),
            )
        ]
        sources.append({**dict(source), "files": files})
    return {
        "schema": "aas-identity-snapshot-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "snapshot_id": snapshot_id,
        "instruments": instruments,
        "assertions": assertions,
        "members": members,
        "sources": sources,
    }


def _plan_report(plan: MembershipPlan) -> dict[str, object]:
    whole = plan.whole
    return {
        "snapshot_id": cast("IdentityPin", plan.pin).snapshot_id,
        "content_hash": plan.pin.content_hash,
        "members": len(cast("list[object]", whole["members"])),
        "instruments": len(cast("list[object]", whole["instruments"])),
        "sources": len(cast("list[object]", whole["sources"])),
        "parts": [
            {"snapshot_id": cast("IdentityPin", pin).snapshot_id, "content_hash": pin.content_hash}
            for pin in plan.part_pins
        ],
    }


def snapshot_identities(
    connection: sqlite3.Connection,
    snapshot_id: str,
    selection: AssertionSelection | None = None,
    *,
    created_at_us: int,
    apply: bool,
) -> dict[str, object]:
    """Plan, or register, the chunked identity snapshot of the selected assertions."""
    document = identity_document(connection, snapshot_id, selection)
    plan = plan_membership_manifest(document, identity=True)
    if not apply:
        return {"mode": "plan", **_plan_report(plan)}
    pin = register_identity_manifest(connection, document, created_at_us=created_at_us)
    if pin != plan.pin:
        raise ValueError("identity snapshot changed while it was being registered")
    return {"mode": "apply", **_plan_report(plan)}


# --- inspection ---------------------------------------------------------------------------


def show_instrument(connection: sqlite3.Connection, instrument_id: str) -> dict[str, object]:
    """SELECT only: one instrument, its issuer and every assertion about it."""
    instrument = _row(connection, "instruments", "instrument_id", instrument_id)
    if instrument is None:
        raise ValueError("unknown instrument")
    issuer_id = instrument["issuer_id"]
    issuer = None if issuer_id is None else _row(connection, "issuers", "issuer_id", str(issuer_id))
    assertions = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM identity_assertions WHERE instrument_id=? "
            "ORDER BY known_from_us,assertion_id",
            (instrument_id,),
        )
    ]
    return {"instrument": instrument, "issuer": issuer, "assertions": assertions}


def show_key(
    connection: sqlite3.Connection, provider: str, namespace: str, token: str
) -> dict[str, object]:
    """SELECT only: every assertion of one provider key, oldest knowledge first."""
    assertions = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM identity_assertions WHERE provider=? AND namespace=? AND token=? "
            "ORDER BY known_from_us,assertion_id",
            (provider, namespace, token),
        )
    ]
    return {
        "key": {"provider": provider, "namespace": namespace, "token": token},
        "assertions": assertions,
        "instrument_ids": sorted({cast("str", row["instrument_id"]) for row in assertions}),
    }


def show_snapshot(connection: sqlite3.Connection, snapshot_id: str) -> dict[str, object]:
    """SELECT only: a snapshot header and, for a manifest, its parts and member counts."""
    header = connection.execute(
        "SELECT content_hash,created_at_us FROM identity_snapshots WHERE snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if header is None:
        raise ValueError("unknown identity snapshot")
    pin = IdentityPin(snapshot_id, str(header[0]))
    parts = membership_parts(connection, pin)
    counted = [
        {
            "snapshot_id": cast("IdentityPin", part).snapshot_id,
            "content_hash": part.content_hash,
            "members": connection.execute(
                "SELECT count(*) FROM identity_snapshot_members WHERE snapshot_id=?",
                (cast("IdentityPin", part).snapshot_id,),
            ).fetchone()[0],
        }
        for part in parts or (pin,)
    ]
    return {
        "snapshot_id": snapshot_id,
        "content_hash": pin.content_hash,
        "created_at_us": header[1],
        "kind": "manifest" if parts else "document",
        "members": sum(cast("int", part["members"]) for part in counted),
        "parts": counted if parts else [],
    }
