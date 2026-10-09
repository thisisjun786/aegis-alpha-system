"""Retire superseded source-library tables only with a recorded proof.

``aas db source-retire --spec FILE --sha256 H [--backup DIR] --plan|--apply`` reads one
``aas-source-retirement-v1`` document. Each group names the sources to retire, the sources
they are equivalent to, one table on each side, the columns compared position by position
and, as ``uncompared``, every column of the retired table left out of the comparison. A
group is retired whole or not at all, and only when

1. no committed generation, binding, identity or universe row refers to any of its
   sources (a source's own ``sl:`` link is lineage, not a reference),
2. the ``aas-rowset-v1`` digest of the compared columns' row multiset is the same on
   both sides (``equivalence_digest``), and every retired table's columns are exactly the
   compared ones plus the declared ``uncompared`` ones, and
3. the verified backup ``--backup`` names holds every one of its commits, each of whose
   tables rehashes there to the digest the commit records, and lives on another device
   than the installation.

``--plan`` writes nothing. ``--apply`` retains the records in ``raw/``, prepares a
``source-retire`` intent whose payload they are, drops the tables in one DuckDB
transaction, writes one immutable ``source_retirements`` row per source and completes the
intent. Commit markers, ``sl:`` links and ``raw/`` bytes stay. Repeating the command or
``aas db recover`` finishes an interrupted intent from its retained records.

The digest proves the compared columns only, as an unkeyed multiset. An ``uncompared``
column is dropped on the owner's grant in the hashed document, the plan reports such a
group as ``partial_columns`` and every record's ``equivalence_spec`` names those columns;
their values come back only from the backup or the source's original bytes.
"""

from __future__ import annotations

# ruff: noqa: S608 -- every dynamic identifier passes schema.quoted; values are bound.
import hashlib
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.bulk_generation import encoded_cell_sql, stream_rowset
from aegis_alpha.storage.market import limit_duckdb, rollback
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.rowset import ROWSET_FORMAT
from aegis_alpha.storage.source_identity import LINK_PREFIX
from aegis_alpha.storage.source_library import (
    RETIREMENT_KIND,
    list_sources,
    manifest_digest,
    retired_sources,
    table_present,
    verify_tables,
)
from aegis_alpha.storage.state import atomic, complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    import duckdb

    from aegis_alpha.storage.workspace import Workspace

SPEC_SCHEMA: Final = "aas-source-retirement-v1"
EQUIVALENCE_FORMAT: Final = "aas-source-equivalence-v1"
REQUEST_SCHEMA: Final = "aas-source-retirement-request-v1"
RECORDS_SCHEMA: Final = "aas-source-retirement-records-v1"
OPERATION_PREFIX: Final = "source-retire:"
MAX_SPEC_BYTES: Final = 8 * 1024 * 1024
_MAX_DOCUMENT: Final = 64 * 1024 * 1024
_SEARCH_CHUNK: Final = 8 * 1024 * 1024
_SIDE_KEYS: Final = frozenset({"sources", "table", "columns"})
_GROUP_KEYS: Final = frozenset({"reason", "retire", "equivalent", "uncompared"})
_DEFAULT_BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
_BATCH_ROWS: Final = 65_536
_ROW_OBJECT_BYTES: Final = 256
_TEXT_FRAMING: Final = 5
_FIXED_WIDTH: Final = {"int": 9, "utc_us": 9, "float": 9, "date": 15, "decimal": 48, "bool": 2}
_U32_MAX: Final = (1 << 32) - 1
_ENCODED: Final = "temp.main.aas_retirement_rows"
_SMALL_INTEGERS: Final = frozenset(
    {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER"}
)
_WIDE_INTEGERS: Final = frozenset({"UBIGINT", "HUGEINT", "UHUGEINT"})
_DECIMAL: Final = re.compile(r"DECIMAL\((\d+),(\d+)\)")
_DECIMAL_SCALE: Final = 12
_DECIMAL_INTEGER_DIGITS: Final = 26
# State columns that point at a source snapshot. A source's own link rows
# (source_snapshots, source_files) are lineage and the retirement record is the proof.
_STATE_LINK_COLUMNS: Final = frozenset({"source_snapshot_id", "source_id", "ref_id"})
_STATE_LINEAGE: Final = frozenset({"source_snapshots", "source_files", "source_retirements"})
_MARKET_LINK_COLUMNS: Final = frozenset({"source_snapshot_id", "source_id"})
# Reasons that leave a group without tables to compare. References and the backup do not:
# a plan still reports the equivalence of a group that is refused for those.
_STRUCTURAL: Final = frozenset(
    {
        "unknown_source",
        "unknown_equivalent_source",
        "equivalent_source_retired",
        "group_partially_retired",
        "store_unsupported",
        "table_missing",
    }
)


class RetirementError(ValueError):
    """A retirement request that cannot be read or finished as written."""


@dataclass(frozen=True, slots=True)
class Side:
    """The sources on one side of a group, their shared table and compared columns."""

    sources: tuple[str, ...]
    table: str
    columns: tuple[str, ...]

    def document(self) -> dict[str, object]:
        return {"sources": list(self.sources), "table": self.table, "columns": list(self.columns)}


@dataclass(frozen=True, slots=True)
class Group:
    reason: str
    retire: Side
    equivalent: Side
    uncompared: tuple[str, ...] = ()

    def equivalence_spec(self) -> str:
        """The canonical ``equivalence_spec`` text every retired source of the group records."""
        return _canonical(
            {
                "format": EQUIVALENCE_FORMAT,
                "rowset_format": ROWSET_FORMAT,
                "retire": self.retire.document(),
                "equivalent": self.equivalent.document(),
                "uncompared": list(self.uncompared),
            }
        ).decode()


@dataclass(frozen=True, slots=True)
class RetirementSpec:
    sha256: str
    groups: tuple[Group, ...]


@dataclass(slots=True)
class _Source:
    source_id: str
    group: int
    rows: int = 0
    digest: str | None = None
    targets: tuple[str, ...] = ()
    uncompared: tuple[str, ...] | None = None
    references: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _GroupResult:
    index: int
    group: Group
    reasons: list[str] = field(default_factory=list)
    retire_rows: int = 0
    equivalent_rows: int = 0
    retire_digest: str | None = None
    equivalent_digest: str | None = None
    already_retired: bool = False


@dataclass(slots=True)
class _Backup:
    root: str | None
    backup_id: str | None
    reasons: list[str]
    market: duckdb.DuckDBPyConnection | None = None
    # Whether ``aas db backup --deep`` made it; retirement rehashes its sources either way.
    deep: bool | None = None


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


# --- the request document ------------------------------------------------------------------


def read_spec_file(path: Path, sha256: str) -> bytes:
    """Read an exact retirement document of at most 8 MiB."""
    absolute = path.absolute()
    with DescriptorTree.open_path(absolute.parent) as tree:
        raw = tree.read_bytes(absolute.name, max_bytes=MAX_SPEC_BYTES)
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise RetirementError("retirement document bytes do not match the expected SHA-256")
    return raw


def _keys(value: object, keys: frozenset[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise RetirementError(f"{name} must have exactly the fields {sorted(keys)}")
    return cast("dict[str, object]", value)


def _names(value: object, name: str, *, empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not empty):
        raise RetirementError(f"{name} must be a {'' if empty else 'nonempty '}list")
    names = []
    for item in value:
        if not isinstance(item, str) or not item or item != item.strip() or "\x00" in item:
            raise RetirementError(f"{name} holds exact nonempty text only")
        names.append(item)
    if len(set(names)) != len(names):
        raise RetirementError(f"{name} repeats an entry")
    return tuple(names)


def _side(value: object, name: str) -> Side:
    body = _keys(value, _SIDE_KEYS, name)
    table = body["table"]
    if not isinstance(table, str) or not table or "\x00" in table:
        raise RetirementError(f"{name}.table must be nonempty text")
    return Side(
        _names(body["sources"], name + ".sources"),
        table,
        _names(body["columns"], name + ".columns"),
    )


def parse_spec(raw: bytes, sha256: str) -> RetirementSpec:  # noqa: C901 -- one document boundary
    """Parse ``aas-source-retirement-v1``; refuse anything ambiguous before reading data."""
    if len(raw) > MAX_SPEC_BYTES:
        raise RetirementError("retirement document exceeds 8 MiB")
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise RetirementError("retirement document bytes do not match the expected SHA-256")
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        raise RetirementError("retirement document must be UTF-8 JSON without BOM or NUL")
    try:
        document = decode_json(raw)
    except (ValueError, RecursionError) as error:
        raise RetirementError(f"retirement document is not strict JSON: {error}") from None
    body = _keys(document, frozenset({"schema_version", "groups"}), "document")
    if body["schema_version"] != SPEC_SCHEMA:
        raise RetirementError("unsupported retirement document schema")
    entries = body["groups"]
    if not isinstance(entries, list) or not entries:
        raise RetirementError("groups must be a nonempty list")
    groups: list[Group] = []
    for number, entry in enumerate(entries):
        item = _keys(entry, _GROUP_KEYS, f"groups[{number}]")
        reason = item["reason"]
        if not isinstance(reason, str) or not reason.strip():
            raise RetirementError(f"groups[{number}].reason must be nonempty text")
        retire = _side(item["retire"], f"groups[{number}].retire")
        equivalent = _side(item["equivalent"], f"groups[{number}].equivalent")
        if len(retire.columns) != len(equivalent.columns):
            raise RetirementError(f"groups[{number}] compares unequal column counts")
        uncompared = _names(item["uncompared"], f"groups[{number}].uncompared", empty=True)
        if set(uncompared) & set(retire.columns):
            raise RetirementError(f"groups[{number}] declares a compared column uncompared")
        groups.append(Group(reason, retire, equivalent, uncompared))
    retiring = [(source, group.retire.table) for group in groups for source in group.retire.sources]
    if len(set(retiring)) != len(retiring):
        raise RetirementError("a source table is retired by more than one group")
    targets = {source for group in groups for source in group.equivalent.sources}
    if targets & {source for source, _ in retiring}:
        raise RetirementError("a source cannot be both retired and an equivalence target")
    return RetirementSpec(sha256, tuple(groups))


# --- catalog ------------------------------------------------------------------------------


def _commits(workspace: Workspace) -> dict[str, tuple[str, str, dict[str, object]]]:
    """Market commit markers by source ID: (operation ID, manifest text, manifest)."""
    if not schema.ensure(workspace):
        return {}
    return {
        str(row[0]): (str(row[1]), str(row[2]), json.loads(str(row[2])))
        for row in workspace.market.execute(
            "SELECT source_id,operation_id,manifest_json FROM source_library_commits"
        ).fetchall()
    }


def _table(manifest: dict[str, object], name: str) -> dict[str, object] | None:
    tables = cast("list[dict[str, object]]", manifest["tables"])
    return next((table for table in tables if table["name"] == name), None)


# --- references ---------------------------------------------------------------------------


def source_references(workspace: Workspace, source_ids: set[str]) -> dict[str, list[str]]:  # noqa: C901, PLR0912 -- one search per store
    """Where each source is still referenced, outside its own ``sl:`` link.

    State rows that name a snapshot or source (identity assertions, universe members,
    dataset sources, input bindings and feature inputs), every market table outside the
    source library with a ``source_snapshot_id`` or ``source_id`` column, every strategy
    registry registration (``strategy_registry``) whose records it holds, and every
    retained document a committed generation is evidenced by (promotion spec, research
    transform, import document) whose JSON names the source.
    """
    found: dict[str, list[str]] = {source: [] for source in source_ids}
    if not source_ids:
        return found
    # A name can denote two candidates (source ``sl:x`` and the link of source ``x``); it
    # counts as a reference to every candidate it may denote.
    names: dict[str, set[str]] = {}
    for source in source_ids:
        names.setdefault(source, set()).add(source)
        names.setdefault(LINK_PREFIX + source, set()).add(source)
    wanted = sorted(names)
    tables = [
        str(row[0])
        for row in workspace.state.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "AND name NOT LIKE 'source_library_%' ORDER BY name"
        )
    ]
    for table in tables:
        if table in _STATE_LINEAGE:
            continue
        columns = [
            str(row[1])
            for row in workspace.state.execute("PRAGMA table_info(" + schema.quoted(table) + ")")
        ]
        for column in sorted(_STATE_LINK_COLUMNS.intersection(columns)):
            for (value,) in workspace.state.execute(
                f"SELECT DISTINCT {schema.quoted(column)} FROM {schema.quoted(table)} "
                f"WHERE {schema.quoted(column)} IN (SELECT value FROM json_each(?))",
                (json.dumps(wanted),),
            ):
                for source in names[value]:
                    found[source].append(f"state.{table}.{column}")
    for table, column in workspace.market.execute(
        "SELECT table_name,column_name FROM duckdb_columns() "
        "WHERE database_name=current_database() AND schema_name='main' "
        "AND column_name IN ('source_snapshot_id','source_id') "
        "AND NOT starts_with(table_name,'sl_') "
        "AND NOT starts_with(table_name,'source_library_') ORDER BY table_name,column_name"
    ).fetchall():
        if column not in _MARKET_LINK_COLUMNS:
            continue
        for (value,) in workspace.market.execute(
            f"SELECT DISTINCT {schema.quoted(column)} FROM {schema.quoted(table)} "
            f"WHERE list_contains(?, CAST({schema.quoted(column)} AS VARCHAR))",
            [wanted],
        ).fetchall():
            for source in names[value]:
                found[source].append(f"market.{table}.{column}")
    if workspace.strategies is not None:
        from aegis_alpha.storage.strategy_registry import (  # noqa: PLC0415
            registry_source_references,
        )

        for source in registry_source_references(workspace.strategies) & source_ids:
            found[source].append("strategies.strategy_registrations.source_id")
    _document_references(workspace, names, found)
    return {source: sorted(set(places)) for source, places in found.items()}


def _document_references(
    workspace: Workspace, names: dict[str, set[str]], found: dict[str, list[str]]
) -> None:
    """Search the retained documents of committed generations for a source ID or link.

    The search is over bytes, for the JSON string of each name (quoted, in both its
    ASCII-escaped and its UTF-8 spelling), in bounded chunks that overlap by the longest
    pattern. A document of any size or format is searched, and any occurrence counts.
    """
    patterns: dict[bytes, set[str]] = {}
    for name, sources in names.items():
        patterns.setdefault(json.dumps(name).encode(), set()).update(sources)
        patterns.setdefault(json.dumps(name, ensure_ascii=False).encode(), set()).update(sources)
    overlap = max(len(pattern) for pattern in patterns) - 1
    rows = workspace.state.execute(
        "SELECT generation_id,transform_hash,manifest_hash FROM dataset_versions "
        "WHERE status='committed' ORDER BY generation_id"
    ).fetchall()
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        for generation, *digests in rows:
            for digest in dict.fromkeys(str(value) for value in digests):
                relative = digest[:2] + "/" + digest
                if not tree.exists(relative):
                    continue
                hits: set[str] = set()
                with tree.binary_reader(relative) as reader:
                    tail = b""
                    while chunk := reader.read(_SEARCH_CHUNK):
                        window = tail + chunk
                        hits.update(
                            source
                            for pattern, sources in patterns.items()
                            if pattern in window
                            for source in sources
                        )
                        tail = window[-overlap:] if overlap else b""
                for source in hits:
                    found[source].append(f"generation {generation} document {digest}")


# --- equivalence --------------------------------------------------------------------------


def _rowset_column(data_type: str, column: str) -> tuple[str, str] | None:  # noqa: PLR0911 -- one form per type
    """The aas-rowset-v1 type and exact SQL value of one source column, if it has one."""
    quoted = schema.quoted(column)
    if data_type == "VARCHAR":
        return "text", quoted
    if data_type == "BOOLEAN":
        return "bool", quoted
    if data_type in _SMALL_INTEGERS or data_type in _WIDE_INTEGERS:
        return "int", f"CAST({quoted} AS BIGINT)"
    if data_type in {"FLOAT", "DOUBLE"}:
        return "float", f"CAST({quoted} AS DOUBLE)"
    if data_type == "DATE":
        return "date", quoted
    if data_type in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_MS", "TIMESTAMP_S"}:
        return "utc_us", f"epoch_us({quoted})"
    if data_type == "TIMESTAMP_NS":
        # Exact only on whole microseconds; any other instant refuses the group.
        nanoseconds = f"epoch_ns({quoted})"
        return "utc_us", (
            f"CASE WHEN {quoted} IS NULL THEN NULL "
            f"WHEN {nanoseconds} % 1000 = 0 THEN {nanoseconds} // 1000 "
            "ELSE CAST(error('timestamp below microsecond precision') AS BIGINT) END"
        )
    match = _DECIMAL.fullmatch(data_type)
    if (
        match is not None
        and int(match[2]) <= _DECIMAL_SCALE
        and int(match[1]) - int(match[2]) <= _DECIMAL_INTEGER_DIGITS
    ):
        return "decimal", f"CAST({quoted} AS DECIMAL(38,12))"
    return None


class _Refused(Exception):  # noqa: N818 -- a refusal reason, not an error
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _relation(
    market: duckdb.DuckDBPyConnection, side: Side, targets: list[str]
) -> tuple[tuple[str, ...], str]:
    """One side's compared columns as ``c0..cN`` of one relation, with their rowset types."""
    types: tuple[str, ...] | None = None
    selects = []
    for target in targets:
        declared = {
            str(row[0]): str(row[1])
            for row in market.execute(
                "SELECT column_name,data_type FROM duckdb_columns() "
                "WHERE database_name=current_database() AND schema_name='main' AND table_name=?",
                [target],
            ).fetchall()
        }
        if not set(side.columns) <= declared.keys():
            raise _Refused("column_missing")
        mapped = [_rowset_column(declared[column], column) for column in side.columns]
        if any(item is None for item in mapped):
            raise _Refused("column_type_unsupported")
        pairs = cast("list[tuple[str, str]]", mapped)
        kinds = tuple(kind for kind, _ in pairs)
        if types is not None and kinds != types:
            raise _Refused("column_types_differ_between_sources")
        types = kinds
        selects.append(
            "SELECT "
            + ", ".join(f"{value} AS c{number}" for number, (_, value) in enumerate(pairs))
            + " FROM "
            + schema.quoted(target)
        )
    if types is None:
        raise _Refused("no_tables")
    return types, " UNION ALL ".join(selects)


def _side_digest(  # noqa: PLR0913 -- one side's exact hashing inputs
    workspace: Workspace,
    side: Side,
    *,
    fields: tuple[str, ...],
    targets: list[str],
    count: int,
    budget: ComputeBudget,
) -> tuple[tuple[str, ...], str]:
    """The rowset types and aas-rowset-v1 digest of one side's compared columns.

    Every cell takes its exact typed form: integers as ``int``, binary64 (and widened
    binary32) as ``float``, timestamps as UTC microseconds, decimals at scale 12. A value
    without that form (an integer beyond int64, NaN or an infinity) refuses the group.
    """
    import duckdb  # noqa: PLC0415 -- the market driver is already loaded by admission

    market = workspace.market
    types, relation = _relation(market, side, targets)
    if count > _U32_MAX:
        raise _Refused("rowset_too_large")
    limit_duckdb(market, budget)
    widths = [
        f"{_TEXT_FRAMING} + coalesce(max(octet_length(encode(c{number}))), 0)"
        if kind == "text"
        else str(_FIXED_WIDTH[kind])
        for number, kind in enumerate(types)
    ]
    floats = [f"isnan(c{n}) OR isinf(c{n})" for n, kind in enumerate(types) if kind == "float"]
    non_finite = (
        f"coalesce(sum(CASE WHEN {' OR '.join(floats)} THEN 1 ELSE 0 END), 0)" if floats else "0"
    )
    rowset = tuple(zip(fields, types, strict=True))
    cells = [encoded_cell_sql(f"c{number}", kind) for number, kind in enumerate(types)]
    try:
        statistics = market.execute(
            f"SELECT count(*), {' + '.join(widths)}, {non_finite} FROM ({relation})"
        ).fetchone()
        if statistics is None or int(statistics[0]) != count:
            raise ValueError("source table row count differs from its commit manifest")
        if int(statistics[2]):
            raise _Refused("non_finite_float")
        row_bytes = int(statistics[1])
        batch = min(_BATCH_ROWS, budget.available_bytes // (2 * row_bytes + _ROW_OBJECT_BYTES))
        if batch < 1:
            raise ComputeResourceError(
                f"one compared row ({row_bytes} encoded bytes) exceeds the materialization budget"
            )
        # Encode into a temporary table first. DuckDB then sorts its own spillable copy;
        # sorting straight off many persistent source tables exhausts the memory limit
        # instead of spilling.
        market.execute(
            f"CREATE OR REPLACE TEMP TABLE {_ENCODED} AS "
            f"SELECT {' || '.join(cells)} AS encoded FROM ({relation})"
        )
        try:
            digest = stream_rowset(
                market,
                rowset,
                ["encoded"],
                f"SELECT encoded FROM {_ENCODED}",
                [],
                count=count,
                batch_rows=batch,
            )
        finally:
            market.execute(f"DROP TABLE IF EXISTS {_ENCODED}")
    except (
        duckdb.ConversionException,
        duckdb.OutOfRangeException,
        duckdb.InvalidInputException,
    ) as error:
        raise _Refused("values_not_encodable") from error
    return types, digest


# --- backup -------------------------------------------------------------------------------


def device_of(path: Path) -> int:
    """The device a path lives on (``st_dev``)."""
    return os.stat(path).st_dev  # noqa: PTH116 -- st_dev of an admitted path


def _open_backup(workspace: Workspace, root: Path | None) -> _Backup:
    """Check a backup once: verified bytes, this installation, another device."""
    import duckdb  # noqa: PLC0415 -- the market driver is already loaded by admission

    from aegis_alpha.storage.backup import manifest_sha256, validated_backup  # noqa: PLC0415
    from aegis_alpha.storage.paths import resolve_home  # noqa: PLC0415

    if root is None:
        return _Backup(None, None, ["backup_missing"])
    backup_root = resolve_home(root)
    manifest = validated_backup(backup_root)
    backup = _Backup(str(backup_root), manifest_sha256(backup_root), [])
    deep = manifest.get("deep")
    backup.deep = deep if isinstance(deep, bool) else None
    if manifest.get("installation_id") != workspace.installation_id:
        backup.reasons.append("backup_of_another_installation")
        return backup
    installed = {
        device_of(path)
        for path in (
            workspace.paths.root,
            workspace.paths.state,
            workspace.paths.market,
            workspace.paths.raw,
        )
    }
    if device_of(backup_root) in installed:
        backup.reasons.append("backup_on_installation_device")
    backup.market = duckdb.connect(
        str(backup_root / "market.duckdb"),
        read_only=True,
        config={"threads": 1, "memory_limit": "256MB", "enable_external_access": False},
    )
    return backup


def _backup_holds(backup: _Backup, source_id: str, manifest_json: str) -> bool:
    """The backup's market store has this exact commit and every one of its tables."""
    if backup.market is None:
        return False
    if not backup.market.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_library_commits'"
    ).fetchone():
        return False
    row = backup.market.execute(
        "SELECT manifest_json FROM source_library_commits WHERE source_id=?", [source_id]
    ).fetchone()
    if row is None or str(row[0]) != manifest_json:
        return False
    for table in cast("list[dict[str, object]]", json.loads(manifest_json)["tables"]):
        target = str(table["target"])
        if not table_present(backup.market, target):
            return False
        counted = backup.market.execute(f"SELECT count(*) FROM {schema.quoted(target)}").fetchone()
        if counted is None or int(counted[0]) != table["rows"]:
            return False
    return True


def _backup_lacks(
    backup: _Backup, source_id: str, manifest_json: str, budget: ComputeBudget
) -> str | None:
    """Why the backup cannot restore this commit, or None when it holds it row for row.

    Each of the commit's tables must rehash in the backup to the digest the commit
    records. A backup verified without ``--deep`` never read those rows, so this is
    what proves them.
    """
    if backup.market is None or not _backup_holds(backup, source_id, manifest_json):
        return "backup_lacks_source"
    try:
        verify_tables(backup.market, json.loads(manifest_json), budget.available_bytes)
    except ComputeResourceError:
        raise
    except ValueError:
        return "backup_content_mismatch"
    return None


# --- plan ---------------------------------------------------------------------------------


@dataclass(slots=True)
class RetirementPlan:
    spec: RetirementSpec
    backup: _Backup
    groups: list[_GroupResult]
    sources: dict[str, _Source]

    def retirable(self) -> list[_GroupResult]:
        return [group for group in self.groups if not group.reasons and not group.already_retired]

    def report(self) -> dict[str, object]:
        candidates = list(self.sources.values())
        retirable = {source for group in self.retirable() for source in group.group.retire.sources}
        refused = Counter(reason for group in self.groups for reason in group.reasons)
        partial = [group for group in self.groups if group.group.uncompared]
        return {
            "spec_sha256": self.spec.sha256,
            "backup_root": self.backup.root,
            "backup_id": self.backup.backup_id,
            "backup_deep": self.backup.deep,
            "candidate_sources": len(candidates),
            "candidate_rows": sum(source.rows for source in candidates),
            "retirable_sources": len(retirable),
            "retirable_rows": sum(self.sources[source].rows for source in retirable),
            "referenced_sources": sum(1 for source in candidates if source.references),
            "references": sum(len(source.references) for source in candidates),
            "partial_column_groups": len(partial),
            "refusals": dict(sorted(refused.items())),
            "groups": [
                {
                    "group": group.index,
                    "reason": group.group.reason,
                    "status": "already_retired"
                    if group.already_retired
                    else ("retire" if not group.reasons else "refused"),
                    "reasons": group.reasons,
                    # What the digest proves: every column, or only the compared ones.
                    "compared": "partial_columns" if group.group.uncompared else "all_columns",
                    "uncompared_columns": list(group.group.uncompared),
                    "sources": len(group.group.retire.sources),
                    "rows": group.retire_rows,
                    "equivalent_sources": len(group.group.equivalent.sources),
                    "equivalent_rows": group.equivalent_rows,
                    "retire_digest": group.retire_digest,
                    "equivalent_digest": group.equivalent_digest,
                }
                for group in self.groups
            ],
            "sources": [
                {
                    "source_id": source.source_id,
                    "group": source.group,
                    "rows": source.rows,
                    "digest": source.digest,
                    "uncompared_columns": None
                    if source.uncompared is None
                    else list(source.uncompared),
                    "references": source.references,
                    "reasons": source.reasons,
                }
                for source in sorted(self.sources.values(), key=lambda item: item.source_id)
            ],
        }


def plan_retirement(  # noqa: C901, PLR0912, PLR0915 -- one ordered proof per group
    workspace: Workspace,
    spec: RetirementSpec,
    *,
    backup_root: Path | None,
    budget: ComputeBudget | None = None,
) -> RetirementPlan:
    """Prove each group without writing anything; refusals carry their reasons."""
    budget = budget or _DEFAULT_BUDGET
    commits = _commits(workspace)
    live = {str(row["source_id"]) for row in list_sources(workspace) if row["store"] == "market"}
    retired = retired_sources(workspace)
    targets_of_retired = {
        source
        for record in retired.values()
        for source in json.loads(str(record["equivalence_spec"]))["equivalent"]["sources"]
    }
    backup = _open_backup(workspace, backup_root)
    groups: list[_GroupResult] = []
    sources: dict[str, _Source] = {}
    covered: dict[str, set[str]] = {}
    for index, group in enumerate(spec.groups):
        result = _GroupResult(index, group)
        groups.append(result)
        states = [source in retired for source in group.retire.sources]
        if all(states):
            records = [retired[source] for source in group.retire.sources]
            if any(record["equivalence_spec"] != group.equivalence_spec() for record in records):
                result.reasons.append("retired_under_another_equivalence")
            result.already_retired = not result.reasons
            continue
        if any(states):
            result.reasons.append("group_partially_retired")
        for source in group.retire.sources:
            entry = sources.setdefault(source, _Source(source, index))
            covered.setdefault(source, set()).add(group.retire.table)
            if source not in commits or (source not in live and source not in retired):
                entry.reasons.append("unknown_source")
                continue
            _, manifest_json, manifest = commits[source]
            entry.digest = manifest_digest(manifest_json)
            tables = cast("list[dict[str, object]]", manifest["tables"])
            entry.rows = sum(cast("int", table["rows"]) for table in tables)
            entry.targets = tuple(str(table["target"]) for table in tables)
            if any(table.get("format") != "arrow" for table in tables):
                entry.reasons.append("store_unsupported")
            if (retired_table := _table(manifest, group.retire.table)) is not None:
                columns = cast("list[str]", retired_table["columns"])
                compared = set(group.retire.columns)
                entry.uncompared = tuple(column for column in columns if column not in compared)
                if set(entry.uncompared) != set(group.uncompared):
                    entry.reasons.append("uncompared_columns_differ")
            if source in targets_of_retired:
                entry.reasons.append("equivalence_target_of_a_retired_source")
        for source in group.equivalent.sources:
            if source in retired:
                result.reasons.append("equivalent_source_retired")
            elif source not in commits or source not in live:
                result.reasons.append("unknown_equivalent_source")
    for source, tables in covered.items():
        if source in commits:
            names = {
                str(table["name"])
                for table in cast("list[dict[str, object]]", commits[source][2]["tables"])
            }
            if names - tables:
                sources[source].reasons.append("table_not_covered")
    references = source_references(
        workspace, {source for source in sources if source not in retired}
    )
    for source, places in references.items():
        sources[source].references = places
        if places:
            sources[source].reasons.append("referenced")
    for source, entry in sources.items():
        if source in retired or "unknown_source" in entry.reasons:
            continue
        entry.reasons.extend(backup.reasons)
        if not backup.reasons and (
            lacking := _backup_lacks(backup, source, commits[source][1], budget)
        ):
            entry.reasons.append(lacking)
    for result in groups:
        if result.already_retired:
            continue
        group = result.group
        for source in group.retire.sources:
            for reason in sources[source].reasons:
                if reason not in result.reasons:
                    result.reasons.append(reason)
        sides = []
        for side in (group.retire, group.equivalent):
            tables = []
            for source in side.sources:
                if source not in commits:
                    break
                table = _table(commits[source][2], side.table)
                if table is None:
                    break
                tables.append(table)
            else:
                sides.append(tables)
                continue
            if "table_missing" not in result.reasons:
                result.reasons.append("table_missing")
        if len(sides) != 2 or _STRUCTURAL.intersection(result.reasons):  # noqa: PLR2004 -- both sides
            continue
        result.retire_rows = sum(cast("int", table["rows"]) for table in sides[0])
        result.equivalent_rows = sum(cast("int", table["rows"]) for table in sides[1])
        if result.retire_rows != result.equivalent_rows:
            result.reasons.append("row_counts_differ")
            continue
        try:
            retire_types, result.retire_digest = _side_digest(
                workspace,
                group.retire,
                fields=group.retire.columns,
                targets=[str(table["target"]) for table in sides[0]],
                count=result.retire_rows,
                budget=budget,
            )
            equivalent_types, result.equivalent_digest = _side_digest(
                workspace,
                group.equivalent,
                fields=group.retire.columns,
                targets=[str(table["target"]) for table in sides[1]],
                count=result.equivalent_rows,
                budget=budget,
            )
        except _Refused as refused:
            result.reasons.append(refused.reason)
            continue
        if retire_types != equivalent_types:
            result.reasons.append("column_types_differ")
        elif result.retire_digest != result.equivalent_digest:
            result.reasons.append("not_equivalent")
    return RetirementPlan(spec, backup, groups, sources)


# --- apply and finish ---------------------------------------------------------------------


def _records(plan: RetirementPlan) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for group in plan.retirable():
        equivalence = group.group.equivalence_spec()
        for source in group.group.retire.sources:
            entry = plan.sources[source]
            records.append(
                {
                    "source_id": source,
                    "digest": entry.digest,
                    "rows": entry.rows,
                    "reason": group.group.reason,
                    "equivalent_to_source_id": group.group.equivalent.sources[0],
                    "equivalence_spec": equivalence,
                    "equivalence_digest": group.retire_digest,
                    "backup_id": plan.backup.backup_id,
                }
            )
    return sorted(records, key=lambda record: str(record["source_id"]))


def request_hash(spec_sha256: str, backup_id: str, records: list[dict[str, object]]) -> str:
    """``aas-source-retirement-request-v1``: what one apply retires, under which proof."""
    return _digest(
        {
            "schema": REQUEST_SCHEMA,
            "spec_sha256": spec_sha256,
            "backup_id": backup_id,
            "records": records,
        }
    )


def _require_v2(workspace: Workspace) -> None:
    if (
        workspace.state.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_retirements'"
        ).fetchone()
        is None
    ):
        raise RetirementError("source retirement needs core schema v2; run aas db migrate --to 2")


def _quiet(workspace: Workspace) -> None:
    if workspace.state.execute("SELECT 1 FROM runs WHERE status='RUNNING'").fetchone() or (
        workspace.state.execute(
            "SELECT 1 FROM storage_operations WHERE phase='PREPARED' AND kind!=?",
            (RETIREMENT_KIND,),
        ).fetchone()
    ):
        raise RetirementError("stop running analyses and recover prepared operations first")


def retire_sources(
    workspace: Workspace,
    spec: RetirementSpec,
    *,
    backup_root: Path | None,
    apply: bool,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Plan (writing nothing) or retire every group whose proof holds, in one intent."""
    if not apply:
        plan = plan_retirement(workspace, spec, backup_root=backup_root, budget=budget)
        try:
            return {
                "plan": True,
                "writes": 0,
                **plan.report(),
                "apply_needs_v2": _needs_v2(workspace),
            }
        finally:
            _close(plan.backup)
    _require_v2(workspace)
    if backup_root is None:
        raise RetirementError("--apply needs --backup naming a verified other-device backup")
    _quiet(workspace)
    finished = [
        str(row[0])
        for row in workspace.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind=? AND phase='PREPARED'",
            (RETIREMENT_KIND,),
        ).fetchall()
    ]
    for operation_id in finished:
        finish_retirement(workspace, operation_id)
    plan = plan_retirement(workspace, spec, backup_root=backup_root, budget=budget)
    try:
        records = _records(plan)
        operation_id = None
        if records:
            backup_id = cast("str", plan.backup.backup_id)
            request = request_hash(spec.sha256, backup_id, records)
            operation_id = OPERATION_PREFIX + request
            payload = _canonical(
                {
                    "schema": RECORDS_SCHEMA,
                    "request_hash": request,
                    "retired_at_us": time.time_ns() // 1000,
                    "records": records,
                }
            )
            if len(payload) > _MAX_DOCUMENT:
                # Recovery reads the payload under this bound; never prepare one it cannot read.
                raise RetirementError(
                    "retirement records exceed 64 MiB; split the document into smaller groups"
                )
            _, payload_hash, _ = put_raw(workspace.paths.raw, payload)
            prepare_operation(
                workspace.state,
                operation_id=operation_id,
                kind=RETIREMENT_KIND,
                request_hash=request,
                target_id=workspace.installation_id,
                expected_parent=None,
                payload_hash=payload_hash,
            )
            finish_retirement(workspace, operation_id)
        return {
            "plan": False,
            **plan.report(),
            "operation_id": operation_id,
            "recovered": finished,
            "retired": [record["source_id"] for record in records],
            "retired_rows": sum(cast("int", record["rows"]) for record in records),
            "raw_deleted": False,
        }
    finally:
        _close(plan.backup)


def _needs_v2(workspace: Workspace) -> bool:
    try:
        _require_v2(workspace)
    except RetirementError:
        return True
    return False


def _close(backup: _Backup) -> None:
    if backup.market is not None:
        backup.market.close()
        backup.market = None


def _payload(workspace: Workspace, operation: dict[str, object]) -> list[dict[str, object]]:
    digest = str(operation["payload_hash"])
    relative = digest[:2] + "/" + digest
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        if not tree.exists(relative):
            raise RetirementError("retained retirement records are absent from raw/")
        payload = tree.read_bytes(relative, max_bytes=_MAX_DOCUMENT)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise RetirementError("retained retirement records do not match their address")
    body = json.loads(payload)
    if (
        not isinstance(body, dict)
        or body.get("schema") != RECORDS_SCHEMA
        or body.get("request_hash") != operation["request_hash"]
    ):
        raise RetirementError("retained retirement records do not belong to this intent")
    records = cast("list[dict[str, object]]", body["records"])
    retired_at = body["retired_at_us"]
    backup = {str(record["backup_id"]) for record in records}
    specs = {str(record["equivalence_spec"]) for record in records}
    if len(backup) != 1 or not specs:
        raise RetirementError("retained retirement records are malformed")
    for record in records:
        record["retired_at_us"] = retired_at
    return records


def retirement_started(workspace: Workspace, operation: dict[str, object]) -> bool:
    """Whether any table the intent retires is already gone (it must then be finished)."""
    commits = _commits(workspace)
    for record in _payload(workspace, operation):
        commit = commits.get(str(record["source_id"]))
        if commit is None:
            continue
        for table in cast("list[dict[str, object]]", commit[2]["tables"]):
            if not table_present(workspace.market, str(table["target"])):
                return True
    return False


def finish_retirement(  # noqa: C901, PLR0912 -- drop, record and complete one intent
    workspace: Workspace, operation_id: str
) -> bool:
    """Drop what the prepared intent still names, record it, then complete the intent.

    The proof was taken before the intent; source tables are immutable, so finishing
    re-checks only what can change: each commit still matches its recorded digest and
    nothing has started to reference a source whose tables are still present.
    """
    operation = get_operation(workspace.state, operation_id)
    if operation is None or operation["kind"] != RETIREMENT_KIND:
        raise RetirementError("expected a source retirement intent")
    if operation["phase"] == "COMPLETED":
        return True
    if operation["phase"] != "PREPARED":
        raise RetirementError("a quarantined retirement intent cannot be finished")
    _require_v2(workspace)
    records = _payload(workspace, operation)
    commits = _commits(workspace)
    drops: list[str] = []
    pending: set[str] = set()
    for record in records:
        source = str(record["source_id"])
        commit = commits.get(source)
        if commit is None or manifest_digest(commit[1]) != record["digest"]:
            raise RetirementError(f"source {source} no longer matches its retirement record")
        present = [
            str(table["target"])
            for table in cast("list[dict[str, object]]", commit[2]["tables"])
            if table_present(workspace.market, str(table["target"]))
        ]
        if present:
            pending.add(source)
            drops.extend(present)
    referenced = {
        source: places for source, places in source_references(workspace, pending).items() if places
    }
    if referenced:
        raise RetirementError(
            "a source this intent retires is referenced now; quarantine "
            + operation_id
            + ": "
            + json.dumps(referenced, sort_keys=True)
        )
    if drops:
        market = workspace.market
        market.execute("BEGIN TRANSACTION")
        try:
            for target in drops:
                market.execute("DROP TABLE " + schema.quoted(target))
            market.execute("COMMIT")
        except BaseException:
            rollback(market)
            raise
    columns = (
        "source_id",
        "digest",
        "rows",
        "reason",
        "equivalent_to_source_id",
        "equivalence_spec",
        "equivalence_digest",
        "backup_id",
        "operation_id",
        "retired_at_us",
    )
    with atomic(workspace.state):
        for record in records:
            row = {**record, "operation_id": operation_id}
            existing = workspace.state.execute(
                "SELECT " + ",".join(columns) + " FROM source_retirements WHERE source_id=?",
                (row["source_id"],),
            ).fetchone()
            if existing is None:
                workspace.state.execute(
                    "INSERT INTO source_retirements("
                    + ",".join(columns)
                    + ") VALUES ("
                    + ",".join("?" for _ in columns)
                    + ")",
                    tuple(row[name] for name in columns),
                )
            elif tuple(existing) != tuple(row[name] for name in columns):
                raise RetirementError(f"source {row['source_id']} has a different retirement")
    complete_operation(workspace.state, operation_id, str(operation["request_hash"]))
    return True


def recover_retirement(workspace: Workspace, operation: dict[str, object]) -> bool:
    """``aas db recover`` finishes a prepared retirement exactly as repeating it would."""
    return finish_retirement(workspace, str(operation["operation_id"]))
