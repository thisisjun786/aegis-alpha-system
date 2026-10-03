"""``aas data promote``: one hashed spec turns pinned source tables into one generation.

Order: verify the pinned sources, identity snapshot, calendars and the generations a mapper
references; map the source rows in
DuckDB; resolve instruments; apply the decimal and time rules; diff against the parent
head; compute quality flags; plan the bulk generation; then (apply only) retain the spec,
request and manifest in ``raw/``, record the intent, commit marker, rows and flags in one
DuckDB transaction, and finish the state catalog. ``--plan`` runs the same computation
and writes nothing.

No clock reaches a market column, so the same request computes the same rows on any
day. The same request returns its existing generation; a request whose parent is no
longer the dataset head fails the parent CAS and is planned again. A promotion
interrupted after its intent is finished by ``aas db recover``, which publishes and
catalogs only what the retained spec recomputes; no provider is called.
"""

from __future__ import annotations

# ruff: noqa: S608 -- every dynamic identifier is engine-owned and quoted; values are bound.
import hashlib
import json
from dataclasses import dataclass, field
from fractions import Fraction
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.bulk_generation import (
    BulkPlan,
    BulkRequest,
    ParentChangedError,
    encoded_cell_sql,
    plan_generation_bulk,
    publish_generation_bulk,
    record_identity_sql,
    stream_rowset,
    verify_generation_bulk,
)
from aegis_alpha.storage.market import (
    RECORD_SCHEMA,
    generation_chain,
    limit_duckdb,
    marker_for,
    market_version,
    record_identity,
)
from aegis_alpha.storage.market_schema import DOMAIN_VERSIONS, DOMAINS, NATURAL_KEYS
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    membership_parts,
    verify_membership_pin,
)
from aegis_alpha.storage.promotion import decimal_rules, formats
from aegis_alpha.storage.promotion.mappers import (
    MANIFEST_ITEMS,
    reference_table,
    references,
    resolved_column,
)
from aegis_alpha.storage.promotion.spec import PromotionSpec, parse_spec
from aegis_alpha.storage.promotion.time_rules import (
    CLAMP_FLAG,
    TIME_COLUMNS,
    UNKNOWN_NULL,
    rule_sql,
)
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_library import source_metadata
from aegis_alpha.storage.source_reader import resolve_source
from aegis_alpha.storage.state import atomic, complete_operation, get_operation, prepare_operation

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import date
    from pathlib import Path

    import duckdb

    from aegis_alpha.storage.market_inputs import GenerationPin
    from aegis_alpha.storage.source_reader import SourcePin
    from aegis_alpha.storage.workspace import Workspace

OPERATION_KIND: Final = "promotion"
MANIFEST_SCHEMA: Final = "aas-promotion-manifest-v1"
DATASET_OWNER: Final = "promotion"
FLAG_SCHEMA: Final = (
    ("record_id", "text"),
    ("revision_id", "text"),
    ("rule_id", "text"),
    ("rule_version", "text"),
    ("flag", "text"),
    ("detail", "text"),
)
_DEFAULT_BUDGET: Final = ComputeBudget(Fraction(1), 512 * 1024 * 1024)
_MAX_RAW: Final = 64 * 1024 * 1024
_SAMPLE: Final = 20
_DAY_US: Final = 86_400_000_000
_BATCH: Final = 4096
_LINK: Final = "sl:"
_TEMP: Final = (
    "src",
    "items",
    "map",
    "identity",
    "res",
    "rows",
    "head",
    "diff",
    "tomb",
    "delta",
    "stage",
    "flags",
    "scope",
    "fix",
    "ref",
    "cal0",
    "cal1",
)
_FLAG_ROW_BYTES: Final = 4096
_COMPARED_KEYS: Final = tuple(name for name in NATURAL_KEYS["prices"] if name != "price_role")
PARTIAL_FLAG: Final = "provider_reported_partial"
PARTITION_CHECK: Final = ("partition_row_count", "1")
_REFERENCE_WINDOW_DAYS: Final = 31
_REFERENCE_LOOKBACK_DAYS: Final = 366


def _t(name: str) -> str:
    return f"_aas_p_{name}"


def _q(name: str) -> str:
    return formats.quote_identifier(name)


@dataclass(frozen=True, slots=True)
class _Flag:
    """One flag condition: its rule, flag name, the column it names, and its SQL over a row."""

    rule_id: str
    version: str
    flag: str
    detail: str
    condition: str


@dataclass(slots=True)
class _Source:
    pin: SourcePin
    target: str
    columns: list[tuple[str, str]]
    retrieved_at_us: int | None


@dataclass(slots=True)
class PromotionPlan:
    """What a promotion would write, with every count the report shows."""

    spec: PromotionSpec
    request_hash: str
    request: bytes
    generation_id: str
    operation_id: str
    version: str
    sequence: int
    report: dict[str, object] = field(default_factory=dict)
    blocking: list[str] = field(default_factory=list)
    refusals: list[str] = field(default_factory=list)
    bulk: BulkPlan | None = None
    flags_digest: str | None = None
    flag_rows: int = 0
    delta_rows: int = 0
    ingested_through_us: int | None = None
    manifest: bytes | None = None

    def summary(self) -> dict[str, object]:
        return {
            "dataset_id": self.spec.dataset_id,
            "domain": self.spec.domain,
            "parent": self.spec.parent,
            "version": self.version,
            "generation_id": self.generation_id,
            "operation_id": self.operation_id,
            "request_hash": self.request_hash,
            "spec_sha256": self.spec.sha256,
            "mapper": self.spec.mapper_name,
            "delta_rows": self.delta_rows,
            "flag_rows": self.flag_rows,
            "blocking": list(self.blocking),
            "refusals": list(self.refusals),
            **self.report,
            "marker": None if self.bulk is None else dict(self.bulk.marker),
        }


# --- identities and evidence ---------------------------------------------------------------


def generation_identity(request_hash: str) -> tuple[str, str]:
    """(generation ID, operation ID) of a promotion request."""
    return "prm-" + request_hash, "promotion:" + request_hash


def _read_raw(workspace: Workspace, digest: str) -> bytes:
    relative = digest[:2] + "/" + digest
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        if not tree.exists(relative):
            raise ValueError(f"retained promotion evidence {digest} is absent from raw/")
        payload = tree.read_bytes(relative, max_bytes=_MAX_RAW)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("retained promotion evidence does not match its address")
    return payload


def read_spec_file(path: Path, sha256: str) -> bytes:
    """Read an exact spec file of at most 64 MiB."""
    absolute = path.absolute()
    with DescriptorTree.open_path(absolute.parent) as tree:
        raw = tree.read_bytes(absolute.name, max_bytes=_MAX_RAW)
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("promotion spec bytes do not match the expected SHA-256")
    return raw


def _count(
    market: duckdb.DuckDBPyConnection, sql: str, parameters: list[object] | None = None
) -> int:
    row = market.execute(sql, parameters or []).fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def _columns(market: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in market.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


# --- pinned inputs -------------------------------------------------------------------------


def _verify_pin(workspace: Workspace, pin: GenerationPin, budget: ComputeBudget) -> list[str]:
    """Check a generation pin against its marker and catalog; return its chain's IDs."""
    marker = marker_for(workspace.market, pin.generation_id)
    if (
        marker["dataset_id"] != pin.dataset_id
        or marker["version"] != pin.version
        or marker["chain_hash"] != pin.chain_hash
        or marker["request_hash"] != pin.manifest_hash
    ):
        raise ValueError(f"generation pin {pin.dataset_id}@{pin.version} does not match its marker")
    if (
        workspace.state.execute(
            "SELECT 1 FROM dataset_versions WHERE status='committed' AND dataset_id=? AND "
            "version=? AND generation_id=? AND chain_hash=? AND manifest_hash=?",
            (pin.dataset_id, pin.version, pin.generation_id, pin.chain_hash, pin.manifest_hash),
        ).fetchone()
        is None
    ):
        raise ValueError(f"generation pin {pin.dataset_id}@{pin.version} is not committed")
    verify_generation_bulk(workspace.market, pin.generation_id, budget=budget)
    return [
        str(item["generation_id"]) for item in generation_chain(workspace.market, pin.generation_id)
    ]


def _heads_sql(domain: str) -> str:
    """Every record's head over a chain given as one VARCHAR[] parameter."""
    return (
        "SELECT * EXCLUDE (_aas_rank) FROM (SELECT p.*, row_number() OVER "
        "(PARTITION BY p.record_id ORDER BY g.sequence DESC) AS _aas_rank "
        f"FROM {_q(domain)} p JOIN market_generations g ON g.generation_id = p.generation_id "
        "WHERE p.generation_id IN (SELECT unnest(?::VARCHAR[]))) WHERE _aas_rank = 1"
    )


def _sources(workspace: Workspace, spec: PromotionSpec, plan: PromotionPlan) -> list[_Source]:
    found = []
    for pin in spec.sources:
        table = resolve_source(workspace, pin)
        if table["store"] != "market" or table["format"] != "arrow":
            raise ValueError("promotion reads source tables held in the market store")
        target = str(table["target"])
        columns = [
            (name, kind)
            for name, kind in _columns(workspace.market, _q(target)).items()
            if name != "_aas_ordinal"
        ]
        if [name for name, _ in columns] != list(cast("list[str]", table["columns"])):
            raise ValueError("source table columns differ from its manifest")
        link = workspace.state.execute(
            "SELECT retrieved_at_us FROM source_snapshots WHERE snapshot_id=?",
            (_LINK + pin.source_id,),
        ).fetchone()
        found.append(_Source(pin, target, columns, None if link is None else int(link[0])))
    unlinked = [source.pin.source_id for source in found if source.retrieved_at_us is None]
    if unlinked:
        plan.blocking.append(
            f"{len(unlinked)} pinned sources have no sl: link; run aas db source-link --apply"
        )
        plan.report["unlinked_sources"] = unlinked
    first = found[0].columns
    if any(source.columns != first for source in found):
        raise ValueError("pinned source tables must share one column schema")
    kinds = dict(first)
    for name, allowed in spec.mapper.source_columns().items():
        if kinds.get(name) not in allowed:
            raise ValueError(
                f"mapper {spec.mapper_name} needs source column {name} as {sorted(allowed)}"
            )
    for _, kind in first:
        if kind not in formats.SOURCE_ROW_TYPES:
            raise ValueError(f"source column type {kind} has no aas-source-row-v1 form")
    return found


def _identity(
    workspace: Workspace, spec: PromotionSpec, plan: PromotionPlan, budget: ComputeBudget
) -> None:
    """Load the pinned snapshot's current resolution rows for the mapper's assertion key."""
    market = workspace.market
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('identity')} (token VARCHAR, instrument_id VARCHAR, "
        "valid_from_us BIGINT, valid_to_us BIGINT)"
    )
    pin = spec.identity_snapshot
    key = spec.mapper.identity(spec.mapper_args)
    if pin is None or key is None:
        return
    if (
        workspace.state.execute(
            "SELECT 1 FROM identity_snapshots WHERE snapshot_id=? AND content_hash=?",
            (pin.snapshot_id, pin.content_hash),
        ).fetchone()
        is None
    ):
        plan.blocking.append(f"identity snapshot {pin.snapshot_id} is not registered")
        return
    parts = membership_parts(workspace.state, pin)
    for part in parts or (pin,):
        verify_membership_pin(
            workspace.state, part, max_materialization_bytes=budget.available_bytes
        )
    names = [cast("IdentityPin", part).snapshot_id for part in parts] or [pin.snapshot_id]
    cursor = workspace.state.execute(
        "SELECT a.token, a.instrument_id, m.valid_from_us, m.valid_to_us "
        "FROM identity_snapshot_members m JOIN identity_assertions a USING (assertion_id) "
        "WHERE m.snapshot_id IN (SELECT value FROM json_each(?)) AND m.known_to_us IS NULL "
        "AND a.provider=? AND a.namespace=?",
        (json.dumps(names), key.provider, key.namespace),
    )
    while batch := cursor.fetchmany(_BATCH):
        market.executemany(
            f"INSERT INTO {_t('identity')} VALUES (?, ?, ?, ?)", [tuple(row) for row in batch]
        )


def _calendars(workspace: Workspace, spec: PromotionSpec, budget: ComputeBudget) -> dict[int, str]:
    """Load each calendar rule's pinned sessions; map the time column index to its table."""
    tables: dict[int, str] = {}
    for index, column in enumerate(TIME_COLUMNS):
        rule = spec.time_rules[column]
        if rule.calendar is None:
            continue
        chain = _verify_pin(workspace, rule.calendar, budget)
        name = _t(f"cal{index}")
        workspace.market.execute(
            f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT session_date, open_at_us, close_at_us "
            f"FROM ({_heads_sql('calendar_sessions')}) WHERE op <> 'TOMBSTONE' "
            "AND calendar_id = ? AND venue = ?",
            [chain, rule.args["calendar_id"], rule.args["venue"]],
        )
        tables[index] = name
    return tables


def _references(workspace: Workspace, spec: PromotionSpec, budget: ComputeBudget) -> None:
    """Load the head rows of each generation the mapper references into its temp table.

    The pin is checked like a calendar pin, and its dataset must hold the referenced
    domain. Only the domain columns of non-TOMBSTONE heads are loaded.
    """
    for name, reference in references(spec.mapper, spec.mapper_args).items():
        chain = _verify_pin(workspace, reference.pin, budget)
        found = workspace.state.execute(
            "SELECT domain FROM datasets WHERE dataset_id=?", (reference.pin.dataset_id,)
        ).fetchone()
        if found is None or str(found[0]) != reference.domain:
            raise ValueError(
                f"mapper reference {name} must pin a {reference.domain} dataset generation"
            )
        columns = ", ".join(_q(column) for column, _ in DOMAINS[reference.domain])
        workspace.market.execute(
            f"CREATE OR REPLACE TEMP TABLE {reference_table(name)} AS SELECT {columns} "
            f"FROM ({_heads_sql(reference.domain)}) WHERE op <> 'TOMBSTONE'",
            [chain],
        )


# --- the SQL pipeline ----------------------------------------------------------------------


def _layered(base: str, layers: Sequence[Sequence[tuple[str, str]]]) -> str:
    sql = base
    for layer in layers:
        if layer:
            added = ", ".join(f"{expr} AS {alias}" for alias, expr in layer)
            sql = f"SELECT *, {added} FROM ({sql})"
    return sql


def _fix(
    market: duckdb.DuckDBPyConnection,
    query: str,
    compute: Callable[[tuple[object, ...]], str],
    table: str,
    column: str,
) -> int:
    """Fill ``column`` in Python for the rows SQL could not compute exactly.

    ``query`` selects ``_aas_pin, _aas_ordinal`` first and has a WHERE clause; rows are
    read in key order a batch at a time, because temp tables live on this connection
    and an open result cannot stay open across the inserts.
    """
    fix = _t("fix")
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {fix} (_aas_pin INTEGER, _aas_ordinal BIGINT, v VARCHAR)"
    )
    last: tuple[int, int] = (-(2**31), -(2**63))
    while batch := market.execute(
        f"{query} AND (_aas_pin, _aas_ordinal) > (?, ?) "
        f"ORDER BY _aas_pin, _aas_ordinal LIMIT {_BATCH}",
        list(last),
    ).fetchall():
        market.executemany(
            f"INSERT INTO {fix} VALUES (?, ?, ?)",
            [(row[0], row[1], compute(tuple(row))) for row in batch],
        )
        last = (int(batch[-1][0]), int(batch[-1][1]))
    fixed = _count(market, f"SELECT count(*) FROM {fix}")
    if fixed:
        market.execute(
            f"UPDATE {table} SET {column} = {fix}.v FROM {fix} WHERE {table}._aas_pin = "
            f"{fix}._aas_pin AND {table}._aas_ordinal = {fix}._aas_ordinal"
        )
    return fixed


def _stage_sources(
    workspace: Workspace, spec: PromotionSpec, sources: list[_Source], plan: PromotionPlan
) -> None:
    market = workspace.market
    columns = sources[0].columns
    names = ", ".join(_q(name) for name, _ in columns)
    where = ""
    if spec.partition is not None:
        day = spec.mapper.partition_sql
        where = (
            f" WHERE ({day}) >= DATE '{spec.partition.start.isoformat()}' "
            f"AND ({day}) < DATE '{spec.partition.end.isoformat()}'"
        )
        undated = sum(
            _count(market, f"SELECT count(*) FROM {_q(source.target)} WHERE ({day}) IS NULL")
            for source in sources
        )
        if undated:
            plan.refusals.append(
                f"{undated} source rows have no partition date, so no partition holds them"
            )
    union = " UNION ALL ".join(
        f"SELECT {index}::INTEGER AS _aas_pin, _aas_ordinal, {names} "
        f"FROM {_q(source.target)}{where}"
        for index, source in enumerate(sources)
    )
    expression, plain, invalid = formats.source_row_hash_sql(columns)
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('src')} AS SELECT *, "
        f"CASE WHEN ({plain}) AND NOT ({invalid}) THEN {expression} END AS _aas_row_hash, "
        f"({invalid}) AS _aas_row_invalid FROM ({union})"
    )
    invalid_rows = _count(market, f"SELECT count(*) FROM {_t('src')} WHERE _aas_row_invalid")
    if invalid_rows:
        plan.refusals.append(f"{invalid_rows} source rows hold dates outside years 1..9999")
    fragments = ", ".join(formats.source_row_fragments_sql(columns))
    _fix(
        market,
        f"SELECT _aas_pin, _aas_ordinal, {fragments} FROM {_t('src')} "
        "WHERE _aas_row_hash IS NULL AND NOT _aas_row_invalid",
        lambda row: formats.source_row_hash_from_fragments(columns, row[2:]),
        _t("src"),
        "_aas_row_hash",
    )


def _stage_items(
    workspace: Workspace, spec: PromotionSpec, sources: list[_Source], plan: PromotionPlan
) -> None:
    """Stage the manifest list the mapper reads as ``MANIFEST_ITEMS``, one row per element."""
    market = workspace.market
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {MANIFEST_ITEMS} (_aas_pin INTEGER, item VARCHAR)"
    )
    name = spec.mapper.manifest_items
    if name is None:
        return
    lacking = []
    unverified = []
    for index, source in enumerate(sources):
        try:
            metadata = source_metadata(workspace, source.pin.source_id)
        except ValueError:
            unverified.append(source.pin.source_id)
            continue
        items = metadata.get(name) if isinstance(metadata, dict) else None
        if not isinstance(items, list):
            lacking.append(source.pin.source_id)
            continue
        for start in range(0, len(items), _BATCH):
            market.executemany(
                f"INSERT INTO {MANIFEST_ITEMS} VALUES (?, ?)",
                [
                    (index, formats.canonical(item).decode())
                    for item in items[start : start + _BATCH]
                ],
            )
    if unverified:
        plan.refusals.append(
            f"{len(unverified)} pinned sources have a manifest that does not match its request hash"
        )
    if lacking:
        plan.refusals.append(f"{len(lacking)} pinned sources have no manifest metadata list {name}")


def _map(workspace: Workspace, spec: PromotionSpec) -> bool:
    """Materialize the mapper's relation and check its columns; return whether it has fields."""
    market = workspace.market
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('map')} AS "
        + spec.mapper.select(_t("src"), spec.mapper_args)
    )
    described = _columns(market, _t("map"))
    numeric = spec.mapper.numeric_columns(spec.mapper_args)
    expected = {
        "_aas_pin": "INTEGER",
        "_aas_ordinal": "BIGINT",
        "_aas_row_hash": "VARCHAR",
        "_aas_ingested_at_us": "BIGINT",
    }
    resolved = _resolved_column(spec)
    if resolved is not None:
        expected |= {"_aas_id_token": "VARCHAR", "_aas_id_at_us": "BIGINT"}
    for name, kind in DOMAINS[spec.domain]:
        if name != resolved:
            expected[name] = numeric.get(name, kind.rstrip("?"))
    for name, kind in spec.mapper.time_inputs.items():
        expected["_aas_t_" + name] = "DATE" if kind == "date" else "BIGINT"
    for column in spec.mapper.row_flags.values():
        expected[column] = "BOOLEAN"
    fields = spec.domain == "prices" and "fields" in described
    if fields:
        expected["fields"] = "VARCHAR"
    if described != expected:
        raise ValueError(f"mapper {spec.mapper_name} output does not match its declared columns")
    return fields


def _resolved_column(spec: PromotionSpec) -> str | None:
    """The domain column the mapper's identity key resolves, or None without a key."""
    if spec.mapper.identity(spec.mapper_args) is None:
        return None
    return resolved_column(spec.domain)


def _resolved(workspace: Workspace, spec: PromotionSpec) -> str:
    """The mapped rows with the resolved instrument and how many instruments matched."""
    market = workspace.market
    column = _resolved_column(spec)
    if column is None:
        # A domain whose instrument is optional, mapped without an identity key, names none.
        unnamed = (
            ", CAST(NULL AS VARCHAR) AS instrument_id"
            if "instrument_id" in dict(DOMAINS[spec.domain])
            else ""
        )
        return f"SELECT m.*{unnamed}, 1 AS _aas_matches FROM {_t('map')} m"
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('res')} AS SELECT m._aas_pin, m._aas_ordinal, "
        "count(DISTINCT i.instrument_id) AS matches, min(i.instrument_id) AS instrument_id "
        f"FROM {_t('map')} m JOIN {_t('identity')} i ON i.token = m._aas_id_token "
        "AND i.valid_from_us <= m._aas_id_at_us "
        "AND (i.valid_to_us IS NULL OR m._aas_id_at_us < i.valid_to_us) GROUP BY ALL"
    )
    return (
        f"SELECT m.*, r.instrument_id AS {_q(column)}, coalesce(r.matches, 0) AS _aas_matches "
        f"FROM {_t('map')} m "
        f"LEFT JOIN {_t('res')} r ON r._aas_pin = m._aas_pin AND r._aas_ordinal = m._aas_ordinal"
    )


def _add_row_flags(spec: PromotionSpec, finals: list[tuple[str, str]], flags: list[_Flag]) -> None:
    """Add the flags a mapper reads off the source row, under the mapper's ``name@major``."""
    for position, (flag, column) in enumerate(sorted(spec.mapper.row_flags.items())):
        alias = f"_aas_rf{position}"
        finals.append((alias, f"coalesce({_q(column)}, false)"))
        flags.append(_Flag(spec.mapper.name, str(spec.mapper.major), flag, "", alias))


def _rows(
    workspace: Workspace,
    spec: PromotionSpec,
    sources: list[_Source],
    calendars: dict[int, str],
    *,
    fields: bool,
) -> list[_Flag]:
    """Resolve, convert and time every mapped row into ``_aas_p_rows``; return flag columns."""
    market = workspace.market
    joins = "".join(
        f" LEFT JOIN {calendars[index]} c{index} ON c{index}.session_date = "
        f"q._aas_t_{spec.time_rules[TIME_COLUMNS[index]].input}"
        for index in sorted(calendars)
    )
    sessions = "".join(
        f", c{index}.open_at_us AS _aas_c{index}_open, c{index}.close_at_us AS _aas_c{index}_close"
        for index in sorted(calendars)
    )
    base = f"SELECT q.*{sessions} FROM ({_resolved(workspace, spec)}) q{joins}"
    numeric = spec.mapper.numeric_columns(spec.mapper_args)
    layers: list[list[tuple[str, str]]] = []
    finals: list[tuple[str, str]] = []
    refused: list[str] = []
    values: dict[str, str] = {}
    flags: list[_Flag] = []
    for position, (column, found) in enumerate(sorted(spec.decimal_rules.items())):
        prefix = f"_aas_d{position}_"
        added, conversion = decimal_rules.conversion(
            found,
            _q(column),
            numeric[column],
            prefix,
            currency=_q("currency") if spec.domain == "prices" else None,
        )
        for index, layer in enumerate(added):
            while len(layers) <= index:
                layers.append([])
            layers[index].extend(layer)
        finals.append((f"{prefix}value", conversion.value))
        finals.append((f"{prefix}refused", f"coalesce({conversion.refused}, false)"))
        refused.append(f"{prefix}refused")
        values[column] = f"CASE WHEN {prefix}refused THEN NULL ELSE {prefix}value END"
        for flag, condition in conversion.flags:
            alias = f"{prefix}f_{flag}"
            finals.append((alias, f"coalesce({condition}, false)"))
            flags.append(_Flag(found.rule_id, found.version, flag, column, alias))
    _add_row_flags(spec, finals, flags)
    links = " ".join(
        f"WHEN {index} THEN {'NULL' if source.retrieved_at_us is None else source.retrieved_at_us}"
        for index, source in enumerate(sources)
    )
    finals.append(("_aas_ingest", f"coalesce(_aas_ingested_at_us, CASE _aas_pin {links} END)"))
    for index, column in enumerate(TIME_COLUMNS):
        rule = spec.time_rules[column]
        session = (f"_aas_c{index}_open", f"_aas_c{index}_close") if index in calendars else None
        source = None if rule.input is None else "_aas_t_" + rule.input
        computed = rule_sql(rule, source, session)
        finals.append((f"_aas_rv{index}", f"CAST({computed.value} AS BIGINT)"))
        finals.append((f"_aas_rb{index}", f"CAST({computed.base} AS BIGINT)"))
    converted = _layered(_layered(base, layers), [finals])
    selected = ["_aas_pin", "_aas_ordinal", "_aas_row_hash", "_aas_ingest"]
    if spec.mapper.identity(spec.mapper_args) is not None:
        selected.append("_aas_id_token AS _aas_token")
    for name, kind in DOMAINS[spec.domain]:
        expression = values.get(name, f"CAST({_q(name)} AS {kind.rstrip('?')})")
        selected.append(f"{expression} AS {_q(name)}")
    if fields:
        selected.append('"fields"')
    required = [
        name
        for name, kind in DOMAINS[spec.domain]
        if not kind.endswith("?") and name not in numeric and name != _resolved_column(spec)
    ]
    missing = " OR ".join(f"{_q(name)} IS NULL" for name in required) or "false"
    held = " OR ".join(
        f"(_aas_rv{index} IS NOT NULL AND _aas_rb{index} IS NOT NULL "
        f"AND _aas_ingest < _aas_rb{index})"
        for index in range(len(TIME_COLUMNS))
    )
    selected.append(
        # A row missing a required column is malformed whether or not it resolves.
        f"CASE WHEN {missing} THEN 'refused_required' "
        "WHEN _aas_matches = 0 THEN 'unresolved' WHEN _aas_matches > 1 THEN 'ambiguous' "
        f"WHEN {' OR '.join(refused) or 'false'} THEN 'refused_number' "
        "WHEN _aas_ingest IS NULL THEN 'refused_ingestion' "
        f"WHEN {held} THEN 'held' ELSE 'ok' END AS _aas_status"
    )
    for index in range(len(TIME_COLUMNS)):
        selected.extend(
            (
                (
                    f"CASE WHEN _aas_rv{index} IS NULL THEN NULL "
                    f"ELSE least(_aas_rv{index}, _aas_ingest) END AS _aas_tv{index}"
                ),
                f"coalesce(_aas_rv{index} > _aas_ingest, false) AS _aas_tc{index}",
                f"_aas_rv{index} IS NULL AS _aas_tn{index}",
                f"coalesce(_aas_ingest < _aas_rb{index}, false) AS _aas_th{index}",
            )
        )
    selected.extend(flag.condition for flag in flags)
    record, plain = record_identity_sql(spec.domain)
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('rows')} AS SELECT *, "
        f"CASE WHEN _aas_status IN ('ok', 'held') AND ({plain}) THEN {record} END AS record_id "
        f"FROM (SELECT {', '.join(selected)} FROM ({converted}))"
    )
    keys = ", ".join(_q(name) for name in NATURAL_KEYS[spec.domain])
    _fix(
        market,
        f"SELECT _aas_pin, _aas_ordinal, {keys} FROM {_t('rows')} "
        "WHERE _aas_status IN ('ok', 'held') AND record_id IS NULL",
        lambda row: record_identity(spec.domain, row[2:]),
        _t("rows"),
        "record_id",
    )
    return flags


def _comparison(domain: str, *, fields: bool, mapped_fields: bool) -> str:
    """Rows equal on every domain column, the only columns a head diff compares."""
    same = [f"r.{_q(name)} IS NOT DISTINCT FROM h.{_q(name)}" for name, _ in DOMAINS[domain]]
    if fields:
        mapped = "coalesce(r.\"fields\", 'ohlcv')" if mapped_fields else "'ohlcv'"
        same.append(f'{mapped} = h."fields"')
    return " AND ".join(same)


def _times(spec: PromotionSpec, op: str) -> list[str]:
    """Each time column of a non-TOMBSTONE row of ``op`` (an SQL op expression)."""
    times = []
    for index, column in enumerate(TIME_COLUMNS):
        if spec.time_rules[column].basis == "record":
            times.append(
                f"CASE WHEN {op} = 'SUPERSEDE' THEN CASE WHEN _aas_tn{index} THEN NULL "
                f"ELSE _aas_ingest END ELSE _aas_tv{index} END"
            )
        else:
            times.append(f"_aas_tv{index}")
    return times


def _diff(
    workspace: Workspace,
    spec: PromotionSpec,
    chain: list[str],
    *,
    fields: bool,
    mapped_fields: bool,
) -> None:
    market = workspace.market
    domain = spec.domain
    scope = "false"
    if spec.tombstone.mode == "absent_in_full_snapshot":
        scope = _scope_sql(workspace, spec, "p")
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('head')} AS SELECT * FROM ({_heads_sql(domain)}) p "
        f"WHERE p.record_id IN (SELECT record_id FROM {_t('rows')} WHERE record_id IS NOT NULL) "
        f"OR ({scope})",
        [chain],
    )
    op = (
        "CASE WHEN h.record_id IS NULL THEN 'ASSERT' WHEN h.op = 'TOMBSTONE' THEN 'SUPERSEDE' "
        f"WHEN {_comparison(domain, fields=fields, mapped_fields=mapped_fields)} THEN 'SKIP' "
        "ELSE 'SUPERSEDE' END"
    )
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('diff')} AS SELECT *, "
        + ", ".join(
            f"{time} AS _aas_t{index}" for index, time in enumerate(_times(spec, "_aas_op"))
        )
        + f" FROM (SELECT r.*, h.revision_id AS _aas_h_rev, h.available_at_us AS _aas_h_av, "
        f"h.revision_known_at_us AS _aas_h_kn, {op} AS _aas_op FROM {_t('rows')} r "
        f"LEFT JOIN {_t('head')} h ON h.record_id = r.record_id WHERE r._aas_status = 'ok')"
    )
    market.execute(f"ALTER TABLE {_t('diff')} ADD COLUMN _aas_stale BOOLEAN DEFAULT false")
    market.execute(
        f"UPDATE {_t('diff')} SET _aas_stale = true WHERE _aas_op = 'SUPERSEDE' AND "
        "(coalesce(_aas_t0 < _aas_h_av, false) OR coalesce(_aas_t1 < _aas_h_kn, false))"
    )


def _scope_day(domain: str, column: str, alias: str) -> str:
    """The DATE a tombstone scope tests: a DATE column, or the UTC day of a microsecond instant."""
    kind = dict(DOMAINS[domain])[column].removesuffix("?")
    qualified = f"{alias}.{_q(column)}"
    if kind == "DATE":
        return qualified
    if kind != "BIGINT":
        raise ValueError(f"domain column {column} is neither a DATE nor an instant")
    return f"(DATE '1970-01-01' + CAST(floor({qualified} / {_DAY_US}.0) AS INTEGER))"


def _scope_sql(workspace: Workspace, spec: PromotionSpec, alias: str) -> str:
    policy = spec.tombstone
    if policy.start is None or policy.end is None:
        raise ValueError("a tombstone scope needs its date interval")
    date_column = _scope_day(spec.domain, spec.mapper.date_column, alias)
    condition = (
        f"{date_column} >= DATE '{policy.start.isoformat()}' "
        f"AND {date_column} < DATE '{policy.end.isoformat()}'"
    )
    if policy.instruments is not None:
        market = workspace.market
        subject = _q(resolved_column(spec.domain))
        market.execute(f"CREATE OR REPLACE TEMP TABLE {_t('scope')} (instrument_id VARCHAR)")
        market.executemany(
            f"INSERT INTO {_t('scope')} VALUES (?)", [[item] for item in policy.instruments]
        )
        condition += f" AND {alias}.{subject} IN (SELECT instrument_id FROM {_t('scope')})"
    return condition


def _tombstones(
    workspace: Workspace, spec: PromotionSpec, sources: list[_Source], plan: PromotionPlan
) -> None:
    market = workspace.market
    policy = spec.tombstone
    if policy.mode != "absent_in_full_snapshot" or policy.source is None:
        market.execute(
            f"CREATE OR REPLACE TEMP TABLE {_t('tomb')} AS SELECT * FROM {_t('head')} LIMIT 0"
        )
        market.execute(f"ALTER TABLE {_t('tomb')} ADD COLUMN _aas_evidence BIGINT")
        market.execute(f"ALTER TABLE {_t('tomb')} ADD COLUMN _aas_stale BOOLEAN")
        return
    times = [source.retrieved_at_us for source in sources]
    evidence = None if any(time is None for time in times) else max(cast("list[int]", times))
    unknown = _count(
        market,
        f"SELECT count(*) FROM {_t('rows')} WHERE _aas_status IN ('unresolved', 'ambiguous')",
    )
    if unknown:
        plan.refusals.append(
            f"{unknown} rows are unresolved, so absence from the snapshot cannot be proven"
        )
    if evidence is None:
        plan.blocking.append("tombstone evidence needs every pinned source's sl: link")
    (full,) = [
        index
        for index, source in enumerate(sources)
        if (source.pin.source_id, source.pin.table)
        == (policy.source.source_id, policy.source.table)
    ]
    # Only the full snapshot proves absence; another pinned table's row inside its scope
    # contradicts the declared completeness, so the plan says so instead of picking one.
    contradicting = _count(
        market,
        f"SELECT count(*) FROM {_t('rows')} r WHERE r._aas_pin <> {full} "
        f"AND r._aas_status IN ('ok', 'held') AND ({_scope_sql(workspace, spec, 'r')})",
    )
    if contradicting:
        plan.refusals.append(
            f"{contradicting} rows of other pinned sources fall in the full snapshot's scope; "
            "promote them in a separate generation"
        )
    absent = (
        f"h.op <> 'TOMBSTONE' AND ({_scope_sql(workspace, spec, 'h')}) AND NOT EXISTS "
        f"(SELECT 1 FROM {_t('rows')} r WHERE r.record_id = h.record_id AND r._aas_pin = {full})"
    )
    stamp = "NULL" if evidence is None else str(evidence)
    stale = (
        " OR ".join(
            f"coalesce({stamp} < h.{column}, false)"
            for column in TIME_COLUMNS
            if spec.time_rules[column].kind is not UNKNOWN_NULL
        )
        or "false"
    )
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('tomb')} AS SELECT h.*, "
        f"CAST({stamp} AS BIGINT) AS _aas_evidence, ({stale}) AS _aas_stale "
        f"FROM {_t('head')} h WHERE {absent}"
    )


def _delta(  # noqa: PLR0913 -- one union spells both revision shapes column by column
    workspace: Workspace,
    spec: PromotionSpec,
    sources: list[_Source],
    flags: list[_Flag],
    *,
    stage_fields: bool,
    mapped_fields: bool,
) -> list[_Flag]:
    """Write ``_aas_p_delta`` and the typed ``_aas_p_stage``; return the time flag columns."""
    market = workspace.market
    domain = spec.domain
    snapshots = " ".join(
        f"WHEN {index} THEN {formats.sql_literal(_LINK + source.pin.source_id)}"
        for index, source in enumerate(sources)
    )
    names = [name for name, _ in DOMAINS[domain]]
    time_flags: list[_Flag] = []
    asserted = [
        "record_id",
        "_aas_op AS op",
        "CASE WHEN _aas_op = 'ASSERT' THEN NULL ELSE _aas_h_rev END AS supersedes",
    ]
    asserted += ["_aas_t0 AS available_at_us", "_aas_t1 AS revision_known_at_us"]
    asserted += [
        "_aas_ingest AS ingested_at_us",
        f"CASE _aas_pin {snapshots} END AS source_snapshot_id",
    ]
    asserted += ["_aas_row_hash AS source_row_hash", *(_q(name) for name in names)]
    if stage_fields:
        asserted.append(
            'coalesce("fields", \'ohlcv\') AS "fields"'
            if mapped_fields
            else "'ohlcv' AS \"fields\""
        )
    asserted += [f"{flag.condition} AS {flag.condition}" for flag in flags]
    for index, column in enumerate(TIME_COLUMNS):
        rule = spec.time_rules[column]
        from_rule = "_aas_op = 'ASSERT'" if rule.basis == "record" else "true"
        clamp = f"_aas_x_clamp{index}"
        asserted.append(f"({from_rule} AND _aas_tc{index}) AS {clamp}")
        time_flags.append(_Flag(rule.kind.rule_id, rule.kind.version, CLAMP_FLAG, column, clamp))
        if rule.kind.flag is not None:
            day = f"_aas_x_day{index}"
            asserted.append(f"({from_rule} AND NOT _aas_tn{index}) AS {day}")
            time_flags.append(
                _Flag(rule.kind.rule_id, rule.kind.version, rule.kind.flag, column, day)
            )
    asserted.append("_aas_pin")
    policy = spec.tombstone
    tombstone_hash = "NULL"
    tombstone_snapshot = "NULL"
    if policy.source is not None:
        tombstone_hash = formats.sql_literal(
            formats.tombstone_hash(
                policy.source.source_id, policy.source.table, policy.source.table_digest
            )
        )
        tombstone_snapshot = formats.sql_literal(_LINK + policy.source.source_id)
    removed = ["record_id", "'TOMBSTONE' AS op", "revision_id AS supersedes"]
    for column in TIME_COLUMNS:
        value = "NULL" if spec.time_rules[column].kind is UNKNOWN_NULL else "_aas_evidence"
        removed.append(f"CAST({value} AS BIGINT) AS {column}")
    removed += ["_aas_evidence AS ingested_at_us", f"{tombstone_snapshot} AS source_snapshot_id"]
    removed += [f"{tombstone_hash} AS source_row_hash", *(_q(name) for name in names)]
    if stage_fields:
        removed.append('"fields"' if _has_fields(market) else "'ohlcv' AS \"fields\"")
    removed += [f"false AS {flag.condition}" for flag in flags]
    removed += [f"false AS {flag.condition}" for flag in time_flags]
    removed.append("-1 AS _aas_pin")
    revision = formats.revision_id_sql(
        spec.dataset_id, "record_id", "op", "supersedes", "source_row_hash"
    )
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('delta')} AS SELECT *, {revision} AS revision_id FROM ("
        f"SELECT {', '.join(asserted)} FROM {_t('diff')} "
        "WHERE _aas_op IN ('ASSERT', 'SUPERSEDE') AND NOT _aas_stale "
        f"UNION ALL BY NAME SELECT {', '.join(removed)} FROM {_t('tomb')} WHERE NOT _aas_stale)"
    )
    kinds = dict(DOMAINS[domain])
    typed = [
        "CAST(record_id AS VARCHAR) AS record_id",
        "CAST(revision_id AS VARCHAR) AS revision_id",
        "CAST(supersedes AS VARCHAR) AS supersedes_revision_id",
        "CAST(op AS VARCHAR) AS op",
        "CAST(available_at_us AS BIGINT) AS available_at_us",
        "CAST(revision_known_at_us AS BIGINT) AS revision_known_at_us",
        "CAST(ingested_at_us AS BIGINT) AS ingested_at_us",
        "CAST(source_snapshot_id AS VARCHAR) AS source_snapshot_id",
        "CAST(source_row_hash AS VARCHAR) AS source_row_hash",
        *(f"CAST({_q(name)} AS {kinds[name].rstrip('?')}) AS {_q(name)}" for name in names),
    ]
    if stage_fields:
        typed.append('CAST("fields" AS VARCHAR) AS "fields"')
    market.execute(
        f"CREATE OR REPLACE TEMP TABLE {_t('stage')} AS "
        f"SELECT {', '.join(typed)} FROM {_t('delta')}"
    )
    return time_flags


def _has_fields(market: duckdb.DuckDBPyConnection) -> bool:
    return "fields" in _columns(market, _t("head"))


def _flags(
    workspace: Workspace,
    spec: PromotionSpec,
    flags: list[_Flag],
    budget: ComputeBudget,
    refusals: list[str],
) -> dict[str, int]:
    """Write ``_aas_p_flags``, one row per (revision, rule, flag) with the columns it names.

    A cross-provider rule compares a delta row with the reference head of the same
    instrument, session, interval, bar end, basis and currency (the price key without the
    role, which differs between a canonical and a reference dataset), so an adjusted
    reference never judges an unadjusted value and a row matches at most one reference.
    A flag key repeated within the delta is refused before anything is prepared.
    """
    market = workspace.market
    groups: dict[tuple[str, str, str], list[_Flag]] = {}
    for flag in flags:
        groups.setdefault((flag.rule_id, flag.version, flag.flag), []).append(flag)
    selects = []
    for (rule_id, version, name), members in sorted(groups.items()):
        ordered = sorted(members, key=lambda item: item.detail)
        # A row flag names no column; its detail is NULL.
        detail = (
            "CAST(NULL AS VARCHAR)"
            if all(not item.detail for item in ordered)
            else "concat_ws(',', "
            + ", ".join(
                f"CASE WHEN {item.condition} THEN {formats.sql_literal(item.detail)} END"
                for item in ordered
            )
            + ")"
        )
        anyone = " OR ".join(item.condition for item in ordered)
        selects.append(
            f"SELECT record_id, revision_id, {formats.sql_literal(rule_id)} AS rule_id, "
            f"{formats.sql_literal(version)} AS rule_version, {formats.sql_literal(name)} AS flag, "
            f"{detail} AS detail FROM {_t('delta')} WHERE {anyone}"
        )
    for rule in spec.quality_rules:
        chain = _verify_pin(workspace, rule.reference, budget)
        column = _q(rule.column)
        keys = ", ".join(_q(name) for name in _COMPARED_KEYS)
        market.execute(
            f"CREATE OR REPLACE TEMP TABLE {_t('ref')} AS SELECT {keys}, {column} AS v "
            f"FROM ({_heads_sql('prices')}) WHERE op <> 'TOMBSTONE' AND value_state = 'present'",
            [chain],
        )
        tolerance = f"CAST('{rule.tolerance}' AS DECIMAL(38,12))"
        same = " AND ".join(
            f"r.{_q(name)} IS NOT DISTINCT FROM d.{_q(name)}" for name in _COMPARED_KEYS
        )
        selects.append(
            f"SELECT d.record_id, d.revision_id, {formats.sql_literal(rule.rule_id)} AS rule_id, "
            f"{formats.sql_literal(rule.version)} AS rule_version, "
            "'cross_provider_mismatch' AS flag, "
            f"{formats.sql_literal(rule.column)} AS detail FROM {_t('delta')} d "
            f"WHERE d.op <> 'TOMBSTONE' AND d.{column} IS NOT NULL AND EXISTS (SELECT 1 FROM "
            f"{_t('ref')} r WHERE {same} AND abs(d.{column} - r.v) > {tolerance} * abs(r.v))"
        )
    if selects:
        market.execute(
            f"CREATE OR REPLACE TEMP TABLE {_t('flags')} AS " + " UNION ALL ".join(selects)
        )
    else:
        market.execute(
            f"CREATE OR REPLACE TEMP TABLE {_t('flags')} (record_id VARCHAR, revision_id VARCHAR, "
            "rule_id VARCHAR, rule_version VARCHAR, flag VARCHAR, detail VARCHAR)"
        )
    duplicated = _count(
        market,
        "SELECT count(*) FROM (SELECT 1 FROM "
        f"{_t('flags')} GROUP BY record_id, revision_id, rule_id, rule_version, flag "
        "HAVING count(*) > 1)",
    )
    if duplicated:
        refusals.append(f"{duplicated} quality flag keys repeat within the delta")
    return {
        str(name): int(count)
        for name, count in market.execute(
            f"SELECT flag, count(*) FROM {_t('flags')} GROUP BY flag ORDER BY flag"
        ).fetchall()
    }


def flags_digest(
    market: duckdb.DuckDBPyConnection,
    relation: str,
    parameters: list[object],
    budget: ComputeBudget,
) -> tuple[str, int]:
    """The ``aas-rowset-v1`` digest and count of quality flag rows (generation excluded)."""
    count = _count(market, f"SELECT count(*) FROM ({relation})", parameters)
    batch = max(1, min(65_536, budget.available_bytes // _FLAG_ROW_BYTES))
    cells = [encoded_cell_sql(name, kind) for name, kind in FLAG_SCHEMA]
    columns = ", ".join(_q(name) for name, _ in FLAG_SCHEMA)
    digest = stream_rowset(
        market,
        FLAG_SCHEMA,
        cells,
        f"SELECT {columns} FROM ({relation})",
        parameters,
        count=count,
        batch_rows=batch,
    )
    return digest, count


def _partition_check(
    workspace: Workspace, spec: PromotionSpec, chain: list[str], flags: list[_Flag]
) -> dict[str, object] | None:
    """``partition_row_count@1``: a partial response's row counts against complete dates.

    For each session date of rows the mapper flags ``provider_reported_partial``, the
    check counts the source rows and the resolved ones (status ``ok`` or ``held``). Its
    reference comes only from the parent chain's complete dates: dates whose live heads
    carry no ``provider_reported_partial`` flag. The latest complete date on or before
    the session anchors a 31-day window, and the reference is the largest live-head
    count on a complete date in that window, so an earlier partial day or one short
    complete day does not lower it. The reference date and the generation that last
    wrote a head on it are recorded. The result is ``below_reference`` when a date
    resolves fewer rows than its reference, ``no_reference`` when no date has one, and
    ``at_least_reference`` otherwise. It is recorded, never a refusal: the rows are
    promoted with their flag.
    """
    partial = [flag for flag in flags if flag.flag == PARTIAL_FLAG and not flag.detail]
    if not partial:
        return None
    market = workspace.market
    day = _q(spec.mapper.date_column)
    found = market.execute(
        f"SELECT {day}, count(*), count(*) FILTER (WHERE _aas_status IN ('ok', 'held')) "
        f"FROM {_t('rows')} WHERE ({' OR '.join(flag.condition for flag in partial)}) "
        f"AND {day} IS NOT NULL GROUP BY 1 ORDER BY 1"
    ).fetchall()
    if not found:
        return None
    complete: list[tuple[date, int, str]] = []
    if chain:
        complete = [
            (cast("date", row[0]), int(row[1]), str(row[2]))
            for row in market.execute(
                f"SELECT {day}, count(*), arg_max(generation_id, sequence) FROM ("
                f"SELECT p.record_id, p.revision_id, p.generation_id, p.op, p.{day}, g.sequence, "
                "row_number() OVER (PARTITION BY p.record_id ORDER BY g.sequence DESC) "
                f"AS _aas_rank FROM {_q(spec.domain)} p JOIN market_generations g "
                "ON g.generation_id = p.generation_id "
                "WHERE p.generation_id IN (SELECT unnest(?::VARCHAR[])) "
                f"AND p.{day} >= ?::DATE - INTERVAL {_REFERENCE_LOOKBACK_DAYS} DAY "
                f"AND p.{day} <= ?::DATE) h "
                "WHERE _aas_rank = 1 AND op <> 'TOMBSTONE' GROUP BY 1 "
                "HAVING NOT bool_or(EXISTS (SELECT 1 FROM quality_flags f "
                "WHERE f.generation_id = h.generation_id AND f.record_id = h.record_id "
                "AND f.revision_id = h.revision_id AND f.flag = ?)) ORDER BY 1",
                [chain, found[0][0], found[-1][0], PARTIAL_FLAG],
            ).fetchall()
        ]
    dates = []
    for row in found:
        session, rows, resolved = cast("date", row[0]), row[1], row[2]
        anchor = next((item[0] for item in reversed(complete) if item[0] <= session), None)
        window = (
            []
            if anchor is None
            else [
                item
                for item in complete
                if item[0] <= anchor and (anchor - item[0]).days <= _REFERENCE_WINDOW_DAYS
            ]
        )
        reference = max(window, key=lambda item: (item[1], item[0])) if window else None
        dates.append(
            {
                "session_date": str(session),
                "rows": int(rows),
                "resolved": int(resolved),
                "reference_date": None if reference is None else str(reference[0]),
                "reference_rows": None if reference is None else reference[1],
                "reference_generation": None if reference is None else reference[2],
            }
        )
    compared = [item for item in dates if item["reference_rows"] is not None]
    result = (
        "no_reference"
        if not compared
        else "below_reference"
        if any(
            cast("int", item["resolved"]) < cast("int", item["reference_rows"]) for item in compared
        )
        else "at_least_reference"
    )
    return {"rule": "@".join(PARTITION_CHECK), "result": result, "dates": dates}


# --- planning ------------------------------------------------------------------------------


def dataset_head(workspace: Workspace, dataset_id: str) -> str | None:
    """The dataset's current head, refusing a market head the catalog has not recorded."""
    market = workspace.market.execute(
        "SELECT generation_id FROM market_generations WHERE dataset_id=? "
        "ORDER BY sequence DESC LIMIT 1",
        [dataset_id],
    ).fetchone()
    catalog = workspace.state.execute(
        "SELECT generation_id FROM dataset_versions WHERE dataset_id=? AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (dataset_id,),
    ).fetchone()
    head = None if market is None else str(market[0])
    if head != (None if catalog is None else str(catalog[0])):
        raise ValueError(f"dataset {dataset_id} awaits catalog recovery; run aas db recover")
    return head


def generation_spec(workspace: Workspace, generation_id: str) -> PromotionSpec:
    """The retained spec of one promoted generation, refusing any other generation."""
    marker = marker_for(workspace.market, generation_id)
    operation = get_operation(workspace.state, str(marker["operation_id"]))
    if operation is None or operation["kind"] != OPERATION_KIND:
        raise ValueError("a promotion extends only a chain of promoted generations")
    catalog = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?", (generation_id,)
    ).fetchone()
    if catalog is None:
        raise ValueError(f"generation {generation_id} is not cataloged")
    return parse_spec(_read_raw(workspace, str(catalog[0])), str(catalog[0]))


def _descends(workspace: Workspace, before: GenerationPin, after: GenerationPin) -> bool:
    """Whether ``after`` is ``before`` or a later generation of the same dataset chain."""
    if before == after:
        return True
    chain = [
        str(item["generation_id"])
        for item in generation_chain(workspace.market, after.generation_id)
    ]
    return after.dataset_id == before.dataset_id and before.generation_id in chain


def _check_evidence_pins(workspace: Workspace, parent: PromotionSpec, spec: PromotionSpec) -> None:
    """Hold each calendar and mapper reference pin at the parent's generation or later.

    These pins are evidence the rules and the mapper read, not the rule: a chain may move
    one to a descendant generation of the same dataset, never back or across datasets.
    """
    for column in TIME_COLUMNS:
        before = parent.time_rules[column].calendar
        after = spec.time_rules[column].calendar
        if before is not None and after is not None and not _descends(workspace, before, after):
            raise ValueError(
                f"the {column} calendar pin must be the parent's calendar generation "
                "or a descendant of it"
            )
    earlier = references(parent.mapper, parent.mapper_args)
    for name, reference in references(spec.mapper, spec.mapper_args).items():
        found = earlier.get(name)
        if found is not None and not _descends(workspace, found.pin, reference.pin):
            raise ValueError(
                f"the mapper's {name} pin must be the parent's {name} generation "
                "or a descendant of it"
            )


def _check_parent(workspace: Workspace, spec: PromotionSpec) -> int:
    """Hold the parent CAS and a parent chain's time rules; return the new sequence."""
    head = dataset_head(workspace, spec.dataset_id)
    if head != spec.parent:
        raise ParentChangedError(
            f"dataset {spec.dataset_id} head is {head}, not the spec parent {spec.parent}; "
            "promote again with the current head as parent"
        )
    if spec.parent is None:
        return 1
    marker = marker_for(workspace.market, spec.parent)
    parent = generation_spec(workspace, spec.parent)
    if parent.domain != spec.domain or parent.dataset_id != spec.dataset_id:
        raise ValueError("the parent generation belongs to another dataset")
    if parent.time_rule_identities() != spec.time_rule_identities():
        raise ValueError(
            "time rules differ from the parent chain's; a new rule generation is a new dataset "
            "(append .r<N> to the dataset ID) promoted from its first generation"
        )
    _check_evidence_pins(workspace, parent, spec)
    return int(cast("int", marker["sequence"])) + 1


def _new_plan(spec: PromotionSpec, sequence: int) -> PromotionPlan:
    digests = [pin.table_digest for pin in spec.sources]
    request = formats.request_document(spec.sha256, digests, spec.parent)
    request_hash = hashlib.sha256(request).hexdigest()
    generation_id, operation_id = generation_identity(request_hash)
    return PromotionPlan(
        spec=spec,
        request_hash=request_hash,
        request=request,
        generation_id=generation_id,
        operation_id=operation_id,
        version=str(sequence),
        sequence=sequence,
    )


def _status_counts(market: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        str(status): int(count)
        for status, count in market.execute(
            f"SELECT _aas_status, count(*) FROM {_t('rows')} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }


def _report(
    workspace: Workspace, spec: PromotionSpec, plan: PromotionPlan, flags: list[_Flag]
) -> None:
    market = workspace.market
    statuses = _status_counts(market)
    plan.report["source_rows"] = _count(market, f"SELECT count(*) FROM {_t('src')}")
    # A mapper that reads one series of a shared table leaves the other rows unselected.
    plan.report["unselected_rows"] = int(plan.report["source_rows"]) - sum(statuses.values())
    plan.report["rows"] = statuses
    for status, count in statuses.items():
        if status.startswith("refused") and count:
            plan.refusals.append(f"{count} rows {status.removeprefix('refused_')} refused")
    if spec.mapper.identity(spec.mapper_args) is not None:
        plan.report["unresolved_tokens"] = [
            str(row[0])
            for row in market.execute(
                f"SELECT DISTINCT _aas_token FROM {_t('rows')} WHERE _aas_status IN "
                f"('unresolved', 'ambiguous') ORDER BY 1 LIMIT {_SAMPLE}"
            ).fetchall()
        ]
        plan.report["unresolved_token_count"] = _count(
            market,
            f"SELECT count(DISTINCT _aas_token) FROM {_t('rows')} "
            "WHERE _aas_status IN ('unresolved', 'ambiguous')",
        )
    mapped: dict[str, int] = {}
    for flag in flags:
        total = _count(market, f"SELECT count(*) FROM {_t('rows')} WHERE {flag.condition}")
        mapped[f"{flag.flag}:{flag.detail}"] = total
    mapped_any = {
        name: _count(
            market,
            f"SELECT count(*) FROM {_t('rows')} WHERE "
            + " OR ".join(flag.condition for flag in flags if flag.flag == name),
        )
        for name in sorted({flag.flag for flag in flags})
    }
    plan.report["mapped_flags"] = {"by_column": mapped, "rows": mapped_any}
    plan.report["time_rules"] = {
        column: {
            "rule": spec.time_rules[column].kind.name,
            "null": _count(market, f"SELECT count(*) FROM {_t('rows')} WHERE _aas_tn{index}"),
            "clamped": _count(
                market,
                f"SELECT count(*) FROM {_t('rows')} WHERE _aas_tc{index} AND _aas_status = 'ok'",
            ),
            "mapped_clamped": _count(
                market, f"SELECT count(*) FROM {_t('rows')} WHERE _aas_tc{index}"
            ),
            "mapped_held": _count(
                market, f"SELECT count(*) FROM {_t('rows')} WHERE _aas_th{index}"
            ),
        }
        for index, column in enumerate(TIME_COLUMNS)
    }
    duplicates = market.execute(
        f"SELECT count(*), coalesce(sum(n), 0) FROM (SELECT record_id, count(*) AS n FROM "
        f"{_t('rows')} WHERE _aas_status IN ('ok', 'held') GROUP BY record_id HAVING count(*) > 1)"
    ).fetchone()
    if duplicates is not None and duplicates[0]:
        plan.refusals.append(
            f"{duplicates[0]} natural keys repeat across {duplicates[1]} source rows"
        )
    plan.report["duplicate_keys"] = 0 if duplicates is None else int(duplicates[0])


def plan_promotion(
    workspace: Workspace, spec: PromotionSpec, *, budget: ComputeBudget | None = None
) -> PromotionPlan:
    """Compute everything a promotion writes, writing nothing; temp tables stay for apply."""
    budget = budget or _DEFAULT_BUDGET
    market = workspace.market
    limit_duckdb(market, budget)
    sequence = _check_parent(workspace, spec)
    plan = _new_plan(spec, sequence)
    version = market_version(market)
    if version < DOMAIN_VERSIONS[spec.domain]:
        plan.blocking.append(
            f"the {spec.domain} domain needs core schema v{DOMAIN_VERSIONS[spec.domain]}; "
            f"run aas db migrate --to {DOMAIN_VERSIONS[spec.domain]}"
        )
    if version < 2:  # noqa: PLR2004 -- quality_flags arrive in v2
        plan.blocking.append("quality flags need core schema v2; run aas db migrate --to 2")
    sources = _sources(workspace, spec, plan)
    _identity(workspace, spec, plan, budget)
    calendars = _calendars(workspace, spec, budget)
    _references(workspace, spec, budget)
    decimal_rules.install(market)
    _stage_sources(workspace, spec, sources, plan)
    _stage_items(workspace, spec, sources, plan)
    fields = _map(workspace, spec)
    flags = _rows(workspace, spec, sources, calendars, fields=fields)
    _report(workspace, spec, plan, flags)
    chain = (
        [str(item["generation_id"]) for item in generation_chain(market, spec.parent)]
        if spec.parent
        else []
    )
    stage_fields = spec.domain == "prices" and version >= 2  # noqa: PLR2004 -- fields arrive in v2
    _diff(workspace, spec, chain, fields=stage_fields, mapped_fields=fields)
    check = _partition_check(workspace, spec, chain, flags)
    if check is not None:
        plan.report["partition_row_count"] = check
    _tombstones(workspace, spec, sources, plan)
    time_flags = _delta(
        workspace, spec, sources, flags, stage_fields=stage_fields, mapped_fields=fields
    )
    flag_counts = _flags(workspace, spec, flags + time_flags, budget, plan.refusals)
    operations = {
        str(op): int(count)
        for op, count in market.execute(
            f"SELECT op, count(*) FROM {_t('delta')} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }
    plan.report["operations"] = operations
    plan.report["unchanged"] = _count(
        market, f"SELECT count(*) FROM {_t('diff')} WHERE _aas_op = 'SKIP'"
    )
    plan.report["time_drift"] = _count(
        market,
        f"SELECT count(*) FROM {_t('diff')} WHERE _aas_op = 'SKIP' AND "
        "(_aas_t0 IS DISTINCT FROM _aas_h_av OR _aas_t1 IS DISTINCT FROM _aas_h_kn)",
    )
    plan.report["stale"] = _count(
        market, f"SELECT count(*) FROM {_t('diff')} WHERE _aas_stale"
    ) + _count(market, f"SELECT count(*) FROM {_t('tomb')} WHERE _aas_stale")
    plan.report["flags"] = flag_counts
    plan.delta_rows = sum(operations.values())
    through = market.execute(f"SELECT max(ingested_at_us) FROM {_t('stage')}").fetchone()
    plan.ingested_through_us = None if through is None or through[0] is None else int(through[0])
    if plan.delta_rows and not plan.refusals:
        plan.flags_digest, plan.flag_rows = flags_digest(
            market, f"SELECT * FROM {_t('flags')}", [], budget
        )
        plan.bulk = plan_generation_bulk(market, _bulk_request(plan), budget=budget)
        plan.manifest = _manifest(plan)
    return plan


def _bulk_request(plan: PromotionPlan) -> BulkRequest:
    return BulkRequest(
        dataset_id=plan.spec.dataset_id,
        version=plan.version,
        generation_id=plan.generation_id,
        operation_id=plan.operation_id,
        request_hash=plan.request_hash,
        parent_id=plan.spec.parent,
        domain=plan.spec.domain,
        staged=_t("stage"),
    )


def _manifest(plan: PromotionPlan) -> bytes:
    """``aas-promotion-manifest-v1``: the generation's marker, counts and flag digest."""
    if plan.bulk is None:
        raise ValueError("an empty delta has no manifest")
    marker = plan.bulk.marker
    spec = plan.spec
    check = plan.report.get("partition_row_count")
    return formats.canonical(
        ({} if check is None else {"partition_row_count": check})
        | {
            "schema": MANIFEST_SCHEMA,
            "request_hash": plan.request_hash,
            "spec_sha256": spec.sha256,
            "dataset_id": spec.dataset_id,
            "domain": spec.domain,
            "version": plan.version,
            "generation_id": plan.generation_id,
            "operation_id": plan.operation_id,
            "parent": spec.parent,
            "sequence": plan.sequence,
            "delta_hash": marker["delta_hash"],
            "chain_hash": marker["chain_hash"],
            "row_count": marker["row_count"],
            "operations": plan.report.get("operations"),
            "flags": {"rowset": plan.flags_digest, "rows": plan.flag_rows},
            "rows": plan.report.get("rows"),
            "unchanged": plan.report.get("unchanged"),
            "stale": plan.report.get("stale"),
            "ingested_through_us": plan.ingested_through_us,
            "sources": [
                {"source_id": pin.source_id, "table": pin.table, "digest": pin.table_digest}
                for pin in spec.sources
            ],
            "identity_snapshot": None
            if spec.identity_snapshot is None
            else {
                "snapshot_id": spec.identity_snapshot.snapshot_id,
                "content_hash": spec.identity_snapshot.content_hash,
            },
        }
    )


def _drop(market: duckdb.DuckDBPyConnection) -> None:
    for name in _TEMP:
        market.execute(f"DROP TABLE IF EXISTS temp.{_t(name)}")
    for (name,) in market.execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary AND starts_with(table_name, ?)",
        [reference_table("")],
    ).fetchall():
        market.execute(f"DROP TABLE IF EXISTS temp.{_q(str(name))}")


# --- apply, recovery and verification ------------------------------------------------------


def promote(
    workspace: Workspace,
    raw: bytes,
    sha256: str,
    *,
    apply: bool,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Plan, or apply, one promotion spec; the same request returns its existing generation."""
    budget = budget or _DEFAULT_BUDGET
    spec = parse_spec(raw, sha256)
    request_hash = formats.request_hash(
        spec.sha256, [pin.table_digest for pin in spec.sources], spec.parent
    )
    generation_id, operation_id = generation_identity(request_hash)
    operation = get_operation(workspace.state, operation_id)
    if operation is not None:
        if operation["phase"] == "PREPARED" and not apply:
            return _pending_plan(workspace, spec, operation, generation_id, budget)
        return _existing(workspace, operation, generation_id, apply=apply, budget=budget)
    if apply:
        pending = workspace.state.execute(
            "SELECT operation_id FROM storage_operations WHERE kind=? AND phase='PREPARED' "
            "AND operation_id<>?",
            (OPERATION_KIND, operation_id),
        ).fetchall()
        if pending:
            raise ValueError("finish or quarantine the pending promotion before another")
    try:
        plan = plan_promotion(workspace, spec, budget=budget)
        result = {"mode": "apply" if apply else "plan", "reused": False, **plan.summary()}
        if not apply:
            return {**result, "published": False}
        if plan.blocking or plan.refusals:
            raise ValueError("promotion refused: " + "; ".join([*plan.blocking, *plan.refusals]))
        if not plan.delta_rows:
            return {**result, "published": False, "empty_delta": True}
        marker = _publish(workspace, plan, budget)
        return {**result, "published": True, "marker": marker}
    finally:
        _drop(workspace.market)


def _pending_plan(
    workspace: Workspace,
    spec: PromotionSpec,
    operation: Mapping[str, object],
    generation_id: str,
    budget: ComputeBudget,
) -> dict[str, object]:
    """Plan a request whose intent is pending, writing nothing and recovering nothing.

    Before the market commit the report is recomputed and says whether it still yields
    the intent's manifest; after it, recovery only has the catalog left to write.
    """
    pending: dict[str, object] = {
        "mode": "plan",
        "reused": False,
        "pending": True,
        "operation_id": operation["operation_id"],
        "generation_id": generation_id,
        "request_hash": operation["request_hash"],
        "published": False,
    }
    committed = workspace.market.execute(
        "SELECT 1 FROM market_generations WHERE generation_id=?", [generation_id]
    ).fetchone()
    if committed is not None:
        return {**pending, "market_committed": True}
    try:
        plan = plan_promotion(workspace, spec, budget=budget)
        manifest = plan.manifest
        recomputes = (
            manifest is not None
            and hashlib.sha256(manifest).hexdigest() == operation["payload_hash"]
        )
        return {
            **plan.summary(),
            **pending,
            "market_committed": False,
            "recomputes_intent": recomputes,
        }
    finally:
        _drop(workspace.market)


def _existing(
    workspace: Workspace,
    operation: Mapping[str, object],
    generation_id: str,
    *,
    apply: bool,
    budget: ComputeBudget,
) -> dict[str, object]:
    """The answer for a request that already has an intent: reuse, resume or refuse."""
    if operation["phase"] == "QUARANTINED":
        raise ValueError("this promotion request is quarantined")
    if operation["phase"] == "PREPARED" and not recover_promotion(
        workspace, operation, budget=budget
    ):
        raise ValueError("the pending promotion no longer recomputes its intent; quarantine it")
    result = verify_promotion(workspace, generation_id, budget=budget)
    return {"mode": "apply" if apply else "plan", "reused": True, **result}


def _publish(workspace: Workspace, plan: PromotionPlan, budget: ComputeBudget) -> dict[str, object]:
    if plan.manifest is None or plan.bulk is None:
        raise ValueError("promotion has nothing to publish")
    for payload in (plan.spec.raw, plan.request, plan.manifest):
        put_raw(workspace.paths.raw, payload)
    manifest_sha = hashlib.sha256(plan.manifest).hexdigest()
    operation = prepare_operation(
        workspace.state,
        operation_id=plan.operation_id,
        kind=OPERATION_KIND,
        request_hash=plan.request_hash,
        target_id=plan.generation_id,
        expected_parent=plan.spec.parent,
        payload_hash=manifest_sha,
    )
    generation_id = plan.generation_id

    def companion(connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(
            "INSERT INTO quality_flags SELECT ?, record_id, revision_id, rule_id, rule_version, "
            f"flag, detail FROM {_t('flags')}",
            [generation_id],
        )

    marker = publish_generation_bulk(
        workspace.market, _bulk_request(plan), budget=budget, plan=plan.bulk, companion=companion
    )
    _complete(workspace, operation, plan.spec, json.loads(plan.manifest), budget)
    return marker


def _check_flags(
    workspace: Workspace,
    domain: str,
    generation_id: str,
    manifest: Mapping[str, object],
    budget: ComputeBudget,
) -> None:
    """The stored flags equal the manifest's digest and name revisions of this generation."""
    flags = cast("dict[str, object]", manifest["flags"])
    if market_version(workspace.market) < 2:  # noqa: PLR2004 -- quality_flags arrive in v2
        raise ValueError("a promoted generation needs the v2 quality_flags table")
    orphans = _count(
        workspace.market,
        f"SELECT count(*) FROM quality_flags f WHERE f.generation_id = ? AND NOT EXISTS "
        f"(SELECT 1 FROM {_q(domain)} d WHERE d.generation_id = f.generation_id "
        "AND d.record_id = f.record_id AND d.revision_id = f.revision_id)",
        [generation_id],
    )
    digest, rows = flags_digest(
        workspace.market,
        "SELECT * FROM quality_flags WHERE generation_id = ?",
        [generation_id],
        budget,
    )
    if orphans or digest != flags["rowset"] or rows != flags["rows"]:
        raise ValueError("quality flags differ from the promotion manifest")


def _manifest_matches(marker: Mapping[str, object], manifest: Mapping[str, object]) -> None:
    for key, name in (
        ("generation_id", "generation_id"),
        ("dataset_id", "dataset_id"),
        ("version", "version"),
        ("parent_id", "parent"),
        ("sequence", "sequence"),
        ("domain", "domain"),
        ("delta_hash", "delta_hash"),
        ("chain_hash", "chain_hash"),
        ("row_count", "row_count"),
        ("operation_id", "operation_id"),
        ("request_hash", "request_hash"),
    ):
        if marker[key] != manifest[name]:
            raise ValueError(f"generation marker {key} differs from the promotion manifest")


def _complete(
    workspace: Workspace,
    operation: Mapping[str, object],
    spec: PromotionSpec,
    manifest: Mapping[str, object],
    budget: ComputeBudget,
) -> None:
    """Verify the committed generation against its manifest, then write the state catalog."""
    generation_id = str(operation["target_id"])
    marker = verify_generation_bulk(workspace.market, generation_id, budget=budget)
    _manifest_matches(marker, manifest)
    _check_flags(workspace, spec.domain, generation_id, manifest, budget)
    state = workspace.state
    created = int(cast("int", operation["created_at_us"]))
    report = {key: manifest[key] for key in ("operations", "rows", "unchanged", "stale", "flags")}
    with atomic(state):
        existing = state.execute(
            "SELECT domain, record_schema FROM datasets WHERE dataset_id=?", (spec.dataset_id,)
        ).fetchone()
        if existing is None:
            state.execute(
                "INSERT INTO datasets(dataset_id, domain, record_schema, owner) VALUES (?,?,?,?)",
                (spec.dataset_id, spec.domain, RECORD_SCHEMA, DATASET_OWNER),
            )
        elif tuple(existing) != (spec.domain, RECORD_SCHEMA):
            raise ValueError("dataset is registered for another domain")
        if (
            state.execute(
                "SELECT 1 FROM dataset_versions WHERE generation_id=?", (generation_id,)
            ).fetchone()
            is None
        ):
            partition = (
                None
                if spec.partition is None
                else {
                    "from": spec.partition.start.isoformat(),
                    "to": spec.partition.end.isoformat(),
                }
            )
            state.execute(
                "INSERT INTO dataset_versions(dataset_id, version, generation_id, "
                "parent_generation_id, sequence, chain_hash, manifest_hash, record_schema, "
                "normalizer_version, transform_hash, identity_snapshot_hash, "
                "authority_policy_hash, row_count, coverage, status) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?,?,'committed')",
                (
                    spec.dataset_id,
                    marker["version"],
                    generation_id,
                    marker["parent_id"],
                    marker["sequence"],
                    marker["chain_hash"],
                    marker["request_hash"],
                    RECORD_SCHEMA,
                    spec.mapper_name,
                    spec.sha256,
                    None if spec.identity_snapshot is None else spec.identity_snapshot.content_hash,
                    marker["row_count"],
                    formats.canonical({"partition": partition}).decode(),
                ),
            )
            for pin in spec.sources:
                state.execute(
                    "INSERT OR IGNORE INTO dataset_sources(dataset_id, version, "
                    "source_snapshot_id) "
                    "VALUES (?,?,?)",
                    (spec.dataset_id, marker["version"], _LINK + pin.source_id),
                )
            state.execute(
                "INSERT INTO quality_checks(check_id, dataset_id, version, rule_id, rule_version, "
                "result, reason, checked_at_us) VALUES (?,?,?,?,?,?,?,?)",
                (
                    "qc-" + hashlib.sha256(f"{generation_id}/promotion".encode()).hexdigest(),
                    spec.dataset_id,
                    marker["version"],
                    "promotion_report",
                    "1",
                    "recorded",
                    formats.canonical(report).decode(),
                    created,
                ),
            )
            check = manifest.get("partition_row_count")
            if isinstance(check, dict):
                rule_id, rule_version = PARTITION_CHECK
                state.execute(
                    "INSERT INTO quality_checks(check_id, dataset_id, version, rule_id, "
                    "rule_version, result, reason, checked_at_us) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        "qc-" + hashlib.sha256(f"{generation_id}/{rule_id}".encode()).hexdigest(),
                        spec.dataset_id,
                        marker["version"],
                        rule_id,
                        rule_version,
                        str(check["result"]),
                        formats.canonical(check["dates"]).decode(),
                        created,
                    ),
                )
            through = manifest["ingested_through_us"]
            if through is not None:
                partition_id = (
                    "all"
                    if spec.partition is None
                    else f"{spec.partition.start.isoformat()}/{spec.partition.end.isoformat()}"
                )
                state.execute(
                    "INSERT INTO watermarks(provider, dataset_id, partition_id, committed_version, "
                    "through_us) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(provider, dataset_id, partition_id) "
                    "DO UPDATE SET committed_version=CASE WHEN excluded.through_us > through_us "
                    "THEN excluded.committed_version ELSE committed_version END, "
                    "through_us=max(through_us, excluded.through_us)",
                    (
                        spec.mapper.provider,
                        spec.dataset_id,
                        partition_id,
                        marker["version"],
                        through,
                    ),
                )
        complete_operation(state, str(operation["operation_id"]), str(operation["request_hash"]))


def _evidence(
    workspace: Workspace, operation: Mapping[str, object]
) -> tuple[PromotionSpec, dict[str, object]]:
    manifest_raw = _read_raw(workspace, str(operation["payload_hash"]))
    manifest = json.loads(manifest_raw)
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema") != MANIFEST_SCHEMA
        or formats.canonical(manifest) != manifest_raw
        or manifest["request_hash"] != operation["request_hash"]
        or manifest["generation_id"] != operation["target_id"]
    ):
        raise ValueError("retained promotion manifest does not match its intent")
    spec_sha = str(manifest["spec_sha256"])
    spec = parse_spec(_read_raw(workspace, spec_sha), spec_sha)
    request = formats.request_document(
        spec.sha256, [pin.table_digest for pin in spec.sources], spec.parent
    )
    if hashlib.sha256(request).hexdigest() != operation["request_hash"]:
        raise ValueError("retained promotion spec does not match its request")
    if _read_raw(workspace, str(operation["request_hash"])) != request:
        raise ValueError("retained promotion request differs from its spec")
    return spec, manifest


def recover_promotion(
    workspace: Workspace, operation: Mapping[str, object], *, budget: ComputeBudget | None = None
) -> bool:
    """Finish one interrupted promotion from its retained spec; never call a provider.

    A committed generation is verified against the manifest and cataloged. Without one,
    the retained spec is planned again and published only if it recomputes exactly the
    manifest the intent recorded; anything else stays pending for quarantine.
    """
    budget = budget or _DEFAULT_BUDGET
    operation_id = str(operation["operation_id"])
    stored = get_operation(workspace.state, operation_id)
    if stored is None or stored["kind"] != OPERATION_KIND or stored["phase"] != "PREPARED":
        raise ValueError("expected a prepared promotion intent")
    operation = stored
    spec, manifest = _evidence(workspace, operation)
    committed = workspace.market.execute(
        "SELECT generation_id FROM market_generations WHERE operation_id=?", [operation_id]
    ).fetchone()
    if committed is not None:
        _complete(workspace, operation, spec, manifest, budget)
        return True
    try:
        plan = plan_promotion(workspace, spec, budget=budget)
        recomputed = None if plan.manifest is None else hashlib.sha256(plan.manifest).hexdigest()
        if recomputed != operation["payload_hash"]:
            return False
        _publish(workspace, plan, budget)
    except ParentChangedError:
        return False
    finally:
        _drop(workspace.market)
    return True


def verify_promotion(
    workspace: Workspace,
    generation_id: str,
    *,
    budget: ComputeBudget | None = None,
    deep: bool = False,
) -> dict[str, object]:
    """Verify a promoted generation and every ancestor's catalog, intent and evidence.

    Each generation's marker matches its catalog row, completed intent, retained spec,
    request and manifest, and its stored quality flags hash to the manifest's digest.
    The chain links are recomputed and the generation's own rows rehashed; ``deep``
    rehashes every delta.
    """
    budget = budget or _DEFAULT_BUDGET
    chain = generation_chain(workspace.market, generation_id)
    for marker in chain:
        operation = get_operation(workspace.state, str(marker["operation_id"]))
        if (
            operation is None
            or operation["kind"] != OPERATION_KIND
            or operation["phase"] != "COMPLETED"
            or operation["request_hash"] != marker["request_hash"]
            or operation["target_id"] != marker["generation_id"]
            or operation["expected_parent"] != marker["parent_id"]
        ):
            raise ValueError("promoted generation has no matching completed intent")
        spec, manifest = _evidence(workspace, operation)
        _manifest_matches(marker, manifest)
        catalog = workspace.state.execute(
            "SELECT 1 FROM dataset_versions WHERE status='committed' AND dataset_id=? AND "
            "version=? AND generation_id=? AND chain_hash=? AND manifest_hash=? AND row_count=? "
            "AND parent_generation_id IS ? AND sequence=? AND record_schema=? AND "
            "transform_hash=? AND normalizer_version=?",
            (
                marker["dataset_id"],
                marker["version"],
                marker["generation_id"],
                marker["chain_hash"],
                marker["request_hash"],
                marker["row_count"],
                marker["parent_id"],
                marker["sequence"],
                marker["record_schema"],
                spec.sha256,
                spec.mapper_name,
            ),
        ).fetchone()
        if catalog is None:
            raise ValueError("catalog and promoted generation disagree")
        _check_flags(workspace, spec.domain, str(marker["generation_id"]), manifest, budget)
    head = verify_generation_bulk(workspace.market, generation_id, budget=budget, deep=deep)
    return {
        "verified": True,
        "dataset_id": head["dataset_id"],
        "version": head["version"],
        "generation_id": head["generation_id"],
        "chain_hash": head["chain_hash"],
        "manifest_hash": head["request_hash"],
        "row_count": head["row_count"],
        "generations": len(chain),
    }


def is_promoted(workspace: Workspace, generation_id: str) -> bool:
    """Whether a market generation was written by a promotion intent."""
    marker = marker_for(workspace.market, generation_id)
    operation = get_operation(workspace.state, str(marker["operation_id"]))
    return operation is not None and operation["kind"] == OPERATION_KIND


def list_promotions(workspace: Workspace) -> list[dict[str, object]]:
    """Every promotion intent with the generation and catalog row it produced."""
    rows = workspace.state.execute(
        "SELECT o.operation_id, o.phase, o.target_id, o.request_hash, o.expected_parent, "
        "o.payload_hash, v.dataset_id, v.version, v.row_count, v.transform_hash, "
        "v.normalizer_version, v.chain_hash FROM storage_operations o "
        "LEFT JOIN dataset_versions v ON v.generation_id = o.target_id "
        "WHERE o.kind=? ORDER BY o.created_at_us, o.operation_id",
        (OPERATION_KIND,),
    ).fetchall()
    return [
        {
            "operation_id": row[0],
            "phase": row[1],
            "generation_id": row[2],
            "request_hash": row[3],
            "parent": row[4],
            "manifest_sha256": row[5],
            "dataset_id": row[6],
            "version": row[7],
            "row_count": row[8],
            "spec_sha256": row[9],
            "mapper": row[10],
            "chain_hash": row[11],
        }
        for row in rows
    ]
