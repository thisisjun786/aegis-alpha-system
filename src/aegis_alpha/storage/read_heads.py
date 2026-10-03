"""Predicate-pushdown head projection over ordered generation pins.

``read_heads`` projects the heads of one or more pinned generation chains inside
DuckDB and returns only the rows a consumer asked for. It keeps ``market.project_heads``
semantics exactly; the equivalence is held by property tests, so a change to either
projection changes the other in the same change.

- A binding (``HeadBinding``) names one domain, an ordered list of exact pins with
  contiguous ``[from, to)`` cutover intervals, the time rules it grants for strict use
  and the quality flags it excludes. Its canonical document (``aas-head-binding-v1``)
  and SHA-256 identify it, so cutovers, grants and exclusions are part of the hash.
- Every date belongs to exactly one pin. A pin's rows outside its interval are never
  returned, and a date no pin covers is reported, never filled from a neighbouring pin.
- Strict reads (a cutoff is given) treat a time whose generation rule is neither
  ``source_column@1`` nor granted as unknown. Inspection reads ignore grants.
- A revision carrying an excluded flag is never available: from the time it is known
  it removes the head it supersedes and its own value is never selected.
- Pins are checked against their markers, every chain link is recomputed from the
  recorded hashes and every generation's row count is recounted before any row is
  read. ``rehash=True`` also rehashes every delta (``verify_generation_bulk`` deep).
- Rows are counted and their text measured in SQL before any is fetched, so a result
  that cannot fit the caller's allocation fails as ``ComputeResourceError`` instead of
  materializing first.
- Each read returns a receipt (``aas-head-read-v1``): the binding, the query, every
  generation's rule provenance, the rules the read relied on or withheld, whether every
  delta was rehashed, and a digest of the selected heads. A run records that receipt
  rather than restating it.
- ``held=True`` also returns, as ``HeadRead.held``, every record the cutoff knows but that
  has no head because a grant withheld its time rule (``ungranted_time_rule``) or its
  evidence has no time (``unknown_<noun>_evidence``). A record not yet known by the cutoff is
  never held. A derived read (``adjusted_prices``) uses it so a known action is never dropped
  silently.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from itertools import pairwise
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Literal, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.bulk_generation import verify_chain_links, verify_generation_bulk
from aegis_alpha.storage.market import generation_chain, limit_duckdb, market_version
from aegis_alpha.storage.market_inputs import CoverageCell, CoverageReport, GenerationPin
from aegis_alpha.storage.market_schema import (
    COMMON,
    DOMAIN_VERSIONS,
    DOMAINS,
    NATURAL_KEYS,
    PRICE_FIELDS,
    text_bytes,
    text_columns,
)

if TYPE_CHECKING:
    import duckdb

BINDING_SCHEMA: Final = "aas-head-binding-v1"
RECEIPT_SCHEMA: Final = "aas-head-read-v1"
PROMOTION_SCHEMA: Final = "aas-promotion-v1"
SOURCE_COLUMN_RULE: Final = "source_column@1"
UNKNOWN_NULL_RULE: Final = "unknown_null@1"
# A recorded source time needs no grant, and a rule that is always null has nothing to grant.
FREE_RULES: Final = frozenset({SOURCE_COLUMN_RULE, UNKNOWN_NULL_RULE})
_RULE: Final = re.compile(r"[a-z][a-z0-9_]*@[1-9][0-9]*")
_FLAG: Final = re.compile(r"[a-z][a-z0-9_]*")
_TIME_COLUMNS: Final = ("available_at_us", "revision_known_at_us")
_ROLES: Final = frozenset({"canonical", "reference"})
_FETCH_ROWS: Final = 8192
# The columns a projected row carries beside its domain values (pin, subject, day, event...).
_BOOKKEEPING_COLUMNS: Final = 12
_DAY_US: Final = 86_400_000_000
# One coverage cell: the dataclass, its reasons tuple and its slot in the cell list.
_CELL_BYTES: Final = 512


def _day_of(column: str) -> str:
    return f"(DATE '1970-01-01' + CAST(floor({column} / {_DAY_US}.0) AS INTEGER))"


# The column (or expression) a binding's subjects and cutover dates select on. A filter
# on natural-key columns runs before projection; any other runs on the projected head,
# because a revision may move a non-key date across a cutover boundary.
_SUBJECTS: Final = {
    "prices": ('"instrument_id"', ("instrument_id",)),
    "corporate_actions": ('"instrument_id"', ("instrument_id",)),
    "instrument_status": ('"instrument_id"', ("instrument_id",)),
    "fundamentals": ('"issuer_id"', ("issuer_id",)),
    "macro_observations": ('"series_id"', ("series_id",)),
    "estimates": ('"instrument_id"', ("instrument_id",)),
    "fx_rates": (
        '("base_currency" || \'/\' || "quote_currency")',
        ("base_currency", "quote_currency"),
    ),
    "calendar_sessions": ('"calendar_id"', ("calendar_id",)),
    "feature_values": ('"instrument_id"', ("instrument_id",)),
    "filings": ('"issuer_id"', ("issuer_id",)),
    "classifications": ('"subject_id"', ("subject_id",)),
}
_DATES: Final = {
    "prices": ('"session_date"', ("session_date",)),
    "corporate_actions": ('"effective_date"', ("effective_date",)),
    "instrument_status": (_day_of('"effective_from_us"'), ("effective_from_us",)),
    "fundamentals": ('"period_end"', ("period_end",)),
    "macro_observations": ('"observation_period"', ("observation_period",)),
    "estimates": ('"target_period"', ("target_period",)),
    "fx_rates": (_day_of('"fixing_at_us"'), ("fixing_at_us",)),
    "calendar_sessions": ('"session_date"', ("session_date",)),
    "feature_values": (_day_of('"feature_at_us"'), ("feature_at_us",)),
    "filings": ('"filed_date"', ("filed_date",)),
    "classifications": ('"effective_from"', ("effective_from",)),
}
_NOUNS: Final = {"prices": "price", "calendar_sessions": "session"}


def _pre(domain: str, columns: tuple[str, ...]) -> bool:
    return set(columns) <= set(NATURAL_KEYS[domain])


def _rule(value: object) -> str:
    if not isinstance(value, str) or _RULE.fullmatch(value) is None:
        raise ValueError("time rule must be an exact id@version")
    return value


def _flag(value: object) -> str:
    if not isinstance(value, str) or _FLAG.fullmatch(value) is None:
        raise ValueError("quality flag must be a lowercase identifier")
    return value


@dataclass(frozen=True, slots=True)
class HeadPin:
    """One exact generation pin and the ``[from_date, to_date)`` interval it serves."""

    pin: GenerationPin
    from_date: date | None = None
    to_date: date | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.pin, GenerationPin):
            raise TypeError("head pin requires an exact generation pin")
        for bound in (self.from_date, self.to_date):
            if bound is not None and type(bound) is not date:
                raise TypeError("cutover bounds must be dates")
        if (
            self.from_date is not None
            and self.to_date is not None
            and not self.from_date < self.to_date
        ):
            raise ValueError("cutover interval must be nonempty")

    def covers(self, day: date) -> bool:
        return (self.from_date is None or self.from_date <= day) and (
            self.to_date is None or day < self.to_date
        )


def _check_pins(pins: object) -> None:
    if type(pins) is not tuple or not pins:
        raise ValueError("binding requires a nonempty tuple of head pins")
    items = cast("tuple[object, ...]", pins)
    if any(not isinstance(item, HeadPin) for item in items):
        raise TypeError("binding pins must be head pins")
    heads = cast("tuple[HeadPin, ...]", items)
    # One generation may serve several intervals (A -> B -> A); rows partition by pin ordinal.
    for previous, current in pairwise(heads):
        # Contiguous: each date belongs to exactly one pin and no pin is skipped.
        if previous.to_date is None or previous.to_date != current.from_date:
            raise ValueError("cutover intervals must be ordered and contiguous")


@dataclass(frozen=True, slots=True)
class HeadBinding:
    """Ordered pins with contiguous cutovers, strict time-rule grants and flag exclusions."""

    domain: str
    pins: tuple[HeadPin, ...]
    granted_rules: tuple[str, ...] = ()
    excluded_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.domain not in DOMAINS:
            raise ValueError("unknown market domain")
        _check_pins(self.pins)
        for name, check in (("granted_rules", _rule), ("excluded_flags", _flag)):
            values = getattr(self, name)
            if type(values) is not tuple:
                raise TypeError("binding grants and exclusions must be tuples")
            names = tuple(sorted(check(value) for value in values))
            if len(set(names)) != len(names):
                raise ValueError("binding grants and exclusions must be unique")
            object.__setattr__(self, name, names)

    def document(self) -> dict[str, object]:
        return {
            "schema": BINDING_SCHEMA,
            "domain": self.domain,
            "pins": [
                {
                    "dataset_id": item.pin.dataset_id,
                    "version": item.pin.version,
                    "generation_id": item.pin.generation_id,
                    "chain_hash": item.pin.chain_hash,
                    "manifest_hash": item.pin.manifest_hash,
                    "from": item.from_date,
                    "to": item.to_date,
                }
                for item in self.pins
            ],
            "granted_rules": list(self.granted_rules),
            "excluded_flags": list(self.excluded_flags),
        }

    @property
    def binding_hash(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.document())).hexdigest()

    def pin_for(self, day: date) -> int | None:
        return next((index for index, item in enumerate(self.pins) if item.covers(day)), None)


_PIN_KEYS: Final = frozenset(
    {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash", "from", "to"}
)
_BINDING_KEYS: Final = frozenset({"schema", "domain", "pins", "granted_rules", "excluded_flags"})


def _bound(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("cutover bounds must be YYYY-MM-DD text or null")
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("cutover bounds must be YYYY-MM-DD text")
    return day


def head_binding(document: Mapping[str, object]) -> HeadBinding:
    """Parse one ``aas-head-binding-v1`` document back into the binding it spells.

    The document must be exactly what ``HeadBinding.document()`` writes (sorted grants and
    exclusions, ISO dates), so its canonical bytes and the binding hash are one identity.
    """
    if not isinstance(document, Mapping) or set(document) != _BINDING_KEYS:
        raise ValueError("head binding document requires exactly its five fields")
    if document["schema"] != BINDING_SCHEMA:
        raise ValueError("unsupported head binding schema")
    pins = document["pins"]
    if not isinstance(pins, list | tuple):
        raise TypeError("head binding pins must be a list")
    parsed = []
    for item in cast("list[object]", pins):
        if not isinstance(item, Mapping) or set(item) != _PIN_KEYS:
            raise ValueError("head pin requires exactly its seven fields")
        entry = cast("Mapping[str, object]", item)
        parsed.append(
            HeadPin(
                GenerationPin(
                    *(
                        cast("str", entry[key])
                        for key in (
                            "dataset_id",
                            "version",
                            "generation_id",
                            "chain_hash",
                            "manifest_hash",
                        )
                    )
                ),
                _bound(entry["from"]),
                _bound(entry["to"]),
            )
        )
    lists = []
    for key in ("granted_rules", "excluded_flags"):
        values = document[key]
        if not isinstance(values, list | tuple):
            raise TypeError("head binding grants and exclusions must be lists")
        lists.append(tuple(cast("list[str]", values)))
    binding = HeadBinding(cast("str", document["domain"]), tuple(parsed), lists[0], lists[1])
    if canonical_json_bytes(binding.document()) != canonical_json_bytes(dict(document)):
        raise ValueError("head binding document is not in its canonical spelling")
    return binding


@dataclass(frozen=True, slots=True)
class HeadQuery:
    """Cutoffs and pushdown filters; a cutoff makes the read strict point-in-time."""

    cutoff_us: int | None = None
    ingestion_cutoff_us: int | None = None
    # Research reads only: a revision recorded as known after this instant is not read,
    # while one with no recorded knowledge time still is (``_Visibility.candidates``).
    known_ceiling_us: int | None = None
    subjects: tuple[str, ...] | None = None
    from_date: date | None = None
    to_date: date | None = None
    price_roles: tuple[str, ...] | None = None
    grid: tuple[date, ...] | None = None

    def __post_init__(self) -> None:
        for cutoff in (self.cutoff_us, self.ingestion_cutoff_us, self.known_ceiling_us):
            if cutoff is not None and (type(cutoff) is not int or not 0 <= cutoff < 2**63):
                raise ValueError("cutoff must be UTC microseconds")
        if self.known_ceiling_us is not None and self.cutoff_us is not None:
            # A strict cutoff already decides knowledge; a second ceiling would be ignored.
            raise ValueError("a known ceiling belongs to a research read only")
        for values in (self.subjects, self.price_roles, self.grid):
            if values is not None and (
                type(values) is not tuple or not values or len(set(values)) != len(values)
            ):
                raise ValueError("query filters must be nonempty tuples of unique values")
        if self.subjects is not None and any(
            not isinstance(value, str) or not value for value in self.subjects
        ):
            raise ValueError("subjects must be nonempty text")
        if self.price_roles is not None and not set(self.price_roles) <= _ROLES:
            raise ValueError("unknown price role")
        self._check_dates()

    def _check_dates(self) -> None:
        for bound in (self.from_date, self.to_date, *(self.grid or ())):
            if bound is not None and type(bound) is not date:
                raise TypeError("query dates must be dates")
        if (
            self.from_date is not None
            and self.to_date is not None
            and not self.from_date < self.to_date
        ):
            raise ValueError("query date range must be nonempty")
        if self.grid is not None:
            if self.subjects is None:
                raise ValueError("a coverage grid requires subjects")
            if tuple(sorted(self.grid)) != self.grid:
                raise ValueError("grid dates must be increasing")

    @property
    def mode(self) -> Literal["strict_pit", "observed_snapshot_research"]:
        return "observed_snapshot_research" if self.cutoff_us is None else "strict_pit"

    def document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "cutoff_us": self.cutoff_us,
            "ingestion_cutoff_us": self.ingestion_cutoff_us,
            "subjects": None if self.subjects is None else sorted(self.subjects),
            "from": self.from_date,
            "to": self.to_date,
            "price_roles": None if self.price_roles is None else sorted(self.price_roles),
            "grid": None if self.grid is None else list(self.grid),
        }
        if self.known_ceiling_us is not None:
            # Present only when set, so every read without one keeps its receipt bytes.
            document["known_ceiling_us"] = self.known_ceiling_us
        return document


@dataclass(frozen=True, slots=True)
class TimeRules:
    """Which rule produced one generation's ``available_at_us`` and ``revision_known_at_us``."""

    available: str
    known: str

    def __post_init__(self) -> None:
        _rule(self.available)
        _rule(self.known)

    def granted(self, column: str, grants: tuple[str, ...]) -> bool:
        rule = self.available if column == "available_at_us" else self.known
        return rule in FREE_RULES or rule in grants


RECORDED_TIMES: Final = TimeRules(SOURCE_COLUMN_RULE, SOURCE_COLUMN_RULE)


def spec_time_rules(spec: Mapping[str, object]) -> TimeRules:
    """Read the time rules an ``aas-promotion-v1`` spec declares for its two time columns."""
    if spec.get("schema_version") != PROMOTION_SCHEMA:
        raise ValueError("not an aas-promotion-v1 spec")
    rules = spec.get("time_rules")
    if not isinstance(rules, Mapping) or set(rules) != set(_TIME_COLUMNS):
        raise ValueError("promotion spec time_rules must name both time columns")
    declared = []
    for column in _TIME_COLUMNS:
        entry = rules[column]
        if not isinstance(entry, Mapping):
            raise TypeError("promotion spec time rule must be an object")
        declared.append(_rule(cast("Mapping[str, object]", entry).get("rule")))
    return TimeRules(*declared)


@dataclass(frozen=True, slots=True)
class QualityFlag:
    rule_id: str
    rule_version: str
    flag: str
    detail: str | None


@dataclass(frozen=True, slots=True)
class HeadRow:
    """One projected head: its pin ordinal, domain values and the flags on its revision."""

    pin: int
    values: Mapping[str, object]
    flags: tuple[QualityFlag, ...] = ()


@dataclass(frozen=True, slots=True)
class HeadRead:
    rows: tuple[HeadRow, ...]
    coverage: CoverageReport | None
    receipt: Mapping[str, object]
    receipt_hash: str
    held: tuple[CoverageCell, ...] = ()
    certified: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class _Record:
    pin: int
    subject: object
    day: object
    record_id: str
    head: HeadRow | None
    reasons: tuple[str, ...]


def _verify_pins(
    connection: duckdb.DuckDBPyConnection,
    binding: HeadBinding,
    time_rules: Mapping[str, TimeRules],
    *,
    budget: ComputeBudget,
    rehash: bool,
) -> list[list[dict[str, object]]]:
    chains = []
    for item in binding.pins:
        chain = generation_chain(connection, item.pin.generation_id)
        head = chain[-1]
        if (
            any(
                head[key] != getattr(item.pin, key)
                for key in ("dataset_id", "version", "generation_id", "chain_hash")
            )
            or head["request_hash"] != item.pin.manifest_hash
        ):
            raise ValueError("exact generation pin does not match its marker")
        if head["domain"] != binding.domain:
            raise ValueError("pin has incompatible dataset/domain")
        verify_chain_links(chain)
        generations = [str(marker["generation_id"]) for marker in chain]
        counts = dict(
            connection.execute(
                f'SELECT generation_id, count(*) FROM "{binding.domain}" '  # noqa: S608 -- domain allowlist
                "WHERE generation_id IN (SELECT unnest($g::VARCHAR[])) GROUP BY generation_id",
                {"g": generations},
            ).fetchall()
        )
        for marker in chain:
            if counts.get(marker["generation_id"], 0) != marker["row_count"]:
                raise ValueError("market generation logical hash/count mismatch")
            rules = time_rules.get(str(marker["generation_id"]))
            if rules is None:
                raise ValueError("every pinned generation needs its time-rule provenance")
            if not isinstance(rules, TimeRules):
                raise TypeError("time-rule provenance must be TimeRules")
        if rehash:
            verify_generation_bulk(connection, item.pin.generation_id, budget=budget, deep=True)
        chains.append(chain)
    return chains


@dataclass(frozen=True, slots=True)
class _Projection:
    """One head projection: its SQL, the domain columns it returns and its parameters."""

    sql: str
    names: tuple[str, ...]
    params: Mapping[str, object]
    domain: str
    strict: bool


def _filters(
    binding: HeadBinding, query: HeadQuery, params: dict[str, object]
) -> tuple[list[str], list[str]]:
    """Place each filter before projection on natural-key columns, after it otherwise."""
    domain = binding.domain
    _, subject_columns = _SUBJECTS[domain]
    _, date_columns = _DATES[domain]
    dates = [] if _pre(domain, date_columns) else None
    subjects = [] if _pre(domain, subject_columns) else None
    pre: list[str] = []
    post: list[str] = []
    day_filters = ["(_from IS NULL OR _day >= _from) AND (_to IS NULL OR _day < _to)"]
    if query.from_date is not None:
        params["qfrom"] = query.from_date
        day_filters.append("_day >= $qfrom")
    if query.to_date is not None:
        params["qto"] = query.to_date
        day_filters.append("_day < $qto")
    if query.grid is not None:
        params["grid"] = list(query.grid)
        day_filters.append("list_contains($grid::DATE[], _day)")
    (pre if dates is not None else post).extend(day_filters)
    if query.subjects is not None:
        params["subjects"] = list(query.subjects)
        (pre if subjects is not None else post).append(
            "list_contains($subjects::VARCHAR[], _subject)"
        )
    if query.price_roles is not None:
        if domain != "prices":
            raise ValueError("price roles filter only prices")
        params["roles"] = list(query.price_roles)
        pre.append("list_contains($roles::VARCHAR[], price_role)")
    if query.ingestion_cutoff_us is not None:
        params["ingested"] = query.ingestion_cutoff_us
        pre.append("ingested_at_us <= $ingested")
    if query.known_ceiling_us is not None:
        params["ceiling"] = query.known_ceiling_us
        pre.append("(revision_known_at_us IS NULL OR revision_known_at_us <= $ceiling)")
    return pre, post


def _exclusion(
    binding: HeadBinding, *, flags: bool, params: dict[str, object]
) -> tuple[str, str, str]:
    """The CTE, join and predicate that mark a revision carrying an excluded flag."""
    if not binding.excluded_flags or not flags:
        return "", "", "false"
    params["flags"] = list(binding.excluded_flags)
    return (
        (
            ", excluded AS (SELECT DISTINCT generation_id, record_id, revision_id "
            "FROM quality_flags WHERE list_contains($flags::VARCHAR[], flag) "
            "AND generation_id IN (SELECT generation_id FROM gens))"
        ),
        (
            "LEFT JOIN excluded x ON x.generation_id = d.generation_id "
            "AND x.record_id = d.record_id AND x.revision_id = d.revision_id"
        ),
        "x.revision_id IS NOT NULL",
    )


def _flagged(*, flags: bool) -> tuple[str, str, str]:
    """The CTE, join and column that return each selected revision's quality flags."""
    if not flags:
        return "", "", "NULL"
    return (
        (
            ", flagged AS (SELECT generation_id, record_id, revision_id, "
            "list(struct_pack(rule_id := rule_id, rule_version := rule_version, flag := flag, "
            "detail := detail) ORDER BY rule_id, rule_version, flag) AS _flags "
            "FROM quality_flags WHERE generation_id IN (SELECT generation_id FROM gens) "
            "GROUP BY generation_id, record_id, revision_id)"
        ),
        (
            "LEFT JOIN flagged fl ON fl.generation_id = r.generation_id "
            "AND fl.record_id = r.record_id AND fl.revision_id = r.revision_id"
        ),
        "fl._flags",
    )


@dataclass(frozen=True, slots=True)
class _Events:
    """Per-row SQL: the projection event and what coverage reports about the row."""

    event: str  # 'set' the head, 'pop' it, or NULL to skip the row (``market.project_heads``)
    withheld: str  # a grant would have let the row through
    seen: str  # the read can see the row at all (known by the cutoff)
    state: str  # what the row means to coverage when it is the latest row seen


def _events(query: HeadQuery, reference: str, params: dict[str, object]) -> _Events:
    if query.cutoff_us is None:
        return _Events(
            "CASE WHEN _excluded THEN CASE WHEN op <> 'ASSERT' THEN 'pop' END ELSE 'set' END",
            "false",
            "true",
            "CASE WHEN _excluded THEN 'flag_excluded' WHEN op = 'TOMBSTONE' THEN 'tombstone' "
            "ELSE 'present' END",
        )
    params["cutoff"] = query.cutoff_us
    return _Events(
        (
            "CASE WHEN _known IS NULL OR _known > $cutoff THEN NULL "
            "WHEN _avail IS NULL OR _avail > $cutoff OR _excluded "
            "THEN CASE WHEN op <> 'ASSERT' THEN 'pop' END "
            f"WHEN {reference} THEN NULL ELSE 'set' END"
        ),
        (
            "(revision_known_at_us IS NOT NULL AND revision_known_at_us <= $cutoff AND "
            "(NOT _k_ok OR (NOT _a_ok AND available_at_us IS NOT NULL "
            "AND available_at_us <= $cutoff)))"
        ),
        "(_known IS NOT NULL AND _known <= $cutoff)",
        # The order of ``market_inputs._absence`` for the latest revision known by the cutoff.
        (
            "CASE WHEN _excluded THEN 'flag_excluded' "
            "WHEN _avail IS NULL THEN CASE WHEN NOT _a_ok AND available_at_us IS NOT NULL "
            "THEN 'ungranted_time_rule' ELSE 'unknown_evidence' END "
            "WHEN _avail > $cutoff THEN 'unavailable' "
            f"WHEN {reference} THEN 'reference_price' "
            "WHEN op = 'TOMBSTONE' THEN 'tombstone' ELSE 'present' END"
        ),
    )


def _columns(connection: duckdb.DuckDBPyConnection, domain: str) -> tuple[list[str], bool]:
    """The columns a read selects and whether the store holds v2 quality flags.

    A v2 store adds which price fields a row carries, so a close-only reference bar keeps
    ``fields``; an OHLCV row drops it again when fetched.
    """
    names = [name for name, _ in COMMON + DOMAINS[domain]]
    flags = market_version(connection) >= 2  # noqa: PLR2004 -- fields and quality_flags arrive in v2
    if domain == "prices" and flags:
        names.append(PRICE_FIELDS[0])
    return names, flags


def _projection(
    connection: duckdb.DuckDBPyConnection,
    binding: HeadBinding,
    query: HeadQuery,
    params: dict[str, object],
    *,
    held: bool = False,
) -> _Projection:
    domain = binding.domain
    names, flags = _columns(connection, domain)
    pre, post = _filters(binding, query, params)
    excluded_cte, excluded_join, excluded = _exclusion(binding, flags=flags, params=params)
    flag_cte, flag_join, flag_select = _flagged(flags=flags)
    reference = "price_role = 'reference'" if domain == "prices" else "false"
    events = _events(query, reference, params)
    selected = ", ".join(f'r."{name}"' for name in names)
    final = "" if query.grid is not None else " AND _event = 'set' AND op <> 'TOMBSTONE'"
    if held and final:
        final = (
            " AND ((_event = 'set' AND op <> 'TOMBSTONE') "
            "OR _withheld_any OR _last_state IN ('ungranted_time_rule', 'unknown_evidence'))"
        )
    sql = f"""
WITH gens AS (
  SELECT unnest($g::VARCHAR[]) AS generation_id, unnest($p::BIGINT[]) AS _pin,
         unnest($s::BIGINT[]) AS _seq, unnest($a::BOOLEAN[]) AS _a_ok,
         unnest($k::BOOLEAN[]) AS _k_ok, unnest($f::DATE[]) AS _from,
         unnest($t::DATE[]) AS _to
){excluded_cte}{flag_cte}, cand AS (
  SELECT * FROM (
    SELECT d.*, g._pin, g._seq, g._a_ok, g._k_ok, g._from, g._to,
           CASE WHEN g._a_ok THEN d.available_at_us END AS _avail,
           CASE WHEN g._k_ok THEN d.revision_known_at_us END AS _known,
           {excluded} AS _excluded, {_SUBJECTS[domain][0]} AS _subject,
           {_DATES[domain][0]} AS _day
    FROM "{domain}" d JOIN gens g ON d.generation_id = g.generation_id {excluded_join}
  ) WHERE {" AND ".join(pre) or "true"}
), events AS (
  SELECT *, {events.event} AS _event, {events.withheld} AS _withheld, {events.seen} AS _seen,
         {events.state} AS _state FROM cand
), ranked AS (
  SELECT *,
         bool_or(_withheld) OVER w AS _withheld_any,
         bool_or(_excluded AND _seen) OVER w AS _excluded_any,
         bool_or(revision_known_at_us IS NULL) OVER w AS _unknown_any,
         arg_max(_state, CASE WHEN _seen THEN _seq END) OVER w AS _last_state
  FROM events
  WINDOW w AS (PARTITION BY _pin, record_id)
  QUALIFY row_number() OVER (
    PARTITION BY _pin, record_id ORDER BY (_event IS NOT NULL) DESC, _seq DESC
  ) = 1
)
SELECT {selected}, r._pin, r._subject, r._day, r._event, r._withheld_any, r._excluded_any,
       r._unknown_any, r._last_state, {flag_select} AS _flags
FROM (SELECT * FROM ranked WHERE {" AND ".join(post) or "true"}{final}) r {flag_join}
ORDER BY r._pin, r.record_id"""  # noqa: S608 -- code-owned schema and fragments; values are parameters
    return _Projection(
        sql, tuple(names), MappingProxyType(params), domain, query.cutoff_us is not None
    )


def _admit(
    connection: duckdb.DuckDBPyConnection,
    projection: _Projection,
    query: HeadQuery,
    budget: ComputeBudget,
) -> None:
    schema = COMMON + DOMAINS[projection.domain]
    text = " + ".join(
        f'coalesce(length("{name}"), 0)' for name, kind in schema if kind.rstrip("?") == "VARCHAR"
    )
    flags = (
        "coalesce(list_sum(list_transform(_flags, lambda x: 512 + length(x.rule_id) + "
        "length(x.rule_version) + length(x.flag) + coalesce(length(x.detail), 0))), 0)"
    )
    count, characters, flag_bytes = cast(
        "tuple[int, int, int]",
        connection.execute(
            f"SELECT count(*), coalesce(sum({text} + coalesce(length(_subject::VARCHAR), 0)), 0), "  # noqa: S608 -- code-owned schema
            f"coalesce(sum({flags}), 0) FROM ({projection.sql})",
            dict(projection.params),
        ).fetchone(),
    )
    # Every fetched row holds its domain values, the projection's bookkeeping columns,
    # the fetched tuple, a dict and its mapping; text is charged by value and by character
    # (text_bytes). On a 21,288-row KR price read the traced Python peak was 3.1 KB per row
    # and this charges about 5.7 KB, so the bound holds with room for wider rows.
    values = count * (text_columns(schema) + 1)
    # A coverage grid builds one cell per requested subject and date whatever the result
    # holds, so a sparse grid is charged for its cells too.
    days = len(query.grid or ())
    cells = days * len(query.subjects or ())
    cell_characters = days * sum(len(subject) for subject in query.subjects or ())
    estimated = (
        64 * 1024
        + count * (1024 + 64 * (len(projection.names) + _BOOKKEEPING_COLUMNS))
        + text_bytes(values, characters)
        + 4 * flag_bytes
        + cells * _CELL_BYTES
        + text_bytes(cells, cell_characters)
    )
    if estimated > budget.available_bytes:
        raise ComputeResourceError(
            f"head read memory estimate {estimated} exceeds admitted "
            f"materialization budget {budget.available_bytes} bytes"
        )


def _reasons(row: Mapping[str, object], noun: str, *, strict: bool) -> tuple[str, ...]:
    """A record's coverage reasons, in ``market_inputs._absence`` terms when it has no head.

    A served head keeps any withheld correction or excluded revision as a reason, so a
    consumer can tell which cells were served a value a grant or an exclusion held back.
    """
    extra = []
    if row["_withheld_any"]:
        extra.append("ungranted_time_rule")
    if row["_excluded_any"]:
        extra.append("flag_excluded")
    if row["_event"] == "set":
        if row["op"] == "TOMBSTONE":
            first: list[str] = ["tombstone"]
        elif noun == "session":
            first = [] if row["status"] == "open" else ["session_closed"]
        else:
            state = row.get("value_state", "present")
            first = [] if state == "present" else [str(state)]
        return tuple(dict.fromkeys([*first, *extra]))
    # No head: the latest revision the read could see says why.
    last = row["_last_state"]
    if last is not None and last != "present":
        reason = {
            "unknown_evidence": f"unknown_{noun}_evidence",
            "unavailable": f"{noun}_unavailable",
        }.get(str(last), str(last))
        return tuple(dict.fromkeys([reason, *extra]))
    if extra:
        return tuple(extra)
    if strict and row["_unknown_any"]:
        return (f"unknown_{noun}_evidence",)
    return (f"missing_{noun}",)


def _fetch(connection: duckdb.DuckDBPyConnection, projection: _Projection) -> list[_Record]:
    cursor = connection.execute(projection.sql, dict(projection.params))
    columns = [item[0] for item in cursor.description or ()]
    names = projection.names
    domain = projection.domain
    strict = projection.strict
    noun = _NOUNS.get(domain, domain)
    records = []
    while batch := cursor.fetchmany(_FETCH_ROWS):
        for fetched in batch:
            row = dict(zip(columns, fetched, strict=True))
            values = {name: row[name] for name in names}
            # The stored default reads back as the v1 shape every OHLCV row was hashed in.
            if values.get(PRICE_FIELDS[0]) == "ohlcv":
                del values[PRICE_FIELDS[0]]
            head = None
            if row["_event"] == "set" and row["op"] != "TOMBSTONE":
                flags = tuple(
                    QualityFlag(item["rule_id"], item["rule_version"], item["flag"], item["detail"])
                    for item in cast("list[dict[str, str]]", row["_flags"] or [])
                )
                head = HeadRow(int(row["_pin"]), MappingProxyType(values), flags)
            records.append(
                _Record(
                    int(row["_pin"]),
                    row["_subject"],
                    row["_day"],
                    str(row["record_id"]),
                    head,
                    _reasons(row, noun, strict=strict),
                )
            )
    return records


def _usable(head: HeadRow, noun: str) -> bool:
    """A head fills its cell: a present value, or an open session for a calendar."""
    if noun == "session":
        return head.values["status"] == "open"
    return head.values.get("value_state", "present") == "present"


def _coverage(binding: HeadBinding, query: HeadQuery, records: list[_Record]) -> CoverageReport:
    noun = _NOUNS.get(binding.domain, binding.domain)
    cells_by_key: dict[tuple[object, object], list[_Record]] = {}
    for record in records:
        cells_by_key.setdefault((record.subject, record.day), []).append(record)
    cells = []
    for day in query.grid or ():
        covered = binding.pin_for(day) is not None
        for subject in query.subjects or ():
            found = cells_by_key.get((subject, day), [])
            heads = [record.head for record in found if record.head is not None]
            # When a head supplies the cell, another record's absence (a skipped reference
            # price beside a canonical one) is not a reason against the cell.
            sources = [record for record in found if record.head is not None] or found
            if not covered:
                reasons: tuple[str, ...] = ("outside_cutover",)
            elif not found:
                reasons = (f"missing_{noun}",)
            else:
                reasons = tuple(
                    dict.fromkeys(reason for record in sources for reason in record.reasons)
                )
            present = any(_usable(head, noun) for head in heads)
            record_id = str(heads[0].values["record_id"]) if len(heads) == 1 else None
            cells.append(CoverageCell(subject, day, present, reasons, record_id=record_id))
    report_reasons = list(dict.fromkeys(reason for cell in cells for reason in cell.reasons))
    return CoverageReport(tuple(cells), tuple(report_reasons))


def _generation_params(
    binding: HeadBinding,
    chains: list[list[dict[str, object]]],
    time_rules: Mapping[str, TimeRules],
    *,
    strict: bool,
) -> dict[str, object]:
    """The ``gens`` parameters: each generation's pin, sequence, interval and granted times."""
    params: dict[str, object] = {"g": [], "p": [], "s": [], "a": [], "k": [], "f": [], "t": []}
    for ordinal, (item, chain) in enumerate(zip(binding.pins, chains, strict=True)):
        for marker in chain:
            rules = time_rules[str(marker["generation_id"])]
            for key, value in (
                ("g", marker["generation_id"]),
                ("p", ordinal),
                ("s", marker["sequence"]),
                ("a", not strict or rules.granted("available_at_us", binding.granted_rules)),
                ("k", not strict or rules.granted("revision_known_at_us", binding.granted_rules)),
                ("f", item.from_date),
                ("t", item.to_date),
            ):
                cast("list[object]", params[key]).append(value)
    return params


def _provenance(
    chains: list[list[dict[str, object]]], time_rules: Mapping[str, TimeRules]
) -> tuple[list[list[object]], set[str]]:
    """Each generation's ``[pin, generation, available rule, known rule]`` and its granted rules.

    The second value holds the rules a grant decides: neither recorded nor always null.
    """
    provenance: list[list[object]] = []
    present: set[str] = set()
    for ordinal, chain in enumerate(chains):
        for marker in chain:
            rules = time_rules[str(marker["generation_id"])]
            provenance.append([ordinal, marker["generation_id"], rules.available, rules.known])
            present.update({rules.available, rules.known} - FREE_RULES)
    return provenance, present


def _receipt(  # noqa: PLR0913 -- the read's inputs, its verification and its result
    binding: HeadBinding,
    query: HeadQuery,
    chains: list[list[dict[str, object]]],
    time_rules: Mapping[str, TimeRules],
    rows: tuple[HeadRow, ...],
    *,
    rehash: bool,
) -> dict[str, object]:
    provenance, present = _provenance(chains, time_rules)
    strict = query.cutoff_us is not None
    heads = [[row.pin, row.values["record_id"], row.values["revision_id"]] for row in rows]
    return {
        "schema": RECEIPT_SCHEMA,
        "binding": binding.document(),
        "binding_hash": binding.binding_hash,
        "query": query.document(),
        "mode": query.mode,
        "time_rules": provenance,
        "applied_rules": sorted(present & set(binding.granted_rules)) if strict else [],
        "withheld_rules": sorted(present - set(binding.granted_rules)) if strict else [],
        # True when every delta was rehashed; false is a structure-only check.
        "rehashed": rehash,
        "heads": len(rows),
        "heads_hash": hashlib.sha256(canonical_json_bytes(heads)).hexdigest(),
    }


def read_heads(  # noqa: PLR0913 -- binding, query and the caller-owned resources
    connection: duckdb.DuckDBPyConnection,
    binding: HeadBinding,
    query: HeadQuery,
    *,
    time_rules: Mapping[str, TimeRules],
    budget: ComputeBudget,
    rehash: bool = False,
    held: bool = False,
) -> HeadRead:
    """Project the binding's heads in DuckDB with every filter pushed into the scan.

    ``time_rules`` maps every generation of every pinned chain to the rules that
    produced its two time columns; ``market_inputs.load_pinned_heads`` derives it from
    each generation's retained evidence. Call under workspace admission. DuckDB limits
    are lowered to the budget and stay lowered; the caller owns the connection.
    """
    import duckdb  # noqa: PLC0415 -- capacity errors at the budgeted query boundary

    if not isinstance(binding, HeadBinding) or not isinstance(query, HeadQuery):
        raise TypeError("read_heads requires a head binding and a head query")
    if DOMAIN_VERSIONS[binding.domain] > market_version(connection):
        raise ValueError(f"the {binding.domain} domain needs aas db migrate --to 2")
    try:
        limit_duckdb(connection, budget)
        chains = _verify_pins(connection, binding, time_rules, budget=budget, rehash=rehash)
        strict = query.cutoff_us is not None
        params = _generation_params(binding, chains, time_rules, strict=strict)
        projection = _projection(connection, binding, query, params, held=held)
        _admit(connection, projection, query, budget)
        records = _fetch(connection, projection)
    except duckdb.OutOfMemoryException as error:
        raise ComputeResourceError(
            "DuckDB cannot project heads within admitted memory limits"
        ) from error
    rows = tuple(record.head for record in records if record.head is not None)
    coverage = _coverage(binding, query, records) if query.grid is not None else None
    receipt = _receipt(binding, query, chains, time_rules, rows, rehash=rehash)
    kept: tuple[CoverageCell, ...] = ()
    if held:
        noun = _NOUNS.get(binding.domain, binding.domain)
        reasons = {"ungranted_time_rule", f"unknown_{noun}_evidence"}
        kept = tuple(
            CoverageCell(
                str(record.subject),
                cast("date", record.day),
                present=False,
                reasons=record.reasons,
                record_id=record.record_id,
            )
            for record in records
            if record.head is None and record.reasons and record.reasons[0] in reasons
        )
        receipt["held"] = [[cell.record_id, list(cell.reasons)] for cell in kept]
    return HeadRead(
        rows,
        coverage,
        MappingProxyType(receipt),
        hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
        kept,
    )


REVISIONS_SCHEMA: Final = "aas-head-revisions-v1"


@dataclass(frozen=True, slots=True)
class Revision:
    """One stored revision as a read of its binding sees it.

    In a strict read a time the binding does not grant is already null. ``excluded`` says
    the revision carries a flag the binding excludes.
    """

    pin: int
    values: Mapping[str, object]
    excluded: bool = False


@dataclass(frozen=True, slots=True)
class RevisionRead:
    """Every revision a read of the binding may project, oldest generation first per pin."""

    revisions: tuple[Revision, ...]
    strict: bool
    receipt: Mapping[str, object]
    receipt_hash: str
    certified: bool = field(default=False, init=False)


def project_revisions(
    revisions: tuple[Revision, ...],
    *,
    strict: bool,
    cutoff_us: int | None = None,
    ingestion_cutoff_us: int | None = None,
) -> tuple[HeadRow, ...]:
    """The heads ``read_heads`` returns for the same binding at ``cutoff_us``.

    The SQL events of ``read_heads`` applied in generation order: a strict read skips a
    revision it cannot know, removes the prior head when it knows a revision it cannot
    use, and never selects a reference price; every read removes the prior head at an
    excluded revision. Revisions must come from ``read_revisions`` with the same mode.
    """
    if strict != (cutoff_us is not None):
        raise ValueError("a strict projection needs a cutoff and a research one none")
    heads: dict[tuple[int, str], Revision] = {}
    for revision in revisions:
        values = revision.values
        key = (revision.pin, str(values["record_id"]))
        if (
            ingestion_cutoff_us is not None
            and cast("int", values["ingested_at_us"]) > ingestion_cutoff_us
        ):
            continue
        known, available = values["revision_known_at_us"], values["available_at_us"]
        if cutoff_us is not None and (known is None or cast("int", known) > cutoff_us):
            continue
        if revision.excluded or (
            cutoff_us is not None and (available is None or cast("int", available) > cutoff_us)
        ):
            if values["op"] != "ASSERT":
                heads.pop(key, None)
            continue
        if cutoff_us is not None and values.get("price_role") == "reference":
            continue
        heads[key] = revision
    return tuple(
        HeadRow(revision.pin, revision.values)
        for key, revision in sorted(heads.items())
        if revision.values["op"] != "TOMBSTONE"
    )


def read_revisions(  # noqa: PLR0913 -- binding, query and the caller-owned resources
    connection: duckdb.DuckDBPyConnection,
    binding: HeadBinding,
    query: HeadQuery,
    *,
    strict: bool,
    time_rules: Mapping[str, TimeRules],
    budget: ComputeBudget,
    rehash: bool = False,
) -> RevisionRead:
    """Read the revisions a consumer projects at many cutoffs, verified like ``read_heads``.

    A consumer that decides at many instants (a schedule choosing each decision on the
    calendar known at that decision) reads once and projects with ``project_revisions``.
    The query holds no cutoff: the consumer supplies one per projection. Every filter must
    run on natural-key columns, because a filter on a projected head cannot be applied to
    a revision. In a strict read the times the binding does not grant are returned null,
    exactly as ``read_heads`` treats them.
    """
    import duckdb  # noqa: PLC0415 -- capacity errors at the budgeted query boundary

    if not isinstance(binding, HeadBinding) or not isinstance(query, HeadQuery):
        raise TypeError("read_revisions requires a head binding and a head query")
    if query.cutoff_us is not None or query.grid is not None:
        raise ValueError("a revision read takes its cutoffs at projection and has no grid")
    if strict and query.known_ceiling_us is not None:
        raise ValueError("a known ceiling belongs to a research read only")
    if DOMAIN_VERSIONS[binding.domain] > market_version(connection):
        raise ValueError(f"the {binding.domain} domain needs aas db migrate --to 2")
    domain = binding.domain
    names, flags = _columns(connection, domain)
    try:
        limit_duckdb(connection, budget)
        chains = _verify_pins(connection, binding, time_rules, budget=budget, rehash=rehash)
        params = _generation_params(binding, chains, time_rules, strict=strict)
        pre, post = _filters(binding, query, params)
        if post:
            raise ValueError("a revision read filters natural-key columns only")
        excluded_cte, excluded_join, excluded = _exclusion(binding, flags=flags, params=params)
        selected = ", ".join(f'"{name}"' for name in names)
        sql = f"""
WITH gens AS (
  SELECT unnest($g::VARCHAR[]) AS generation_id, unnest($p::BIGINT[]) AS _pin,
         unnest($s::BIGINT[]) AS _seq, unnest($a::BOOLEAN[]) AS _a_ok,
         unnest($k::BOOLEAN[]) AS _k_ok, unnest($f::DATE[]) AS _from,
         unnest($t::DATE[]) AS _to
){excluded_cte}
SELECT {selected}, _pin, _subject, _excluded, NULL AS _flags FROM (
  SELECT d.* REPLACE (
           CASE WHEN g._a_ok THEN d.available_at_us END AS available_at_us,
           CASE WHEN g._k_ok THEN d.revision_known_at_us END AS revision_known_at_us
         ),
         g._pin, g._seq, g._from, g._to,
         {excluded} AS _excluded, {_SUBJECTS[domain][0]} AS _subject,
         {_DATES[domain][0]} AS _day
  FROM "{domain}" d JOIN gens g ON d.generation_id = g.generation_id {excluded_join}
) WHERE {" AND ".join(pre) or "true"}
ORDER BY _pin, _seq, record_id"""  # noqa: S608 -- code-owned schema and fragments; values are parameters
        projection = _Projection(sql, tuple(names), MappingProxyType(params), domain, strict)
        _admit(connection, projection, query, budget)
        cursor = connection.execute(sql, params)
        columns = [item[0] for item in cursor.description or ()]
        revisions = []
        while batch := cursor.fetchmany(_FETCH_ROWS):
            for fetched in batch:
                row = dict(zip(columns, fetched, strict=True))
                values = {name: row[name] for name in names}
                if values.get(PRICE_FIELDS[0]) == "ohlcv":
                    del values[PRICE_FIELDS[0]]
                revisions.append(
                    Revision(int(row["_pin"]), MappingProxyType(values), bool(row["_excluded"]))
                )
    except duckdb.OutOfMemoryException as error:
        raise ComputeResourceError(
            "DuckDB cannot read revisions within admitted memory limits"
        ) from error
    provenance, present = _provenance(chains, time_rules)
    listed = [
        [item.pin, item.values["record_id"], item.values["revision_id"], item.excluded]
        for item in revisions
    ]
    receipt = {
        "schema": REVISIONS_SCHEMA,
        "binding": binding.document(),
        "binding_hash": binding.binding_hash,
        "query": query.document(),
        "mode": "strict_pit" if strict else "observed_snapshot_research",
        "time_rules": provenance,
        "applied_rules": sorted(present & set(binding.granted_rules)) if strict else [],
        "withheld_rules": sorted(present - set(binding.granted_rules)) if strict else [],
        "rehashed": rehash,
        "revisions": len(listed),
        "revisions_hash": hashlib.sha256(canonical_json_bytes(listed)).hexdigest(),
    }
    return RevisionRead(
        tuple(revisions),
        strict,
        MappingProxyType(receipt),
        hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
    )
