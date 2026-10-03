"""Versioned rules that give day-granular source rows a conservative time of knowledge.

A spec names one rule for each of ``available_at_us`` and ``revision_known_at_us``. A rule
value is an upper bound on when the row became public, never ingestion filling in an
unknown: a rule without a basis yields NULL. Every rule also has a physical base, the
earliest instant the bytes could exist (a session's close, a local midnight).

- A value later than the row's ingestion is lowered to the ingestion time and the row is
  flagged ``time_clamped_to_ingestion``; the ingestion time is still an upper bound.
- A row ingested before its base (a daily bar fetched before the session closed) is held
  and reported, never promoted.
- A ``record`` basis computes from the record's date, so it cannot tell when a correction
  became public. A SUPERSEDE under it takes the ingestion time of the source that carries
  the correction (NULL when the rule has no value); a TOMBSTONE takes the evidence time of
  the snapshot that proves the absence, and NULL under ``unknown_null@1``.
- Strict readers use a rule's times only under a consumer grant of its ``id@version``.

- ``source_column@1`` (input: an instant): the source's own time column; base: the value.
- ``session_close_plus_lag@1`` (input: a date): the pinned session's ``close_at_us`` plus
  ``lag_us``; base: the close.
- ``local_day_end@1`` (input: a date): 23:59:59.999999 local in ``timezone``, flagged
  ``time_precision_day``; base: local midnight.
- ``exdate_open@1`` (input: a date): the pinned session's ``open_at_us``; base: the value.
- ``unknown_null@1`` (no input): NULL; no base.

Local times resolve through DuckDB's ICU time zone data, which the locked DuckDB fixes.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Final, Literal
from zoneinfo import ZoneInfo

from aegis_alpha.storage.market_inputs import GenerationPin

if TYPE_CHECKING:
    from collections.abc import Sequence

TIME_COLUMNS: Final = ("available_at_us", "revision_known_at_us")
CLAMP_FLAG: Final = "time_clamped_to_ingestion"
DAY_FLAG: Final = "time_precision_day"
type Basis = Literal["record", "revision"]
type InputKind = Literal["date", "utc_us"]
_ZONE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)*")
_EPOCH: Final = datetime(1970, 1, 1)  # noqa: DTZ001 -- naive epoch for UTC arithmetic
_MICROSECOND: Final = timedelta(microseconds=1)
_INT64_MAX: Final = 2**63 - 1


@dataclass(frozen=True, slots=True)
class RuleKind:
    rule_id: str
    version: str
    input_kind: InputKind | None
    bases: frozenset[str]
    args: frozenset[str]
    flag: str | None = None

    @property
    def name(self) -> str:
        return f"{self.rule_id}@{self.version}"


SOURCE_COLUMN = RuleKind("source_column", "1", "utc_us", frozenset({"revision"}), frozenset())
SESSION_CLOSE = RuleKind(
    "session_close_plus_lag",
    "1",
    "date",
    frozenset({"record"}),
    frozenset({"calendar", "calendar_id", "venue", "lag_us"}),
)
LOCAL_DAY_END = RuleKind(
    "local_day_end",
    "1",
    "date",
    frozenset({"record", "revision"}),
    frozenset({"timezone"}),
    DAY_FLAG,
)
EXDATE_OPEN = RuleKind(
    "exdate_open",
    "1",
    "date",
    frozenset({"record"}),
    frozenset({"calendar", "calendar_id", "venue"}),
)
UNKNOWN_NULL = RuleKind("unknown_null", "1", None, frozenset({"record", "revision"}), frozenset())
RULES: Final = {
    kind.name: kind
    for kind in (SOURCE_COLUMN, SESSION_CLOSE, LOCAL_DAY_END, EXDATE_OPEN, UNKNOWN_NULL)
}
_PIN_KEYS: Final = frozenset(
    {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash"}
)


@dataclass(frozen=True, slots=True)
class TimeRule:
    """One time column's rule as a spec declares it."""

    kind: RuleKind
    basis: Basis
    input: str | None
    args: Mapping[str, object]

    @property
    def calendar(self) -> GenerationPin | None:
        pin = self.args.get("calendar")
        return pin if isinstance(pin, GenerationPin) else None

    def document(self) -> dict[str, object]:
        """The canonical spec form, used to compare a chain's rules."""
        args = {
            key: (
                {name: getattr(value, name) for name in sorted(_PIN_KEYS)}
                if isinstance(value, GenerationPin)
                else value
            )
            for key, value in self.args.items()
        }
        return {"rule": self.kind.name, "basis": self.basis, "input": self.input, "args": args}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        raise ValueError(f"time rule {name} must be exact nonempty text")
    return value


def _pin(value: object) -> GenerationPin:
    if not isinstance(value, dict) or set(value) != _PIN_KEYS:
        raise ValueError(
            "a calendar pin needs dataset_id, version, generation_id, chain_hash, manifest_hash"
        )
    return GenerationPin(**{key: _text(item, key) for key, item in value.items()})


def _argument(key: str, item: object) -> object:
    if key == "calendar":
        return _pin(item)
    if key == "lag_us":
        if type(item) is not int or not 0 <= item <= _INT64_MAX:
            raise ValueError("lag_us must be a nonnegative int64")
        return item
    if key == "timezone":
        zone = _text(item, key)
        if _ZONE.fullmatch(zone) is None:
            raise ValueError("timezone must be an IANA zone name")
        ZoneInfo(zone)
        return zone
    return _text(item, key)


def parse_rule(column: str, value: object, inputs: Mapping[str, InputKind]) -> TimeRule:
    """Validate one ``time_rules`` entry against the rule registry and the mapper's inputs."""
    if not isinstance(value, dict) or set(value) != {"rule", "basis", "input", "args"}:
        raise ValueError(f"time rule for {column} needs exactly rule, basis, input and args")
    name = value["rule"]
    if name not in RULES:
        raise ValueError(f"unknown time rule {name}")
    kind = RULES[name]
    basis = value["basis"]
    if basis not in kind.bases:
        raise ValueError(f"time rule {name} takes basis {sorted(kind.bases)}")
    source = value["input"]
    if kind.input_kind is None:
        if source is not None:
            raise ValueError(f"time rule {name} takes no input")
    elif not isinstance(source, str) or inputs.get(source) != kind.input_kind:
        raise ValueError(f"time rule {name} needs a mapper {kind.input_kind} input")
    args = value["args"]
    if not isinstance(args, dict) or set(args) != kind.args:
        raise ValueError(f"time rule {name} takes args {sorted(kind.args)}")
    parsed = {key: _argument(key, item) for key, item in args.items()}
    return TimeRule(kind, basis, source, parsed)


@dataclass(frozen=True, slots=True)
class RuleSql:
    """A rule's value and physical base over one row, as SQL expressions."""

    value: str
    base: str


def rule_sql(rule: TimeRule, input_column: str | None, session: tuple[str, str] | None) -> RuleSql:
    """The SQL of ``rule``; ``session`` names the joined session's open and close columns."""
    from aegis_alpha.storage.promotion.formats import sql_literal  # noqa: PLC0415 -- leaf helper

    kind = rule.kind
    if kind is UNKNOWN_NULL or input_column is None:
        return RuleSql("CAST(NULL AS BIGINT)", "CAST(NULL AS BIGINT)")
    if kind is SOURCE_COLUMN:
        return RuleSql(input_column, input_column)
    if kind is LOCAL_DAY_END:
        zone = sql_literal(str(rule.args["timezone"]))
        start = f"epoch_us(timezone({zone}, CAST({input_column} AS TIMESTAMP)))"
        end = (
            f"(epoch_us(timezone({zone}, CAST({input_column} AS TIMESTAMP) + INTERVAL 1 DAY)) - 1)"
        )
        return RuleSql(end, start)
    if session is None:
        raise ValueError(f"time rule {kind.name} needs its pinned calendar")
    opened, closed = session
    if kind is SESSION_CLOSE:
        return RuleSql(f"({closed} + {rule.args['lag_us']})", closed)
    return RuleSql(opened, opened)


# --- Python reference --------------------------------------------------------------------


def _utc_us(moment: datetime) -> int:
    return (moment.astimezone(ZoneInfo("UTC")).replace(tzinfo=None) - _EPOCH) // _MICROSECOND


def local_day_end(day: date, zone: str) -> tuple[int, int]:
    """(value, physical base) of ``local_day_end@1`` with Python's zone data."""
    tz = ZoneInfo(zone)
    start = datetime.combine(day, time(), tz)
    end = datetime.combine(day + timedelta(days=1), time(), tz)
    return _utc_us(end) - 1, _utc_us(start)


def session_close_plus_lag(
    sessions: Mapping[date, int | None], day: date, lag_us: int
) -> tuple[int | None, int | None]:
    close = sessions.get(day)
    return (None, None) if close is None else (close + lag_us, close)


@dataclass(frozen=True, slots=True)
class Bounded:
    """A rule value after the ingestion bound: the stored time, the clamp, and a hold."""

    value: int | None
    clamped: bool
    held: bool


def bound(value: int | None, base: int | None, ingested: int) -> Bounded:
    """Cap a rule value at ingestion, or hold a row ingested before the physical base."""
    if value is None:
        return Bounded(None, clamped=False, held=False)
    if base is not None and ingested < base:
        return Bounded(None, clamped=False, held=True)
    return Bounded(min(value, ingested), clamped=value > ingested, held=False)


def revision_time(  # noqa: PLR0913 -- every input the revision time table names
    op: str,
    *,
    basis: Basis,
    rule: RuleKind,
    bounded: int | None,
    ingested: int,
    evidence: int,
) -> int | None:
    """The stored time of one column for ``op``, after the ingestion bound."""
    if op == "TOMBSTONE":
        return None if rule is UNKNOWN_NULL else evidence
    if op == "SUPERSEDE" and basis == "record":
        return None if bounded is None else ingested
    return bounded


def rule_names(rules: Sequence[TimeRule]) -> tuple[str, ...]:
    return tuple(sorted({rule.kind.name for rule in rules}))
