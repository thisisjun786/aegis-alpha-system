"""Versioned source-selection rules: one canonical source per data kind and date interval.

A ledger dataset holds one provider and stays immutable; choosing between providers is a
read rule. A rule (``select.<kind>@<version>``) names a market domain and an ordered list
of ``[from, to)`` segments, each served by exactly one dataset. A request pins the rule by
name and version and one exact generation per dataset (``Selection``); the rule becomes an
``aas-head-binding-v1`` binding, so every date belongs to exactly one dataset, a date whose
selected dataset has no head stays missing, and nothing falls back to another provider.

- A registered rule never changes. A new cutover or source is a new version, so a stored
  run that recorded ``select.prices.us@1`` reads the same rows forever. Each rule's
  document (``aas-source-selection-rule-v1``) hashes to a frozen digest in its tests.
- A price rule may name witnesses: datasets read over the interval where they lose to the
  selected source, only to surface the quality flags the rule lists (the promotion's
  ``cross_provider_mismatch``) on the selected bar of the same instrument, session,
  interval, basis and role. A witness value is never served.
- Adjusted prices are never stored or stitched from provider-adjusted series:
  ``read_selected_adjusted`` derives them at the read's cutoff from the selected
  unadjusted bars and the selected corporate actions (``adjusted_prices``,
  ``aas-adjustment-v1``).
- Every read returns, per row, the dataset that served it, and a receipt naming the rule,
  its hash, the binding and the underlying head-read receipts.
- ``overlap_close_differences`` and ``seam_factor_continuity`` are read-only audit helpers
  for the seam checks of the design notes: the close-difference distribution where a
  witness overlaps the selected source, and whether the derived adjustment factor is
  continuous across each cutover.
"""

from __future__ import annotations

import hashlib
import re
from bisect import bisect_left
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from itertools import pairwise
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.adjusted_prices import (
    AdjustedRead,
    load_adjusted_prices,
    read_adjusted_prices,
)
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.market_schema import DOMAINS
from aegis_alpha.storage.read_heads import (
    HeadBinding,
    HeadPin,
    HeadQuery,
    HeadRead,
    QualityFlag,
    TimeRules,
    read_heads,
)

if TYPE_CHECKING:
    import duckdb

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

RULE_SCHEMA: Final = "aas-source-selection-rule-v1"
SELECTION_SCHEMA: Final = "aas-source-selection-v1"
READ_SCHEMA: Final = "aas-source-selection-read-v1"
ADJUSTED_SCHEMA: Final = "aas-source-selection-adjusted-v1"
_NAME: Final = re.compile(r"select\.[a-z0-9_]+(?:\.[a-z0-9_]+)*")
_REF: Final = re.compile(r"(select\.[a-z0-9_]+(?:\.[a-z0-9_]+)*)@([1-9][0-9]*)")
_FLAG: Final = re.compile(r"[a-z][a-z0-9_]*")
# A witness flag lands on the selected bar with the same values in these columns.
_PRICE_CELL: Final = ("instrument_id", "session_date", "interval", "basis", "price_role")
# What one witness row holds live while the selected read runs (as adjusted_prices charges).
_HELD_ROW_BYTES: Final = 6 * 1024


@dataclass(frozen=True, slots=True)
class SelectionSegment:
    """One dataset serving the dates ``[from_date, to_date)``; None leaves a side open."""

    dataset_id: str
    from_date: date | None = None
    to_date: date | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError("selection segment requires a dataset ID")
        for bound in (self.from_date, self.to_date):
            if bound is not None and type(bound) is not date:
                raise TypeError("selection bounds must be dates")
        if (
            self.from_date is not None
            and self.to_date is not None
            and not self.from_date < self.to_date
        ):
            raise ValueError("selection segment must be nonempty")

    def document(self) -> dict[str, object]:
        return {"dataset_id": self.dataset_id, "from": self.from_date, "to": self.to_date}


def _check_segments(
    segments: tuple[SelectionSegment, ...], witnesses: tuple[SelectionSegment, ...]
) -> None:
    for values in (segments, witnesses):
        if type(values) is not tuple or any(
            not isinstance(item, SelectionSegment) for item in values
        ):
            raise TypeError("selection segments must be a tuple of segments")
    if not segments:
        raise ValueError("selection rule requires a segment")
    for previous, current in pairwise(segments):
        if previous.to_date is None or previous.to_date != current.from_date:
            raise ValueError("selection segments must be ordered and contiguous")


def _flags(values: object) -> tuple[str, ...]:
    if type(values) is not tuple or any(
        not isinstance(flag, str) or _FLAG.fullmatch(flag) is None for flag in values
    ):
        raise ValueError("surfaced flags must be lowercase identifiers")
    flags = cast("tuple[str, ...]", values)
    if len(set(flags)) != len(flags):
        raise ValueError("surfaced flags must be unique")
    return tuple(sorted(flags))


@dataclass(frozen=True, slots=True)
class SelectionRule:
    """A named, versioned rule: ordered contiguous segments and optional price witnesses."""

    name: str
    version: int
    domain: str
    segments: tuple[SelectionSegment, ...]
    witnesses: tuple[SelectionSegment, ...] = ()
    surfaced_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _NAME.fullmatch(self.name) is None:
            raise ValueError("selection rule name must be select.<kind>")
        if type(self.version) is not int or self.version < 1:
            raise ValueError("selection rule version must be a positive integer")
        if self.domain not in DOMAINS:
            raise ValueError("unknown market domain")
        _check_segments(self.segments, self.witnesses)
        if self.witnesses and self.domain != "prices":
            raise ValueError("only a price rule may name witnesses")
        if bool(self.witnesses) != bool(self.surfaced_flags):
            raise ValueError("witnesses and surfaced flags come together")
        object.__setattr__(self, "surfaced_flags", _flags(self.surfaced_flags))

    @property
    def ref(self) -> str:
        return f"{self.name}@{self.version}"

    @property
    def datasets(self) -> tuple[str, ...]:
        """Every dataset the rule reads, segments first, each once."""
        return tuple(dict.fromkeys(item.dataset_id for item in (*self.segments, *self.witnesses)))

    def document(self) -> dict[str, object]:
        return {
            "schema": RULE_SCHEMA,
            "rule": self.ref,
            "domain": self.domain,
            "segments": [item.document() for item in self.segments],
            "witnesses": [item.document() for item in self.witnesses],
            "surfaced_flags": list(self.surfaced_flags),
        }

    @property
    def rule_hash(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.document())).hexdigest()


def _register(*rules: SelectionRule) -> Mapping[str, SelectionRule]:
    registry: dict[str, SelectionRule] = {}
    for rule in rules:
        if rule.ref in registry:
            raise ValueError(f"selection rule {rule.ref} is registered twice")
        registry[rule.ref] = rule
    return MappingProxyType(registry)


# The design notes' source table (dev-notes/design/data-vertical.md, "버전 있는 원천 선택
# 규칙"). Never edit a registered rule; add the next version beside it.
RULES: Final = _register(
    SelectionRule(
        "select.prices.us",
        1,
        "prices",
        (
            SelectionSegment("prices.us.norgate", None, date(2026, 9, 9)),
            SelectionSegment("prices.us.eodhd", date(2026, 9, 9), None),
        ),
        witnesses=(SelectionSegment("prices.us.eodhd", None, date(2026, 9, 9)),),
        surfaced_flags=("cross_provider_mismatch",),
    ),
    SelectionRule(
        "select.actions.us",
        1,
        "corporate_actions",
        (
            SelectionSegment("actions.us.norgate", None, date(2026, 7, 28)),
            SelectionSegment("actions.us.eodhd", date(2026, 7, 28), None),
        ),
    ),
    SelectionRule("select.prices.kr", 1, "prices", (SelectionSegment("prices.kr.eodhd"),)),
    SelectionRule(
        "select.actions.kr", 1, "corporate_actions", (SelectionSegment("actions.kr.eodhd"),)
    ),
    SelectionRule(
        "select.fx.usdkrw",
        1,
        "fx_rates",
        (
            SelectionSegment("fx.usdkrw.norgate", None, date(2026, 9, 10)),
            SelectionSegment("fx.usdkrw.eodhd", date(2026, 9, 10), None),
        ),
    ),
)


def selection_rule(ref: str) -> SelectionRule:
    """The registered rule ``name@version``; there is no latest-version default."""
    if not isinstance(ref, str) or _REF.fullmatch(ref) is None:
        raise ValueError("selection rule reference must be an exact name@version")
    try:
        return RULES[ref]
    except KeyError:
        raise KeyError(f"unregistered source-selection rule {ref}") from None


@dataclass(frozen=True, slots=True)
class Selection:
    """A rule pinned to one exact generation per dataset, with the binding's grants."""

    rule: SelectionRule
    pins: tuple[GenerationPin, ...]
    granted_rules: tuple[str, ...] = ()
    excluded_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.rule, SelectionRule):
            raise TypeError("selection requires a selection rule")
        if type(self.pins) is not tuple or any(
            not isinstance(pin, GenerationPin) for pin in self.pins
        ):
            raise TypeError("selection pins must be a tuple of generation pins")
        datasets = self.rule.datasets
        for pin in self.pins:
            if pin.dataset_id not in datasets:
                raise ValueError(f"pin dataset {pin.dataset_id} is not in rule {self.rule.ref}")
        named = [pin.dataset_id for pin in self.pins]
        if sorted(named) != sorted(datasets):
            raise ValueError(f"rule {self.rule.ref} needs exactly one pin per dataset")
        # Validates grants and exclusions the way every binding does.
        object.__setattr__(self, "granted_rules", self.binding.granted_rules)
        object.__setattr__(self, "excluded_flags", self.binding.excluded_flags)

    def _pin(self, dataset_id: str) -> GenerationPin:
        return next(pin for pin in self.pins if pin.dataset_id == dataset_id)

    @property
    def binding(self) -> HeadBinding:
        return HeadBinding(
            self.rule.domain,
            tuple(
                HeadPin(self._pin(item.dataset_id), item.from_date, item.to_date)
                for item in self.rule.segments
            ),
            tuple(self.granted_rules),
            tuple(self.excluded_flags),
        )

    @property
    def witness_bindings(self) -> tuple[HeadBinding, ...]:
        # No exclusions: an excluded revision would hide the very flag a witness reports.
        return tuple(
            HeadBinding(
                self.rule.domain,
                (HeadPin(self._pin(item.dataset_id), item.from_date, item.to_date),),
                tuple(self.granted_rules),
            )
            for item in self.rule.witnesses
        )

    def source(self, pin: int) -> str:
        """The dataset that serves the binding's pin ordinal ``pin``."""
        return self.rule.segments[pin].dataset_id

    def document(self) -> dict[str, object]:
        binding = self.binding
        return {
            "schema": SELECTION_SCHEMA,
            "rule": self.rule.ref,
            "rule_hash": self.rule.rule_hash,
            "binding": binding.document(),
            "binding_hash": binding.binding_hash,
        }


@dataclass(frozen=True, slots=True)
class SelectedRow:
    """One selected head, the dataset that served it and the witness flags on its cell."""

    pin: int
    source: str
    values: Mapping[str, object]
    flags: tuple[QualityFlag, ...] = ()
    witness_flags: tuple[tuple[str, QualityFlag], ...] = ()


@dataclass(frozen=True, slots=True)
class SelectedRead:
    rows: tuple[SelectedRow, ...]
    selection: Selection
    heads: HeadRead
    witnesses: tuple[HeadRead, ...]
    receipt: Mapping[str, object]
    receipt_hash: str
    certified: bool = field(default=False, init=False)


type _Reader = Callable[[HeadBinding, HeadQuery, ComputeBudget], HeadRead]


def _cell(values: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(values.get(name) for name in _PRICE_CELL)


def _retaining(budget: ComputeBudget, read: HeadRead) -> ComputeBudget:
    held = _HELD_ROW_BYTES * len(read.rows)
    return replace(budget, reserved_bytes=budget.reserved_bytes + held)


def _flag_document(item: tuple[str, QualityFlag]) -> list[object]:
    dataset, flag = item
    return [dataset, flag.rule_id, flag.rule_version, flag.flag, flag.detail]


def _select(
    read: _Reader, selection: Selection, query: HeadQuery, budget: ComputeBudget
) -> SelectedRead:
    # A witness is read over every date the query asks, without the coverage grid.
    witness_query = query if query.grid is None else replace(query, grid=None)
    witnesses = []
    found: dict[tuple[object, ...], list[tuple[str, QualityFlag]]] = {}
    surfaced = set(selection.rule.surfaced_flags)
    for binding in selection.witness_bindings:
        witness = read(binding, witness_query, budget)
        budget = _retaining(budget, witness)
        witnesses.append(witness)
        dataset = binding.pins[0].pin.dataset_id
        for row in witness.rows:
            for flag in row.flags:
                if flag.flag in surfaced:
                    found.setdefault(_cell(row.values), []).append((dataset, flag))
    heads = read(selection.binding, query, budget)
    rows = tuple(
        SelectedRow(
            row.pin,
            selection.source(row.pin),
            row.values,
            row.flags,
            tuple(found.get(_cell(row.values), ())) if found else (),
        )
        for row in heads.rows
    )
    digest = [
        [
            row.pin,
            row.source,
            row.values["record_id"],
            row.values["revision_id"],
            [_flag_document(item) for item in row.witness_flags],
        ]
        for row in rows
    ]
    receipt = {
        "schema": READ_SCHEMA,
        "rule": selection.rule.ref,
        "rule_hash": selection.rule.rule_hash,
        "selection": selection.document(),
        "heads_hash": heads.receipt_hash,
        "witness_hashes": [witness.receipt_hash for witness in witnesses],
        "rows": len(rows),
        "rows_hash": hashlib.sha256(canonical_json_bytes(digest)).hexdigest(),
    }
    return SelectedRead(
        rows,
        selection,
        heads,
        tuple(witnesses),
        MappingProxyType(receipt),
        hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
    )


def read_selected(  # noqa: PLR0913 -- selection, query and the caller-owned resources
    connection: duckdb.DuckDBPyConnection,
    selection: Selection,
    query: HeadQuery,
    *,
    time_rules: Mapping[str, TimeRules],
    budget: ComputeBudget,
    rehash: bool = False,
) -> SelectedRead:
    """Read the rule's selected heads, each row with the dataset that served it.

    ``time_rules`` covers every generation of every pinned dataset, as for ``read_heads``.
    """

    def read(binding: HeadBinding, part: HeadQuery, allowed: ComputeBudget) -> HeadRead:
        return read_heads(
            connection, binding, part, time_rules=time_rules, budget=allowed, rehash=rehash
        )

    return _select(read, selection, query, budget)


def load_selected(
    workspace: Workspace,
    selection: Selection,
    query: HeadQuery,
    *,
    budget: ComputeBudget,
    rehash: bool = False,
) -> SelectedRead:
    """``read_selected`` under workspace admission, with retained time-rule provenance."""
    from aegis_alpha.storage.market_inputs import load_pinned_heads  # noqa: PLC0415

    def read(binding: HeadBinding, part: HeadQuery, allowed: ComputeBudget) -> HeadRead:
        return load_pinned_heads(workspace, binding, part, budget=allowed, rehash=rehash)

    return _select(read, selection, query, budget)


@dataclass(frozen=True, slots=True)
class SelectedAdjustedRow:
    """One derived bar, the dataset its unadjusted bar came from, its factor and reasons."""

    pin: int
    source: str
    values: Mapping[str, object]
    factor: Decimal | None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SelectedAdjustedRead:
    rows: tuple[SelectedAdjustedRow, ...]
    prices: Selection
    actions: Selection
    adjusted: AdjustedRead
    receipt: Mapping[str, object]
    receipt_hash: str
    certified: bool = field(default=False, init=False)


def _check_adjusted(prices: Selection, actions: Selection) -> None:
    if prices.rule.domain != "prices" or actions.rule.domain != "corporate_actions":
        raise ValueError("an adjusted selection pairs a price rule with an action rule")


def _adjusted_result(
    prices: Selection, actions: Selection, adjusted: AdjustedRead
) -> SelectedAdjustedRead:
    rows = tuple(
        SelectedAdjustedRow(row.pin, prices.source(row.pin), row.values, row.factor, row.reasons)
        for row in adjusted.rows
    )
    sources = [[row.pin, row.source, row.values["record_id"]] for row in rows]
    receipt = {
        "schema": ADJUSTED_SCHEMA,
        "prices_rule": prices.rule.ref,
        "prices_rule_hash": prices.rule.rule_hash,
        "actions_rule": actions.rule.ref,
        "actions_rule_hash": actions.rule.rule_hash,
        "prices_selection": prices.document(),
        "actions_selection": actions.document(),
        "adjusted_hash": adjusted.receipt_hash,
        "rows": len(rows),
        "sources_hash": hashlib.sha256(canonical_json_bytes(sources)).hexdigest(),
    }
    return SelectedAdjustedRead(
        rows,
        prices,
        actions,
        adjusted,
        MappingProxyType(receipt),
        hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
    )


def read_selected_adjusted(  # noqa: PLR0913 -- two selections, the query and resources
    connection: duckdb.DuckDBPyConnection,
    prices: Selection,
    actions: Selection,
    query: HeadQuery,
    *,
    basis: str,
    time_rules: Mapping[str, TimeRules],
    budget: ComputeBudget,
    rehash: bool = False,
) -> SelectedAdjustedRead:
    """Derive ``basis`` prices at the query's cutoff from the selected bars and actions."""
    _check_adjusted(prices, actions)
    adjusted = read_adjusted_prices(
        connection,
        prices.binding,
        actions.binding,
        query,
        basis=basis,
        time_rules=time_rules,
        budget=budget,
        rehash=rehash,
    )
    return _adjusted_result(prices, actions, adjusted)


def load_selected_adjusted(  # noqa: PLR0913 -- two selections, the query and resources
    workspace: Workspace,
    prices: Selection,
    actions: Selection,
    query: HeadQuery,
    *,
    basis: str,
    budget: ComputeBudget,
    rehash: bool = False,
) -> SelectedAdjustedRead:
    """``read_selected_adjusted`` under workspace admission."""
    _check_adjusted(prices, actions)
    adjusted = load_adjusted_prices(
        workspace,
        prices.binding,
        actions.binding,
        query,
        basis=basis,
        budget=budget,
        rehash=rehash,
    )
    return _adjusted_result(prices, actions, adjusted)


@dataclass(frozen=True, slots=True)
class OverlapReport:
    """Close differences where a witness and the selected source both hold a bar.

    ``relative`` differences are ``|witness - selected| / |selected|``; ``quantiles`` are
    nearest-rank at 50, 90 and 99 percent. ``witness_only`` counts witness bars whose
    cell the selected source does not hold (they are missing, never filled).
    """

    pairs: int
    flagged: int
    over_tolerance: int
    max_abs_relative: Decimal | None
    quantiles: tuple[tuple[str, Decimal], ...]
    witness_only: int
    sessions: tuple[tuple[date, int], ...]


def _close(values: Mapping[str, object]) -> Decimal | None:
    close = values.get("close")
    if values.get("value_state") != "present" or not isinstance(close, Decimal):
        return None
    return close


def overlap_close_differences(read: SelectedRead, *, tolerance: Decimal) -> OverlapReport:
    """Summarize the witness overlap of a selected price read (design seam check 1)."""
    if read.selection.rule.domain != "prices":
        raise ValueError("close differences need a price selection")
    selected = {_cell(row.values): row for row in read.rows}
    relative: list[Decimal] = []
    flagged = witness_only = 0
    sessions: dict[date, int] = {}
    for witness in read.witnesses:
        for row in witness.rows:
            chosen = selected.get(_cell(row.values))
            if chosen is None:
                witness_only += 1
                continue
            mine, theirs = _close(chosen.values), _close(row.values)
            if mine is None or theirs is None or mine == 0:
                continue
            relative.append(abs(theirs - mine) / abs(mine))
            day = cast("date", chosen.values["session_date"])
            sessions[day] = sessions.get(day, 0) + 1
            flagged += bool(chosen.witness_flags)
    ordered = sorted(relative)
    quantiles = tuple(
        (label, ordered[max(0, -(-len(ordered) * percent // 100) - 1)])
        for label, percent in (("p50", 50), ("p90", 90), ("p99", 99))
        if ordered
    )
    return OverlapReport(
        len(ordered),
        flagged,
        sum(value > tolerance for value in ordered),
        ordered[-1] if ordered else None,
        quantiles,
        witness_only,
        tuple(sorted(sessions.items())),
    )


@dataclass(frozen=True, slots=True)
class SeamCheck:
    """One instrument's derived factor across one cutover (design seam check 2).

    ``factor_step`` is the last bar's factor before the cutover over the first bar's at or
    after it; ``actions_between`` counts the selected actions effective in between. A step
    of 1 is ``continuous``, a step with an action between is ``explained_step``, any other
    step is ``unexplained_step``, and a bar without a factor is ``unknown_factor``.
    ``price_jump`` says the adjusted close moved by more than the tolerance across it.
    """

    instrument_id: str
    boundary: date
    before: tuple[date, str]
    after: tuple[date, str]
    factor_before: Decimal | None
    factor_after: Decimal | None
    factor_step: Decimal | None
    actions_between: int
    adjusted_return: Decimal | None
    price_jump: bool
    status: str


def _status(step: Decimal | None, actions: int) -> str:
    if step is None:
        return "unknown_factor"
    if step == 1:
        return "continuous"
    return "explained_step" if actions else "unexplained_step"


def seam_factor_continuity(
    read: SelectedAdjustedRead, *, tolerance: Decimal
) -> tuple[SeamCheck, ...]:
    """Check each price-rule cutover for every instrument with bars on both sides."""
    boundaries = [cast("date", item.from_date) for item in read.prices.rule.segments[1:]]
    series: dict[str, list[SelectedAdjustedRow]] = {}
    for row in read.rows:
        series.setdefault(str(row.values["instrument_id"]), []).append(row)
    effective: dict[str, list[date]] = {}
    for action in read.adjusted.actions.rows:
        effective.setdefault(str(action.values["instrument_id"]), []).append(
            cast("date", action.values["effective_date"])
        )
    checks = []
    for instrument, rows in sorted(series.items()):
        rows.sort(key=lambda row: cast("date", row.values["session_date"]))
        days = [cast("date", row.values["session_date"]) for row in rows]
        for boundary in boundaries:
            index = bisect_left(days, boundary)
            if index == 0 or index == len(rows):
                continue
            before, after = rows[index - 1], rows[index]
            step = (
                None
                if before.factor is None or after.factor is None or after.factor == 0
                else before.factor / after.factor
            )
            between = sum(
                days[index - 1] < day <= days[index] for day in effective.get(instrument, ())
            )
            low, high = _close(before.values), _close(after.values)
            moved = None if low is None or high is None or low == 0 else high / low - 1
            checks.append(
                SeamCheck(
                    instrument,
                    boundary,
                    (days[index - 1], before.source),
                    (days[index], after.source),
                    before.factor,
                    after.factor,
                    step,
                    between,
                    moved,
                    moved is not None and abs(moved) > tolerance,
                    _status(step, between),
                )
            )
    return tuple(checks)
