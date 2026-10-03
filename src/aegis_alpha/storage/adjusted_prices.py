"""Split-adjusted and total return prices derived in the reader from unadjusted bars.

A provider's adjusted price is reference evidence only. A consumer that needs adjusted
prices reads unadjusted bars and the corporate actions known by the same cutoff, and this
module derives the adjustment from them, so an action that was not yet known never reaches
an earlier price (DV-28). ``read_adjusted_prices`` makes the two ``read_heads`` reads with
one query; ``load_adjusted_prices`` is the workspace entry and goes through
``market_inputs.load_pinned_heads`` for both.

The derivation (``aas-adjustment-v1``) works per instrument on the bars read, oldest first:

- Only actions whose ``effective_date`` (the ex-date) is after the instrument's first bar and
  no later than its last bar apply, so the last bar read stays unadjusted and every earlier
  bar is expressed in its terms.
- A ratio action (``split``, ``stock_dividend``, ``capital_adjustment``) with ratio ``r`` new
  shares per old share multiplies every earlier price by ``1/r`` and volume by ``r``.
- Under ``total_return`` a ``dividend`` of ``D`` per share multiplies every earlier price by
  ``(C - D) / C``, where ``C`` is the unadjusted close of the session before the ex-date: the
  dividend is reinvested at that close. That session is the bar read just before the
  ex-date, and it must be ``present``. The derivation always reads every bar in the query's
  dates, grid or not, and returns only the grid's dates; a grid date after that bar and
  before the ex-date is a session with no bar, so the dividend has no close.
- An action that cannot be applied (another action type, a value that is not ``present``, a
  dividend in a currency other than the close it is reinvested at, no close on the session
  before it, or a dividend not below
  that close) leaves every earlier bar ``invalid`` with no values, with the reason
  ``unadjustable_action``. Nothing is skipped silently. ``split_adjusted`` ignores dividends
  entirely.
- An action the cutoff knows but the actions read holds back (``read_heads(held=True)``:
  a time rule the binding does not grant, or evidence with no time) is unadjustable the same
  way under either basis (a held record has no values, so not even its type is read), and
  the earlier bars carry the held record's reason (``ungranted_time_rule`` or
  ``unknown_corporate_actions_evidence``) beside ``unadjustable_action``. An action not yet
  known by the cutoff never applies.
- Factors are exact decimals multiplied under a 50-digit context; each adjusted value is
  rounded to 12 decimals half to even, the precision of the domain's ``DECIMAL(38,12)``.

A result row keeps the bar's record and revision IDs and every domain value, with ``basis``
set to the derived basis and the cumulative price factor beside it. The receipt
(``aas-adjusted-read-v1``) records the basis, the method, the head-read receipts (``prices``
is the read the query asked for, ``series`` the gridless read the derivation used; they are
the same read without a grid), the time rules either read withheld (``withheld_rules``) and a
digest of the derived rows with their factors and reasons.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_left, bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.read_heads import (
    HeadBinding,
    HeadQuery,
    HeadRead,
    HeadRow,
    TimeRules,
    read_heads,
)

if TYPE_CHECKING:
    from datetime import date

    import duckdb

    from aegis_alpha.storage.workspace import Workspace

RECEIPT_SCHEMA: Final = "aas-adjusted-read-v1"
METHOD: Final = "aas-adjustment-v1"
BASES: Final = ("split_adjusted", "total_return")
RATIO_ACTIONS: Final = frozenset({"split", "stock_dividend", "capital_adjustment"})
CASH_ACTIONS: Final = frozenset({"dividend"})
UNADJUSTABLE: Final = "unadjustable_action"
_PRICES: Final = ("open", "high", "low", "close")
_SCALE: Final = Decimal("0.000000000001")
_PRECISION: Final = 50
# One derived row: its dict, the copied values and its factor, beside the row it came from.
_ROW_BYTES: Final = 2048
# What one head-read row or cell holds live while the next read runs: the admission charge
# of a wide price row (read_heads charges about 5.7 KB per KR price row) with room to spare.
_HELD_ROW_BYTES: Final = 6 * 1024


@dataclass(frozen=True, slots=True)
class AdjustedRow:
    """One derived bar: its pin, domain values, cumulative price factor and reasons."""

    pin: int
    values: Mapping[str, object]
    factor: Decimal | None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AdjustedRead:
    rows: tuple[AdjustedRow, ...]
    prices: HeadRead
    series: HeadRead
    actions: HeadRead
    receipt: Mapping[str, object]
    receipt_hash: str
    certified: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class _Step:
    """One applicable action: the bars before ``day`` take ``price`` and ``volume``.

    No ``price`` makes them invalid with ``reasons``.
    """

    day: date
    price: Decimal | None
    volume: Decimal
    reasons: tuple[str, ...] = (UNADJUSTABLE,)


def _decimal(value: object) -> Decimal | None:
    return value if isinstance(value, Decimal) else None


def _dividend(
    bars: Sequence[Mapping[str, object]],
    days: Sequence[date],
    action: Mapping[str, object],
    grid: Sequence[date] | None,
) -> Decimal | None:
    """A dividend's price factor ``(C - D) / C``, or None when it cannot be applied."""
    day = cast("date", action["effective_date"])
    index = bisect_left(days, day) - 1
    bar = bars[index]
    close = _decimal(bar.get("close")) if bar.get("value_state") == "present" else None
    amount = _decimal(action.get("amount"))
    if (
        action.get("value_state") != "present"
        or action.get("currency") != bar.get("currency")
        or close is None
        or amount is None
        or not 0 < amount < close
    ):
        return None
    # A grid date between that bar and the ex-date is a session with no bar.
    if grid is not None:
        after = bisect_right(grid, days[index])
        if after < len(grid) and grid[after] < day:
            return None
    return (close - amount) / close


def _steps(
    bars: Sequence[Mapping[str, object]],
    actions: Sequence[Mapping[str, object]],
    basis: str,
    held: Sequence[tuple[date, tuple[str, ...]]],
    grid: Sequence[date] | None,
) -> list[_Step]:
    """The applicable actions of one instrument as price and volume factors, by ex-date."""
    days = [cast("date", bar["session_date"]) for bar in bars]
    steps = [
        _Step(day, None, Decimal(1), (*reasons, UNADJUSTABLE))
        for day, reasons in held
        if days[0] < day <= days[-1]
    ]
    for action in sorted(actions, key=lambda item: cast("date", item["effective_date"])):
        day = cast("date", action["effective_date"])
        if not days[0] < day <= days[-1]:
            continue
        kind = action["action_type"]
        if kind in RATIO_ACTIONS:
            ratio = _decimal(action.get("ratio"))
            if action.get("value_state") == "present" and ratio is not None and ratio > 0:
                steps.append(_Step(day, 1 / ratio, ratio))
            else:
                steps.append(_Step(day, None, Decimal(1)))
            continue
        if kind in CASH_ACTIONS:
            if basis == "total_return":
                steps.append(_Step(day, _dividend(bars, days, action, grid), Decimal(1)))
            continue
        steps.append(_Step(day, None, Decimal(1)))
    return steps


def _scaled(
    bar: Mapping[str, object], basis: str, price: Decimal | None, volume: Decimal
) -> Mapping[str, object]:
    """One bar's values under the cumulative factors; no price factor makes it invalid."""
    values = dict(bar)
    values["basis"] = basis
    if price is None:
        for name in (*_PRICES, "volume"):
            if name in values:
                values[name] = None
        values["value_state"] = "invalid"
        return MappingProxyType(values)
    for name, factor in (*((name, price) for name in _PRICES), ("volume", volume)):
        found = _decimal(values.get(name))
        if found is not None:
            values[name] = (found * factor).quantize(_SCALE, ROUND_HALF_EVEN)
    return MappingProxyType(values)


def adjust(
    bars: Sequence[Mapping[str, object]],
    actions: Sequence[Mapping[str, object]],
    basis: str,
    *,
    held: Sequence[tuple[date, tuple[str, ...]]] = (),
    grid: Sequence[date] | None = None,
) -> list[tuple[Mapping[str, object], Decimal | None, tuple[str, ...]]]:
    """Derive ``basis`` prices for one instrument's unadjusted bars from its actions.

    ``bars`` are one instrument's unadjusted bars, one per session date, with no session
    left out; ``actions`` are its corporate actions and ``held`` the ex-dates and reasons of
    the actions its read held back. ``grid``, when given, is the increasing list of dates
    the bars should cover. Returns, oldest first, each bar's adjusted values, the cumulative
    price factor (None when an unadjustable action follows the bar) and its reasons.
    """
    if basis not in BASES:
        raise ValueError(f"adjusted basis must be one of {list(BASES)}")
    ordered = sorted(bars, key=lambda bar: cast("date", bar["session_date"]))
    if not ordered:
        return []
    days = [bar["session_date"] for bar in ordered]
    if len(set(days)) != len(days):
        raise ValueError("an instrument has two bars on one session date")
    if any(bar.get("basis") != "unadjusted" for bar in ordered):
        raise ValueError("adjusted prices derive from unadjusted bars only")
    with localcontext() as context:
        context.prec = _PRECISION
        steps = _steps(ordered, actions, basis, held, grid)
        derived: list[tuple[Mapping[str, object], Decimal | None, tuple[str, ...]]] = []
        price: Decimal | None = Decimal(1)
        volume = Decimal(1)
        reasons: tuple[str, ...] = ()
        pending = sorted(steps, key=lambda step: step.day, reverse=True)
        for bar in reversed(ordered):
            day = cast("date", bar["session_date"])
            while pending and pending[0].day > day:
                step = pending.pop(0)
                price = None if price is None or step.price is None else price * step.price
                volume *= step.volume
                if step.price is None:
                    reasons = tuple(dict.fromkeys((*reasons, *step.reasons)))
            derived.append((_scaled(bar, basis, price, volume), price, reasons))
    derived.reverse()
    return derived


def _derive(
    series: HeadRead, actions: HeadRead, query: HeadQuery, basis: str, budget: ComputeBudget
) -> list[AdjustedRow]:
    if basis not in BASES:
        raise ValueError(f"adjusted basis must be one of {list(BASES)}")
    count = len(series.rows) + len(actions.rows) + len(actions.held)
    estimated = 64 * 1024 + _ROW_BYTES * count
    if estimated > budget.available_bytes:
        raise ComputeResourceError(
            f"adjusted price memory estimate {estimated} exceeds admitted "
            f"materialization budget {budget.available_bytes} bytes"
        )
    # One instrument is one series across every cutover pin; each bar keeps its own pin.
    bars: dict[str, list[HeadRow]] = {}
    for row in series.rows:
        bars.setdefault(str(row.values["instrument_id"]), []).append(row)
    events: dict[str, list[Mapping[str, object]]] = {}
    for row in actions.rows:
        events.setdefault(str(row.values["instrument_id"]), []).append(row.values)
    held: dict[str, list[tuple[date, tuple[str, ...]]]] = {}
    for cell in actions.held:
        held.setdefault(cell.instrument_id, []).append(
            (cast("date", cell.session_date), cell.reasons)
        )
    grid = None if query.grid is None else set(query.grid)
    rows = []
    for instrument, found in sorted(bars.items()):
        found.sort(key=lambda row: cast("date", row.values["session_date"]))
        derived = adjust(
            [row.values for row in found],
            events.get(instrument, []),
            basis,
            held=held.get(instrument, ()),
            grid=query.grid,
        )
        for row, (values, factor, reasons) in zip(found, derived, strict=True):
            if grid is None or row.values["session_date"] in grid:
                rows.append(AdjustedRow(row.pin, values, factor, reasons))
    rows.sort(key=lambda row: (row.pin, str(row.values["record_id"])))
    return rows


def _result(
    prices: HeadRead, series: HeadRead, actions: HeadRead, basis: str, rows: list[AdjustedRow]
) -> AdjustedRead:
    digest = [
        [
            row.pin,
            row.values["record_id"],
            row.values["revision_id"],
            None if row.factor is None else str(row.factor),
            list(row.reasons),
        ]
        for row in rows
    ]
    withheld = {
        str(rule)
        for read in (series, actions)
        for rule in cast("list[object]", read.receipt["withheld_rules"])
    }
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "method": METHOD,
        "basis": basis,
        "prices": dict(prices.receipt),
        "prices_hash": prices.receipt_hash,
        "series_hash": series.receipt_hash,
        "actions": dict(actions.receipt),
        "actions_hash": actions.receipt_hash,
        "withheld_rules": sorted(withheld),
        "rows": len(rows),
        "rows_hash": hashlib.sha256(canonical_json_bytes(digest)).hexdigest(),
    }
    return AdjustedRead(
        tuple(rows),
        prices,
        series,
        actions,
        MappingProxyType(receipt),
        hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
    )


def actions_query(query: HeadQuery) -> HeadQuery:
    """The actions read of an adjusted read: the same cutoffs, subjects and dates."""
    return HeadQuery(
        cutoff_us=query.cutoff_us,
        ingestion_cutoff_us=query.ingestion_cutoff_us,
        known_ceiling_us=query.known_ceiling_us,
        subjects=query.subjects,
        from_date=query.from_date,
        to_date=query.to_date,
    )


def _retaining(budget: ComputeBudget, read: HeadRead) -> ComputeBudget:
    """``budget`` with ``read`` reserved, so a later read or derivation never charges its bytes."""
    cells = len(read.coverage.cells) if read.coverage is not None else 0
    held = _HELD_ROW_BYTES * (len(read.rows) + len(read.held) + cells)
    return replace(budget, reserved_bytes=budget.reserved_bytes + held)


def series_query(query: HeadQuery) -> HeadQuery:
    """The price read the derivation uses: the query without its grid, so no bar is missed."""
    return query if query.grid is None else replace(query, grid=None)


def _check(prices: HeadBinding, actions: HeadBinding) -> None:
    if prices.domain != "prices" or actions.domain != "corporate_actions":
        raise ValueError("an adjusted read binds prices and corporate_actions")


def read_adjusted_prices(  # noqa: PLR0913 -- two bindings, the query and caller-owned resources
    connection: duckdb.DuckDBPyConnection,
    prices: HeadBinding,
    actions: HeadBinding,
    query: HeadQuery,
    *,
    basis: str,
    time_rules: Mapping[str, TimeRules],
    budget: ComputeBudget,
    rehash: bool = False,
) -> AdjustedRead:
    """Read unadjusted bars and the actions known by the same cutoff, then derive ``basis``.

    ``time_rules`` covers every generation of both bindings, as for ``read_heads``. Each
    read's rows stay reserved in ``budget`` while the next read and the derivation run.
    """
    _check(prices, actions)

    def read(
        binding: HeadBinding, part: HeadQuery, allowed: ComputeBudget, *, held: bool = False
    ) -> HeadRead:
        return read_heads(
            connection,
            binding,
            part,
            time_rules=time_rules,
            budget=allowed,
            rehash=rehash,
            held=held,
        )

    series = read(prices, series_query(query), budget)
    budget = _retaining(budget, series)
    price_read = series
    if query.grid is not None:
        price_read = read(prices, query, budget)
        budget = _retaining(budget, price_read)
    action_read = read(actions, actions_query(query), budget, held=True)
    rows = _derive(series, action_read, query, basis, _retaining(budget, action_read))
    return _result(price_read, series, action_read, basis, rows)


def load_adjusted_prices(  # noqa: PLR0913 -- two bindings, the query and caller-owned resources
    workspace: Workspace,
    prices: HeadBinding,
    actions: HeadBinding,
    query: HeadQuery,
    *,
    basis: str,
    budget: ComputeBudget,
    rehash: bool = False,
) -> AdjustedRead:
    """``read_adjusted_prices`` under workspace admission, with retained time-rule provenance."""
    from aegis_alpha.storage.market_inputs import load_pinned_heads  # noqa: PLC0415

    _check(prices, actions)

    def read(
        binding: HeadBinding, part: HeadQuery, allowed: ComputeBudget, *, held: bool = False
    ) -> HeadRead:
        return load_pinned_heads(workspace, binding, part, budget=allowed, rehash=rehash, held=held)

    series = read(prices, series_query(query), budget)
    budget = _retaining(budget, series)
    price_read = series
    if query.grid is not None:
        price_read = read(prices, query, budget)
        budget = _retaining(budget, price_read)
    action_read = read(actions, actions_query(query), budget, held=True)
    rows = _derive(series, action_read, query, basis, _retaining(budget, action_read))
    return _result(price_read, series, action_read, basis, rows)
