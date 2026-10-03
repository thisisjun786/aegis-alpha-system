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
  ``(C - D) / C``, where ``C`` is the last present unadjusted close before the ex-date: the
  dividend is reinvested at that close.
- An action that cannot be applied (another action type, a value that is not ``present``, a
  dividend in another currency, no earlier close, or a dividend not below it) leaves every
  earlier bar ``invalid`` with no values, with the reason ``unadjustable_action``. Nothing is
  skipped silently. ``split_adjusted`` ignores dividends entirely.
- Factors are exact decimals multiplied under a 50-digit context; each adjusted value is
  rounded to 12 decimals half to even, the precision of the domain's ``DECIMAL(38,12)``.

A result row keeps the bar's record and revision IDs and every domain value, with ``basis``
set to the derived basis and the cumulative price factor beside it. The receipt
(``aas-adjusted-read-v1``) records the basis, the method, both head-read receipts and a digest
of the derived rows.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
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
    actions: HeadRead
    receipt: Mapping[str, object]
    receipt_hash: str
    certified: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class _Step:
    """One applicable action: the bars before ``day`` take ``price`` and ``volume``."""

    day: date
    price: Decimal | None
    volume: Decimal


def _decimal(value: object) -> Decimal | None:
    return value if isinstance(value, Decimal) else None


def _steps(
    bars: Sequence[Mapping[str, object]], actions: Sequence[Mapping[str, object]], basis: str
) -> list[_Step]:
    """The applicable actions of one instrument as price and volume factors, by ex-date."""
    days = [cast("date", bar["session_date"]) for bar in bars]
    closes = [
        (day, _decimal(bar.get("close")))
        for day, bar in zip(days, bars, strict=True)
        if bar.get("value_state") == "present" and _decimal(bar.get("close"))
    ]
    close_days = [day for day, _ in closes]
    currency = bars[-1].get("currency")
    steps = []
    for action in sorted(actions, key=lambda item: cast("date", item["effective_date"])):
        day = cast("date", action["effective_date"])
        if not days[0] < day <= days[-1]:
            continue
        kind = action["action_type"]
        present = action.get("value_state") == "present"
        if kind in RATIO_ACTIONS:
            ratio = _decimal(action.get("ratio"))
            if present and ratio is not None and ratio > 0:
                steps.append(_Step(day, 1 / ratio, ratio))
            else:
                steps.append(_Step(day, None, Decimal(1)))
            continue
        if kind in CASH_ACTIONS:
            if basis != "total_return":
                continue
            amount = _decimal(action.get("amount"))
            index = bisect_left(close_days, day) - 1
            close = closes[index][1] if index >= 0 else None
            usable = (
                present
                and action.get("currency") == currency
                and amount is not None
                and close is not None
                and 0 < amount < close
            )
            factor = None
            if usable and close is not None and amount is not None:
                factor = (close - amount) / close
            steps.append(_Step(day, factor, Decimal(1)))
            continue
        steps.append(_Step(day, None, Decimal(1)))
    return steps


def _scaled(
    bar: Mapping[str, object], basis: str, price: Decimal | None, volume: Decimal
) -> tuple[Mapping[str, object], tuple[str, ...]]:
    """One bar's values under the cumulative factors; no price factor makes it invalid."""
    values = dict(bar)
    values["basis"] = basis
    if price is None:
        for name in (*_PRICES, "volume"):
            if name in values:
                values[name] = None
        values["value_state"] = "invalid"
        return MappingProxyType(values), (UNADJUSTABLE,)
    for name, factor in (*((name, price) for name in _PRICES), ("volume", volume)):
        found = _decimal(values.get(name))
        if found is not None:
            values[name] = (found * factor).quantize(_SCALE, ROUND_HALF_EVEN)
    return MappingProxyType(values), ()


def adjust(
    bars: Sequence[Mapping[str, object]],
    actions: Sequence[Mapping[str, object]],
    basis: str,
) -> list[tuple[Mapping[str, object], Decimal | None, tuple[str, ...]]]:
    """Derive ``basis`` prices for one instrument's unadjusted bars from its actions.

    ``bars`` are one instrument's unadjusted bars, one per session date; ``actions`` are its
    corporate actions. Returns, oldest first, each bar's adjusted values, the cumulative
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
        steps = _steps(ordered, actions, basis)
        derived: list[tuple[Mapping[str, object], Decimal | None, tuple[str, ...]]] = []
        price: Decimal | None = Decimal(1)
        volume = Decimal(1)
        pending = sorted(steps, key=lambda step: step.day, reverse=True)
        for bar in reversed(ordered):
            day = cast("date", bar["session_date"])
            while pending and pending[0].day > day:
                step = pending.pop(0)
                price = None if price is None or step.price is None else price * step.price
                volume *= step.volume
            values, reasons = _scaled(bar, basis, price, volume)
            derived.append((values, price, reasons))
    derived.reverse()
    return derived


def _derive(
    prices: HeadRead, actions: HeadRead, basis: str, budget: ComputeBudget
) -> dict[str, object]:
    if basis not in BASES:
        raise ValueError(f"adjusted basis must be one of {list(BASES)}")
    estimated = 64 * 1024 + _ROW_BYTES * (len(prices.rows) + len(actions.rows))
    if estimated > budget.available_bytes:
        raise ComputeResourceError(
            f"adjusted price memory estimate {estimated} exceeds admitted "
            f"materialization budget {budget.available_bytes} bytes"
        )
    # One instrument is one series across every cutover pin; each bar keeps its own pin.
    bars: dict[str, list[HeadRow]] = {}
    for row in prices.rows:
        bars.setdefault(str(row.values["instrument_id"]), []).append(row)
    events: dict[str, list[Mapping[str, object]]] = {}
    for row in actions.rows:
        events.setdefault(str(row.values["instrument_id"]), []).append(row.values)
    rows = []
    for instrument, found in sorted(bars.items()):
        found.sort(key=lambda row: cast("date", row.values["session_date"]))
        derived = adjust([row.values for row in found], events.get(instrument, []), basis)
        for row, (values, factor, reasons) in zip(found, derived, strict=True):
            rows.append(AdjustedRow(row.pin, values, factor, reasons))
    rows.sort(key=lambda row: (row.pin, str(row.values["record_id"])))
    digest = [
        [
            row.pin,
            row.values["record_id"],
            row.values["revision_id"],
            None if row.factor is None else str(row.factor),
        ]
        for row in rows
    ]
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "method": METHOD,
        "basis": basis,
        "prices": dict(prices.receipt),
        "prices_hash": prices.receipt_hash,
        "actions": dict(actions.receipt),
        "actions_hash": actions.receipt_hash,
        "rows": len(rows),
        "rows_hash": hashlib.sha256(canonical_json_bytes(digest)).hexdigest(),
    }
    return {"rows": tuple(rows), "receipt": receipt}


def _result(prices: HeadRead, actions: HeadRead, derived: dict[str, object]) -> AdjustedRead:
    receipt = cast("dict[str, object]", derived["receipt"])
    return AdjustedRead(
        cast("tuple[AdjustedRow, ...]", derived["rows"]),
        prices,
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

    ``time_rules`` covers every generation of both bindings, as for ``read_heads``.
    """
    _check(prices, actions)
    price_read = read_heads(
        connection, prices, query, time_rules=time_rules, budget=budget, rehash=rehash
    )
    action_read = read_heads(
        connection,
        actions,
        actions_query(query),
        time_rules=time_rules,
        budget=budget,
        rehash=rehash,
    )
    return _result(price_read, action_read, _derive(price_read, action_read, basis, budget))


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
    price_read = load_pinned_heads(workspace, prices, query, budget=budget, rehash=rehash)
    action_read = load_pinned_heads(
        workspace, actions, actions_query(query), budget=budget, rehash=rehash
    )
    return _result(price_read, action_read, _derive(price_read, action_read, basis, budget))
