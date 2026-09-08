"""Generic derived series from named bound inputs and explicit operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from aegis_alpha.engine.calendar import calendar_month_end, lag_month, trailing_window_start
from aegis_alpha.engine.errors import (
    BlockReason,
    ObservationKind,
    ReplayBlockedError,
)
from aegis_alpha.engine.models import DerivedSeriesSpec, StaleGateSpec
from aegis_alpha.engine.numbers import require_finite_observation, require_positive_observation
from aegis_alpha.engine.pit import reject_stale
from aegis_alpha.engine.signals import MacroPoint


@dataclass(frozen=True, slots=True)
class DerivedCashflows:
    fields: Mapping[str, Sequence[tuple[date, float, date]]]
    identities: Mapping[str, tuple[str, str]]

    def __post_init__(self) -> None:
        fields = {}
        for name, series in self.fields.items():
            validate = (
                require_positive_observation if name == "price" else require_finite_observation
            )
            fields[name] = tuple(
                (when, validate(value, field=name), observed) for when, value, observed in series
            )
        object.__setattr__(self, "fields", MappingProxyType(fields))
        object.__setattr__(self, "identities", MappingProxyType(dict(self.identities)))


def derive_series(
    spec: DerivedSeriesSpec,
    cashflows: DerivedCashflows,
    *,
    as_of: date,
    gates: StaleGateSpec,
    knowledge_as_of: date | None = None,
) -> MacroPoint:
    cutoff = knowledge_as_of or as_of
    if not spec.input_bindings:
        raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, spec.series_id)
    _require_bound_identities(spec, cashflows)
    price_series = _field_series(cashflows, "price", spec.series_id)
    month_end_price = _month_end(price_series, as_of)
    if month_end_price is None:
        raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, spec.series_id)
    price_date, price, price_observed = month_end_price
    if (price_date.year, price_date.month) != (as_of.year, as_of.month):
        raise ReplayBlockedError(
            BlockReason.MISSING_DERIVED_INPUT, f"{spec.series_id} missing price bucket"
        )
    reject_stale(
        observation_date=price_observed,
        as_of=cutoff,
        gates=gates,
        kind=ObservationKind.MACRO,
    )
    observed = price_observed
    trailing, observed = _trailing_window(
        _field_series(cashflows, "addend_a", spec.series_id),
        as_of,
        months=spec.trailing_months,
        gates=gates,
        observed=observed,
        cutoff=cutoff,
    )
    if spec.operation == "trailing_sum_plus_trailing_sum_over_price":
        extra, observed = _trailing_window(
            _field_series(cashflows, "addend_b", spec.series_id),
            as_of,
            months=spec.trailing_months,
            gates=gates,
            observed=observed,
            cutoff=cutoff,
        )
        if extra is None or trailing is None:
            raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, spec.series_id)
        trailing = trailing + extra
    elif spec.operation != "trailing_sum_over_price":
        raise ReplayBlockedError(
            BlockReason.UNSUPPORTED_OPERATION,
            f"unsupported derived operation {spec.operation!r}",
        )
    if trailing is None or price == 0:
        raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, spec.series_id)
    # Signals address calendar-month buckets even when the last trading day is earlier.
    return MacroPoint(as_of=calendar_month_end(as_of), value=trailing / price, observed_on=observed)


def derive_history(
    specs: Sequence[DerivedSeriesSpec],
    *,
    inputs: Mapping[str, Mapping[str, object]],
    as_of: date,
    gates: StaleGateSpec,
    signal_date: date,
) -> dict[str, tuple[MacroPoint, ...]]:
    history: dict[str, list[MacroPoint]] = {}
    for spec in specs:
        bound = inputs.get(spec.series_id)
        if bound is None:
            raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, spec.series_id)
        cashflows = _cashflows(bound)
        points: list[MacroPoint] = []
        seen: set[date] = set()
        for lag in sorted(set(spec.signal_lag_months)):
            bucket = lag_month(signal_date, lag)
            if bucket in seen or bucket > as_of:
                continue
            point = derive_series(
                spec,
                cashflows,
                as_of=bucket,
                gates=gates,
                knowledge_as_of=as_of,
            )
            points.append(point)
            seen.add(point.as_of)
        history[spec.series_id] = points
    return {name: tuple(points) for name, points in history.items()}


def _cashflows(bound: Mapping[str, object]) -> DerivedCashflows:
    raw_fields = bound.get("fields")
    if not isinstance(raw_fields, Mapping):
        raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, "fields")
    fields: dict[str, Sequence[tuple[date, float, date]]] = {}
    for key, value in raw_fields.items():
        if not isinstance(key, str):
            raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, "fields")
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, key)
        points: list[tuple[date, float, date]] = []
        for item in value:
            if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
                raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, key)
            try:
                item_date, amount, observed = item
            except ValueError as error:
                raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, key) from error
            if (
                not isinstance(item_date, date)
                or not isinstance(observed, date)
                or not isinstance(amount, (int, float))
                or isinstance(amount, bool)
            ):
                raise ReplayBlockedError(BlockReason.MISSING_DERIVED_INPUT, key)
            points.append((item_date, float(amount), observed))
        fields[key] = tuple(points)
    return DerivedCashflows(fields=fields, identities=_identities(bound))


def _identities(bound: Mapping[str, object]) -> Mapping[str, tuple[str, str]]:
    raw = bound.get("identities", {})
    if not isinstance(raw, Mapping):
        return {}
    identities: dict[str, tuple[str, str]] = {}
    for key, value in raw.items():
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            continue
        try:
            dataset_id, dataset_version = value
        except ValueError:
            continue
        if isinstance(dataset_id, str) and isinstance(dataset_version, str):
            identities[str(key)] = (dataset_id, dataset_version)
    return identities


def _require_bound_identities(spec: DerivedSeriesSpec, cashflows: DerivedCashflows) -> None:
    for binding in spec.input_bindings:
        observed = cashflows.identities.get(binding.series)
        if observed != (binding.dataset_id, binding.dataset_version):
            raise ReplayBlockedError(
                BlockReason.MISSING_DERIVED_INPUT,
                f"{spec.series_id} input {binding.series} is not the bound "
                f"{binding.dataset_id}/{binding.dataset_version}",
            )


def _field_series(
    cashflows: DerivedCashflows,
    field_name: str,
    series_id: str,
) -> Sequence[tuple[date, float, date]]:
    series = cashflows.fields.get(field_name)
    if series is None:
        raise ReplayBlockedError(
            BlockReason.MISSING_DERIVED_INPUT,
            f"{series_id} missing field {field_name}",
        )
    return series


def _month_end(
    series: Sequence[tuple[date, float, date]],
    as_of: date,
) -> tuple[date, float, date] | None:
    eligible = tuple(item for item in series if item[0] <= as_of)
    if not eligible:
        return None
    by_month: dict[tuple[int, int], tuple[date, float, date]] = {}
    for item in eligible:
        key = (item[0].year, item[0].month)
        current = by_month.get(key)
        if current is None or item[0] > current[0]:
            by_month[key] = item
    if as_of != calendar_month_end(as_of):
        by_month.pop((as_of.year, as_of.month), None)
    if not by_month:
        return None
    return by_month[max(by_month)]


def _trailing_window(  # noqa: PLR0913 -- window plus point-in-time context
    series: Sequence[tuple[date, float, date]],
    as_of: date,
    *,
    months: int,
    gates: StaleGateSpec,
    observed: date,
    cutoff: date,
) -> tuple[float | None, date]:
    start = trailing_window_start(as_of, months)
    total = 0.0
    found = False
    latest = observed
    for item_date, value, item_observed in series:
        if item_date < start or item_date > as_of:
            continue
        reject_stale(
            observation_date=item_observed,
            as_of=cutoff,
            gates=gates,
            kind=ObservationKind.MACRO,
        )
        total += value
        found = True
        latest = max(latest, item_observed)
    if not found:
        return None, latest
    return total, latest
