"""Explicit FX conversion of a price into the account currency, under one named rule.

A conversion is granted, never inferred: the caller names the price currency, the account
currency, the fixing series that relates them, how old a fixing may be, and whether the
strategy's signals see converted prices or prices in their own currency. This module
applies that grant to explicitly supplied fixings. It reads no store and chooses no series.

The rule ``fx_latest_fixing_on_or_before@1``: a value dated ``d`` in the price currency is
stated in the account currency with the latest present fixing whose date is on or before
``d`` and at most ``max_fixing_age_days`` earlier. A series ``PRICE/ACCOUNT`` multiplies by
its rate (account units per price unit); a series ``ACCOUNT/PRICE`` divides by it. A value
with no such fixing has no account-currency value: it is never filled from a later fixing
or carried forward past the declared age.
"""

from __future__ import annotations

import math
import re
from bisect import bisect_right
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import date

__all__ = [
    "FX_CONVERSION_SCHEMA",
    "FX_FIXING_RULE",
    "SIGNAL_BASES",
    "FxConversion",
    "FxFixing",
    "FxRates",
]

FX_CONVERSION_SCHEMA = "aas-fx-conversion-v1"
FX_FIXING_RULE = "fx_latest_fixing_on_or_before@1"
# Whether a strategy's signals read prices converted into the account currency or prices
# in their own currency. Both are research choices that change a result, so a grant names
# one rather than inheriting a default.
SIGNAL_BASES = ("account_currency", "price_currency")
_CURRENCY = re.compile(r"[A-Z]{3}")


@dataclass(frozen=True, slots=True)
class FxConversion:
    """One granted conversion of prices in ``currency`` into the ``account`` currency."""

    currency: str
    account: str
    series_id: str
    max_fixing_age_days: int
    signal_basis: str

    def __post_init__(self) -> None:
        for code in (self.currency, self.account):
            if not isinstance(code, str) or _CURRENCY.fullmatch(code) is None:
                raise ValueError("an FX conversion names three-letter uppercase currencies")
        if self.currency == self.account:
            raise ValueError("an FX conversion converts a currency other than the account's")
        if self.series_id not in (
            self.currency + "/" + self.account,
            self.account + "/" + self.currency,
        ):
            raise ValueError(
                "an FX conversion series is "
                + self.currency
                + "/"
                + self.account
                + " or "
                + self.account
                + "/"
                + self.currency
            )
        age = self.max_fixing_age_days
        if type(age) is not int or age < 0:
            raise ValueError("max_fixing_age_days must be a nonnegative integer")
        if self.signal_basis not in SIGNAL_BASES:
            raise ValueError("signal_basis must be " + " or ".join(SIGNAL_BASES))

    @property
    def inverse(self) -> bool:
        """True when the series quotes the price currency per account unit."""
        return self.series_id.startswith(self.account + "/")

    def document(self) -> dict[str, object]:
        """The conversion as the run records it, rule included."""
        return {
            "schema": FX_CONVERSION_SCHEMA,
            "rule": FX_FIXING_RULE,
            "currency": self.currency,
            "account_currency": self.account,
            "series_id": self.series_id,
            "direction": "divide" if self.inverse else "multiply",
            "max_fixing_age_days": self.max_fixing_age_days,
            "signal_basis": self.signal_basis,
        }


@dataclass(frozen=True, slots=True)
class FxFixing:
    """One present fixing: the date the rule compares and the rate it applies."""

    day: date
    rate: float

    def __post_init__(self) -> None:
        if (
            isinstance(self.rate, bool)
            or not isinstance(self.rate, (int, float))
            or not math.isfinite(self.rate)
            or self.rate <= 0
        ):
            raise ValueError("an FX fixing rate must be finite and positive")


class FxRates:
    """The fixings one conversion may use, in date order, one per date."""

    __slots__ = ("_days", "_fixings", "conversion")

    def __init__(self, conversion: FxConversion, fixings: Iterable[FxFixing]) -> None:
        ordered = sorted(fixings, key=lambda fixing: fixing.day)
        days = [fixing.day for fixing in ordered]
        if len(set(days)) != len(days):
            raise ValueError("an FX series repeats one fixing date for " + conversion.series_id)
        self.conversion = conversion
        self._fixings = tuple(ordered)
        self._days = tuple(days)

    def __len__(self) -> int:
        return len(self._fixings)

    def fixing_for(self, day: date) -> FxFixing | None:
        """The fixing the rule applies to a value dated ``day``, or None."""
        index = bisect_right(self._days, day)
        if index == 0:
            return None
        fixing = self._fixings[index - 1]
        if (day - fixing.day).days > self.conversion.max_fixing_age_days:
            return None
        return fixing

    def convert(self, value: float, day: date) -> tuple[float, FxFixing] | None:
        """``value`` dated ``day`` in the account currency, with the fixing used."""
        fixing = self.fixing_for(day)
        if fixing is None:
            return None
        converted = value / fixing.rate if self.conversion.inverse else value * fixing.rate
        return converted, fixing
