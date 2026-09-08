"""Typed operator selections for bounded FMP dataset collection."""

from __future__ import annotations

from argparse import ArgumentParser
from enum import StrEnum
from typing import Final

PROFILE_DATASET: Final = "fmp_profile"
FULL_PRICE_DATASET: Final = "fmp_price_eod_full"
WINDOWED_DATASETS: Final = (
    FULL_PRICE_DATASET,
    "fmp_price_eod_non_split_adjusted",
    "fmp_price_eod_dividend_adjusted",
)
ACTION_DATASETS: Final = ("fmp_splits", "fmp_dividends")
PROBE_DATASETS: Final = (FULL_PRICE_DATASET, PROFILE_DATASET)
ALL_DATASETS: Final = (*WINDOWED_DATASETS, *ACTION_DATASETS, PROFILE_DATASET)
DAILY_PLAN_DATASET: Final = "fmp_daily_all"


class DatasetSelection(StrEnum):
    """One control-plane dataset target, plus the preserved live probe set."""

    PROBE = "probe"
    PRICE_EOD_FULL = FULL_PRICE_DATASET
    PRICE_EOD_NON_SPLIT_ADJUSTED = "fmp_price_eod_non_split_adjusted"
    PRICE_EOD_DIVIDEND_ADJUSTED = "fmp_price_eod_dividend_adjusted"
    SPLITS = "fmp_splits"
    DIVIDENDS = "fmp_dividends"
    PROFILE = PROFILE_DATASET
    ALL = "all"

    @property
    def datasets(self) -> tuple[str, ...]:
        if self is DatasetSelection.PROBE:
            return PROBE_DATASETS
        if self is DatasetSelection.ALL:
            return ALL_DATASETS
        if self is DatasetSelection.PROFILE:
            return (PROFILE_DATASET,)
        return (self.value, PROFILE_DATASET)

    @property
    def plan_dataset(self) -> str:
        if self is DatasetSelection.PROBE:
            return FULL_PRICE_DATASET
        if self is DatasetSelection.ALL:
            return DAILY_PLAN_DATASET
        return self.value


def add_dataset_selection_argument(parser: ArgumentParser) -> None:
    """Expose the bounded selection without expanding the legacy CLI module."""

    parser.add_argument(
        "--dataset-selection",
        choices=tuple(selection.value for selection in DatasetSelection),
        default=DatasetSelection.PROBE.value,
        help="probe preserves profile + full EOD; dataset values select one planned target",
    )
