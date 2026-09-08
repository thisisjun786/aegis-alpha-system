"""Validated invocation identity and roots for the SEC durable runtime."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from aegis_alpha.data.sec_collector import (
    CollectorConfig,
    CollectorError,
    containing_git_repository,
)
from aegis_alpha.data.sec_evidence import open_directory
from aegis_alpha.data.sec_identity import Admission
from aegis_alpha.data.sec_rate_limit import RateLimiter


@dataclass(frozen=True, slots=True)
class FrozenSecIdentity:
    admissions: tuple[Admission, ...]
    as_of: datetime

    def requested_instruments(self) -> tuple[str, ...]:
        return tuple(item.instrument_id for item in self.admissions)

    def admit(self, instrument_id: str, as_of: datetime) -> Admission:
        if as_of != self.as_of:
            raise CollectorError("SEC identity cutoff changed")
        return next(item for item in self.admissions if item.instrument_id == instrument_id)


def validate_runtime_config(config: CollectorConfig, limiter: RateLimiter) -> None:
    if (
        type(config.max_calls) is not int
        or config.max_calls < 1
        or config.max_calls != limiter.max_calls
    ):
        raise CollectorError("SEC runtime requires one exact positive call budget")
    if limiter.calls_attempted != 0:
        raise CollectorError("SEC runtime requires a fresh invocation limiter")
    if not isinstance(config.run_identity, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", config.run_identity
    ):
        raise CollectorError("SEC run identity must be a stable nonempty safe label")
    if (
        not isinstance(config.as_of, datetime)
        or config.as_of.tzinfo is None
        or config.as_of.utcoffset() is None
    ):
        raise CollectorError("SEC cutoff must be timezone-aware")
    from aegis_alpha.data.sec_normalize import NORMALIZED_VERSION  # noqa: PLC0415

    if config.normalized_version != NORMALIZED_VERSION:
        raise CollectorError("unsupported SEC normalization version")
    for path in (config.raw_store_root, config.dataset_root, config.receipt_path):
        if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
            raise CollectorError("SEC runtime paths must be absolute without parent traversal")
        if containing_git_repository(path):
            raise CollectorError("SEC runtime output must be outside Git")
        ancestor = path.parent
        while not ancestor.exists():
            ancestor = ancestor.parent
        with open_directory(ancestor):
            pass


def run_config(config: CollectorConfig, run_id: str) -> CollectorConfig:
    """Namespace raw/data roots; the app already supplied the invocation receipt path."""
    return replace(
        config,
        raw_store_root=config.raw_store_root / "runs" / run_id,
        dataset_root=config.dataset_root / "runs" / run_id,
        receipt_path=config.receipt_path,
    )
