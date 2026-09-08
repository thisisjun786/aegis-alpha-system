"""Fail-closed errors for the public engine contract and replay."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

# Exceptions must allow Python/contextlib to assign __traceback__; only value models are frozen.


class BlockReason(StrEnum):
    STALE_PRICE = "stale_price"
    STALE_MACRO = "stale_macro"
    LAG_BUCKET_MISS = "lag_bucket_miss"
    POST_CUTOFF = "post_cutoff"
    AS_OF_MISMATCH = "as_of_mismatch"
    MISSING_DERIVED_INPUT = "missing_derived_input"
    MISSING_HISTORY = "missing_history"
    INVALID_TOP_N = "invalid_top_n"
    MISSING_STRATEGY = "missing_strategy"
    RECEIPT_MISMATCH = "receipt_mismatch"
    UNSUPPORTED_SIGNAL_DATE = "unsupported_signal_date"
    MISSING_CONFIG = "missing_config"
    UNSUPPORTED_OPERATION = "unsupported_operation"


class ObservationKind(StrEnum):
    PRICE = "price"
    MACRO = "macro"


@dataclass(slots=True)
class ContractDefinitionError(ValueError):
    reason: str

    def __str__(self) -> str:
        return f"invalid engine contract definition: {self.reason}"


@dataclass(slots=True)
class ContractVersionError(ValueError):
    version: str

    def __str__(self) -> str:
        return f"unsupported engine contract version: {self.version}"


@dataclass(slots=True)
class BundleVersionError(ValueError):
    version: str

    def __str__(self) -> str:
        return f"unsupported engine bundle schema: {self.version}"


@dataclass(slots=True)
class ContractParseError(ValueError):
    field: str
    reason: str

    def __str__(self) -> str:
        return f"invalid engine contract field {self.field}: {self.reason}"


@dataclass(slots=True)
class BundleParseError(ValueError):
    field: str
    reason: str

    def __str__(self) -> str:
        return f"invalid engine bundle field {self.field}: {self.reason}"


@dataclass(slots=True)
class BundleIdentityError(ValueError):
    reason: str

    def __str__(self) -> str:
        return f"engine bundle identity mismatch: {self.reason}"


class StrategyPackLineageError(ContractDefinitionError):
    """A constructed pack violates structural lineage rules."""

    def __str__(self) -> str:
        return f"invalid strategy pack lineage: {self.reason}"


class EnsembleMembershipError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    def __str__(self) -> str:
        return f"invalid ensemble membership: {self.reason}"


@dataclass(slots=True)
class ReplayBlockedError(RuntimeError):
    reason: BlockReason
    detail: str

    def __str__(self) -> str:
        return f"engine replay blocked ({self.reason}): {self.detail}"
