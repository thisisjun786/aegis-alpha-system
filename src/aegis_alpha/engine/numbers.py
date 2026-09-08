"""Finite and positive numeric guards shared by injected observations."""

from __future__ import annotations

import math

from aegis_alpha.engine.errors import BlockReason, ContractDefinitionError, ReplayBlockedError


def require_finite(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractDefinitionError(f"{field} must be a finite number")
    try:
        converted = float(value)
    except OverflowError as error:
        raise ContractDefinitionError(f"{field} must be a finite number") from error
    if not math.isfinite(converted):
        raise ContractDefinitionError(f"{field} must be a finite number")
    return converted


def require_positive(value: object, *, field: str) -> float:
    converted = require_finite(value, field=field)
    if converted <= 0:
        raise ContractDefinitionError(f"{field} must be a positive finite number")
    return converted


def require_finite_observation(value: object, *, field: str) -> float:
    try:
        return require_finite(value, field=field)
    except ContractDefinitionError as error:
        raise ReplayBlockedError(BlockReason.MISSING_HISTORY, str(error)) from error


def require_positive_observation(value: object, *, field: str) -> float:
    try:
        return require_positive(value, field=field)
    except ContractDefinitionError as error:
        raise ReplayBlockedError(BlockReason.MISSING_HISTORY, str(error)) from error
