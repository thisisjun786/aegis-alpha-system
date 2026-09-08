"""Caller-supplied ensemble membership and its canonical digest."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal

from aegis_alpha.engine.errors import EnsembleMembershipError

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class MembershipRow:
    name: str
    weight: Decimal

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise EnsembleMembershipError("membership name must be nonempty")
        if not self.weight.is_finite() or self.weight <= 0:
            raise EnsembleMembershipError("membership weights must be finite and positive")


def membership_hash(rows: tuple[MembershipRow, ...]) -> str:
    """Hash sorted [name, rounded-weight] pairs with compact JSON."""
    projection = [
        [row.name, float(round(row.weight, 10))] for row in sorted(rows, key=lambda item: item.name)
    ]
    encoded = json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def require_sha256_hex(value: str, *, field: str) -> str:
    if not _SHA256_HEX.fullmatch(value):
        raise EnsembleMembershipError(f"{field} must be a 64-character lowercase hex digest")
    return value


def require_finite_number(value: float, *, field: str) -> float:
    converted = float(value)
    if not math.isfinite(converted):
        raise EnsembleMembershipError(f"{field} must be a finite number")
    return converted
