"""Caller-supplied weighted ensemble. Positive weights only, renormalized to 1.0."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType

from aegis_alpha.engine.errors import BlockReason, EnsembleMembershipError, ReplayBlockedError
from aegis_alpha.engine.membership import MembershipRow, membership_hash, require_sha256_hex


@dataclass(frozen=True, slots=True)
class EnsembleMembership:
    rows: tuple[MembershipRow, ...]
    expected_membership_sha256: str

    def __post_init__(self) -> None:
        if not self.rows:
            raise EnsembleMembershipError("membership must contain at least one row")
        names = tuple(row.name for row in self.rows)
        if len(set(names)) != len(names):
            raise EnsembleMembershipError("membership names must be unique")
        require_sha256_hex(self.expected_membership_sha256, field="expected_membership_sha256")
        if membership_hash(self.rows) != self.expected_membership_sha256:
            raise EnsembleMembershipError(
                "membership digest does not match the declared ensemble hash"
            )

    @property
    def membership_sha256(self) -> str:
        return membership_hash(self.rows)


def combine_ensemble(
    per_strategy: Mapping[str, Mapping[str, float]],
    membership: EnsembleMembership,
) -> Mapping[str, float]:
    positive = tuple(row for row in membership.rows if row.weight > 0)
    scale = sum((row.weight for row in positive), start=Decimal(0))
    if scale == 0:
        return MappingProxyType({})
    totals: dict[str, float] = {}
    for row in positive:
        if row.name not in per_strategy:
            raise ReplayBlockedError(
                BlockReason.MISSING_STRATEGY,
                f"membership member {row.name} is missing from per_strategy",
            )
        allocation = per_strategy[row.name]
        weight = float(row.weight / scale)
        for asset_id, share in allocation.items():
            totals[asset_id] = totals.get(asset_id, 0.0) + share * weight
    combined = sum(totals.values())
    if combined == 0:
        return MappingProxyType({})
    return MappingProxyType({asset_id: value / combined for asset_id, value in totals.items()})


def membership_from_rows(
    rows: Sequence[MembershipRow],
    *,
    expected_membership_sha256: str,
) -> EnsembleMembership:
    return EnsembleMembership(tuple(rows), expected_membership_sha256)
