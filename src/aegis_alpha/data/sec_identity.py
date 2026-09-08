"""Read-only 007 CIK admission for the AAS-DATA-013 SEC verifier collector.

The collector never searches EDGAR by ticker and never writes ``identity_*``
tables. Admission is a projection of a frozen 007 snapshot (or an in-memory
test double of the same shape).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

from aegis_alpha.identity.records import (
    IdentifierType,
    IdentifierValueError,
    interval_contains,
    normalize_identifier,
)

CIK_DIGITS = 10
IDENTITY_NAMESPACE = "cik"


class IdentityError(ValueError):
    """A 007 snapshot is unusable for CIK admission."""


class AdmissionState(StrEnum):
    ADMITTED = "admitted"
    SKIPPED_NO_CIK = "skipped_no_cik"
    SKIPPED_CONFLICT = "skipped_conflict"


class ConflictReason(StrEnum):
    MAPPING_CONFLICT = "identity_mapping_conflicts"
    CIK_BOUND_TO_MULTIPLE_INSTRUMENTS = "cik_bound_to_multiple_instruments"
    INSTRUMENT_HAS_CONFLICTING_CIKS = "instrument_has_conflicting_ciks"


def pad_cik(source_value: str) -> str:
    """Return the 10-digit zero-padded CIK. The unpadded source stays in raw."""

    try:
        return normalize_identifier(IdentifierType.CIK, source_value)
    except IdentifierValueError as error:
        raise IdentityError("CIK source is not a normalizable 1-10 digit value") from error


def _require_aware(field_name: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IdentityError(f"{field_name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _parse_instant(field_name: str, value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise IdentityError(f"{field_name} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise IdentityError(f"{field_name} must be an RFC 3339 timestamp") from error
    return _require_aware(field_name, parsed)


def _optional_instant(field_name: str, value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_instant(field_name, value)


def _require_nonempty(field_name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IdentityError(f"{field_name} must be a nonempty string")
    return value


@dataclass(frozen=True, slots=True)
class MappingConflictView:
    """Read-only projection of ``identity_mapping_conflicts``. Never persisted here."""

    provider: str
    namespace: str
    provider_identifier: str
    attempted_instrument_id: str
    existing_instrument_id: str


@dataclass(frozen=True, slots=True)
class InstrumentCikClaim:
    """One instrument's issuer-level CIK claim, or the absence of one."""

    instrument_id: str
    issuer_id: str
    cik_source: str | None
    effective_start: datetime
    effective_end: datetime | None

    @property
    def cik(self) -> str | None:
        if self.cik_source is None:
            return None
        return pad_cik(self.cik_source)

    def effective_at(self, as_of: datetime) -> bool:
        return interval_contains(self.effective_start, self.effective_end, as_of)


@dataclass(frozen=True, slots=True)
class Admission:
    instrument_id: str
    state: AdmissionState
    issuer_id: str | None = None
    cik: str | None = None
    cik_source: str | None = None
    reason: str | None = None


class IdentityPort(Protocol):
    """The only identity surface 013 may call. There is no write method."""

    def requested_instruments(self) -> tuple[str, ...]: ...

    def admit(self, instrument_id: str, as_of: datetime) -> Admission: ...


@dataclass(frozen=True, slots=True)
class IdentitySnapshot:
    """Frozen 007 double used by G-A. Production tables are never opened."""

    as_of: datetime
    claims: tuple[InstrumentCikClaim, ...]
    conflicts: tuple[MappingConflictView, ...]

    def requested_instruments(self) -> tuple[str, ...]:
        return tuple(claim.instrument_id for claim in self.claims)

    def admit(self, instrument_id: str, as_of: datetime) -> Admission:
        _require_aware("as_of", as_of)
        effective = tuple(
            claim
            for claim in self.claims
            if claim.instrument_id == instrument_id and claim.effective_at(as_of)
        )
        if not effective:
            return Admission(instrument_id=instrument_id, state=AdmissionState.SKIPPED_NO_CIK)
        ciks = {claim.cik for claim in effective if claim.cik is not None}
        if not ciks:
            issuer_id = effective[0].issuer_id
            return Admission(
                instrument_id=instrument_id,
                state=AdmissionState.SKIPPED_NO_CIK,
                issuer_id=issuer_id,
            )
        if len(ciks) > 1:
            return Admission(
                instrument_id=instrument_id,
                state=AdmissionState.SKIPPED_CONFLICT,
                issuer_id=effective[0].issuer_id,
                reason=ConflictReason.INSTRUMENT_HAS_CONFLICTING_CIKS.value,
            )
        cik = next(iter(ciks))
        claim = next(item for item in effective if item.cik == cik)
        if self._conflict_blocks(cik, instrument_id):
            return Admission(
                instrument_id=instrument_id,
                issuer_id=claim.issuer_id,
                cik=cik,
                cik_source=claim.cik_source,
                state=AdmissionState.SKIPPED_CONFLICT,
                reason=ConflictReason.MAPPING_CONFLICT.value,
            )
        peers = self._instruments_for_cik(cik, as_of)
        if len(peers) > 1:
            return Admission(
                instrument_id=instrument_id,
                issuer_id=claim.issuer_id,
                cik=cik,
                cik_source=claim.cik_source,
                state=AdmissionState.SKIPPED_CONFLICT,
                reason=ConflictReason.CIK_BOUND_TO_MULTIPLE_INSTRUMENTS.value,
            )
        return Admission(
            instrument_id=instrument_id,
            issuer_id=claim.issuer_id,
            cik=cik,
            cik_source=claim.cik_source,
            state=AdmissionState.ADMITTED,
        )

    def _conflict_blocks(self, cik: str, instrument_id: str) -> bool:
        for conflict in self.conflicts:
            if conflict.namespace != IDENTITY_NAMESPACE:
                continue
            try:
                conflict_cik = pad_cik(conflict.provider_identifier)
            except IdentityError:
                continue
            if conflict_cik != cik:
                continue
            if instrument_id in {
                conflict.attempted_instrument_id,
                conflict.existing_instrument_id,
            }:
                return True
        return False

    def _instruments_for_cik(self, cik: str, as_of: datetime) -> frozenset[str]:
        return frozenset(
            claim.instrument_id
            for claim in self.claims
            if claim.effective_at(as_of) and claim.cik == cik
        )


def load_identity_snapshot(path: Path) -> IdentitySnapshot:
    """Load a hash-free G-A identity double. This is not a production export."""

    try:
        document = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise IdentityError("identity snapshot is not readable JSON") from error
    if not isinstance(document, Mapping):
        raise IdentityError("identity snapshot must be a JSON object")
    payload = cast("Mapping[str, object]", document)
    as_of = _parse_instant("as_of_utc", payload.get("as_of_utc"))
    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, Sequence) or isinstance(raw_instruments, (str, bytes)):
        raise IdentityError("identity snapshot instruments must be an array")
    claims = tuple(_parse_claim(entry) for entry in raw_instruments)
    raw_conflicts = payload.get("conflicts", ())
    if not isinstance(raw_conflicts, Sequence) or isinstance(raw_conflicts, (str, bytes)):
        raise IdentityError("identity snapshot conflicts must be an array")
    conflicts = tuple(_parse_conflict(entry) for entry in raw_conflicts)
    return IdentitySnapshot(as_of=as_of, claims=claims, conflicts=conflicts)


def _parse_claim(raw: object) -> InstrumentCikClaim:
    if not isinstance(raw, Mapping):
        raise IdentityError("identity claim must be a JSON object")
    document = cast("Mapping[str, object]", raw)
    cik_source = document.get("cik_source")
    if cik_source is not None:
        cik_source = _require_nonempty("cik_source", cik_source)
    return InstrumentCikClaim(
        instrument_id=_require_nonempty("instrument_id", document.get("instrument_id")),
        issuer_id=_require_nonempty("issuer_id", document.get("issuer_id")),
        cik_source=cik_source,
        effective_start=_parse_instant("effective_start", document.get("effective_start")),
        effective_end=_optional_instant("effective_end", document.get("effective_end")),
    )


def _parse_conflict(raw: object) -> MappingConflictView:
    if not isinstance(raw, Mapping):
        raise IdentityError("identity conflict must be a JSON object")
    document = cast("Mapping[str, object]", raw)
    return MappingConflictView(
        provider=_require_nonempty("provider", document.get("provider")),
        namespace=_require_nonempty("namespace", document.get("namespace")),
        provider_identifier=_require_nonempty(
            "provider_identifier", document.get("provider_identifier")
        ),
        attempted_instrument_id=_require_nonempty(
            "attempted_instrument_id", document.get("attempted_instrument_id")
        ),
        existing_instrument_id=_require_nonempty(
            "existing_instrument_id", document.get("existing_instrument_id")
        ),
    )


def admit_universe(
    identity: IdentityPort,
    instrument_ids: Sequence[str] | None,
    as_of: datetime,
) -> tuple[Admission, ...]:
    """Admit each requested instrument. Ticker search is structurally absent."""

    requested = (
        identity.requested_instruments() if instrument_ids is None else tuple(instrument_ids)
    )
    return tuple(identity.admit(instrument_id, as_of) for instrument_id in requested)
