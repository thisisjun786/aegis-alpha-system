from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

from aegis_alpha.metadata.records import (
    freeze_json_metadata,
    reject_credential_metadata,
    validated_json_copy,
)

_CIK_DIGITS = 10
_LEI_LENGTH = 20
_CUSIP_LENGTH = 9
_ISIN_LENGTH = 12
_FIGI_LENGTH = 12
_ISO7064_MOD = 97
_ISO7064_REMAINDER = 1
_DECIMAL_BASE = 10
_ALPHA_OFFSET = 55  # ord('A') - 10, so 'A' maps to 10 and 'Z' maps to 35.


class EntityType(StrEnum):
    ISSUER = "issuer"
    INSTRUMENT = "instrument"


class InstrumentKind(StrEnum):
    EQUITY = "equity"
    ETF = "etf"
    FUND = "fund"
    ADR = "adr"
    UNIT = "unit"
    PREFERRED = "preferred"
    WARRANT = "warrant"
    RIGHT = "right"
    INDEX = "index"
    OTHER = "other"


class IdentifierType(StrEnum):
    CIK = "cik"
    LEI = "lei"
    TICKER = "ticker"
    MIC = "mic"
    CUSIP = "cusip"
    ISIN = "isin"
    FIGI = "figi"
    COMPOSITE_FIGI = "composite_figi"
    SHARE_CLASS_FIGI = "share_class_figi"
    NORGATE_ASSETID = "norgate_assetid"


class ConflictClass(StrEnum):
    INSTRUMENT_DISAGREEMENT = "instrument_disagreement"
    INTERVAL_OVERLAP = "interval_overlap"


#: An identifier describes exactly one entity level. A CIK or LEI names the
#: issuer; everything else names a single tradable instrument. Recording this
#: explicitly is what stops an issuer-level fact from being flattened onto one
#: of its instruments.
IDENTIFIER_ENTITY_LEVEL: Mapping[IdentifierType, EntityType] = {
    IdentifierType.CIK: EntityType.ISSUER,
    IdentifierType.LEI: EntityType.ISSUER,
    IdentifierType.TICKER: EntityType.INSTRUMENT,
    IdentifierType.MIC: EntityType.INSTRUMENT,
    IdentifierType.CUSIP: EntityType.INSTRUMENT,
    IdentifierType.ISIN: EntityType.INSTRUMENT,
    IdentifierType.FIGI: EntityType.INSTRUMENT,
    IdentifierType.COMPOSITE_FIGI: EntityType.INSTRUMENT,
    IdentifierType.SHARE_CLASS_FIGI: EntityType.INSTRUMENT,
    IdentifierType.NORGATE_ASSETID: EntityType.INSTRUMENT,
}

_FIGI_PATTERN = re.compile(r"^BBG[BCDFGHJKLMNPQRSTVWXYZ0-9]{8}[0-9]$")
_IDENTIFIER_PATTERNS: Mapping[IdentifierType, re.Pattern[str]] = {
    IdentifierType.CIK: re.compile(r"^[0-9]{10}$"),
    IdentifierType.LEI: re.compile(r"^[A-Z0-9]{20}$"),
    IdentifierType.TICKER: re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$"),
    IdentifierType.MIC: re.compile(r"^[A-Z0-9]{4}$"),
    IdentifierType.CUSIP: re.compile(r"^[A-Z0-9]{9}$"),
    IdentifierType.ISIN: re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$"),
    IdentifierType.FIGI: _FIGI_PATTERN,
    IdentifierType.COMPOSITE_FIGI: _FIGI_PATTERN,
    IdentifierType.SHARE_CLASS_FIGI: _FIGI_PATTERN,
    IdentifierType.NORGATE_ASSETID: re.compile(r"^[0-9]{1,18}$"),
}

_FIGI_TYPES = frozenset(
    {IdentifierType.FIGI, IdentifierType.COMPOSITE_FIGI, IdentifierType.SHARE_CLASS_FIGI}
)


class IdentifierValueError(ValueError):
    """A source identifier is not a valid, normalizable value of its type."""


_ASCII_IDENTIFIER_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-*@# "
)


def _require_ascii_source(value: str) -> None:
    """Reject any source character outside the identifier ASCII allowlist.

    Compatibility normalization is powerful enough to turn characters that are
    not identifiers at all into ones that look canonical: ``\uff21\uff21\uff30\uff2c`` would become
    ``AAPL``, ``\u00b923`` would become ``123``, and ``\ufb03`` would become ``FFI``. Accepting
    those would launder malformed evidence into a clean-looking identity, so the
    allowlist is applied to the raw source before any normalization runs.
    """

    illegal = {character for character in value if character not in _ASCII_IDENTIFIER_CHARACTERS}
    if illegal:
        raise IdentifierValueError(
            "identifier source_value must contain only ASCII identifier characters"
        )


def _require_nonempty(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must be nonempty")


def _require_timezone_aware(field_name: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _frozen_json_object(field_name: str, value: object) -> Mapping[str, object]:
    frozen = freeze_json_metadata(value)
    reject_credential_metadata(frozen)
    if not isinstance(frozen, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return cast("Mapping[str, object]", frozen)


def _require_effective_interval(start: datetime, end: datetime | None) -> None:
    """Validate a half-open ``[start, end)`` UTC interval.

    ``end`` is exclusive, so an interval that ends exactly where the next one
    begins is adjacent rather than overlapping. A ``None`` end means the fact is
    still in effect. A zero-length interval carries no observable time and is
    rejected.
    """
    _require_timezone_aware("effective_start", start)
    if end is None:
        return
    _require_timezone_aware("effective_end", end)
    if end <= start:
        raise ValueError("effective_end must be strictly after effective_start")


def interval_contains(start: datetime, end: datetime | None, moment: datetime) -> bool:
    """Return whether ``moment`` falls inside the half-open interval."""

    _require_timezone_aware("as_of", moment)
    return start <= moment and (end is None or moment < end)


def intervals_overlap(
    first_start: datetime,
    first_end: datetime | None,
    second_start: datetime,
    second_end: datetime | None,
) -> bool:
    """Return whether two half-open intervals share any instant."""

    starts_before_other_ends = first_end is None or second_start < first_end
    other_starts_before_this_ends = second_end is None or first_start < second_end
    return starts_before_other_ends and other_starts_before_this_ends


def _character_value(character: str) -> int:
    if character.isdigit():
        return int(character)
    return ord(character) - _ALPHA_OFFSET


def _digit_sum(value: int) -> int:
    return sum(int(digit) for digit in str(value))


def _lei_checksum_valid(value: str) -> bool:
    """Verify the ISO 7064 MOD 97-10 checksum carried by a LEI."""

    expanded = "".join(str(_character_value(character)) for character in value)
    return int(expanded) % _ISO7064_MOD == _ISO7064_REMAINDER


def _cusip_checksum_valid(value: str) -> bool:
    total = 0
    for position, character in enumerate(value[:-1]):
        digit = _character_value(character)
        if position % 2 == 1:
            digit *= 2
        total += _digit_sum(digit)
    expected = (_DECIMAL_BASE - total % _DECIMAL_BASE) % _DECIMAL_BASE
    return expected == int(value[-1]) if value[-1].isdigit() else False


def _isin_checksum_valid(value: str) -> bool:
    """Verify the ISIN Luhn checksum over the letter-expanded body."""

    expanded = "".join(str(_character_value(character)) for character in value[:-1])
    total = 0
    for offset, digit_character in enumerate(reversed(expanded)):
        digit = int(digit_character)
        if offset % 2 == 0:
            digit *= 2
        total += _digit_sum(digit)
    expected = (_DECIMAL_BASE - total % _DECIMAL_BASE) % _DECIMAL_BASE
    return expected == int(value[-1])


def _figi_checksum_valid(value: str) -> bool:
    total = 0
    for position, character in enumerate(value[:-1]):
        digit = _character_value(character)
        if position % 2 == 1:
            digit *= 2
        total += _digit_sum(digit)
    expected = (_DECIMAL_BASE - total % _DECIMAL_BASE) % _DECIMAL_BASE
    return expected == int(value[-1])


_CHECKSUM_VALIDATORS = {
    IdentifierType.LEI: (_lei_checksum_valid, _LEI_LENGTH),
    IdentifierType.CUSIP: (_cusip_checksum_valid, _CUSIP_LENGTH),
    IdentifierType.ISIN: (_isin_checksum_valid, _ISIN_LENGTH),
}


def _normalize_case(identifier_type: IdentifierType, candidate: str) -> str:
    """Apply the small, per-type canonical form and nothing more."""

    if identifier_type is IdentifierType.CIK:
        if not candidate.isdigit() or len(candidate) > _CIK_DIGITS:
            raise IdentifierValueError("cik must be at most 10 digits")
        return candidate.zfill(_CIK_DIGITS)
    if identifier_type is IdentifierType.NORGATE_ASSETID:
        if not candidate.isdigit():
            raise IdentifierValueError("norgate_assetid must be digits")
        if len(candidate) > 1 and candidate.startswith("0"):
            raise IdentifierValueError("norgate_assetid must not carry leading zeros")
        return candidate
    return candidate.upper()


def _require_checksum(identifier_type: IdentifierType, normalized: str) -> None:
    checksum = _CHECKSUM_VALIDATORS.get(identifier_type)
    if checksum is not None:
        validator, expected_length = checksum
        if len(normalized) != expected_length or not validator(normalized):
            raise IdentifierValueError(f"{identifier_type.value} checksum is invalid")
    if identifier_type in _FIGI_TYPES and (
        len(normalized) != _FIGI_LENGTH or not _figi_checksum_valid(normalized)
    ):
        raise IdentifierValueError(f"{identifier_type.value} checksum is invalid")


def normalize_identifier(identifier_type: IdentifierType, source_value: str) -> str:
    """Return the canonical value of ``source_value`` for ``identifier_type``.

    Normalization is deliberately small, per-type, idempotent, and documented.
    It never repairs a malformed source: a value that does not survive its
    declared normalization is rejected so the caller keeps the original
    evidence problem instead of receiving a laundered identifier.
    """

    if not isinstance(source_value, str):
        raise TypeError("identifier source_value must be a string")
    _require_ascii_source(source_value)
    candidate = unicodedata.normalize("NFKC", source_value).strip()
    if not candidate:
        raise IdentifierValueError("identifier source_value must be nonempty")
    normalized = _normalize_case(identifier_type, candidate)
    pattern = _IDENTIFIER_PATTERNS[identifier_type]
    if pattern.fullmatch(normalized) is None:
        raise IdentifierValueError(f"{identifier_type.value} value has invalid syntax")
    _require_checksum(identifier_type, normalized)
    return normalized


def _instant_literal(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _canonical_digest(projection: Mapping[str, object]) -> str:
    canonical = json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def identifier_assertion_digest(assertion: IdentifierAssertion) -> str:
    return _canonical_digest(
        {
            "asserted_at_utc": _instant_literal(assertion.asserted_at_utc),
            "effective_end": _instant_literal(assertion.effective_end),
            "effective_start": _instant_literal(assertion.effective_start),
            "entity_id": assertion.entity_id,
            "entity_type": assertion.entity_type.value,
            "evidence": validated_json_copy(assertion.evidence),
            "identifier_type": assertion.identifier_type.value,
            "identifier_value": assertion.identifier_value,
            "schema_version": assertion.schema_version,
            "source_snapshot_id": assertion.source_snapshot_id,
            "source_value": assertion.source_value,
        }
    )


def provider_mapping_digest(mapping: ProviderMapping) -> str:
    return _canonical_digest(
        {
            "asserted_at_utc": _instant_literal(mapping.asserted_at_utc),
            "effective_end": _instant_literal(mapping.effective_end),
            "effective_start": _instant_literal(mapping.effective_start),
            "evidence": validated_json_copy(mapping.evidence),
            "instrument_id": mapping.instrument_id,
            "namespace": mapping.namespace,
            "provider": mapping.provider,
            "provider_identifier": mapping.provider_identifier,
            "schema_version": mapping.schema_version,
            "source_snapshot_id": mapping.source_snapshot_id,
        }
    )


@dataclass(frozen=True, slots=True)
class Issuer:
    issuer_id: str
    created_at_utc: datetime
    schema_version: int = 1
    display_name: str | None = None
    jurisdiction: str | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty("issuer_id", self.issuer_id)
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if self.display_name is not None:
            _require_nonempty("display_name", self.display_name)
        if self.jurisdiction is not None:
            jurisdiction = self.jurisdiction.strip().upper()
            if re.fullmatch(r"[A-Z]{2}", jurisdiction) is None:
                raise ValueError("jurisdiction must be an ISO 3166-1 alpha-2 code")
            object.__setattr__(self, "jurisdiction", jurisdiction)
        _require_timezone_aware("created_at_utc", self.created_at_utc)
        object.__setattr__(self, "evidence", _frozen_json_object("issuer evidence", self.evidence))


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_id: str
    issuer_id: str
    instrument_kind: InstrumentKind
    created_at_utc: datetime
    schema_version: int = 1
    display_name: str | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty("instrument_id", self.instrument_id)
        _require_nonempty("issuer_id", self.issuer_id)
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        try:
            kind = InstrumentKind(self.instrument_kind)
        except ValueError:
            allowed = ", ".join(member.value for member in InstrumentKind)
            raise ValueError(f"instrument_kind must be one of: {allowed}") from None
        object.__setattr__(self, "instrument_kind", kind)
        if self.display_name is not None:
            _require_nonempty("display_name", self.display_name)
        _require_timezone_aware("created_at_utc", self.created_at_utc)
        object.__setattr__(
            self, "evidence", _frozen_json_object("instrument evidence", self.evidence)
        )


@dataclass(frozen=True, slots=True)
class IdentifierAssertion:
    """A dated, source-backed claim that an entity carries an identifier.

    ``source_value`` preserves what the source actually presented and
    ``identifier_value`` holds the canonical form. Both are stored so a later
    reader can audit the normalization instead of trusting it.
    """

    assertion_id: str
    entity_type: EntityType
    entity_id: str
    identifier_type: IdentifierType
    source_value: str
    source_snapshot_id: str
    effective_start: datetime
    asserted_at_utc: datetime
    effective_end: datetime | None = None
    schema_version: int = 1
    evidence: Mapping[str, object] = field(default_factory=dict)
    identifier_value: str = field(init=False)
    assertion_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonempty("assertion_id", self.assertion_id)
        _require_nonempty("entity_id", self.entity_id)
        _require_nonempty("source_snapshot_id", self.source_snapshot_id)
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        try:
            entity_type = EntityType(self.entity_type)
        except ValueError:
            allowed = ", ".join(member.value for member in EntityType)
            raise ValueError(f"entity_type must be one of: {allowed}") from None
        try:
            identifier_type = IdentifierType(self.identifier_type)
        except ValueError:
            allowed = ", ".join(member.value for member in IdentifierType)
            raise ValueError(f"identifier_type must be one of: {allowed}") from None
        expected_level = IDENTIFIER_ENTITY_LEVEL[identifier_type]
        if entity_type is not expected_level:
            raise ValueError(
                f"{identifier_type.value} identifies a {expected_level.value}, "
                f"not a {entity_type.value}"
            )
        object.__setattr__(self, "entity_type", entity_type)
        object.__setattr__(self, "identifier_type", identifier_type)
        object.__setattr__(
            self, "identifier_value", normalize_identifier(identifier_type, self.source_value)
        )
        _require_effective_interval(self.effective_start, self.effective_end)
        _require_timezone_aware("asserted_at_utc", self.asserted_at_utc)
        object.__setattr__(
            self, "evidence", _frozen_json_object("assertion evidence", self.evidence)
        )
        object.__setattr__(self, "assertion_sha256", identifier_assertion_digest(self))


@dataclass(frozen=True, slots=True)
class ProviderMapping:
    """A dated, source-backed mapping from a provider key to one instrument.

    The provider key is ``(provider, namespace, provider_identifier)``. A
    ticker may appear as a ``provider_identifier``, but never as an entity
    identity, so nothing in the schema can be joined by ticker alone.
    """

    mapping_id: str
    provider: str
    namespace: str
    provider_identifier: str
    instrument_id: str
    source_snapshot_id: str
    effective_start: datetime
    asserted_at_utc: datetime
    effective_end: datetime | None = None
    schema_version: int = 1
    evidence: Mapping[str, object] = field(default_factory=dict)
    mapping_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _require_nonempty("mapping_id", self.mapping_id)
        _require_nonempty("provider", self.provider)
        _require_nonempty("namespace", self.namespace)
        _require_nonempty("provider_identifier", self.provider_identifier)
        _require_nonempty("instrument_id", self.instrument_id)
        _require_nonempty("source_snapshot_id", self.source_snapshot_id)
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        _require_effective_interval(self.effective_start, self.effective_end)
        _require_timezone_aware("asserted_at_utc", self.asserted_at_utc)
        object.__setattr__(self, "evidence", _frozen_json_object("mapping evidence", self.evidence))
        object.__setattr__(self, "mapping_sha256", provider_mapping_digest(self))


@dataclass(frozen=True, slots=True)
class MappingConflict:
    """Durable blocked evidence for a rejected mapping attempt."""

    conflict_id: str
    provider: str
    namespace: str
    provider_identifier: str
    conflict_class: ConflictClass
    attempted_instrument_id: str
    attempted_source_snapshot_id: str
    attempted_effective_start: datetime
    existing_mapping_id: str
    existing_instrument_id: str
    detected_at_utc: datetime
    attempted_effective_end: datetime | None = None
    details: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ResolvedIdentifier:
    entity_type: EntityType
    entity_id: str
    identifier_type: IdentifierType
    identifier_value: str
    source_value: str
    source_snapshot_id: str
    effective_start: datetime
    effective_end: datetime | None
