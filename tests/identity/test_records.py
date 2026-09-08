from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

import pytest

from aegis_alpha.identity.records import (
    EntityType,
    IdentifierAssertion,
    IdentifierType,
    IdentifierValueError,
    Instrument,
    InstrumentKind,
    Issuer,
    ProviderMapping,
    identifier_assertion_digest,
    interval_contains,
    intervals_overlap,
    normalize_identifier,
    provider_mapping_digest,
)

_IdentityRecord = IdentifierAssertion | ProviderMapping
_RecordFactory = Callable[..., _IdentityRecord]

_ASSERTED_AT = datetime(2026, 7, 31, 9, 0, tzinfo=UTC)
_START = datetime(2026, 7, 1, tzinfo=UTC)
_END = datetime(2026, 8, 1, tzinfo=UTC)


def _assertion(**overrides: object) -> IdentifierAssertion:
    values: dict[str, object] = {
        "assertion_id": "assertion-1",
        "entity_type": EntityType.INSTRUMENT,
        "entity_id": "instrument-apple-common",
        "identifier_type": IdentifierType.ISIN,
        "source_value": "US0378331005",
        "source_snapshot_id": "snapshot-1",
        "effective_start": _START,
        "effective_end": _END,
        "asserted_at_utc": _ASSERTED_AT,
        "evidence": {"source": "sec"},
    }
    values.update(overrides)
    return IdentifierAssertion(**values)  # ty: ignore[invalid-argument-type]


def _mapping(**overrides: object) -> ProviderMapping:
    values: dict[str, object] = {
        "mapping_id": "mapping-1",
        "provider": "norgate",
        "namespace": "us-equities",
        "provider_identifier": "12345",
        "instrument_id": "instrument-apple-common",
        "source_snapshot_id": "snapshot-1",
        "effective_start": _START,
        "effective_end": _END,
        "asserted_at_utc": _ASSERTED_AT,
        "evidence": {"source": "provider-export"},
    }
    values.update(overrides)
    return ProviderMapping(**values)  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize(
    ("identifier_type", "source_value", "expected"),
    [
        (IdentifierType.CIK, "  320193 ", "0000320193"),
        (IdentifierType.LEI, " hwupkr0mpou8fgxbt394 ", "HWUPKR0MPOU8FGXBT394"),
        (IdentifierType.TICKER, " brk.b ", "BRK.B"),
        (IdentifierType.MIC, " xnas ", "XNAS"),
        (IdentifierType.CUSIP, " 037833100 ", "037833100"),
        (IdentifierType.ISIN, " us0378331005 ", "US0378331005"),
        (IdentifierType.FIGI, " bbg000b9xry4 ", "BBG000B9XRY4"),
        (IdentifierType.COMPOSITE_FIGI, " bbg000b9xry4 ", "BBG000B9XRY4"),
        (IdentifierType.SHARE_CLASS_FIGI, " bbg000b9xry4 ", "BBG000B9XRY4"),
        (IdentifierType.NORGATE_ASSETID, " 12345 ", "12345"),
    ],
)
def test_normalize_identifier_normalizes_every_type(
    identifier_type: IdentifierType, source_value: str, expected: str
) -> None:
    assert normalize_identifier(identifier_type, source_value) == expected


@pytest.mark.parametrize(
    ("identifier_type", "value"),
    [
        (IdentifierType.ISIN, "US0378331005"),
        (IdentifierType.ISIN, "US5949181045"),
        (IdentifierType.ISIN, "GB0002634946"),
        (IdentifierType.CUSIP, "037833100"),
        (IdentifierType.CUSIP, "594918104"),
        (IdentifierType.CUSIP, "17275R102"),
        (IdentifierType.LEI, "HWUPKR0MPOU8FGXBT394"),
        (IdentifierType.LEI, "549300DTUYXVMJXZNY75"),
        (IdentifierType.FIGI, "BBG000B9XRY4"),
        (IdentifierType.FIGI, "BBG000BLNNH6"),
        (IdentifierType.FIGI, "BBG001S5N8V8"),
    ],
)
def test_real_world_identifiers_are_accepted(identifier_type: IdentifierType, value: str) -> None:
    assert normalize_identifier(identifier_type, value) == value


@pytest.mark.parametrize(
    ("identifier_type", "value"),
    [
        (IdentifierType.LEI, "HWUPKR0MPOU8FGXBT394"),
        (IdentifierType.CUSIP, "037833100"),
        (IdentifierType.ISIN, "US0378331005"),
        (IdentifierType.FIGI, "BBG000B9XRY4"),
        (IdentifierType.COMPOSITE_FIGI, "BBG000B9XRY4"),
        (IdentifierType.SHARE_CLASS_FIGI, "BBG000B9XRY4"),
    ],
)
def test_checksum_tampering_is_rejected(identifier_type: IdentifierType, value: str) -> None:
    tampered = f"{value[:-1]}{'0' if value[-1] != '0' else '1'}"

    with pytest.raises(IdentifierValueError, match="checksum"):
        normalize_identifier(identifier_type, tampered)


@pytest.mark.parametrize(
    ("identifier_type", "value", "message"),
    [
        (IdentifierType.CIK, "12345678901", "at most 10 digits"),
        (IdentifierType.LEI, "HWUPKR0MPOU8FGXBT39", "invalid syntax"),
        (IdentifierType.TICKER, "BRK B", "invalid syntax"),
        (IdentifierType.MIC, "XNA", "invalid syntax"),
        (IdentifierType.CUSIP, "0378331000", "invalid syntax"),
        (IdentifierType.ISIN, "US03783310055", "invalid syntax"),
        (IdentifierType.FIGI, "BBG000B9XRA4", "invalid syntax"),
        (IdentifierType.NORGATE_ASSETID, "12A45", "digits"),
        (IdentifierType.NORGATE_ASSETID, "0012345", "leading zeros"),
    ],
)
def test_identifier_syntax_violations_are_rejected(
    identifier_type: IdentifierType, value: str, message: str
) -> None:
    with pytest.raises(IdentifierValueError, match=message):
        normalize_identifier(identifier_type, value)


@pytest.mark.parametrize(
    ("identifier_type", "source_value"),
    [
        (IdentifierType.TICKER, "AAPL"),
        (IdentifierType.ISIN, "US0378331005"),
        (IdentifierType.CUSIP, "037833100"),
        (IdentifierType.FIGI, "BBG000B9XRY4"),
        (IdentifierType.MIC, "XNAS"),
        (IdentifierType.NORGATE_ASSETID, "12345"),
    ],
)
def test_identifier_assertion_enforces_entity_level(
    identifier_type: IdentifierType, source_value: str
) -> None:
    with pytest.raises(ValueError, match="identifies"):
        _assertion(
            entity_type=EntityType.ISSUER,
            identifier_type=identifier_type,
            source_value=source_value,
        )

    assert (
        _assertion(identifier_type=identifier_type, source_value=source_value).entity_type
        is EntityType.INSTRUMENT
    )


@pytest.mark.parametrize(
    ("identifier_type", "source_value"),
    [(IdentifierType.CIK, "320193"), (IdentifierType.LEI, "HWUPKR0MPOU8FGXBT394")],
)
def test_issuer_identifiers_require_issuer_entities(
    identifier_type: IdentifierType, source_value: str
) -> None:
    with pytest.raises(ValueError, match="identifies"):
        _assertion(identifier_type=identifier_type, source_value=source_value)

    assertion = _assertion(
        entity_type=EntityType.ISSUER,
        entity_id="issuer-apple",
        identifier_type=identifier_type,
        source_value=source_value,
    )
    assert assertion.entity_type is EntityType.ISSUER


@pytest.mark.parametrize("record_factory", [_assertion, _mapping])
@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (_START.replace(tzinfo=None), _END, "effective_start"),
        (_START, _END.replace(tzinfo=None), "effective_end"),
        (_START, _START, "strictly after"),
        (_END, _START, "strictly after"),
    ],
)
def test_records_enforce_timezone_aware_nonempty_intervals(
    record_factory: _RecordFactory, start: datetime, end: datetime, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        record_factory(effective_start=start, effective_end=end)


@pytest.mark.parametrize("record_factory", [_assertion, _mapping])
def test_records_accept_open_ended_intervals(record_factory: _RecordFactory) -> None:
    assert record_factory(effective_end=None).effective_end is None


def test_interval_helpers_use_half_open_semantics() -> None:
    adjacent_start = _END
    adjacent_end = _END + timedelta(days=31)

    assert interval_contains(_START, _END, _START)
    assert not interval_contains(_START, _END, _END)
    assert not intervals_overlap(_START, _END, adjacent_start, adjacent_end)
    assert intervals_overlap(_START, _END, _START, _END)
    assert intervals_overlap(_START, None, adjacent_start, adjacent_end)


def test_identifier_assertion_digest_is_stable_and_evidence_aware() -> None:
    assertion = _assertion()

    assert assertion.assertion_sha256 == identifier_assertion_digest(assertion)
    assert _assertion().assertion_sha256 == assertion.assertion_sha256
    for changed in (
        _assertion(entity_id="instrument-microsoft-common"),
        _assertion(source_value="US5949181045"),
        _assertion(source_snapshot_id="snapshot-2"),
        _assertion(effective_start=_START + timedelta(days=1)),
        _assertion(effective_end=_END + timedelta(days=1)),
        _assertion(evidence={"source": "other"}),
    ):
        assert changed.assertion_sha256 != assertion.assertion_sha256


def test_provider_mapping_digest_is_stable_and_evidence_aware() -> None:
    mapping = _mapping()

    assert mapping.mapping_sha256 == provider_mapping_digest(mapping)
    assert _mapping().mapping_sha256 == mapping.mapping_sha256
    for changed in (
        _mapping(instrument_id="instrument-microsoft-common"),
        _mapping(provider="other-provider"),
        _mapping(namespace="other-namespace"),
        _mapping(provider_identifier="other-key"),
        _mapping(source_snapshot_id="snapshot-2"),
        _mapping(effective_start=_START + timedelta(days=1)),
        _mapping(effective_end=_END + timedelta(days=1)),
        _mapping(evidence={"source": "other"}),
    ):
        assert changed.mapping_sha256 != mapping.mapping_sha256


def test_digests_normalize_equivalent_instants_to_utc() -> None:
    korean_time = timezone(timedelta(hours=9))

    assert (
        _assertion(
            effective_start=_START.astimezone(korean_time),
            effective_end=_END.astimezone(korean_time),
            asserted_at_utc=_ASSERTED_AT.astimezone(korean_time),
        ).assertion_sha256
        == _assertion().assertion_sha256
    )
    assert (
        _mapping(
            effective_start=_START.astimezone(korean_time),
            effective_end=_END.astimezone(korean_time),
            asserted_at_utc=_ASSERTED_AT.astimezone(korean_time),
        ).mapping_sha256
        == _mapping().mapping_sha256
    )


@pytest.mark.parametrize("record_factory", [_assertion, _mapping])
@pytest.mark.parametrize(
    "evidence",
    [{"api_key": "not-a-real-secret"}, {"note": "Bearer abcdef123456"}, {"bad": {1, 2}}],
)
def test_record_evidence_rejects_credentials_and_non_json_values(
    record_factory: _RecordFactory, evidence: object
) -> None:
    with pytest.raises((TypeError, ValueError), match=r"credential|JSON-compatible"):
        record_factory(evidence=evidence)


@pytest.mark.parametrize("record_factory", [_assertion, _mapping])
def test_record_evidence_is_immutable_and_detached(record_factory: _RecordFactory) -> None:
    supplied = {"nested": {"items": ["original"]}}
    record = record_factory(evidence=supplied)
    supplied["nested"]["items"].append("later")

    assert record.evidence == {"nested": {"items": ("original",)}}
    with pytest.raises(TypeError):
        record.evidence["new"] = "value"  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        record.evidence["nested"]["new"] = "value"  # ty: ignore[invalid-assignment]


@pytest.mark.parametrize(
    ("factory", "overrides", "message"),
    [
        (Issuer, {"issuer_id": " "}, "issuer_id"),
        (Instrument, {"instrument_id": " "}, "instrument_id"),
        (Instrument, {"issuer_id": " "}, "issuer_id"),
        (Issuer, {"schema_version": 0}, "schema_version"),
        (Instrument, {"schema_version": 0}, "schema_version"),
    ],
)
def test_issuer_and_instrument_reject_empty_ids_and_invalid_schema_versions(
    factory: _RecordFactory, overrides: dict[str, object], message: str
) -> None:
    issuer_values = {"issuer_id": "issuer-apple", "created_at_utc": _ASSERTED_AT}
    instrument_values = {
        "instrument_id": "instrument-apple-common",
        "issuer_id": "issuer-apple",
        "instrument_kind": InstrumentKind.EQUITY,
        "created_at_utc": _ASSERTED_AT,
    }
    values = issuer_values if factory is Issuer else instrument_values

    with pytest.raises(ValueError, match=message):
        factory(**(values | overrides))


def test_issuer_and_instrument_validate_domain_fields() -> None:
    assert Issuer("issuer-apple", _ASSERTED_AT, jurisdiction=" us ").jurisdiction == "US"
    with pytest.raises(ValueError, match="ISO 3166"):
        Issuer("issuer-apple", _ASSERTED_AT, jurisdiction="USA")
    with pytest.raises(ValueError, match="instrument_kind"):
        Instrument(
            "instrument-apple-common",
            "issuer-apple",
            "bond",  # ty: ignore[invalid-argument-type]
            _ASSERTED_AT,
        )
    with pytest.raises(ValueError, match="created_at_utc"):
        Issuer("issuer-apple", _ASSERTED_AT.replace(tzinfo=None))
    with pytest.raises(ValueError, match="created_at_utc"):
        Instrument(
            "instrument-apple-common",
            "issuer-apple",
            InstrumentKind.EQUITY,
            _ASSERTED_AT.replace(tzinfo=None),
        )
