"""CIK admission: skip without CIK, fail-closed conflict, pad short values."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sec_collector_support import AS_OF, load_named_identity

from aegis_alpha.data.sec_identity import (
    AdmissionState,
    ConflictReason,
    IdentityError,
    IdentitySnapshot,
    InstrumentCikClaim,
    MappingConflictView,
    admit_universe,
    pad_cik,
)


def test_pad_cik_keeps_ten_digits_and_rejects_non_digits() -> None:
    assert pad_cik("990001") == "0000990001"
    assert pad_cik("0000990001") == "0000990001"
    with pytest.raises(IdentityError):
        pad_cik("not-a-cik")


def test_instrument_without_cik_is_skipped_and_does_not_search_edgar() -> None:
    snapshot = load_named_identity("identity_no_cik.json")

    admissions = admit_universe(snapshot, None, snapshot.as_of)

    assert len(admissions) == 1
    assert admissions[0].state is AdmissionState.SKIPPED_NO_CIK
    assert admissions[0].cik is None
    assert admissions[0].instrument_id == "inst-no-cik"


def test_conflicting_cik_is_fail_closed_and_does_not_admit() -> None:
    snapshot = load_named_identity("identity_conflict.json")

    admissions = admit_universe(snapshot, None, snapshot.as_of)

    assert {item.state for item in admissions} == {AdmissionState.SKIPPED_CONFLICT}
    assert all(item.state is not AdmissionState.ADMITTED for item in admissions)
    reasons = {item.reason for item in admissions}
    assert reasons <= {
        ConflictReason.MAPPING_CONFLICT.value,
        ConflictReason.CIK_BOUND_TO_MULTIPLE_INSTRUMENTS.value,
    }


def test_same_cik_on_two_instruments_is_fail_closed_without_conflict_table() -> None:
    start = datetime(2020, 1, 1, tzinfo=UTC)
    snapshot = IdentitySnapshot(
        as_of=AS_OF,
        claims=(
            InstrumentCikClaim("inst-a", "issuer-a", "42", start, None),
            InstrumentCikClaim("inst-b", "issuer-b", "42", start, None),
        ),
        conflicts=(),
    )

    admissions = admit_universe(snapshot, None, AS_OF)

    assert all(item.state is AdmissionState.SKIPPED_CONFLICT for item in admissions)
    assert all(
        item.reason == ConflictReason.CIK_BOUND_TO_MULTIPLE_INSTRUMENTS.value for item in admissions
    )


def test_mapping_conflict_blocks_even_a_single_instrument() -> None:
    start = datetime(2020, 1, 1, tzinfo=UTC)
    snapshot = IdentitySnapshot(
        as_of=AS_OF,
        claims=(InstrumentCikClaim("inst-a", "issuer-a", "42", start, None),),
        conflicts=(
            MappingConflictView(
                provider="sec",
                namespace="cik",
                provider_identifier="0000000042",
                attempted_instrument_id="inst-a",
                existing_instrument_id="inst-other",
            ),
        ),
    )

    admission = snapshot.admit("inst-a", AS_OF)

    assert admission.state is AdmissionState.SKIPPED_CONFLICT
    assert admission.reason == ConflictReason.MAPPING_CONFLICT.value


def test_admitted_virtual_cik_is_padded_and_source_is_preserved() -> None:
    snapshot = load_named_identity("identity_admitted.json")

    admission = snapshot.admit("inst-synth-common", snapshot.as_of)

    assert admission.state is AdmissionState.ADMITTED
    assert admission.cik == "0000990001"
    assert admission.cik_source == "990001"
