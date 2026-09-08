"""Durable recurring authority audit published under a live clock."""

from __future__ import annotations

from datetime import date, datetime

from aegis_alpha.data.fmp_collector import publish_bundle
from aegis_alpha.data.fmp_live_authorization import LiveAuthorization
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority
from aegis_alpha.data.fmp_recurring_scope import validate_recurring_resume_scope
from aegis_alpha.data.serialization import canonical_json_bytes


def publish_recurring_audit(
    *,
    authority: VerifiedRecurringAuthority,
    authorization: LiveAuthorization,
    attempt_index: int,
    service_day: date,
    now: datetime,
) -> None:
    """Recheck live authority before publishing its durable run evidence."""

    authority.require_request(now)
    scope_sha256 = authorization.artifact_hashes["recurring_scope_sha256"]
    validate_recurring_resume_scope(
        authorization.raw_store_root,
        authorization.run_id,
        scope_sha256,
    )
    audit = canonical_json_bytes(
        {
            "authority_artifact_sha256": authority.authority_artifact_sha256,
            "attempt_index": attempt_index,
            "recurring_authority_payload_sha256": authority.payload_sha256,
            "recurring_authority_signature_sha256": authority.signature_sha256,
            "run_identity": authorization.run_id,
            "scope_sha256": scope_sha256,
            "service_day": service_day.isoformat(),
        }
    )
    _ = publish_bundle(
        [
            (
                authorization.raw_store_root
                / "fmp"
                / "runs"
                / authorization.run_id
                / f"recurring-authority-audit-{authority.payload_sha256}.json",
                audit,
            )
        ]
    )
