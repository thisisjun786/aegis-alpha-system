"""Recurring scope identity shared by authorization and resume validation."""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Final

from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_live_authorization import _resume_scope
from aegis_alpha.data.serialization import canonical_json_bytes

RECURRING_SCOPE_CONTRACT: Final = "aegis-alpha/fmp-recurring-scope/v1"


def recurring_scope_sha256(  # noqa: PLR0913
    *,
    authority_payload_sha256: str,
    attempt_index: int,
    command: str,
    dataset_selection: str,
    manifest_sha256: str | None,
    output_path: Path,
    schedule_id: str,
    service_day: date,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "authority_payload_sha256": authority_payload_sha256,
                "attempt_index": attempt_index,
                "command": command,
                "dataset_selection": dataset_selection,
                "manifest_sha256": manifest_sha256,
                "output_path": str(output_path),
                "schedule_id": schedule_id,
                "service_day": service_day.isoformat(),
            }
        )
    ).hexdigest()


def confine_recurring_path(root: Path, candidate: Path, label: str) -> Path:
    resolved_root = root.resolve(strict=False)
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(resolved_root):
        raise PreconditionError(f"{label} escapes the signed recurring output root")
    return resolved


def validate_recurring_resume_scope(
    raw_store_root: Path,
    run_id: str,
    scope_sha256: str,
) -> None:
    _resume_scope(
        raw_store_root,
        run_id,
        scope_sha256,
        scope_sha256_key="recurring_scope_sha256",
        scope_contract_key="recurring_scope_contract",
        expected_scope_contract=RECURRING_SCOPE_CONTRACT,
    )
