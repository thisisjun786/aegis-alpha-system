"""Standing-authority loading for the daily FMP operator entry point."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_live_authorization import OWNER_AUTHORITY_ENV
from aegis_alpha.data.fmp_owner_authority import load_owner_approval_authority
from aegis_alpha.data.fmp_recurring_artifacts import load_recurring_authority
from aegis_alpha.data.fmp_recurring_authority import (
    VerifiedRecurringAuthority,
    recurring_authority_issued_at,
    verify_recurring_authority,
)


def verified_daily_authority(
    arguments: argparse.Namespace,
    environment: Mapping[str, str],
    moment: datetime,
) -> VerifiedRecurringAuthority:
    authority_path = environment.get(OWNER_AUTHORITY_ENV)
    if not authority_path:
        raise PreconditionError(f"{OWNER_AUTHORITY_ENV} is required")
    payload, signature = load_recurring_authority(
        arguments.recurring_authority,
        arguments.recurring_authority_signature,
    )
    trust = load_owner_approval_authority(
        Path(authority_path),
        recurring_authority_issued_at(payload),
    )
    return verify_recurring_authority(payload, signature, trust, now=moment)
