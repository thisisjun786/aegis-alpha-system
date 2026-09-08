"""Native Qveris raw collection; no legacy database configuration is opened."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_client import QverisClient
from aegis_alpha.data.qveris_contracts import load_jobs
from aegis_alpha.data.sec_evidence import read_bytes

if TYPE_CHECKING:
    from aegis_alpha.application.provider_config import ProviderProfile
    from aegis_alpha.data.qveris_billing import QverisPort


def run_qveris_profile(
    profile: ProviderProfile, *, client: QverisPort | None = None
) -> dict[str, object]:
    budget = InvocationBudget(profile.max_calls, Decimal(str(profile.options["max_credits"])))
    payload = read_bytes(Path(str(profile.options["jobs"])))
    if hashlib.sha256(payload).hexdigest() != profile.options["jobs_sha256"]:
        raise ValueError("Qveris jobs manifest hash differs")
    jobs = load_jobs(payload)
    root = Path(str(profile.options["raw_store_root"]))
    if client is None:
        if profile.credential_file is None:
            raise ValueError("Qveris requires a private credential file")
        client = QverisClient(
            profile.credential_file,
            timeout_seconds=float(str(profile.options["timeout_seconds"])),
        )
    try:
        result = acquire_jobs(jobs, root, client, budget=budget)
    except (OSError, ValueError, TypeError, RuntimeError) as error:
        return {
            "provider": "qveris",
            "status": "failed",
            "exit_code": 1,
            "execution_started": True,
            "error_type": type(error).__name__,
            "message": "collection stopped; inspect durable Qveris page evidence",
            "result": {"provider_calls": None, "reserved_calls": budget.reserved_calls},
        }
    return {
        "provider": "qveris",
        "status": "succeeded" if result["status"] == "RAW_ACQUIRED" else "failed",
        "exit_code": 0 if result["status"] == "RAW_ACQUIRED" else 1,
        "execution_started": True,
        "result": {
            **result,
            "provider_calls": result["provider_calls_this_run"],
            "source_only": True,
            "native_import_completed": False,
            "reserved_credits": str(budget.reserved_credits),
        },
    }
