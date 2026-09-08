"""Native Qveris raw collection; no legacy database configuration is opened."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.data.qveris import InvocationBudget, RequestBudgetPort
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_client import QverisClient
from aegis_alpha.data.qveris_contracts import load_jobs
from aegis_alpha.data.sec_evidence import read_bytes

if TYPE_CHECKING:
    from aegis_alpha.application.provider_config import ProviderProfile
    from aegis_alpha.data.qveris_billing import QverisPort


_REQUEST_ADMISSION_SECONDS = 900
_AUDIT_REQUESTS_PER_EXECUTION = 16
_MIN_HTTP_REQUESTS = 32


def run_qveris_profile(
    profile: ProviderProfile, *, client: QverisPort | None = None
) -> dict[str, object]:
    port = None
    try:
        budget = InvocationBudget(profile.max_calls, Decimal(str(profile.options["max_credits"])))
        payload = read_bytes(Path(str(profile.options["jobs"])))
        if hashlib.sha256(payload).hexdigest() != profile.options["jobs_sha256"]:
            raise ValueError("Qveris jobs manifest hash differs")  # noqa: TRY301 -- sanitized boundary
        jobs = load_jobs(payload)
        root = Path(str(profile.options["raw_store_root"]))
        if client is None:
            if profile.credential_file is None:
                raise ValueError("Qveris requires a private credential file")  # noqa: TRY301
            client = QverisClient(
                profile.credential_file,
                timeout_seconds=float(str(profile.options["timeout_seconds"])),
            )
        port = RequestBudgetPort(
            client,
            max_requests=max(_MIN_HTTP_REQUESTS, profile.max_calls * _AUDIT_REQUESTS_PER_EXECUTION),
            seconds=_REQUEST_ADMISSION_SECONDS,
        )
        result = acquire_jobs(jobs, root, port, budget=budget)
    except (OSError, ValueError, TypeError, RuntimeError) as error:
        return {
            "provider": "qveris",
            "status": "failed",
            "exit_code": 1,
            "execution_started": port is not None,
            "error_type": type(error).__name__,
            "message": "collection stopped; inspect durable Qveris page evidence",
            "result": {
                "provider_calls": 0 if port is None else port.paid_executions,
                "http_requests": 0 if port is None else port.http_requests,
            },
        }
    return {
        "provider": "qveris",
        "status": "succeeded" if result["status"] == "RAW_ACQUIRED" else "failed",
        "exit_code": 0 if result["status"] == "RAW_ACQUIRED" else 1,
        "execution_started": True,
        "result": {
            **result,
            "provider_calls": result["provider_calls_this_run"],
            "paid_executions": port.paid_executions,
            "http_requests": port.http_requests,
            "max_http_requests": port.max_requests,
            "request_admission_seconds": _REQUEST_ADMISSION_SECONDS,
            "source_only": True,
            "native_import_completed": False,
            "reserved_credits": str(budget.reserved_credits),
        },
    }
