"""Run-bound owner approval checks for every live FMP invocation."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from aegis_alpha.data.fmp_cli_artifacts import ApprovalArtifact, PreconditionError
from aegis_alpha.data.fmp_owner_approval import VerifiedOwnerApproval

_APPROVED_RUN_ID = re.compile(r"fmp-run-[0-9a-f]{32}")


class ApprovalExpiredError(RuntimeError):
    """A running collection lost its bounded approval window."""


class RequestApproval(Protocol):
    """Authority checked immediately before each durable or provider boundary."""

    def require_request(self) -> None:
        """Fail when the authority no longer permits another request."""


@dataclass(frozen=True, slots=True)
class BoundApproval:
    """Verified approval evidence bound to one approved run and clock."""

    approval: VerifiedOwnerApproval
    clock: Callable[[], datetime]

    def require_request(self) -> None:
        if self.approval.expires_at_utc <= self.clock():
            raise ApprovalExpiredError("signed owner approval expired during collection")


def approved_candidate_run_id(artifact: ApprovalArtifact) -> str:
    """Return the exact normalized run identity approved by the operator."""

    if _APPROVED_RUN_ID.fullmatch(artifact.run_identity) is None:
        raise PreconditionError(
            "contract-review artifact run_identity must be a canonical fmp-run UUID"
        )
    return artifact.run_identity


def require_approved_identity(approved: str | None, effective: str) -> None:
    """Reject resume selection that would change an approved identity."""

    if approved is not None and effective != approved:
        raise PreconditionError("resume run identity does not match the approved run identity")


def fail_closed_if_unconsumed_expiry(
    error: ApprovalExpiredError,
    *,
    consumed_new_slot: bool,
    durable_boundary_committed: bool,
) -> None:
    """Propagate expiry when replay or marker-only finalization consumed no new slot."""

    if durable_boundary_committed or not consumed_new_slot:
        raise error


def bind_verified_approval(
    approval: VerifiedOwnerApproval, run_identity: str, *, clock: Callable[[], datetime]
) -> BoundApproval:
    """Bind authenticated owner authority to its exact candidate run."""

    if approval.run_identity != run_identity:
        raise PreconditionError("signed owner approval does not match the candidate run ID")
    return BoundApproval(approval, clock)
