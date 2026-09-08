"""Request-time binding for verified recurring FMP authority."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from aegis_alpha.data.fmp_approval import RequestApproval
from aegis_alpha.data.fmp_recurring_authority import VerifiedRecurringAuthority


@dataclass(frozen=True, slots=True)
class BoundRecurringApproval(RequestApproval):
    authority: VerifiedRecurringAuthority
    service_day: date
    clock: Callable[[], datetime]

    def require_request(self) -> None:
        self.authority.require_request(self.clock())
