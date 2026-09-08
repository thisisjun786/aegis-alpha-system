"""Strict field parsers for the recurring FMP authority contract."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final

from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9:._-]{2,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_UTC_FORMAT: Final = "%Y-%m-%dT%H:%M:%S.%fZ"


def identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise RecurringAuthorityError(f"recurring authority {label} is invalid")
    return value


def timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise RecurringAuthorityError(f"recurring authority {label} must be UTC text")
    try:
        return datetime.strptime(value, _UTC_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise RecurringAuthorityError(
            f"recurring authority {label} must use canonical UTC text"
        ) from None


def absolute_path(value: object, label: str) -> Path:
    if not isinstance(value, str):
        raise RecurringAuthorityError(f"recurring authority {label} must be a path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or str(path) != value:
        raise RecurringAuthorityError(f"recurring authority {label} must be canonical absolute")
    return path


def sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RecurringAuthorityError(f"recurring authority {label} is invalid")
    return value


def snapshot_range(snapshot: object, backfill: object) -> tuple[date, date]:
    if not isinstance(snapshot, str) or not isinstance(backfill, str):
        raise RecurringAuthorityError("recurring snapshot dates must be ISO text")
    try:
        snapshot_date = date.fromisoformat(snapshot)
        backfill_from = date.fromisoformat(backfill)
    except ValueError:
        raise RecurringAuthorityError("recurring snapshot dates are invalid") from None
    if backfill_from != snapshot_date + timedelta(days=1):
        raise RecurringAuthorityError("recurring backfill must start after Norgate snapshot")
    return snapshot_date, backfill_from
