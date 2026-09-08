"""Typed standing-authority errors for FRED/ALFRED collection."""


class RecurringAuthorityError(ValueError):
    """The standing authority is malformed, invalid, revoked, or outside its grant."""


class DailyBudgetError(RecurringAuthorityError):
    """The signed UTC-day call budget cannot be read, reserved, or remaining."""
