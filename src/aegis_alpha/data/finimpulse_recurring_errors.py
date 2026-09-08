"""Typed standing-authority errors for FinImpulse probe collection."""


class RecurringAuthorityError(ValueError):
    """The standing probe scope is malformed, invalid, revoked, or outside its grant."""
