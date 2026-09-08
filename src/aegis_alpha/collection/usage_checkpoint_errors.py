from __future__ import annotations


class CheckpointContractError(ValueError):
    """A typed checkpoint contract violation with stable field context."""

    def __init__(self, *, field: str, requirement: str) -> None:
        self.field = field
        self.requirement = requirement
        super().__init__(field, requirement)

    def __str__(self) -> str:
        return f"{self.field} {self.requirement}"


class UnsupportedContractError(CheckpointContractError):
    """A checkpoint selects a contract version or algorithm this code does not support."""


class MalformedEncodingError(ValueError):
    """Encoded checkpoint material has an invalid shape or size."""

    def __init__(self, *, material: str) -> None:
        self.material = material
        super().__init__(material)

    def __str__(self) -> str:
        return f"{self.material} has malformed encoding"


class UnknownVerificationKeyError(LookupError):
    def __str__(self) -> str:
        return "no trusted public key matches the checkpoint authority and key identifiers"


class StaleVerificationKeyError(ValueError):
    def __str__(self) -> str:
        return "the trusted public key is not valid at the checkpoint generation time"


class SignatureVerificationError(ValueError):
    def __str__(self) -> str:
        return "usage checkpoint verification failed"


class ProviderUsageIntegrityError(ValueError):
    """Authenticated checkpoint metadata disagrees with current provider usage."""
