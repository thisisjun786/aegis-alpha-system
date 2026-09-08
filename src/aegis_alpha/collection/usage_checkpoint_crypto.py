from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_errors import (
    CheckpointContractError,
    MalformedEncodingError,
    SignatureVerificationError,
    StaleVerificationKeyError,
    UnknownVerificationKeyError,
    UnsupportedContractError,
)
from aegis_alpha.collection.usage_checkpoint_schema import (
    SignedUsageCheckpoint,
    validate_checkpoint_leaves,
)

_PUBLIC_KEY_BYTES = 32


@dataclass(frozen=True, slots=True)
class TrustedEd25519PublicKey:
    """Public verification material with an optional half-open trust interval."""

    encoded: bytes
    valid_from_utc: datetime | None = None
    valid_until_utc: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.encoded) is not bytes or len(self.encoded) != _PUBLIC_KEY_BYTES:
            raise MalformedEncodingError(material="Ed25519 public key")
        for value in (self.valid_from_utc, self.valid_until_utc):
            if value is not None and (
                value.utcoffset() is None or value.utcoffset() != UTC.utcoffset(None)
            ):
                raise ValueError("verification key validity bounds must be UTC timestamps")
        if (
            self.valid_from_utc is not None
            and self.valid_until_utc is not None
            and self.valid_until_utc <= self.valid_from_utc
        ):
            raise ValueError("verification key validity interval must be increasing")

    def valid_at(self, generated_at_utc: datetime) -> bool:
        return (self.valid_from_utc is None or generated_at_utc >= self.valid_from_utc) and (
            self.valid_until_utc is None or generated_at_utc < self.valid_until_utc
        )


@dataclass(frozen=True, slots=True)
class _ParsedVerificationKey:
    public_key: Ed25519PublicKey
    trust: TrustedEd25519PublicKey


@dataclass(frozen=True, slots=True, init=False)
class Ed25519PublicKeyring:
    """Immutable authority/key-id lookup containing public verification material only."""

    _keys: Mapping[tuple[str, str], _ParsedVerificationKey]

    def __init__(
        self,
        keys: Mapping[tuple[str, str], bytes | TrustedEd25519PublicKey],
    ) -> None:
        parsed: dict[tuple[str, str], _ParsedVerificationKey] = {}
        for identity, material in keys.items():
            trust = TrustedEd25519PublicKey(material) if type(material) is bytes else material
            if not isinstance(trust, TrustedEd25519PublicKey):
                raise MalformedEncodingError(material="Ed25519 public key")
            parsed[identity] = _ParsedVerificationKey(
                Ed25519PublicKey.from_public_bytes(trust.encoded), trust
            )
        object.__setattr__(self, "_keys", MappingProxyType(parsed))

    def public_key(
        self,
        authority_id: str,
        key_id: str,
        generated_at_utc: datetime | None = None,
    ) -> Ed25519PublicKey:
        try:
            parsed = self._keys[(authority_id, key_id)]
        except KeyError:
            raise UnknownVerificationKeyError from None
        if generated_at_utc is not None and not parsed.trust.valid_at(generated_at_utc):
            raise StaleVerificationKeyError
        return parsed.public_key


def verify_usage_checkpoint_signature(
    signed: SignedUsageCheckpoint,
    keyring: Ed25519PublicKeyring,
) -> None:
    """Authenticate persisted checkpoint metadata without asserting live-row integrity."""

    checkpoint = signed.checkpoint
    if checkpoint.signature_algorithm != "ed25519" or checkpoint.signature_version != 1:
        raise UnsupportedContractError(field="signature contract", requirement="is unsupported")
    public_key = keyring.public_key(
        checkpoint.authority_id,
        checkpoint.key_id,
        checkpoint.generated_at_utc,
    )
    try:
        public_key.verify(signed.signature, checkpoint.canonical_bytes())
    except InvalidSignature:
        raise SignatureVerificationError from None


def verify_usage_checkpoint(
    signed: SignedUsageCheckpoint,
    leaves: Sequence[UsageRecordLeaf],
    keyring: Ed25519PublicKeyring,
) -> None:
    """Verify authority signature and the exact ordered usage-record commitment."""

    leaf_tuple = tuple(leaves)
    verify_usage_checkpoint_signature(signed, keyring)
    try:
        validate_checkpoint_leaves(signed.checkpoint, leaf_tuple)
    except CheckpointContractError:
        raise SignatureVerificationError from None
