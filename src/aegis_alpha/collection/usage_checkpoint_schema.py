from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict

from aegis_alpha.collection.usage_checkpoint import (
    UsageCheckpoint,
    UsageRecordLeaf,
    usage_records_root,
)
from aegis_alpha.collection.usage_checkpoint_errors import (
    CheckpointContractError,
    MalformedEncodingError,
)

_SIGNATURE_BYTES = 64


def validate_checkpoint_leaves(
    checkpoint: UsageCheckpoint,
    leaves: Sequence[UsageRecordLeaf],
) -> None:
    """Validate every cross-field invariant between a checkpoint and its leaves."""

    for leaf in leaves:
        if leaf.provider != checkpoint.provider:
            raise CheckpointContractError(
                field="usage leaf provider",
                requirement="must match the checkpoint provider",
            )
        if not checkpoint.coverage_start_utc <= leaf.recorded_at_utc < checkpoint.coverage_end_utc:
            raise CheckpointContractError(
                field="usage leaf timestamp",
                requirement="must be inside checkpoint coverage [start,end)",
            )
    if len(leaves) != checkpoint.usage_record_count:
        raise CheckpointContractError(
            field="usage leaves", requirement="must match the checkpoint record count"
        )
    if usage_records_root(leaves) != checkpoint.usage_records_root_sha256:
        raise CheckpointContractError(
            field="usage leaves", requirement="must match the checkpoint records root"
        )


class CheckpointSchemaFields(TypedDict):
    checkpoint_id: str
    schema_version: int
    provider: str
    coverage_start_utc: datetime
    coverage_end_utc: datetime
    usage_record_count: int
    usage_records_root_sha256: str
    authority_id: str
    key_id: str
    generated_at_utc: datetime
    signature_algorithm: str
    signature_version: int
    signature: bytes


@dataclass(frozen=True, slots=True)
class SignedUsageCheckpoint:
    checkpoint: UsageCheckpoint
    signature: bytes

    def __post_init__(self) -> None:
        if type(self.signature) is not bytes or len(self.signature) != _SIGNATURE_BYTES:
            raise MalformedEncodingError(material="Ed25519 signature")

    def to_schema_fields(self) -> CheckpointSchemaFields:
        checkpoint = self.checkpoint
        return {
            "checkpoint_id": checkpoint.checkpoint_id,
            "schema_version": checkpoint.schema_version,
            "provider": checkpoint.provider,
            "coverage_start_utc": checkpoint.coverage_start_utc,
            "coverage_end_utc": checkpoint.coverage_end_utc,
            "usage_record_count": checkpoint.usage_record_count,
            "usage_records_root_sha256": checkpoint.usage_records_root_sha256,
            "authority_id": checkpoint.authority_id,
            "key_id": checkpoint.key_id,
            "generated_at_utc": checkpoint.generated_at_utc,
            "signature_algorithm": checkpoint.signature_algorithm,
            "signature_version": checkpoint.signature_version,
            "signature": self.signature,
        }

    @classmethod
    def from_schema_metadata(cls, fields: CheckpointSchemaFields) -> SignedUsageCheckpoint:
        """Reconstruct the signed envelope without claiming its live leaves were checked."""

        checkpoint = UsageCheckpoint(
            checkpoint_id=fields["checkpoint_id"],
            schema_version=fields["schema_version"],
            provider=fields["provider"],
            coverage_start_utc=fields["coverage_start_utc"],
            coverage_end_utc=fields["coverage_end_utc"],
            usage_record_count=fields["usage_record_count"],
            usage_records_root_sha256=fields["usage_records_root_sha256"],
            authority_id=fields["authority_id"],
            key_id=fields["key_id"],
            generated_at_utc=fields["generated_at_utc"],
            signature_algorithm=fields["signature_algorithm"],
            signature_version=fields["signature_version"],
        )
        return cls(checkpoint=checkpoint, signature=fields["signature"])

    @classmethod
    def from_schema_fields(
        cls,
        fields: CheckpointSchemaFields,
        leaves: Sequence[UsageRecordLeaf],
    ) -> SignedUsageCheckpoint:
        leaf_tuple = tuple(leaves)
        signed = cls.from_schema_metadata(fields)
        validate_checkpoint_leaves(signed.checkpoint, leaf_tuple)
        return signed
