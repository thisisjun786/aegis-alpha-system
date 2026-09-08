"""SQLAlchemy metadata for the additive canonical-generation registry.

Wave 4 deliberately keeps this module separate from the offline generation
implementation.  Importing it only adds table objects to the repository's
shared :mod:`aegis_alpha.metadata.schema` metadata; it performs no database I/O
and grants no production authority.

The registry stores immutable evidence.  The database constraints below are
therefore intentionally conservative: text identities are non-empty, content
identities are lowercase SHA-256 values, paths are relative and traversal-safe,
and all row/count fields are non-negative.  Generation topology is typed in the
database as well as in the writer: ``genesis_v1`` is sequence zero and carries
no parent/run/delta evidence, while ``delta_v2`` is a positive sequence with a
direct parent and complete frozen evidence.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)

from aegis_alpha.collection import schema as _collection_schema  # noqa: F401
from aegis_alpha.metadata.schema import _json_type, metadata


def _sha256_check(column_name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"{column_name} IS NULL OR "
        f"(length({column_name}) = 64 AND {column_name} ~ '^[0-9a-f]{{64}}$')"
        if nullable
        else f"length({column_name}) = 64 AND {column_name} ~ '^[0-9a-f]{{64}}$'"
    )
    return CheckConstraint(expression, name=f"{column_name}_sha256")


def _nonempty_check(column_name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"{column_name} IS NULL OR btrim({column_name}) <> ''"
        if nullable
        else f"btrim({column_name}) <> ''"
    )
    return CheckConstraint(expression, name=f"{column_name}_nonempty")


def _count_check(column_name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"{column_name} IS NULL OR {column_name} >= 0" if nullable else f"{column_name} >= 0"
    )
    return CheckConstraint(expression, name=f"{column_name}_nonnegative")


def _safe_path_check(column_name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"{column_name} IS NULL OR "
        f"({column_name} !~ '(^/|(^|/)\\.\\.(/|$))' AND btrim({column_name}) <> '')"
        if nullable
        else f"{column_name} !~ '(^/|(^|/)\\.\\.(/|$))' AND btrim({column_name}) <> ''"
    )
    return CheckConstraint(expression, name=f"{column_name}_safe")


def _registered_at() -> Column[datetime]:
    return Column(
        "registered_at_utc",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


canonical_series = Table(
    "canonical_series",
    metadata,
    Column("series_id", String(255), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("contract_version", String(150), nullable=False),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    _registered_at(),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("series_id"),
    _nonempty_check("contract_version"),
    PrimaryKeyConstraint("series_id", name="pk_canonical_series"),
)


canonical_generations = Table(
    "canonical_generations",
    metadata,
    Column("generation_id", String(255), nullable=False),
    Column("series_id", String(255), nullable=False),
    Column("generation_type", String(16), nullable=False),
    Column("seq", BigInteger, nullable=False),
    Column("parent_generation_id", String(255), nullable=True),
    Column("run_id", String(255), nullable=True),
    Column("attempt_number", Integer, nullable=True),
    Column("source_snapshot_id", String(255), nullable=True),
    Column("expected_parent_history_root", String(64), nullable=True),
    Column("delta_merkle_root", String(64), nullable=True),
    Column("manifest_core_sha256", String(64), nullable=True),
    Column("history_root", String(64), nullable=False),
    Column("plan_sha256", String(64), nullable=True),
    Column("source_snapshot_sha256", String(64), nullable=True),
    Column("identity_authority_sha256", String(64), nullable=True),
    Column("row_count", BigInteger, nullable=True),
    Column("semantic_count", BigInteger, nullable=True),
    Column("partition_count", BigInteger, nullable=True),
    Column("assert_count", BigInteger, nullable=True),
    Column("supersede_count", BigInteger, nullable=True),
    Column("tombstone_count", BigInteger, nullable=True),
    Column("evidence_count", BigInteger, nullable=True),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    _registered_at(),
    CheckConstraint(
        "generation_type IN ('genesis_v1', 'delta_v2')",
        name="generation_type_allowed",
    ),
    CheckConstraint("seq >= 0", name="seq_nonnegative"),
    _nonempty_check("generation_id"),
    _nonempty_check("series_id"),
    _nonempty_check("parent_generation_id", nullable=True),
    _nonempty_check("run_id", nullable=True),
    _nonempty_check("source_snapshot_id", nullable=True),
    CheckConstraint(
        "attempt_number IS NULL OR attempt_number > 0",
        name="attempt_number_positive",
    ),
    _sha256_check("expected_parent_history_root", nullable=True),
    _sha256_check("delta_merkle_root", nullable=True),
    _sha256_check("manifest_core_sha256", nullable=True),
    _sha256_check("history_root"),
    _sha256_check("plan_sha256", nullable=True),
    _sha256_check("source_snapshot_sha256", nullable=True),
    _sha256_check("identity_authority_sha256", nullable=True),
    _count_check("row_count", nullable=True),
    _count_check("semantic_count", nullable=True),
    _count_check("partition_count", nullable=True),
    _count_check("assert_count", nullable=True),
    _count_check("supersede_count", nullable=True),
    _count_check("tombstone_count", nullable=True),
    _count_check("evidence_count", nullable=True),
    # Genesis is a schema anchor, not a collected run.  Keep all delta-only
    # evidence absent (or zero for counters) so a direct SQL writer cannot
    # smuggle a partially typed delta into sequence zero.
    CheckConstraint(
        "(generation_type = 'genesis_v1' AND seq = 0 "
        "AND parent_generation_id IS NULL AND run_id IS NULL "
        "AND attempt_number IS NULL AND source_snapshot_id IS NULL "
        "AND expected_parent_history_root IS NULL "
        "AND delta_merkle_root IS NULL AND manifest_core_sha256 IS NULL "
        "AND plan_sha256 IS NULL AND source_snapshot_sha256 IS NULL "
        "AND identity_authority_sha256 IS NULL "
        "AND (row_count IS NULL OR row_count = 0) "
        "AND (semantic_count IS NULL OR semantic_count = 0) "
        "AND (partition_count IS NULL OR partition_count = 0) "
        "AND (assert_count IS NULL OR assert_count = 0) "
        "AND (supersede_count IS NULL OR supersede_count = 0) "
        "AND (tombstone_count IS NULL OR tombstone_count = 0) "
        "AND (evidence_count IS NULL OR evidence_count = 0)) "
        "OR (generation_type = 'delta_v2' AND seq > 0 "
        "AND parent_generation_id IS NOT NULL AND run_id IS NOT NULL "
        "AND attempt_number > 0 AND source_snapshot_id IS NOT NULL "
        "AND expected_parent_history_root IS NOT NULL "
        "AND delta_merkle_root IS NOT NULL AND manifest_core_sha256 IS NOT NULL "
        "AND history_root IS NOT NULL AND plan_sha256 IS NOT NULL "
        "AND source_snapshot_sha256 IS NOT NULL "
        "AND identity_authority_sha256 IS NOT NULL "
        "AND row_count > 0 AND semantic_count > 0 AND partition_count > 0 "
        "AND assert_count IS NOT NULL AND supersede_count IS NOT NULL "
        "AND tombstone_count IS NOT NULL AND evidence_count IS NOT NULL)",
        name="typed_generation_projection",
    ),
    ForeignKeyConstraint(
        ["series_id"],
        ["canonical_series.series_id"],
        name="fk_canonical_generations_series_id_canonical_series",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["run_id"],
        ["collection_runs.run_id"],
        name="fk_canonical_generations_run_id_collection_runs",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["run_id", "attempt_number", "source_snapshot_id"],
        [
            "collection_run_receipts.run_id",
            "collection_run_receipts.attempt_number",
            "collection_run_receipts.source_snapshot_id",
        ],
        name="fk_canonical_generations_receipt_lineage",
        ondelete="NO ACTION",
    ),
    # The scalar self-FK is useful to callers inspecting the topology.  The
    # composite FK below additionally prevents a parent from another series.
    ForeignKeyConstraint(
        ["parent_generation_id"],
        ["canonical_generations.generation_id"],
        name="fk_canonical_generations_parent_generation_id_canonical_generations",
        ondelete="NO ACTION",
    ),
    UniqueConstraint(
        "series_id",
        "generation_id",
        name="uq_canonical_generations_series_generation",
    ),
    UniqueConstraint(
        "series_id",
        "history_root",
        name="uq_canonical_generations_series_history_root",
    ),
    UniqueConstraint(
        "series_id",
        "generation_id",
        "history_root",
        name="uq_canonical_generations_series_generation_history_root",
    ),
    UniqueConstraint(
        "generation_id",
        "run_id",
        "attempt_number",
        "source_snapshot_id",
        name="uq_canonical_generations_generation_lineage",
    ),
    UniqueConstraint(
        "run_id",
        "attempt_number",
        name="uq_canonical_generations_run_attempt",
    ),
    ForeignKeyConstraint(
        ["series_id", "parent_generation_id", "expected_parent_history_root"],
        [
            "canonical_generations.series_id",
            "canonical_generations.generation_id",
            "canonical_generations.history_root",
        ],
        name="fk_canonical_generations_parent_history_root",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["series_id", "parent_generation_id"],
        ["canonical_generations.series_id", "canonical_generations.generation_id"],
        name="fk_canonical_generations_parent_series_generation",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint("generation_id", name="pk_canonical_generations"),
)


Index(
    "uq_canonical_generations_series_seq",
    canonical_generations.c.series_id,
    canonical_generations.c.seq,
    unique=True,
)
Index(
    "uq_canonical_generations_series_parent",
    canonical_generations.c.series_id,
    canonical_generations.c.parent_generation_id,
    unique=True,
    postgresql_where=canonical_generations.c.parent_generation_id.is_not(None),
)


canonical_generation_sources = Table(
    "canonical_generation_sources",
    metadata,
    Column("generation_id", String(255), nullable=False),
    Column("ordinal", BigInteger, nullable=False),
    Column("source_kind", String(64), nullable=False),
    Column("source_id", String(255), nullable=False),
    Column("source_sha256", String(64), nullable=False),
    Column("run_id", String(255), nullable=False),
    Column("attempt_number", Integer, nullable=False),
    Column("source_snapshot_id", String(255), nullable=False),
    Column("relative_path", Text, nullable=True),
    Column("size_bytes", BigInteger, nullable=True),
    Column("row_count", BigInteger, nullable=True),
    _registered_at(),
    _nonempty_check("source_kind"),
    _nonempty_check("source_id"),
    CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
    _sha256_check("source_sha256"),
    _nonempty_check("run_id"),
    CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
    _nonempty_check("source_snapshot_id"),
    _safe_path_check("relative_path", nullable=True),
    _count_check("size_bytes", nullable=True),
    _count_check("row_count", nullable=True),
    ForeignKeyConstraint(
        ["generation_id"],
        ["canonical_generations.generation_id"],
        name="fk_canonical_generation_sources_generation_id_canonical_generations",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["generation_id", "run_id", "attempt_number", "source_snapshot_id"],
        [
            "canonical_generations.generation_id",
            "canonical_generations.run_id",
            "canonical_generations.attempt_number",
            "canonical_generations.source_snapshot_id",
        ],
        name="fk_canonical_generation_sources_generation_lineage",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["run_id", "attempt_number", "source_snapshot_id"],
        [
            "collection_run_receipts.run_id",
            "collection_run_receipts.attempt_number",
            "collection_run_receipts.source_snapshot_id",
        ],
        name="fk_canonical_generation_sources_receipt_lineage",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ["source_snapshot_id"],
        ["source_snapshots.snapshot_id"],
        name="fk_canonical_generation_sources_source_snapshot_id_source_snapshots",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint(
        "generation_id",
        "ordinal",
        name="pk_canonical_generation_sources",
    ),
    UniqueConstraint(
        "generation_id",
        "source_kind",
        "source_id",
        name="uq_canonical_generation_sources_identity",
    ),
)


canonical_generation_artifacts = Table(
    "canonical_generation_artifacts",
    metadata,
    Column("generation_id", String(255), nullable=False),
    Column("ordinal", BigInteger, nullable=False),
    Column("relative_path", Text, nullable=False),
    Column("artifact_kind", String(64), nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("row_count", BigInteger, nullable=True),
    Column("schema_sha256", String(64), nullable=True),
    Column("media_type", String(150), nullable=True),
    _registered_at(),
    _safe_path_check("relative_path"),
    CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
    _nonempty_check("artifact_kind"),
    CheckConstraint(
        "artifact_kind IN ('partition', 'manifest')",
        name="artifact_kind_allowed",
    ),
    _sha256_check("content_sha256"),
    _sha256_check("schema_sha256", nullable=True),
    _nonempty_check("media_type", nullable=True),
    _count_check("size_bytes"),
    _count_check("row_count", nullable=True),
    ForeignKeyConstraint(
        ["generation_id"],
        ["canonical_generations.generation_id"],
        name="fk_canonical_generation_artifacts_generation_id_canonical_generations",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint(
        "generation_id",
        "ordinal",
        name="pk_canonical_generation_artifacts",
    ),
    UniqueConstraint(
        "generation_id",
        "relative_path",
        name="uq_canonical_generation_artifacts_path",
    ),
)


canonical_partition_evidence = Table(
    "canonical_partition_evidence",
    metadata,
    Column("generation_id", String(255), nullable=False),
    Column("ordinal", BigInteger, nullable=False),
    Column("relative_path", Text, nullable=False),
    Column("section", String(64), nullable=False),
    Column("leaf_kind", String(16), nullable=False),
    Column("adjustment_basis", String(64), nullable=True),
    Column("shard_count", Integer, nullable=False),
    Column("shard_id", Integer, nullable=False),
    Column("partition_key_json", _json_type(), nullable=False),
    Column("receipt_projection_json", _json_type(), nullable=False),
    Column("receipt_sha256", String(64), nullable=False),
    Column("task_id", String(255), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("schema_sha256", String(64), nullable=False),
    Column("partition_records_sha256", String(64), nullable=False),
    Column("semantic_count", BigInteger, nullable=False),
    Column("semantic_checksum", String(64), nullable=False),
    Column("source_snapshot_sha256", String(64), nullable=False),
    Column("selected_input_sha256", String(64), nullable=False),
    Column("transformation_sha256", String(64), nullable=False),
    Column("plan_sha256", String(64), nullable=False),
    Column("identity_authority_sha256", String(64), nullable=False),
    Column("effective_min", Date, nullable=True),
    Column("effective_max", Date, nullable=True),
    Column("availability_min", DateTime(timezone=True), nullable=True),
    Column("availability_max", DateTime(timezone=True), nullable=True),
    Column("revision_min", DateTime(timezone=True), nullable=True),
    Column("revision_max", DateTime(timezone=True), nullable=True),
    Column("logical_key_min", Text, nullable=True),
    Column("logical_key_max", Text, nullable=True),
    Column("assert_count", BigInteger, nullable=False),
    Column("supersede_count", BigInteger, nullable=False),
    Column("tombstone_count", BigInteger, nullable=False),
    Column("evidence_count", BigInteger, nullable=False),
    _registered_at(),
    CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
    _nonempty_check("relative_path"),
    _safe_path_check("relative_path"),
    _nonempty_check("section"),
    _nonempty_check("leaf_kind"),
    CheckConstraint("leaf_kind IN ('state', 'evidence')", name="leaf_kind_allowed"),
    _nonempty_check("adjustment_basis", nullable=True),
    _nonempty_check("task_id"),
    CheckConstraint("shard_count > 0", name="shard_count_positive"),
    CheckConstraint("shard_id >= 0 AND shard_id < shard_count", name="shard_id_in_range"),
    CheckConstraint(
        "(shard_count & (shard_count - 1)) = 0",
        name="shard_count_power_of_two",
    ),
    CheckConstraint(
        "section IN ('corporate_actions', 'diagnostics', 'disagreements', "
        "'identity_bindings', 'prices')",
        name="section_allowed",
    ),
    CheckConstraint(
        "(leaf_kind = 'state' AND section IN ('corporate_actions', 'identity_bindings', 'prices')) "
        "OR (leaf_kind = 'evidence' AND section IN ('diagnostics', 'disagreements'))",
        name="section_leaf_kind_coherent",
    ),
    CheckConstraint(
        "(section = 'prices' AND adjustment_basis IS NOT NULL) "
        "OR (section <> 'prices' AND adjustment_basis IS NULL)",
        name="prices_adjustment_basis_coherent",
    ),
    CheckConstraint(
        "section = 'prices' OR (shard_count = 1 AND shard_id = 0)",
        name="non_price_single_shard",
    ),
    _sha256_check("content_sha256"),
    _sha256_check("receipt_sha256"),
    _sha256_check("schema_sha256"),
    _sha256_check("partition_records_sha256"),
    _sha256_check("semantic_checksum"),
    _sha256_check("source_snapshot_sha256"),
    _sha256_check("selected_input_sha256"),
    _sha256_check("transformation_sha256"),
    _sha256_check("plan_sha256"),
    _sha256_check("identity_authority_sha256"),
    CheckConstraint(
        "logical_key_min IS NULL OR (btrim(logical_key_min) <> '' "
        "AND logical_key_min ~ '^[0-9a-f]+$' AND length(logical_key_min) % 2 = 0)",
        name="logical_key_min_hex",
    ),
    CheckConstraint(
        "logical_key_max IS NULL OR (btrim(logical_key_max) <> '' "
        "AND logical_key_max ~ '^[0-9a-f]+$' AND length(logical_key_max) % 2 = 0)",
        name="logical_key_max_hex",
    ),
    _count_check("size_bytes"),
    CheckConstraint("row_count > 0", name="row_count_positive"),
    CheckConstraint("semantic_count > 0", name="semantic_count_positive"),
    _count_check("assert_count"),
    _count_check("supersede_count"),
    _count_check("tombstone_count"),
    _count_check("evidence_count"),
    CheckConstraint("semantic_count = row_count", name="semantic_count_matches_row_count"),
    CheckConstraint(
        "(logical_key_min IS NULL) = (logical_key_max IS NULL)",
        name="logical_bounds_paired",
    ),
    CheckConstraint(
        'logical_key_min IS NULL OR logical_key_min <= logical_key_max COLLATE "C"',
        name="logical_bounds_order",
    ),
    CheckConstraint(
        "(leaf_kind = 'state' AND evidence_count = 0 "
        "AND assert_count + supersede_count + tombstone_count = row_count) "
        "OR (leaf_kind = 'evidence' AND assert_count = 0 AND supersede_count = 0 "
        "AND tombstone_count = 0 AND evidence_count = row_count)",
        name="leaf_count_coherence",
    ),
    CheckConstraint(
        "(effective_min IS NULL) = (effective_max IS NULL) AND "
        "(availability_min IS NULL) = (availability_max IS NULL) AND "
        "(revision_min IS NULL) = (revision_max IS NULL)",
        name="bounds_paired",
    ),
    CheckConstraint(
        "effective_min IS NULL OR effective_max >= effective_min",
        name="effective_bounds_order",
    ),
    CheckConstraint(
        "availability_min IS NULL OR availability_max >= availability_min",
        name="availability_bounds_order",
    ),
    CheckConstraint(
        "revision_min IS NULL OR revision_max >= revision_min",
        name="revision_bounds_order",
    ),
    ForeignKeyConstraint(
        ["generation_id", "relative_path"],
        [
            "canonical_generation_artifacts.generation_id",
            "canonical_generation_artifacts.relative_path",
        ],
        name="fk_canonical_partition_evidence_artifact",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint(
        "generation_id",
        "ordinal",
        name="pk_canonical_partition_evidence",
    ),
    UniqueConstraint(
        "generation_id",
        "relative_path",
        name="uq_canonical_partition_evidence_path",
    ),
    UniqueConstraint("task_id", name="uq_canonical_partition_evidence_task_id"),
)


canonical_generation_attestations = Table(
    "canonical_generation_attestations",
    metadata,
    Column("attestation_id", String(255), nullable=False),
    Column("generation_id", String(255), nullable=False),
    Column("ordinal", BigInteger, nullable=False),
    Column("attestation_kind", String(64), nullable=False),
    Column("contract_version", String(150), nullable=False),
    Column("status", String(32), nullable=False),
    Column("attestation_sha256", String(64), nullable=False),
    Column("manifest_sha256", String(64), nullable=True),
    Column("output_tree_sha256", String(64), nullable=True),
    Column("relative_path", Text, nullable=True),
    Column("size_bytes", BigInteger, nullable=True),
    Column("attested_at_utc", DateTime(timezone=True), nullable=False),
    _registered_at(),
    _nonempty_check("attestation_id"),
    CheckConstraint("ordinal >= 0", name="ordinal_nonnegative"),
    _nonempty_check("attestation_kind"),
    CheckConstraint(
        "attestation_kind IN ('v1_genesis_postflight', 'v2_prepared_generation')",
        name="attestation_kind_allowed",
    ),
    _nonempty_check("contract_version"),
    CheckConstraint(
        "status IN ('PASS', 'PREPARED_NOT_CANONICAL', 'ADMITTED')",
        name="status_allowed",
    ),
    _sha256_check("attestation_sha256"),
    _sha256_check("manifest_sha256", nullable=True),
    _sha256_check("output_tree_sha256", nullable=True),
    _safe_path_check("relative_path", nullable=True),
    _count_check("size_bytes", nullable=True),
    ForeignKeyConstraint(
        ["generation_id"],
        ["canonical_generations.generation_id"],
        name="fk_canonical_generation_attestations_generation_id_canonical_generations",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint("attestation_id", name="pk_canonical_generation_attestations"),
    UniqueConstraint(
        "generation_id",
        "ordinal",
        name="uq_canonical_generation_attestations_generation_ordinal",
    ),
    UniqueConstraint(
        "generation_id",
        "attestation_sha256",
        name="uq_canonical_generation_attestations_generation_sha256",
    ),
)


canonical_series_heads = Table(
    "canonical_series_heads",
    metadata,
    Column("series_id", String(255), nullable=False),
    Column("generation_id", String(255), nullable=False),
    Column("updated_at_utc", DateTime(timezone=True), nullable=False),
    _registered_at(),
    _nonempty_check("series_id"),
    _nonempty_check("generation_id"),
    ForeignKeyConstraint(
        ["series_id"],
        ["canonical_series.series_id"],
        name="fk_canonical_series_heads_series_id_canonical_series",
        ondelete="NO ACTION",
    ),
    # Composite identity is deliberate: a head cannot point at a generation
    # from another series even when the generation_id itself exists.
    ForeignKeyConstraint(
        ["series_id", "generation_id"],
        ["canonical_generations.series_id", "canonical_generations.generation_id"],
        name="fk_canonical_series_heads_generation_exact",
        ondelete="NO ACTION",
    ),
    PrimaryKeyConstraint("series_id", name="pk_canonical_series_heads"),
)


# Keep the named exports explicit for callers and Alembic env.py imports.
__all__ = [
    "canonical_generation_artifacts",
    "canonical_generation_attestations",
    "canonical_generation_sources",
    "canonical_generations",
    "canonical_partition_evidence",
    "canonical_series",
    "canonical_series_heads",
]
