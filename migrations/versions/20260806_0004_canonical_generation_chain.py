"""add the canonical-generation evidence registry

Revision ID: 20260806_0004
Revises: 20260731_0003
Create Date: 2026-08-06 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision: str = "20260806_0004"
down_revision: str | None = "20260731_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_TABLES = {
    "canonical_generation_artifacts",
    "canonical_generation_attestations",
    "canonical_generation_sources",
    "canonical_generations",
    "canonical_partition_evidence",
    "canonical_series",
    "canonical_series_heads",
}
_OWNED_TABLES = {
    "alembic_version",
    "collection_run_events",
    "collection_run_plans",
    "collection_run_receipts",
    "collection_runs",
    "collection_usage_checkpoints",
    "collection_usage_records",
    "collection_watermarks",
    "dataset_artifacts",
    "dataset_input_files",
    "dataset_sources",
    "dataset_versions",
    "identity_identifier_assertions",
    "identity_instruments",
    "identity_issuers",
    "identity_mapping_conflicts",
    "identity_provider_mappings",
    "quality_results",
    "source_snapshot_files",
    "source_snapshots",
    *_NEW_TABLES,
}
_OWNER_TOKEN_HEX_LENGTH = 32


def _json_column(name: str) -> sa.Column[object]:
    return sa.Column(
        name,
        sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
        nullable=False,
    )


def _registered_at() -> sa.Column[object]:
    return sa.Column(
        "registered_at_utc",
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    )


def upgrade() -> None:
    if not context.is_offline_mode():
        _assert_fixture_owned_database()
    op.create_table(
        "canonical_series",
        sa.Column("series_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("contract_version", sa.String(length=150), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        _registered_at(),
        sa.CheckConstraint(
            "schema_version > 0", name=op.f("ck_canonical_series_schema_version_positive")
        ),
        sa.CheckConstraint(
            "btrim(series_id) <> ''", name=op.f("ck_canonical_series_series_id_nonempty")
        ),
        sa.CheckConstraint(
            "btrim(contract_version) <> ''",
            name=op.f("ck_canonical_series_contract_version_nonempty"),
        ),
        sa.PrimaryKeyConstraint("series_id", name=op.f("pk_canonical_series")),
    )

    op.create_table(
        "canonical_generations",
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("series_id", sa.String(length=255), nullable=False),
        sa.Column("generation_type", sa.String(length=16), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("parent_generation_id", sa.String(length=255), nullable=True),
        sa.Column("run_id", sa.String(length=255), nullable=True),
        sa.Column("attempt_number", sa.Integer(), nullable=True),
        sa.Column("source_snapshot_id", sa.String(length=255), nullable=True),
        sa.Column("expected_parent_history_root", sa.String(length=64), nullable=True),
        sa.Column("delta_merkle_root", sa.String(length=64), nullable=True),
        sa.Column("manifest_core_sha256", sa.String(length=64), nullable=True),
        sa.Column("history_root", sa.String(length=64), nullable=False),
        sa.Column("plan_sha256", sa.String(length=64), nullable=True),
        sa.Column("source_snapshot_sha256", sa.String(length=64), nullable=True),
        sa.Column("identity_authority_sha256", sa.String(length=64), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("semantic_count", sa.BigInteger(), nullable=True),
        sa.Column("partition_count", sa.BigInteger(), nullable=True),
        sa.Column("assert_count", sa.BigInteger(), nullable=True),
        sa.Column("supersede_count", sa.BigInteger(), nullable=True),
        sa.Column("tombstone_count", sa.BigInteger(), nullable=True),
        sa.Column("evidence_count", sa.BigInteger(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        _registered_at(),
        sa.CheckConstraint(
            "generation_type IN ('genesis_v1', 'delta_v2')",
            name=op.f("ck_canonical_generations_generation_type_allowed"),
        ),
        sa.CheckConstraint("seq >= 0", name=op.f("ck_canonical_generations_seq_nonnegative")),
        sa.CheckConstraint(
            "btrim(generation_id) <> ''",
            name=op.f("ck_canonical_generations_generation_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(series_id) <> ''", name=op.f("ck_canonical_generations_series_id_nonempty")
        ),
        sa.CheckConstraint(
            "parent_generation_id IS NULL OR btrim(parent_generation_id) <> ''",
            name=op.f("ck_canonical_generations_parent_generation_id_nonempty"),
        ),
        sa.CheckConstraint(
            "run_id IS NULL OR btrim(run_id) <> ''",
            name=op.f("ck_canonical_generations_run_id_nonempty"),
        ),
        sa.CheckConstraint(
            "source_snapshot_id IS NULL OR btrim(source_snapshot_id) <> ''",
            name=op.f("ck_canonical_generations_source_snapshot_id_nonempty"),
        ),
        sa.CheckConstraint(
            "attempt_number IS NULL OR attempt_number > 0",
            name=op.f("ck_canonical_generations_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "expected_parent_history_root IS NULL OR "
            "(length(expected_parent_history_root) = 64 AND "
            "expected_parent_history_root ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_expected_parent_history_root_sha256"),
        ),
        sa.CheckConstraint(
            "delta_merkle_root IS NULL OR "
            "(length(delta_merkle_root) = 64 AND delta_merkle_root ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_delta_merkle_root_sha256"),
        ),
        sa.CheckConstraint(
            "manifest_core_sha256 IS NULL OR "
            "(length(manifest_core_sha256) = 64 AND manifest_core_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_manifest_core_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(history_root) = 64 AND history_root ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_generations_history_root_sha256"),
        ),
        sa.CheckConstraint(
            "plan_sha256 IS NULL OR (length(plan_sha256) = 64 AND plan_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_plan_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "source_snapshot_sha256 IS NULL OR "
            "(length(source_snapshot_sha256) = 64 AND source_snapshot_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_source_snapshot_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "identity_authority_sha256 IS NULL OR "
            "(length(identity_authority_sha256) = 64 AND "
            "identity_authority_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generations_identity_authority_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "row_count IS NULL OR row_count >= 0",
            name=op.f("ck_canonical_generations_row_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "semantic_count IS NULL OR semantic_count >= 0",
            name=op.f("ck_canonical_generations_semantic_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "partition_count IS NULL OR partition_count >= 0",
            name=op.f("ck_canonical_generations_partition_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "assert_count IS NULL OR assert_count >= 0",
            name=op.f("ck_canonical_generations_assert_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "supersede_count IS NULL OR supersede_count >= 0",
            name=op.f("ck_canonical_generations_supersede_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "tombstone_count IS NULL OR tombstone_count >= 0",
            name=op.f("ck_canonical_generations_tombstone_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "evidence_count IS NULL OR evidence_count >= 0",
            name=op.f("ck_canonical_generations_evidence_count_nonnegative"),
        ),
        sa.CheckConstraint(
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
            name=op.f("ck_canonical_generations_typed_generation_projection"),
        ),
        sa.ForeignKeyConstraint(
            ["series_id"],
            ["canonical_series.series_id"],
            name=op.f("fk_canonical_generations_series_id_canonical_series"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["collection_runs.run_id"],
            name=op.f("fk_canonical_generations_run_id_collection_runs"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "attempt_number", "source_snapshot_id"],
            [
                "collection_run_receipts.run_id",
                "collection_run_receipts.attempt_number",
                "collection_run_receipts.source_snapshot_id",
            ],
            name=op.f("fk_canonical_generations_receipt_lineage"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["parent_generation_id"],
            ["canonical_generations.generation_id"],
            name=op.f("fk_canonical_generations_parent_generation_id_canonical_generations"),
            ondelete="NO ACTION",
        ),
        sa.UniqueConstraint(
            "series_id",
            "generation_id",
            name=op.f("uq_canonical_generations_series_generation"),
        ),
        sa.UniqueConstraint(
            "series_id",
            "history_root",
            name=op.f("uq_canonical_generations_series_history_root"),
        ),
        sa.UniqueConstraint(
            "series_id",
            "generation_id",
            "history_root",
            name=op.f("uq_canonical_generations_series_generation_history_root"),
        ),
        sa.UniqueConstraint(
            "generation_id",
            "run_id",
            "attempt_number",
            "source_snapshot_id",
            name=op.f("uq_canonical_generations_generation_lineage"),
        ),
        sa.UniqueConstraint(
            "run_id",
            "attempt_number",
            name=op.f("uq_canonical_generations_run_attempt"),
        ),
        sa.ForeignKeyConstraint(
            ["series_id", "parent_generation_id", "expected_parent_history_root"],
            [
                "canonical_generations.series_id",
                "canonical_generations.generation_id",
                "canonical_generations.history_root",
            ],
            name=op.f("fk_canonical_generations_parent_history_root"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["series_id", "parent_generation_id"],
            ["canonical_generations.series_id", "canonical_generations.generation_id"],
            name=op.f("fk_canonical_generations_parent_series_generation"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint("generation_id", name=op.f("pk_canonical_generations")),
    )
    op.create_index(
        "uq_canonical_generations_series_seq",
        "canonical_generations",
        ["series_id", "seq"],
        unique=True,
    )
    op.create_index(
        "uq_canonical_generations_series_parent",
        "canonical_generations",
        ["series_id", "parent_generation_id"],
        unique=True,
        postgresql_where=sa.text("parent_generation_id IS NOT NULL"),
    )

    op.execute(
        """
        CREATE FUNCTION canonical_enforce_generation_sequence() RETURNS trigger AS $$
        DECLARE
            parent_seq bigint;
        BEGIN
            IF NEW.generation_type = 'genesis_v1' THEN
                RETURN NEW;
            END IF;
            SELECT seq INTO parent_seq
            FROM canonical_generations
            WHERE generation_id = NEW.parent_generation_id;
            IF parent_seq IS NULL OR NEW.seq <> parent_seq + 1 THEN
                RAISE EXCEPTION
                    'delta generation sequence must be the direct parent sequence plus one';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
    )
    op.execute(
        """
        CREATE TRIGGER canonical_generations_direct_sequence
        BEFORE INSERT OR UPDATE OF series_id, generation_type, seq, parent_generation_id
        ON canonical_generations
        FOR EACH ROW EXECUTE FUNCTION canonical_enforce_generation_sequence()
        """,
    )

    op.create_table(
        "canonical_generation_sources",
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=255), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        _registered_at(),
        sa.CheckConstraint(
            "ordinal >= 0", name=op.f("ck_canonical_generation_sources_ordinal_nonnegative")
        ),
        sa.CheckConstraint(
            "btrim(source_kind) <> ''",
            name=op.f("ck_canonical_generation_sources_source_kind_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(source_id) <> ''",
            name=op.f("ck_canonical_generation_sources_source_id_nonempty"),
        ),
        sa.CheckConstraint(
            "length(source_sha256) = 64 AND source_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_generation_sources_source_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "btrim(run_id) <> ''", name=op.f("ck_canonical_generation_sources_run_id_nonempty")
        ),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=op.f("ck_canonical_generation_sources_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "btrim(source_snapshot_id) <> ''",
            name=op.f("ck_canonical_generation_sources_source_snapshot_id_nonempty"),
        ),
        sa.CheckConstraint(
            "relative_path IS NULL OR (relative_path !~ '(^/|(^|/)\\.\\.(/|$))' "
            "AND btrim(relative_path) <> '')",
            name=op.f("ck_canonical_generation_sources_relative_path_safe"),
        ),
        sa.CheckConstraint(
            "size_bytes IS NULL OR size_bytes >= 0",
            name=op.f("ck_canonical_generation_sources_size_bytes_nonnegative"),
        ),
        sa.CheckConstraint(
            "row_count IS NULL OR row_count >= 0",
            name=op.f("ck_canonical_generation_sources_row_count_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["canonical_generations.generation_id"],
            name=op.f("fk_canonical_generation_sources_generation_id_canonical_generations"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["generation_id", "run_id", "attempt_number", "source_snapshot_id"],
            [
                "canonical_generations.generation_id",
                "canonical_generations.run_id",
                "canonical_generations.attempt_number",
                "canonical_generations.source_snapshot_id",
            ],
            name=op.f("fk_canonical_generation_sources_generation_lineage"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["run_id", "attempt_number", "source_snapshot_id"],
            [
                "collection_run_receipts.run_id",
                "collection_run_receipts.attempt_number",
                "collection_run_receipts.source_snapshot_id",
            ],
            name=op.f("fk_canonical_generation_sources_receipt_lineage"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshots.snapshot_id"],
            name=op.f("fk_canonical_generation_sources_source_snapshot_id_source_snapshots"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint(
            "generation_id", "ordinal", name=op.f("pk_canonical_generation_sources")
        ),
        sa.UniqueConstraint(
            "generation_id",
            "source_kind",
            "source_id",
            name=op.f("uq_canonical_generation_sources_identity"),
        ),
    )

    op.create_table(
        "canonical_generation_artifacts",
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("artifact_kind", sa.String(length=64), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("schema_sha256", sa.String(length=64), nullable=True),
        sa.Column("media_type", sa.String(length=150), nullable=True),
        _registered_at(),
        sa.CheckConstraint(
            "ordinal >= 0", name=op.f("ck_canonical_generation_artifacts_ordinal_nonnegative")
        ),
        sa.CheckConstraint(
            "relative_path !~ '(^/|(^|/)\\.\\.(/|$))' AND btrim(relative_path) <> ''",
            name=op.f("ck_canonical_generation_artifacts_relative_path_safe"),
        ),
        sa.CheckConstraint(
            "btrim(artifact_kind) <> ''",
            name=op.f("ck_canonical_generation_artifacts_artifact_kind_nonempty"),
        ),
        sa.CheckConstraint(
            "artifact_kind IN ('partition', 'manifest')",
            name=op.f("ck_canonical_generation_artifacts_artifact_kind_allowed"),
        ),
        sa.CheckConstraint(
            "length(content_sha256) = 64 AND content_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_generation_artifacts_content_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "schema_sha256 IS NULL OR (length(schema_sha256) = 64 AND "
            "schema_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generation_artifacts_schema_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "media_type IS NULL OR btrim(media_type) <> ''",
            name=op.f("ck_canonical_generation_artifacts_media_type_nonempty"),
        ),
        sa.CheckConstraint(
            "size_bytes >= 0", name=op.f("ck_canonical_generation_artifacts_size_bytes_nonnegative")
        ),
        sa.CheckConstraint(
            "row_count IS NULL OR row_count >= 0",
            name=op.f("ck_canonical_generation_artifacts_row_count_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["canonical_generations.generation_id"],
            name=op.f("fk_canonical_generation_artifacts_generation_id_canonical_generations"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint(
            "generation_id",
            "ordinal",
            name=op.f("pk_canonical_generation_artifacts"),
        ),
        sa.UniqueConstraint(
            "generation_id",
            "relative_path",
            name=op.f("uq_canonical_generation_artifacts_path"),
        ),
    )

    op.create_table(
        "canonical_partition_evidence",
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("section", sa.String(length=64), nullable=False),
        sa.Column("leaf_kind", sa.String(length=16), nullable=False),
        sa.Column("adjustment_basis", sa.String(length=64), nullable=True),
        sa.Column("shard_count", sa.Integer(), nullable=False),
        sa.Column("shard_id", sa.Integer(), nullable=False),
        _json_column("partition_key_json"),
        _json_column("receipt_projection_json"),
        sa.Column("receipt_sha256", sa.String(length=64), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("schema_sha256", sa.String(length=64), nullable=False),
        sa.Column("partition_records_sha256", sa.String(length=64), nullable=False),
        sa.Column("semantic_count", sa.BigInteger(), nullable=False),
        sa.Column("semantic_checksum", sa.String(length=64), nullable=False),
        sa.Column("source_snapshot_sha256", sa.String(length=64), nullable=False),
        sa.Column("selected_input_sha256", sa.String(length=64), nullable=False),
        sa.Column("transformation_sha256", sa.String(length=64), nullable=False),
        sa.Column("plan_sha256", sa.String(length=64), nullable=False),
        sa.Column("identity_authority_sha256", sa.String(length=64), nullable=False),
        sa.Column("effective_min", sa.Date(), nullable=True),
        sa.Column("effective_max", sa.Date(), nullable=True),
        sa.Column("availability_min", sa.DateTime(timezone=True), nullable=True),
        sa.Column("availability_max", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revision_min", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revision_max", sa.DateTime(timezone=True), nullable=True),
        sa.Column("logical_key_min", sa.Text(), nullable=True),
        sa.Column("logical_key_max", sa.Text(), nullable=True),
        sa.Column("assert_count", sa.BigInteger(), nullable=False),
        sa.Column("supersede_count", sa.BigInteger(), nullable=False),
        sa.Column("tombstone_count", sa.BigInteger(), nullable=False),
        sa.Column("evidence_count", sa.BigInteger(), nullable=False),
        _registered_at(),
        sa.CheckConstraint(
            "ordinal >= 0", name=op.f("ck_canonical_partition_evidence_ordinal_nonnegative")
        ),
        sa.CheckConstraint(
            "btrim(relative_path) <> ''",
            name=op.f("ck_canonical_partition_evidence_relative_path_nonempty"),
        ),
        sa.CheckConstraint(
            "relative_path !~ '(^/|(^|/)\\.\\.(/|$))'",
            name=op.f("ck_canonical_partition_evidence_relative_path_safe"),
        ),
        sa.CheckConstraint(
            "btrim(section) <> ''", name=op.f("ck_canonical_partition_evidence_section_nonempty")
        ),
        sa.CheckConstraint(
            "btrim(leaf_kind) <> ''",
            name=op.f("ck_canonical_partition_evidence_leaf_kind_nonempty"),
        ),
        sa.CheckConstraint(
            "leaf_kind IN ('state', 'evidence')",
            name=op.f("ck_canonical_partition_evidence_leaf_kind_allowed"),
        ),
        sa.CheckConstraint(
            "adjustment_basis IS NULL OR btrim(adjustment_basis) <> ''",
            name=op.f("ck_canonical_partition_evidence_adjustment_basis_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(task_id) <> ''", name=op.f("ck_canonical_partition_evidence_task_id_nonempty")
        ),
        sa.CheckConstraint(
            "shard_count > 0", name=op.f("ck_canonical_partition_evidence_shard_count_positive")
        ),
        sa.CheckConstraint(
            "shard_id >= 0 AND shard_id < shard_count",
            name=op.f("ck_canonical_partition_evidence_shard_id_in_range"),
        ),
        sa.CheckConstraint(
            "(shard_count & (shard_count - 1)) = 0",
            name=op.f("ck_canonical_partition_evidence_shard_count_power_of_two"),
        ),
        sa.CheckConstraint(
            "section IN ('corporate_actions', 'diagnostics', 'disagreements', "
            "'identity_bindings', 'prices')",
            name=op.f("ck_canonical_partition_evidence_section_allowed"),
        ),
        sa.CheckConstraint(
            "(leaf_kind = 'state' AND section IN "
            "('corporate_actions', 'identity_bindings', 'prices')) "
            "OR (leaf_kind = 'evidence' AND section IN ('diagnostics', 'disagreements'))",
            name=op.f("ck_canonical_partition_evidence_section_leaf_kind_coherent"),
        ),
        sa.CheckConstraint(
            "(section = 'prices' AND adjustment_basis IS NOT NULL) "
            "OR (section <> 'prices' AND adjustment_basis IS NULL)",
            name=op.f("ck_canonical_partition_evidence_prices_adjustment_basis_coherent"),
        ),
        sa.CheckConstraint(
            "section = 'prices' OR (shard_count = 1 AND shard_id = 0)",
            name=op.f("ck_canonical_partition_evidence_non_price_single_shard"),
        ),
        sa.CheckConstraint(
            "size_bytes >= 0", name=op.f("ck_canonical_partition_evidence_size_bytes_nonnegative")
        ),
        sa.CheckConstraint(
            "row_count > 0", name=op.f("ck_canonical_partition_evidence_row_count_positive")
        ),
        sa.CheckConstraint(
            "semantic_count > 0",
            name=op.f("ck_canonical_partition_evidence_semantic_count_positive"),
        ),
        sa.CheckConstraint(
            "assert_count >= 0",
            name=op.f("ck_canonical_partition_evidence_assert_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "supersede_count >= 0",
            name=op.f("ck_canonical_partition_evidence_supersede_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "tombstone_count >= 0",
            name=op.f("ck_canonical_partition_evidence_tombstone_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "evidence_count >= 0",
            name=op.f("ck_canonical_partition_evidence_evidence_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "semantic_count = row_count",
            name=op.f("ck_canonical_partition_evidence_semantic_count_matches_row_count"),
        ),
        sa.CheckConstraint(
            "length(content_sha256) = 64 AND content_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_content_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(receipt_sha256) = 64 AND receipt_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_receipt_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(schema_sha256) = 64 AND schema_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_schema_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(partition_records_sha256) = 64 AND partition_records_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_partition_records_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(semantic_checksum) = 64 AND semantic_checksum ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_semantic_checksum_sha256"),
        ),
        sa.CheckConstraint(
            "length(source_snapshot_sha256) = 64 AND source_snapshot_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_source_snapshot_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(selected_input_sha256) = 64 AND selected_input_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_selected_input_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(transformation_sha256) = 64 AND transformation_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_transformation_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(plan_sha256) = 64 AND plan_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_plan_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(identity_authority_sha256) = 64 AND "
            "identity_authority_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_partition_evidence_identity_authority_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "logical_key_min IS NULL OR (btrim(logical_key_min) <> '' "
            "AND logical_key_min ~ '^[0-9a-f]+$' AND length(logical_key_min) % 2 = 0)",
            name=op.f("ck_canonical_partition_evidence_logical_key_min_hex"),
        ),
        sa.CheckConstraint(
            "logical_key_max IS NULL OR (btrim(logical_key_max) <> '' "
            "AND logical_key_max ~ '^[0-9a-f]+$' AND length(logical_key_max) % 2 = 0)",
            name=op.f("ck_canonical_partition_evidence_logical_key_max_hex"),
        ),
        sa.CheckConstraint(
            "(logical_key_min IS NULL) = (logical_key_max IS NULL)",
            name=op.f("ck_canonical_partition_evidence_logical_bounds_paired"),
        ),
        sa.CheckConstraint(
            'logical_key_min IS NULL OR logical_key_min <= logical_key_max COLLATE "C"',
            name=op.f("ck_canonical_partition_evidence_logical_bounds_order"),
        ),
        sa.CheckConstraint(
            "(leaf_kind = 'state' AND evidence_count = 0 "
            "AND assert_count + supersede_count + tombstone_count = row_count) "
            "OR (leaf_kind = 'evidence' AND assert_count = 0 AND supersede_count = 0 "
            "AND tombstone_count = 0 AND evidence_count = row_count)",
            name=op.f("ck_canonical_partition_evidence_leaf_count_coherence"),
        ),
        sa.CheckConstraint(
            "(effective_min IS NULL) = (effective_max IS NULL) AND "
            "(availability_min IS NULL) = (availability_max IS NULL) AND "
            "(revision_min IS NULL) = (revision_max IS NULL)",
            name=op.f("ck_canonical_partition_evidence_bounds_paired"),
        ),
        sa.CheckConstraint(
            "effective_min IS NULL OR effective_max >= effective_min",
            name=op.f("ck_canonical_partition_evidence_effective_bounds_order"),
        ),
        sa.CheckConstraint(
            "availability_min IS NULL OR availability_max >= availability_min",
            name=op.f("ck_canonical_partition_evidence_availability_bounds_order"),
        ),
        sa.CheckConstraint(
            "revision_min IS NULL OR revision_max >= revision_min",
            name=op.f("ck_canonical_partition_evidence_revision_bounds_order"),
        ),
        sa.ForeignKeyConstraint(
            ["generation_id", "relative_path"],
            [
                "canonical_generation_artifacts.generation_id",
                "canonical_generation_artifacts.relative_path",
            ],
            name=op.f("fk_canonical_partition_evidence_artifact"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint(
            "generation_id",
            "ordinal",
            name=op.f("pk_canonical_partition_evidence"),
        ),
        sa.UniqueConstraint(
            "generation_id",
            "relative_path",
            name=op.f("uq_canonical_partition_evidence_path"),
        ),
        sa.UniqueConstraint(
            "task_id",
            name=op.f("uq_canonical_partition_evidence_task_id"),
        ),
    )

    op.execute(
        """
        CREATE FUNCTION canonical_enforce_partition_artifact_identity() RETURNS trigger AS $$
        DECLARE
            expected_kind text;
            expected_content_sha256 text;
            expected_size_bytes bigint;
            expected_row_count bigint;
            expected_schema_sha256 text;
        BEGIN
            SELECT artifact_kind, content_sha256, size_bytes, row_count, schema_sha256
                INTO expected_kind, expected_content_sha256, expected_size_bytes,
                     expected_row_count, expected_schema_sha256
            FROM canonical_generation_artifacts
            WHERE generation_id = NEW.generation_id
              AND relative_path = NEW.relative_path;
            IF expected_kind IS DISTINCT FROM 'partition'
                OR expected_content_sha256 IS DISTINCT FROM NEW.content_sha256
                OR expected_size_bytes IS DISTINCT FROM NEW.size_bytes
                OR expected_row_count IS DISTINCT FROM NEW.row_count
                OR expected_schema_sha256 IS DISTINCT FROM NEW.schema_sha256
            THEN
                RAISE EXCEPTION
                    'partition evidence does not match its partition artifact identity';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
    )
    op.execute(
        """
        CREATE TRIGGER canonical_partition_evidence_artifact_identity
        BEFORE INSERT OR UPDATE ON canonical_partition_evidence
        FOR EACH ROW EXECUTE FUNCTION canonical_enforce_partition_artifact_identity()
        """,
    )

    op.create_table(
        "canonical_generation_attestations",
        sa.Column("attestation_id", sa.String(length=255), nullable=False),
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.Column("attestation_kind", sa.String(length=64), nullable=False),
        sa.Column("contract_version", sa.String(length=150), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attestation_sha256", sa.String(length=64), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=True),
        sa.Column("output_tree_sha256", sa.String(length=64), nullable=True),
        sa.Column("relative_path", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("attested_at_utc", sa.DateTime(timezone=True), nullable=False),
        _registered_at(),
        sa.CheckConstraint(
            "btrim(attestation_id) <> ''",
            name=op.f("ck_canonical_generation_attestations_attestation_id_nonempty"),
        ),
        sa.CheckConstraint(
            "ordinal >= 0", name=op.f("ck_canonical_generation_attestations_ordinal_nonnegative")
        ),
        sa.CheckConstraint(
            "btrim(attestation_kind) <> ''",
            name=op.f("ck_canonical_generation_attestations_attestation_kind_nonempty"),
        ),
        sa.CheckConstraint(
            "attestation_kind IN ('v1_genesis_postflight', 'v2_prepared_generation')",
            name=op.f("ck_canonical_generation_attestations_attestation_kind_allowed"),
        ),
        sa.CheckConstraint(
            "btrim(contract_version) <> ''",
            name=op.f("ck_canonical_generation_attestations_contract_version_nonempty"),
        ),
        sa.CheckConstraint(
            "status IN ('PASS', 'PREPARED_NOT_CANONICAL', 'ADMITTED')",
            name=op.f("ck_canonical_generation_attestations_status_allowed"),
        ),
        sa.CheckConstraint(
            "length(attestation_sha256) = 64 AND attestation_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_canonical_generation_attestations_attestation_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "manifest_sha256 IS NULL OR (length(manifest_sha256) = 64 AND "
            "manifest_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generation_attestations_manifest_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "output_tree_sha256 IS NULL OR (length(output_tree_sha256) = 64 AND "
            "output_tree_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_canonical_generation_attestations_output_tree_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "relative_path IS NULL OR (relative_path !~ '(^/|(^|/)\\.\\.(/|$))' "
            "AND btrim(relative_path) <> '')",
            name=op.f("ck_canonical_generation_attestations_relative_path_safe"),
        ),
        sa.CheckConstraint(
            "size_bytes IS NULL OR size_bytes >= 0",
            name=op.f("ck_canonical_generation_attestations_size_bytes_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["generation_id"],
            ["canonical_generations.generation_id"],
            name=op.f("fk_canonical_generation_attestations_generation_id_canonical_generations"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint(
            "attestation_id", name=op.f("pk_canonical_generation_attestations")
        ),
        sa.UniqueConstraint(
            "generation_id",
            "ordinal",
            name=op.f("uq_canonical_generation_attestations_generation_ordinal"),
        ),
        sa.UniqueConstraint(
            "generation_id",
            "attestation_sha256",
            name=op.f("uq_canonical_generation_attestations_generation_sha256"),
        ),
    )

    op.create_table(
        "canonical_series_heads",
        sa.Column("series_id", sa.String(length=255), nullable=False),
        sa.Column("generation_id", sa.String(length=255), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False),
        _registered_at(),
        sa.CheckConstraint(
            "btrim(series_id) <> ''", name=op.f("ck_canonical_series_heads_series_id_nonempty")
        ),
        sa.CheckConstraint(
            "btrim(generation_id) <> ''",
            name=op.f("ck_canonical_series_heads_generation_id_nonempty"),
        ),
        sa.ForeignKeyConstraint(
            ["series_id"],
            ["canonical_series.series_id"],
            name=op.f("fk_canonical_series_heads_series_id_canonical_series"),
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["series_id", "generation_id"],
            ["canonical_generations.series_id", "canonical_generations.generation_id"],
            name=op.f("fk_canonical_series_heads_generation_exact"),
            ondelete="NO ACTION",
        ),
        sa.PrimaryKeyConstraint("series_id", name=op.f("pk_canonical_series_heads")),
    )

    # A head starts at the genesis anchor and can only move to the direct child
    # of its current generation.  The transaction owner still takes the series
    # lock before changing the row; this trigger is the final database-side
    # fail-closed check for direct SQL writers.
    op.execute(
        """
        CREATE FUNCTION canonical_enforce_forward_head() RETURNS trigger AS $$
        DECLARE
            current_seq bigint;
            next_seq bigint;
            next_parent text;
            next_type text;
        BEGIN
            SELECT seq, generation_type INTO current_seq, next_type
            FROM canonical_generations
            WHERE generation_id = NEW.generation_id;
            IF TG_OP = 'INSERT' THEN
                IF current_seq <> 0 OR next_type <> 'genesis_v1' THEN
                    RAISE EXCEPTION 'a series head must start at genesis_v1 sequence zero';
                END IF;
                RETURN NEW;
            END IF;

            IF NEW.series_id IS DISTINCT FROM OLD.series_id THEN
                RAISE EXCEPTION 'series head update cannot change series identity';
            END IF;
            SELECT seq INTO current_seq
            FROM canonical_generations
            WHERE generation_id = OLD.generation_id;
            SELECT seq, parent_generation_id, generation_type
                INTO next_seq, next_parent, next_type
            FROM canonical_generations
            WHERE generation_id = NEW.generation_id;
            IF next_type <> 'delta_v2'
                OR next_seq <> current_seq + 1
                OR next_parent IS DISTINCT FROM OLD.generation_id
            THEN
                RAISE EXCEPTION 'series head update must select the direct next generation';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
    )
    op.execute(
        """
        CREATE TRIGGER canonical_series_heads_forward_only
        BEFORE INSERT OR UPDATE ON canonical_series_heads
        FOR EACH ROW EXECUTE FUNCTION canonical_enforce_forward_head()
        """,
    )


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute("DROP TRIGGER canonical_series_heads_forward_only ON canonical_series_heads")
    op.execute("DROP FUNCTION canonical_enforce_forward_head()")
    op.execute(
        "DROP TRIGGER canonical_partition_evidence_artifact_identity "
        "ON canonical_partition_evidence"
    )
    op.execute("DROP FUNCTION canonical_enforce_partition_artifact_identity()")
    op.execute("DROP TRIGGER canonical_generations_direct_sequence ON canonical_generations")
    op.execute("DROP FUNCTION canonical_enforce_generation_sequence()")
    op.drop_table("canonical_series_heads")
    op.drop_table("canonical_generation_attestations")
    op.drop_table("canonical_partition_evidence")
    op.drop_table("canonical_generation_artifacts")
    op.drop_table("canonical_generation_sources")
    op.drop_index("uq_canonical_generations_series_parent", table_name="canonical_generations")
    op.drop_index("uq_canonical_generations_series_seq", table_name="canonical_generations")
    op.drop_table("canonical_generations")
    op.drop_table("canonical_series")


def _assert_fixture_owned_database() -> None:
    """Fail closed: the Wave 4 migration is test-fixture-only."""

    expected_name = context.config.attributes.get("aas_test_database_name")
    expected_token = context.config.attributes.get("aas_test_owner_token")
    connection = op.get_bind()
    current_name = connection.scalar(sa.text("SELECT current_database()"))
    observed_token = connection.scalar(
        sa.text(
            "SELECT shobj_description(oid, 'pg_database') "
            "FROM pg_database WHERE datname = current_database()"
        )
    )
    observed_tables = set(sa.inspect(connection).get_table_names())
    if (
        not isinstance(expected_name, str)
        or not isinstance(expected_token, str)
        or len(expected_token) != _OWNER_TOKEN_HEX_LENGTH
        or any(character not in "0123456789abcdef" for character in expected_token)
        or expected_name != f"aas_owned_{expected_token}_test"
        or current_name != expected_name
        or observed_token != expected_token
        or not observed_tables <= _OWNED_TABLES
    ):
        raise RuntimeError("refusing Wave 4 migration outside an owned empty test database")
