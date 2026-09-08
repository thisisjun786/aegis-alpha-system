"""create signed usage checkpoints

Revision ID: 20260818_0004
Revises: 20260731_0003
Create Date: 2026-08-18 12:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260818_0004"
down_revision: str | None = "20260731_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

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
}
_OWNER_TOKEN_HEX_LENGTH = 32

_REFUSE_MUTATION = """
CREATE FUNCTION collection_refuse_usage_checkpoint_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'collection usage checkpoints are append-only';
END;
$$ LANGUAGE plpgsql
"""


def upgrade() -> None:
    op.create_table(
        "collection_usage_checkpoints",
        sa.Column("checkpoint_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("coverage_start_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coverage_end_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("usage_record_count", sa.BigInteger(), nullable=False),
        sa.Column("usage_records_root_sha256", sa.String(length=64), nullable=False),
        sa.Column("authority_id", sa.String(length=255), nullable=False),
        sa.Column("key_id", sa.String(length=255), nullable=False),
        sa.Column("generated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signature_algorithm", sa.String(length=32), nullable=False),
        sa.Column("signature_version", sa.Integer(), nullable=False),
        sa.Column("signature", sa.LargeBinary(), nullable=False),
        sa.Column(
            "registered_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "authority_id = btrim(authority_id) AND authority_id ~ '^[^[:space:]]+$'",
            name=op.f("ck_collection_usage_checkpoints_authority_id_normalized"),
        ),
        sa.CheckConstraint(
            "checkpoint_id = btrim(checkpoint_id) AND checkpoint_id ~ '^[^[:space:]]+$'",
            name=op.f("ck_collection_usage_checkpoints_checkpoint_id_normalized"),
        ),
        sa.CheckConstraint(
            "coverage_end_utc > coverage_start_utc",
            name=op.f("ck_collection_usage_checkpoints_coverage_interval_order"),
        ),
        sa.CheckConstraint(
            "generated_at_utc >= coverage_end_utc",
            name=op.f("ck_collection_usage_checkpoints_generation_after_coverage"),
        ),
        sa.CheckConstraint(
            "key_id = btrim(key_id) AND key_id ~ '^[^[:space:]]+$'",
            name=op.f("ck_collection_usage_checkpoints_key_id_normalized"),
        ),
        sa.CheckConstraint(
            "provider ~ '^[a-z0-9][a-z0-9._-]*$'",
            name=op.f("ck_collection_usage_checkpoints_provider_normalized"),
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_collection_usage_checkpoints_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "signature_algorithm = 'ed25519'",
            name=op.f("ck_collection_usage_checkpoints_signature_algorithm_supported"),
        ),
        sa.CheckConstraint(
            "octet_length(signature) = 64",
            name=op.f("ck_collection_usage_checkpoints_signature_ed25519_size"),
        ),
        sa.CheckConstraint(
            "signature_version = 1",
            name=op.f("ck_collection_usage_checkpoints_signature_version_supported"),
        ),
        sa.CheckConstraint(
            "usage_record_count >= 0",
            name=op.f("ck_collection_usage_checkpoints_usage_record_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "length(usage_records_root_sha256) = 64 AND "
            "usage_records_root_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_collection_usage_checkpoints_usage_records_root_sha256_sha256"),
        ),
        sa.PrimaryKeyConstraint(
            "checkpoint_id",
            name=op.f("pk_collection_usage_checkpoints"),
        ),
    )
    op.create_index(
        op.f("ix_collection_usage_checkpoints_provider"),
        "collection_usage_checkpoints",
        ["provider"],
        unique=False,
    )
    op.execute(_REFUSE_MUTATION)
    op.execute(
        "CREATE TRIGGER collection_usage_checkpoints_append_only "
        "BEFORE UPDATE OR DELETE ON collection_usage_checkpoints "
        "FOR EACH ROW EXECUTE FUNCTION collection_refuse_usage_checkpoint_mutation()"
    )


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute(
        "DROP TRIGGER collection_usage_checkpoints_append_only ON collection_usage_checkpoints"
    )
    op.execute("DROP FUNCTION collection_refuse_usage_checkpoint_mutation()")
    op.drop_index(
        op.f("ix_collection_usage_checkpoints_provider"),
        table_name="collection_usage_checkpoints",
    )
    op.drop_table("collection_usage_checkpoints")


def _assert_fixture_owned_database() -> None:
    """Fail closed: destructive downgrade is test-fixture-only in schema v1."""

    context = op.get_context()
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
        raise RuntimeError("refusing destructive downgrade outside an owned empty test database")
