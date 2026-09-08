"""feature contract registry

Revision ID: 20260822_0006
Revises: 20260821_0005
Create Date: 2026-08-22 00:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "20260822_0006"
down_revision: str | None = "20260821_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FEATURE_REFUSE_CONTRACT_MUTATION = """
CREATE OR REPLACE FUNCTION feature_refuse_contract_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'feature contracts are append-only';
END;
$$ LANGUAGE plpgsql;
"""

_FEATURE_REFUSE_CONTRACT_INPUT_MUTATION = """
CREATE OR REPLACE FUNCTION feature_refuse_contract_input_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'feature contract inputs are append-only';
END;
$$ LANGUAGE plpgsql;
"""

_FEATURE_CONTRACT_MUTATION_TRIGGER = """
CREATE TRIGGER feature_refuse_contract_mutation_trigger
BEFORE UPDATE OR DELETE ON feature_contracts
FOR EACH ROW
EXECUTE FUNCTION feature_refuse_contract_mutation();
"""

_FEATURE_CONTRACT_INPUT_MUTATION_TRIGGER = """
CREATE TRIGGER feature_refuse_contract_input_mutation_trigger
BEFORE UPDATE OR DELETE ON feature_contract_inputs
FOR EACH ROW
EXECUTE FUNCTION feature_refuse_contract_input_mutation();
"""

_FEATURE_DROP_CONTRACT_INPUT_MUTATION_TRIGGER = (
    "DROP TRIGGER IF EXISTS feature_refuse_contract_input_mutation_trigger "
    "ON feature_contract_inputs"
)

_FEATURE_DROP_CONTRACT_MUTATION_TRIGGER = (
    "DROP TRIGGER IF EXISTS feature_refuse_contract_mutation_trigger ON feature_contracts"
)

_FEATURE_DROP_CONTRACT_INPUT_MUTATION_FUNCTION = (
    "DROP FUNCTION IF EXISTS feature_refuse_contract_input_mutation()"
)

_FEATURE_DROP_CONTRACT_MUTATION_FUNCTION = (
    "DROP FUNCTION IF EXISTS feature_refuse_contract_mutation()"
)

_OWNER_TOKEN_HEX_LENGTH = 32

_OWNED_TABLES = {
    "alembic_version",
    "canonical_generation_artifacts",
    "canonical_generation_attestations",
    "canonical_generation_sources",
    "canonical_generations",
    "canonical_partition_evidence",
    "canonical_series",
    "canonical_series_heads",
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
    "feature_contract_inputs",
    "feature_contracts",
    "identity_identifier_assertions",
    "identity_instruments",
    "identity_issuers",
    "identity_mapping_conflicts",
    "identity_provider_mappings",
    "quality_results",
    "source_snapshot_files",
    "source_snapshots",
}


def _assert_fixture_owned_database() -> None:
    """Fail closed: destructive downgrade is test-fixture-only in schema v1."""

    context = op.get_context()
    config = context.config
    if config is None:
        raise RuntimeError("refusing destructive downgrade outside an owned empty test database")
    expected_name = config.attributes.get("aas_test_database_name")
    expected_token = config.attributes.get("aas_test_owner_token")
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


def upgrade() -> None:
    op.create_table(
        "feature_contracts",
        sa.Column("contract_name", sa.String(150), nullable=False),
        sa.Column("contract_version", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column(
            "parameters_json",
            sa.JSON().with_variant(JSONB(), "postgresql"),
            nullable=False,
        ),
        sa.Column("definition_artifact_sha256", sa.String(64), nullable=False),
        sa.Column("canonical_serialization_sha256", sa.String(64), nullable=False),
        sa.Column("output_schema_ref", sa.String(255), nullable=False),
        sa.Column("consumes_capital", sa.Boolean(), nullable=False),
        sa.Column("consumes_totalreturn", sa.Boolean(), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "contract_name",
            "contract_version",
            name=op.f("pk_feature_contracts"),
        ),
        sa.UniqueConstraint(
            "canonical_serialization_sha256",
            name=op.f("uq_feature_contracts_canonical_serialization_sha256"),
        ),
        sa.CheckConstraint(
            "schema_version = 1", name=op.f("ck_feature_contracts_schema_version_one")
        ),
        sa.CheckConstraint(
            "consumes_capital OR consumes_totalreturn",
            name=op.f("ck_feature_contracts_consumes_at_least_one"),
        ),
        sa.CheckConstraint(
            "btrim(contract_name) <> ''",
            name=op.f("ck_feature_contracts_contract_name_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(contract_version) <> ''",
            name=op.f("ck_feature_contracts_contract_version_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(output_schema_ref) <> ''",
            name=op.f("ck_feature_contracts_output_schema_ref_nonempty"),
        ),
        sa.CheckConstraint(
            "length(definition_artifact_sha256) = 64 AND "
            "definition_artifact_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_feature_contracts_definition_artifact_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "length(canonical_serialization_sha256) = 64 AND "
            "canonical_serialization_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_feature_contracts_canonical_serialization_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(parameters_json) = 'object'",
            name=op.f("ck_feature_contracts_parameters_json_object"),
        ),
    )

    op.create_table(
        "feature_contract_inputs",
        sa.Column("contract_name", sa.String(150), nullable=False),
        sa.Column("contract_version", sa.String(100), nullable=False),
        sa.Column("input_ordinal", sa.Integer(), nullable=False),
        sa.Column("input_kind", sa.String(32), nullable=False),
        sa.Column("dataset_id", sa.String(255), nullable=True),
        sa.Column("dataset_version", sa.String(100), nullable=True),
        sa.Column("upstream_contract_name", sa.String(150), nullable=True),
        sa.Column("upstream_contract_version", sa.String(100), nullable=True),
        sa.Column("expected_digest_sha256", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint(
            "contract_name",
            "contract_version",
            "input_ordinal",
            name=op.f("pk_feature_contract_inputs"),
        ),
        sa.ForeignKeyConstraint(
            ["contract_name", "contract_version"],
            ["feature_contracts.contract_name", "feature_contracts.contract_version"],
            name=op.f("fk_feature_contract_inputs_contract_name_feature_contracts"),
            match="FULL",
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id", "dataset_version"],
            ["dataset_versions.dataset_id", "dataset_versions.dataset_version"],
            name=op.f("fk_feature_contract_inputs_dataset_id_dataset_versions"),
            match="FULL",
            ondelete="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["upstream_contract_name", "upstream_contract_version"],
            ["feature_contracts.contract_name", "feature_contracts.contract_version"],
            name=op.f("fk_feature_contract_inputs_upstream_contract_name_feature_contracts"),
            match="FULL",
            ondelete="NO ACTION",
        ),
        sa.CheckConstraint(
            "input_ordinal > 0",
            name=op.f("ck_feature_contract_inputs_input_ordinal_positive"),
        ),
        sa.CheckConstraint(
            "input_kind IN ('dataset_version', 'feature_contract')",
            name=op.f("ck_feature_contract_inputs_input_kind_allowed"),
        ),
        sa.CheckConstraint(
            "length(expected_digest_sha256) = 64 AND expected_digest_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_feature_contract_inputs_expected_digest_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "(input_kind = 'dataset_version' AND dataset_id IS NOT NULL "
            "AND dataset_version IS NOT NULL AND upstream_contract_name IS NULL "
            "AND upstream_contract_version IS NULL) "
            "OR (input_kind = 'feature_contract' AND upstream_contract_name IS NOT NULL "
            "AND upstream_contract_version IS NOT NULL AND dataset_id IS NULL "
            "AND dataset_version IS NULL)",
            name=op.f("ck_feature_contract_inputs_exactly_one_subject"),
        ),
    )

    op.execute(_FEATURE_REFUSE_CONTRACT_MUTATION)
    op.execute(_FEATURE_REFUSE_CONTRACT_INPUT_MUTATION)
    op.execute(_FEATURE_CONTRACT_MUTATION_TRIGGER)
    op.execute(_FEATURE_CONTRACT_INPUT_MUTATION_TRIGGER)


def downgrade() -> None:
    _assert_fixture_owned_database()

    conn = op.get_bind()

    feature_contracts_count = conn.execute(
        sa.text("SELECT COUNT(*) FROM feature_contracts")
    ).scalar()
    feature_contract_inputs_count = conn.execute(
        sa.text("SELECT COUNT(*) FROM feature_contract_inputs")
    ).scalar()
    if feature_contracts_count or feature_contract_inputs_count:
        raise RuntimeError(
            "refusing destructive downgrade: feature_contracts or "
            "feature_contract_inputs contain rows"
        )

    op.execute(_FEATURE_DROP_CONTRACT_INPUT_MUTATION_TRIGGER)
    op.execute(_FEATURE_DROP_CONTRACT_MUTATION_TRIGGER)
    op.execute(_FEATURE_DROP_CONTRACT_INPUT_MUTATION_FUNCTION)
    op.execute(_FEATURE_DROP_CONTRACT_MUTATION_FUNCTION)

    op.drop_table("feature_contract_inputs")
    op.drop_table("feature_contracts")
