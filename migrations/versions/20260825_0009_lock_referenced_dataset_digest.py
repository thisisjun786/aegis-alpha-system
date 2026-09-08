"""lock referenced dataset digest during feature-contract input insert

Revision ID: 20260825_0009
Revises: 20260825_0008
Create Date: 2026-08-25 00:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260825_0009"
down_revision: str | None = "20260825_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FEATURE_LOCK_REFERENCED_DATASET_DIGEST = """
CREATE OR REPLACE FUNCTION feature_lock_referenced_dataset_digest()
RETURNS trigger AS $$
DECLARE
    observed_digest text;
BEGIN
    IF NEW.input_kind IS DISTINCT FROM 'dataset_version'
       OR NEW.dataset_id IS NULL
       OR NEW.dataset_version IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT dataset_versions.aggregate_content_sha256
      INTO observed_digest
      FROM dataset_versions
     WHERE dataset_versions.dataset_id = NEW.dataset_id
       AND dataset_versions.dataset_version = NEW.dataset_version
     FOR UPDATE;
    IF observed_digest IS NULL THEN
        RAISE EXCEPTION
            'feature contract input references an unknown dataset version';
    END IF;
    IF observed_digest IS DISTINCT FROM NEW.expected_digest_sha256 THEN
        RAISE EXCEPTION
            'feature contract input digest does not match dataset aggregate_content_sha256';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_FEATURE_LOCK_REFERENCED_DATASET_DIGEST_TRIGGER = """
CREATE TRIGGER feature_lock_referenced_dataset_digest_trigger
BEFORE INSERT ON feature_contract_inputs
FOR EACH ROW
EXECUTE FUNCTION feature_lock_referenced_dataset_digest();
"""

_FEATURE_DROP_LOCK_REFERENCED_DATASET_DIGEST_TRIGGER = (
    "DROP TRIGGER IF EXISTS feature_lock_referenced_dataset_digest_trigger "
    "ON feature_contract_inputs"
)

_FEATURE_DROP_LOCK_REFERENCED_DATASET_DIGEST_FUNCTION = (
    "DROP FUNCTION IF EXISTS feature_lock_referenced_dataset_digest()"
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
    "eligibility_decisions",
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
    connection = op.get_bind()
    stale_count = connection.scalar(
        sa.text(
            """
            SELECT COUNT(*)
            FROM feature_contract_inputs AS input
            WHERE input.input_kind = 'dataset_version'
              AND NOT EXISTS (
                    SELECT 1
                    FROM dataset_versions AS dataset
                    WHERE dataset.dataset_id = input.dataset_id
                      AND dataset.dataset_version = input.dataset_version
                      AND dataset.aggregate_content_sha256 = input.expected_digest_sha256
              )
            """
        )
    )
    if stale_count:
        raise RuntimeError(
            "refusing upgrade: feature_contract_inputs digest does not match "
            "dataset aggregate_content_sha256"
        )
    op.execute(_FEATURE_LOCK_REFERENCED_DATASET_DIGEST)
    op.execute(_FEATURE_LOCK_REFERENCED_DATASET_DIGEST_TRIGGER)


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute(_FEATURE_DROP_LOCK_REFERENCED_DATASET_DIGEST_TRIGGER)
    op.execute(_FEATURE_DROP_LOCK_REFERENCED_DATASET_DIGEST_FUNCTION)
