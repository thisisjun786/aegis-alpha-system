"""freeze referenced dataset aggregate digest

Revision ID: 20260825_0008
Revises: 20260822_0007
Create Date: 2026-08-25 00:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260825_0008"
down_revision: str | None = "20260822_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FEATURE_FREEZE_REFERENCED_DATASET_DIGEST = """
CREATE OR REPLACE FUNCTION feature_freeze_referenced_dataset_digest()
RETURNS trigger AS $$
BEGIN
    IF NEW.aggregate_content_sha256 IS DISTINCT FROM OLD.aggregate_content_sha256
       AND EXISTS (
            SELECT 1
            FROM feature_contract_inputs AS input
            WHERE input.input_kind = 'dataset_version'
              AND input.dataset_id = OLD.dataset_id
              AND input.dataset_version = OLD.dataset_version
       )
    THEN
        RAISE EXCEPTION
            'dataset aggregate_content_sha256 is frozen while a feature contract references it';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_FEATURE_FREEZE_REFERENCED_DATASET_DIGEST_TRIGGER = """
CREATE TRIGGER feature_freeze_referenced_dataset_digest_trigger
BEFORE UPDATE ON dataset_versions
FOR EACH ROW
EXECUTE FUNCTION feature_freeze_referenced_dataset_digest();
"""

_FEATURE_DROP_FREEZE_REFERENCED_DATASET_DIGEST_TRIGGER = (
    "DROP TRIGGER IF EXISTS feature_freeze_referenced_dataset_digest_trigger ON dataset_versions"
)

_FEATURE_DROP_FREEZE_REFERENCED_DATASET_DIGEST_FUNCTION = (
    "DROP FUNCTION IF EXISTS feature_freeze_referenced_dataset_digest()"
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
    op.execute(_FEATURE_FREEZE_REFERENCED_DATASET_DIGEST)
    op.execute(_FEATURE_FREEZE_REFERENCED_DATASET_DIGEST_TRIGGER)


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute(_FEATURE_DROP_FREEZE_REFERENCED_DATASET_DIGEST_TRIGGER)
    op.execute(_FEATURE_DROP_FREEZE_REFERENCED_DATASET_DIGEST_FUNCTION)
