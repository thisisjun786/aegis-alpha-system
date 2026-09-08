"""eligibility decisions

# noqa: SIZE_OK - one atomic revision owns the eligibility trigger contract
Revision ID: 20260822_0007
Revises: 20260822_0006
Create Date: 2026-08-22 00:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260822_0007"
down_revision: str | None = "20260822_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ELIGIBILITY_REFUSE_DECISION_MUTATION = """
CREATE OR REPLACE FUNCTION eligibility_refuse_decision_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'eligibility decisions are append-only';
END;
$$ LANGUAGE plpgsql;
"""

_ELIGIBILITY_DECISION_MUTATION_TRIGGER = """
CREATE TRIGGER eligibility_refuse_decision_mutation_trigger
BEFORE UPDATE OR DELETE ON eligibility_decisions
FOR EACH ROW
EXECUTE FUNCTION eligibility_refuse_decision_mutation();
"""

_ELIGIBILITY_ENFORCE_GRANT_STAGE = """
CREATE OR REPLACE FUNCTION eligibility_enforce_grant_stage()
RETURNS trigger AS $$
BEGIN
    IF NEW.polarity IS DISTINCT FROM 'GRANT' THEN
        RETURN NEW;
    END IF;
    IF NEW.flag = 'paper' AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = 'backtest'
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
    ) IS DISTINCT FROM 'GRANT' THEN
        RAISE EXCEPTION 'paper eligibility GRANT requires current backtest GRANT';
    END IF;
    IF NEW.flag = 'order' AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = 'paper'
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
    ) IS DISTINCT FROM 'GRANT' THEN
        RAISE EXCEPTION 'order eligibility GRANT requires current paper GRANT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_ELIGIBILITY_GRANT_STAGE_TRIGGER = """
CREATE TRIGGER eligibility_enforce_grant_stage_trigger
BEFORE INSERT ON eligibility_decisions
FOR EACH ROW
EXECUTE FUNCTION eligibility_enforce_grant_stage();
"""

_ELIGIBILITY_CASCADE_REVOKE = """
CREATE OR REPLACE FUNCTION eligibility_cascade_revoke()
RETURNS trigger AS $$
BEGIN
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;
    IF NEW.polarity IS DISTINCT FROM 'REVOKE' THEN
        RETURN NEW;
    END IF;
    INSERT INTO eligibility_decisions (
        dataset_id,
        dataset_version,
        flag,
        polarity,
        owner_receipt_sha256,
        decided_at_utc,
        decided_by
    )
    SELECT
        NEW.dataset_id,
        NEW.dataset_version,
        higher.flag,
        'REVOKE',
        NEW.owner_receipt_sha256,
        NEW.decided_at_utc,
        NEW.decided_by
    FROM (VALUES ('paper', 2), ('order', 3)) AS higher(flag, stage_rank)
    WHERE (
        NEW.flag = 'backtest'
        OR (NEW.flag = 'paper' AND higher.flag = 'order')
    )
      AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = higher.flag
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
      ) = 'GRANT'
    ORDER BY higher.stage_rank;
    UPDATE dataset_versions AS version
    SET
        backtest_eligible = CASE
            WHEN NEW.flag = 'backtest' THEN FALSE
            ELSE version.backtest_eligible
        END,
        paper_eligible = CASE
            WHEN NEW.flag IN ('backtest', 'paper') THEN FALSE
            ELSE version.paper_eligible
        END,
        order_eligible = CASE
            WHEN NEW.flag IN ('backtest', 'paper', 'order') THEN FALSE
            ELSE version.order_eligible
        END
    WHERE version.dataset_id = NEW.dataset_id
      AND version.dataset_version = NEW.dataset_version;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_ELIGIBILITY_CASCADE_REVOKE_TRIGGER = """
CREATE TRIGGER eligibility_cascade_revoke_trigger
AFTER INSERT ON eligibility_decisions
FOR EACH ROW
EXECUTE FUNCTION eligibility_cascade_revoke();
"""

_ELIGIBILITY_ENFORCE_DATASET_FLAGS = """
CREATE OR REPLACE FUNCTION eligibility_enforce_dataset_flags()
RETURNS trigger AS $$
BEGIN
    IF NEW.canonical_eligible THEN
        RAISE EXCEPTION 'canonical eligibility is permanently blocked';
    END IF;
    IF NEW.paper_eligible AND NOT NEW.backtest_eligible THEN
        RAISE EXCEPTION 'paper eligibility requires backtest eligibility';
    END IF;
    IF NEW.order_eligible AND NOT NEW.paper_eligible THEN
        RAISE EXCEPTION 'order eligibility requires paper eligibility';
    END IF;
    IF NEW.backtest_eligible AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = 'backtest'
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
    ) IS DISTINCT FROM 'GRANT' THEN
        RAISE EXCEPTION 'backtest eligibility requires current GRANT';
    END IF;
    IF NEW.paper_eligible AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = 'paper'
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
    ) IS DISTINCT FROM 'GRANT' THEN
        RAISE EXCEPTION 'paper eligibility requires current GRANT';
    END IF;
    IF NEW.order_eligible AND (
        SELECT decision.polarity
        FROM eligibility_decisions AS decision
        WHERE decision.dataset_id = NEW.dataset_id
          AND decision.dataset_version = NEW.dataset_version
          AND decision.flag = 'order'
        ORDER BY decision.ledger_sequence DESC
        LIMIT 1
    ) IS DISTINCT FROM 'GRANT' THEN
        RAISE EXCEPTION 'order eligibility requires current GRANT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_ELIGIBILITY_DATASET_FLAGS_TRIGGER = """
CREATE TRIGGER eligibility_enforce_dataset_flags_trigger
BEFORE INSERT OR UPDATE ON dataset_versions
FOR EACH ROW
EXECUTE FUNCTION eligibility_enforce_dataset_flags();
"""

_ELIGIBILITY_DROP_DATASET_FLAGS_TRIGGER = (
    "DROP TRIGGER IF EXISTS eligibility_enforce_dataset_flags_trigger ON dataset_versions"
)

_ELIGIBILITY_DROP_CASCADE_REVOKE_TRIGGER = (
    "DROP TRIGGER IF EXISTS eligibility_cascade_revoke_trigger ON eligibility_decisions"
)

_ELIGIBILITY_DROP_GRANT_STAGE_TRIGGER = (
    "DROP TRIGGER IF EXISTS eligibility_enforce_grant_stage_trigger ON eligibility_decisions"
)

_ELIGIBILITY_DROP_DATASET_FLAGS_FUNCTION = (
    "DROP FUNCTION IF EXISTS eligibility_enforce_dataset_flags()"
)

_ELIGIBILITY_DROP_CASCADE_REVOKE_FUNCTION = "DROP FUNCTION IF EXISTS eligibility_cascade_revoke()"

_ELIGIBILITY_DROP_GRANT_STAGE_FUNCTION = "DROP FUNCTION IF EXISTS eligibility_enforce_grant_stage()"

_ELIGIBILITY_DROP_DECISION_MUTATION_TRIGGER = (
    "DROP TRIGGER IF EXISTS eligibility_refuse_decision_mutation_trigger ON eligibility_decisions"
)

_ELIGIBILITY_DROP_DECISION_MUTATION_FUNCTION = (
    "DROP FUNCTION IF EXISTS eligibility_refuse_decision_mutation()"
)

_ASSERT_ELIGIBILITY_FLAGS_FALSE = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM dataset_versions
        WHERE canonical_eligible OR backtest_eligible
           OR paper_eligible OR order_eligible
    ) THEN
        RAISE EXCEPTION 'cannot replace v1 eligibility block with promoted dataset flags';
    END IF;
END;
$$;
"""

_OWNER_TOKEN_HEX_LENGTH = 32


class _DestructiveDowngradeError(RuntimeError):
    """Raised when revision 0007 cannot be downgraded safely."""


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
        raise _DestructiveDowngradeError(
            "refusing destructive downgrade outside an owned empty test database"
        )
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
        raise _DestructiveDowngradeError(
            "refusing destructive downgrade outside an owned empty test database"
        )


def upgrade() -> None:
    op.create_table(
        "eligibility_decisions",
        sa.Column("ledger_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("dataset_id", sa.String(255), nullable=False),
        sa.Column("dataset_version", sa.String(100), nullable=False),
        sa.Column("flag", sa.String(16), nullable=False),
        sa.Column("polarity", sa.String(16), nullable=False),
        sa.Column("owner_receipt_sha256", sa.String(64), nullable=False),
        sa.Column("decided_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_by", sa.String(255), nullable=False),
        sa.Column(
            "registered_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "ledger_sequence",
            name=op.f("pk_eligibility_decisions"),
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id", "dataset_version"],
            ["dataset_versions.dataset_id", "dataset_versions.dataset_version"],
            name=op.f("fk_eligibility_decisions_dataset_id_dataset_versions"),
            match="FULL",
            ondelete="NO ACTION",
        ),
        sa.CheckConstraint(
            "flag IN ('backtest', 'paper', 'order')",
            name=op.f("ck_eligibility_decisions_flag_allowed"),
        ),
        sa.CheckConstraint(
            "polarity IN ('GRANT', 'REVOKE')",
            name=op.f("ck_eligibility_decisions_polarity_allowed"),
        ),
        sa.CheckConstraint(
            "btrim(dataset_id) <> ''",
            name=op.f("ck_eligibility_decisions_dataset_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(dataset_version) <> ''",
            name=op.f("ck_eligibility_decisions_dataset_version_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(decided_by) <> ''",
            name=op.f("ck_eligibility_decisions_decided_by_nonempty"),
        ),
        sa.CheckConstraint(
            "length(owner_receipt_sha256) = 64 AND owner_receipt_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_eligibility_decisions_owner_receipt_sha256_sha256"),
        ),
    )

    op.create_index(
        op.f("ix_eligibility_decisions_dataset_version_flag_ledger_sequence"),
        "eligibility_decisions",
        ["dataset_id", "dataset_version", "flag", sa.text("ledger_sequence DESC")],
        unique=False,
    )

    op.execute(_ELIGIBILITY_REFUSE_DECISION_MUTATION)
    op.execute(_ELIGIBILITY_DECISION_MUTATION_TRIGGER)
    op.execute(_ELIGIBILITY_ENFORCE_GRANT_STAGE)
    op.execute(_ELIGIBILITY_GRANT_STAGE_TRIGGER)
    op.execute(_ELIGIBILITY_CASCADE_REVOKE)
    op.execute(_ELIGIBILITY_CASCADE_REVOKE_TRIGGER)
    op.execute(_ELIGIBILITY_ENFORCE_DATASET_FLAGS)
    op.execute(_ELIGIBILITY_DATASET_FLAGS_TRIGGER)
    op.create_check_constraint(
        op.f("ck_dataset_versions_canonical_eligibility_blocked"),
        "dataset_versions",
        "NOT canonical_eligible",
    )
    op.execute(_ASSERT_ELIGIBILITY_FLAGS_FALSE)
    op.drop_constraint(
        op.f("ck_dataset_versions_eligibility_blocked_in_v1"),
        "dataset_versions",
        type_="check",
    )


def downgrade() -> None:
    _assert_fixture_owned_database()

    conn = op.get_bind()

    eligibility_decisions_count = conn.execute(
        sa.text("SELECT COUNT(*) FROM eligibility_decisions")
    ).scalar()
    if eligibility_decisions_count:
        raise _DestructiveDowngradeError(
            "refusing destructive downgrade: eligibility_decisions contains rows"
        )
    eligibility_flags_present = conn.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM dataset_versions "
            "WHERE canonical_eligible OR backtest_eligible "
            "OR paper_eligible OR order_eligible)"
        )
    ).scalar()
    if eligibility_flags_present:
        raise _DestructiveDowngradeError(
            "refusing destructive downgrade: dataset eligibility flags are true"
        )

    op.execute(_ELIGIBILITY_DROP_DATASET_FLAGS_TRIGGER)
    op.execute(_ELIGIBILITY_DROP_CASCADE_REVOKE_TRIGGER)
    op.execute(_ELIGIBILITY_DROP_GRANT_STAGE_TRIGGER)
    op.execute(_ELIGIBILITY_DROP_DATASET_FLAGS_FUNCTION)
    op.execute(_ELIGIBILITY_DROP_CASCADE_REVOKE_FUNCTION)
    op.execute(_ELIGIBILITY_DROP_GRANT_STAGE_FUNCTION)
    op.create_check_constraint(
        op.f("ck_dataset_versions_eligibility_blocked_in_v1"),
        "dataset_versions",
        "NOT canonical_eligible AND NOT backtest_eligible "
        "AND NOT paper_eligible AND NOT order_eligible",
    )
    op.drop_constraint(
        op.f("ck_dataset_versions_canonical_eligibility_blocked"),
        "dataset_versions",
        type_="check",
    )

    op.execute(_ELIGIBILITY_DROP_DECISION_MUTATION_TRIGGER)
    op.execute(_ELIGIBILITY_DROP_DECISION_MUTATION_FUNCTION)
    op.drop_index(
        op.f("ix_eligibility_decisions_dataset_version_flag_ledger_sequence"),
        table_name="eligibility_decisions",
    )
    op.drop_table("eligibility_decisions")
