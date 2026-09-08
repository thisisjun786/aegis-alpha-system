"""create collection control plane

Revision ID: 20260729_0002
Revises: 20260729_0001
Create Date: 2026-07-29 23:05:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260729_0002"
down_revision: str | None = "20260729_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OWNED_TABLES = {
    "alembic_version",
    "collection_run_events",
    "collection_run_plans",
    "collection_run_receipts",
    "collection_runs",
    "collection_usage_records",
    "collection_watermarks",
    "dataset_artifacts",
    "dataset_input_files",
    "dataset_sources",
    "dataset_versions",
    "quality_results",
    "source_snapshot_files",
    "source_snapshots",
}
_OWNED_VIEWS = {"collection_run_states", "collection_watermark_current"}
_OWNER_TOKEN_HEX_LENGTH = 32

_RUN_STATES_VIEW = """
CREATE VIEW collection_run_states AS
SELECT
    r.run_id,
    r.plan_id,
    latest.event_type AS state,
    COALESCE(
        latest.event_type IN ('run_succeeded', 'run_failed', 'run_cancelled'),
        false
    ) AS terminal,
    attempts.attempt_count,
    latest.event_seq AS last_event_seq,
    latest.occurred_at_utc AS last_occurred_at_utc
FROM collection_runs r
LEFT JOIN LATERAL (
    SELECT e.event_type, e.event_seq, e.occurred_at_utc
    FROM collection_run_events e
    WHERE e.run_id = r.run_id
    ORDER BY e.event_seq DESC
    LIMIT 1
) latest ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS attempt_count
    FROM collection_run_events e2
    WHERE e2.run_id = r.run_id AND e2.event_type = 'attempt_started'
) attempts ON true
"""

_WATERMARK_CURRENT_VIEW = """
CREATE VIEW collection_watermark_current AS
SELECT DISTINCT ON (provider, dataset, stream)
    provider,
    dataset,
    stream,
    watermark_seq,
    run_id,
    watermark_value,
    watermark_position,
    recorded_at_utc
FROM collection_watermarks
ORDER BY provider, dataset, stream, watermark_position DESC, watermark_seq DESC
"""


def upgrade() -> None:
    op.create_table(
        "collection_run_plans",
        sa.Column("plan_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("dataset", sa.String(length=150), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("requested_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("requested_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "parameters_json",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.Column("plan_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "registered_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(plan_sha256) = 64 AND plan_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_collection_run_plans_plan_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "mode IN ('probe', 'incremental', 'backfill')",
            name=op.f("ck_collection_run_plans_mode_allowed"),
        ),
        sa.CheckConstraint(
            "(requested_window_start IS NULL) = (requested_window_end IS NULL)",
            name=op.f("ck_collection_run_plans_requested_window_paired"),
        ),
        sa.CheckConstraint(
            "requested_window_start IS NULL OR requested_window_end IS NULL OR "
            "requested_window_end >= requested_window_start",
            name=op.f("ck_collection_run_plans_requested_window_order"),
        ),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_collection_run_plans_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "btrim(plan_id) <> ''",
            name=op.f("ck_collection_run_plans_plan_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider) <> ''",
            name=op.f("ck_collection_run_plans_provider_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(dataset) <> ''",
            name=op.f("ck_collection_run_plans_dataset_nonempty"),
        ),
        sa.PrimaryKeyConstraint("plan_id", name=op.f("pk_collection_run_plans")),
        sa.UniqueConstraint("plan_sha256", name=op.f("uq_collection_run_plans_plan_sha256")),
    )
    op.create_index(
        op.f("ix_collection_run_plans_provider"),
        "collection_run_plans",
        ["provider"],
        unique=False,
    )
    op.create_table(
        "collection_runs",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("plan_id", sa.String(length=255), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "registered_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(run_id) <> ''",
            name=op.f("ck_collection_runs_run_id_nonempty"),
        ),
        sa.ForeignKeyConstraint(
            ["plan_id"],
            ["collection_run_plans.plan_id"],
            name=op.f("fk_collection_runs_plan_id_collection_run_plans"),
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_collection_runs")),
    )
    op.create_table(
        "collection_run_events",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("event_seq", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=True),
        sa.Column("retry_of_attempt", sa.Integer(), nullable=True),
        sa.Column("error_class", sa.String(length=150), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("occurred_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "details_json",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.Column(
            "recorded_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_number IS NULL OR attempt_number > 0",
            name=op.f("ck_collection_run_events_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "(event_type IN ('attempt_started', 'attempt_succeeded', 'attempt_failed')) = "
            "(attempt_number IS NOT NULL)",
            name=op.f("ck_collection_run_events_attempt_events_require_attempt_number"),
        ),
        sa.CheckConstraint(
            "error_message IS NULL OR error_class IS NOT NULL",
            name=op.f("ck_collection_run_events_error_message_requires_error_class"),
        ),
        sa.CheckConstraint(
            "event_seq >= 1",
            name=op.f("ck_collection_run_events_event_seq_positive"),
        ),
        sa.CheckConstraint(
            "event_type IN ('attempt_started', 'attempt_succeeded', 'attempt_failed', "
            "'run_succeeded', 'run_failed', 'run_cancelled')",
            name=op.f("ck_collection_run_events_event_type_allowed"),
        ),
        sa.CheckConstraint(
            "(event_type IN ('attempt_failed', 'run_failed') AND error_class IS NOT NULL AND "
            "btrim(error_class) <> '') OR "
            "(event_type NOT IN ('attempt_failed', 'run_failed') AND error_class IS NULL)",
            name=op.f("ck_collection_run_events_failed_events_require_error_class"),
        ),
        sa.CheckConstraint(
            "retry_of_attempt IS NULL OR (event_type = 'attempt_started' AND retry_of_attempt > 0)",
            name=op.f("ck_collection_run_events_retry_link_only_on_attempt_start"),
        ),
        sa.CheckConstraint(
            "(event_type NOT IN ('run_succeeded', 'run_failed', 'run_cancelled')) OR "
            "(attempt_number IS NULL)",
            name=op.f("ck_collection_run_events_terminal_events_are_run_level"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["collection_runs.run_id"],
            name=op.f("fk_collection_run_events_run_id_collection_runs"),
        ),
        sa.PrimaryKeyConstraint("run_id", "event_seq", name=op.f("pk_collection_run_events")),
    )
    op.create_table(
        "collection_run_receipts",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=255), nullable=False),
        sa.Column("observed_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("byte_count", sa.BigInteger(), nullable=True),
        sa.Column("receipt_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "registered_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=op.f("ck_collection_run_receipts_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "byte_count IS NULL OR byte_count >= 0",
            name=op.f("ck_collection_run_receipts_byte_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "(observed_window_start IS NULL) = (observed_window_end IS NULL)",
            name=op.f("ck_collection_run_receipts_observed_window_paired"),
        ),
        sa.CheckConstraint(
            "observed_window_start IS NULL OR observed_window_end IS NULL OR "
            "observed_window_end >= observed_window_start",
            name=op.f("ck_collection_run_receipts_observed_window_order"),
        ),
        sa.CheckConstraint(
            "receipt_sha256 IS NULL OR (length(receipt_sha256) = 64 AND "
            "receipt_sha256 ~ '^[0-9a-f]{64}$')",
            name=op.f("ck_collection_run_receipts_receipt_sha256_sha256"),
        ),
        sa.CheckConstraint(
            "row_count IS NULL OR row_count >= 0",
            name=op.f("ck_collection_run_receipts_row_count_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["collection_runs.run_id"],
            name=op.f("fk_collection_run_receipts_run_id_collection_runs"),
        ),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshots.snapshot_id"],
            name=op.f("fk_collection_run_receipts_source_snapshot_id_source_snapshots"),
        ),
        sa.PrimaryKeyConstraint(
            "run_id",
            "attempt_number",
            "source_snapshot_id",
            name=op.f("pk_collection_run_receipts"),
        ),
    )
    op.create_table(
        "collection_watermarks",
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("dataset", sa.String(length=150), nullable=False),
        sa.Column("stream", sa.String(length=150), nullable=False),
        sa.Column("watermark_seq", sa.BigInteger(), nullable=False),
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("watermark_value", sa.Text(), nullable=False),
        sa.Column("watermark_position", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(dataset) <> ''",
            name=op.f("ck_collection_watermarks_dataset_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider) <> ''",
            name=op.f("ck_collection_watermarks_provider_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(stream) <> ''",
            name=op.f("ck_collection_watermarks_stream_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(watermark_value) <> ''",
            name=op.f("ck_collection_watermarks_watermark_value_nonempty"),
        ),
        sa.CheckConstraint(
            "watermark_seq >= 1",
            name=op.f("ck_collection_watermarks_watermark_seq_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["collection_runs.run_id"],
            name=op.f("fk_collection_watermarks_run_id_collection_runs"),
        ),
        sa.PrimaryKeyConstraint(
            "provider",
            "dataset",
            "stream",
            "watermark_seq",
            name=op.f("pk_collection_watermarks"),
        ),
        sa.UniqueConstraint(
            "provider",
            "dataset",
            "stream",
            "run_id",
            name="stream_run",
        ),
    )
    op.create_table(
        "collection_usage_records",
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("usage_seq", sa.BigInteger(), nullable=False),
        sa.Column("metric", sa.String(length=100), nullable=False),
        sa.Column("quantity", sa.Numeric(precision=24, scale=6), nullable=False),
        sa.Column("unit", sa.String(length=50), nullable=False),
        sa.Column(
            "evidence_json",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.Column("recorded_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "btrim(metric) <> ''",
            name=op.f("ck_collection_usage_records_metric_nonempty"),
        ),
        sa.CheckConstraint(
            "quantity >= 0",
            name=op.f("ck_collection_usage_records_quantity_nonnegative"),
        ),
        sa.CheckConstraint(
            "btrim(unit) <> ''",
            name=op.f("ck_collection_usage_records_unit_nonempty"),
        ),
        sa.CheckConstraint(
            "usage_seq >= 1",
            name=op.f("ck_collection_usage_records_usage_seq_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["collection_runs.run_id"],
            name=op.f("fk_collection_usage_records_run_id_collection_runs"),
        ),
        sa.PrimaryKeyConstraint("run_id", "usage_seq", name=op.f("pk_collection_usage_records")),
    )
    op.execute(_RUN_STATES_VIEW)
    op.execute(_WATERMARK_CURRENT_VIEW)


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute("DROP VIEW collection_watermark_current")
    op.execute("DROP VIEW collection_run_states")
    op.drop_table("collection_usage_records")
    op.drop_table("collection_watermarks")
    op.drop_table("collection_run_receipts")
    op.drop_table("collection_run_events")
    op.drop_table("collection_runs")
    op.drop_index(op.f("ix_collection_run_plans_provider"), table_name="collection_run_plans")
    op.drop_table("collection_run_plans")


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
