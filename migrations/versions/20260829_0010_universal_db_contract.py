"""universal DB contract — engine ledger, module output tables, targets

Revision ID: 20260829_0010
Revises: 20260825_0009
Create Date: 2026-08-29 00:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "20260829_0010"
down_revision: str | None = "20260825_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CREATE_SCHEMAS = [
    "CREATE SCHEMA IF NOT EXISTS engine",
    "CREATE SCHEMA IF NOT EXISTS aegis",
    "CREATE SCHEMA IF NOT EXISTS alpha",
    "CREATE SCHEMA IF NOT EXISTS risk",
]


def upgrade() -> None:
    for statement in _CREATE_SCHEMAS:
        op.execute(statement)

    op.create_table(
        "engine_runs",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("engine", sa.String(20), nullable=False),
        sa.Column("mode", sa.String(20), nullable=False),
        sa.Column("run_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("git_sha", sa.String(40), nullable=False),
        sa.Column("config_sha256", sa.String(64), nullable=False),
        sa.Column("inputs_parquet_sha256", sa.String(64), nullable=False),
        sa.Column("dataset_digest", sa.String(64), nullable=False),
        sa.Column("feature_contract_version", sa.String(100)),
        sa.Column("outputs_sha256", sa.String(64)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "engine IN ('aegis', 'alpha', 'risk', 'data')",
            name="engine_runs_engine_allowed",
        ),
        sa.CheckConstraint(
            "mode IN ('baseline', 'paper', 'replay')",
            name="engine_runs_mode_allowed",
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed')",
            name="engine_runs_status_allowed",
        ),
        sa.CheckConstraint(
            "length(config_sha256) = 64 AND config_sha256 ~ '^[0-9a-f]{64}$'",
            name="config_sha256_sha256",
        ),
        sa.CheckConstraint(
            "length(inputs_parquet_sha256) = 64 AND inputs_parquet_sha256 ~ '^[0-9a-f]{64}$'",
            name="inputs_parquet_sha256_sha256",
        ),
        sa.CheckConstraint(
            "length(dataset_digest) = 64 AND dataset_digest ~ '^[0-9a-f]{64}$'",
            name="dataset_digest_sha256",
        ),
        sa.CheckConstraint(
            "outputs_sha256 IS NULL OR "
            "(length(outputs_sha256) = 64 AND outputs_sha256 ~ '^[0-9a-f]{64}$')",
            name="outputs_sha256_sha256",
        ),
        sa.CheckConstraint("btrim(git_sha) <> ''", name="git_sha_nonempty"),
        schema="engine",
    )

    op.create_table(
        "engine_run_events",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("payload", sa.JSON().with_variant(JSONB(), "postgresql"), nullable=False),
        sa.Column(
            "event_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ("run_id",),
            ("engine.engine_runs.id",),
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("btrim(event_type) <> ''", name="event_type_nonempty"),
        schema="engine",
    )

    for schema_name in ("aegis", "alpha", "risk"):
        op.create_table(
            "daily_output",
            sa.Column("as_of", sa.Date, nullable=False),
            sa.Column("run_id", sa.String(36), nullable=False),
            sa.Column("payload", sa.JSON().with_variant(JSONB(), "postgresql"), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.PrimaryKeyConstraint("as_of", "run_id"),
            sa.ForeignKeyConstraint(
                ("run_id",),
                ("engine.engine_runs.id",),
                ondelete="CASCADE",
            ),
            schema=schema_name,
        )

    op.create_table(
        "collection_targets",
        sa.Column("domain", sa.String(50), nullable=False),
        sa.Column("symbol", sa.String(50), nullable=False),
        sa.Column("valid_from", sa.Date, nullable=False),
        sa.Column("valid_until", sa.Date),
        sa.Column("reason", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("domain", "symbol", "valid_from"),
        sa.CheckConstraint("btrim(domain) <> ''", name="collection_targets_domain_nonempty"),
        sa.CheckConstraint("btrim(symbol) <> ''", name="collection_targets_symbol_nonempty"),
        sa.CheckConstraint("btrim(reason) <> ''", name="collection_targets_reason_nonempty"),
    )

    op.add_column(
        "dataset_versions",
        sa.Column("source_run_id", sa.String(36)),
    )
    op.create_foreign_key(
        "fk_dataset_versions_source_run_id_engine_runs",
        "dataset_versions",
        "engine_runs",
        ["source_run_id"],
        ["id"],
        source_schema=None,
        referent_schema="engine",
    )


def downgrade() -> None:
    connection = op.get_bind()
    counts = {
        "engine_runs": connection.execute(
            sa.text("SELECT COUNT(*) FROM engine.engine_runs")
        ).scalar(),
        "aegis.daily_output": connection.execute(
            sa.text("SELECT COUNT(*) FROM aegis.daily_output")
        ).scalar(),
        "alpha.daily_output": connection.execute(
            sa.text("SELECT COUNT(*) FROM alpha.daily_output")
        ).scalar(),
        "risk.daily_output": connection.execute(
            sa.text("SELECT COUNT(*) FROM risk.daily_output")
        ).scalar(),
        "collection_targets": connection.execute(
            sa.text("SELECT COUNT(*) FROM collection_targets")
        ).scalar(),
    }
    if any(counts.values()):
        raise RuntimeError(
            "refusing destructive downgrade: universal DB contract tables contain rows"
        )

    op.drop_constraint(
        "fk_dataset_versions_source_run_id_engine_runs",
        "dataset_versions",
        type_="foreignkey",
    )
    op.drop_column("dataset_versions", "source_run_id")

    op.drop_table("collection_targets")
    for schema_name in ("aegis", "alpha", "risk"):
        op.drop_table("daily_output", schema=schema_name)
    op.drop_table("engine_run_events", schema="engine")
    op.drop_table("engine_runs", schema="engine")
