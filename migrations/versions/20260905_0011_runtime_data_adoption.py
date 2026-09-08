"""Runtime adoption receipts and intrinsic engine result protections.

Revision ID: 20260905_0011
Revises: 20260829_0010
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260905_0011"
down_revision: str | None = "20260829_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OUTPUT_SCHEMAS = ("aegis", "alpha", "risk")
_ENGINE_TABLES = (
    "engine.engine_runs",
    "engine.engine_run_events",
    "aegis.daily_output",
    "alpha.daily_output",
    "risk.daily_output",
)

_REJECT_MUTATION = """
CREATE FUNCTION engine.reject_runtime_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
        USING ERRCODE = '23514';
END;
$$
"""

_GUARD_RUN = """
CREATE FUNCTION engine.guard_runtime_run() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- UPDATE/DELETE already hold this parent row's write lock. Output INSERT
    -- takes FOR UPDATE on the same row before deciding whether it is admissible.
    IF TG_OP <> 'INSERT' THEN
        IF OLD.status IN ('succeeded', 'failed') THEN
            RAISE EXCEPTION 'terminal engine run is immutable' USING ERRCODE = '23514';
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        IF ROW(NEW.id, NEW.engine, NEW.mode, NEW.run_ts, NEW.git_sha,
               NEW.config_sha256, NEW.inputs_parquet_sha256, NEW.dataset_digest,
               NEW.feature_contract_version)
           IS DISTINCT FROM
           ROW(OLD.id, OLD.engine, OLD.mode, OLD.run_ts, OLD.git_sha,
               OLD.config_sha256, OLD.inputs_parquet_sha256, OLD.dataset_digest,
               OLD.feature_contract_version) THEN
            RAISE EXCEPTION 'engine run identity is immutable' USING ERRCODE = '23514';
        END IF;
    END IF;
    IF NEW.status = 'succeeded' AND NEW.outputs_sha256 IS NULL THEN
        RAISE EXCEPTION 'successful engine run requires output digest' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$
"""

_GUARD_OUTPUT = """
CREATE FUNCTION engine.guard_runtime_output() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent_engine text;
    parent_status text;
BEGIN
    SELECT engine, status INTO parent_engine, parent_status
    FROM engine.engine_runs WHERE id = NEW.run_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'output requires an existing engine run' USING ERRCODE = '23503';
    END IF;
    IF parent_engine IS DISTINCT FROM TG_TABLE_SCHEMA THEN
        RAISE EXCEPTION 'output module does not match parent engine' USING ERRCODE = '23514';
    END IF;
    IF parent_status IS DISTINCT FROM 'succeeded' THEN
        RAISE EXCEPTION 'output requires a succeeded engine run' USING ERRCODE = '23514';
    END IF;
    IF jsonb_typeof(NEW.payload) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'output payload must be a JSON object' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$
"""


def _require_empty_engine(*, include_adoptions: bool, operation: str) -> None:
    """Hold exclusive locks through DDL so concurrent writes cannot bypass preflight."""
    tables = (*_ENGINE_TABLES, "engine.data_adoptions") if include_adoptions else _ENGINE_TABLES
    connection = op.get_bind()
    # Identifiers are revision-local constants, never caller input.
    connection.execute(sa.text(f"LOCK TABLE {', '.join(tables)} IN ACCESS EXCLUSIVE MODE"))
    for table in tables:
        if connection.scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM {table})")):  # noqa: S608
            raise RuntimeError(f"refusing {operation}: {table} contains rows")


def _create_adoptions() -> None:
    # Freeze the receipt definition in this revision; do not import mutable models.
    op.create_table(
        "data_adoptions",
        sa.Column("adoption_id", sa.Text(), nullable=False),
        sa.Column("source_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("source_system_identifier", sa.Text(), nullable=False),
        sa.Column("source_database", sa.Text(), nullable=False),
        sa.Column("source_alembic_head", sa.Text(), nullable=False),
        sa.Column("adopted_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("table_count", sa.Integer(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("adoption_id"),
        sa.UniqueConstraint("source_manifest_sha256"),
        sa.CheckConstraint("btrim(adoption_id) <> ''", name="adoption_id_nonempty"),
        sa.CheckConstraint(
            "length(source_manifest_sha256) = 64 AND source_manifest_sha256 ~ '^[0-9a-f]{64}$'",
            name="source_manifest_sha256_sha256",
        ),
        sa.CheckConstraint(
            "btrim(source_system_identifier) <> ''", name="source_system_identifier_nonempty"
        ),
        sa.CheckConstraint("btrim(source_database) <> ''", name="source_database_nonempty"),
        sa.CheckConstraint("btrim(source_alembic_head) <> ''", name="source_alembic_head_nonempty"),
        sa.CheckConstraint("table_count > 0", name="table_count_positive"),
        sa.CheckConstraint("row_count >= 0", name="row_count_nonnegative"),
        sa.CheckConstraint("file_count >= 0", name="file_count_nonnegative"),
        schema="engine",
    )


def upgrade() -> None:
    _require_empty_engine(include_adoptions=False, operation="runtime engine upgrade")
    _create_adoptions()
    for statement in (_REJECT_MUTATION, _GUARD_RUN, _GUARD_OUTPUT):
        op.execute(statement)
    op.execute(
        "CREATE TRIGGER guard_runtime_run BEFORE INSERT OR UPDATE OR DELETE "
        "ON engine.engine_runs FOR EACH ROW EXECUTE FUNCTION engine.guard_runtime_run()"
    )
    for table in ("engine.data_adoptions", "engine.engine_run_events"):
        op.execute(
            f"CREATE TRIGGER reject_runtime_mutation BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION engine.reject_runtime_mutation()"
        )
    for schema in _OUTPUT_SCHEMAS:
        op.execute(
            f"CREATE TRIGGER guard_runtime_output BEFORE INSERT ON {schema}.daily_output "
            "FOR EACH ROW EXECUTE FUNCTION engine.guard_runtime_output()"
        )
        op.execute(
            f"CREATE TRIGGER reject_runtime_mutation BEFORE UPDATE OR DELETE "
            f"ON {schema}.daily_output FOR EACH ROW "
            "EXECUTE FUNCTION engine.reject_runtime_mutation()"
        )


def downgrade() -> None:
    _require_empty_engine(include_adoptions=True, operation="destructive runtime downgrade")
    for schema in _OUTPUT_SCHEMAS:
        op.execute(f"DROP TRIGGER guard_runtime_output ON {schema}.daily_output")
        op.execute(f"DROP TRIGGER reject_runtime_mutation ON {schema}.daily_output")
    for table in ("engine.data_adoptions", "engine.engine_run_events"):
        op.execute(f"DROP TRIGGER reject_runtime_mutation ON {table}")
    op.execute("DROP TRIGGER guard_runtime_run ON engine.engine_runs")
    for function in ("guard_runtime_output", "guard_runtime_run", "reject_runtime_mutation"):
        op.execute(f"DROP FUNCTION engine.{function}()")
    op.drop_table("data_adoptions", schema="engine")
