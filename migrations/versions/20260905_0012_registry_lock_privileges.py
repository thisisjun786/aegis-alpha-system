"""Permit registry row locks without allowing key changes.

Revision ID: 20260905_0012
Revises: 20260905_0011
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260905_0012"
down_revision: str | None = "20260905_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# One immutable column grants PostgreSQL's SELECT FOR UPDATE privilege; the
# unconditional trigger prevents any statement from using it to change rows.
_LOCK_COLUMNS = {
    "source_snapshots": "snapshot_id",
    "dataset_versions": "dataset_id",
    "collection_run_plans": "plan_id",
    "collection_runs": "run_id",
    "collection_run_events": "run_id",
    "collection_run_receipts": "run_id",
    "collection_watermarks": "provider",
    "collection_usage_records": "run_id",
}


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION engine.reject_registry_key_update() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            RAISE EXCEPTION 'registry lock column is immutable' USING ERRCODE = '23514';
        END;
        $$
    """)
    for table, column in _LOCK_COLUMNS.items():
        op.execute(
            f"CREATE TRIGGER reject_registry_key_update BEFORE UPDATE OF {column} "
            f"ON public.{table} FOR EACH ROW "
            "EXECUTE FUNCTION engine.reject_registry_key_update()"
        )


def downgrade() -> None:
    connection = op.get_bind()
    # Match installer order, including the version relation first. A grant
    # transaction must commit before we inspect ACLs, not after that inspection.
    tables = ("alembic_version", *sorted(_LOCK_COLUMNS))
    op.execute(
        "LOCK TABLE "
        + ", ".join(f"public.{table}" for table in tables)
        + " IN ACCESS EXCLUSIVE MODE"
    )
    for table, column in _LOCK_COLUMNS.items():
        grants = connection.scalar(
            sa.text("""
                SELECT count(*) FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) acl
                WHERE n.nspname='public' AND c.relname=:table AND a.attname=:column
                AND acl.privilege_type='UPDATE' AND acl.grantee<>c.relowner
            """),
            {"table": table, "column": column},
        )
        if grants:
            raise RuntimeError("revoke runtime lock-column grants before downgrade")
    for table in _LOCK_COLUMNS:
        op.execute(f"DROP TRIGGER reject_registry_key_update ON public.{table}")
    op.execute("DROP FUNCTION engine.reject_registry_key_update()")
