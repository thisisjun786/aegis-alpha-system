"""create security identity registry

Revision ID: 20260731_0003
Revises: 20260729_0002
Create Date: 2026-07-31 09:40:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260731_0003"
down_revision: str | None = "20260729_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTITY_TABLES = {
    "identity_identifier_assertions",
    "identity_instruments",
    "identity_issuers",
    "identity_mapping_conflicts",
    "identity_provider_mappings",
}
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
    *_IDENTITY_TABLES,
}
_OWNER_TOKEN_HEX_LENGTH = 32

_IDENTIFIER_SYNTAX = (
    "(identifier_type = 'cik' AND identifier_value ~ '^[0-9]{10}$') OR "
    "(identifier_type = 'lei' AND identifier_value ~ '^[A-Z0-9]{20}$') OR "
    "(identifier_type = 'ticker' AND identifier_value ~ '^[A-Z0-9][A-Z0-9.-]{0,19}$') OR "
    "(identifier_type = 'mic' AND identifier_value ~ '^[A-Z0-9]{4}$') OR "
    "(identifier_type = 'cusip' AND identifier_value ~ '^[A-Z0-9]{9}$') OR "
    "(identifier_type = 'isin' AND identifier_value ~ '^[A-Z]{2}[A-Z0-9]{9}[0-9]$') OR "
    "(identifier_type IN ('figi', 'composite_figi', 'share_class_figi') AND "
    "identifier_value ~ '^BBG[BCDFGHJKLMNPQRSTVWXYZ0-9]{8}[0-9]$') OR "
    "(identifier_type = 'norgate_assetid' AND identifier_value ~ '^[0-9]{1,18}$')"
)
_IDENTIFIER_TYPES = (
    "'cik', 'lei', 'ticker', 'mic', 'cusip', 'isin', "
    "'figi', 'composite_figi', 'share_class_figi', 'norgate_assetid'"
)
_ISSUER_IDENTIFIER_TYPES = "'cik', 'lei'"
_INSTRUMENT_IDENTIFIER_TYPES = (
    "'ticker', 'mic', 'cusip', 'isin', "
    "'figi', 'composite_figi', 'share_class_figi', 'norgate_assetid'"
)
_INSTRUMENT_KINDS = (
    "'equity', 'etf', 'fund', 'adr', 'unit', 'preferred', 'warrant', 'right', 'index', 'other'"
)
_CONFLICT_CLASSES = "'instrument_disagreement', 'interval_overlap'"
_ADMISSIBLE_SNAPSHOT_STATUSES = "'PASS', 'WARN'"

# The denormalized snapshot lineage columns exist so the database can enforce
# provider agreement and admissibility. These triggers keep them equal to the
# referenced snapshot, so a direct SQL writer cannot forge a clean lineage.
#
# `FOR SHARE` is load-bearing rather than incidental: it holds the parent row
# against concurrent modification for the rest of the inserting transaction, so
# a child cannot be admitted against lineage that another transaction is in the
# middle of changing. It pairs with the parent-side guard below, which refuses
# to change lineage while any identity evidence still references the snapshot.
_SYNC_ASSERTION_LINEAGE = """
CREATE FUNCTION identity_sync_assertion_lineage() RETURNS trigger AS $$
DECLARE
    snapshot_status text;
BEGIN
    SELECT validation_status INTO snapshot_status
    FROM source_snapshots WHERE snapshot_id = NEW.source_snapshot_id FOR SHARE;
    IF snapshot_status IS NULL THEN
        RAISE EXCEPTION 'identity evidence requires a registered source snapshot';
    END IF;
    NEW.source_validation_status := snapshot_status;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""

_SYNC_MAPPING_LINEAGE = """
CREATE FUNCTION identity_sync_mapping_lineage() RETURNS trigger AS $$
DECLARE
    snapshot_provider text;
    snapshot_status text;
BEGIN
    SELECT provider, validation_status INTO snapshot_provider, snapshot_status
    FROM source_snapshots WHERE snapshot_id = NEW.source_snapshot_id FOR SHARE;
    IF snapshot_provider IS NULL THEN
        RAISE EXCEPTION 'identity evidence requires a registered source snapshot';
    END IF;
    NEW.source_provider := snapshot_provider;
    NEW.source_validation_status := snapshot_status;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""

# Child-side synchronization alone only fixes lineage at write time. Without
# this parent-side guard, later flipping a snapshot to BLOCKED or to another
# provider would leave already-admitted identity evidence backed by inadmissible
# or foreign evidence while its stored lineage still looked clean.
#
# Identity evidence is deliberately immutable, so the safe rule is that a
# referenced snapshot's lineage is frozen too. Unrelated snapshot columns stay
# freely updatable, and lineage remains editable until identity evidence
# actually references that snapshot.
_GUARD_SNAPSHOT_LINEAGE = """
CREATE FUNCTION identity_guard_snapshot_lineage() RETURNS trigger AS $$
BEGIN
    IF NEW.provider IS DISTINCT FROM OLD.provider
        OR NEW.validation_status IS DISTINCT FROM OLD.validation_status
    THEN
        IF EXISTS (
            SELECT 1 FROM identity_provider_mappings
            WHERE source_snapshot_id = OLD.snapshot_id
        ) OR EXISTS (
            SELECT 1 FROM identity_identifier_assertions
            WHERE source_snapshot_id = OLD.snapshot_id
        ) THEN
            RAISE EXCEPTION
                'source snapshot lineage is immutable while identity evidence references it';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""


def _json_column(name: str) -> sa.Column[object]:
    return sa.Column(
        name,
        sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
        nullable=False,
    )


def _registered_at(name: str) -> sa.Column[object]:
    return sa.Column(
        name,
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    )


def upgrade() -> None:
    # Required for the provider-mapping EXCLUDE constraint, which mixes equality
    # on the provider key with range overlap on the effective interval.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.create_table(
        "identity_issuers",
        sa.Column("issuer_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("jurisdiction", sa.String(length=2), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        _json_column("evidence_json"),
        _registered_at("registered_at_utc"),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_identity_issuers_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "btrim(issuer_id) <> ''",
            name=op.f("ck_identity_issuers_issuer_id_nonempty"),
        ),
        sa.CheckConstraint(
            "display_name IS NULL OR btrim(display_name) <> ''",
            name=op.f("ck_identity_issuers_display_name_nonempty"),
        ),
        sa.CheckConstraint(
            "jurisdiction IS NULL OR jurisdiction ~ '^[A-Z]{2}$'",
            name=op.f("ck_identity_issuers_jurisdiction_iso3166"),
        ),
        sa.PrimaryKeyConstraint("issuer_id", name=op.f("pk_identity_issuers")),
    )
    op.create_table(
        "identity_instruments",
        sa.Column("instrument_id", sa.String(length=255), nullable=False),
        sa.Column("issuer_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("instrument_kind", sa.String(length=16), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        _json_column("evidence_json"),
        _registered_at("registered_at_utc"),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_identity_instruments_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "btrim(instrument_id) <> ''",
            name=op.f("ck_identity_instruments_instrument_id_nonempty"),
        ),
        sa.CheckConstraint(
            f"instrument_kind IN ({_INSTRUMENT_KINDS})",
            name=op.f("ck_identity_instruments_instrument_kind_allowed"),
        ),
        sa.CheckConstraint(
            "display_name IS NULL OR btrim(display_name) <> ''",
            name=op.f("ck_identity_instruments_display_name_nonempty"),
        ),
        sa.ForeignKeyConstraint(
            ["issuer_id"],
            ["identity_issuers.issuer_id"],
            name=op.f("fk_identity_instruments_issuer_id_identity_issuers"),
        ),
        sa.PrimaryKeyConstraint("instrument_id", name=op.f("pk_identity_instruments")),
    )
    op.create_index(
        op.f("ix_identity_instruments_issuer_id"),
        "identity_instruments",
        ["issuer_id"],
        unique=False,
    )
    op.create_table(
        "identity_identifier_assertions",
        sa.Column("assertion_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column(
            "issuer_ref",
            sa.String(length=255),
            sa.Computed("CASE WHEN entity_type = 'issuer' THEN entity_id END", persisted=True),
            nullable=True,
        ),
        sa.Column(
            "instrument_ref",
            sa.String(length=255),
            sa.Computed("CASE WHEN entity_type = 'instrument' THEN entity_id END", persisted=True),
            nullable=True,
        ),
        sa.Column("identifier_type", sa.String(length=32), nullable=False),
        sa.Column("identifier_value", sa.String(length=64), nullable=False),
        sa.Column("source_value", sa.Text(), nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=255), nullable=False),
        sa.Column("effective_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("asserted_at_utc", sa.DateTime(timezone=True), nullable=False),
        _json_column("evidence_json"),
        sa.Column("assertion_sha256", sa.String(length=64), nullable=False),
        _registered_at("registered_at_utc"),
        sa.Column("source_validation_status", sa.String(length=16), nullable=False),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_identity_identifier_assertions_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "btrim(assertion_id) <> ''",
            name=op.f("ck_identity_identifier_assertions_assertion_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(entity_id) <> ''",
            name=op.f("ck_identity_identifier_assertions_entity_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(source_value) <> ''",
            name=op.f("ck_identity_identifier_assertions_source_value_nonempty"),
        ),
        sa.CheckConstraint(
            "entity_type IN ('issuer', 'instrument')",
            name=op.f("ck_identity_identifier_assertions_entity_type_allowed"),
        ),
        sa.CheckConstraint(
            f"identifier_type IN ({_IDENTIFIER_TYPES})",
            name=op.f("ck_identity_identifier_assertions_identifier_type_allowed"),
        ),
        sa.CheckConstraint(
            f"(entity_type = 'issuer' AND identifier_type IN ({_ISSUER_IDENTIFIER_TYPES})) OR "
            f"(entity_type = 'instrument' AND "
            f"identifier_type IN ({_INSTRUMENT_IDENTIFIER_TYPES}))",
            name=op.f("ck_identity_identifier_assertions_identifier_entity_level"),
        ),
        sa.CheckConstraint(
            _IDENTIFIER_SYNTAX,
            name=op.f("ck_identity_identifier_assertions_identifier_value_syntax"),
        ),
        sa.CheckConstraint(
            "effective_end IS NULL OR effective_end > effective_start",
            name=op.f("ck_identity_identifier_assertions_effective_interval_order"),
        ),
        sa.CheckConstraint(
            f"source_validation_status IN ({_ADMISSIBLE_SNAPSHOT_STATUSES})",
            name=op.f("ck_identity_identifier_assertions_source_status_admissible"),
        ),
        sa.CheckConstraint(
            "length(assertion_sha256) = 64 AND assertion_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_identity_identifier_assertions_assertion_sha256_sha256"),
        ),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshots.snapshot_id"],
            name=op.f("fk_identity_identifier_assertions_source_snapshot_id_source_snapshots"),
        ),
        sa.ForeignKeyConstraint(
            ["issuer_ref"],
            ["identity_issuers.issuer_id"],
            name=op.f("fk_identity_identifier_assertions_issuer_ref_identity_issuers"),
        ),
        sa.ForeignKeyConstraint(
            ["instrument_ref"],
            ["identity_instruments.instrument_id"],
            name=op.f("fk_identity_identifier_assertions_instrument_ref_identity_instruments"),
        ),
        sa.PrimaryKeyConstraint("assertion_id", name=op.f("pk_identity_identifier_assertions")),
        sa.UniqueConstraint(
            "assertion_sha256",
            name=op.f("uq_identity_identifier_assertions_assertion_sha256"),
        ),
        sa.UniqueConstraint(
            "entity_type",
            "entity_id",
            "identifier_type",
            "identifier_value",
            "effective_start",
            name="assertion_effective_identity",
        ),
    )
    op.create_index(
        "ix_identity_identifier_assertions_entity",
        "identity_identifier_assertions",
        ["entity_type", "entity_id", "effective_start"],
        unique=False,
    )
    op.create_index(
        "ix_identity_identifier_assertions_lookup",
        "identity_identifier_assertions",
        ["identifier_type", "identifier_value", "effective_start"],
        unique=False,
    )
    # Identity history must not contradict itself in either direction: one
    # entity cannot hold two different values of the same identifier type at
    # once, and one identifier value cannot name two different entities at once.
    # Half-open '[)' keeps adjacency legal, so a dated ticker change and a later
    # reuse of a retired symbol both remain expressible.
    op.execute(
        "ALTER TABLE identity_identifier_assertions "
        "ADD CONSTRAINT ex_identity_identifier_assertions_one_value "
        "EXCLUDE USING gist ("
        "entity_type WITH =, entity_id WITH =, identifier_type WITH =, "
        "identifier_value WITH <>, tstzrange(effective_start, effective_end, '[)') WITH &&"
        ")"
    )
    op.execute(
        "ALTER TABLE identity_identifier_assertions "
        "ADD CONSTRAINT ex_identity_identifier_assertions_one_entity "
        "EXCLUDE USING gist ("
        "identifier_type WITH =, identifier_value WITH =, entity_id WITH <>, "
        "tstzrange(effective_start, effective_end, '[)') WITH &&"
        ")"
    )
    op.execute(_SYNC_ASSERTION_LINEAGE)
    op.execute(
        "CREATE TRIGGER identity_assertion_lineage "
        "BEFORE INSERT OR UPDATE ON identity_identifier_assertions "
        "FOR EACH ROW EXECUTE FUNCTION identity_sync_assertion_lineage()"
    )
    op.create_table(
        "identity_provider_mappings",
        sa.Column("mapping_id", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("namespace", sa.String(length=100), nullable=False),
        sa.Column("provider_identifier", sa.String(length=128), nullable=False),
        sa.Column("instrument_id", sa.String(length=255), nullable=False),
        sa.Column("source_snapshot_id", sa.String(length=255), nullable=False),
        sa.Column("effective_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("asserted_at_utc", sa.DateTime(timezone=True), nullable=False),
        _json_column("evidence_json"),
        sa.Column("mapping_sha256", sa.String(length=64), nullable=False),
        _registered_at("registered_at_utc"),
        sa.Column("source_provider", sa.String(length=100), nullable=False),
        sa.Column("source_validation_status", sa.String(length=16), nullable=False),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_identity_provider_mappings_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "btrim(mapping_id) <> ''",
            name=op.f("ck_identity_provider_mappings_mapping_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider) <> ''",
            name=op.f("ck_identity_provider_mappings_provider_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(namespace) <> ''",
            name=op.f("ck_identity_provider_mappings_namespace_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider_identifier) <> ''",
            name=op.f("ck_identity_provider_mappings_provider_identifier_nonempty"),
        ),
        sa.CheckConstraint(
            "effective_end IS NULL OR effective_end > effective_start",
            name=op.f("ck_identity_provider_mappings_effective_interval_order"),
        ),
        sa.CheckConstraint(
            "source_provider = provider",
            name=op.f("ck_identity_provider_mappings_source_provider_matches"),
        ),
        sa.CheckConstraint(
            f"source_validation_status IN ({_ADMISSIBLE_SNAPSHOT_STATUSES})",
            name=op.f("ck_identity_provider_mappings_source_status_admissible"),
        ),
        sa.CheckConstraint(
            "length(mapping_sha256) = 64 AND mapping_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_identity_provider_mappings_mapping_sha256_sha256"),
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["identity_instruments.instrument_id"],
            name=op.f("fk_identity_provider_mappings_instrument_id_identity_instruments"),
        ),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshots.snapshot_id"],
            name=op.f("fk_identity_provider_mappings_source_snapshot_id_source_snapshots"),
        ),
        sa.PrimaryKeyConstraint("mapping_id", name=op.f("pk_identity_provider_mappings")),
        sa.UniqueConstraint(
            "mapping_sha256",
            name=op.f("uq_identity_provider_mappings_mapping_sha256"),
        ),
        sa.UniqueConstraint(
            "provider",
            "namespace",
            "provider_identifier",
            "effective_start",
            name="provider_key_effective_start",
        ),
    )
    op.create_index(
        op.f("ix_identity_provider_mappings_instrument_id"),
        "identity_provider_mappings",
        ["instrument_id"],
        unique=False,
    )
    op.create_index(
        "ix_identity_provider_mappings_lookup",
        "identity_provider_mappings",
        ["provider", "namespace", "provider_identifier", "effective_start"],
        unique=False,
    )
    # A provider identifier may have at most one open-ended mapping. This is the
    # database backstop behind the registry's interval-overlap rejection.
    op.create_index(
        "uq_identity_provider_mappings_open_interval",
        "identity_provider_mappings",
        ["provider", "namespace", "provider_identifier"],
        unique=True,
        postgresql_where=sa.text("effective_end IS NULL"),
    )
    # Database-level guarantee that one provider identifier never maps to two
    # instruments at the same effective time, even if a writer bypasses the
    # registry API or two transactions race. Half-open '[)' matches the
    # documented Python interval semantics exactly.
    op.execute(
        "ALTER TABLE identity_provider_mappings "
        "ADD CONSTRAINT ex_identity_provider_mappings_no_overlap "
        "EXCLUDE USING gist ("
        "provider WITH =, namespace WITH =, provider_identifier WITH =, "
        "tstzrange(effective_start, effective_end, '[)') WITH &&"
        ")"
    )
    op.execute(_SYNC_MAPPING_LINEAGE)
    op.execute(
        "CREATE TRIGGER identity_mapping_lineage "
        "BEFORE INSERT OR UPDATE ON identity_provider_mappings "
        "FOR EACH ROW EXECUTE FUNCTION identity_sync_mapping_lineage()"
    )
    op.create_table(
        "identity_mapping_conflicts",
        sa.Column("conflict_id", sa.String(length=255), nullable=False),
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("namespace", sa.String(length=100), nullable=False),
        sa.Column("provider_identifier", sa.String(length=128), nullable=False),
        sa.Column("conflict_class", sa.String(length=32), nullable=False),
        sa.Column("attempted_instrument_id", sa.String(length=255), nullable=False),
        sa.Column("attempted_source_snapshot_id", sa.String(length=255), nullable=False),
        sa.Column("attempted_effective_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempted_effective_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("existing_mapping_id", sa.String(length=255), nullable=False),
        sa.Column("existing_instrument_id", sa.String(length=255), nullable=False),
        sa.Column("detected_at_utc", sa.DateTime(timezone=True), nullable=False),
        _json_column("details_json"),
        _registered_at("recorded_at_utc"),
        sa.CheckConstraint(
            "btrim(conflict_id) <> ''",
            name=op.f("ck_identity_mapping_conflicts_conflict_id_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider) <> ''",
            name=op.f("ck_identity_mapping_conflicts_provider_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(namespace) <> ''",
            name=op.f("ck_identity_mapping_conflicts_namespace_nonempty"),
        ),
        sa.CheckConstraint(
            "btrim(provider_identifier) <> ''",
            name=op.f("ck_identity_mapping_conflicts_provider_identifier_nonempty"),
        ),
        sa.CheckConstraint(
            f"conflict_class IN ({_CONFLICT_CLASSES})",
            name=op.f("ck_identity_mapping_conflicts_conflict_class_allowed"),
        ),
        sa.CheckConstraint(
            "attempted_effective_end IS NULL OR "
            "attempted_effective_end > attempted_effective_start",
            name=op.f("ck_identity_mapping_conflicts_attempted_interval_order"),
        ),
        sa.ForeignKeyConstraint(
            ["attempted_instrument_id"],
            ["identity_instruments.instrument_id"],
            name=op.f("fk_identity_mapping_conflicts_attempted_instrument_id_identity_instruments"),
        ),
        sa.ForeignKeyConstraint(
            ["attempted_source_snapshot_id"],
            ["source_snapshots.snapshot_id"],
            name=op.f(
                "fk_identity_mapping_conflicts_attempted_source_snapshot_id_source_snapshots"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["existing_instrument_id"],
            ["identity_instruments.instrument_id"],
            name=op.f("fk_identity_mapping_conflicts_existing_instrument_id_identity_instruments"),
        ),
        sa.ForeignKeyConstraint(
            ["existing_mapping_id"],
            ["identity_provider_mappings.mapping_id"],
            name=op.f(
                "fk_identity_mapping_conflicts_existing_mapping_id_identity_provider_mappings"
            ),
        ),
        sa.PrimaryKeyConstraint("conflict_id", name=op.f("pk_identity_mapping_conflicts")),
    )
    op.create_index(
        "ix_identity_mapping_conflicts_provider_key",
        "identity_mapping_conflicts",
        ["provider", "namespace", "provider_identifier"],
        unique=False,
    )
    # Created last: the guard body references both identity child tables, so
    # they must already exist.
    op.execute(_GUARD_SNAPSHOT_LINEAGE)
    op.execute(
        "CREATE TRIGGER identity_guard_snapshot_lineage "
        "BEFORE UPDATE ON source_snapshots "
        "FOR EACH ROW EXECUTE FUNCTION identity_guard_snapshot_lineage()"
    )


def downgrade() -> None:
    _assert_fixture_owned_database()
    op.execute("DROP TRIGGER identity_guard_snapshot_lineage ON source_snapshots")
    op.execute("DROP FUNCTION identity_guard_snapshot_lineage()")
    op.drop_index(
        "ix_identity_mapping_conflicts_provider_key",
        table_name="identity_mapping_conflicts",
    )
    op.drop_table("identity_mapping_conflicts")
    op.execute("DROP TRIGGER identity_mapping_lineage ON identity_provider_mappings")
    op.execute("DROP FUNCTION identity_sync_mapping_lineage()")
    op.execute(
        "ALTER TABLE identity_provider_mappings "
        "DROP CONSTRAINT ex_identity_provider_mappings_no_overlap"
    )
    op.drop_index(
        "uq_identity_provider_mappings_open_interval",
        table_name="identity_provider_mappings",
    )
    op.drop_index(
        "ix_identity_provider_mappings_lookup",
        table_name="identity_provider_mappings",
    )
    op.drop_index(
        op.f("ix_identity_provider_mappings_instrument_id"),
        table_name="identity_provider_mappings",
    )
    op.drop_table("identity_provider_mappings")
    op.execute("DROP TRIGGER identity_assertion_lineage ON identity_identifier_assertions")
    op.execute("DROP FUNCTION identity_sync_assertion_lineage()")
    op.execute(
        "ALTER TABLE identity_identifier_assertions "
        "DROP CONSTRAINT ex_identity_identifier_assertions_one_entity"
    )
    op.execute(
        "ALTER TABLE identity_identifier_assertions "
        "DROP CONSTRAINT ex_identity_identifier_assertions_one_value"
    )
    op.drop_index(
        "ix_identity_identifier_assertions_lookup",
        table_name="identity_identifier_assertions",
    )
    op.drop_index(
        "ix_identity_identifier_assertions_entity",
        table_name="identity_identifier_assertions",
    )
    op.drop_table("identity_identifier_assertions")
    op.drop_index(
        op.f("ix_identity_instruments_issuer_id"),
        table_name="identity_instruments",
    )
    op.drop_table("identity_instruments")
    op.drop_table("identity_issuers")


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
