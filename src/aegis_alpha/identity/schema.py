from __future__ import annotations

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

from aegis_alpha.metadata.schema import metadata


def _json_type() -> JSON:
    return JSON().with_variant(JSONB(), "postgresql")


def _sha256_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(
        f"length({column_name}) = 64 AND {column_name} ~ '^[0-9a-f]{{64}}$'",
        name=f"{column_name}_sha256",
    )


def _nonempty_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(f"btrim({column_name}) <> ''", name=f"{column_name}_nonempty")


ENTITY_TYPES = ("issuer", "instrument")
_ENTITY_TYPES_SQL = "('issuer', 'instrument')"

INSTRUMENT_KINDS = (
    "equity",
    "etf",
    "fund",
    "adr",
    "unit",
    "preferred",
    "warrant",
    "right",
    "index",
    "other",
)
_INSTRUMENT_KINDS_SQL = ", ".join(f"'{kind}'" for kind in INSTRUMENT_KINDS)

ISSUER_IDENTIFIER_TYPES = ("cik", "lei")
INSTRUMENT_IDENTIFIER_TYPES = (
    "ticker",
    "mic",
    "cusip",
    "isin",
    "figi",
    "composite_figi",
    "share_class_figi",
    "norgate_assetid",
)
IDENTIFIER_TYPES = ISSUER_IDENTIFIER_TYPES + INSTRUMENT_IDENTIFIER_TYPES
_IDENTIFIER_TYPES_SQL = ", ".join(f"'{name}'" for name in IDENTIFIER_TYPES)
_ISSUER_IDENTIFIER_TYPES_SQL = ", ".join(f"'{name}'" for name in ISSUER_IDENTIFIER_TYPES)
_INSTRUMENT_IDENTIFIER_TYPES_SQL = ", ".join(f"'{name}'" for name in INSTRUMENT_IDENTIFIER_TYPES)

CONFLICT_CLASSES = ("instrument_disagreement", "interval_overlap")
_CONFLICT_CLASSES_SQL = ", ".join(f"'{name}'" for name in CONFLICT_CLASSES)

#: Snapshot validation statuses that may back a canonical identity assertion or
#: provider mapping. A ``BLOCKED`` snapshot is retained as evidence but must
#: never become the basis of an identity fact.
ADMISSIBLE_SNAPSHOT_STATUSES = ("PASS", "WARN")
_ADMISSIBLE_SNAPSHOT_STATUSES_SQL = ", ".join(
    f"'{status}'" for status in ADMISSIBLE_SNAPSHOT_STATUSES
)

# Identifier syntax is checked in the database as well as the record layer so a
# direct SQL writer cannot introduce a malformed identity. Checksum digits are
# verified only in the record layer; SQL check constraints cannot express them.
_IDENTIFIER_SYNTAX_SQL = (
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

_EFFECTIVE_INTERVAL_SQL = "effective_end IS NULL OR effective_end > effective_start"


identity_issuers = Table(
    "identity_issuers",
    metadata,
    Column("issuer_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("display_name", Text),
    Column("jurisdiction", String(2)),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    Column("evidence_json", _json_type(), nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("issuer_id"),
    CheckConstraint(
        "display_name IS NULL OR btrim(display_name) <> ''",
        name="display_name_nonempty",
    ),
    CheckConstraint(
        "jurisdiction IS NULL OR jurisdiction ~ '^[A-Z]{2}$'",
        name="jurisdiction_iso3166",
    ),
)

identity_instruments = Table(
    "identity_instruments",
    metadata,
    Column("instrument_id", String(255), primary_key=True),
    Column(
        "issuer_id",
        String(255),
        ForeignKey("identity_issuers.issuer_id"),
        nullable=False,
        index=True,
    ),
    Column("schema_version", Integer, nullable=False),
    Column("instrument_kind", String(16), nullable=False),
    Column("display_name", Text),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    Column("evidence_json", _json_type(), nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("instrument_id"),
    CheckConstraint(
        f"instrument_kind IN ({_INSTRUMENT_KINDS_SQL})",
        name="instrument_kind_allowed",
    ),
    CheckConstraint(
        "display_name IS NULL OR btrim(display_name) <> ''",
        name="display_name_nonempty",
    ),
)

identity_identifier_assertions = Table(
    "identity_identifier_assertions",
    metadata,
    Column("assertion_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("entity_type", String(16), nullable=False),
    Column("entity_id", String(255), nullable=False),
    # Split the polymorphic entity reference into two typed generated columns so
    # each can carry a real foreign key. Without this an assertion could name a
    # nonexistent entity, or name an instrument while declaring issuer level.
    Column(
        "issuer_ref",
        String(255),
        Computed("CASE WHEN entity_type = 'issuer' THEN entity_id END", persisted=True),
    ),
    Column(
        "instrument_ref",
        String(255),
        Computed("CASE WHEN entity_type = 'instrument' THEN entity_id END", persisted=True),
    ),
    Column("identifier_type", String(32), nullable=False),
    Column("identifier_value", String(64), nullable=False),
    Column("source_value", Text, nullable=False),
    Column(
        "source_snapshot_id",
        String(255),
        ForeignKey("source_snapshots.snapshot_id"),
        nullable=False,
    ),
    ForeignKeyConstraint(
        ("issuer_ref",),
        ("identity_issuers.issuer_id",),
    ),
    ForeignKeyConstraint(
        ("instrument_ref",),
        ("identity_instruments.instrument_id",),
    ),
    Column("effective_start", DateTime(timezone=True), nullable=False),
    Column("effective_end", DateTime(timezone=True)),
    Column("asserted_at_utc", DateTime(timezone=True), nullable=False),
    Column("evidence_json", _json_type(), nullable=False),
    Column("assertion_sha256", String(64), nullable=False, unique=True),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Denormalized snapshot status. An assertion carries no provider of its own,
    # so only admissibility applies here; the migration adds a trigger keeping
    # this equal to the referenced snapshot so it cannot be forged.
    Column("source_validation_status", String(16), nullable=False),
    UniqueConstraint(
        "entity_type",
        "entity_id",
        "identifier_type",
        "identifier_value",
        "effective_start",
        name="assertion_effective_identity",
    ),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("assertion_id"),
    _nonempty_check("entity_id"),
    _nonempty_check("source_value"),
    CheckConstraint(f"entity_type IN {_ENTITY_TYPES_SQL}", name="entity_type_allowed"),
    CheckConstraint(
        f"identifier_type IN ({_IDENTIFIER_TYPES_SQL})",
        name="identifier_type_allowed",
    ),
    CheckConstraint(
        f"(entity_type = 'issuer' AND identifier_type IN ({_ISSUER_IDENTIFIER_TYPES_SQL})) OR "
        f"(entity_type = 'instrument' AND "
        f"identifier_type IN ({_INSTRUMENT_IDENTIFIER_TYPES_SQL}))",
        name="identifier_entity_level",
    ),
    CheckConstraint(_IDENTIFIER_SYNTAX_SQL, name="identifier_value_syntax"),
    CheckConstraint(_EFFECTIVE_INTERVAL_SQL, name="effective_interval_order"),
    CheckConstraint(
        f"source_validation_status IN ({_ADMISSIBLE_SNAPSHOT_STATUSES_SQL})",
        name="source_status_admissible",
    ),
    _sha256_check("assertion_sha256"),
    Index(
        "ix_identity_identifier_assertions_lookup",
        "identifier_type",
        "identifier_value",
        "effective_start",
    ),
    Index(
        "ix_identity_identifier_assertions_entity",
        "entity_type",
        "entity_id",
        "effective_start",
    ),
)

identity_provider_mappings = Table(
    "identity_provider_mappings",
    metadata,
    Column("mapping_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("provider", String(100), nullable=False),
    Column("namespace", String(100), nullable=False),
    Column("provider_identifier", String(128), nullable=False),
    Column(
        "instrument_id",
        String(255),
        ForeignKey("identity_instruments.instrument_id"),
        nullable=False,
        index=True,
    ),
    Column(
        "source_snapshot_id",
        String(255),
        ForeignKey("source_snapshots.snapshot_id"),
        nullable=False,
    ),
    Column("effective_start", DateTime(timezone=True), nullable=False),
    Column("effective_end", DateTime(timezone=True)),
    Column("asserted_at_utc", DateTime(timezone=True), nullable=False),
    Column("evidence_json", _json_type(), nullable=False),
    Column("mapping_sha256", String(64), nullable=False, unique=True),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Denormalized snapshot lineage. A mapping asserts what a *provider* meant,
    # so evidence from a different provider cannot support it, and a BLOCKED
    # snapshot may not back an identity fact at all. The migration adds a
    # trigger keeping both equal to the referenced snapshot so neither can be
    # forged by a direct SQL writer.
    Column("source_provider", String(100), nullable=False),
    Column("source_validation_status", String(16), nullable=False),
    UniqueConstraint(
        "provider",
        "namespace",
        "provider_identifier",
        "effective_start",
        name="provider_key_effective_start",
    ),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("mapping_id"),
    _nonempty_check("provider"),
    _nonempty_check("namespace"),
    _nonempty_check("provider_identifier"),
    CheckConstraint(_EFFECTIVE_INTERVAL_SQL, name="effective_interval_order"),
    CheckConstraint("source_provider = provider", name="source_provider_matches"),
    CheckConstraint(
        f"source_validation_status IN ({_ADMISSIBLE_SNAPSHOT_STATUSES_SQL})",
        name="source_status_admissible",
    ),
    _sha256_check("mapping_sha256"),
    Index(
        "ix_identity_provider_mappings_lookup",
        "provider",
        "namespace",
        "provider_identifier",
        "effective_start",
    ),
)

# At most one open-ended mapping may exist for a provider key. This partial
# unique index is the database backstop for the API-level overlap rejection.
identity_provider_mappings_open_interval = Index(
    "uq_identity_provider_mappings_open_interval",
    identity_provider_mappings.c.provider,
    identity_provider_mappings.c.namespace,
    identity_provider_mappings.c.provider_identifier,
    unique=True,
    postgresql_where=identity_provider_mappings.c.effective_end.is_(None),
)

identity_mapping_conflicts = Table(
    "identity_mapping_conflicts",
    metadata,
    Column("conflict_id", String(255), primary_key=True),
    Column("provider", String(100), nullable=False),
    Column("namespace", String(100), nullable=False),
    Column("provider_identifier", String(128), nullable=False),
    Column("conflict_class", String(32), nullable=False),
    Column(
        "attempted_instrument_id",
        String(255),
        ForeignKey("identity_instruments.instrument_id"),
        nullable=False,
    ),
    Column(
        "attempted_source_snapshot_id",
        String(255),
        ForeignKey("source_snapshots.snapshot_id"),
        nullable=False,
    ),
    Column("attempted_effective_start", DateTime(timezone=True), nullable=False),
    Column("attempted_effective_end", DateTime(timezone=True)),
    Column(
        "existing_mapping_id",
        String(255),
        ForeignKey("identity_provider_mappings.mapping_id"),
        nullable=False,
    ),
    Column(
        "existing_instrument_id",
        String(255),
        ForeignKey("identity_instruments.instrument_id"),
        nullable=False,
    ),
    Column("detected_at_utc", DateTime(timezone=True), nullable=False),
    Column("details_json", _json_type(), nullable=False),
    Column("recorded_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    _nonempty_check("conflict_id"),
    _nonempty_check("provider"),
    _nonempty_check("namespace"),
    _nonempty_check("provider_identifier"),
    CheckConstraint(
        f"conflict_class IN ({_CONFLICT_CLASSES_SQL})",
        name="conflict_class_allowed",
    ),
    CheckConstraint(
        "attempted_effective_end IS NULL OR attempted_effective_end > attempted_effective_start",
        name="attempted_interval_order",
    ),
    Index(
        "ix_identity_mapping_conflicts_provider_key",
        "provider",
        "namespace",
        "provider_identifier",
    ),
)
