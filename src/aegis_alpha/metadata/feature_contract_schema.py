from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
)

from aegis_alpha.metadata.schema import (
    _json_type,
    _nonempty_check,
    _sha256_check,
    metadata,
)

feature_contracts = Table(
    "feature_contracts",
    metadata,
    Column("contract_name", String(150), nullable=False),
    Column("contract_version", String(100), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("parameters_json", _json_type(), nullable=False),
    Column("definition_artifact_sha256", String(64), nullable=False),
    Column("canonical_serialization_sha256", String(64), nullable=False),
    Column("output_schema_ref", String(255), nullable=False),
    Column("consumes_capital", Boolean, nullable=False),
    Column("consumes_totalreturn", Boolean, nullable=False),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("contract_name", "contract_version"),
    UniqueConstraint("canonical_serialization_sha256"),
    CheckConstraint("schema_version = 1", name="schema_version_one"),
    CheckConstraint("consumes_capital OR consumes_totalreturn", name="consumes_at_least_one"),
    _nonempty_check("contract_name"),
    _nonempty_check("contract_version"),
    _nonempty_check("output_schema_ref"),
    _sha256_check("definition_artifact_sha256"),
    _sha256_check("canonical_serialization_sha256"),
    CheckConstraint("jsonb_typeof(parameters_json) = 'object'", name="parameters_json_object"),
)

feature_contract_inputs = Table(
    "feature_contract_inputs",
    metadata,
    Column("contract_name", String(150), nullable=False),
    Column("contract_version", String(100), nullable=False),
    Column("input_ordinal", Integer, nullable=False),
    Column("input_kind", String(32), nullable=False),
    Column("dataset_id", String(255)),
    Column("dataset_version", String(100)),
    Column("upstream_contract_name", String(150)),
    Column("upstream_contract_version", String(100)),
    Column("expected_digest_sha256", String(64), nullable=False),
    PrimaryKeyConstraint("contract_name", "contract_version", "input_ordinal"),
    ForeignKeyConstraint(
        ("contract_name", "contract_version"),
        ("feature_contracts.contract_name", "feature_contracts.contract_version"),
        match="FULL",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version"),
        ("dataset_versions.dataset_id", "dataset_versions.dataset_version"),
        match="FULL",
        ondelete="NO ACTION",
    ),
    ForeignKeyConstraint(
        ("upstream_contract_name", "upstream_contract_version"),
        ("feature_contracts.contract_name", "feature_contracts.contract_version"),
        match="FULL",
        ondelete="NO ACTION",
    ),
    CheckConstraint("input_ordinal > 0", name="input_ordinal_positive"),
    CheckConstraint(
        "input_kind IN ('dataset_version', 'feature_contract')",
        name="input_kind_allowed",
    ),
    CheckConstraint(
        "(input_kind = 'dataset_version' AND dataset_id IS NOT NULL "
        "AND dataset_version IS NOT NULL AND upstream_contract_name IS NULL "
        "AND upstream_contract_version IS NULL) "
        "OR (input_kind = 'feature_contract' AND upstream_contract_name IS NOT NULL "
        "AND upstream_contract_version IS NOT NULL AND dataset_id IS NULL "
        "AND dataset_version IS NULL)",
        name="exactly_one_subject",
    ),
    _sha256_check("expected_digest_sha256"),
)

__all__ = ["feature_contract_inputs", "feature_contracts"]
