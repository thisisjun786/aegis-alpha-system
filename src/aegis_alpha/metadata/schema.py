from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

_NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=_NAMING_CONVENTION)


def _json_type() -> JSON:
    return JSON().with_variant(JSONB(), "postgresql")


def _sha256_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(
        f"length({column_name}) = 64 AND {column_name} ~ '^[0-9a-f]{{64}}$'",
        name=f"{column_name}_sha256",
    )


def _nonempty_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(f"btrim({column_name}) <> ''", name=f"{column_name}_nonempty")


source_snapshots = Table(
    "source_snapshots",
    metadata,
    Column("snapshot_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("provider", String(100), nullable=False, index=True),
    Column("dataset", String(150), nullable=False),
    Column("source_uri", Text, nullable=False),
    Column("request_fingerprint", String(71), nullable=False),
    Column("parameters_json", _json_type(), nullable=False),
    Column("requested_at_utc", DateTime(timezone=True), nullable=False),
    Column("retrieved_at_utc", DateTime(timezone=True), nullable=False),
    Column("provider_published_at_utc", DateTime(timezone=True)),
    Column("provider_watermark", Text),
    Column("observation_date", String(32)),
    Column("content_type", String(150), nullable=False),
    Column("encoding", String(100)),
    Column("compression", String(100)),
    Column("raw_byte_length", BigInteger, nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("tree_sha256", String(64)),
    Column("parser_name", String(150), nullable=False),
    Column("parser_version", String(100), nullable=False),
    Column("validation_status", String(16), nullable=False),
    Column("row_count", BigInteger),
    Column("coverage_start", Date),
    Column("coverage_end", Date),
    Column("license_classification", String(100), nullable=False),
    Column("retention_classification", String(100), nullable=False),
    Column("manifest_json", _json_type(), nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("snapshot_id"),
    _nonempty_check("provider"),
    _nonempty_check("dataset"),
    _nonempty_check("source_uri"),
    _nonempty_check("content_type"),
    _nonempty_check("parser_name"),
    _nonempty_check("parser_version"),
    _nonempty_check("license_classification"),
    _nonempty_check("retention_classification"),
    CheckConstraint("raw_byte_length >= 0", name="raw_byte_length_nonnegative"),
    CheckConstraint("row_count IS NULL OR row_count >= 0", name="row_count_nonnegative"),
    CheckConstraint(
        "retrieved_at_utc >= requested_at_utc",
        name="request_retrieval_order",
    ),
    CheckConstraint(
        "coverage_start IS NULL OR coverage_end IS NULL OR coverage_end >= coverage_start",
        name="coverage_order",
    ),
    CheckConstraint(
        "validation_status IN ('PASS', 'WARN', 'BLOCKED')",
        name="validation_status_allowed",
    ),
    _sha256_check("content_sha256"),
    CheckConstraint(
        "tree_sha256 IS NULL OR (length(tree_sha256) = 64 AND tree_sha256 ~ '^[0-9a-f]{64}$')",
        name="tree_sha256",
    ),
    CheckConstraint(
        "request_fingerprint ~ '^sha256:[0-9a-f]{64}$'",
        name="request_fingerprint_sha256",
    ),
)

source_snapshot_files = Table(
    "source_snapshot_files",
    metadata,
    Column("snapshot_id", String(255), ForeignKey("source_snapshots.snapshot_id"), nullable=False),
    Column("relative_path", Text, nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("content_sha256", String(64), nullable=False),
    PrimaryKeyConstraint("snapshot_id", "relative_path"),
    _nonempty_check("relative_path"),
    CheckConstraint("relative_path !~ '(^/|(^|/)\\.\\.(/|$))'", name="relative_path_safe"),
    CheckConstraint("size_bytes >= 0", name="size_bytes_nonnegative"),
    _sha256_check("content_sha256"),
)

dataset_versions = Table(
    "dataset_versions",
    metadata,
    Column("dataset_id", String(255), nullable=False),
    Column("dataset_version", String(100), nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("coverage_start", Date),
    Column("coverage_end", Date),
    Column("identity_coverage", Numeric(6, 5), nullable=False),
    Column("freshness_status", String(16), nullable=False),
    Column("transformation_version", String(100), nullable=False),
    Column("aggregate_content_sha256", String(64), nullable=False),
    Column("canonical_eligible", Boolean, nullable=False),
    Column("backtest_eligible", Boolean, nullable=False),
    Column("paper_eligible", Boolean, nullable=False),
    Column("order_eligible", Boolean, nullable=False),
    Column("source_run_id", String(36)),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("dataset_id", "dataset_version"),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("dataset_id"),
    _nonempty_check("dataset_version"),
    _nonempty_check("transformation_version"),
    CheckConstraint("row_count >= 0", name="row_count_nonnegative"),
    CheckConstraint(
        "coverage_start IS NULL OR coverage_end IS NULL OR coverage_end >= coverage_start",
        name="coverage_order",
    ),
    CheckConstraint(
        "identity_coverage >= 0 AND identity_coverage <= 1",
        name="identity_coverage_range",
    ),
    CheckConstraint(
        "freshness_status IN ('PASS', 'WARN', 'BLOCKED')",
        name="freshness_status_allowed",
    ),
    _sha256_check("aggregate_content_sha256"),
    ForeignKeyConstraint(("source_run_id",), ("engine.engine_runs.id",)),
)

dataset_sources = Table(
    "dataset_sources",
    metadata,
    Column("dataset_id", String(255), nullable=False),
    Column("dataset_version", String(100), nullable=False),
    Column(
        "source_snapshot_id",
        String(255),
        ForeignKey("source_snapshots.snapshot_id"),
        nullable=False,
    ),
    PrimaryKeyConstraint("dataset_id", "dataset_version", "source_snapshot_id"),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version"),
        ("dataset_versions.dataset_id", "dataset_versions.dataset_version"),
        match="FULL",
    ),
)

dataset_input_files = Table(
    "dataset_input_files",
    metadata,
    Column("dataset_id", String(255), nullable=False),
    Column("dataset_version", String(100), nullable=False),
    Column("source_snapshot_id", String(255), nullable=False),
    Column("relative_path", Text, nullable=False),
    PrimaryKeyConstraint(
        "dataset_id",
        "dataset_version",
        "source_snapshot_id",
        "relative_path",
    ),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version", "source_snapshot_id"),
        (
            "dataset_sources.dataset_id",
            "dataset_sources.dataset_version",
            "dataset_sources.source_snapshot_id",
        ),
        match="FULL",
    ),
    ForeignKeyConstraint(
        ("source_snapshot_id", "relative_path"),
        ("source_snapshot_files.snapshot_id", "source_snapshot_files.relative_path"),
        match="FULL",
    ),
    _nonempty_check("relative_path"),
)

dataset_artifacts = Table(
    "dataset_artifacts",
    metadata,
    Column("dataset_id", String(255), nullable=False),
    Column("dataset_version", String(100), nullable=False),
    Column("relative_path", Text, nullable=False),
    Column("media_type", String(150), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("row_count", BigInteger),
    Column("content_sha256", String(64), nullable=False),
    Column("partition_values_json", _json_type(), nullable=False),
    PrimaryKeyConstraint("dataset_id", "dataset_version", "relative_path"),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version"),
        ("dataset_versions.dataset_id", "dataset_versions.dataset_version"),
        match="FULL",
    ),
    _nonempty_check("relative_path"),
    _nonempty_check("media_type"),
    CheckConstraint("relative_path !~ '(^/|(^|/)\\.\\.(/|$))'", name="relative_path_safe"),
    CheckConstraint("size_bytes >= 0", name="size_bytes_nonnegative"),
    CheckConstraint("row_count IS NULL OR row_count >= 0", name="row_count_nonnegative"),
    _sha256_check("content_sha256"),
)

quality_results = Table(
    "quality_results",
    metadata,
    Column("result_id", String(255), primary_key=True),
    Column("check_id", String(255), nullable=False),
    Column("check_version", String(100), nullable=False),
    Column("status", String(16), nullable=False),
    Column("dimensions_json", _json_type(), nullable=False),
    Column("details_json", _json_type(), nullable=False),
    Column("safe_next_action", Text, nullable=False),
    Column("checked_at_utc", DateTime(timezone=True), nullable=False),
    Column("source_snapshot_id", String(255), ForeignKey("source_snapshots.snapshot_id")),
    Column("dataset_id", String(255)),
    Column("dataset_version", String(100)),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version"),
        ("dataset_versions.dataset_id", "dataset_versions.dataset_version"),
        match="FULL",
    ),
    UniqueConstraint(
        "dataset_id",
        "dataset_version",
        "result_id",
        name="dataset_quality_result",
    ),
    CheckConstraint("status IN ('PASS', 'WARN', 'BLOCKED')", name="status_allowed"),
    _nonempty_check("result_id"),
    _nonempty_check("check_id"),
    _nonempty_check("check_version"),
    _nonempty_check("safe_next_action"),
    CheckConstraint(
        "(source_snapshot_id IS NOT NULL AND dataset_id IS NULL AND dataset_version IS NULL) OR "
        "(source_snapshot_id IS NULL AND dataset_id IS NOT NULL AND dataset_version IS NOT NULL)",
        name="exactly_one_subject",
    ),
)

# --- 범용 DB 계약(ADR 0014): 실행 원장 + 모듈별 write-scope ---
# 원칙: 모듈별 스키마 네임스페이스가 write-scope의 물리적 단위이며, 원장 테이블은
# append-only다(UPDATE/DELETE 금지는 Role/권한과 트리거로 강제).

engine_runs = Table(
    "engine_runs",
    metadata,
    Column("id", String(36), nullable=False),
    Column("engine", String(20), nullable=False),
    Column("mode", String(20), nullable=False),
    Column("run_ts", DateTime(timezone=True), nullable=False),
    Column("git_sha", String(40), nullable=False),
    Column("config_sha256", String(64), nullable=False),
    Column("inputs_parquet_sha256", String(64), nullable=False),
    Column("dataset_digest", String(64), nullable=False),
    Column("feature_contract_version", String(100)),
    Column("outputs_sha256", String(64)),
    Column("status", String(20), nullable=False),
    PrimaryKeyConstraint("id"),
    CheckConstraint(
        "engine IN ('aegis', 'alpha', 'risk', 'data')",
        name="engine_runs_engine_allowed",
    ),
    CheckConstraint(
        "mode IN ('baseline', 'paper', 'replay')",
        name="engine_runs_mode_allowed",
    ),
    CheckConstraint(
        "status IN ('running', 'succeeded', 'failed')",
        name="engine_runs_status_allowed",
    ),
    _sha256_check("config_sha256"),
    _sha256_check("inputs_parquet_sha256"),
    _sha256_check("dataset_digest"),
    _sha256_check("outputs_sha256"),
    _nonempty_check("git_sha"),
    schema="engine",
)

engine_run_events = Table(
    "engine_run_events",
    metadata,
    Column("id", String(36), nullable=False),
    Column("run_id", String(36), nullable=False),
    Column("event_type", String(50), nullable=False),
    Column("payload", _json_type(), nullable=False),
    Column("event_ts", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    PrimaryKeyConstraint("id"),
    ForeignKeyConstraint(("run_id",), ("engine.engine_runs.id",), ondelete="CASCADE"),
    _nonempty_check("event_type"),
    schema="engine",
)


def _module_daily_output(schema_name: str) -> Table:
    return Table(
        "daily_output",
        metadata,
        Column("as_of", Date, nullable=False),
        Column("run_id", String(36), nullable=False),
        Column("payload", _json_type(), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        PrimaryKeyConstraint("as_of", "run_id"),
        ForeignKeyConstraint(("run_id",), ("engine.engine_runs.id",), ondelete="CASCADE"),
        schema=schema_name,
    )


aegis_daily_output = _module_daily_output("aegis")
alpha_daily_output = _module_daily_output("alpha")
risk_daily_output = _module_daily_output("risk")

collection_targets = Table(
    "collection_targets",
    metadata,
    Column("domain", String(50), nullable=False),
    Column("symbol", String(50), nullable=False),
    Column("valid_from", Date, nullable=False),
    Column("valid_until", Date),
    Column("reason", Text, nullable=False),
    PrimaryKeyConstraint("domain", "symbol", "valid_from"),
    _nonempty_check("domain"),
    _nonempty_check("symbol"),
    _nonempty_check("reason"),
)
