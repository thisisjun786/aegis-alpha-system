from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
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


def _optional_sha256_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(
        f"{column_name} IS NULL OR (length({column_name}) = 64 AND "
        f"{column_name} ~ '^[0-9a-f]{{64}}$')",
        name=f"{column_name}_sha256",
    )


def _nonempty_check(column_name: str) -> CheckConstraint:
    return CheckConstraint(f"btrim({column_name}) <> ''", name=f"{column_name}_nonempty")


_ATTEMPT_EVENT_TYPES = "('attempt_started', 'attempt_succeeded', 'attempt_failed')"
_TERMINAL_EVENT_TYPES = "('run_succeeded', 'run_failed', 'run_cancelled')"

collection_run_plans = Table(
    "collection_run_plans",
    metadata,
    Column("plan_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("provider", String(100), nullable=False, index=True),
    Column("dataset", String(150), nullable=False),
    Column("mode", String(16), nullable=False),
    Column("requested_window_start", DateTime(timezone=True)),
    Column("requested_window_end", DateTime(timezone=True)),
    Column("parameters_json", _json_type(), nullable=False),
    Column("plan_sha256", String(64), nullable=False, unique=True),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("schema_version > 0", name="schema_version_positive"),
    _nonempty_check("plan_id"),
    _nonempty_check("provider"),
    _nonempty_check("dataset"),
    CheckConstraint("mode IN ('probe', 'incremental', 'backfill')", name="mode_allowed"),
    CheckConstraint(
        "(requested_window_start IS NULL) = (requested_window_end IS NULL)",
        name="requested_window_paired",
    ),
    CheckConstraint(
        "requested_window_start IS NULL OR requested_window_end IS NULL OR "
        "requested_window_end >= requested_window_start",
        name="requested_window_order",
    ),
    _sha256_check("plan_sha256"),
)

collection_runs = Table(
    "collection_runs",
    metadata,
    Column("run_id", String(255), primary_key=True),
    Column(
        "plan_id",
        String(255),
        ForeignKey("collection_run_plans.plan_id"),
        nullable=False,
    ),
    Column("created_at_utc", DateTime(timezone=True), nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    _nonempty_check("run_id"),
)

collection_run_events = Table(
    "collection_run_events",
    metadata,
    Column("run_id", String(255), ForeignKey("collection_runs.run_id"), nullable=False),
    Column("event_seq", BigInteger, nullable=False),
    Column("event_type", String(32), nullable=False),
    Column("attempt_number", Integer),
    Column("retry_of_attempt", Integer),
    Column("error_class", String(150)),
    Column("error_message", Text),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("details_json", _json_type(), nullable=False),
    Column("recorded_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("run_id", "event_seq"),
    CheckConstraint("event_seq >= 1", name="event_seq_positive"),
    CheckConstraint(
        "event_type IN ('attempt_started', 'attempt_succeeded', 'attempt_failed', "
        "'run_succeeded', 'run_failed', 'run_cancelled')",
        name="event_type_allowed",
    ),
    CheckConstraint(
        f"(event_type IN {_ATTEMPT_EVENT_TYPES}) = (attempt_number IS NOT NULL)",
        name="attempt_events_require_attempt_number",
    ),
    CheckConstraint(
        "attempt_number IS NULL OR attempt_number > 0",
        name="attempt_number_positive",
    ),
    CheckConstraint(
        "retry_of_attempt IS NULL OR (event_type = 'attempt_started' AND retry_of_attempt > 0)",
        name="retry_link_only_on_attempt_start",
    ),
    CheckConstraint(
        "(event_type IN ('attempt_failed', 'run_failed') AND error_class IS NOT NULL AND "
        "btrim(error_class) <> '') OR "
        "(event_type NOT IN ('attempt_failed', 'run_failed') AND error_class IS NULL)",
        name="failed_events_require_error_class",
    ),
    CheckConstraint(
        "error_message IS NULL OR error_class IS NOT NULL",
        name="error_message_requires_error_class",
    ),
    CheckConstraint(
        f"(event_type NOT IN {_TERMINAL_EVENT_TYPES}) OR (attempt_number IS NULL)",
        name="terminal_events_are_run_level",
    ),
)

collection_run_receipts = Table(
    "collection_run_receipts",
    metadata,
    Column("run_id", String(255), ForeignKey("collection_runs.run_id"), nullable=False),
    Column("attempt_number", Integer, nullable=False),
    Column(
        "source_snapshot_id",
        String(255),
        ForeignKey("source_snapshots.snapshot_id"),
        nullable=False,
    ),
    Column("observed_window_start", DateTime(timezone=True)),
    Column("observed_window_end", DateTime(timezone=True)),
    Column("row_count", BigInteger),
    Column("byte_count", BigInteger),
    Column("receipt_sha256", String(64)),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("run_id", "attempt_number", "source_snapshot_id"),
    CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
    CheckConstraint(
        "(observed_window_start IS NULL) = (observed_window_end IS NULL)",
        name="observed_window_paired",
    ),
    CheckConstraint(
        "observed_window_start IS NULL OR observed_window_end IS NULL OR "
        "observed_window_end >= observed_window_start",
        name="observed_window_order",
    ),
    CheckConstraint("row_count IS NULL OR row_count >= 0", name="row_count_nonnegative"),
    CheckConstraint("byte_count IS NULL OR byte_count >= 0", name="byte_count_nonnegative"),
    _optional_sha256_check("receipt_sha256"),
)

collection_watermarks = Table(
    "collection_watermarks",
    metadata,
    Column("provider", String(100), nullable=False),
    Column("dataset", String(150), nullable=False),
    Column("stream", String(150), nullable=False),
    Column("watermark_seq", BigInteger, nullable=False),
    Column("run_id", String(255), ForeignKey("collection_runs.run_id"), nullable=False),
    Column("watermark_value", Text, nullable=False),
    Column("watermark_position", DateTime(timezone=True), nullable=False),
    Column("recorded_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("provider", "dataset", "stream", "watermark_seq"),
    UniqueConstraint("provider", "dataset", "stream", "run_id", name="stream_run"),
    CheckConstraint("watermark_seq >= 1", name="watermark_seq_positive"),
    _nonempty_check("provider"),
    _nonempty_check("dataset"),
    _nonempty_check("stream"),
    _nonempty_check("watermark_value"),
)

collection_usage_checkpoints = Table(
    "collection_usage_checkpoints",
    metadata,
    Column("checkpoint_id", String(255), primary_key=True),
    Column("schema_version", Integer, nullable=False),
    Column("provider", String(100), nullable=False, index=True),
    Column("coverage_start_utc", DateTime(timezone=True), nullable=False),
    Column("coverage_end_utc", DateTime(timezone=True), nullable=False),
    Column("usage_record_count", BigInteger, nullable=False),
    Column("usage_records_root_sha256", String(64), nullable=False),
    Column("authority_id", String(255), nullable=False),
    Column("key_id", String(255), nullable=False),
    Column("generated_at_utc", DateTime(timezone=True), nullable=False),
    Column("signature_algorithm", String(32), nullable=False),
    Column("signature_version", Integer, nullable=False),
    Column("signature", LargeBinary, nullable=False),
    Column("registered_at_utc", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("schema_version = 1", name="schema_version_supported"),
    CheckConstraint(
        "checkpoint_id = btrim(checkpoint_id) AND checkpoint_id ~ '^[^[:space:]]+$'",
        name="checkpoint_id_normalized",
    ),
    CheckConstraint(
        "provider ~ '^[a-z0-9][a-z0-9._-]*$'",
        name="provider_normalized",
    ),
    CheckConstraint("coverage_end_utc > coverage_start_utc", name="coverage_interval_order"),
    CheckConstraint("usage_record_count >= 0", name="usage_record_count_nonnegative"),
    _sha256_check("usage_records_root_sha256"),
    CheckConstraint(
        "authority_id = btrim(authority_id) AND authority_id ~ '^[^[:space:]]+$'",
        name="authority_id_normalized",
    ),
    CheckConstraint(
        "key_id = btrim(key_id) AND key_id ~ '^[^[:space:]]+$'",
        name="key_id_normalized",
    ),
    CheckConstraint("generated_at_utc >= coverage_end_utc", name="generation_after_coverage"),
    CheckConstraint("signature_algorithm = 'ed25519'", name="signature_algorithm_supported"),
    CheckConstraint("signature_version = 1", name="signature_version_supported"),
    CheckConstraint("octet_length(signature) = 64", name="signature_ed25519_size"),
)

collection_usage_records = Table(
    "collection_usage_records",
    metadata,
    Column("run_id", String(255), ForeignKey("collection_runs.run_id"), nullable=False),
    Column("usage_seq", BigInteger, nullable=False),
    Column("metric", String(100), nullable=False),
    Column("quantity", Numeric(24, 6), nullable=False),
    Column("unit", String(50), nullable=False),
    Column("evidence_json", _json_type(), nullable=False),
    Column("recorded_at_utc", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("run_id", "usage_seq"),
    CheckConstraint("usage_seq >= 1", name="usage_seq_positive"),
    _nonempty_check("metric"),
    _nonempty_check("unit"),
    CheckConstraint("quantity >= 0", name="quantity_nonnegative"),
)
