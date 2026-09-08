"""Immutable receipts for verified legacy snapshot adoption."""

from __future__ import annotations

from sqlalchemy import BigInteger, CheckConstraint, Column, DateTime, Integer, Table, Text

from aegis_alpha.metadata.schema import _nonempty_check, _sha256_check, metadata

data_adoptions = Table(
    "data_adoptions",
    metadata,
    Column("adoption_id", Text, primary_key=True),
    Column("source_manifest_sha256", Text, nullable=False, unique=True),
    Column("source_system_identifier", Text, nullable=False),
    Column("source_database", Text, nullable=False),
    Column("source_alembic_head", Text, nullable=False),
    Column("adopted_at_utc", DateTime(timezone=True), nullable=False),
    Column("table_count", Integer, nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("file_count", Integer, nullable=False),
    _nonempty_check("adoption_id"),
    _sha256_check("source_manifest_sha256"),
    _nonempty_check("source_system_identifier"),
    _nonempty_check("source_database"),
    _nonempty_check("source_alembic_head"),
    CheckConstraint("table_count > 0", name="table_count_positive"),
    CheckConstraint("row_count >= 0", name="row_count_nonnegative"),
    CheckConstraint("file_count >= 0", name="file_count_nonnegative"),
    schema="engine",
)
