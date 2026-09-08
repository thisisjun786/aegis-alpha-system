from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Identity,
    Index,
    String,
    Table,
    func,
)

from aegis_alpha.metadata.schema import (
    _nonempty_check,
    _sha256_check,
    metadata,
)

eligibility_decisions = Table(
    "eligibility_decisions",
    metadata,
    Column("ledger_sequence", BigInteger, Identity(), primary_key=True),
    Column("dataset_id", String(255), nullable=False),
    Column("dataset_version", String(100), nullable=False),
    Column(
        "flag",
        String(16),
        nullable=False,
    ),
    Column(
        "polarity",
        String(16),
        nullable=False,
    ),
    Column("owner_receipt_sha256", String(64), nullable=False),
    Column("decided_at_utc", DateTime(timezone=True), nullable=False),
    Column("decided_by", String(255), nullable=False),
    Column(
        "registered_at_utc",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    ForeignKeyConstraint(
        ("dataset_id", "dataset_version"),
        ("dataset_versions.dataset_id", "dataset_versions.dataset_version"),
        match="FULL",
        ondelete="NO ACTION",
    ),
    _nonempty_check("dataset_id"),
    _nonempty_check("dataset_version"),
    _nonempty_check("decided_by"),
    _sha256_check("owner_receipt_sha256"),
    CheckConstraint(
        "flag IN ('backtest', 'paper', 'order')",
        name="flag_allowed",
    ),
    CheckConstraint(
        "polarity IN ('GRANT', 'REVOKE')",
        name="polarity_allowed",
    ),
)

_ = Index(
    "ix_eligibility_decisions_dataset_version_flag_ledger_sequence",
    eligibility_decisions.c.dataset_id,
    eligibility_decisions.c.dataset_version,
    eligibility_decisions.c.flag,
    eligibility_decisions.c.ledger_sequence.desc(),
)

__all__ = ["eligibility_decisions"]
