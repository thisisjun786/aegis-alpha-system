from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import Connection, Select, select

from aegis_alpha.collection.schema import (
    collection_run_plans,
    collection_runs,
    collection_usage_records,
)
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal


class ProviderUsageRepository:
    """Read usage only through its authoritative run-plan provider lineage."""

    @staticmethod
    def select_leaves(
        *,
        provider: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
    ) -> Select[Any]:
        """Canonical FMP usage read; text ORDER BY keys use PostgreSQL C collation."""

        return (
            select(
                collection_usage_records.c.run_id,
                collection_usage_records.c.usage_seq,
                collection_runs.c.plan_id,
                collection_run_plans.c.plan_sha256,
                collection_run_plans.c.provider,
                collection_usage_records.c.metric,
                collection_usage_records.c.quantity,
                collection_usage_records.c.unit,
                collection_usage_records.c.recorded_at_utc,
            )
            .select_from(collection_usage_records)
            .join(
                collection_runs,
                collection_runs.c.run_id == collection_usage_records.c.run_id,
            )
            .join(
                collection_run_plans,
                collection_run_plans.c.plan_id == collection_runs.c.plan_id,
            )
            .where(
                collection_run_plans.c.provider == provider,
                collection_usage_records.c.recorded_at_utc >= coverage_start_utc,
                collection_usage_records.c.recorded_at_utc < coverage_end_utc,
            )
            .order_by(
                collection_usage_records.c.recorded_at_utc,
                collection_usage_records.c.run_id.collate("C"),
                collection_usage_records.c.usage_seq,
                collection_runs.c.plan_id.collate("C"),
                collection_run_plans.c.plan_sha256.collate("C"),
                collection_run_plans.c.provider.collate("C"),
                collection_usage_records.c.metric.collate("C"),
                collection_usage_records.c.quantity,
                collection_usage_records.c.unit.collate("C"),
            )
        )

    @staticmethod
    def leaves(
        connection: Connection,
        *,
        provider: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
    ) -> Sequence[UsageRecordLeaf]:
        statement = ProviderUsageRepository.select_leaves(
            provider=provider,
            coverage_start_utc=coverage_start_utc,
            coverage_end_utc=coverage_end_utc,
        )
        rows = connection.execute(statement).mappings()
        return tuple(
            UsageRecordLeaf(
                run_id=cast("str", row["run_id"]),
                usage_seq=cast("int", row["usage_seq"]),
                plan_id=cast("str", row["plan_id"]),
                plan_sha256=cast("str", row["plan_sha256"]),
                provider=cast("str", row["provider"]),
                metric=cast("str", row["metric"]),
                quantity=cast("Decimal", row["quantity"]),
                unit=cast("str", row["unit"]),
                recorded_at_utc=cast("datetime", row["recorded_at_utc"]),
            )
            for row in rows
        )
