from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import Engine, event
from sqlalchemy.dialects import postgresql

from aegis_alpha.collection.provider_usage_repository import ProviderUsageRepository
from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunPlan,
    CollectionUsageRecord,
)
from aegis_alpha.collection.registry import CollectionRegistry

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)
_RECORDED = _START + timedelta(hours=1)
_PREFIX_SHORTER = "norgate-assetid-155484-assertion"
_PREFIX_LONGER = "norgate-assetid-1554840-assertion"
_TEXT_ORDER_COLUMNS = (
    "collection_usage_records.run_id",
    "collection_runs.plan_id",
    "collection_run_plans.plan_sha256",
    "collection_run_plans.provider",
    "collection_usage_records.metric",
    "collection_usage_records.unit",
)


def _compiled_leaf_sql() -> str:
    statement = ProviderUsageRepository.select_leaves(
        provider="fmp",
        coverage_start_utc=_START,
        coverage_end_utc=_END,
    )
    return str(statement.compile(dialect=postgresql.dialect()))


def test_usage_leaf_select_emits_c_collation_for_every_text_order_key() -> None:
    sql = " ".join(_compiled_leaf_sql().split())
    for column in _TEXT_ORDER_COLUMNS:
        assert f'{column} COLLATE "C"' in sql
    assert "recorded_at_utc COLLATE" not in sql
    assert "usage_seq COLLATE" not in sql
    assert "quantity COLLATE" not in sql


def test_prefix_ids_use_c_collation_on_the_real_database_path(clean_postgres: Engine) -> None:
    registry = CollectionRegistry(clean_postgres)
    for run_id in (_PREFIX_LONGER, _PREFIX_SHORTER):
        plan = CollectionRunPlan(
            plan_id=f"plan-{run_id}",
            schema_version=1,
            provider="fmp",
            dataset="usage-order",
            mode=CollectionMode.PROBE,
            requested_window_start=None,
            requested_window_end=None,
            parameters={"run": run_id},
            created_at_utc=_START,
        )
        registry.register_plan(plan)
        registry.start_run(
            CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=plan.created_at_utc)
        )
        registry.record_usage(
            CollectionUsageRecord(
                run_id=run_id,
                usage_seq=1,
                metric="provider_requests",
                quantity=Decimal(1),
                unit="count",
                recorded_at_utc=_RECORDED,
                evidence={"test": True},
            )
        )

    ordered_statements: list[str] = []

    def capture_ordered_query(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: object,
    ) -> None:
        normalized = " ".join(statement.split())
        if " ORDER BY " in f" {normalized.upper()} ":
            ordered_statements.append(normalized)

    event.listen(clean_postgres, "before_cursor_execute", capture_ordered_query)
    try:
        with clean_postgres.connect() as connection:
            leaves = ProviderUsageRepository.leaves(
                connection,
                provider="fmp",
                coverage_start_utc=_START,
                coverage_end_utc=_END,
            )
    finally:
        event.remove(clean_postgres, "before_cursor_execute", capture_ordered_query)

    assert [leaf.run_id for leaf in leaves] == [_PREFIX_SHORTER, _PREFIX_LONGER]
    assert _PREFIX_SHORTER < _PREFIX_LONGER
    matching = [
        statement for statement in ordered_statements if "collection_usage_records" in statement
    ]
    assert matching
    assert all('COLLATE "C"' in statement for statement in matching)
    for column in _TEXT_ORDER_COLUMNS:
        assert any(f'{column} COLLATE "C"' in statement for statement in matching)
