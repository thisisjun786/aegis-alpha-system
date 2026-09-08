from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

_T0 = datetime(2026, 7, 29, 11, 0, tzinfo=UTC)


def _seed_plan_and_run(clean_postgres: Engine) -> None:
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_run_plans (plan_id, schema_version, provider, dataset, "
                "mode, requested_window_start, requested_window_end, parameters_json, "
                "plan_sha256, created_at_utc) VALUES ('plan-c', 1, 'fmp', 'eod_prices', "
                "'incremental', NULL, NULL, '{}', :sha, :created)"
            ),
            {"sha": "a" * 64, "created": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO collection_runs (run_id, plan_id, created_at_utc) "
                "VALUES ('run-c', 'plan-c', :created)"
            ),
            {"created": _T0},
        )


def _insert_event(clean_postgres: Engine, **overrides: object) -> None:
    values: dict[str, object] = {
        "run_id": "run-c",
        "event_seq": 1,
        "event_type": "attempt_started",
        "attempt_number": 1,
        "retry_of_attempt": None,
        "error_class": None,
        "error_message": None,
        "occurred_at_utc": _T0,
        "details_json": "{}",
    }
    values.update(overrides)
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_run_events (run_id, event_seq, event_type, "
                "attempt_number, retry_of_attempt, error_class, error_message, "
                "occurred_at_utc, details_json) VALUES (:run_id, :event_seq, :event_type, "
                ":attempt_number, :retry_of_attempt, :error_class, :error_message, "
                ":occurred_at_utc, :details_json)"
            ),
            values,
        )


def test_failed_event_without_error_class_is_rejected_at_database(
    clean_postgres: Engine,
) -> None:
    _seed_plan_and_run(clean_postgres)

    with pytest.raises(IntegrityError):
        _insert_event(clean_postgres, event_type="attempt_failed")


def test_failed_event_with_blank_error_class_is_rejected_at_database(
    clean_postgres: Engine,
) -> None:
    _seed_plan_and_run(clean_postgres)

    with pytest.raises(IntegrityError):
        _insert_event(clean_postgres, event_type="attempt_failed", error_class="   ")


def test_succeeded_event_with_error_class_is_rejected_at_database(
    clean_postgres: Engine,
) -> None:
    _seed_plan_and_run(clean_postgres)

    with pytest.raises(IntegrityError):
        _insert_event(
            clean_postgres,
            event_type="attempt_succeeded",
            error_class="Boom",
            error_message="boom",
        )


def test_run_view_projects_terminal_false_for_run_without_events(
    clean_postgres: Engine,
) -> None:
    _seed_plan_and_run(clean_postgres)

    with clean_postgres.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT state, terminal, attempt_count, last_event_seq "
                    "FROM collection_run_states WHERE run_id = 'run-c'"
                )
            )
            .mappings()
            .one()
        )
    assert row["state"] is None
    assert row["terminal"] is False
    assert row["attempt_count"] == 0
    assert row["last_event_seq"] is None


def test_plan_mode_must_be_a_known_literal(clean_postgres: Engine) -> None:
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_run_plans (plan_id, schema_version, provider, dataset, "
                "mode, requested_window_start, requested_window_end, parameters_json, "
                "plan_sha256, created_at_utc) VALUES ('plan-bad', 1, 'fmp', 'eod_prices', "
                "'scheduled', NULL, NULL, '{}', :sha, :created)"
            ),
            {"sha": "b" * 64, "created": _T0},
        )


def test_plan_window_must_be_paired(clean_postgres: Engine) -> None:
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_run_plans (plan_id, schema_version, provider, dataset, "
                "mode, requested_window_start, requested_window_end, parameters_json, "
                "plan_sha256, created_at_utc) VALUES ('plan-bad', 1, 'fmp', 'eod_prices', "
                "'probe', :start, NULL, '{}', :sha, :created)"
            ),
            {"sha": "c" * 64, "created": _T0, "start": _T0},
        )


def test_usage_quantity_must_be_nonnegative_at_database(clean_postgres: Engine) -> None:
    _seed_plan_and_run(clean_postgres)

    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_usage_records (run_id, usage_seq, metric, quantity, "
                "unit, evidence_json, recorded_at_utc) VALUES ('run-c', 1, 'provider_cost', "
                ":quantity, 'USD', '{}', :recorded)"
            ),
            {"quantity": Decimal("-0.5"), "recorded": _T0},
        )
