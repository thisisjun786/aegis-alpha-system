"""Settle bound FRED reservations without granting direct usage UPDATE."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "20260905_0013"
down_revision: str | None = "20260905_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SIGNATURE = "engine.settle_fred_budget(text, text, bigint, bigint, timestamp with time zone)"


def upgrade() -> None:
    op.execute("""
CREATE FUNCTION engine.settle_fred_budget(
    reservation_id text, actual_id text, requested bigint, actual bigint, recorded timestamptz
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    budget record;
    run_plan record;
    prior record;
    day_start timestamptz;
    actual_evidence jsonb;
BEGIN
    IF reservation_id IS NULL OR actual_id IS NULL OR reservation_id = actual_id
       OR requested IS NULL OR requested < 1 OR requested > 2147483647
       OR actual IS NULL OR actual < 0 OR actual > requested
       OR recorded IS NULL OR NOT isfinite(recorded) THEN
        RAISE EXCEPTION 'invalid FRED settlement request' USING ERRCODE = '23514';
    END IF;
    day_start := date_trunc('day', recorded AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    IF day_start <> date_trunc('day', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'FRED settlement crosses UTC day' USING ERRCODE = '23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'aegis_alpha.fred_alfred.daily_budget:fred_alfred:' ||
        to_char(day_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 0));
    SELECT u.*, p.provider, p.dataset, p.parameters_json INTO budget
    FROM public.collection_usage_records u
    JOIN public.collection_runs r ON r.run_id=u.run_id
    JOIN public.collection_run_plans p ON p.plan_id=r.plan_id
    WHERE u.run_id=reservation_id AND u.usage_seq=1 FOR UPDATE OF u;
    SELECT p.provider, p.dataset, r.created_at_utc INTO run_plan
    FROM public.collection_runs r JOIN public.collection_run_plans p ON p.plan_id=r.plan_id
    WHERE r.run_id=actual_id;
    IF budget.run_id IS NULL OR run_plan.provider IS DISTINCT FROM 'fred_alfred'
       OR run_plan.dataset IS DISTINCT FROM 'fred_alfred_observations'
       OR budget.provider IS DISTINCT FROM 'fred_alfred'
       OR budget.dataset IS DISTINCT FROM 'fred_alfred_daily_budget'
       OR budget.metric IS DISTINCT FROM 'calls_attempted' OR budget.unit IS DISTINCT FROM 'call'
       OR budget.parameters_json->>'actual_run_id' IS DISTINCT FROM actual_id
       OR budget.parameters_json->>'requested_calls' IS DISTINCT FROM requested::text
       OR budget.recorded_at_utc < day_start
       OR budget.recorded_at_utc >= day_start + interval '24 hours'
       OR run_plan.created_at_utc < day_start
       OR run_plan.created_at_utc >= day_start + interval '24 hours'
       OR run_plan.created_at_utc IS NULL THEN
        RAISE EXCEPTION 'FRED reservation binding is invalid' USING ERRCODE = '23514';
    END IF;
    actual_evidence := jsonb_build_object('source', 'fred-alfred-actual-consumption',
                                        'reconciles_reservation', reservation_id);
    SELECT * INTO prior FROM public.collection_usage_records WHERE run_id=actual_id AND usage_seq=1;
    -- Advisory and reservation-row waits must not reuse the pre-wait day check.
    IF day_start <> date_trunc('day', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'FRED settlement crosses UTC day after lock wait' USING ERRCODE = '23514';
    END IF;
    IF budget.evidence_json->>'source' = 'fred-alfred-reconciled-reservation' THEN
        IF budget.quantity = 0 AND budget.evidence_json->>'actual_run_id' = actual_id
           AND budget.evidence_json->>'actual_calls' = actual::text
           AND budget.evidence_json->>'reserved_calls' = requested::text
           AND prior.run_id = actual_id AND prior.quantity = actual
           AND prior.metric = 'calls_attempted' AND prior.unit = 'call'
           AND prior.evidence_json = actual_evidence
           AND prior.recorded_at_utc >= day_start
           AND prior.recorded_at_utc < day_start + interval '24 hours' THEN
            RETURN;
        END IF;
        RAISE EXCEPTION 'FRED settlement retry conflicts' USING ERRCODE = '23514';
    END IF;
    IF budget.evidence_json->>'source' IS DISTINCT FROM 'fred-alfred-daily-budget-reservation'
       OR budget.quantity IS DISTINCT FROM requested::numeric OR prior.run_id IS NOT NULL THEN
        RAISE EXCEPTION 'FRED settlement evidence conflicts' USING ERRCODE = '23514';
    END IF;
    INSERT INTO public.collection_usage_records
        (run_id, usage_seq, metric, quantity, unit, evidence_json, recorded_at_utc)
    VALUES (actual_id, 1, 'calls_attempted', actual, 'call', actual_evidence, recorded);
    UPDATE public.collection_usage_records SET quantity=0, evidence_json=jsonb_build_object(
        'source', 'fred-alfred-reconciled-reservation', 'actual_run_id', actual_id,
        'reserved_calls', requested, 'actual_calls', actual)
    WHERE run_id=reservation_id AND usage_seq=1;
    -- An INSERT can wait on an invisible unique-key contender. Refuse the whole
    -- statement if that wait (or any write) crossed midnight, rolling back both rows.
    IF day_start <> date_trunc('day', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'FRED settlement crosses UTC day after write wait' USING ERRCODE = '23514';
    END IF;
END;
$$;
    """)
    op.execute(f"REVOKE ALL ON FUNCTION {_SIGNATURE} FROM PUBLIC")


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {_SIGNATURE}")
