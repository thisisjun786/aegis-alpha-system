"""Approval-free derived finalization for a validated collection marker."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

from aegis_alpha.collection.records import CollectionRunPlan, RunEventType
from aegis_alpha.data.fmp_approval import ApprovalExpiredError
from aegis_alpha.data.fmp_collection_work import publish_completion
from aegis_alpha.data.fmp_collector import FmpCollector
from aegis_alpha.data.fmp_collector_state import marker_advances, marker_ledger, marker_path
from aegis_alpha.data.fmp_marker_recovery_context import marker_finalization_context


class PostSuccessWatermarkError(RuntimeError):
    """A successful run needs same-run watermark recovery."""


def finalize_collection_marker(
    collector: FmpCollector,
    *,
    run_id: str,
    plan: CollectionRunPlan,
    publication: Mapping[str, object],
    marker_recovery: bool,
) -> tuple[tuple[str, str, str], ...]:
    ledger = marker_ledger(publication)
    state = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
    with marker_finalization_context(collector, marker_recovery=marker_recovery):
        if state is not None and state.state is RunEventType.ATTEMPT_SUCCEEDED:
            collector.succeed(run_id=run_id)
        elif state is None or state.state is not RunEventType.RUN_SUCCEEDED:
            collector.record_usage(
                run_id=run_id,
                recorded_at_utc=collector._clock(),  # noqa: SLF001
                ledger=ledger,
            )
            receipt = collector.build_receipt(
                run_id=run_id,
                plan=plan,
                ledger=ledger,
                publication_marker_sha256=hashlib.sha256(
                    marker_path(collector, run_id).read_bytes()
                ).hexdigest(),
            )
            collector.publish(dataset_publications=(), receipt_bytes=receipt)
            publish_completion(
                collector,
                run_id=run_id,
                plan_id=plan.plan_id,
                receipt=receipt,
            )
            collector.succeed(run_id=run_id)
        try:
            return collector.advance_watermarks(
                run_id=run_id,
                advances=marker_advances(publication),
            )
        except ApprovalExpiredError:
            raise
        except Exception as error:
            raise PostSuccessWatermarkError("successful run requires watermark recovery") from error
