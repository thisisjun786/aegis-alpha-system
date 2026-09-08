"""Measure the AAS-DATA-009B retained corporate-action envelope without I/O.

The existing streaming writer holds actions for final global ordering.  This
script builds the exact observed 711,626-action cardinality using the same
``CanonicalCorporateAction`` and ``SourceLineage`` objects, then reproduces the
list/partition-tuple/final-sort retention sequence.  It does not mount or read
the Norgate source, open a database, make a provider call, or create an output
file.
"""

from __future__ import annotations

import json
import platform
import resource
import time
from datetime import UTC, date, datetime

from aegis_alpha.data.canonical_records import (
    CanonicalCorporateAction,
    CorporateActionType,
    SourceLineage,
)

OBSERVED_POSITIVE_DIVIDEND_ROWS = 711_626


def _rss_bytes(value: int) -> int:
    """Normalize ``getrusage`` RSS units across macOS and Linux."""

    return value if platform.system() == "Darwin" else value * 1024


def main() -> int:
    """Print one JSON measurement of the full retained action envelope."""

    started = time.perf_counter()
    baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    shared_source = {
        "provider": "norgate",
        "dataset_id": "norgate-us-platinum-provider-normalized",
        "dataset_version": (
            "2026-07-29-us-platinum.full-v1."
            "f75c94172a988530fc6f9e674d5c4264f418a9aebabd7034982269870f61a987"
        ),
        "source_snapshot_id": "norgate-2026-07-29-us-platinum",
        "artifact_relative_path": "adjustment_type=CAPITAL/year=2020/part-00000.parquet",
        "artifact_sha256": "a" * 64,
    }
    observed = datetime(2026, 7, 28, 21, tzinfo=UTC)
    actions = [
        CanonicalCorporateAction(
            instrument_id=f"instrument-{OBSERVED_POSITIVE_DIVIDEND_ROWS - ordinal:036d}",
            action_type=CorporateActionType.DIVIDEND,
            effective_date=date(2020 + ordinal % 7, 1 + ordinal % 12, 1 + ordinal % 28),
            value=0.25,
            currency="USD",
            observed_at=observed,
            available_at=observed,
            lineage=SourceLineage(**shared_source, row_ordinal=ordinal),
            derived_from="norgate-us-platinum-provider-normalized:dividend",
        )
        for ordinal in range(OBSERVED_POSITIVE_DIVIDEND_ROWS)
    ]
    partition_actions = tuple(actions)
    final_actions = tuple(
        sorted(
            (action for state in (partition_actions,) for action in state),
            key=lambda item: (
                item.instrument_id,
                item.effective_date.isoformat(),
                item.action_type.value,
            ),
        )
    )
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(  # noqa: T201 - this standalone measurement artifact intentionally emits JSON
        json.dumps(
            {
                "action_count": len(final_actions),
                "baseline_max_rss_bytes": _rss_bytes(baseline),
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "measurement_scope": "synthetic action objects plus list/tuple/sort retention",
                "peak_max_rss_bytes": _rss_bytes(maximum),
                "platform": platform.system(),
                "provider_calls_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
