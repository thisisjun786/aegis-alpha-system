# ruff: noqa: T201 -- this opt-in benchmark emits JSON measurements to stdout
"""Opt-in synthetic ALFRED chain benchmark: python -m tests.storage.benchmark_promotion.

Uses a disposable installation and one fixed source of vintages revising one record.
Reports complete apply times (planning, publication and catalog completion), excluding
source installation. Query-count regressions belong in pytest; timings are evidence,
not a CI assertion dependent on runner load.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

from tests.isolation import isolate, live_roots, live_state_refusal


def benchmark(root: Path, generations: int) -> None:
    from aegis_alpha.storage.promotion.engine import (  # noqa: PLC0415 -- after isolation
        promote,
        verify_promotion,
    )
    from aegis_alpha.storage.workspace import (  # noqa: PLC0415 -- after isolation
        initialize,
        open_workspace,
    )
    from tests.storage.test_macro_fx import (  # noqa: PLC0415 -- reuse synthetic fixtures after isolation
        ALFRED,
        OPEN_END,
        RETRIEVED,
        _alfred_spec,
        _source,
    )

    start = date(2000, 1, 1)
    rows = [
        ("SYNTHETIC", start, start + timedelta(days=i), OPEN_END, str(i + 1), RETRIEVED)
        for i in range(generations)
    ]
    initialize(root)
    times = []
    with open_workspace(root, writable=True, strategy_write=True) as workspace:
        pin = _source(workspace, ALFRED, rows, tag="alfred-scaling")
        parent = None
        for ordinal in range(generations):
            day = start + timedelta(days=ordinal)
            document = _alfred_spec(pin, parent=parent, partition=(day, day + timedelta(days=1)))
            before = time.perf_counter()
            result = promote(workspace, *document, apply=True)
            times.append(time.perf_counter() - before)
            if result["published"] is not True or result["delta_rows"] != 1:
                raise AssertionError(result)
            parent = str(result["generation_id"])
            if (ordinal + 1) % 100 == 0:
                print(
                    json.dumps(
                        {"generations": ordinal + 1, "last_20_mean_s": statistics.mean(times[-20:])}
                    ),
                    flush=True,
                )
        assert parent is not None
        verified = verify_promotion(workspace, parent, deep=True)
        print(
            json.dumps(
                {
                    "generations": generations,
                    "first_20_mean_s": statistics.mean(times[5:25]),
                    "last_20_mean_s": statistics.mean(times[-20:]),
                    "total_apply_s": sum(times),
                    "deep_verified": verified,
                }
            ),
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generations", type=int, default=2539)
    args = parser.parse_args()
    if not 25 <= args.generations <= 9000:  # noqa: PLR2004 -- bounded fixture before its retrieval date
        parser.error("generations must be between 25 and 9000")
    roots = live_roots(os.environ.get("HOME"))
    with tempfile.TemporaryDirectory(prefix="aas-promotion-benchmark-") as scratch:
        root = Path(scratch)
        isolate(root, os.environ)
        refusal = live_state_refusal(os.environ, roots)
        if refusal:
            raise ValueError(refusal)
        benchmark(root / "installation", args.generations)


if __name__ == "__main__":
    main()
