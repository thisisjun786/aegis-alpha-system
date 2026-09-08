"""Manual SEC bulk archive acquisition CLI; planning is side-effect free."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from aegis_alpha.compute_resources import resolve_compute_budget
from aegis_alpha.data.sec_bulk import ARCHIVES, acquire_archive, archive_plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", choices=tuple(ARCHIVES), required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = archive_plan(args.archive, args.output_root, args.max_bytes)
        if args.execute:
            budget = resolve_compute_budget()
            result = acquire_archive(
                args.archive,
                args.output_root,
                user_agent=os.environ.get("SEC_USER_AGENT", ""),
                max_bytes=args.max_bytes,
                workers=min(64, budget.hash_workers),
            )
        print(json.dumps(result, sort_keys=True, indent=2))  # noqa: T201 -- CLI result
    except (ValueError, OSError, RuntimeError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)  # noqa: T201 -- CLI diagnostic
        return 1
    return 0
