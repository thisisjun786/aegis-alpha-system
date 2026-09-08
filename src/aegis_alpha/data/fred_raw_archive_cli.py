"""Manual FRED raw archive entry point; unrelated to standing collection grants."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from aegis_alpha.data.fred_alfred_series import MACRO_SERIES_IDS
from aegis_alpha.data.fred_raw_archive import acquire_raw, plan_archive

NOTICE = (
    "This product uses the FRED® API but is not endorsed or certified by "
    "the Federal Reserve Bank of St. Louis."
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, epilog=NOTICE)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--max-calls", type=int, required=True)
    parser.add_argument("--series", action="append")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args(argv)
    credential = os.environ.get("FRED_API_KEY", "") if args.execute else ""
    try:
        series = tuple(args.series) if args.series else MACRO_SERIES_IDS
        result = {
            **plan_archive(args.output_root, series, args.max_calls),
            "as_of": args.as_of.isoformat(),
        }
        if args.execute:
            result = acquire_raw(
                args.output_root,
                credential=credential,
                max_calls=args.max_calls,
                series=series,
                as_of=args.as_of,
            )
        print(json.dumps({**result, "notice": NOTICE}, indent=2))  # noqa: T201 -- CLI result
    except Exception as error:  # noqa: BLE001 -- never allow a key-bearing traceback at CLI
        message = str(error)
        if credential and credential in message:
            message = f"FRED raw acquisition failed: {type(error).__name__}"
        print(json.dumps({"error": message}), file=sys.stderr)  # noqa: T201 -- safe diagnostic
        return 1
    return 0
