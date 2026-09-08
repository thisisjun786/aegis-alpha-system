"""Operator entry point for the bounded AAS-DATA-013 SEC collector.

G-A is synthetic. A live invocation requires a cleared SEC registry policy,
SEC_USER_AGENT, and --max-calls; every failed precondition makes zero provider
calls. The User-Agent value is never printed.
"""

from __future__ import annotations

import sys

from aegis_alpha.data.sec_collector_cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
